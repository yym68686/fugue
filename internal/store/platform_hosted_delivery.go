package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/routeartifact"
)

func hostedAuthorityDigest(p pc.HostedScopeProposal) (string, error) {
	return pc.Digest([]any{"fugue.hosted-authority/v1", p.Base, p.Authority})
}

func (s *Store) hostedSelectedBase(read dnsRouteSourceReader, p pc.HostedScopeProposal) (model.PlatformArtifact, error) {
	keys := s.platformArtifactSigningKeyring()
	if err := validateDNSProducerPolicySelection(read, p.Authority.Producer, keys); err != nil {
		return model.PlatformArtifact{}, err
	}
	f := p.Base
	full, rel, err := dnsProducerSelection(read, model.PlatformArtifactKindReleaseSet, pc.GlobalScopeKey, "full", model.PlatformPublicationPrecondition{ArtifactID: f.ReleaseSetID, ContentHash: f.ReleaseSetDigest, ReleaseID: f.ReleaseID, FencingToken: f.FencingToken}, keys)
	if err != nil || full.Metadata[platformproducer.PolicyReleaseMetadata] != p.Authority.Producer.ReleaseID || full.Content["publication_role"] != nil && full.Content["publication_role"] != "" || rel.VerificationState != model.PlatformArtifactVerificationStateVerified || rel.VerifiedLKGGeneration != full.Generation {
		return model.PlatformArtifact{}, ErrConflict
	}
	lkg, err := read.lkg(pc.GlobalScopeKey)
	if err != nil || lkg == nil || lkg.ID != p.Authority.LKGID || lkg.VerifiedByReleaseID != rel.ID || lkg.VerificationEvidenceHash != p.Authority.EvidenceHash || !lkg.ExpiresAt.Equal(p.Authority.ExpiresAt) || !platformsafety.EvaluatePlatformLKGSnapshot(*lkg, full, keys, time.Now().UTC()).Pass {
		return model.PlatformArtifact{}, ErrConflict
	}
	return full, nil
}

// Input closure checks on the parent may take time. Recheck the exact host
// head and current business revision before disclosing the command. This
// read never creates a decision, even for a previously accepted draft.
func (s *Store) CheckHostedMemberSelection(ctx context.Context, p routeartifact.HostedSelectedPacket) error {
	if s == nil || s.db == nil {
		return ErrNotFound
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var ok bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_hosted_selections WHERE hostname=$1 AND scope_key=$2 AND project_id=$3 AND fence=$4 AND decision=$5
 AND phase=$6 AND draft_id=$7 AND draft_digest=$8 AND program_digest=$9 AND authority_digest=$10 AND business_digest=$11 AND valid_until=$12 AND valid_until>clock_timestamp())`,
		p.Delivery.Host, p.Scope, p.Project, p.Fence, p.Decision, p.Delivery.Phase, p.Draft, p.Delivery.DraftDigest, p.Delivery.ImageDigest, p.Authority, p.Business, p.ValidUntil).Scan(&ok)
	if err != nil {
		return err
	}
	if !ok {
		return ErrConflict
	}
	in, err := pgProjectHostInputs(ctx, tx, p.Project, p.Delivery.Host, false)
	if err != nil {
		return err
	}
	business, err := hostedBusinessRevision(in)
	if err != nil || business != p.Business || !p.ValidUntil.After(time.Now()) {
		return ErrConflict
	}
	return tx.Commit()
}

// Resolve the chosen hostname from durable state. Neither a latest offer nor
// a global consumer ACK can enter here as a decision. The handler resolves
// the *serving* assignment and its input closure before and after this read.
// Original project/owner, full, producer, verified LKG, set and image are
// checked in one snapshot. No network call is made with the transaction held.
func (s *Store) ReadHostedMemberSelection(ctx context.Context, host string, who platformcontrol.PlatformComponentIdentityClaims, a model.PlatformConsumerAssignment, bind func(ProjectHostRouteInputs, pc.HostedScopeProposal) (string, error)) (routeartifact.HostedSelectedPacket, error) {
	var empty routeartifact.HostedSelectedPacket
	if s == nil || s.db == nil || bind == nil || host == "" || a.ScopeKey != pc.GlobalScopeKey || who.ScopeKey != a.ScopeKey || a.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || who.CredentialID == "" || who.TokenID == "" || who.ExpiresAtUnix <= time.Now().Unix() {
		return empty, ErrNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	packet, err := s.pgReadHostedMemberSelection(ctx, tx, host, who, a, bind, false)
	if err != nil {
		return empty, err
	}
	return packet, tx.Commit()
}

func (s *Store) pgReadHostedMemberSelection(ctx context.Context, tx *sql.Tx, host string, who platformcontrol.PlatformComponentIdentityClaims, a model.PlatformConsumerAssignment, bind func(ProjectHostRouteInputs, pc.HostedScopeProposal) (string, error), lockBusiness bool) (routeartifact.HostedSelectedPacket, error) {
	var empty, packet routeartifact.HostedSelectedPacket
	var program, draftHash string
	var body, manifest, priorManifest []byte
	var phase string
	err := tx.QueryRowContext(ctx, `SELECT scope_key,project_id,offer_sequence,draft_id,draft_digest,program_digest,program,
 fence,decision,phase,members,authority_digest,business_digest,selected_at,valid_until,previous_decision,previous_members,skipped_forward_from FROM fugue_hosted_selections
 WHERE hostname=$1 AND valid_until>clock_timestamp()`, host).Scan(&packet.Scope, &packet.Project, &packet.Offer, &packet.Draft, &draftHash, &program, &body, &packet.Fence, &packet.Decision, &phase, &manifest, &packet.Authority, &packet.Business, &packet.SelectedAt, &packet.ValidUntil, &packet.Previous, &priorManifest, &packet.SkippedForwardFrom)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, ErrNotFound
	}
	if err != nil {
		return empty, err
	}
	read := pgDNSRouteSourceReader(ctx, tx)
	p, err := s.hostedHeadProposal(read, host, packet.Scope, packet.Draft, draftHash, packet.Authority, packet.ValidUntil)
	if err != nil {
		return empty, err
	}
	f := p.Base
	if a.ReleaseSetID != f.ReleaseSetID || a.ArtifactReleaseID != f.ReleaseID || a.FencingToken != f.FencingToken {
		return empty, ErrNotFound
	}
	full, err := s.hostedSelectedBase(read, p)
	if err != nil {
		return empty, err
	}
	set, err := scanPlatformExpectedConsumerSet(tx.QueryRowContext(ctx, hostedCurrentSetSQL, a.ReleaseSetID, a.ArtifactReleaseID, a.ArtifactKind, a.ScopeKey))
	if err != nil || set.ID != a.ExpectedConsumerSetID || set.Revision != a.Revision || platformcontrol.ValidateDeclaredTrafficConsumerSet(full, set) != nil {
		return empty, ErrNotFound
	}
	view, _, err := routeartifact.AddressHostedPreparation(p, draftHash, program, body, phase, who, a, set)
	if err != nil {
		return empty, ErrNotFound
	}
	if err = s.checkHostedLiveHolds(ctx, tx, read, p, full, time.Now().UTC()); err != nil {
		return empty, err
	}
	members := map[string]string{}
	prior := map[string]string{}
	if err = json.Unmarshal(manifest, &members); err != nil || len(members) > 8192 || json.Unmarshal(priorManifest, &prior) != nil || len(prior) > 8192 {
		return empty, ErrConflict
	}
	packet.PriorParts = map[string]string{}
	for _, part := range view.Parts {
		k, e := view.PartKey(part)
		if e != nil || members[k] != part.ContentDigest {
			return empty, ErrNotFound
		}
		if packet.Fence > 1 {
			if !platformproducer.ValidDigest(prior[k]) {
				return empty, ErrConflict
			}
			packet.PriorParts[k] = prior[k]
		}
	}
	lock := ""
	if lockBusiness {
		lock = " FOR SHARE"
	}
	in, err := pgReadProjectHostInputs(ctx, tx, packet.Project, host, lock, lock)
	if err != nil {
		return empty, err
	}
	dir, err := bind(in, p)
	if err != nil || dir != "forward" && dir != "baseline" || (phase == "withdraw") != (dir == "baseline") {
		return empty, ErrConflict
	}
	actual, err := hostedBusinessRevision(in)
	if err != nil || actual != packet.Business || in.Project.ID != packet.Project || in.Domain.TenantID != p.Before.Owner.TenantID || in.Domain.AppID != p.Before.Owner.AppID || in.Domain.Hostname != host {
		return empty, ErrConflict
	}
	packet.Parent, packet.Child, packet.ChildDigest, packet.Delivery = a.ReleaseSetID, a.ArtifactID, a.ContentHash, view
	if !packet.ValidUntil.After(time.Now()) || who.ExpiresAtUnix <= time.Now().Unix() {
		return empty, ErrConflict
	}
	return packet, nil
}

func (s *Store) hostedHeadProposal(read dnsRouteSourceReader, host, scopeKey, id, hash, authority string, until time.Time) (pc.HostedScopeProposal, error) {
	p, err := s.hostedVerifiedDraft(read, host, scopeKey, id, hash, until)
	if err != nil {
		return p, err
	}
	auth, err := hostedAuthorityDigest(p)
	if err != nil || auth != authority {
		return p, ErrConflict
	}
	return p, nil
}

func (s *Store) hostedVerifiedDraft(read dnsRouteSourceReader, host, scopeKey, id, hash string, until time.Time) (pc.HostedScopeProposal, error) {
	var p pc.HostedScopeProposal
	draft, err := read.artifact(id)
	if err != nil {
		return p, err
	}
	raw, err := json.Marshal(draft.Content)
	if err != nil || len(raw) > 1<<20 || json.Unmarshal(raw, &p) != nil || p.Schema != pc.HostedProposalSchema || p.Host != host || p.Before.Owner.Hostname != host || p.Base.Validate() != nil ||
		draft.ID != id || draft.ContentHash != hash || draft.ScopeKey != scopeKey || draft.ArtifactKind != model.PlatformArtifactKindHostedPathProposal || draft.Status != model.PlatformArtifactStatusDraft || draft.CreatedByID != platformproducer.Actor || draft.CreatedByType != model.ActorTypeBootstrap || !platformsafety.EvaluateArtifactIntegrity(draft, s.platformArtifactSigningKeyring()).Pass {
		return p, ErrConflict
	}
	scope, key := NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: host, TenantID: p.Before.Owner.TenantID, AppID: p.Before.Owner.AppID})
	if scope != draft.Scope || key != scopeKey || until.After(p.Authority.ExpiresAt) {
		return p, ErrConflict
	}
	return p, nil
}

// Resolve the current recipient inside the write transaction. A receipt
// cannot introduce its own manifest, choose a node/zone, renew a lease or
// replace another domain. Match the producer's lock order before taking the
// single host lock; both DNS transports for every zone commit together.
func (s *Store) RecordHostedMemberObservation(ctx context.Context, host string, who platformcontrol.PlatformComponentIdentityClaims, a model.PlatformConsumerAssignment, o routeartifact.HostedObservation, bind func(ProjectHostRouteInputs, pc.HostedScopeProposal) (string, error)) error {
	if s == nil || s.db == nil || bind == nil || host == "" || host != o.Host || a.ScopeKey != pc.GlobalScopeKey || who.ScopeKey != a.ScopeKey || a.ReleaseChannel != "full" || who.CredentialID == "" || who.TokenID == "" || who.ConsumerID() != o.Member || who.ExpiresAtUnix <= time.Now().Unix() {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, "SET LOCAL lock_timeout='400ms'"); e != nil {
		return e
	}
	for _, k := range []string{platformproducer.Scope, pc.GlobalScopeKey} {
		if e = pgLockPromotionScope(ctx, tx, k, false); e != nil {
			return e
		}
	}
	p, e := s.pgReadHostedMemberSelection(ctx, tx, host, who, a, bind, true)
	if e != nil {
		return e
	}
	now := time.Now().UTC()
	rows, e := o.Evaluate(p, now)
	if e != nil {
		return ErrConflict
	}
	// The authoritative freshness limit is signed by the parent compiler,
	// not a duration chosen by the submitting consumer.
	child, e := pgDNSRouteSourceReader(ctx, tx).artifact(a.ArtifactID)
	if e != nil {
		return e
	}
	if child.ContentHash != a.ContentHash || !platformsafety.EvaluateArtifactIntegrity(child, s.platformArtifactSigningKeyring()).Pass {
		return ErrConflict
	}
	raw, e := json.Marshal(child.Content["policy"])
	var policy pc.PolicySnapshot
	if e != nil || json.Unmarshal(raw, &policy) != nil {
		return ErrConflict
	}
	probes := policy.TLSReadiness
	if a.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
		probes = policy.DNSReadiness
	}
	if probes == nil || pc.ValidateReadinessProbePolicy(probes) != nil {
		return ErrConflict
	}
	until := o.StartedAt.Add(time.Duration(probes.FactFreshnessSeconds) * time.Second)
	if !until.After(now) {
		return ErrConflict
	}
	if e = pgLockPromotionScope(ctx, tx, "hosted-selection:"+host, true); e != nil {
		return e
	}
	if e = s.acceptHostedWorker(ctx, tx, who, p.Delivery.SelectedWorkerRequired); e != nil {
		return e
	}
	for _, r := range rows {
		if until.Before(r.ValidUntil) {
			r.ValidUntil = until
		}
		e = pgRecordHostedPartLocked(ctx, tx, hostedPartReceipt{Host: host, Decision: p.Decision, Fence: p.Fence, Part: r.Part, Content: r.Content, Evidence: r.Evidence, Passing: r.Passing, ObservedAt: r.ObservedAt, ValidUntil: r.ValidUntil})
		if e != nil {
			return e
		}
	}
	if o.Failure != "local_apply" {
		// A valid wire attempt following local application proves this
		// executor still speaks the protocol; a failed attempt does not
		// count as a positive receipt for A (or for another host B).
		if p.ValidUntil.Before(until) {
			until = p.ValidUntil
		}
		if tokenUntil := time.Unix(who.ExpiresAtUnix, 0); tokenUntil.Before(until) {
			until = tokenUntil
		}
		if _, e = tx.ExecContext(ctx, hostedExecutorSeenSQL, o.Member, a.ArtifactKind, who.CredentialID, a.ReleaseSetID, a.ArtifactID, a.ExpectedConsumerSetID, a.FencingToken, o.StartedAt, until); e != nil {
			return e
		}
	}
	if who.ExpiresAtUnix <= time.Now().Unix() {
		return ErrConflict
	}
	return tx.Commit()
}
