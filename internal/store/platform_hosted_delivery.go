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
	var empty, packet routeartifact.HostedSelectedPacket
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
	var program, draftHash string
	var body, manifest, priorManifest []byte
	var phase string
	err = tx.QueryRowContext(ctx, `SELECT scope_key,project_id,offer_sequence,draft_id,draft_digest,program_digest,program,
 fence,decision,phase,members,authority_digest,business_digest,selected_at,valid_until,previous_decision,previous_members,skipped_forward_from FROM fugue_hosted_selections
 WHERE hostname=$1 AND valid_until>clock_timestamp()`, host).Scan(&packet.Scope, &packet.Project, &packet.Offer, &packet.Draft, &draftHash, &program, &body, &packet.Fence, &packet.Decision, &phase, &manifest, &packet.Authority, &packet.Business, &packet.SelectedAt, &packet.ValidUntil, &packet.Previous, &priorManifest, &packet.SkippedForwardFrom)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, ErrNotFound
	}
	if err != nil {
		return empty, err
	}
	read := pgDNSRouteSourceReader(ctx, tx)
	draft, err := read.artifact(packet.Draft)
	if err != nil {
		return empty, err
	}
	keys := s.platformArtifactSigningKeyring()
	var p pc.HostedScopeProposal
	raw, err := json.Marshal(draft.Content)
	if err != nil || len(raw) > 1<<20 || json.Unmarshal(raw, &p) != nil || p.Schema != pc.HostedProposalSchema || p.Host != host || p.Before.Owner.Hostname != host || p.Base.Validate() != nil ||
		draft.ID != packet.Draft || draft.ContentHash != draftHash || draft.ScopeKey != packet.Scope || draft.ArtifactKind != model.PlatformArtifactKindHostedPathProposal || draft.Status != model.PlatformArtifactStatusDraft || draft.CreatedByID != platformproducer.Actor || draft.CreatedByType != model.ActorTypeBootstrap || !platformsafety.EvaluateArtifactIntegrity(draft, keys).Pass {
		return empty, ErrConflict
	}
	scope, scopeKey := NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "hosted-path", Hostname: host, TenantID: p.Before.Owner.TenantID, AppID: p.Before.Owner.AppID})
	auth, err := hostedAuthorityDigest(p)
	if err != nil || auth != packet.Authority || scope != draft.Scope || scopeKey != packet.Scope || packet.ValidUntil.After(p.Authority.ExpiresAt) {
		return empty, ErrConflict
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
	in, err := pgProjectHostInputs(ctx, tx, packet.Project, host, false)
	if err != nil {
		return empty, err
	}
	dir, err := bind(in, p)
	if err != nil || dir != "forward" && dir != "baseline" || (phase == "withdraw") != (dir == "baseline") {
		return empty, ErrConflict
	}
	actual, err := hostedBusinessRevision(in)
	if err != nil || actual != packet.Business || in.Project.ID != packet.Project || in.Domain.TenantID != scope.TenantID || in.Domain.AppID != scope.AppID || in.Domain.Hostname != host {
		return empty, ErrConflict
	}
	packet.Parent, packet.Child, packet.ChildDigest, packet.Delivery = a.ReleaseSetID, a.ArtifactID, a.ContentHash, view
	if !packet.ValidUntil.After(time.Now()) || who.ExpiresAtUnix <= time.Now().Unix() {
		return empty, ErrConflict
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	return packet, nil
}
