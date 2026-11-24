package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
)

const hostedCurrentSetSQL = `SELECT id,release_set_id,artifact_release_id,artifact_kind,scope_key,scope_json,expected_generation,topology_revision,revision,requires_consumers,required_cardinality,optional_cardinality,heartbeat_deadline,convergence_deadline,consumers_json,created_at,updated_at
 FROM fugue_platform_expected_consumer_sets WHERE release_set_id=$1 AND artifact_release_id=$2 AND artifact_kind=$3 AND scope_key=$4 ORDER BY revision DESC,id LIMIT 1`

const hostedMemberFactReadSQL = `SELECT id,consumer_id,credential_id,token_id,component,node_id,artifact_kind,scope_key,release_set_id,expected_consumer_set_id,fencing_token,supported_kinds_json,protocol_version,schema_version,compatibility_capabilities_json,sequence,issued_at,nonce,generation_sequence,evidence_hash,identity_verified,desired_generation,actual_generation,candidate_generation,lkg_generation,apply_status,probe_status,serving_lkg,lkg_expired,last_error,last_heartbeat_at,updated_at
 FROM fugue_platform_consumer_instances WHERE consumer_id=$1 AND artifact_kind=$2 AND scope_key=$3`
const hostedMemberFactSQL = hostedMemberFactReadSQL + " FOR SHARE"

// Do not add this capability to a generic client or to old global receipts.
// Only a process that can persist, serve, observe and withdraw a scoped
// selection for this kind may advertise it. The fact does not count as an ACK.
const hostedMemberProtocol = routeartifact.HostedPacketKind

// Return the same refusal for the writer and read-only status. Report no
// credentials, raw heartbeats, or untrusted consumer error text to callers.
func pgHostedMemberBlocker(ctx context.Context, tx *sql.Tx, p pc.HostedScopeProposal, m model.PlatformExpectedConsumer, a model.PlatformConsumerAssignment, now time.Time, lock bool) (code, capability string, err error) {
	q := hostedMemberFactReadSQL
	if lock {
		q = hostedMemberFactSQL
	}
	f, err := scanPlatformConsumerInstance(tx.QueryRowContext(ctx, q, m.ConsumerID, a.ArtifactKind, pc.GlobalScopeKey))
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrNotFound) {
		return "member_evidence_missing", "", nil
	}
	if err != nil {
		return "", "", err
	}
	// Do not attribute the absence of a capability to the expected executor
	// when the stored fact belongs to a different or unauthenticated one.
	if !f.IdentityVerified || f.ConsumerID != m.ConsumerID || f.Component != m.Component || f.NodeID != m.NodeID ||
		f.ReleaseSetID != a.ReleaseSetID || f.ExpectedConsumerSetID != a.ExpectedConsumerSetID || f.FencingToken != a.FencingToken {
		return "member_evidence_not_current", "", nil
	}
	if !slices.Contains(f.CompatibilityCapabilities, hostedMemberProtocol) {
		return "member_capability_missing", hostedMemberProtocol, nil
	}
	if p.HasMemberBaselines() && !slices.Contains(f.CompatibilityCapabilities, routeartifact.HostedContinuationCapability) {
		return "member_capability_missing", routeartifact.HostedContinuationCapability, nil
	}
	if p.ExternalPath != nil && a.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
		if !slices.Contains(f.CompatibilityCapabilities, routeartifact.HostedSharedTargetCapability) {
			return "member_capability_missing", routeartifact.HostedSharedTargetCapability, nil
		}
		if len(p.ExternalPath.Held) > 0 && !slices.Contains(f.CompatibilityCapabilities, routeartifact.HostedSharedHoldCapability) {
			return "member_capability_missing", routeartifact.HostedSharedHoldCapability, nil
		}
	}
	ok, err := pgHostedMemberCapable(ctx, tx, m, a, f, now)
	if !ok && err == nil {
		return "member_evidence_not_current", "", nil
	}
	return "", "", err
}

func hostedMemberCapable(member model.PlatformExpectedConsumer, a model.PlatformConsumerAssignment, f model.PlatformConsumerInstance, now time.Time) bool {
	return trafficCapabilityFactFresh(member, f, now) && f.ConsumerID == member.ConsumerID && f.ArtifactKind == a.ArtifactKind && f.ScopeKey == a.ScopeKey &&
		f.ReleaseSetID == a.ReleaseSetID && f.ExpectedConsumerSetID == a.ExpectedConsumerSetID && f.FencingToken == a.FencingToken &&
		f.DesiredGeneration == a.ExpectedGeneration && f.ActualGeneration == a.ExpectedGeneration && f.ApplyStatus == "applied" && f.ProbeStatus == "passed" && !f.LKGExpired &&
		slices.Contains(f.SupportedKinds, a.ArtifactKind) && slices.Contains(f.CompatibilityCapabilities, hostedMemberProtocol)
}

// A full positive is required on first contact. After an executor takes
// ownership of any host, it must not say the entire global bundle passed.
// Retain only the old authenticated capability, renewed by the same actual
// executor against the same selected parent. Neither path verifies a host.
func pgHostedMemberCapable(ctx context.Context, tx *sql.Tx, m model.PlatformExpectedConsumer, a model.PlatformConsumerAssignment, f model.PlatformConsumerInstance, now time.Time) (bool, error) {
	if hostedMemberCapable(m, a, f, now) {
		return true, nil
	}
	if f.LastHeartbeatAt.After(now) || !hostedMemberCapable(m, a, f, f.LastHeartbeatAt) {
		return false, nil
	}
	freshness := time.Duration(m.HeartbeatFreshnessSeconds) * time.Second
	if freshness <= 0 {
		freshness = 90 * time.Second
	}
	var ok bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_hosted_executor_presence
 WHERE member_id=$1 AND kind=$2 AND credential_id=$3 AND parent_id=$4 AND child_id=$5 AND set_id=$6 AND parent_fence=$7
 AND observed_at>=$8 AND observed_at>$9 AND observed_at<=clock_timestamp() AND valid_until>clock_timestamp())`,
		m.ConsumerID, a.ArtifactKind, f.CredentialID, a.ReleaseSetID, a.ArtifactID, a.ExpectedConsumerSetID, a.FencingToken, f.LastHeartbeatAt, now.Add(-freshness)).Scan(&ok)
	return ok, err
}

const hostedExecutorSeenSQL = `INSERT INTO fugue_hosted_executor_presence AS old(member_id,kind,credential_id,parent_id,child_id,set_id,parent_fence,observed_at,valid_until)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
 ON CONFLICT(member_id,kind) DO UPDATE SET (credential_id,parent_id,child_id,set_id,parent_fence,observed_at,valid_until)=
 (EXCLUDED.credential_id,EXCLUDED.parent_id,EXCLUDED.child_id,EXCLUDED.set_id,EXCLUDED.parent_fence,EXCLUDED.observed_at,EXCLUDED.valid_until)
 WHERE old.observed_at<EXCLUDED.observed_at`

// A change for B may introduce another hold, but a new or lifted veto on
// A's own physical input must not be hidden by a prepared image.
func hostedHoldsCurrent(p pc.HostedScopeProposal, holds []pc.HostedAddressHold) bool {
	if len(p.Physical) == 0 || len(p.Eligible) == 0 {
		return false
	}
	blocked := map[pc.HostedAddressHold]bool{}
	for _, h := range holds {
		blocked[h] = true
	}
	eligible := map[pc.HostedAddressHold]bool{}
	for _, h := range p.Eligible {
		if eligible[h] {
			return false
		}
		eligible[h] = true
	}
	seen := map[pc.HostedAddressHold]bool{}
	for _, v := range p.Physical {
		if seen[v] || eligible[v] == blocked[v] {
			return false
		}
		seen[v] = true
		delete(eligible, v)
	}
	return len(eligible) == 0
}

// Called by the real offer writer, under its producer/full/business locks.
// A legacy process or missing set leaves the offer intact and performs no
// selection. Never shrink the expected set, even for held physical routes.
func (s *Store) pgAdmitHostedOffer(ctx context.Context, tx *sql.Tx, full model.PlatformArtifact, p pc.HostedScopeProposal, in ProjectHostRouteInputs, c HostedSuccessorClaim, program routeartifact.CompiledHostedProgram, holds []pc.HostedAddressHold) error {
	if !hostedHoldsCurrent(p, holds) {
		return nil
	}
	var same bool
	carry := false
	currentHostObserved := false
	var pred hostedSelectionHead
	err := tx.QueryRowContext(ctx, `SELECT fence,decision,phase,(scope_key=$2 AND project_id=$3 AND offer_sequence=$4 AND draft_id=$5 AND draft_digest=$6 AND program_digest=$7)
 FROM fugue_hosted_selections WHERE hostname=$1`, p.Host, c.Scope, c.Project, c.Sequence, c.Draft, c.DraftDigest, program.Digest).Scan(&pred.Fence, &pred.Decision, &pred.Phase, &same)
	switch {
	case errors.Is(err, sql.ErrNoRows): // Only the very first prepare has no predecessor.
	case err != nil:
		return err
	case !same && p.HasMemberBaselines() && (pred.Phase == "forward" || pred.Phase == "prepare"):
		carry = true
		c.Phase = pred.Phase
	case pred.Phase == "forward" || !same && pred.Phase != "withdraw":
		return nil
	default:
		verified, e := pgHostedSelectionStatus(ctx, tx, c)
		if e != nil {
			return e
		}
		if verified.hostedSelectionHead != pred || verified.Required == 0 || verified.Required != verified.Passing || !verified.EvidenceUntil.After(time.Now()) {
			return nil
		}
		if same {
			c.Phase = "forward"
			currentHostObserved = p.HasMemberBaselines()
		}
	}
	// First contact still requires a fresh global capability. A same-phase
	// carry only issues a conditional command to the identical enrolled
	// receivers of a proven predecessor; no observation crosses that boundary.
	// Afterward only new authenticated receipts for *this exact host head*
	// can advance it. Requiring a new global positive for either transition
	// is a deadlock: a process serving scoped hosts cannot attest all hosts.
	var capable func(model.PlatformExpectedConsumer, model.PlatformConsumerAssignment) (bool, error)
	if !carry && !currentHostObserved {
		var anyCapable bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fugue_platform_consumer_instances WHERE scope_key=$1 AND artifact_kind=$2 AND release_set_id=$3 AND identity_verified AND compatibility_capabilities_json::jsonb ? $4)`, pc.GlobalScopeKey, model.PlatformArtifactKindDNSAnswerBundle, full.ID, hostedMemberProtocol).Scan(&anyCapable); err != nil || !anyCapable {
			return err
		}
		capable = func(member model.PlatformExpectedConsumer, a model.PlatformConsumerAssignment) (bool, error) {
			code, _, err := pgHostedMemberBlocker(ctx, tx, p, member, a, time.Now().UTC(), true)
			return code == "" && err == nil, err
		}
	}
	unit, err := routeartifact.OpenHostedPhase(p, c.DraftDigest, program.Digest, program.Wire, c.Phase)
	if err != nil {
		return err
	}
	var receivers hostedDeliverySet
	var collect []hostedDeliveryVisitor
	if carry {
		receivers = hostedDeliverySet{}
		collect = append(collect, receivers.add)
	}
	parts, directory, err := pgHostedCurrentManifest(ctx, tx, full, p, unit, capable, nil, nil, collect...)
	if err != nil || len(parts) == 0 {
		return err
	}
	c.Business, err = hostedBusinessRevision(in)
	if err != nil {
		return err
	}
	auth, err := hostedAuthorityDigest(p)
	if err != nil {
		return err
	}
	paths := map[string]string{}
	for _, r := range p.Prepared.Routes {
		if r.Hostname != p.Host || r.AppID == "" || r.PathPrefix == "" || paths[r.PathPrefix] != "" {
			return ErrConflict
		}
		paths[r.PathPrefix] = r.AppID
	}
	if paths["/"] != p.Before.Owner.AppID || paths["/"] == "" {
		return ErrConflict
	}
	baseline, err := pc.Digest(paths)
	if err != nil || baseline == c.Paths {
		return ErrConflict
	}
	cmd := hostedSelectionCommand{Claim: c, Program: program.Digest, Authority: auth, Parts: parts, ValidUntil: p.Authority.ExpiresAt, Previous: pred.Decision, Fence: pred.Fence, BaselinePaths: baseline}
	if carry {
		return s.pgContinueHostedOffer(ctx, tx, full, p, program, cmd, directory, receivers, unit)
	}
	_, err = pgSelectHostedPhase(ctx, tx, cmd)
	if err != nil {
		return err
	}
	if pred.Fence > 0 {
		return nil
	} // slot membership cannot change across a head CAS
	rows, err := json.Marshal(directory)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, hostedDirectoryInsertSQL, c.Host, string(rows))
	return err
}

// Reconstruct exact parts from the selected image and current physical registrations.
// Only admission requires capability facts; status reads never issue member credentials
// or turn a registration into an observation.
type hostedDeliveryVisitor func(model.PlatformConsumerAssignment, routeartifact.HostedDelivery) error

func pgHostedCurrentManifest(ctx context.Context, tx *sql.Tx, full model.PlatformArtifact, p pc.HostedScopeProposal, unit *routeartifact.HostedPhaseResolver, capable func(model.PlatformExpectedConsumer, model.PlatformConsumerAssignment) (bool, error), visitPart func(string, routeartifact.HostedRecipient), visitMissing func(node, kind string), visitors ...hostedDeliveryVisitor) (map[string]string, []hostedMemberDirectoryEntry, error) {
	type receiver struct{ node, kind, group, component string }
	need := map[receiver]bool{}
	slots := map[routeartifact.HostedRecipient]bool{}
	for _, r := range unit.Required() {
		need[receiver{r.Node, r.Kind, r.Group, r.Component}] = true
		slots[r] = true
	}
	if len(slots) == 0 || len(slots) > 8192 {
		return nil, nil, ErrInvalidInput
	}
	parts := map[string]string{}
	var directory []hostedMemberDirectoryEntry
	missingSet := false
	for _, spec := range []struct{ kind, id, hash string }{
		{model.PlatformArtifactKindEdgeRouteBundle, p.Base.RouteArtifactID, p.Base.RouteArtifactDigest},
		{model.PlatformArtifactKindCaddyRouteConfig, p.Base.TLSArtifactID, p.Base.TLSArtifactDigest},
		{model.PlatformArtifactKindDNSAnswerBundle, p.Base.DNSArtifactID, p.Base.DNSArtifactDigest}} {
		set, e := scanPlatformExpectedConsumerSet(tx.QueryRowContext(ctx, hostedCurrentSetSQL, full.ID, p.Base.ReleaseID, spec.kind, pc.GlobalScopeKey))
		if errors.Is(e, sql.ErrNoRows) || errors.Is(e, ErrNotFound) {
			if visitMissing == nil {
				return nil, nil, nil
			}
			// Inspect the other sets for diagnostics only. This manifest
			// is still incomplete, even if this set has no hosted parts.
			missingSet = true
			continue
		}
		if e != nil {
			return nil, nil, e
		}
		if platformcontrol.ValidateDeclaredTrafficConsumerSet(full, set) != nil {
			return nil, nil, ErrConflict
		}
		a := model.PlatformConsumerAssignment{ScopeKey: pc.GlobalScopeKey, ReleaseChannel: "full", ReleaseSetID: full.ID, ArtifactReleaseID: p.Base.ReleaseID, FencingToken: p.Base.FencingToken, ExpectedConsumerSetID: set.ID, Revision: set.Revision, ExpectedGeneration: set.ExpectedGeneration, ArtifactID: spec.id, ArtifactKind: spec.kind, ContentHash: spec.hash}
		for _, member := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
			key := receiver{member.NodeID, spec.kind, member.Cohort, member.Component}
			if !need[key] {
				continue
			}
			if capable != nil {
				ok, e := capable(member, a)
				if e != nil || !ok {
					return nil, nil, e
				}
			}
			enrollment, e := platformcontrol.ConsumerEnrollmentFromID(member.Component, member.NodeID, member.AuthorityID, member.ScopeKey, member.ConsumerID)
			if e != nil {
				return nil, nil, ErrConflict
			}
			who := platformcontrol.PlatformComponentIdentityClaims{Component: member.Component, NodeID: member.NodeID, AuthorityID: member.AuthorityID, ScopeKey: member.ScopeKey, ArtifactKinds: []string{a.ArtifactKind}, WorkerEnrollmentID: enrollment}
			delivery, _, e := unit.ForMember(who, a, set)
			if e != nil {
				return nil, nil, ErrConflict
			}
			for _, visit := range visitors {
				if e = visit(a, delivery); e != nil {
					return nil, nil, e
				}
			}
			for _, part := range delivery.Parts {
				k, e := delivery.PartKey(part)
				if e != nil || parts[k] != "" || !slots[part.Recipient] {
					return nil, nil, ErrConflict
				}
				delete(slots, part.Recipient)
				parts[k] = part.ContentDigest
				if visitPart != nil {
					visitPart(k, part.Recipient)
				}
			}
			delete(need, key)
			directory = append(directory, hostedMemberDirectoryEntry{delivery.Member, spec.kind, set.ID, spec.id})
		}
	}
	if missingSet || len(need) != 0 || len(slots) != 0 {
		if visitMissing != nil {
			for key := range need {
				visitMissing(key.node, key.kind)
			}
		}
		return nil, nil, nil
	}
	return parts, directory, nil
}
