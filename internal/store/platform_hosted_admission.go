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

const hostedMemberFactSQL = `SELECT id,consumer_id,credential_id,token_id,component,node_id,artifact_kind,scope_key,release_set_id,expected_consumer_set_id,fencing_token,supported_kinds_json,protocol_version,schema_version,compatibility_capabilities_json,sequence,issued_at,nonce,generation_sequence,evidence_hash,identity_verified,desired_generation,actual_generation,candidate_generation,lkg_generation,apply_status,probe_status,serving_lkg,lkg_expired,last_error,last_heartbeat_at,updated_at
 FROM fugue_platform_consumer_instances WHERE consumer_id=$1 AND artifact_kind=$2 AND scope_key=$3 FOR SHARE`

// Do not add this capability to a generic client or to old global receipts.
// Only a process that can persist, serve, observe and withdraw a scoped
// selection for this kind may advertise it. The fact does not count as an ACK.
const hostedMemberProtocol = routeartifact.HostedPacketKind

func hostedMemberCapable(member model.PlatformExpectedConsumer, a model.PlatformConsumerAssignment, f model.PlatformConsumerInstance, now time.Time) bool {
	return trafficCapabilityFactFresh(member, f, now) && f.ConsumerID == member.ConsumerID && f.ArtifactKind == a.ArtifactKind && f.ScopeKey == a.ScopeKey &&
		f.ReleaseSetID == a.ReleaseSetID && f.ExpectedConsumerSetID == a.ExpectedConsumerSetID && f.FencingToken == a.FencingToken &&
		f.DesiredGeneration == a.ExpectedGeneration && f.ActualGeneration == a.ExpectedGeneration && f.ApplyStatus == "applied" && f.ProbeStatus == "passed" && !f.LKGExpired &&
		slices.Contains(f.SupportedKinds, a.ArtifactKind) && slices.Contains(f.CompatibilityCapabilities, hostedMemberProtocol)
}

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
func pgAdmitHostedOffer(ctx context.Context, tx *sql.Tx, full model.PlatformArtifact, p pc.HostedScopeProposal, in ProjectHostRouteInputs, c HostedSuccessorClaim, program routeartifact.CompiledHostedProgram, holds []pc.HostedAddressHold) error {
	if !hostedHoldsCurrent(p, holds) {
		return nil
	}
	var same bool
	var pred hostedSelectionHead
	err := tx.QueryRowContext(ctx, `SELECT fence,decision,phase,(scope_key=$2 AND project_id=$3 AND offer_sequence=$4 AND draft_id=$5 AND draft_digest=$6 AND program_digest=$7)
 FROM fugue_hosted_selections WHERE hostname=$1`, p.Host, c.Scope, c.Project, c.Sequence, c.Draft, c.DraftDigest, program.Digest).Scan(&pred.Fence, &pred.Decision, &pred.Phase, &same)
	switch {
	case errors.Is(err, sql.ErrNoRows): // Only the very first prepare has no predecessor.
	case err != nil:
		return err
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
		}
	}
	// Old deployments require no image decode or scanning of global history.
	// The indexed, authoritative per-member reads below independently recheck.
	var anyCapable bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fugue_platform_consumer_instances WHERE scope_key=$1 AND artifact_kind=$2 AND release_set_id=$3 AND identity_verified AND compatibility_capabilities_json::jsonb ? $4)`, pc.GlobalScopeKey, model.PlatformArtifactKindDNSAnswerBundle, full.ID, hostedMemberProtocol).Scan(&anyCapable); err != nil || !anyCapable {
		return err
	}
	unit, err := routeartifact.OpenHostedPhase(p, c.DraftDigest, program.Digest, program.Wire, c.Phase)
	if err != nil {
		return err
	}
	type receiver struct{ node, kind, group, component string }
	need := map[receiver]bool{}
	slots := map[routeartifact.HostedRecipient]bool{}
	for _, r := range unit.Required() {
		need[receiver{r.Node, r.Kind, r.Group, r.Component}] = true
		slots[r] = true
	}
	if len(slots) == 0 || len(slots) > 8192 {
		return ErrInvalidInput
	}
	parts := map[string]string{}
	var directory []hostedMemberDirectoryEntry
	for _, spec := range []struct{ kind, id, hash string }{
		{model.PlatformArtifactKindEdgeRouteBundle, p.Base.RouteArtifactID, p.Base.RouteArtifactDigest},
		{model.PlatformArtifactKindCaddyRouteConfig, p.Base.TLSArtifactID, p.Base.TLSArtifactDigest},
		{model.PlatformArtifactKindDNSAnswerBundle, p.Base.DNSArtifactID, p.Base.DNSArtifactDigest}} {
		set, e := scanPlatformExpectedConsumerSet(tx.QueryRowContext(ctx, hostedCurrentSetSQL, full.ID, p.Base.ReleaseID, spec.kind, pc.GlobalScopeKey))
		if errors.Is(e, sql.ErrNoRows) || errors.Is(e, ErrNotFound) {
			return nil
		}
		if e != nil {
			return e
		}
		if platformcontrol.ValidateDeclaredTrafficConsumerSet(full, set) != nil {
			return ErrConflict
		}
		a := model.PlatformConsumerAssignment{ScopeKey: pc.GlobalScopeKey, ReleaseChannel: "full", ReleaseSetID: full.ID, ArtifactReleaseID: p.Base.ReleaseID, FencingToken: p.Base.FencingToken, ExpectedConsumerSetID: set.ID, Revision: set.Revision, ExpectedGeneration: set.ExpectedGeneration, ArtifactID: spec.id, ArtifactKind: spec.kind, ContentHash: spec.hash}
		for _, member := range platformcontrol.ProjectExpectedConsumerOwners(set).Consumers {
			key := receiver{member.NodeID, spec.kind, member.Cohort, member.Component}
			if !need[key] {
				continue
			}
			f, e := scanPlatformConsumerInstance(tx.QueryRowContext(ctx, hostedMemberFactSQL, member.ConsumerID, spec.kind, pc.GlobalScopeKey))
			if errors.Is(e, sql.ErrNoRows) || errors.Is(e, ErrNotFound) {
				return nil
			}
			if e != nil {
				return e
			}
			if !hostedMemberCapable(member, a, f, time.Now().UTC()) {
				return nil
			}
			enrollment, e := platformcontrol.ConsumerEnrollmentFromID(member.Component, member.NodeID, member.AuthorityID, member.ScopeKey, member.ConsumerID)
			if e != nil {
				return ErrConflict
			}
			who := platformcontrol.PlatformComponentIdentityClaims{Component: member.Component, NodeID: member.NodeID, AuthorityID: member.AuthorityID, ScopeKey: member.ScopeKey, ArtifactKinds: f.SupportedKinds, WorkerEnrollmentID: enrollment}
			delivery, _, e := unit.ForMember(who, a, set)
			if e != nil {
				return ErrConflict
			}
			for _, part := range delivery.Parts {
				k, e := delivery.PartKey(part)
				if e != nil || parts[k] != "" || !slots[part.Recipient] {
					return ErrConflict
				}
				delete(slots, part.Recipient)
				parts[k] = part.ContentDigest
			}
			delete(need, key)
			directory = append(directory, hostedMemberDirectoryEntry{delivery.Member, spec.kind, set.ID, spec.id})
		}
	}
	if len(need) != 0 || len(slots) != 0 {
		return nil
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
	_, err = pgSelectHostedPhase(ctx, tx, hostedSelectionCommand{Claim: c, Program: program.Digest, Authority: auth, Parts: parts, ValidUntil: p.Authority.ExpiresAt, Previous: pred.Decision, Fence: pred.Fence, BaselinePaths: baseline})
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
