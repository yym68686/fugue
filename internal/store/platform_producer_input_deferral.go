package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/schemamigrate"
)

// ProducerInputDeferred is a read-only admission decision for ONE new attempt.
// Its transaction serializes with selecting a signed finite control, not with
// the lifetime of previous work. It installs no schema, participant, ticket or
// migration, and is never a drain/retirement proof. No selection means OFF.
func (s *Store) ProducerInputDeferred(ctx context.Context, scope string, policy model.PlatformPublicationPrecondition) (deferred bool, err error) {
	if !platformproducer.IsPolicyScope(scope) || policy.ArtifactID == "" || policy.ReleaseID == "" || policy.FencingToken <= 0 || !platformproducer.ValidDigest(policy.ContentHash) {
		return false, ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !s.usingDatabase() {
		err = s.withLockedState(false, func(state *model.State) error {
			selected, err := s.inputDeferralFromState(state)
			if err != nil || selected == nil {
				return err
			}
			deferred, err = selected.Defers(time.Now().UTC(), scope, policy)
			return err
		})
		return deferred, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if err := setCompilerInputAdmissionTimeouts(ctx, tx); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `SELECT set_config('work_mem','1MB',true)`); err != nil {
		return false, err
	}
	if err := schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return false, err
	}
	if err := pgLockPromotionScope(ctx, tx, platformproducer.InputDeferralScope, false); err != nil {
		return false, err
	}
	selected, err := s.pgInputDeferralSelection(ctx, tx)
	if err != nil {
		return false, err
	}
	if selected != nil {
		at, err := captureDeferralNow(ctx, tx)
		if err != nil {
			return false, err
		}
		deferred, err = selected.Defers(at, scope, policy)
		if err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err // an unknown read/commit outcome never admits capture
	}
	return deferred, nil
}

func (s *Store) inputDeferralFromState(state *model.State) (*platformproducer.InputDeferral, error) {
	key := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindPolicySnapshot, platformproducer.InputDeferralScope, "shadow")
	lane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, key)
	if !ok {
		return nil, nil
	}
	if lane.Frozen {
		return nil, ErrConflict
	}
	if lane.ActiveReleaseID == "" {
		return nil, nil
	}
	i := platformArtifactReleaseIndex(state.PlatformArtifactReleases, lane.ActiveReleaseID)
	if i < 0 {
		return nil, ErrConflict
	}
	r := state.PlatformArtifactReleases[i]
	i = platformArtifactIndex(state.PlatformArtifacts, r.ArtifactID)
	if i < 0 {
		return nil, ErrConflict
	}
	a := state.PlatformArtifacts[i]
	if r.Status != "active" || r.LaneKey != key || r.FencingToken != lane.FencingToken || r.FencingToken <= 0 || r.Generation != a.Generation {
		return nil, ErrConflict
	}
	return s.decodeSignedInputDeferral(a)
}

func (s *Store) decodeSignedInputDeferral(a model.PlatformArtifact) (*platformproducer.InputDeferral, error) {
	p, err := platformproducer.DecodeInputDeferral(a)
	if err != nil || a.Status != model.PlatformArtifactStatusValidated || s.VerifyPlatformArtifactIntegrity(a) != nil {
		return nil, ErrConflict
	}
	return &p, nil
}

// Exact indexed lookups only. Bounds are applied before JSON is transferred or
// decoded; no input bodies, archive objects or historical graph are read here.
// JSONB text adds whitespace; transport allows twice the canonical content
// limit, which DecodeInputDeferral independently enforces after decoding.
func (s *Store) pgInputDeferralSelection(ctx context.Context, tx *sql.Tx) (*platformproducer.InputDeferral, error) {
	var id string
	var frozen bool
	key := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindPolicySnapshot, platformproducer.InputDeferralScope, "shadow")
	err := tx.QueryRowContext(ctx, `SELECT active_release_id,frozen FROM fugue_platform_release_lanes WHERE lane_key=$1`, key).Scan(&id, &frozen)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil || frozen {
		return nil, errors.Join(err, ErrConflict)
	}
	if id == "" {
		return nil, nil
	}
	a, err := scanPlatformArtifact(tx.QueryRowContext(ctx, `
SELECT a.id,a.artifact_kind,a.scope_key,a.scope_json,a.schema_version,a.generation,a.generation_sequence,a.status,a.content_hash,
 fugue_platform_artifact_body(a.content_hash,a.content_json),a.validation_results_json,a.compatibility_floor,a.metadata_json,a.created_by_type,a.created_by_id,a.provenance_json,a.created_at,a.updated_at
FROM fugue_platform_artifact_releases r JOIN fugue_platform_release_lanes l ON l.active_release_id=r.id
JOIN fugue_platform_artifacts a ON a.id=r.artifact_id
WHERE r.id=$1 AND l.lane_key=$2 AND NOT l.frozen AND r.status='active'
 AND r.lane_key=l.lane_key AND r.fencing_token=l.fencing_token AND r.fencing_token>0
 AND r.artifact_kind=$3 AND r.scope_key=$4 AND r.release_channel='shadow' AND r.generation=a.generation
 AND octet_length(fugue_platform_artifact_body(a.content_hash,a.content_json)::text)<=$5
 AND octet_length(a.metadata_json::text)<=65536 AND octet_length(a.provenance_json::text)<=65536
 AND octet_length(a.validation_results_json::text)<=65536`, id, key, model.PlatformArtifactKindPolicySnapshot, platformproducer.InputDeferralScope, 2*platformproducer.MaxInputDeferralBytes))
	if err != nil {
		return nil, err
	}
	return s.decodeSignedInputDeferral(a)
}

func inputDeferralPublicationRequest(a model.PlatformArtifact, req model.PlatformArtifactReleaseRequest, principal model.Principal) (platformproducer.InputDeferral, error) {
	p, err := platformproducer.DecodeInputDeferral(a)
	if err != nil || principal.ActorID == "" || principal.ActorType == "" || !principal.IsPlatformAdmin() || !principal.HasExplicitScope("producer_capture.control") ||
		NormalizePlatformReleaseChannel(req.ReleaseChannel) != "shadow" || req.IdempotencyKey != p.IdempotencyKey() ||
		req.SoftOverride || req.ForcePublish || req.KernelBreakGlass != nil || req.ProducerReconfiguration != nil || req.DNSPublicationPrecondition != nil {
		return p, fmt.Errorf("%w: finite input control requires explicit admin control and exact shadow intent", ErrConflict)
	}
	return p, nil
}

func (s *Store) validateInputDeferralPublication(state *model.State, a model.PlatformArtifact, req model.PlatformArtifactReleaseRequest, principal model.Principal, now time.Time) error {
	if a.ScopeKey != platformproducer.InputDeferralScope {
		return nil
	}
	p, err := inputDeferralPublicationRequest(a, req, principal)
	if err != nil {
		return err
	}
	previous, err := s.inputDeferralFromState(state)
	if err != nil {
		return err
	}
	if err := platformproducer.ValidateInputDeferralTransition(p, previous, now); err != nil {
		return err
	}
	if p.Action == "release" {
		return nil // release removes only the exact restriction, even after policy drift
	}
	for _, b := range p.Request.Authorities {
		key := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindPolicySnapshot, b.Scope, "shadow")
		lane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, key)
		i := platformArtifactReleaseIndex(state.PlatformArtifactReleases, b.Policy.ReleaseID)
		j := platformArtifactIndex(state.PlatformArtifacts, b.Policy.ArtifactID)
		if !ok || lane.Frozen || lane.ActiveReleaseID != b.Policy.ReleaseID || lane.FencingToken != b.Policy.FencingToken || i < 0 || j < 0 {
			return ErrConflict
		}
		r, a := state.PlatformArtifactReleases[i], state.PlatformArtifacts[j]
		if r.ArtifactID != a.ID || r.Status != "active" || r.LaneKey != key || r.FencingToken != b.Policy.FencingToken || r.Generation != a.Generation || a.ContentHash != b.Policy.ContentHash || a.ScopeKey != b.Scope || a.Status != model.PlatformArtifactStatusValidated || s.VerifyPlatformArtifactIntegrity(a) != nil {
			return ErrConflict
		}
		if _, err := platformproducer.Decode(a); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) pgValidateInputDeferralPublication(ctx context.Context, tx *sql.Tx, a model.PlatformArtifact, req model.PlatformArtifactReleaseRequest, principal model.Principal) error {
	if a.ScopeKey != platformproducer.InputDeferralScope {
		return nil
	}
	p, err := inputDeferralPublicationRequest(a, req, principal)
	if err != nil {
		return err
	}
	previous, err := s.pgInputDeferralSelection(ctx, tx)
	if err != nil {
		return err
	}
	now, err := captureDeferralNow(ctx, tx)
	if err != nil {
		return err
	}
	if err := platformproducer.ValidateInputDeferralTransition(p, previous, now); err != nil {
		return err
	}
	if p.Action == "release" {
		return nil
	}
	// Lock the original authorities in canonical order. No original policy is
	// replaced and no quality cache, ownership or pending release is rebound.
	bindings := slices.Clone(p.Request.Authorities)
	slices.SortFunc(bindings, func(a, b platformproducer.CaptureDeferralAuthority) int { return strings.Compare(a.Scope, b.Scope) })
	for _, b := range bindings {
		if err := pgLockPromotionScope(ctx, tx, b.Scope, false); err != nil {
			return err
		}
		var binding model.PlatformPublicationPrecondition
		key := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindPolicySnapshot, b.Scope, "shadow")
		err := tx.QueryRowContext(ctx, `SELECT a.id,a.content_hash,r.id,r.fencing_token
FROM fugue_platform_release_lanes l JOIN fugue_platform_artifact_releases r ON r.id=l.active_release_id
JOIN fugue_platform_artifacts a ON a.id=r.artifact_id
WHERE l.lane_key=$1 AND NOT l.frozen AND r.lane_key=l.lane_key AND r.status='active'
 AND r.fencing_token=l.fencing_token AND r.artifact_kind='policy_snapshot' AND r.release_channel='shadow'
 AND r.generation=a.generation AND a.scope_key=r.scope_key AND a.status='validated'
 FOR SHARE OF l,r,a`, key).Scan(&binding.ArtifactID, &binding.ContentHash, &binding.ReleaseID, &binding.FencingToken)
		if err != nil || binding != b.Policy {
			return errors.Join(err, ErrConflict)
		}
		// Producer policy size was already bounded and signed on publication.
		// Verify the current immutable artifact again, never only caller hashes.
		var size int
		if err := tx.QueryRowContext(ctx, `SELECT octet_length(fugue_platform_artifact_body(content_hash,content_json)::text) FROM fugue_platform_artifacts WHERE id=$1`, binding.ArtifactID).Scan(&size); err != nil || size > 1048576 {
			return errors.Join(err, ErrConflict)
		}
		policy, err := pgGetPlatformArtifactForUpdate(ctx, tx, binding.ArtifactID, false)
		if err != nil || s.VerifyPlatformArtifactIntegrity(policy) != nil {
			return errors.Join(err, ErrConflict)
		}
		if _, err := platformproducer.Decode(policy); err != nil {
			return err
		}
	}
	return nil
}
