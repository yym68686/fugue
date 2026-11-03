package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"reflect"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func dependencyReconfiguration(operation string) bool {
	return operation == "placement_constraints" || operation == "dns_source"
}

// A dependency repair authorizes a new producer input, never a positive traffic
// checkpoint. The exact signed baseline survives unchanged, including rollback
// fences that cannot yet be verified because the old dependency was revoked.
func validateDependencyReconfiguration(state *model.State, a model.PlatformArtifact, p platformproducer.Policy, req model.PlatformArtifactReleaseRequest, keys bundleauth.Keyring, now time.Time) error {
	r := req.ProducerReconfiguration
	old, _, err := exactPublication(state, r.PreviousPolicy, a.ArtifactKind, a.ScopeKey, "shadow", keys, false)
	if err != nil {
		return err
	}
	previous, err := platformproducer.Decode(old)
	if err != nil || previous.Mode != "serving" || previous.Serving == nil || previous.Serving.SinglePublication || a.GenerationSequence <= old.GenerationSequence || previous.Generation == p.Generation {
		return fmt.Errorf("%w: dependency predecessor is not a continuous serving policy", ErrConflict)
	}
	before, after := previous, p
	before.Generation, after.Generation = "", ""
	if r.Operation == "placement_constraints" {
		if before.RoutePlacementTransition == nil || after.RoutePlacementTransition == nil ||
			!reflect.DeepEqual(before.RoutePlacementTransition.PreviousTopology, after.RoutePlacementTransition.PreviousTopology) ||
			!reflect.DeepEqual(before.RoutePlacementTransition.NextTopology, after.RoutePlacementTransition.NextTopology) {
			return ErrConflict
		}
		before.RoutePlacementTransition, after.RoutePlacementTransition = nil, nil
	} else {
		if before.DNSSource == nil || after.DNSSource == nil || before.DNSSource.ProducerPolicy == after.DNSSource.ProducerPolicy ||
			!reflect.DeepEqual(before.DNSSource.PreviousTopology, after.DNSSource.PreviousTopology) || !reflect.DeepEqual(before.DNSSource.DNSNodeIDs, after.DNSSource.DNSNodeIDs) {
			return ErrConflict
		}
		if err := validateDNSProducerPolicySelection(stateDNSRouteSourceReader(state), p.DNSSource.ProducerPolicy, keys); err != nil {
			return err
		}
		before.DNSSource, after.DNSSource = nil, nil
	}
	if !reflect.DeepEqual(before, after) {
		return fmt.Errorf("%w: dependency refresh changes unrelated settings", ErrConflict)
	}
	laneKey := platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, "shadow")
	lane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, laneKey)
	if !ok || lane.Frozen {
		return ErrConflict
	}
	if existing, ok := platformReleaseByIdempotencyKey(state.PlatformArtifactReleases, laneKey, req.IdempotencyKey); ok {
		if existing.ArtifactID != a.ID || existing.Status != model.PlatformArtifactReleaseStatusActive || lane.ActiveReleaseID != existing.ID || lane.FencingToken != existing.FencingToken {
			return ErrConflict
		}
	} else if _, _, err := exactPublication(state, r.PreviousPolicy, a.ArtifactKind, a.ScopeKey, "shadow", keys, true); err != nil {
		return err
	}
	for _, channel := range []string{"gray", "full"} {
		if other, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, channel)); ok && (other.Frozen || other.ActiveReleaseID != "") {
			return ErrConflict
		}
	}
	full, release, err := exactPublication(state, r.ServingFull, model.PlatformArtifactKindReleaseSet, p.TargetScope, "full", keys, true)
	if err != nil {
		return fmt.Errorf("%w: dependency full selection does not match its signed precondition", err)
	}
	if full.Content["publication_role"] != p.PublicationRole || full.Metadata[platformproducer.PolicyReleaseMetadata] != r.PreviousPolicy.ReleaseID || !producerOwnsPublication(release) {
		return fmt.Errorf("%w: dependency baseline is not the exact predecessor-owned full", ErrConflict)
	}
	if _, err := platformconfig.ValidateReleaseComposition(full); err != nil {
		return fmt.Errorf("%w: dependency baseline composition: %s", ErrConflict, err)
	}
	if _, err := platformconfig.ResolveTrafficCanary(full, p.Serving.CanaryRuleRef); err != nil {
		return fmt.Errorf("%w: dependency baseline cohort: %s", ErrConflict, err)
	}
	lkg := verifiedPlatformLKGSnapshotFromState(state, full.ArtifactKind, p.TargetScope, now, keys)
	if lkg == nil || lkg.ArtifactID != full.ID || lkg.ContentHash != full.ContentHash || lkg.VerificationEvidenceHash != r.VerificationEvidenceHash ||
		(lkg.VerifiedByReleaseID != release.ID || release.VerificationState != model.PlatformArtifactVerificationStateVerified || release.VerifiedLKGGeneration != full.Generation) && !availabilityVerifiedRollbackBaseline(state, full, release, keys, now) {
		return fmt.Errorf("%w: dependency baseline lacks exact positive LKG or authenticated rollback", ErrConflict)
	}
	if grayLane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, platformsafety.ReleaseLaneKey(full.ArtifactKind, p.TargetScope, "gray")); ok {
		if grayLane.Frozen {
			return ErrConflict
		}
		if grayLane.ActiveReleaseID != "" {
			i := platformArtifactReleaseIndex(state.PlatformArtifactReleases, grayLane.ActiveReleaseID)
			if i < 0 || state.PlatformArtifactReleases[i].ReleasedAt.After(release.ReleasedAt) && state.PlatformArtifactReleases[i].VerificationState != model.PlatformArtifactVerificationStateFailed {
				return ErrConflict
			}
		}
	}
	return nil
}

// Cross-authority locks never wait while holding a target authority lock.
func pgTrySharedPromotionScope(ctx context.Context, tx *sql.Tx, scope string) error {
	sum := sha256.Sum256([]byte("fugue.platform.release-set-promotion/v1\x00" + scope))
	var locked bool
	if err := tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock_shared($1)", int64(binary.BigEndian.Uint64(sum[:8]))).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return ErrConflict
	}
	return nil
}

func pgLoadDependencyReconfiguration(ctx context.Context, tx *sql.Tx, state *model.State, p platformproducer.Policy) error {
	key := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindReleaseSet, p.TargetScope, "gray")
	lane, err := pgGetPlatformReleaseLaneForUpdate(ctx, tx, key)
	if err != nil && err != ErrNotFound {
		return err
	}
	if err == nil {
		state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, lane)
		if lane.ActiveReleaseID != "" {
			r, err := pgGetPlatformArtifactRelease(ctx, tx, lane.ActiveReleaseID, false)
			if err != nil {
				return err
			}
			state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
		}
	}
	if p.DNSSource == nil {
		return nil
	}
	if err := pgTrySharedPromotionScope(ctx, tx, platformproducer.Scope); err != nil {
		return err
	}
	read := pgDNSRouteSourceReader(ctx, tx)
	lane, err = read.lane(platformsafety.ReleaseLaneKey(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow"))
	if err != nil {
		return err
	}
	a, err := read.artifact(p.DNSSource.ProducerPolicy.ArtifactID)
	if err != nil {
		return err
	}
	r, err := read.release(p.DNSSource.ProducerPolicy.ReleaseID)
	if err != nil {
		return err
	}
	state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, lane)
	state.PlatformArtifacts = append(state.PlatformArtifacts, a)
	state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
	return nil
}
