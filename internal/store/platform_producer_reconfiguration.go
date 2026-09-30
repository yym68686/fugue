package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func producerReconfigurationKey(a model.PlatformArtifact, precondition model.PlatformProducerReconfiguration) (string, error) {
	raw, err := json.Marshal(precondition)
	if err != nil {
		return "", err
	}
	var object map[string]any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if err := d.Decode(&object); err != nil {
		return "", err
	}
	digest, err := platformconfig.Digest(map[string]any{"artifact_id": a.ID, "content_hash": a.ContentHash, "precondition": object})
	return "producer-reconfiguration/" + digest, err
}

func validateProducerReconfigurationRequest(a model.PlatformArtifact, req model.PlatformArtifactReleaseRequest) (platformproducer.Policy, error) {
	p, err := platformproducer.Decode(a)
	if err != nil || req.ProducerReconfiguration == nil || req.ReleaseChannel != "shadow" || req.CanaryRuleRef != "" || req.SoftOverride || req.ForcePublish || req.KernelBreakGlass != nil || p.PublicationRole != platformconfig.PublicationRoleCellRoutes || p.RoutePlacementTransition == nil {
		return p, ErrInvalidInput
	}
	r := req.ProducerReconfiguration
	switch r.Operation {
	case "", "placement":
		if p.Mode != "shadow" {
			return p, ErrInvalidInput
		}
	case "activate_serving":
		if p.Mode != "serving" || p.Serving == nil || !p.Serving.SinglePublication {
			return p, ErrInvalidInput
		}
	default:
		return p, ErrInvalidInput
	}
	for _, ref := range []model.PlatformPublicationPrecondition{r.PreviousPolicy, r.ServingFull} {
		if ref.ArtifactID == "" || len(ref.ArtifactID) > 256 || ref.ArtifactID != strings.TrimSpace(ref.ArtifactID) || ref.ReleaseID == "" || len(ref.ReleaseID) > 256 || ref.ReleaseID != strings.TrimSpace(ref.ReleaseID) || ref.FencingToken <= 0 || !platformproducer.ValidDigest(ref.ContentHash) {
			return p, ErrInvalidInput
		}
	}
	key, err := producerReconfigurationKey(a, *r)
	if err != nil || req.IdempotencyKey != key || !platformproducer.ValidDigest(r.VerificationEvidenceHash) || a.ID == r.PreviousPolicy.ArtifactID {
		return p, ErrInvalidInput
	}
	return p, nil
}

func exactPublication(state *model.State, ref model.PlatformPublicationPrecondition, kind, scope, channel string, keys bundleauth.Keyring, current bool) (model.PlatformArtifact, model.PlatformArtifactRelease, error) {
	ai, ri := platformArtifactIndex(state.PlatformArtifacts, ref.ArtifactID), platformArtifactReleaseIndex(state.PlatformArtifactReleases, ref.ReleaseID)
	if ai < 0 || ri < 0 {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, ErrConflict
	}
	a, r := state.PlatformArtifacts[ai], state.PlatformArtifactReleases[ri]
	laneKey := platformsafety.ReleaseLaneKey(kind, scope, channel)
	if a.ID != ref.ArtifactID || a.ContentHash != ref.ContentHash || a.ArtifactKind != kind || a.ScopeKey != scope || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass || r.ID != ref.ReleaseID || r.ArtifactID != a.ID || r.ArtifactKind != kind || r.ScopeKey != scope || r.ReleaseChannel != channel || r.LaneKey != laneKey || r.FencingToken != ref.FencingToken || r.Generation != a.Generation {
		return a, r, ErrConflict
	}
	if current {
		lane, exists := platformReleaseLaneByKey(state.PlatformReleaseLanes, laneKey)
		if !exists || lane.Frozen || lane.ActiveReleaseID != r.ID || lane.FencingToken != r.FencingToken || r.Status != model.PlatformArtifactReleaseStatusActive {
			return a, r, ErrConflict
		}
	}
	return a, r, nil
}

func validateProducerReconfiguration(state *model.State, a model.PlatformArtifact, req model.PlatformArtifactReleaseRequest, keys bundleauth.Keyring, now time.Time) error {
	if req.ProducerReconfiguration == nil {
		return nil
	}
	p, err := validateProducerReconfigurationRequest(a, req)
	if err != nil {
		return err
	}
	if a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
		return ErrConflict
	}
	r := req.ProducerReconfiguration
	laneKey := platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, "shadow")
	lane, exists := platformReleaseLaneByKey(state.PlatformReleaseLanes, laneKey)
	if !exists || lane.Frozen {
		return ErrConflict
	}
	old, _, err := exactPublication(state, r.PreviousPolicy, a.ArtifactKind, a.ScopeKey, "shadow", keys, false)
	if err != nil {
		return err
	}
	previous, err := platformproducer.Decode(old)
	if err != nil || (previous.Mode != "shadow" && previous.Mode != "paused") || p.Generation == previous.Generation || a.GenerationSequence <= old.GenerationSequence {
		return ErrConflict
	}
	// Each explicit operation has a disjoint change boundary. In particular,
	// activation cannot also alter the already reviewed placement transition.
	previous.Generation, p.Generation = "", ""
	if r.Operation == "activate_serving" {
		previous.Serving, p.Serving = nil, nil
	} else {
		previous.RoutePlacementTransition, p.RoutePlacementTransition = nil, nil
	}
	previous.Mode = p.Mode
	if !reflect.DeepEqual(previous, p) {
		return ErrConflict
	}
	for _, channel := range []string{"gray", "full"} {
		other, found := platformReleaseLaneByKey(state.PlatformReleaseLanes, platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, channel))
		if found && (other.ActiveReleaseID != "" || other.Frozen) {
			return ErrConflict
		}
	}
	if existing, found := platformReleaseByIdempotencyKey(state.PlatformArtifactReleases, laneKey, req.IdempotencyKey); found {
		if existing.ArtifactID != a.ID || existing.ReleaseChannel != "shadow" || existing.Status != model.PlatformArtifactReleaseStatusActive || existing.FencingToken != lane.FencingToken || existing.ID != lane.ActiveReleaseID {
			return ErrConflict
		}
	} else if _, _, err := exactPublication(state, r.PreviousPolicy, a.ArtifactKind, a.ScopeKey, "shadow", keys, true); err != nil {
		return err
	}
	full, release, err := exactPublication(state, r.ServingFull, model.PlatformArtifactKindReleaseSet, p.TargetScope, "full", keys, true)
	if err != nil || full.Content["publication_role"] != platformconfig.PublicationRoleCellRoutes {
		return ErrConflict
	}
	if _, err := platformconfig.ValidateReleaseComposition(full); err != nil {
		return ErrConflict
	}
	if r.Operation == "activate_serving" {
		// Resolve the cohort against the exact current baseline before enabling
		// automatic publication; fresh candidate admission remains independent.
		target, err := platformproducer.Decode(a)
		if err != nil {
			return ErrConflict
		}
		if _, err := platformconfig.ResolveTrafficCanary(full, target.Serving.CanaryRuleRef); err != nil {
			return ErrConflict
		}
		if release.VerificationState != model.PlatformArtifactVerificationStateVerified || release.VerifiedLKGGeneration != full.Generation {
			return ErrConflict
		}
	}
	lkg := verifiedPlatformLKGSnapshotFromState(state, model.PlatformArtifactKindReleaseSet, p.TargetScope, now, keys)
	if lkg == nil || lkg.ArtifactID != full.ID || lkg.ContentHash != full.ContentHash || lkg.VerifiedByReleaseID != release.ID || lkg.VerificationEvidenceHash != r.VerificationEvidenceHash {
		return ErrConflict
	}
	return nil
}

// Take both scope locks before any mutable reads. This follows the same
// producer-before-target order as automatic publication and LKG verification.
func pgLockProducerReconfiguration(ctx context.Context, tx *sql.Tx, id string, req model.PlatformArtifactReleaseRequest) error {
	if req.ProducerReconfiguration == nil {
		return nil
	}
	a, err := pgGetPlatformArtifactForUpdate(ctx, tx, id, false)
	if err != nil {
		return err
	}
	if a.ID != id {
		return ErrInvalidInput
	}
	p, err := validateProducerReconfigurationRequest(a, req)
	if err != nil {
		return err
	}
	if err := pgLockPromotionScope(ctx, tx, a.ScopeKey, true); err != nil {
		return err
	}
	return pgLockPromotionScope(ctx, tx, p.TargetScope, true)
}

func (s *Store) pgProducerReconfiguration(ctx context.Context, tx *sql.Tx, a model.PlatformArtifact, req model.PlatformArtifactReleaseRequest) error {
	if req.ProducerReconfiguration == nil {
		return nil
	}
	p, err := validateProducerReconfigurationRequest(a, req)
	if err != nil {
		return err
	}
	state := &model.State{PlatformArtifacts: []model.PlatformArtifact{a}}
	r := req.ProducerReconfiguration
	for _, ref := range []model.PlatformPublicationPrecondition{r.PreviousPolicy, r.ServingFull} {
		artifact, err := pgGetPlatformArtifactForUpdate(ctx, tx, ref.ArtifactID, true)
		if err != nil {
			return err
		}
		release, err := pgGetPlatformArtifactRelease(ctx, tx, ref.ReleaseID, false)
		if err != nil {
			return err
		}
		state.PlatformArtifacts = append(state.PlatformArtifacts, artifact)
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, release)
	}
	for _, key := range []string{
		platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, "shadow"),
		platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, "gray"),
		platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, "full"),
		platformsafety.ReleaseLaneKey(model.PlatformArtifactKindReleaseSet, p.TargetScope, "full"),
	} {
		lane, err := pgGetPlatformReleaseLaneForUpdate(ctx, tx, key)
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return err
		}
		state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, lane)
	}
	if existing, found, err := pgGetPlatformArtifactReleaseByIdempotency(ctx, tx, platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, "shadow"), req.IdempotencyKey); err != nil {
		return err
	} else if found {
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, existing)
	}
	lkg, err := pgGetPlatformLKGForUpdate(ctx, tx, model.PlatformArtifactKindReleaseSet, p.TargetScope)
	if err != nil {
		return err
	}
	if lkg != nil {
		state.PlatformLKGSnapshots = append(state.PlatformLKGSnapshots, *lkg)
	}
	return validateProducerReconfiguration(state, a, req, s.platformArtifactSigningKeyring(), time.Now().UTC())
}
