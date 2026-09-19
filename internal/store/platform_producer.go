package store

import (
	"context"
	"database/sql"
	"fmt"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

type platformProducerReleaseGuard struct {
	PolicyReleaseID   string
	PreviousReleaseID string
}

func (s *Store) ReleaseProducedPlatformArtifact(id, policyReleaseID, previousReleaseID string, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	return s.releasePlatformArtifact(id, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", Reason: "versioned platform configuration producer", IdempotencyKey: "producer/" + policyReleaseID + "/" + id}, principal, &platformProducerReleaseGuard{PolicyReleaseID: policyReleaseID, PreviousReleaseID: previousReleaseID})
}

func validateProducerPolicyPublication(a model.PlatformArtifact, channel string) error {
	if a.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || a.ScopeKey != platformproducer.Scope {
		return nil
	}
	if _, err := platformproducer.Decode(a); err != nil || channel != model.PlatformArtifactReleaseChannelShadow {
		return fmt.Errorf("%w: producer policy requires valid shadow publication", ErrConflict)
	}
	return nil
}

func validateProducerReleaseGuard(state *model.State, parent model.PlatformArtifact, req model.PlatformArtifactReleaseRequest, principal model.Principal, keys bundleauth.Keyring, guard *platformProducerReleaseGuard) error {
	if guard == nil {
		return nil
	}
	if req.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow || parent.ArtifactKind != model.PlatformArtifactKindReleaseSet || parent.ScopeKey != "global" || principal.ActorType != model.ActorTypeBootstrap || principal.ActorID != platformproducer.Actor || guard.PolicyReleaseID == "" || parent.Metadata[platformproducer.PolicyReleaseMetadata] != guard.PolicyReleaseID || parent.Metadata[platformproducer.SourceDigestMetadata] == "" || parent.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(parent, keys).Pass {
		return ErrConflict
	}
	policyLaneKey := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, model.PlatformArtifactReleaseChannelShadow)
	policyLane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, policyLaneKey)
	if !ok || policyLane.Frozen || policyLane.ActiveReleaseID != guard.PolicyReleaseID {
		return ErrConflict
	}
	var policyRelease model.PlatformArtifactRelease
	for _, release := range state.PlatformArtifactReleases {
		if release.ID == guard.PolicyReleaseID {
			policyRelease = release
			break
		}
	}
	index := platformArtifactIndex(state.PlatformArtifacts, policyRelease.ArtifactID)
	if index < 0 || policyRelease.Status != model.PlatformArtifactReleaseStatusActive || policyRelease.LaneKey != policyLaneKey || policyRelease.FencingToken != policyLane.FencingToken || policyRelease.FencingToken <= 0 || policyRelease.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || policyRelease.ScopeKey != platformproducer.Scope || policyRelease.ReleaseChannel != "shadow" {
		return ErrConflict
	}
	artifact := state.PlatformArtifacts[index]
	policy, err := platformproducer.Decode(artifact)
	if err != nil || policy.Mode != "shadow" || policyRelease.Generation != artifact.Generation || artifact.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(artifact, keys).Pass {
		return ErrConflict
	}
	if policy.InputSource == "business-static-intent" {
		index := platformArtifactIndex(state.PlatformArtifacts, policy.StaticIntentArtifactID)
		if index < 0 {
			return ErrConflict
		}
		base := state.PlatformArtifacts[index]
		if base.ID != policy.StaticIntentArtifactID || base.ContentHash != policy.StaticIntentDigest || base.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(base, keys).Pass || parent.Metadata[platformproducer.StaticIntentIDMetadata] != base.ID || parent.Metadata[platformproducer.StaticIntentDigestMetadata] != base.ContentHash {
			return ErrConflict
		}
		static, err := platformproducer.DecodeStaticIntent(base)
		if err != nil {
			return ErrConflict
		}
		if policy.RequireApplicationDomains && static.ApplicationDomains == nil {
			return ErrConflict
		}
		if policy.DNSPolicyArtifactID != "" {
			index := platformArtifactIndex(state.PlatformArtifacts, policy.DNSPolicyArtifactID)
			if index < 0 {
				return ErrConflict
			}
			a := state.PlatformArtifacts[index]
			if a.ID != policy.DNSPolicyArtifactID || a.ContentHash != policy.DNSPolicyDigest || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass || parent.Metadata[platformproducer.DNSPolicyIDMetadata] != a.ID || parent.Metadata[platformproducer.DNSPolicyDigestMetadata] != a.ContentHash {
				return ErrConflict
			}
			if _, err := platformproducer.DecodeDNSInputs(a, static.Consumers, policy.HostedZoneTemplates); err != nil {
				return ErrConflict
			}
		} else if len(static.Consumers) > 0 || parent.Metadata[platformproducer.DNSPolicyIDMetadata] != "" || parent.Metadata[platformproducer.DNSPolicyDigestMetadata] != "" {
			return ErrConflict
		}
	} else if parent.Metadata[platformproducer.StaticIntentIDMetadata] != "" || parent.Metadata[platformproducer.StaticIntentDigestMetadata] != "" || parent.Metadata[platformproducer.DNSPolicyIDMetadata] != "" || parent.Metadata[platformproducer.DNSPolicyDigestMetadata] != "" {
		return ErrConflict
	}
	ids, idsOK := parent.Content["artifact_ids"].([]any)
	kinds, kindsOK := parent.Content["artifact_kinds"].([]any)
	if !idsOK || !kindsOK || len(ids) != 3 || len(kinds) != 3 {
		return ErrConflict
	}
	seen := map[string]bool{}
	for i, raw := range ids {
		id, ok := raw.(string)
		if !ok {
			return ErrConflict
		}
		index := platformArtifactIndex(state.PlatformArtifacts, id)
		if index < 0 {
			return ErrConflict
		}
		child := state.PlatformArtifacts[index]
		if seen[child.ArtifactKind] || kinds[i] != child.ArtifactKind || child.ScopeKey != parent.ScopeKey || child.Status != model.PlatformArtifactStatusValidated || child.Metadata["release_set_generation"] != parent.Generation || !platformsafety.EvaluateArtifactIntegrity(child, keys).Pass {
			return ErrConflict
		}
		for _, key := range []string{"intent_digest", "policy_digest", "compiler_version", "input_snapshot_digest", "intent_generation", "policy_generation"} {
			if parent.Metadata[key] == "" || child.Metadata[key] != parent.Metadata[key] {
				return ErrConflict
			}
		}
		if platformconfig.ValidateTrafficCohortProjection(parent, child) != nil {
			return ErrConflict
		}
		seen[child.ArtifactKind] = true
	}
	if !seen[model.PlatformArtifactKindEdgeRouteBundle] || !seen[model.PlatformArtifactKindDNSAnswerBundle] || !seen[model.PlatformArtifactKindCaddyRouteConfig] {
		return ErrConflict
	}
	lane, found := platformReleaseLaneByKey(state.PlatformReleaseLanes, platformsafety.ReleaseLaneKey(parent.ArtifactKind, parent.ScopeKey, req.ReleaseChannel))
	if found && lane.Frozen {
		return ErrConflict
	}
	if lane.ActiveReleaseID != guard.PreviousReleaseID {
		for _, previous := range state.PlatformArtifactReleases {
			if found && previous.ID == lane.ActiveReleaseID && previous.ArtifactID == parent.ID && previous.Status == model.PlatformArtifactReleaseStatusActive && previous.FencingToken == lane.FencingToken && previous.IdempotencyKey == req.IdempotencyKey && req.IdempotencyKey != "" {
				return nil
			}
		}
		return ErrConflict
	}
	return nil
}

// Lock the producer policy scope before the target scope. Policy writers use
// the same scope lock; reading its lane first cannot deadlock against a writer.
func (s *Store) pgProducerReleaseGuard(ctx context.Context, tx *sql.Tx, parent model.PlatformArtifact, req model.PlatformArtifactReleaseRequest, principal model.Principal, guard *platformProducerReleaseGuard) error {
	if guard == nil {
		return nil
	}
	state := &model.State{}
	for _, key := range []string{
		platformsafety.ReleaseLaneKey(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, model.PlatformArtifactReleaseChannelShadow),
		platformsafety.ReleaseLaneKey(parent.ArtifactKind, parent.ScopeKey, req.ReleaseChannel),
	} {
		lane, err := pgGetPlatformReleaseLaneForUpdate(ctx, tx, key)
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return err
		}
		state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, lane)
		if lane.ActiveReleaseID != "" {
			r, err := pgGetPlatformArtifactRelease(ctx, tx, lane.ActiveReleaseID, false)
			if err != nil {
				return err
			}
			state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
		}
	}
	release, err := pgGetPlatformArtifactRelease(ctx, tx, guard.PolicyReleaseID, false)
	if err != nil {
		return err
	}
	artifact, err := pgGetPlatformArtifactForUpdate(ctx, tx, release.ArtifactID, true)
	if err != nil {
		return err
	}
	state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, release)
	state.PlatformArtifacts = []model.PlatformArtifact{artifact}
	policy, err := platformproducer.Decode(artifact)
	if err != nil {
		return ErrConflict
	}
	if policy.StaticIntentArtifactID != "" {
		base, err := pgGetPlatformArtifactForUpdate(ctx, tx, policy.StaticIntentArtifactID, true)
		if err != nil {
			return err
		}
		state.PlatformArtifacts = append(state.PlatformArtifacts, base)
	}
	if policy.DNSPolicyArtifactID != "" {
		a, err := pgGetPlatformArtifactForUpdate(ctx, tx, policy.DNSPolicyArtifactID, true)
		if err != nil {
			return err
		}
		state.PlatformArtifacts = append(state.PlatformArtifacts, a)
	}
	ids, ok := parent.Content["artifact_ids"].([]any)
	if !ok || len(ids) != 3 {
		return ErrConflict
	}
	for _, raw := range ids {
		id, ok := raw.(string)
		if !ok || id == parent.ID {
			return ErrConflict
		}
		child, err := pgGetPlatformArtifactForUpdate(ctx, tx, id, true)
		if err != nil {
			return err
		}
		state.PlatformArtifacts = append(state.PlatformArtifacts, child)
	}
	return validateProducerReleaseGuard(state, parent, req, principal, s.platformArtifactSigningKeyring(), guard)
}
