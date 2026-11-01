package api

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

const inPlaceDeployReadinessReason = "release deployment image or current generation is not ready"

// An automatic code-release handoff is not an instruction to withdraw serving
// configuration. If the new target has not passed the independent runtime gate,
// refuse this capture rather than publish an unavailable replacement for a
// still-valid, verified positive LKG. The producer retries its normal capture;
// this neither copies an old binding into new authority nor renews old evidence.
// Explicit configuration/policy changes and first deployments keep the normal
// fail-closed compiler behavior. No release is invented or declared healthy.
func (s *Server) guardPendingStableHandoffs(ctx context.Context, p platformIntentProjectionResponse, business store.RouteBusinessSnapshot) error {
	policies := map[string]model.AppTrafficPolicy{}
	automatic := map[string]bool{}
	inFlightManagedDeploy := map[string]bool{}
	enabled := map[string]bool{}
	for _, route := range p.Intent.Routes {
		if route.Enabled {
			enabled[route.TenantID+"\x00"+route.AppID] = true
		}
	}
	managedRuntimes := map[string]bool{model.DefaultManagedRuntimeID: true}
	for _, runtime := range business.Runtimes {
		if runtime.Type == model.RuntimeTypeManagedOwned || runtime.Type == model.RuntimeTypeManagedShared {
			managedRuntimes[runtime.ID] = true
		}
	}
	for _, app := range business.Apps {
		key := app.TenantID + "\x00" + app.ID
		// The transactional business snapshot is frozen before the Kubernetes
		// read. An in-place deployment may finish between those two reads: the
		// new image is complete while the frozen stable release still names the
		// old image. Durable phase is only permission to defer publication, never
		// serving proof. The existing LKG and exact authority gates below remain
		// mandatory and its original evidence expiry is not extended.
		if strings.EqualFold(strings.TrimSpace(app.Status.Phase), "deploying") &&
			strings.TrimSpace(app.Status.LastOperationID) != "" &&
			!model.AppHasCurrentFailedOperation(app.Status) && managedRuntimes[strings.TrimSpace(app.Spec.RuntimeID)] {
			inFlightManagedDeploy[key] = true
		}
	}
	for _, policy := range business.TrafficPolicies {
		key := policy.TenantID + "\x00" + policy.AppID
		policies[key] = policy
		// This is the store's system-authored completed-deploy sync identity,
		// not a user policy edit. It grants no new traffic authority.
		if policy.UpdatedByType == model.ActorTypeSystem && policy.UpdatedByID == "stable-release-sync" && policy.Mode == model.AppTrafficModeSingle && policy.StableWeight == 100 && policy.CandidateWeight == 0 && policy.CandidateReleaseID == "" {
			automatic[key] = true
		}
	}
	pending := []platformconfig.ReleaseObservation{}
	for _, release := range p.RuntimeSnapshot.Releases {
		key := release.TenantID + "\x00" + release.AppID
		policy := policies[key]
		stablePolicy := policy.Mode == model.AppTrafficModeSingle && policy.StableWeight == 100 && policy.CandidateWeight == 0 && policy.CandidateReleaseID == ""
		preSyncTransition := inFlightManagedDeploy[key] && release.StatusReason == inPlaceDeployReadinessReason
		if stablePolicy && (automatic[key] || preSyncTransition) && enabled[key] && release.ID == policy.StableReleaseID && release.Status != model.EdgeRouteStatusActive {
			pending = append(pending, release)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	source, err := s.readRetainedTrafficSource(ctx, p.Intent.Scope)
	if err != nil {
		// No independently verified positive source means no special handoff
		// protection. Keep normal strict compilation (including configuration
		// recovery) rather than making failed evidence a new serving authority.
		return nil
	}
	adjusted := p.Policy
	adjusted.TrafficConstraints = append([]platformconfig.TrafficPolicyConstraint(nil), p.Policy.TrafficConstraints...)
	protected := []platformconfig.ReleaseObservation{}
	for _, release := range pending {
		var oldRule, nextRule platformconfig.TrafficPolicyConstraint
		for _, rule := range source.policy.TrafficConstraints {
			if rule.AppID == release.AppID && rule.TenantID == release.TenantID {
				oldRule = rule
			}
		}
		for _, rule := range p.Policy.TrafficConstraints {
			if rule.AppID == release.AppID && rule.TenantID == release.TenantID {
				nextRule = rule
			}
		}
		if oldRule.StableReleaseID == "" || nextRule.StableReleaseID != release.ID || oldRule.Mode != model.AppTrafficModeSingle || oldRule.StableWeight != 100 || oldRule.CandidateWeight != 0 || oldRule.CandidateReleaseID != "" {
			continue
		}
		key := release.TenantID + "\x00" + release.AppID
		if oldRule.StableReleaseID == nextRule.StableReleaseID {
			if !inFlightManagedDeploy[key] || release.StatusReason != inPlaceDeployReadinessReason {
				continue
			}
		} else if !automatic[key] {
			// A running code deployment cannot authorize an explicit stable
			// selection. Only the store's completed-deploy sync may change it.
			continue
		}
		// Compare all per-app serving authority, including source, topology, TLS,
		// cache, route ownership, limits and constraints. Only the system-authored
		// stable release identity may differ; do not loosen retention's digest gate.
		trial := p.Policy
		trial.TrafficConstraints = append([]platformconfig.TrafficPolicyConstraint(nil), p.Policy.TrafficConstraints...)
		for i := range trial.TrafficConstraints {
			if trial.TrafficConstraints[i].AppID == release.AppID && trial.TrafficConstraints[i].TenantID == release.TenantID {
				trial.TrafficConstraints[i].StableReleaseID = oldRule.StableReleaseID
			}
		}
		before, beforeErr := platformconfig.TrafficPartitionInputDigest(source.intent, source.policy, release.TenantID, release.AppID)
		after, afterErr := platformconfig.TrafficPartitionInputDigest(p.Intent, trial, release.TenantID, release.AppID)
		if beforeErr != nil || afterErr != nil || before != after {
			continue
		}
		previous := source.routes[release.TenantID+"\x00"+release.AppID]
		if len(previous) == 0 {
			continue
		}
		positive := true
		for _, route := range previous {
			positive = positive && route.Enabled && (route.Status == "" || route.Status == model.EdgeRouteStatusActive) && len(route.Upstreams) == 1 && route.Upstreams[0].ReleaseID == oldRule.StableReleaseID && route.Upstreams[0].Weight == 100
		}
		if !positive || !source.expiry(release.TenantID, release.AppID).After(p.CapturedAt) {
			continue
		}
		for i := range adjusted.TrafficConstraints {
			if adjusted.TrafficConstraints[i].AppID == release.AppID && adjusted.TrafficConstraints[i].TenantID == release.TenantID {
				adjusted.TrafficConstraints[i].StableReleaseID = oldRule.StableReleaseID
			}
		}
		protected = append(protected, release)
	}
	if len(protected) > 0 && stableHandoffAuthorityEqual(source.intent, source.policy, p.Intent, adjusted) {
		release := protected[0]
		return fmt.Errorf("managed app serving handoff pending: app_id=%s release_id=%s; preserving current verified serving artifact without publication; runtime_reason=%s", release.AppID, release.ID, release.StatusReason)
	}
	return nil
}

// Capture is scope-wide: pausing it must never block another app's explicit
// change, a DNS recovery, revocation, or policy adjustment. Only a pure automatic
// code handoff may wait on the old artifact. No old partition is copied into a
// new artifact under changed authority; normal strict compilation handles that.
func stableHandoffAuthorityEqual(oldIntent platformconfig.PlatformIntent, oldPolicy platformconfig.PolicySnapshot, nextIntent platformconfig.PlatformIntent, nextPolicy platformconfig.PolicySnapshot) bool {
	oldIntent, nextIntent = platformconfig.NormalizePlatformIntent(oldIntent), platformconfig.NormalizePlatformIntent(nextIntent)
	oldPolicy, nextPolicy = platformconfig.NormalizePolicySnapshot(oldPolicy), platformconfig.NormalizePolicySnapshot(nextPolicy)
	oldIntent.Generation, nextIntent.Generation = "", ""
	oldIntent.CreatedAt, nextIntent.CreatedAt = time.Time{}, time.Time{}
	oldPolicy.Generation, nextPolicy.Generation = "", ""
	oldPolicy.CreatedAt, nextPolicy.CreatedAt = time.Time{}, time.Time{}
	before, err := platformconfig.Digest(struct {
		Intent platformconfig.PlatformIntent
		Policy platformconfig.PolicySnapshot
	}{oldIntent, oldPolicy})
	after, nextErr := platformconfig.Digest(struct {
		Intent platformconfig.PlatformIntent
		Policy platformconfig.PolicySnapshot
	}{nextIntent, nextPolicy})
	return err == nil && nextErr == nil && before == after
}
