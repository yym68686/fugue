package api

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

type retainedTrafficSource struct {
	parent, route model.PlatformArtifact
	release       model.PlatformArtifactRelease
	lkg           model.PlatformLKGSnapshot
	intent        platformconfig.PlatformIntent
	policy        platformconfig.PolicySnapshot
	snapshot      platformconfig.RuntimeSnapshot
	routes        map[string][]platformconfig.CompiledRoute
}

func (s *Server) readRetainedTrafficSource(ctx context.Context, scope string) (retainedTrafficSource, error) {
	var source retainedTrafficSource
	parent, release, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, scope, "full")
	if err != nil || !found || release.VerificationState != model.PlatformArtifactVerificationStateVerified || release.VerifiedLKGGeneration != parent.Generation || !s.validateReleaseSetReferences(parent).Pass {
		return source, fmt.Errorf("retained partition has no verified full source")
	}
	lkg, err := s.store.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, scope)
	if err != nil || lkg == nil || lkg.VerifiedByReleaseID != release.ID || !platformsafety.EvaluatePlatformLKGSnapshot(*lkg, parent, s.bundleKeyring(), time.Now().UTC()).Pass {
		return source, fmt.Errorf("retained partition positive LKG unavailable")
	}
	route, err := s.consumerAssignmentChild(parent, model.PlatformArtifactKindEdgeRouteBundle)
	if err != nil || !platformsafety.EvaluateArtifactIntegrity(route, s.bundleKeyring()).Pass {
		return source, fmt.Errorf("retained partition route signature unavailable")
	}
	if gate := s.validateReleaseSetConvergence(ctx, parent); !gate.Pass {
		return source, fmt.Errorf("retained partition current consumer evidence unavailable")
	}
	for _, in := range []struct {
		kind, generation string
		target           any
	}{
		{model.PlatformArtifactKindPlatformIntent, parent.Metadata["intent_generation"], &source.intent},
		{model.PlatformArtifactKindPolicySnapshot, parent.Metadata["policy_generation"], &source.policy},
	} {
		a, err := s.store.GetPlatformArtifactByIdentity(in.kind, scope, in.generation)
		if err != nil || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, s.bundleKeyring()).Pass {
			return source, fmt.Errorf("retained partition source input unavailable")
		}
		raw, err := json.Marshal(a.Content)
		if err != nil || json.Unmarshal(raw, in.target) != nil {
			return source, fmt.Errorf("retained partition source input invalid")
		}
	}
	if digest, _ := platformconfig.Digest(source.intent); digest != parent.Metadata["intent_digest"] {
		return source, fmt.Errorf("retained partition intent lineage differs")
	}
	if digest, _ := platformconfig.Digest(source.policy); digest != parent.Metadata["policy_digest"] {
		return source, fmt.Errorf("retained partition policy lineage differs")
	}
	source.snapshot, err = s.store.GetPlatformCompilerInput(parent.Metadata["input_snapshot_digest"])
	if err != nil || source.snapshot.IntentGeneration != source.intent.Generation || source.snapshot.PolicyGeneration != source.policy.Generation {
		return source, fmt.Errorf("retained partition source observations unavailable")
	}
	var routes []platformconfig.CompiledRoute
	raw, _ := json.Marshal(route.Content["routes"])
	if json.Unmarshal(raw, &routes) != nil {
		return source, fmt.Errorf("retained partition route payload invalid")
	}
	source.routes = map[string][]platformconfig.CompiledRoute{}
	for _, r := range routes {
		source.routes[r.TenantID+"\x00"+r.AppID] = append(source.routes[r.TenantID+"\x00"+r.AppID], r)
	}
	source.parent, source.route, source.release, source.lkg = parent, route, release, *lkg
	return source, nil
}

// The oldest input timestamp is not renewed by capture or by a new publication.
// Neither a recently read old artifact nor its still-live signature is a fresh
// observation of the failed workload.
func (source retainedTrafficSource) expiry(tenant, app string) time.Time {
	expires := source.lkg.ExpiresAt
	cut := func(at time.Time) {
		candidate := at.Add(time.Duration(source.policy.MaxStaleSeconds) * time.Second)
		if at.IsZero() {
			candidate = time.Time{}
		}
		if candidate.Before(expires) {
			expires = candidate
		}
	}
	if source.snapshot.CapturedAt == nil {
		return time.Time{}
	}
	cut(*source.snapshot.CapturedAt)
	for _, part := range source.snapshot.RetainedTrafficPartitions {
		if part.TenantID == tenant && part.AppID == app && part.ExpiresAt.Before(expires) {
			expires = part.ExpiresAt
		}
	}
	origins := map[string]bool{}
	for _, r := range source.intent.Routes {
		if r.TenantID == tenant && r.AppID == app {
			origins[r.OriginRef] = true
		}
	}
	for _, origin := range source.snapshot.Origins {
		if origins[origin.Ref] {
			cut(origin.ObservedAt)
		}
	}
	for _, release := range source.snapshot.Releases {
		if release.TenantID == tenant && release.AppID == app {
			cut(release.ObservedAt)
		}
	}
	return expires
}

func (s *Server) captureRetainedTrafficPartitions(ctx context.Context, p *platformIntentProjectionResponse, budgets ...time.Duration) {
	if !p.RuntimeSnapshot.IsolateTrafficAvailability {
		return
	}
	bad := map[string][]platformconfig.ReleaseObservation{}
	unavailable := map[string][]platformconfig.ReleaseObservation{}
	for _, r := range p.RuntimeSnapshot.Releases {
		if r.Status != model.EdgeRouteStatusActive {
			key := r.TenantID + "\x00" + r.AppID
			unavailable[key] = append(unavailable[key], r)
		}
	}
	routes := make([]platformconfig.CompiledRoute, 0, len(p.Intent.Routes))
	for _, r := range p.Intent.Routes {
		routes = append(routes, platformconfig.CompiledRoute{RouteIntent: r})
	}
	routes, err := platformconfig.ApplyRoutePolicyConstraints(routes, p.Policy, p.RuntimeSnapshot.CapturedAt)
	if err != nil {
		return // The strict readiness projection reports malformed authority.
	}
	routes, err = platformconfig.ApplyTrafficPolicyConstraints(routes, p.Policy, p.RuntimeSnapshot)
	if err != nil {
		return
	}
	for _, r := range routes {
		if strings.HasPrefix(r.StatusReason, "application traffic partition pending:") {
			key := r.TenantID + "\x00" + r.AppID
			if fact, ok := unavailable[key]; ok {
				bad[key] = fact
			}
		}
	}
	if len(bad) == 0 {
		return
	}
	source, sourceErr := s.readRetainedTrafficSource(ctx, p.Intent.Scope)
	minimumExpiry := p.CapturedAt
	if len(budgets) > 0 && budgets[0] > 0 {
		minimumExpiry = minimumExpiry.Add(budgets[0])
	}
	keys := make([]string, 0, len(bad))
	for key := range bad {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		facts := bad[key]
		r := facts[0] // Only the owner is used for retention, never a guessed failing release.
		reasons := make([]string, 0, len(facts))
		for _, fact := range facts {
			reasons = append(reasons, "release_id="+fact.ID+" "+fact.StatusReason)
		}
		sort.Strings(reasons)
		issue := platformProjectionIssue{Code: "traffic_partition_pending", Reason: "app_id=" + r.AppID + " unavailable_observations=[" + strings.Join(reasons, "; ") + "]"}
		if sourceErr == nil {
			before, err := platformconfig.TrafficPartitionInputDigest(source.intent, source.policy, r.TenantID, r.AppID)
			after, currentErr := platformconfig.TrafficPartitionInputDigest(p.Intent, p.Policy, r.TenantID, r.AppID)
			expires := source.expiry(r.TenantID, r.AppID)
			if err == nil && currentErr == nil && before == after && expires.After(minimumExpiry) {
				part := platformconfig.RetainedTrafficPartition{AppID: r.AppID, TenantID: r.TenantID, InputDigest: after, Source: model.PlatformPublicationPrecondition{ArtifactID: source.parent.ID, ContentHash: source.parent.ContentHash, ReleaseID: source.release.ID, FencingToken: source.release.FencingToken}, RouteArtifactID: source.route.ID, RouteArtifactDigest: source.route.ContentHash, ObservedAt: p.CapturedAt, ExpiresAt: expires, Routes: platformconfig.NormalizeTrafficPartitionRoutes(source.routes[key])}
				trial := p.RuntimeSnapshot
				trial.RetainedTrafficPartitions = []platformconfig.RetainedTrafficPartition{part}
				routes, err := platformconfig.ResolveRouteOrigins(p.Intent.Routes, trial, p.Policy)
				if err == nil {
					routes, err = platformconfig.ApplyRoutePolicyConstraints(routes, p.Policy, trial.CapturedAt)
				}
				if err == nil {
					_, err = platformconfig.ApplyTrafficPolicyConstraints(routes, p.Policy, trial)
				}
				if err == nil {
					p.RuntimeSnapshot.RetainedTrafficPartitions = append(p.RuntimeSnapshot.RetainedTrafficPartitions, part)
					issue.Code = "traffic_partition_retained"
				}
			}
		}
		p.Issues = append(p.Issues, issue)
		if s.log != nil {
			s.log.Printf("platform traffic partition app=%s unavailable_releases=%d state=%s source_available=%t", r.AppID, len(facts), issue.Code, sourceErr == nil)
		}
	}
}

func (s *Server) validateRetainedTrafficCompilation(ctx context.Context, compiled platformconfig.CompileResult) error {
	parts := compiled.InputSnapshot.RetainedTrafficPartitions
	if len(parts) == 0 {
		return nil
	}
	source, err := s.readRetainedTrafficSource(ctx, compiled.ReleaseSet.Scope)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, part := range parts {
		pin := part.Source
		digest, err := platformconfig.TrafficPartitionInputDigest(source.intent, source.policy, part.TenantID, part.AppID)
		if err != nil || digest != part.InputDigest || pin.ArtifactID != source.parent.ID || pin.ContentHash != source.parent.ContentHash || pin.ReleaseID != source.release.ID || pin.FencingToken != source.release.FencingToken || part.RouteArtifactID != source.route.ID || part.RouteArtifactDigest != source.route.ContentHash || !part.ExpiresAt.After(now) || part.ExpiresAt.After(source.expiry(part.TenantID, part.AppID)) || !reflect.DeepEqual(platformconfig.NormalizeTrafficPartitionRoutes(source.routes[part.TenantID+"\x00"+part.AppID]), part.Routes) {
			return fmt.Errorf("retained traffic partition source, expiry or exact route content changed")
		}
	}
	return nil
}
