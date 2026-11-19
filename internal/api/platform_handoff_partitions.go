package api

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

// Only a durable, nonfailed managed deployment operation can explain the exact
// old-stable/current-image transition. Neither an arbitrary runtime failure nor
// the presence of a caller-provided fact is permission to preserve a route.
func managedHandoffOperation(b store.RouteBusinessSnapshot, h platformconfig.ManagedServingHandoff, reason string) (app model.App, completed bool, ok bool) {
	managed := map[string]bool{model.DefaultManagedRuntimeID: true}
	for _, r := range b.Runtimes {
		if r.Type == model.RuntimeTypeManagedShared || r.Type == model.RuntimeTypeManagedOwned {
			managed[r.ID] = true
		}
	}
	for _, a := range b.Apps {
		if a.ID == h.AppID && a.TenantID == h.TenantID {
			app = a
			break
		}
	}
	if app.ID == "" || !managed[app.Spec.RuntimeID] || h.OperationID == "" || app.Status.LastOperationID != h.OperationID || model.AppHasCurrentFailedOperation(app.Status) {
		return app, false, false
	}
	for _, p := range b.TrafficPolicies {
		if p.TenantID == h.TenantID && p.AppID == h.AppID && p.Mode == model.AppTrafficModeSingle && p.StableWeight == 100 && p.CandidateWeight == 0 && p.CandidateReleaseID == "" && p.StableReleaseID == h.ReleaseID {
			for _, op := range b.Operations {
				if op.ID != h.OperationID || op.AppID != app.ID || op.TenantID != app.TenantID {
					continue
				}
				switch reason {
				case platformconfig.ManagedHandoffReleaseReason:
					if op.ActiveDeploy() {
						return app, false, true
					}
					fallthrough
				case platformconfig.ManagedHandoffEndpointsReason:
					if op.SyncedStableRelease(p) {
						return app, true, true
					}
				}
			}
		}
	}
	return app, false, false
}

// The observer may report deployed before the operation commits its new image
// and stable release. Require the exact current operation's nonterminal state,
// not the app's presentation phase, both at capture and publication admission.
func activeRouteDeployOperation(b store.RouteBusinessSnapshot, app model.App) bool {
	if app.Status.LastOperationID == "" || model.AppHasCurrentFailedOperation(app.Status) {
		return false
	}
	for _, op := range b.Operations {
		if op.ID == app.Status.LastOperationID && op.AppID == app.ID && op.TenantID == app.TenantID && op.ActiveDeploy() {
			return true
		}
	}
	return false
}

func handoffRouteKey(r platformconfig.RouteIntent) string {
	return r.TenantID + "\x00" + r.AppID + "\x00" + r.Hostname + "\x00" + model.NormalizeAppRoutePathPrefix(r.PathPrefix)
}

// Compare a managed handoff against the previous epochs only after proving
// they are generated deployment/cache values. Callers still have to compare
// all remaining authority; this copy is never a compiler or publication input.
func intentWithPreviousHandoffEpochs(source retainedTrafficSource, intent platformconfig.PlatformIntent, tenantID, appID, operationID string) (platformconfig.PlatformIntent, bool) {
	if operationID == "" {
		return intent, false
	}
	trial := intent
	trial.Routes = append([]platformconfig.RouteIntent(nil), intent.Routes...)
	old := map[string]platformconfig.RouteIntent{}
	for _, r := range source.intent.Routes {
		if r.TenantID == tenantID && r.AppID == appID {
			old[handoffRouteKey(r)] = r
		}
	}
	count := 0
	for i := range trial.Routes {
		r := &trial.Routes[i]
		if r.AppID != appID || r.TenantID != tenantID {
			continue
		}
		previous, ok := old[handoffRouteKey(*r)]
		if !ok || !r.Enabled || r.Status != "" && r.Status != model.EdgeRouteStatusActive || r.RoutePolicy != "" && !model.EdgeRoutePolicyAllowsTraffic(r.RoutePolicy) || r.DeploymentGeneration != operationID || previous.DeploymentGeneration == "" {
			return intent, false
		}
		if r.CacheNamespace != previous.CacheNamespace {
			if r.CachePolicyID == "" || previous.CachePolicyID == "" || r.CacheNamespace != appID+"_"+operationID || previous.CacheNamespace != appID+"_"+previous.DeploymentGeneration {
				return intent, false
			}
		} else if r.CachePolicyID != "" && r.DeploymentGeneration != previous.DeploymentGeneration {
			// A changed operation with an unchanged custom cache namespace is
			// explicit authority, not the generated namespace this guard owns.
			return intent, false
		}
		r.DeploymentGeneration, r.CacheNamespace = previous.DeploymentGeneration, previous.CacheNamespace
		count++
	}
	if count == 0 || count != len(old) {
		return intent, false
	}
	return trial, true
}

// Do not remove deployment/cache epochs from the normal authorization digest.
// Only this proven transition may compare the rest of the per-app authority.
func managedHandoffAuthorityEqual(source retainedTrafficSource, intent platformconfig.PlatformIntent, policy platformconfig.PolicySnapshot, h platformconfig.ManagedServingHandoff) bool {
	trial, ok := intentWithPreviousHandoffEpochs(source, intent, h.TenantID, h.AppID, h.OperationID)
	if !ok {
		return false
	}
	before, e1 := platformconfig.TrafficPartitionInputDigest(source.intent, source.policy, h.TenantID, h.AppID)
	after, e2 := platformconfig.TrafficPartitionInputDigest(trial, policy, h.TenantID, h.AppID)
	return e1 == nil && e2 == nil && before == after
}

func managedHandoffSourcePositive(source retainedTrafficSource, h platformconfig.ManagedServingHandoff, runtimeID string) bool {
	routes := source.routes[h.TenantID+"\x00"+h.AppID]
	if len(routes) == 0 {
		return false
	}
	for _, r := range routes {
		if !r.Enabled || r.Status != "" && r.Status != model.EdgeRouteStatusActive || len(r.Upstreams) != 1 || r.Upstreams[0].ReleaseID != h.ReleaseID || r.Upstreams[0].Weight != 100 || r.Upstreams[0].RuntimeID != runtimeID {
			return false
		}
	}
	return true
}

// Successive automatic deployments can advance the durable stable selection
// before an intermediate traffic publication becomes verified. Compare the
// current operation's epoch and that system-authored selection together. This
// authorizes only the exact earlier verified path, never a relabeled upstream
// or readiness for the intermediate release. An operator selection cannot use
// this exception; admission repeats the durable actor and operation checks.
func managedHandoffMatchesSource(source retainedTrafficSource, intent platformconfig.PlatformIntent, policy platformconfig.PolicySnapshot, b store.RouteBusinessSnapshot, h platformconfig.ManagedServingHandoff, runtimeID string, completed bool) bool {
	previous := h
	foundSourcePolicy := false
	for _, rule := range source.policy.TrafficConstraints {
		if rule.AppID == h.AppID && rule.TenantID == h.TenantID {
			if rule.Mode != model.AppTrafficModeSingle || rule.StableWeight != 100 || rule.CandidateWeight != 0 || rule.CandidateReleaseID != "" || rule.StableReleaseID == "" {
				return false
			}
			previous.ReleaseID = rule.StableReleaseID
			foundSourcePolicy = true
		}
	}
	if !foundSourcePolicy {
		return false
	}
	if previous.ReleaseID != h.ReleaseID {
		automatic := false
		for _, current := range b.TrafficPolicies {
			if current.AppID == h.AppID && current.TenantID == h.TenantID && current.StableReleaseID == h.ReleaseID && current.UpdatedByType == model.ActorTypeSystem && current.UpdatedByID == "stable-release-sync" && current.Mode == model.AppTrafficModeSingle && current.StableWeight == 100 && current.CandidateWeight == 0 && current.CandidateReleaseID == "" {
				automatic = true
			}
		}
		if !automatic {
			return false
		}
		policy.TrafficConstraints = append([]platformconfig.TrafficPolicyConstraint(nil), policy.TrafficConstraints...)
		found := false
		for i := range policy.TrafficConstraints {
			rule := &policy.TrafficConstraints[i]
			if rule.AppID == h.AppID && rule.TenantID == h.TenantID {
				if rule.StableReleaseID != h.ReleaseID {
					return false
				}
				rule.StableReleaseID = previous.ReleaseID
				found = true
			}
		}
		if !found {
			return false
		}
	}
	if !managedHandoffAuthorityEqual(source, intent, policy, h) {
		return false
	}
	routes := source.routes[h.TenantID+"\x00"+h.AppID]
	if len(routes) == 0 || len(routes[0].Upstreams) != 1 {
		return false
	}
	if selected := routes[0].Upstreams[0].ReleaseID; selected != previous.ReleaseID {
		// A verified retained publication can itself be the next source. Its
		// signed retention metadata must account for the exact earlier route;
		// never infer a new release identity or flatten away that provenance.
		parts, err := platformconfig.RetainedTrafficPartitionsFromArtifact(source.route)
		if err != nil {
			return false
		}
		retained := false
		for _, part := range parts {
			if part.TenantID == h.TenantID && part.AppID == h.AppID && reflect.DeepEqual(platformconfig.NormalizeTrafficPartitionRoutes(part.Routes), platformconfig.NormalizeTrafficPartitionRoutes(routes)) {
				retained = true
			}
		}
		if !retained || selected == "" {
			return false
		}
		previous.ReleaseID = selected
	}
	// Once the actual new stable has a positive publication this is no longer
	// a handoff. Ordinary retention and failure policy own subsequent outages.
	return (!completed || previous.ReleaseID != h.ReleaseID) && managedHandoffSourcePositive(source, previous, runtimeID)
}

func (s *Server) captureManagedHandoffPartitions(ctx context.Context, p *platformIntentProjectionResponse, b store.RouteBusinessSnapshot, budgets ...time.Duration) error {
	return s.captureManagedHandoffPartitionsWithSource(ctx, p, b, s.newRetainedTrafficSourceReader(ctx, p.Intent.Scope), budgets...)
}

func (s *Server) captureManagedHandoffPartitionsWithSource(ctx context.Context, p *platformIntentProjectionResponse, b store.RouteBusinessSnapshot, readSource retainedTrafficSourceReader, budgets ...time.Duration) error {
	if !p.RuntimeSnapshot.IsolateTrafficAvailability {
		return nil
	}
	type candidate struct {
		handoff   platformconfig.ManagedServingHandoff
		runtimeID string
		completed bool
	}
	var candidates []candidate
	for _, r := range p.RuntimeSnapshot.Releases {
		if r.Status != model.EdgeRouteStatusUnavailable || (r.StatusReason != platformconfig.ManagedHandoffReleaseReason && r.StatusReason != platformconfig.ManagedHandoffEndpointsReason) {
			continue
		}
		for _, a := range b.Apps {
			if a.ID != r.AppID || a.TenantID != r.TenantID {
				continue
			}
			h := platformconfig.ManagedServingHandoff{TenantID: r.TenantID, AppID: r.AppID, ReleaseID: r.ID, OperationID: a.Status.LastOperationID}
			if app, completed, ok := managedHandoffOperation(b, h, r.StatusReason); ok {
				candidates = append(candidates, candidate{h, app.Spec.RuntimeID, completed})
			}
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	source, err := readSource()
	if err != nil {
		for _, c := range candidates {
			h := c.handoff
			if managedHandoffMatchesSource(source, p.Intent, p.Policy, b, h, c.runtimeID, c.completed) && (c.completed || managedHandoffOriginPending(*p, h)) {
				// A failed complete proof is not a negative workload observation.
				// Do not publish an empty replacement for a signed positive path.
				// Retry capture; do not copy the partial proof, renew its expiry or
				// mark the new workload healthy. Existing Edge expiry still applies.
				return fmt.Errorf("managed app serving handoff evidence unavailable: app_id=%s operation_id=%s; retry capture without publication: %w", h.AppID, h.OperationID, err)
			}
		}
		return nil
	}
	minimum := p.CapturedAt
	if len(budgets) > 0 && budgets[0] > 0 {
		minimum = minimum.Add(budgets[0])
	}
	var facts []platformconfig.ManagedServingHandoff
	for _, c := range candidates {
		h := c.handoff
		if !managedHandoffMatchesSource(source, p.Intent, p.Policy, b, h, c.runtimeID, c.completed) {
			continue
		}
		expires := source.expiry(h.TenantID, h.AppID)
		if !expires.After(minimum) {
			continue
		}
		digest, err := platformconfig.TrafficPartitionInputDigest(p.Intent, p.Policy, h.TenantID, h.AppID)
		if err != nil {
			continue
		}
		part := platformconfig.RetainedTrafficPartition{TenantID: h.TenantID, AppID: h.AppID, InputDigest: digest, Source: model.PlatformPublicationPrecondition{ArtifactID: source.parent.ID, ContentHash: source.parent.ContentHash, ReleaseID: source.release.ID, FencingToken: source.release.FencingToken}, RouteArtifactID: source.route.ID, RouteArtifactDigest: source.route.ContentHash, ObservedAt: p.CapturedAt, ExpiresAt: expires, Routes: platformconfig.NormalizeTrafficPartitionRoutes(source.routes[h.TenantID+"\x00"+h.AppID])}
		p.RuntimeSnapshot.RetainedTrafficPartitions = append(p.RuntimeSnapshot.RetainedTrafficPartitions, part)
		facts = append(facts, h)
		p.Issues = append(p.Issues, platformProjectionIssue{Code: "managed_handoff_retained", Reason: "app_id=" + h.AppID + " exact verified route retained within original evidence expiry"})
		if s.log != nil {
			s.log.Printf("platform managed handoff app=%s operation=%s source=%s expires_at=%s", h.AppID, h.OperationID, source.parent.ID, expires.UTC().Format(time.RFC3339Nano))
		}
	}
	if len(facts) > 0 {
		if p.RuntimeSnapshot.Facts == nil {
			p.RuntimeSnapshot.Facts = map[string]any{}
		}
		// Facts persist through JSON/JSONB. Keep their canonical generic shape
		// so digest bytes remain identical after a decode on another API replica.
		raw, _ := json.Marshal(facts)
		var values []any
		_ = json.Unmarshal(raw, &values)
		p.RuntimeSnapshot.Facts[platformconfig.ManagedServingHandoffsFact] = values
	}
	return nil
}

func managedHandoffOriginPending(p platformIntentProjectionResponse, h platformconfig.ManagedServingHandoff) bool {
	origins := map[string]platformconfig.OriginObservation{}
	for _, origin := range p.RuntimeSnapshot.Origins {
		origins[origin.Ref] = origin
	}
	found := false
	for _, r := range p.Intent.Routes {
		if r.AppID != h.AppID || r.TenantID != h.TenantID {
			continue
		}
		origin, ok := origins[r.OriginRef]
		if !ok || origin.Status != model.EdgeRouteStatusUnavailable || origin.StatusReason != platformconfig.ManagedHandoffOriginReason {
			return false
		}
		found = true
	}
	return found
}

// Admission repeats the business gate, not just the claimed runtime fact. A
// replaced/failed operation or explicit selection cannot admit an old
// partition. A completed operation must prove its exact automatic policy sync.
func (s *Server) validateManagedHandoffCompilation(ctx context.Context, compiled platformconfig.CompileResult, source retainedTrafficSource) (map[string]bool, error) {
	out := map[string]bool{}
	facts, err := platformconfig.ManagedServingHandoffs(compiled.InputSnapshot)
	if err != nil || len(facts) == 0 {
		return out, err
	}
	b, err := s.store.CaptureRouteBusinessSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	var intent platformconfig.PlatformIntent
	var policy platformconfig.PolicySnapshot
	iraw, e1 := json.Marshal(compiled.IntentArtifact.Content)
	praw, e2 := json.Marshal(compiled.PolicyArtifact.Content)
	if e1 != nil || e2 != nil || json.Unmarshal(iraw, &intent) != nil || json.Unmarshal(praw, &policy) != nil {
		return nil, fmt.Errorf("managed handoff immutable inputs invalid")
	}
	reasons := make(map[string]string, len(compiled.InputSnapshot.Releases))
	for _, r := range compiled.InputSnapshot.Releases {
		if r.Status == model.EdgeRouteStatusUnavailable {
			reasons[r.TenantID+"\x00"+r.AppID+"\x00"+r.ID] = r.StatusReason
		}
	}
	for _, h := range facts {
		app, completed, ok := managedHandoffOperation(b, h, reasons[h.TenantID+"\x00"+h.AppID+"\x00"+h.ReleaseID])
		if !ok || !managedHandoffMatchesSource(source, intent, policy, b, h, app.Spec.RuntimeID, completed) {
			return nil, fmt.Errorf("managed handoff operation or authority changed")
		}
		digest, err := platformconfig.TrafficPartitionInputDigest(intent, policy, h.TenantID, h.AppID)
		if err != nil {
			return nil, err
		}
		for _, part := range compiled.InputSnapshot.RetainedTrafficPartitions {
			if part.TenantID == h.TenantID && part.AppID == h.AppID && part.InputDigest != digest {
				return nil, fmt.Errorf("managed handoff target digest changed")
			}
		}
		out[h.TenantID+"\x00"+h.AppID] = true
	}
	return out, nil
}
