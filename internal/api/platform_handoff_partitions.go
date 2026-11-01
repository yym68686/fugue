package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

// Only a durable, nonfailed managed deploying operation can explain the exact
// old-stable/current-image transition. Neither an arbitrary runtime failure nor
// the presence of a caller-provided fact is permission to preserve a route.
func managedHandoffOperation(b store.RouteBusinessSnapshot, h platformconfig.ManagedServingHandoff) (model.App, bool) {
	managed := map[string]bool{model.DefaultManagedRuntimeID: true}
	for _, r := range b.Runtimes {
		if r.Type == model.RuntimeTypeManagedShared || r.Type == model.RuntimeTypeManagedOwned {
			managed[r.ID] = true
		}
	}
	var app model.App
	for _, a := range b.Apps {
		if a.ID == h.AppID && a.TenantID == h.TenantID {
			app = a
			break
		}
	}
	if !managed[app.Spec.RuntimeID] || !strings.EqualFold(strings.TrimSpace(app.Status.Phase), "deploying") || app.Status.LastOperationID != h.OperationID || h.OperationID == "" || model.AppHasCurrentFailedOperation(app.Status) {
		return app, false
	}
	for _, p := range b.TrafficPolicies {
		if p.TenantID == h.TenantID && p.AppID == h.AppID && p.Mode == model.AppTrafficModeSingle && p.StableWeight == 100 && p.CandidateWeight == 0 && p.CandidateReleaseID == "" && p.StableReleaseID == h.ReleaseID {
			return app, true
		}
	}
	return app, false
}

func handoffRouteKey(r platformconfig.RouteIntent) string {
	return r.TenantID + "\x00" + r.AppID + "\x00" + r.Hostname + "\x00" + model.NormalizeAppRoutePathPrefix(r.PathPrefix)
}

// Do not remove deployment/cache epochs from the normal authorization digest.
// For this one transition only, prove both are the generic operation-derived
// values before comparing every remaining per-app intent/policy dependency.
// The desired inputs remain untouched; the output retains the old exact route.
func managedHandoffAuthorityEqual(source retainedTrafficSource, intent platformconfig.PlatformIntent, policy platformconfig.PolicySnapshot, h platformconfig.ManagedServingHandoff) bool {
	trial := intent
	trial.Routes = append([]platformconfig.RouteIntent(nil), intent.Routes...)
	old := map[string]platformconfig.RouteIntent{}
	for _, r := range source.intent.Routes {
		if r.TenantID == h.TenantID && r.AppID == h.AppID {
			old[handoffRouteKey(r)] = r
		}
	}
	count := 0
	for i := range trial.Routes {
		r := &trial.Routes[i]
		if r.AppID != h.AppID || r.TenantID != h.TenantID {
			continue
		}
		previous, ok := old[handoffRouteKey(*r)]
		if !ok || !r.Enabled || r.Status != "" && r.Status != model.EdgeRouteStatusActive || r.RoutePolicy != "" && !model.EdgeRoutePolicyAllowsTraffic(r.RoutePolicy) || r.DeploymentGeneration != h.OperationID || previous.DeploymentGeneration == "" {
			return false
		}
		if r.CacheNamespace != previous.CacheNamespace {
			if r.CachePolicyID == "" || previous.CachePolicyID == "" || r.CacheNamespace != h.AppID+"_"+h.OperationID || previous.CacheNamespace != h.AppID+"_"+previous.DeploymentGeneration {
				return false
			}
		} else if r.CachePolicyID != "" && r.DeploymentGeneration != previous.DeploymentGeneration {
			// A changed operation with an unchanged custom cache namespace is
			// explicit authority, not the generated namespace this guard owns.
			return false
		}
		r.DeploymentGeneration, r.CacheNamespace = previous.DeploymentGeneration, previous.CacheNamespace
		count++
	}
	if count == 0 || count != len(old) {
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

func (s *Server) captureManagedHandoffPartitions(ctx context.Context, p *platformIntentProjectionResponse, b store.RouteBusinessSnapshot, budgets ...time.Duration) {
	if !p.RuntimeSnapshot.IsolateTrafficAvailability {
		return
	}
	var candidates []platformconfig.ManagedServingHandoff
	for _, r := range p.RuntimeSnapshot.Releases {
		if r.Status != model.EdgeRouteStatusUnavailable || r.StatusReason != platformconfig.ManagedHandoffReleaseReason {
			continue
		}
		for _, a := range b.Apps {
			if a.ID != r.AppID || a.TenantID != r.TenantID {
				continue
			}
			h := platformconfig.ManagedServingHandoff{TenantID: r.TenantID, AppID: r.AppID, ReleaseID: r.ID, OperationID: a.Status.LastOperationID}
			if _, ok := managedHandoffOperation(b, h); ok {
				candidates = append(candidates, h)
			}
		}
	}
	if len(candidates) == 0 {
		return
	}
	source, err := s.readRetainedTrafficSource(ctx, p.Intent.Scope)
	if err != nil {
		return
	}
	minimum := p.CapturedAt
	if len(budgets) > 0 && budgets[0] > 0 {
		minimum = minimum.Add(budgets[0])
	}
	var facts []platformconfig.ManagedServingHandoff
	for _, h := range candidates {
		app, _ := managedHandoffOperation(b, h)
		if !managedHandoffAuthorityEqual(source, p.Intent, p.Policy, h) || !managedHandoffSourcePositive(source, h, app.Spec.RuntimeID) {
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
}

// Admission repeats the business gate, not just the claimed runtime fact. A
// finished/replaced/failed operation or explicit selection cannot admit an old
// partition captured earlier. Normal producer retry gets a fresh projection.
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
	for _, h := range facts {
		app, ok := managedHandoffOperation(b, h)
		if !ok || !managedHandoffAuthorityEqual(source, intent, policy, h) || !managedHandoffSourcePositive(source, h, app.Spec.RuntimeID) {
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
