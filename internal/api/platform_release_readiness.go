package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/releaseproof"
	"fugue/internal/store"
)

// This is compiler input evidence, not an assertion that the release is serving.
// Store only resource identities and readiness, never a workload's environment.
type releaseRuntimeReadiness = releaseproof.Readiness

func (s *Server) captureReleaseRuntimeReadiness(ctx context.Context, projection *platformIntentProjectionResponse, business store.RouteBusinessSnapshot, retentionBudgets ...time.Duration) error {
	if len(projection.RuntimeSnapshot.Releases) == 0 {
		return nil
	}
	apps := make(map[string]model.App, len(business.Apps))
	for _, a := range business.Apps {
		apps[a.ID] = a
	}
	runtimes := make(map[string]model.Runtime, len(business.Runtimes))
	for _, r := range business.Runtimes {
		runtimes[r.ID] = r
	}
	releases := make(map[string]model.AppRelease, len(business.Releases))
	for _, r := range business.Releases {
		releases[r.ID] = r
	}
	stable := map[string]string{}
	for _, p := range business.TrafficPolicies {
		stable[p.AppID] = p.StableReleaseID
	}
	managed := func(r model.AppRelease) bool {
		if r.RuntimeID == model.DefaultManagedRuntimeID {
			return true
		}
		v, ok := runtimes[r.RuntimeID]
		return ok && (v.Type == model.RuntimeTypeManagedOwned || v.Type == model.RuntimeTypeManagedShared)
	}
	required := false
	for _, r := range projection.RuntimeSnapshot.Releases {
		if managed(releases[r.ID]) {
			required = true
			break
		}
	}
	if !required {
		return nil
	}
	client, err := s.managedAppStatusClient()
	if err != nil {
		return fmt.Errorf("release readiness observer unavailable: %w", err)
	}
	defer client.closeIdleConnections()
	cluster, err := client.getClusterID(ctx)
	if err != nil {
		return fmt.Errorf("release readiness cluster unavailable: %w", err)
	}
	observedAt := time.Now().UTC()
	snapshot, err := client.readRuntimeSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("release readiness snapshot unavailable: %w", err)
	}
	if err := client.captureReleaseEndpointOwners(ctx, &snapshot); err != nil {
		return fmt.Errorf("release endpoint ownership unavailable: %w", err)
	}
	confirmed, err := client.getClusterID(ctx)
	if err != nil || cluster == "" || confirmed != cluster {
		return fmt.Errorf("release readiness cluster identity changed or unavailable")
	}
	proofs := []releaseRuntimeReadiness{}
	for i, old := range projection.RuntimeSnapshot.Releases {
		release := releases[old.ID]
		if !managed(release) {
			continue
		}
		app := apps[release.AppID]
		fact, proof := s.observeReleaseRuntimeReadiness(app, release, stable[release.AppID] == release.ID, snapshot, observedAt)
		// The route graph, release ids and desired weights are already frozen.
		// Only independently captured runtime facts may be replaced here.
		projection.RuntimeSnapshot.Releases[i] = fact
		proofs = append(proofs, proof)
	}
	raw, err := json.Marshal(struct {
		Schema     string                    `json:"schema"`
		ClusterID  string                    `json:"cluster_id"`
		ObservedAt time.Time                 `json:"observed_at"`
		Releases   []releaseRuntimeReadiness `json:"releases"`
	}{"fugue.release-runtime-readiness/v1", cluster, observedAt, proofs})
	if err != nil {
		return err
	}
	var value map[string]any
	if err = json.Unmarshal(raw, &value); err != nil {
		return err
	}
	if projection.RuntimeSnapshot.Facts == nil {
		projection.RuntimeSnapshot.Facts = map[string]any{}
	}
	projection.RuntimeSnapshot.Facts["release_readiness"] = value
	projection.CapturedAt = time.Now().UTC()
	projection.RuntimeSnapshot.CapturedAt = &projection.CapturedAt
	if err := s.guardPendingStableHandoffs(ctx, *projection, business); err != nil {
		return err
	}
	s.captureRetainedTrafficPartitions(ctx, projection, retentionBudgets...)
	if err := projectReadyReleaseOrigins(projection, proofs); err != nil {
		return releaseReadinessDiagnostic(err, proofs)
	}
	return nil
}

// A failed strict capture cannot return its runtime snapshot. Preserve the
// failure and expose bounded factual resource diagnostics instead of making an
// operator guess which independently observed release was unavailable.
func releaseReadinessDiagnostic(cause error, proofs []releaseRuntimeReadiness) error {
	items := make([]string, 0, 8)
	for _, proof := range proofs {
		if proof.Ready {
			continue
		}
		items = append(items, fmt.Sprintf("release_id=%s reason=%s", proof.ReleaseID, proof.Reason))
		if len(items) == 8 {
			break
		}
	}
	if len(items) == 0 {
		return cause
	}
	return fmt.Errorf("%w; unready runtime observations: %s", cause, strings.Join(items, "; "))
}

func (s *Server) observeReleaseRuntimeReadiness(app model.App, release model.AppRelease, stable bool, snapshot managedAppKubeSnapshot, observedAt time.Time) (platformconfig.ReleaseObservation, releaseRuntimeReadiness) {
	return releaseproof.Observe(app, release, stable, releaseproof.Snapshot{
		Namespaces: snapshot.namespaces, Deployments: snapshot.deployments,
		Services: snapshot.serviceDetails, EndpointSlicesAvailable: snapshot.endpointSlicesAvailable,
		EndpointSlices: snapshot.releaseEndpointSlices, Pods: snapshot.releasePods, ReplicaSets: snapshot.releaseReplicaSets,
	}, observedAt, s.observedRuntimeImageRefsEquivalent)
}

// The canonical workload may still be draining an earlier code version. A
// release-owned route instead obtains origin readiness from the exact workloads
// selected by its frozen traffic policy. This changes only captured facts;
// explicit intent, policy, release weights and edge publication gates stay intact.
func projectReadyReleaseOrigins(projection *platformIntentProjectionResponse, proofs []releaseRuntimeReadiness) error {
	ready := make(map[string]bool, len(proofs))
	for _, proof := range proofs {
		ready[proof.ReleaseID] = proof.Ready
	}
	routes := make([]platformconfig.CompiledRoute, 0, len(projection.Intent.Routes))
	for _, route := range projection.Intent.Routes {
		routes = append(routes, platformconfig.CompiledRoute{RouteIntent: route})
	}
	routes, err := platformconfig.ApplyRoutePolicyConstraints(routes, projection.Policy, projection.RuntimeSnapshot.CapturedAt)
	if err != nil {
		return err
	}
	routes, err = platformconfig.ApplyTrafficPolicyConstraints(routes, projection.Policy, projection.RuntimeSnapshot)
	if err != nil {
		return err
	}
	observed := make(map[string]time.Time, len(projection.RuntimeSnapshot.Releases))
	for _, release := range projection.RuntimeSnapshot.Releases {
		observed[release.ID] = release.ObservedAt
	}
	origins := make(map[string]time.Time)
	for _, route := range routes {
		if route.OriginRef == "" || len(route.Upstreams) == 0 {
			continue
		}
		valid := true
		var oldest time.Time
		for _, upstream := range route.Upstreams {
			valid = valid && ready[upstream.ReleaseID] && upstream.RuntimeID == route.RuntimeID
			if at := observed[upstream.ReleaseID]; oldest.IsZero() || at.Before(oldest) {
				oldest = at
			}
		}
		if valid && !oldest.IsZero() {
			origins[route.OriginRef] = oldest
		}
	}
	for i := range projection.RuntimeSnapshot.Origins {
		origin := &projection.RuntimeSnapshot.Origins[i]
		if at, ok := origins[origin.Ref]; ok {
			origin.ObservedAt = at
			origin.Status = model.EdgeRouteStatusActive
			origin.StatusReason = ""
		}
	}
	return nil
}
