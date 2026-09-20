package controller

import (
	"context"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
)

type safeRolloutTrafficStoreFixture struct {
	policy   model.AppTrafficPolicy
	releases []model.AppRelease
}

func (f safeRolloutTrafficStoreFixture) GetAppTrafficPolicy(string, bool, string) (model.AppTrafficPolicy, error) {
	return f.policy, nil
}

func (f safeRolloutTrafficStoreFixture) ListAppReleases(model.AppReleaseFilter) ([]model.AppRelease, error) {
	return append([]model.AppRelease(nil), f.releases...), nil
}

func TestSafeRolloutEdgeObserverRequiresExactApplicationTrafficProof(t *testing.T) {
	now := time.Now().UTC()
	heartbeat := now.Add(-time.Second)
	app := model.App{ID: "app", TenantID: "tenant", Route: &model.AppRoute{Hostname: "app.example.test", PathPrefix: "/", ServicePort: 8080}}
	candidate := model.AppRelease{ID: "candidate", AppID: app.ID, TenantID: app.TenantID}
	stable := model.AppRelease{ID: "stable", AppID: app.ID, TenantID: app.TenantID, UpstreamURL: "http://stable:8080", RuntimeID: "runtime-stable", ResolvedImageRef: "stable-image"}
	candidate.UpstreamURL, candidate.RuntimeID, candidate.ResolvedImageRef = "http://candidate:8080", "runtime-candidate", "candidate-image"
	fixture := safeRolloutTrafficStoreFixture{policy: model.AppTrafficPolicy{AppID: app.ID, TenantID: app.TenantID, StableReleaseID: stable.ID, CandidateReleaseID: candidate.ID, StableWeight: 80, CandidateWeight: 20, StickyCookie: "Fugue-Release-Stickiness"}, releases: []model.AppRelease{stable, candidate}}
	route := model.EdgeRouteBinding{Hostname: app.Route.Hostname, PathPrefix: app.Route.PathPrefix, AppID: app.ID, TenantID: app.TenantID, Upstreams: []model.EdgeRouteUpstream{
		{Role: model.AppReleaseRoleStable, ReleaseID: stable.ID, Weight: 80, UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService, UpstreamURL: stable.UpstreamURL, ServicePort: 8080, RuntimeID: stable.RuntimeID, DeploymentGeneration: stable.ResolvedImageRef},
		{Role: model.AppReleaseRoleCandidate, ReleaseID: candidate.ID, Weight: 20, UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService, UpstreamURL: candidate.UpstreamURL, ServicePort: 8080, RuntimeID: candidate.RuntimeID, DeploymentGeneration: candidate.ResolvedImageRef},
	}}
	digest, err := routeproof.AppTrafficDigest(route)
	if err != nil {
		t.Fatal(err)
	}
	base := model.EdgeNode{ID: "edge-1", EdgeGroupID: "group-1", Healthy: true, Status: model.EdgeHealthHealthy, CaddyRouteCount: 10, RouteBundleVersion: "bundle-1", CaddyAppliedVersion: "bundle-1", PublicIPv4: "203.0.113.9", LastHeartbeatAt: &heartbeat}
	for name, mutate := range map[string]func(*routeprobe.Proof){
		"matching proof":        func(p *routeprobe.Proof) { p.AppTrafficDigest = digest },
		"wrong release weights": func(p *routeprobe.Proof) { p.AppTrafficDigest = "sha256:" + "a" + string(make([]byte, 63)) },
		"missing proof":         func(*routeprobe.Proof) {},
		"wrong edge":            func(p *routeprobe.Proof) { p.AppTrafficDigest = digest; p.EdgeID = "other" },
		"wrong bundle":          func(p *routeprobe.Proof) { p.AppTrafficDigest = digest; p.Version = "old-bundle" },
	} {
		t.Run(name, func(t *testing.T) {
			proof := routeprobe.Proof{Digest: "sha256:" + "b" + string(make([]byte, 63)), Version: "bundle-1", EdgeID: "edge-1", GroupID: "group-1", CheckedAt: now, ValidUntil: now.Add(time.Minute)}
			mutate(&proof)
			observer := storeSafeRolloutEdgeBundleObserver{Store: staticEdgeNodeLister{nodes: []model.EdgeNode{base}}, Traffic: fixture, Probe: func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				return proof, nil
			}, Now: func() time.Time { return now }}
			observation, err := observer.observe(context.Background(), app, candidate, 20, now.Add(-time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			wantReady := name == "matching proof"
			if observation.Ready != wantReady || observation.ReadyNodes != btoi(wantReady) {
				t.Fatalf("ready=%v nodes=%d want=%v: %+v", observation.Ready, observation.ReadyNodes, wantReady, observation)
			}
		})
	}
}

func btoi(v bool) int {
	if v {
		return 1
	}
	return 0
}
