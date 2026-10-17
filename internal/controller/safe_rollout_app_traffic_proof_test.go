package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
	"fugue/internal/store"
)

type safeRolloutTrafficStoreFixture struct {
	policy   model.AppTrafficPolicy
	releases []model.AppRelease
}

func (f safeRolloutTrafficStoreFixture) GetAppTrafficPolicy(string, bool, string) (model.AppTrafficPolicy, error) {
	return f.policy, nil
}

func (f safeRolloutTrafficStoreFixture) ListAppReleaseMetadata(model.AppReleaseFilter) ([]model.AppRelease, error) {
	releases := append([]model.AppRelease(nil), f.releases...)
	for i := range releases {
		releases[i].SpecSnapshot = nil
	}
	return releases, nil
}

func appTrafficObserverFixture(t *testing.T, now time.Time) (storeSafeRolloutEdgeBundleObserver, model.App, model.AppRelease, model.EdgeNode, routeprobe.Proof) {
	t.Helper()
	heartbeat := now.Add(-time.Second)
	app := model.App{ID: "app", TenantID: "tenant", Route: &model.AppRoute{Hostname: "app.example.test", PathPrefix: "/", ServicePort: 8080}}
	candidate := model.AppRelease{ID: "candidate", AppID: app.ID, TenantID: app.TenantID}
	stable := model.AppRelease{ID: "stable", AppID: app.ID, TenantID: app.TenantID, UpstreamURL: "http://stable:8080", RuntimeID: "runtime-stable", ResolvedImageRef: "stable-image"}
	candidate.UpstreamURL, candidate.RuntimeID, candidate.ResolvedImageRef = "http://candidate:8080", "runtime-candidate", "candidate-image"
	fixture := safeRolloutTrafficStoreFixture{policy: model.AppTrafficPolicy{Mode: model.AppTrafficModeCanary, AppID: app.ID, TenantID: app.TenantID, StableReleaseID: stable.ID, CandidateReleaseID: candidate.ID, StableWeight: 80, CandidateWeight: 20, StickyCookie: "Fugue-Release-Stickiness"}, releases: []model.AppRelease{stable, candidate}}
	route := model.EdgeRouteBinding{Hostname: app.Route.Hostname, PathPrefix: app.Route.PathPrefix, AppID: app.ID, TenantID: app.TenantID, Upstreams: []model.EdgeRouteUpstream{
		{Role: model.AppReleaseRoleStable, ReleaseID: stable.ID, Weight: 80, UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService, UpstreamURL: stable.UpstreamURL, ServicePort: 8080, RuntimeID: stable.RuntimeID, DeploymentGeneration: stable.ResolvedImageRef},
		{Role: model.AppReleaseRoleCandidate, ReleaseID: candidate.ID, Weight: 20, UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService, UpstreamURL: candidate.UpstreamURL, ServicePort: 8080, RuntimeID: candidate.RuntimeID, DeploymentGeneration: candidate.ResolvedImageRef},
	}}
	digest, err := routeproof.AppTrafficDigest(route)
	if err != nil {
		t.Fatal(err)
	}
	base := model.EdgeNode{ID: "edge-1", EdgeGroupID: "group-1", Healthy: true, Status: model.EdgeHealthHealthy, CaddyRouteCount: 10, RouteBundleVersion: "bundle-1", CaddyAppliedVersion: "bundle-1", PublicIPv4: "203.0.113.9", LastHeartbeatAt: &heartbeat}
	proof := routeprobe.Proof{Digest: "sha256:" + strings.Repeat("b", 64), AppTrafficDigest: digest, Version: base.RouteBundleVersion, EdgeID: base.ID, GroupID: base.EdgeGroupID, CheckedAt: now, ValidUntil: now.Add(time.Minute)}
	observer := storeSafeRolloutEdgeBundleObserver{Store: staticEdgeNodeLister{nodes: []model.EdgeNode{base}}, Traffic: &fixture, Now: func() time.Time { return now }, Probe: func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		return proof, nil
	}}
	return observer, app, candidate, base, proof
}

func TestSafeRolloutEdgeObserverRequiresExactApplicationTrafficProof(t *testing.T) {
	now := time.Now().UTC()
	for name, mutate := range map[string]func(*routeprobe.Proof){
		"matching proof":     func(*routeprobe.Proof) {},
		"wrong weights":      func(p *routeprobe.Proof) { p.AppTrafficDigest = "sha256:" + strings.Repeat("a", 64) },
		"missing proof":      func(p *routeprobe.Proof) { p.AppTrafficDigest = "" },
		"wrong edge":         func(p *routeprobe.Proof) { p.EdgeID = "other" },
		"wrong group":        func(p *routeprobe.Proof) { p.GroupID = "other" },
		"wrong bundle":       func(p *routeprobe.Proof) { p.Version = "old-bundle" },
		"expired":            func(p *routeprobe.Proof) { p.ValidUntil = now },
		"old observation":    func(p *routeprobe.Proof) { p.CheckedAt = now.Add(-time.Second) },
		"future observation": func(p *routeprobe.Proof) { p.CheckedAt = now.Add(time.Second) },
		"inactive route":     func(p *routeprobe.Proof) { p.State = "disabled" },
	} {
		t.Run(name, func(t *testing.T) {
			observer, app, release, _, proof := appTrafficObserverFixture(t, now)
			mutate(&proof)
			observer.Probe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				return proof, nil
			}
			observation, err := observer.observe(context.Background(), app, release, 20, now.Add(-time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if observation.Ready != (name == "matching proof") {
				t.Fatalf("observation=%+v", observation)
			}
		})
	}
}

func TestSafeRolloutEdgeObserverDoesNotDropUnhealthyMembers(t *testing.T) {
	now := time.Now().UTC()
	for name, mutate := range map[string]func(*model.EdgeNode){
		"unhealthy":           func(n *model.EdgeNode) { n.Healthy = false },
		"status":              func(n *model.EdgeNode) { n.Status = model.EdgeHealthUnhealthy },
		"old heartbeat":       func(n *model.EdgeNode) { old := now.Add(-3 * time.Minute); n.LastHeartbeatAt = &old },
		"missing heartbeat":   func(n *model.EdgeNode) { n.LastHeartbeatAt = nil },
		"future heartbeat":    func(n *model.EdgeNode) { future := now.Add(time.Hour); n.LastHeartbeatAt = &future },
		"no route generation": func(n *model.EdgeNode) { n.RouteBundleVersion = ""; n.CaddyAppliedVersion = ""; n.CaddyRouteCount = 0 },
		"missing caddy apply": func(n *model.EdgeNode) { n.CaddyAppliedVersion = "" },
		"old caddy apply":     func(n *model.EdgeNode) { n.CaddyAppliedVersion = "old" },
	} {
		t.Run(name, func(t *testing.T) {
			o, a, r, node, _ := appTrafficObserverFixture(t, now)
			mutate(&node)
			o.Store = staticEdgeNodeLister{nodes: []model.EdgeNode{node}}
			got, err := o.observe(context.Background(), a, r, 20, now.Add(-time.Minute))
			if err != nil || got.Ready || got.RequiredNodes != 1 || got.ReadyNodes != 0 || len(got.WaitingNodes) != 1 {
				t.Fatalf("missing failure: %+v %v", got, err)
			}
		})
	}
	o, a, r, _, _ := appTrafficObserverFixture(t, now)
	o.Store = staticEdgeNodeLister{}
	got, err := o.observe(context.Background(), a, r, 20, now.Add(-time.Minute))
	if err != nil || got.Ready {
		t.Fatalf("empty inventory passed: %+v %v", got, err)
	}
	o.Traffic = nil
	if got, err = o.observe(context.Background(), a, r, 20, now); err == nil || got.Ready {
		t.Fatal("missing traffic source passed")
	}
}

type changingEdgeNodeLister struct{ nodes []model.EdgeNode }

func (f *changingEdgeNodeLister) ListActiveEdgeNodes(string) ([]model.EdgeNode, []model.EdgeGroup, error) {
	return f.nodes, nil, nil
}

func TestSafeRolloutWaitRetainsMissingAndReassignedNodes(t *testing.T) {
	for _, mode := range []string{"missing", "draining", "group changed", "recovered"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().UTC()
			o, a, r, node, proof := appTrafficObserverFixture(t, now)
			inventory := &changingEdgeNodeLister{nodes: []model.EdgeNode{node}}
			o.Store = inventory
			o.Now = func() time.Time { return now }
			o.Timeout = time.Second
			o.Interval = time.Millisecond
			polls := 0
			o.Probe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				polls++
				if polls == 1 {
					return routeprobe.Proof{}, fmt.Errorf("not applied yet")
				}
				proof.CheckedAt = now
				return proof, nil
			}
			o.Sleep = func(context.Context, time.Duration) error {
				now = now.Add(time.Second)
				switch mode {
				case "missing":
					inventory.nodes = nil
				case "draining":
					inventory.nodes[0].Draining = true
				case "group changed":
					inventory.nodes[0].EdgeGroupID = "other"
				}
				return nil
			}
			got, err := o.WaitForSafeRolloutEdgeRouteBundle(context.Background(), a, r, 20, now.Add(-time.Minute))
			if err != nil || got.RequiredNodes != 1 || got.Ready != (mode == "recovered") {
				t.Fatalf("lost member expectation: %+v %v", got, err)
			}
		})
	}
}

func TestSafeRolloutWaitRejectsMovingTrafficTarget(t *testing.T) {
	for _, when := range []string{"before", "during probe", "between polls"} {
		t.Run(when, func(t *testing.T) {
			now := time.Now().UTC()
			o, a, r, _, proof := appTrafficObserverFixture(t, now)
			fixture := o.Traffic.(*safeRolloutTrafficStoreFixture)
			mutate := func() { fixture.releases[1].UpstreamURL = "http://other:8080" }
			if when == "before" {
				mutate()
			}
			o.Probe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				if when == "during probe" {
					mutate()
				}
				if when == "between polls" {
					return routeprobe.Proof{}, fmt.Errorf("not yet")
				}
				return proof, nil
			}
			o.Sleep = func(context.Context, time.Duration) error { mutate(); return nil }
			got, err := o.WaitForSafeRolloutEdgeRouteBundle(context.Background(), a, r, 20, now.Add(-time.Minute))
			if err == nil || got.Ready {
				t.Fatalf("moving target passed: %+v %v", got, err)
			}
		})
	}
}

func TestSafeRolloutProbeExpiryRecheckedAfterAllNodes(t *testing.T) {
	now := time.Now().UTC()
	o, a, r, node, proof := appTrafficObserverFixture(t, now)
	o.Now = func() time.Time { return now }
	second := node
	second.ID = "edge-2"
	second.PublicIPv4 = "203.0.113.10"
	o.Store = staticEdgeNodeLister{nodes: []model.EdgeNode{node, second}}
	o.Probe = func(_ context.Context, _, _, ip, _ string, _ time.Duration) (routeprobe.Proof, error) {
		p := proof
		if ip == second.PublicIPv4 {
			now = now.Add(2 * time.Second)
			p.EdgeID = second.ID
		}
		p.CheckedAt = now
		p.ValidUntil = now.Add(time.Second)
		return p, nil
	}
	got, err := o.observe(context.Background(), a, r, 20, now.Add(-time.Minute))
	if err != nil || got.Ready || got.ReadyNodes != 1 || len(got.WaitingNodes) != 1 {
		t.Fatalf("expired earlier proof accepted: %+v %v", got, err)
	}
}

func TestSafeRolloutTopologyChangeDuringProbesBlocksSuccess(t *testing.T) {
	for _, mode := range []string{"removed", "added", "unhealthy", "bundle changed"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().UTC()
			o, a, r, node, proof := appTrafficObserverFixture(t, now)
			inventory := &changingEdgeNodeLister{nodes: []model.EdgeNode{node}}
			o.Store = inventory
			o.Probe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				switch mode {
				case "removed":
					inventory.nodes = nil
				case "added":
					second := node
					second.ID = "edge-2"
					inventory.nodes = append(inventory.nodes, second)
				case "unhealthy":
					inventory.nodes[0].Healthy = false
				case "bundle changed":
					inventory.nodes[0].RouteBundleVersion = "changed"
				}
				return proof, nil
			}
			got, err := o.observe(context.Background(), a, r, 20, now.Add(-time.Minute))
			if err != nil || got.Ready || len(got.WaitingNodes) != 1 {
				t.Fatalf("topology change ignored: %+v %v", got, err)
			}
		})
	}
}

func TestSafeRolloutEdgeWaitHonorsCancellation(t *testing.T) {
	now := time.Now().UTC()
	o, a, r, _, _ := appTrafficObserverFixture(t, now)
	o.Timeout = 10 * time.Millisecond
	o.Probe = func(ctx context.Context, _, _, _, _ string, _ time.Duration) (routeprobe.Proof, error) {
		<-ctx.Done()
		return routeprobe.Proof{}, ctx.Err()
	}
	started := time.Now()
	got, err := o.WaitForSafeRolloutEdgeRouteBundle(context.Background(), a, r, 20, now.Add(-time.Minute))
	if err == nil || got.Ready || time.Since(started) > time.Second {
		t.Fatalf("wait did not honor deadline: %+v %v", got, err)
	}
}

// This opt-in test observes the real HTTPS protocol without changing a release.
// The input contains app/traffic/release metadata and an Edge inventory snapshot;
// it must be captured immediately before the test, without workload env/secrets.
func TestSafeRolloutLiveAppTrafficProof(t *testing.T) {
	path := os.Getenv("FUGUE_APP_TRAFFIC_PROOF_INPUT")
	if path == "" {
		t.Skip("set FUGUE_APP_TRAFFIC_PROOF_INPUT to a fresh metadata-only snapshot")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var input struct {
		App      model.App              `json:"app"`
		Policy   model.AppTrafficPolicy `json:"policy"`
		Releases []model.AppRelease     `json:"releases"`
		Nodes    []model.EdgeNode       `json:"nodes"`
		Weight   int                    `json:"candidate_weight"`
		Machines []model.Machine        `json:"machines,omitempty"`
	}
	if err = json.Unmarshal(raw, &input); err != nil {
		t.Fatal(err)
	}
	targetID := input.Policy.StableReleaseID
	if input.Weight > 0 {
		targetID = input.Policy.CandidateReleaseID
	}
	var target model.AppRelease
	for _, r := range input.Releases {
		if r.ID == targetID {
			target = r
		}
	}
	observer := storeSafeRolloutEdgeBundleObserver{Store: staticEdgeNodeLister{nodes: input.Nodes}, Traffic: safeRolloutTrafficStoreFixture{policy: input.Policy, releases: input.Releases}}
	if input.Machines != nil {
		observer.Membership = &rolloutMembershipFixture{machines: input.Machines}
	}
	got, err := observer.observe(context.Background(), input.App, target, input.Weight, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	result, _ := json.Marshal(got.Summary)
	t.Log(string(result))
	if !got.Ready || got.RequiredNodes == 0 || got.ReadyNodes != got.RequiredNodes {
		t.Fatal("live Edge app traffic proof did not match release metadata")
	}
}

type rolloutMembershipFixture struct {
	machines []model.Machine
	err      error
}

func (f *rolloutMembershipFixture) ListMachines(string, bool) ([]model.Machine, error) {
	return f.machines, f.err
}

func TestSafeRolloutMembershipUsesExplicitPolicyNotHeartbeatHistory(t *testing.T) {
	now := time.Now().UTC()
	for _, scenario := range []string{"disabled history", "missing declared", "unhealthy declared", "unknown policy", "ambiguous policy", "policy read failed", "disabled during wait"} {
		t.Run(scenario, func(t *testing.T) {
			o, a, r, node, proof := appTrafficObserverFixture(t, now)
			old := node
			old.ID = "edge-old"
			old.Healthy = false
			old.LastHeartbeatAt = nil
			inventory := &changingEdgeNodeLister{nodes: []model.EdgeNode{node, old}}
			o.Store = inventory
			policy := &rolloutMembershipFixture{machines: []model.Machine{{ClusterNodeName: node.ID, Policy: model.MachinePolicy{AllowEdge: true}}, {ClusterNodeName: old.ID, Policy: model.MachinePolicy{AllowEdge: false}}}}
			o.Membership = policy
			switch scenario {
			case "missing declared":
				inventory.nodes = inventory.nodes[1:]
			case "unhealthy declared":
				inventory.nodes[0].Healthy = false
			case "unknown policy":
				policy.machines = policy.machines[:1]
			case "ambiguous policy":
				policy.machines = append(policy.machines, policy.machines[0])
			case "policy read failed":
				policy.err = fmt.Errorf("offline")
			case "disabled during wait":
				o.Probe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
					policy.machines[0].Policy.AllowEdge = false
					return proof, nil
				}
			}
			got, err := o.observe(context.Background(), a, r, 20, now.Add(-time.Minute))
			if scenario == "disabled history" {
				if err != nil || !got.Ready || got.RequiredNodes != 1 {
					t.Fatalf("revoked Edge history blocked current membership: %+v %v", got, err)
				}
			} else if got.Ready {
				t.Fatalf("missing/invalid membership proved serving: %+v %v", got, err)
			}
		})
	}
}

type rolloutServingEdgesFixture struct{ exclude string }

func (f *rolloutServingEdgesFixture) SelectSafeRolloutServingEdges(nodes []model.EdgeNode) ([]model.EdgeNode, error) {
	out := []model.EdgeNode{}
	for _, node := range nodes {
		if node.ID != f.exclude {
			out = append(out, node)
		}
	}
	return out, nil
}

func TestSafeRolloutMembershipFollowsServingOfflineIntent(t *testing.T) {
	now := time.Now().UTC()
	o, a, r, node, _ := appTrafficObserverFixture(t, now)
	old := node
	old.ID = "offline-edge"
	old.LastHeartbeatAt = nil
	o.Store = staticEdgeNodeLister{nodes: []model.EdgeNode{node, old}}
	o.Membership = &rolloutMembershipFixture{machines: []model.Machine{{ClusterNodeName: node.ID, Policy: model.MachinePolicy{AllowEdge: true}}, {ClusterNodeName: old.ID, Policy: model.MachinePolicy{AllowEdge: true}}}}
	serving := &rolloutServingEdgesFixture{exclude: old.ID}
	o.ServingEdges = serving
	got, err := o.observe(context.Background(), a, r, 20, now.Add(-time.Minute))
	if err != nil || !got.Ready || got.RequiredNodes != 1 {
		t.Fatalf("verified offline intent ignored: %+v %v", got, err)
	}
	serving.exclude = ""
	got, err = o.observe(context.Background(), a, r, 20, now.Add(-time.Minute))
	if err != nil || got.Ready || got.RequiredNodes != 2 {
		t.Fatalf("removed hold did not restore required proof: %+v %v", got, err)
	}
}

func TestSafeRolloutHostnameExclusionRequiresAppliedAuthenticatedProofAndServingCapacity(t *testing.T) {
	for _, mode := range []string{"excluded and serving", "all excluded", "unreachable", "wrong edge", "wrong group", "expired", "unapplied exclusion", "proof has traffic"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().UTC()
			o, a, r, node, proof := appTrafficObserverFixture(t, now)
			other := node
			other.ID = "excluded-edge"
			other.PublicIPv4 = "203.0.113.10"
			o.Store = staticEdgeNodeLister{nodes: []model.EdgeNode{node, other}}
			excludedCalls := 0
			o.Probe = func(_ context.Context, host, path, ip, state string, _ time.Duration) (routeprobe.Proof, error) {
				if host != a.Route.Hostname || path != a.Route.PathPrefix {
					t.Fatal("probe target changed")
				}
				if ip == node.PublicIPv4 && mode != "all excluded" {
					return proof, nil
				}
				if state == "" {
					return routeprobe.Proof{}, fmt.Errorf("not a serving route")
				}
				if state != routeproof.StateExcluded {
					t.Fatal("incorrect exclusion protocol")
				}
				excludedCalls++
				p := proof
				p.State = routeproof.StateExcluded
				p.AppTrafficDigest = ""
				p.EdgeID = other.ID
				if ip == node.PublicIPv4 {
					p.EdgeID = node.ID
				}
				switch mode {
				case "unreachable":
					return routeprobe.Proof{}, fmt.Errorf("offline")
				case "wrong edge":
					p.EdgeID = "unexpected"
				case "wrong group":
					p.GroupID = "unexpected"
				case "expired":
					p.ValidUntil = now
				case "unapplied exclusion":
					p.Version = "unapplied"
				case "proof has traffic":
					p.AppTrafficDigest = proof.AppTrafficDigest
				}
				return p, nil
			}
			got, err := o.observe(context.Background(), a, r, 20, now.Add(-time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if excludedCalls == 0 || got.Ready != (mode == "excluded and serving") {
				t.Fatalf("mode=%s observation=%+v calls=%d", mode, got, excludedCalls)
			}
			if got.Ready && (got.Summary["serving_nodes"] != 1 || got.Summary["excluded_nodes"] != 1) {
				t.Fatalf("capacity and exclusions conflated: %+v", got)
			}
		})
	}
}

func TestSafeRolloutWaitAllowsIndependentPublicationWithoutRelaxingTrafficProof(t *testing.T) {
	now := time.Now().UTC()
	start := now
	o, a, r, node, proof := appTrafficObserverFixture(t, now)
	inventory := &changingEdgeNodeLister{nodes: []model.EdgeNode{node}}
	o.Store = inventory
	o.Now = func() time.Time { return now }
	o.Probe = func(_ context.Context, _, _, _, state string, _ time.Duration) (routeprobe.Proof, error) {
		if now.Sub(start) < 2*time.Minute || state != "" {
			return routeprobe.Proof{}, fmt.Errorf("publication still applying")
		}
		p := proof
		p.CheckedAt = now
		p.ValidUntil = now.Add(time.Minute)
		return p, nil
	}
	o.Sleep = func(context.Context, time.Duration) error {
		now = now.Add(30 * time.Second)
		heartbeat := now
		inventory.nodes[0].LastHeartbeatAt = &heartbeat
		return nil
	}
	got, err := o.WaitForSafeRolloutEdgeRouteBundle(context.Background(), a, r, 20, start.Add(-time.Minute))
	if err != nil || !got.Ready || now.Sub(start) != 2*time.Minute {
		t.Fatalf("publication must converge with its exact proof: %+v %v elapsed=%v", got, err, now.Sub(start))
	}
}

// The callback can model independently scheduled producer verification without
// substituting a stale or unverified membership list.
type changingRolloutServingEdges func([]model.EdgeNode) ([]model.EdgeNode, error)

func (f changingRolloutServingEdges) SelectSafeRolloutServingEdges(nodes []model.EdgeNode) ([]model.EdgeNode, error) {
	return f(nodes)
}

func TestSafeRolloutWaitForVerifiedServingPublication(t *testing.T) {
	for _, mode := range []string{"converges", "changes during probe", "times out", "invalid evidence", "retains earlier member"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().UTC()
			start := now
			o, a, r, node, proof := appTrafficObserverFixture(t, now)
			o.Now = func() time.Time { return now }
			o.Timeout = 3 * time.Second
			o.Interval = time.Second
			o.Membership = &rolloutMembershipFixture{machines: []model.Machine{{ClusterNodeName: node.ID, Policy: model.MachinePolicy{AllowEdge: true}}}}
			polls, probes := 0, 0
			o.ServingEdges = changingRolloutServingEdges(func(nodes []model.EdgeNode) ([]model.EdgeNode, error) {
				polls++
				if mode == "invalid evidence" {
					return nil, fmt.Errorf("invalid signature")
				}
				if (mode == "retains earlier member" || mode == "changes during probe") && polls == 1 {
					return nodes, nil
				}
				if polls <= 2 || mode == "times out" {
					return nil, fmt.Errorf("awaiting ACKs: %w", store.ErrSafeRolloutServingPublicationPending)
				}
				if mode == "retains earlier member" {
					return nil, nil
				}
				return nodes, nil
			})
			o.Probe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				probes++
				if mode == "retains earlier member" {
					return routeprobe.Proof{}, fmt.Errorf("not applied yet")
				}
				p := proof
				p.CheckedAt = now
				return p, nil
			}
			o.Sleep = func(context.Context, time.Duration) error { now = now.Add(time.Second); return nil }
			got, err := o.WaitForSafeRolloutEdgeRouteBundle(context.Background(), a, r, 20, start.Add(-time.Minute))
			switch mode {
			case "converges":
				if err != nil || !got.Ready || polls != 4 || probes != 1 {
					t.Fatalf("did not await exact proof: %+v %v polls=%d probes=%d", got, err, polls, probes)
				}
			case "changes during probe":
				if err != nil || !got.Ready || polls != 4 || probes != 2 || now.Sub(start) != time.Second {
					t.Fatalf("did not repeat proof after publication switched: %+v %v polls=%d probes=%d", got, err, polls, probes)
				}
			case "times out":
				if err != nil || got.Ready || polls != 4 || probes != 0 || got.Summary["serving_publication_pending"] == nil {
					t.Fatalf("pending evidence did not fail closed within deadline: %+v %v polls=%d probes=%d", got, err, polls, probes)
				}
			case "invalid evidence":
				if err == nil || got.Ready || polls != 1 || probes != 0 {
					t.Fatalf("invalid evidence retried or accepted: %+v %v", got, err)
				}
			case "retains earlier member":
				if err != nil || got.Ready || got.RequiredNodes != 1 {
					t.Fatalf("pending publication forgot required member: %+v %v", got, err)
				}
			}
		})
	}
}
