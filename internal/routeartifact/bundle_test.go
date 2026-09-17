package routeartifact

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/edgecontrol"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeproof"
)

func TestMaterializedCandidateMatchesRealGroupCompiler(t *testing.T) {
	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	streaming := false
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "intent", Routes: []platformconfig.RouteIntent{
			{Hostname: "app.example.test", UpstreamURL: "http://root:8080", Enabled: true},
			{Hostname: "app.example.test", PathPrefix: "/api", AppID: "child-owner", UpstreamURL: "http://child:8081", ServicePort: 8081, Enabled: true, Streaming: &streaming, CachePolicyID: "ASSETS", CacheNamespace: "app-v1", DeploymentGeneration: "deploy-v1", RuntimeID: "runtime", OriginRef: "origin", RequestBodyPolicies: []model.EdgeRequestBodyPolicy{{Name: "upload", Methods: []string{"POST"}, Paths: []string{"/upload"}, MaxBytes: 1024, TimeoutSeconds: 10, MaxConcurrent: 2, RetryAfterSeconds: 5}}},
			{Hostname: "app.example.test", PathPrefix: "/disabled", UpstreamURL: "http://disabled", Enabled: false},
			{Hostname: "app.example.test", PathPrefix: "/offline", UpstreamURL: "http://offline", Enabled: true, Status: model.EdgeRouteStatusUnavailable},
			{Hostname: "pin.example.test", UpstreamURL: "http://pin", Enabled: true, EdgeGroupID: "edge-group-test-a", CachePolicyID: "pin", CacheNamespace: "pin-v1"},
			{Hostname: "nocache.example.test", UpstreamURL: "http://nocache", Enabled: true, CachePolicyID: "OFF"},
			{Hostname: "weighted.example.test", UpstreamURL: "http://fallback", AppID: "app-weighted", TenantID: "tenant", Enabled: true},
		}, CachePolicies: []model.CachePolicy{
			{ID: "assets", Kind: model.CachePolicyKindStaticAssets, HostnameScope: "app.example.test", PathPatterns: []string{"/api/*"}, TTLSeconds: 60},
			{ID: "pin", Kind: model.CachePolicyKindStaticAssets, HostnameScope: "pin.example.test", TTLSeconds: 90},
			{ID: "off", Kind: model.CachePolicyKindDisabled},
		}},
		Policy: platformconfig.PolicySnapshot{Generation: "policy", MinimumHealthyEdges: 2, RouteConstraints: []platformconfig.RoutePolicyConstraint{{ID: "exclude", Hostname: "app.example.test", AppID: "child-owner", RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true, MinHealthyEdgeNodes: 3, ExcludedEdgeIDs: []string{"excluded-node"}}}, TrafficConstraints: []platformconfig.TrafficPolicyConstraint{{ID: "traffic", AppID: "app-weighted", TenantID: "tenant", Mode: model.AppTrafficModeCanary, StableReleaseID: "stable", CandidateReleaseID: "candidate", StableWeight: 80, CandidateWeight: 20}}},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now,
			Origins: []platformconfig.OriginObservation{{Ref: "origin", RuntimeID: "runtime", RuntimeType: "managed", RuntimeEdgeGroupID: "edge-group-test-a", RuntimeClusterNode: "node", Status: model.EdgeRouteStatusActive, ObservedAt: now}},
			Releases: []platformconfig.ReleaseObservation{
				{ID: "stable", AppID: "app-weighted", TenantID: "tenant", RuntimeID: "runtime-stable", UpstreamURL: "http://stable", Status: model.EdgeRouteStatusActive, ObservedAt: now},
				{ID: "candidate", AppID: "app-weighted", TenantID: "tenant", RuntimeID: "runtime-canary", UpstreamURL: "http://canary", Status: model.EdgeRouteStatusActive, ObservedAt: now},
			}},
	})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(compiled.RouteArtifact)
	snapshot, err := Project(compiled.RouteArtifact)
	if err != nil {
		t.Fatal(err)
	}
	ledger := edgecontrol.NewMemoryGroupShadowLedger()
	compiler := edgecontrol.GroupShadowCompiler{Inventory: candidateInventory{now}, Ledger: ledger, Now: func() time.Time { return now }}
	groups := []string{"edge-group-test-a", "edge-group-test-b"}
	batch, err := compiler.Reconcile(context.Background(), snapshot, groups)
	if err != nil || batch.Succeeded != len(groups) {
		t.Fatalf("group compiler rejected the fixture: %+v %v", batch, err)
	}
	for _, group := range groups {
		bundle, err := MaterializeForGroup(compiled.RouteArtifact, group)
		if err != nil {
			t.Fatal(err)
		}
		head, found, err := ledger.Head(context.Background(), group)
		if err != nil || !found || head.Bundle == nil || len(bundle.Routes) != len(head.Bundle.Routes) {
			t.Fatalf("group route cardinality differs: %s %v", group, err)
		}
		if !reflect.DeepEqual(bundle.CachePolicies, head.Bundle.CachePolicies) {
			t.Fatalf("cache policies differ for %s", group)
		}
		want := map[string]string{}
		for _, route := range head.Bundle.Routes {
			want[route.Hostname+route.PathPrefix], err = routeproof.Digest(route)
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, route := range bundle.Routes {
			got, err := routeproof.Digest(route)
			if err != nil || got != want[route.Hostname+route.PathPrefix] {
				t.Fatalf("candidate differs from group compiler: %s %s %s", group, route.Hostname, route.PathPrefix)
			}
			switch route.PathPrefix {
			case "/disabled", "/offline":
				if route.Status == model.EdgeRouteStatusActive || route.UpstreamURL != "" || len(route.Upstreams) != 0 {
					t.Fatal("unavailable route regained an upstream")
				}
			case "/api":
				if route.Streaming || route.CachePolicyID != "assets" || route.RuntimeClusterNode != "node" || route.ServicePort != 8081 || len(route.RequestBodyPolicies) != 1 {
					t.Fatalf("child behavior lost: %+v", route)
				}
			default:
				if !route.Streaming {
					t.Fatal("default streaming changed")
				}
			}
			if route.Hostname == "weighted.example.test" && (len(route.Upstreams) != 2 || route.Upstreams[0].Status != model.EdgeRouteStatusActive || route.Upstreams[0].Weight != 80 || route.Upstreams[1].Weight != 20) {
				t.Fatal("weighted eligibility lost")
			}
			if route.Hostname == "nocache.example.test" && route.CachePolicyID != "" {
				t.Fatal("disabled cache remained active")
			}
			if route.EdgeGroupID != group || route.HealthyEdgeNodeCount != 0 {
				t.Fatal("candidate fabricated runtime facts")
			}
		}
		if !bundle.ValidUntil.IsZero() || bundle.Signature != "" {
			t.Fatal("candidate fabricated a lease or signature")
		}
		bundle.Routes[0].UpstreamURL = "http://mutated"
		if len(bundle.CachePolicies) > 0 {
			bundle.CachePolicies[0].ID = "mutated"
		}
	}
	after, _ := json.Marshal(compiled.RouteArtifact)
	if string(before) != string(after) {
		t.Fatal("materialization mutated the immutable artifact")
	}
}

type candidateInventory struct{ now time.Time }

func (p candidateInventory) ReadGroupInventory(_ context.Context, group string) (edgecontrol.GroupInventorySnapshot, error) {
	return edgecontrol.GroupInventorySnapshot{
		Schema: edgecontrol.GroupInventorySchemaV1, GroupID: group, FaultDomainID: "fault-domain-test", EdgePoolID: "edge-pool-test", Sequence: 1, Generation: "inventory-test", ObservedAt: p.now,
		ActiveEpoch: edgecontrol.GroupActiveEpoch{GroupID: group, FaultDomainID: "fault-domain-test", EdgePoolID: "edge-pool-test", Slot: model.EdgeSlotA, ReleaseEpoch: "epoch", FenceSequence: 1, MinHealthyInstances: 1},
		Instances:   []edgecontrol.GroupInstance{{GroupID: group, FaultDomainID: "fault-domain-test", EdgePoolID: "edge-pool-test", EdgeID: "edge-test", InstanceUID: "instance-test", Slot: model.EdgeSlotA, ReleaseEpoch: "epoch", EffectiveHealthy: true, NodeHealthy: true, NodeStatus: model.EdgeHealthHealthy}},
	}, nil
}
