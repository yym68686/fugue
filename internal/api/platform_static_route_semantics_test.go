package api

import (
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"reflect"
	"testing"
	"time"
)

func TestStaticProducerProjectionRetainsPinnedGroupModeAndTTL(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, mode := range []string{"region_aware", "all_healthy", "pinned"} {
		t.Run(mode, func(t *testing.T) {
			static := model.PlatformRoute{Hostname: "entry.example.test", Kind: "platform", UpstreamURL: "http://entry:8080", UpstreamKind: "kubernetes-service", UpstreamScope: "cluster", TLSPolicy: "platform", RoutePolicy: "edge_enabled", EdgeGroupMode: mode, TTL: 180, Status: "active"}
			if mode == "pinned" {
				static.EdgeGroupID = "edge-group-a"
			}
			snapshot := model.EdgeRouteIntentSnapshot{GeneratedAt: now, Routes: []model.EdgeRouteIntent{edgeRouteIntentFromPlatformRoute(static)}}
			// The execution projection has intentionally lost region-aware and TTL.
			first, err := projectBusinessRouteDraft(snapshot, nil, nil, []model.PlatformRoute{static}, nil, nil, nil, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(first.Intent.Routes) != 1 {
				t.Fatal(first.Intent.Routes)
			}
			desired := first.Intent.Routes[0]
			if desired.EdgeGroupMode != mode || desired.EdgeGroupID != static.EdgeGroupID || desired.TTL != 180 {
				t.Fatal("pinned intent semantics lost", desired)
			}
			compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: first.Intent, Policy: first.Policy, RuntimeSnapshot: first.RuntimeSnapshot})
			if err != nil {
				t.Fatal(err)
			}
			actual, err := discoveryPlatformRouteSummary(compiled.RouteArtifact, []model.PlatformRoute{static})
			if err != nil || len(actual) != 1 {
				t.Fatal(actual, err)
			}
			if actual[0].EdgeGroupMode != mode || actual[0].EdgeGroupID != static.EdgeGroupID || actual[0].TTL != 180 {
				t.Fatal("compiled public summary differs", actual)
			}
			again, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: first.Intent, Policy: first.Policy, RuntimeSnapshot: first.RuntimeSnapshot})
			firstDigest, firstErr := platformconfig.Digest(compiled.RouteArtifact.Content)
			againDigest, againErr := platformconfig.Digest(again.RouteArtifact.Content)
			if err != nil || firstErr != nil || againErr != nil || firstDigest == "" || againDigest != firstDigest {
				t.Fatal("fixed input is not deterministic", err)
			}
			changed := static
			changed.TTL = 300
			next, err := projectBusinessRouteDraft(snapshot, nil, nil, []model.PlatformRoute{changed}, nil, nil, nil, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if next.Intent.Generation == first.Intent.Generation || next.Intent.Routes[0].TTL != 300 || !reflect.DeepEqual(first.Policy, next.Policy) {
				t.Fatal("TTL update did not version intent independently", next.Intent)
			}
			// A business app owning the same hostname must not inherit static placement
			// metadata merely because it appears in the signed base's platform list.
			owned := snapshot
			owned.Routes = append([]model.EdgeRouteIntent(nil), snapshot.Routes...)
			owned.Routes[0].AppID = "app"
			owned.Routes[0].TenantID = "tenant"
			owned.Routes[0].ServicePort = 8080
			owned.Routes[0].TargetGroupMode = model.EdgeRouteIntentGroupModeAllGroups
			owned.Routes[0].PinnedEdgeGroupID = ""
			apps := map[string]model.App{"app": {ID: "app", TenantID: "tenant", Spec: model.AppSpec{RuntimeID: "runtime", Replicas: 1}}}
			business, err := projectBusinessRouteDraft(owned, apps, apps, []model.PlatformRoute{changed}, nil, nil, nil, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if business.Intent.Routes[0].TTL != 0 || business.Intent.Routes[0].EdgeGroupMode != "all_healthy" {
				b, _ := json.Marshal(business.Intent.Routes[0])
				t.Fatal("static fields leaked into business-owned intent", string(b))
			}
		})
	}
}
