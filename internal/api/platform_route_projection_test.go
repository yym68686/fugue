package api

import (
	"context"
	"net/http"
	"reflect"
	"testing"
	"time"

	"fugue/internal/edgecontrol"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

// Exercise the producer API, signed persistence/LKG, component response and
// the actual Edge Control compiler, not just fields in the API projection.
func TestCompiledPlatformArtifactFeedsEdgeControl(t *testing.T) {
	_, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, platformConfigCompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "intent-projection", Routes: []platformconfig.RouteIntent{
			{Hostname: "active.example.test", UpstreamURL: "http://active:8080", Enabled: true},
			{Hostname: "disabled.example.test", UpstreamURL: "http://disabled:8080", Enabled: false},
			{Hostname: "pinned.example.test", UpstreamURL: "http://pinned:8080", Enabled: true, EdgeGroupID: "edge-group-test-a"},
		}},
		Policy: platformconfig.PolicySnapshot{Generation: "policy-projection", MinimumHealthyEdges: 2},
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("compile: %d %s", response.Code, response.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, response, &compiled)
	seedVerifiedPlatformArtifactAPI(t, server, admin, compiled.RouteArtifact.ID)
	snapshot := readArtifactRouteProjection(t, server)
	if snapshot.Generation != compiled.RouteArtifact.Generation {
		t.Fatal("projection lost the artifact generation binding")
	}
	for _, route := range snapshot.Routes {
		if route.Generation != edgeRouteIntentGeneration(route) || route.MinHealthyEdgeNodes != 2 {
			t.Fatalf("route identity or policy changed: %+v", route)
		}
	}
	now := time.Now().UTC()
	ledger := edgecontrol.NewMemoryGroupShadowLedger()
	compiler := edgecontrol.GroupShadowCompiler{Inventory: projectionInventory{now}, Ledger: ledger, Now: func() time.Time { return now }}
	batch, err := compiler.Reconcile(context.Background(), snapshot, []string{"edge-group-test-a", "edge-group-test-b"})
	if err != nil || batch.Succeeded != 2 {
		t.Fatalf("Edge Control rejected compiled artifact: %+v %v", batch, err)
	}
	for _, group := range []string{"edge-group-test-a", "edge-group-test-b"} {
		head, found, err := ledger.Head(context.Background(), group)
		if err != nil || !found || head.Bundle == nil {
			t.Fatalf("missing candidate for %s: %v", group, err)
		}
		routes := map[string]model.EdgeRouteBinding{}
		for _, route := range head.Bundle.Routes {
			routes[route.Hostname] = route
		}
		active := routes["active.example.test"]
		if active.Status != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(active.RoutePolicy) || active.UpstreamURL != "http://active:8080" {
			t.Fatalf("active route cannot serve: %+v", active)
		}
		disabled := routes["disabled.example.test"]
		if disabled.Status != model.EdgeRouteStatusDisabled || disabled.UpstreamURL != "" {
			t.Fatalf("disabled route regained an upstream: %+v", disabled)
		}
		_, pinned := routes["pinned.example.test"]
		if pinned != (group == "edge-group-test-a") {
			t.Fatalf("pinned route leaked across groups: %s %+v", group, routes)
		}
	}

	// Validation timestamps are mutable runtime facts and must not rotate the
	// semantic identities of otherwise identical routes.
	artifact := compiled.RouteArtifact
	first, err := projectPlatformRouteArtifact(artifact)
	if err != nil {
		t.Fatal(err)
	}
	artifact.UpdatedAt = artifact.UpdatedAt.Add(time.Hour)
	second, err := projectPlatformRouteArtifact(artifact)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatal("validation time changed the serving projection", err)
	}
}

type projectionInventory struct{ now time.Time }

func (p projectionInventory) ReadGroupInventory(_ context.Context, group string) (edgecontrol.GroupInventorySnapshot, error) {
	faultDomainID := "fault-domain-test"
	edgePoolID := "edge-pool-test"
	return edgecontrol.GroupInventorySnapshot{
		Schema: edgecontrol.GroupInventorySchemaV1, GroupID: group, FaultDomainID: faultDomainID, EdgePoolID: edgePoolID, Sequence: 1, Generation: "inventory-test", ObservedAt: p.now,
		ActiveEpoch: edgecontrol.GroupActiveEpoch{GroupID: group, FaultDomainID: faultDomainID, EdgePoolID: edgePoolID, Slot: model.EdgeSlotA, ReleaseEpoch: "epoch-test", FenceSequence: 1, MinHealthyInstances: 1},
		Instances:   []edgecontrol.GroupInstance{{GroupID: group, FaultDomainID: faultDomainID, EdgePoolID: edgePoolID, EdgeID: "edge-test", InstanceUID: "instance-test", Slot: model.EdgeSlotA, ReleaseEpoch: "epoch-test", EffectiveHealthy: true, NodeHealthy: true, NodeStatus: model.EdgeHealthHealthy}},
	}, nil
}

func readArtifactRouteProjection(t *testing.T, server *Server) model.EdgeRouteIntentSnapshot {
	t.Helper()
	server.auth.EdgeRouteIntentIdentityKeyring = edgeRouteIntentTestKeyring()
	token, err := platformcontrol.IssuePlatformComponentIdentity(edgeRouteIntentTestKeyring(), *edgeRouteIntentTestClaims(model.PlatformConsumerComponentEdgeControl, "global", []string{model.PlatformArtifactKindEdgeRouteIntent}), time.Now().UTC(), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	response := performJSONRequest(t, server, http.MethodGet, "/v1/edge/route-intents", token, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("route projection: %d %s", response.Code, response.Body.String())
	}
	var snapshot model.EdgeRouteIntentSnapshot
	mustDecodeJSON(t, response, &snapshot)
	if response.Header().Get("X-Fugue-Route-Intent-Generation") != snapshot.Generation {
		t.Fatal("transport identity mismatch")
	}
	if response.Header().Get("X-Fugue-Route-Intent-Source") != "verified-artifact" {
		t.Fatalf("route intent source is not explicit: %q", response.Header().Get("X-Fugue-Route-Intent-Source"))
	}
	return snapshot
}

func TestPlatformRouteProjectionRejectsAmbiguousPayload(t *testing.T) {
	for name, content := range map[string]map[string]any{
		"missing routes":     {},
		"unknown schema":     {"schema_version": "future", "routes": []any{}},
		"legacy rich route":  {"routes": []any{map[string]any{"hostname": "legacy.example.test", "route_policy": "edge_enabled"}}},
		"implicit enabled":   {"routes": []any{map[string]any{"hostname": "missing.example.test", "upstream_url": "http://origin"}}},
		"invalid upstream":   {"routes": []any{map[string]any{"hostname": "invalid.example.test", "enabled": true, "upstream_url": "origin"}}},
		"invalid group":      {"routes": []any{map[string]any{"hostname": "invalid.example.test", "enabled": true, "upstream_url": "http://origin", "edge_group_id": "bad group"}}},
		"duplicate hostname": {"routes": []any{map[string]any{"hostname": "DUP.example.test.", "enabled": false}, map[string]any{"hostname": "dup.example.test", "enabled": false}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := projectPlatformRouteArtifact(model.PlatformArtifact{Content: content}); err == nil {
				t.Fatal("ambiguous payload was accepted")
			}
		})
	}
}

func TestInvalidRouteLKGDoesNotFallBackToBusinessProjection(t *testing.T) {
	_, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, platformConfigCompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "intent-invalid-lkg", Routes: []platformconfig.RouteIntent{{Hostname: "route.example.test", Enabled: true, UpstreamURL: "http://origin"}}},
		Policy: platformconfig.PolicySnapshot{Generation: "policy-invalid-lkg"},
	})
	if response.Code != http.StatusCreated {
		t.Fatal(response.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, response, &compiled)
	seedVerifiedPlatformArtifactAPI(t, server, admin, compiled.RouteArtifact.ID)
	server.bundleRevokedKeyIDs = append(server.bundleRevokedKeyIDs, compiled.RouteArtifact.Provenance.KeyID)
	_, found, err := server.edgeRouteIntentSnapshotFromVerifiedArtifact()
	if err == nil || !found {
		t.Fatal("invalid LKG was treated as absence of configuration")
	}
	server.auth.EdgeRouteIntentIdentityKeyring = edgeRouteIntentTestKeyring()
	token, err := platformcontrol.IssuePlatformComponentIdentity(edgeRouteIntentTestKeyring(), *edgeRouteIntentTestClaims(model.PlatformConsumerComponentEdgeControl, "global", []string{model.PlatformArtifactKindEdgeRouteIntent}), time.Now().UTC(), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	failed := performJSONRequest(t, server, http.MethodGet, "/v1/edge/route-intents", token, nil)
	if failed.Code != http.StatusServiceUnavailable {
		t.Fatalf("invalid LKG fell back to mutable sources: %d %s", failed.Code, failed.Body.String())
	}
}

func TestCompiledRoutePathsPreservePortAndExplicitStreaming(t *testing.T) {
	_, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	streaming := false
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, platformConfigCompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "route-paths", Routes: []platformconfig.RouteIntent{
			{Hostname: "app.example.test", PathPrefix: "/api", UpstreamURL: "http://api:9000", ServicePort: 9000, Enabled: true, Streaming: &streaming},
			{Hostname: "app.example.test", UpstreamURL: "http://web:8080", Enabled: true},
		}}, Policy: platformconfig.PolicySnapshot{Generation: "route-path-policy"},
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("compile: %d %s", response.Code, response.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, response, &compiled)
	projection, err := projectPlatformRouteArtifact(compiled.RouteArtifact)
	if err != nil || len(projection.Routes) != 2 {
		t.Fatalf("projection: %+v %v", projection, err)
	}
	root, apiRoute := projection.Routes[0], projection.Routes[1]
	if root.PathPrefix != "/" || !root.Streaming || root.ServicePort != 0 || apiRoute.PathPrefix != "/api" || apiRoute.Streaming || apiRoute.ServicePort != 9000 {
		t.Fatalf("path/port/streaming lost: %+v", projection.Routes)
	}
	for _, route := range projection.Routes {
		if route.Generation != edgeRouteIntentGeneration(route) {
			t.Fatal("path route generation does not bind all semantics")
		}
	}
	now := time.Now().UTC()
	ledger := edgecontrol.NewMemoryGroupShadowLedger()
	compiler := edgecontrol.GroupShadowCompiler{Inventory: projectionInventory{now}, Ledger: ledger, Now: func() time.Time { return now }}
	batch, err := compiler.Reconcile(context.Background(), projection, []string{"edge-group-test-a"})
	if err != nil || batch.Succeeded != 1 {
		t.Fatalf("Edge Control failed to materialize paths: %+v %v", batch, err)
	}
	head, found, err := ledger.Head(context.Background(), "edge-group-test-a")
	if err != nil || !found || head.Bundle == nil || len(head.Bundle.Routes) != 2 {
		t.Fatalf("Edge Control lost a path: %+v %v", head, err)
	}
	for _, route := range head.Bundle.Routes {
		if route.PathPrefix == "/api" && (route.ServicePort != 9000 || route.Streaming || route.UpstreamURL != "http://api:9000") {
			t.Fatalf("materialized semantics changed: %+v", route)
		}
	}
}

func TestCompiledWeightedUpstreamsProjectWithoutRuntimeFacts(t *testing.T) {
	_, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, platformConfigCompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "weighted-route", Routes: []platformconfig.RouteIntent{{
			Hostname: "weighted.example.test", UpstreamURL: "http://stable:8080", Enabled: true,
			Upstreams: []platformconfig.UpstreamIntent{
				{Role: "stable", ReleaseID: "release-stable", Weight: 80, UpstreamURL: "http://stable:8080", ServicePort: 8080},
				{Role: "canary", ReleaseID: "release-canary", Weight: 20, UpstreamURL: "http://canary:8080", ServicePort: 8080},
			},
		}}},
		Policy: platformconfig.PolicySnapshot{Generation: "weighted-route-policy"},
	})
	if response.Code != http.StatusCreated {
		t.Fatalf("compile: %d %s", response.Code, response.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, response, &compiled)
	projection, err := projectPlatformRouteArtifact(compiled.RouteArtifact)
	if err != nil || len(projection.Routes) != 1 || len(projection.Routes[0].Upstreams) != 2 {
		t.Fatalf("weighted projection: %+v %v", projection, err)
	}
	upstreams := projection.Routes[0].Upstreams
	if upstreams[0].ReleaseID != "release-stable" || upstreams[0].Weight != 80 || upstreams[1].ReleaseID != "release-canary" || upstreams[1].Weight != 20 || upstreams[0].Status != "" {
		t.Fatalf("weighted intent mixed runtime facts or changed order: %+v", upstreams)
	}
}

func TestCompiledOriginObservationPreservesDisabledTrafficAndIdentity(t *testing.T) {
	_, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	captured := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, status := range []string{model.EdgeRouteStatusActive, model.EdgeRouteStatusUnavailable, model.EdgeRouteStatusDisabled} {
		t.Run(status, func(t *testing.T) {
			response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, platformConfigCompileRequest{
				Intent: platformconfig.PlatformIntent{Generation: "origin-test-intent", Routes: []platformconfig.RouteIntent{{
					Hostname: "origin.example.test", AppID: "app-test", TenantID: "tenant-test", RuntimeID: "runtime-test", OriginRef: "origin-test",
					UpstreamURL: "http://service:8080", Enabled: true,
					Upstreams: []platformconfig.UpstreamIntent{{Weight: 100, UpstreamURL: "http://service:8080"}},
				}}},
				Policy: platformconfig.PolicySnapshot{Generation: "origin-test-policy", MaxStaleSeconds: 60},
				RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &captured, Origins: []platformconfig.OriginObservation{{
					Ref: "origin-test", ObservedAt: captured, Status: status, RuntimeID: "runtime-test", RuntimeType: "managed", RuntimeEdgeGroupID: "edge-group-test", RuntimeClusterNode: "node-test",
				}}},
			})
			if response.Code != http.StatusCreated {
				t.Fatalf("compile: %d %s", response.Code, response.Body.String())
			}
			var compiled platformConfigCompileResponse
			mustDecodeJSON(t, response, &compiled)
			projection, err := projectPlatformRouteArtifact(compiled.RouteArtifact)
			if err != nil || len(projection.Routes) != 1 {
				t.Fatalf("projection: %+v %v", projection, err)
			}
			route := projection.Routes[0]
			if route.AppID != "app-test" || route.TenantID != "tenant-test" || route.RuntimeID != "runtime-test" || route.RuntimeClusterNode != "node-test" || route.RuntimeEdgeGroupID != "edge-group-test" {
				t.Fatalf("origin identity lost: %+v", route)
			}
			if route.OriginStatus != status {
				t.Fatalf("origin status lost: %+v", route)
			}
			if status == model.EdgeRouteStatusActive {
				if route.UpstreamURL != "http://service:8080" || len(route.Upstreams) != 1 {
					t.Fatal("active origin cannot serve")
				}
			} else if route.UpstreamURL != "" || len(route.Upstreams) != 0 {
				t.Fatal("unavailable/disabled origin regained traffic")
			}
			if routes := compiled.IntentArtifact.Content["routes"].([]any); routes[0].(map[string]any)["status"] != nil {
				t.Fatal("origin status was stored as intent")
			}
		})
	}
}
