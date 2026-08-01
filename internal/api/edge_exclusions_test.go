package api

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestExpiredAndLegacyExclusionsRemainFailClosedInRoutes(t *testing.T) {
	now := time.Date(2026, 8, 2, 8, 0, 0, 0, time.UTC)
	expired := now.Add(-time.Second)
	base := model.EdgeRouteBinding{
		Hostname: "api.example.test", AppID: "app", TenantID: "tenant",
		RuntimeEdgeGroupID: "edge-group-country-de", EdgeGroupID: "edge-group-country-de",
		RouteKind: model.EdgeRouteKindPlatform, RoutePolicy: model.EdgeRoutePolicyEnabled,
		Status: model.EdgeRouteStatusActive,
	}
	for _, tc := range []struct {
		name   string
		policy model.EdgeRoutePolicy
		want   string
	}{
		{"expired", model.EdgeRoutePolicy{Hostname: base.Hostname, AppID: base.AppID, TenantID: base.TenantID, ExcludedEdgeGroupIDs: []string{"edge-group-country-de"}, ExclusionOwnerDigest: "sha256:owner", ExclusionGeneration: 2, ExclusionFence: "fence", ExclusionExpiresAt: &expired, RoutePolicy: model.EdgeRoutePolicyEnabled}, model.EdgeExclusionLifecycleExpiredHold},
		{"legacy", model.EdgeRoutePolicy{Hostname: base.Hostname, AppID: base.AppID, TenantID: base.TenantID, ExcludedEdgeGroupIDs: []string{"edge-group-country-de"}, ExclusionExpiresAt: &expired, RoutePolicy: model.EdgeRoutePolicyEnabled}, model.EdgeExclusionLifecycleLegacyHold},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := applyEdgeRoutePolicy(base, map[string]model.EdgeRoutePolicy{base.Hostname: tc.policy}, map[string]bool{"edge-group-country-de": true}, map[string][]string{"edge-group-country-de": {"edge-de-1"}}, now)
			if len(got.ExcludedEdgeGroupIDs) != 1 || got.ExcludedEdgeGroupIDs[0] != "edge-group-country-de" {
				t.Fatalf("%s exclusion silently cleared: %+v", tc.want, got)
			}
			if got.Status != model.EdgeRouteStatusUnavailable {
				t.Fatalf("unsafe group remained routable: %+v", got)
			}
			if got.EdgeRedundancyStatus != "at_risk" || !strings.Contains(got.EdgeRedundancyReason, tc.want) {
				t.Fatalf("hold not explicit in route redundancy: %+v", got)
			}
		})
	}
}

func TestEdgeExclusionOwnerAuthorityCannotBeSelfReported(t *testing.T) {
	principal := model.Principal{ActorType: "api_key", ActorID: "admin-a", Scopes: map[string]struct{}{"platform.admin": {}}}
	owner := edgeExclusionPrincipalDigest(principal)
	if owner == "" || strings.Contains(owner, "admin-a") {
		t.Fatalf("owner digest leaks or is empty: %q", owner)
	}
	other := model.Principal{ActorType: "api_key", ActorID: "admin-b", Scopes: map[string]struct{}{"platform.admin": {}}}
	if edgeExclusionPrincipalCanMutate(other, edgeExclusionPrincipalDigest(other), owner) {
		t.Fatal("ordinary platform admin overrode another exclusion owner")
	}
	other.Scopes["edge.exclusion.override"] = struct{}{}
	if !edgeExclusionPrincipalCanMutate(other, edgeExclusionPrincipalDigest(other), owner) {
		t.Fatal("explicit central override was not recognized")
	}

}

func TestEdgeExclusionAPIPartialClearAndExpiredDeleteUseOwnerEvidenceCAS(t *testing.T) {
	t.Setenv("FUGUE_EDGE_EXCLUSION_CLEAR_ENABLED", "false")
	storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordHealthyEdgeForRouteTest(t, storeState, "edge-us-1", "edge-group-country-us", "15.204.94.71")
	recordHealthyEdgeForRouteTest(t, storeState, "edge-de-1", "edge-group-country-de", "51.38.126.103")
	instances, _, err := storeState.ListEdgeNodeInstances("")
	if err != nil {
		t.Fatal(err)
	}
	active := make([]model.EdgeNodeInstance, 0, 2)
	for _, instance := range instances {
		if instance.Slot == model.EdgeSlotDirect && (instance.EdgeID == "edge-us-1" || instance.EdgeID == "edge-de-1") {
			active = append(active, instance)
		}
	}
	if len(active) != 2 {
		t.Fatalf("active fixture instances = %+v", active)
	}
	activateExactEpochForAPITest(t, storeState, active...)
	enforceExactEpochForExclusionTest(t, storeState, active...)

	unknownOwner := performJSONRequest(t, server, "PUT", "/v1/edge/route-policies/"+app.Route.Hostname, platformAdminKey, map[string]any{
		"route_policy": model.EdgeRoutePolicyEnabled, "excluded_edge_ids": []string{"edge-de-1"},
		"exclusion_reason": "tls failure", "exclusion_owner_digest": "caller-controlled",
	})
	if unknownOwner.Code != 400 {
		t.Fatalf("caller owner field accepted: %d %s", unknownOwner.Code, unknownOwner.Body.String())
	}

	createdRecorder := performJSONRequest(t, server, "PUT", "/v1/edge/route-policies/"+app.Route.Hostname, platformAdminKey, map[string]any{
		"route_policy": model.EdgeRoutePolicyEnabled, "excluded_edge_ids": []string{"edge-de-1", "edge-us-1"},
		"exclusion_reason": "tls failure", "min_healthy_edge_nodes": 1,
	})
	if createdRecorder.Code != 200 {
		t.Fatalf("create exclusion: %d %s", createdRecorder.Code, createdRecorder.Body.String())
	}
	var created struct {
		Policy model.EdgeRoutePolicy `json:"policy"`
	}
	mustDecodeJSON(t, createdRecorder, &created)
	if created.Policy.ExclusionOwnerDigest == "" || created.Policy.ExclusionGeneration != 1 || created.Policy.ExclusionFence == "" {
		t.Fatalf("server did not bind authenticated owner/CAS: %+v", created.Policy)
	}
	defaultOff := performJSONRequest(t, server, "PUT", "/v1/edge/route-policies/"+app.Route.Hostname, platformAdminKey, map[string]any{
		"route_policy": model.EdgeRoutePolicyEnabled, "excluded_edge_ids": []string{"edge-us-1"},
		"exclusion_reason": "tls failure", "expected_exclusion_generation": created.Policy.ExclusionGeneration,
		"expected_exclusion_fence": created.Policy.ExclusionFence,
	})
	if defaultOff.Code != 409 || !strings.Contains(defaultOff.Body.String(), "clear is disabled") {
		t.Fatalf("default-off clear did not fail closed: %d %s", defaultOff.Code, defaultOff.Body.String())
	}
	t.Setenv("FUGUE_EDGE_EXCLUSION_CLEAR_ENABLED", "true")
	for _, instance := range active {
		if instance.EdgeID != "edge-de-1" {
			continue
		}
		instance.Node.TLSStatus = model.EdgeTLSStatusPending
		for i := 0; i < 2; i++ {
			if _, err := storeState.UpdateEdgeInstanceHeartbeat(instance); err != nil {
				t.Fatal(err)
			}
		}
	}
	tlsBlocked := performJSONRequest(t, server, "PUT", "/v1/edge/route-policies/"+app.Route.Hostname, platformAdminKey, map[string]any{
		"route_policy": model.EdgeRoutePolicyEnabled, "excluded_edge_ids": []string{"edge-us-1"},
		"exclusion_reason": "tls failure", "expected_exclusion_generation": created.Policy.ExclusionGeneration,
		"expected_exclusion_fence": created.Policy.ExclusionFence,
	})
	if tlsBlocked.Code != 409 || !strings.Contains(tlsBlocked.Body.String(), "TLS-ready") {
		t.Fatalf("TLS-not-ready clear did not fail closed: %d %s", tlsBlocked.Code, tlsBlocked.Body.String())
	}
	for _, instance := range active {
		if instance.EdgeID != "edge-de-1" {
			continue
		}
		instance.Node.TLSStatus = model.EdgeTLSStatusReady
		for i := 0; i < 2; i++ {
			if _, err := storeState.UpdateEdgeInstanceHeartbeat(instance); err != nil {
				t.Fatal(err)
			}
		}
	}

	partialRecorder := performJSONRequest(t, server, "PUT", "/v1/edge/route-policies/"+app.Route.Hostname, platformAdminKey, map[string]any{
		"route_policy": model.EdgeRoutePolicyEnabled, "excluded_edge_ids": []string{"edge-us-1"},
		"exclusion_reason": "tls failure", "min_healthy_edge_nodes": 1,
		"expected_exclusion_generation": created.Policy.ExclusionGeneration,
		"expected_exclusion_fence":      created.Policy.ExclusionFence,
	})
	if partialRecorder.Code != 200 {
		t.Fatalf("partial clear: %d %s", partialRecorder.Code, partialRecorder.Body.String())
	}
	var partial struct {
		Policy model.EdgeRoutePolicy `json:"policy"`
	}
	mustDecodeJSON(t, partialRecorder, &partial)
	if len(partial.Policy.ExcludedEdgeIDs) != 1 || partial.Policy.ExcludedEdgeIDs[0] != "edge-us-1" || partial.Policy.ExclusionGeneration != 2 {
		t.Fatalf("partial clear mismatch: %+v", partial.Policy)
	}
	stale := performJSONRequest(t, server, "PUT", "/v1/edge/route-policies/"+app.Route.Hostname, platformAdminKey, map[string]any{
		"route_policy": model.EdgeRoutePolicyEnabled, "excluded_edge_ids": []string{"edge-us-1"},
		"exclusion_reason": "tls failure", "expected_exclusion_generation": created.Policy.ExclusionGeneration,
		"expected_exclusion_fence": created.Policy.ExclusionFence,
	})
	if stale.Code != 409 {
		t.Fatalf("stale replay = %d %s", stale.Code, stale.Body.String())
	}

	expired := partial.Policy
	past := time.Now().UTC().Add(-time.Hour)
	expired.ExclusionExpiresAt = &past
	expired, err = storeState.PutEdgeRoutePolicyCAS(expired, partial.Policy.ExclusionGeneration, partial.Policy.ExclusionFence)
	if err != nil {
		t.Fatal(err)
	}
	if model.EdgeRoutePolicyExclusionLifecycleAt(expired, time.Now().UTC()) != model.EdgeExclusionLifecycleExpiredHold {
		t.Fatalf("expiry did not enter hold: %+v", expired)
	}
	shownRecorder := performJSONRequest(t, server, "GET", "/v1/edge/route-policies/"+app.Route.Hostname, platformAdminKey, nil)
	if shownRecorder.Code != 200 {
		t.Fatalf("show expired hold: %d %s", shownRecorder.Code, shownRecorder.Body.String())
	}
	var shown struct {
		Policy model.EdgeRoutePolicy `json:"policy"`
	}
	mustDecodeJSON(t, shownRecorder, &shown)
	if shown.Policy.ExclusionLifecycle != model.EdgeExclusionLifecycleExpiredHold || !shown.Policy.ExclusionEvidenceFresh || shown.Policy.ExclusionEvidenceCheckedAt == nil {
		t.Fatalf("read view omitted lifecycle/evidence freshness: %+v", shown.Policy)
	}
	deletePath := "/v1/edge/route-policies/" + app.Route.Hostname + "?expected_exclusion_generation=" + fmt.Sprintf("%d", expired.ExclusionGeneration) + "&expected_exclusion_fence=" + url.QueryEscape(expired.ExclusionFence)
	deleted := performJSONRequest(t, server, "DELETE", deletePath, platformAdminKey, nil)
	if deleted.Code != 200 {
		t.Fatalf("expired evidence-backed delete: %d %s", deleted.Code, deleted.Body.String())
	}
}

func TestEdgeExclusionMetricsAreLowCardinalityAndAlertOnRedundancy(t *testing.T) {
	storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	recordHealthyEdgeForRouteTest(t, storeState, "edge-us-1", "edge-group-country-us", "15.204.94.71")
	created := performJSONRequest(t, server, "PUT", "/v1/edge/route-policies/"+app.Route.Hostname, platformAdminKey, map[string]any{
		"route_policy": model.EdgeRoutePolicyEnabled, "excluded_edge_ids": []string{"edge-us-1"},
		"exclusion_reason": "tls failure", "min_healthy_edge_nodes": 2,
	})
	if created.Code != 200 {
		t.Fatalf("create exclusion: %d %s", created.Code, created.Body.String())
	}
	var response struct {
		Policy model.EdgeRoutePolicy `json:"policy"`
	}
	mustDecodeJSON(t, created, &response)
	var out strings.Builder
	server.writeEdgeExclusionMetrics(&out)
	metrics := out.String()
	for _, want := range []string{
		`fugue_edge_exclusion_policies{lifecycle="active",scope="edge"} 1.000000`,
		`fugue_edge_exclusion_redundancy_at_risk 1.000000`,
		`fugue_edge_exclusion_no_safe_route 1.000000`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
	if strings.Contains(metrics, app.Route.Hostname) || strings.Contains(metrics, response.Policy.ExclusionOwnerDigest) || strings.Contains(metrics, response.Policy.ExclusionFence) {
		t.Fatalf("metrics leaked high-cardinality policy identity:\n%s", metrics)
	}
}

func TestExpiredHoldKeepsSafeFallbackAndNeverReenablesDoubleFailure(t *testing.T) {
	now := time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
	expired := now.Add(-24 * time.Hour)
	base := model.EdgeRouteBinding{Hostname: "api.example.test", AppID: "app", TenantID: "tenant", RuntimeEdgeGroupID: "edge-group-country-de", EdgeGroupID: "edge-group-country-de", RouteKind: model.EdgeRouteKindPlatform, RoutePolicy: model.EdgeRoutePolicyEnabled, Status: model.EdgeRouteStatusActive}
	healthyGroups := map[string]bool{"edge-group-country-de": true, "edge-group-country-us": true}
	healthyNodes := map[string][]string{"edge-group-country-de": {"edge-de-1"}, "edge-group-country-us": {"edge-us-1"}}
	policy := model.EdgeRoutePolicy{Hostname: base.Hostname, AppID: base.AppID, TenantID: base.TenantID, ExcludedEdgeGroupIDs: []string{"edge-group-country-de"}, ExclusionReason: "DE TLS failure", ExclusionOwnerDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExclusionGeneration: 4, ExclusionFence: "fence", ExclusionExpiresAt: &expired, RoutePolicy: model.EdgeRoutePolicyEnabled}
	fallback := applyEdgeRoutePolicy(base, map[string]model.EdgeRoutePolicy{base.Hostname: policy}, healthyGroups, healthyNodes, now)
	if fallback.Status != model.EdgeRouteStatusActive || fallback.EdgeGroupID != "edge-group-country-us" {
		t.Fatalf("expired DE hold did not preserve safe US fallback: %+v", fallback)
	}
	policy.ExcludedEdgeGroupIDs = []string{"edge-group-country-de", "edge-group-country-us"}
	blocked := applyEdgeRoutePolicy(base, map[string]model.EdgeRoutePolicy{base.Hostname: policy}, healthyGroups, healthyNodes, now.Add(30*24*time.Hour))
	if blocked.Status != model.EdgeRouteStatusUnavailable || blocked.SelectedEdgeGroup != "" {
		t.Fatalf("double-group hold unsafely re-enabled a route: %+v", blocked)
	}
}

func enforceExactEpochForExclusionTest(t *testing.T, storeState *store.Store, instances ...model.EdgeNodeInstance) {
	t.Helper()
	state, err := storeState.GetEdgeActivationState()
	if err != nil {
		t.Fatal(err)
	}
	expected := make([]model.EdgeExpectedInstance, 0, len(instances))
	for _, instance := range instances {
		expected = append(expected, model.EdgeExpectedInstance{EdgeID: instance.EdgeID, EdgeGroupID: instance.EdgeGroupID, Slot: instance.Slot, InstanceUID: instance.InstanceUID, ReleaseEpoch: instance.ReleaseEpoch})
	}
	_, err = storeState.AdvanceEdgeActivation(model.EdgeActivationAdvance{
		ExpectedGeneration: state.Generation, ToPhase: model.EdgeActivationPhaseEnforced,
		PlanDigest: state.PlanDigest, EvidenceDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ReleaseID: state.ReleaseID, ReleaseRecordUID: state.ReleaseRecordUID, ReleaseRecordVersion: state.ReleaseRecordVersion, ReleaseRecordDigest: state.ReleaseRecordDigest,
		ExpectedInstances: expected, LegacySnapshotDigest: state.LegacySnapshotDigest, APIReplicaGeneration: state.APIReplicaGeneration,
		Actor: "bootstrap/test", ReleaseFence: "github:test/repo:1:1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		PhaseNonce: fmt.Sprintf("sha256:%064x", state.Generation), AuthorizationDigest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		AuthorizationKeyID: "test-key", AuthorizationKeyGeneration: "generation-test", AuthorizationRunnerObservedSecretUID: "secret-uid", AuthorizationRunnerObservedSecretVersion: "1",
	})
	if err != nil {
		t.Fatalf("advance edge activation to enforced: %v", err)
	}
}

func TestPlatformDomainDeleteCannotBypassHeldExclusion(t *testing.T) {
	storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	const hostname = "held.fugue.pro"
	putDomain := performJSONRequest(t, server, "PUT", "/v1/admin/domains/"+hostname, platformAdminKey, map[string]any{"app_id": app.ID, "route_policy": model.EdgeRoutePolicyCanary, "edge_group_id": "edge-group-country-us"})
	if putDomain.Code != 200 {
		t.Fatalf("put domain: %d %s", putDomain.Code, putDomain.Body.String())
	}
	policy, err := storeState.GetEdgeRoutePolicy(hostname)
	if err != nil {
		t.Fatal(err)
	}
	policy.ExcludedEdgeIDs = []string{"edge-de-1"}
	policy.ExclusionReason = "DE TLS failure"
	policy.ExclusionOwnerDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := storeState.PutEdgeRoutePolicyCAS(policy, policy.ExclusionGeneration, policy.ExclusionFence); err != nil {
		t.Fatal(err)
	}
	deleted := performJSONRequest(t, server, "DELETE", "/v1/admin/domains/"+hostname, platformAdminKey, nil)
	if deleted.Code != 409 || !strings.Contains(deleted.Body.String(), "evidence-cleared") {
		t.Fatalf("platform domain delete bypassed exclusion: %d %s", deleted.Code, deleted.Body.String())
	}
	if _, err := storeState.GetAppDomain(hostname); err != nil {
		t.Fatalf("domain was partially deleted before exclusion check: %v", err)
	}
}
