package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestEdgeRouteIntentsRequireExactEdgeControlIdentity(t *testing.T) {
	t.Parallel()

	_, server, tenantKey, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	keyring := edgeRouteIntentTestKeyring()
	server.auth.EdgeRouteIntentIdentityKeyring = keyring

	for _, test := range []struct {
		name       string
		token      string
		claims     *platformcontrol.PlatformComponentIdentityClaims
		wantStatus int
	}{
		{name: "anonymous", wantStatus: http.StatusUnauthorized},
		{name: "tenant api key", token: tenantKey, wantStatus: http.StatusUnauthorized},
		{name: "platform admin api key", token: platformAdminKey, wantStatus: http.StatusUnauthorized},
		{name: "wrong component", claims: edgeRouteIntentTestClaims(model.PlatformConsumerComponentEdgeWorker, "global", []string{model.PlatformArtifactKindEdgeRouteIntent}), wantStatus: http.StatusForbidden},
		{name: "wrong scope", claims: edgeRouteIntentTestClaims(model.PlatformConsumerComponentEdgeControl, "edge-group-country-us", []string{model.PlatformArtifactKindEdgeRouteIntent}), wantStatus: http.StatusForbidden},
		{name: "wrong capability", claims: edgeRouteIntentTestClaims(model.PlatformConsumerComponentEdgeControl, "global", []string{model.PlatformArtifactKindEdgeRouteBundle}), wantStatus: http.StatusForbidden},
		{name: "multiple capabilities", claims: edgeRouteIntentTestClaims(model.PlatformConsumerComponentEdgeControl, "global", []string{model.PlatformArtifactKindEdgeRouteIntent, model.PlatformArtifactKindEdgeRouteBundle}), wantStatus: http.StatusForbidden},
		{name: "exact identity", claims: edgeRouteIntentTestClaims(model.PlatformConsumerComponentEdgeControl, "global", []string{model.PlatformArtifactKindEdgeRouteIntent}), wantStatus: http.StatusOK},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			token := test.token
			if test.claims != nil {
				var err error
				token, err = platformcontrol.IssuePlatformComponentIdentity(keyring, *test.claims, time.Now().UTC(), 5*time.Minute)
				if err != nil {
					t.Fatalf("issue platform component identity: %v", err)
				}
			}
			recorder := performJSONRequest(t, server, http.MethodGet, "/v1/edge/route-intents", token, nil)
			if recorder.Code != test.wantStatus {
				t.Fatalf("expected status %d, got %d body=%s", test.wantStatus, recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestEdgeRouteIntentSourceCannotReadEdgeInventory(t *testing.T) {
	t.Parallel()

	sourceType := reflect.TypeOf((*edgeRouteIntentSource)(nil)).Elem()
	for _, forbidden := range []string{
		"ListActiveEdgeNodes", "ListEdgeNodes", "GetEdgeActivationState",
		"ListEdgeNodeInstances", "ListEdgeGroups",
	} {
		if _, exists := sourceType.MethodByName(forbidden); exists {
			t.Fatalf("RouteIntent source exposes Edge Control-owned inventory method %s", forbidden)
		}
	}
}

func TestEdgeRouteIntentSnapshotIsIndependentOfMixedEdgeInventory(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	app = deployAppForEdgeRouteTest(t, storeState, app)
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/route-intents", nil)

	before, err := server.deriveEdgeRouteIntentSnapshot(req, storeState)
	if err != nil {
		t.Fatalf("derive route intents before edge inventory: %v", err)
	}
	if before.SchemaVersion != model.EdgeRouteIntentSchemaVersionV1 || before.Generation == "" || len(before.Routes) != 1 {
		t.Fatalf("unexpected initial route intent snapshot: %+v", before)
	}
	intent := before.Routes[0]
	if intent.Hostname != app.Route.Hostname || intent.TargetGroupMode != model.EdgeRouteIntentGroupModeAllGroups || intent.PinnedEdgeGroupID != "" {
		t.Fatalf("unexpected inventory-independent intent: %+v", intent)
	}

	active := []model.EdgeNodeInstance{
		edgeRouteIntentActiveInstance("edge-us-1", "edge-group-country-us", "15.204.94.71"),
		edgeRouteIntentActiveInstance("edge-de-1", "edge-group-country-de", "51.38.126.103"),
	}
	for _, instance := range active {
		if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, instance.Node); err != nil {
			t.Fatalf("record active edge heartbeat: %v", err)
		}
	}
	activateExactEpochForAPITest(t, storeState, active...)
	for _, legacy := range []model.EdgeNode{
		{ID: "edge-hk-legacy", EdgeGroupID: "edge-group-country-hk", Status: model.EdgeHealthHealthy, Healthy: true},
		{ID: "edge-jp-legacy", EdgeGroupID: "edge-group-country-jp", Status: model.EdgeHealthHealthy, Healthy: true},
	} {
		if _, _, err := storeState.UpdateEdgeHeartbeat(legacy); err != nil {
			t.Fatalf("record legacy edge heartbeat: %v", err)
		}
	}

	after, err := server.deriveEdgeRouteIntentSnapshot(req, storeState)
	if err != nil {
		t.Fatalf("derive route intents after mixed edge inventory: %v", err)
	}
	if before.Generation != after.Generation || !reflect.DeepEqual(before.Routes, after.Routes) || !reflect.DeepEqual(before.TLSAllowlist, after.TLSAllowlist) || !reflect.DeepEqual(before.CachePolicies, after.CachePolicies) {
		t.Fatalf("edge inventory changed route intent: before=%+v after=%+v", before, after)
	}

	payload, err := json.Marshal(after)
	if err != nil {
		t.Fatalf("marshal route intent snapshot: %v", err)
	}
	for _, forbidden := range []string{
		`"edge_group_id"`, `"selected_edge_group"`, `"fallback_edge_group_id"`,
		`"healthy_edge_node_count"`, `"edge_redundancy_status"`, `"edge_redundancy_reason"`,
		`"signature"`, `"key_id"`, `"valid_until"`,
	} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("route intent leaked Edge Control-owned field %s: %s", forbidden, payload)
		}
	}
}

func TestEdgeRouteIntentEndpointDoesNotMutateLegacyRouteBundle(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	deployAppForEdgeRouteTest(t, storeState, app)
	active := []model.EdgeNodeInstance{
		edgeRouteIntentActiveInstance("edge-us-1", "edge-group-country-us", "15.204.94.71"),
		edgeRouteIntentActiveInstance("edge-de-1", "edge-group-country-de", "51.38.126.103"),
	}
	for _, instance := range active {
		if err := recordActiveEdgeHeartbeatForAPITest(t, storeState, instance.Node); err != nil {
			t.Fatalf("record active edge heartbeat: %v", err)
		}
	}
	activateExactEpochForAPITest(t, storeState, active...)

	req := httptest.NewRequest(http.MethodGet, "/v1/edge/routes", nil)
	before, err := server.deriveEdgeRouteBundle(req, edgeRouteBundleOptions{})
	if err != nil {
		t.Fatalf("derive legacy route bundle before intent read: %v", err)
	}
	if _, err := server.deriveEdgeRouteIntentSnapshot(req, storeState); err != nil {
		t.Fatalf("derive route intent snapshot: %v", err)
	}
	after, err := server.deriveEdgeRouteBundle(req, edgeRouteBundleOptions{})
	if err != nil {
		t.Fatalf("derive legacy route bundle after intent read: %v", err)
	}
	if before.Version != after.Version || !reflect.DeepEqual(before.Routes, after.Routes) || !reflect.DeepEqual(before.TLSAllowlist, after.TLSAllowlist) || !reflect.DeepEqual(before.CachePolicies, after.CachePolicies) {
		t.Fatalf("read-only RouteIntent changed legacy bundle: before=%+v after=%+v", before, after)
	}
}

func TestEdgeRouteIntentGenerationIgnoresTransportTimestamp(t *testing.T) {
	t.Parallel()

	snapshot := model.EdgeRouteIntentSnapshot{
		SchemaVersion: model.EdgeRouteIntentSchemaVersionV1,
		GeneratedAt:   time.Date(2026, 8, 3, 1, 2, 3, 0, time.UTC),
		Routes: []model.EdgeRouteIntent{{
			Hostname: "app.example.com", PathPrefix: "/", TargetGroupMode: model.EdgeRouteIntentGroupModeAllGroups,
		}},
	}
	first := edgeRouteIntentSnapshotGeneration(snapshot)
	snapshot.GeneratedAt = snapshot.GeneratedAt.Add(time.Hour)
	if second := edgeRouteIntentSnapshotGeneration(snapshot); first != second {
		t.Fatalf("transport timestamp changed generation: first=%s second=%s", first, second)
	}
}

func edgeRouteIntentTestKeyring() platformcontrol.PlatformComponentIdentityKeyring {
	return platformcontrol.PlatformComponentIdentityKeyring{
		ActiveKeyID: "edge-route-intent-test-key",
		Keys: map[string]string{
			"edge-route-intent-test-key": "edge-route-intent-test-secret",
		},
	}
}

func edgeRouteIntentTestClaims(component, scope string, artifactKinds []string) *platformcontrol.PlatformComponentIdentityClaims {
	return &platformcontrol.PlatformComponentIdentityClaims{
		CredentialID: "edge-control-route-intent-reader", Component: component, NodeID: "edge-control-shadow-1",
		ScopeKey: scope, ArtifactKinds: artifactKinds,
	}
}

func edgeRouteIntentActiveInstance(edgeID, edgeGroupID, publicIPv4 string) model.EdgeNodeInstance {
	node := model.EdgeNode{
		ID: edgeID, EdgeGroupID: edgeGroupID, PublicIPv4: publicIPv4,
		Status: model.EdgeHealthHealthy, Healthy: true, TLSStatus: model.EdgeTLSStatusReady,
	}
	return model.EdgeNodeInstance{
		EdgeID: edgeID, EdgeGroupID: edgeGroupID, Slot: model.EdgeSlotDirect,
		InstanceUID: "test-" + edgeID, ReleaseEpoch: "test-" + edgeGroupID, Node: node,
	}
}
