package api

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestEdgeRouteIntentDerivationPersistsGenerationDriftWithoutChangingRoutes(t *testing.T) {
	state, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	app = deployAppForEdgeRouteTest(t, state, app)
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatal(err)
	}
	managed.Metadata.Generation = 3
	managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, DesiredReplicas: 1, ReadyReplicas: 1, ObservedGeneration: 3}
	proof := edgeRouteObservationTestProvenance()
	proof.evidence.appObservationKey = managedAppRuntimeEvidenceObservationKey(app)
	proof.evidence.managedGeneration = 3
	proof.evidence.managedObservedGeneration = 3
	proof.evidence.deploymentGeneration = 9
	proof.evidence.deploymentObservedGeneration = 8
	proof.evidence.invariantViolations = []string{"deployment_generation_unobserved"}
	now := time.Now().UTC()
	server.managedAppStatusCache.setList(managedAppStatusListCacheEntry{
		items:    map[string]runtime.ManagedAppObject{app.ID: managed},
		evidence: map[string]managedAppRuntimeEvidence{app.ID: proof.evidence},
		ok:       true, clusterID: "cluster-test", refreshedAt: now, expiresAt: now.Add(time.Hour),
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/edge/route-intents", nil)
	server.log = nil
	before, err := server.deriveEdgeRouteIntentSnapshot(req, state)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	server.log = log.New(&logs, "", 0)
	after, err := server.deriveEdgeRouteIntentSnapshot(req, state)
	if err != nil {
		t.Fatal(err)
	}
	if before.Generation != after.Generation || !reflect.DeepEqual(before.Routes, after.Routes) || !reflect.DeepEqual(before.TLSAllowlist, after.TLSAllowlist) || !reflect.DeepEqual(before.CachePolicies, after.CachePolicies) {
		t.Fatal("diagnostic logging changed route intent")
	}
	if len(after.Routes) != 1 || after.Routes[0].OriginStatus != model.EdgeRouteStatusUnavailable || !strings.Contains(after.Routes[0].OriginStatusReason, "deployment_generation_unobserved") {
		t.Fatalf("generation protection was weakened: %+v", after.Routes)
	}
	var event struct {
		Type             string `json:"event_type"`
		Table            string `json:"fugue_table"`
		AppID            string `json:"app_id"`
		IntentGeneration string `json:"source_intent_generation"`
		Material         string `json:"material_json"`
	}
	observations := func() []string {
		var entries []string
		for _, line := range strings.Split(logs.String(), "\n") {
			if strings.Contains(line, `"event_type":"edge_route_intent_observation"`) {
				entries = append(entries, line)
			}
		}
		return entries
	}
	entries := observations()
	if len(entries) != 1 {
		t.Fatalf("expected one observation, got %d", len(entries))
	}
	if err := json.Unmarshal([]byte(entries[0]), &event); err != nil {
		t.Fatalf("decode single observation: %v (%s)", err, logs.String())
	}
	if event.Type != "edge_route_intent_observation" || event.Table != "app_events" || event.AppID != app.ID || event.IntentGeneration != after.Generation {
		t.Fatalf("missing projection identity: %+v", event)
	}
	var material edgeRouteDecisionMaterial
	if err := json.Unmarshal([]byte(event.Material), &material); err != nil {
		t.Fatal(err)
	}
	if material.DeploymentKubernetesGeneration != 9 || material.DeploymentObservedGeneration != 8 || material.DeploymentReadyReplicas != 1 || material.EndpointReady == nil || !*material.EndpointReady || !material.ObservationFresh {
		t.Fatalf("generation/serving evidence was lost: %+v", material)
	}
	logs.Reset()
	if _, err := server.deriveEdgeRouteIntentSnapshot(req, state); err != nil {
		t.Fatal(err)
	}
	if len(observations()) != 0 {
		t.Fatalf("unchanged polling duplicated evidence: %s", logs.String())
	}
}

func TestEdgeRouteIntentObservationEmitsFactChangesButNotRefreshChurn(t *testing.T) {
	app := edgeRouteObservationTestApp("deploying", "op_synthetic")
	app.Spec.Env = map[string]string{"TOKEN": "secret-env-value"}
	proof := edgeRouteObservationTestProvenance()
	snapshot := model.EdgeRouteIntentSnapshot{Generation: "intent-synthetic", Routes: []model.EdgeRouteIntent{{AppID: app.ID, Hostname: "app.example.test", PathPrefix: "/", Generation: "route-synthetic", OriginStatus: model.EdgeRouteStatusUnavailable, OriginStatusReason: "runtime invariant violation"}}}
	var logs bytes.Buffer
	server := &Server{log: log.New(&logs, "", 0)}
	emit := func() {
		server.logEdgeRouteIntentObservations(snapshot, map[string]model.App{app.ID: app}, map[string]managedAppObservationProvenance{app.ID: proof})
	}
	emit()
	if logs.Len() == 0 || strings.Contains(logs.String(), "secret-env-value") || strings.Contains(logs.String(), app.Spec.Image) {
		t.Fatal("evidence missing or raw configuration leaked")
	}
	logs.Reset()
	proof.cacheExpired = true
	proof.refreshedAt = proof.refreshedAt.Add(time.Minute)
	proof.evidence.imageLocationObservedAt = proof.refreshedAt
	emit()
	if logs.Len() != 0 {
		t.Fatal("refresh bookkeeping generated duplicate evidence")
	}
	proof.evidence.deploymentObservedGeneration--
	emit()
	if logs.Len() == 0 {
		t.Fatal("a generation change was not recorded")
	}
}
