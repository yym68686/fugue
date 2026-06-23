package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestRobustnessStatusRequiresPlatformAdmin(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/robustness/status", apiKey, nil)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}
}

func TestRobustnessStatusExposesStructuredChecksAndIncidents(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	configureRobustnessTestPreflight(t, server)

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/robustness/status", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.RobustnessStatusResponse
	mustDecodeJSON(t, recorder, &response)
	if response.Status.GeneratedAt.IsZero() {
		t.Fatalf("expected generated_at to be set: %+v", response.Status)
	}
	if len(response.Status.Checks) == 0 {
		t.Fatalf("expected structured robustness checks: %+v", response.Status)
	}
	if !hasRobustnessCheck(response.Status.Checks, "dns_preflight") {
		t.Fatalf("expected dns_preflight check in %+v", response.Status.Checks)
	}
	if len(response.Status.Incidents) == 0 {
		t.Fatalf("expected incidents from missing DNS nodes in test fixture: %+v", response.Status)
	}
	if !response.Status.BlockRollout {
		t.Fatalf("expected missing DNS nodes to block rollout: %+v", response.Status)
	}
	for _, want := range []string{
		"generated_artifact_edge_route_bundle",
		"generated_artifact_edge_dns_bundle",
		"backup_backend_readiness",
		"operation_inventory",
	} {
		if !hasRobustnessCheck(response.Status.Checks, want) {
			t.Fatalf("expected robustness check %q in %+v", want, response.Status.Checks)
		}
	}
}

func TestRobustnessStatusIncludesNodeBackupAndOperationEvidence(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	configureRobustnessTestPreflight(t, server)
	now := time.Now().UTC()
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:                  "edge-us-1",
		EdgeGroupID:         "edge-group-country-us",
		Status:              model.EdgeHealthHealthy,
		Healthy:             true,
		PublicIPv4:          "203.0.113.10",
		RouteBundleVersion:  "routegen_live",
		ServingGeneration:   "routegen_live",
		LKGGeneration:       "routegen_live",
		CaddyRouteCount:     3,
		CaddyAppliedVersion: "routegen_live",
		TLSStatus:           model.EdgeTLSStatusReady,
		LastSeenAt:          &now,
		LastHeartbeatAt:     &now,
	}); err != nil {
		t.Fatalf("record edge heartbeat: %v", err)
	}
	if _, err := storeState.UpdateDNSHeartbeat(model.DNSNode{
		ID:                "dns-us-1",
		EdgeGroupID:       "edge-group-country-us",
		Status:            model.EdgeHealthHealthy,
		Healthy:           true,
		PublicIPv4:        "198.51.100.10",
		Zone:              "fugue.pro",
		DNSBundleVersion:  "dnsgen_live",
		ServingGeneration: "dnsgen_live",
		LKGGeneration:     "dnsgen_live",
		RecordCount:       4,
		CacheStatus:       "ready",
		UDPListen:         true,
		TCPListen:         true,
		LastSeenAt:        &now,
		LastHeartbeatAt:   &now,
	}); err != nil {
		t.Fatalf("record dns heartbeat: %v", err)
	}
	replicas := 2
	if _, err := storeState.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		Type:            model.OperationTypeScale,
		DesiredReplicas: &replicas,
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/robustness/status", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.RobustnessStatusResponse
	mustDecodeJSON(t, recorder, &response)
	for _, want := range []string{
		"edge_route_generation_drift",
		"edge_caddy_reload",
		"dns_generation_drift",
		"dns_node_lkg_reporting",
		"backup_artifact_integrity",
		"operation_stuck_detection",
	} {
		if !hasRobustnessCheck(response.Status.Checks, want) {
			t.Fatalf("expected robustness check %q in %+v", want, response.Status.Checks)
		}
	}
}

func TestRobustnessStatusDetectsManagedPostgresRuntimeNotReady(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	configureRobustnessTestPreflight(t, server)
	raiseManagedTestCap(t, storeState, app.TenantID)

	dbApp, err := storeState.CreateApp(app.TenantID, app.ProjectID, "database-app", "", model.AppSpec{
		Image:     "ghcr.io/example/database-app:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			RuntimeID:           "runtime_managed_shared",
			Database:            "app",
			User:                "app",
			Password:            "postgres-password-123",
			ServiceName:         "database-app-postgres",
			StorageSize:         "5Gi",
			StorageClassName:    "fugue-postgres-rwo",
			PrimaryNodeName:     "node-a",
			Instances:           1,
			SynchronousReplicas: 0,
		},
	})
	if err != nil {
		t.Fatalf("create app with postgres: %v", err)
	}
	if len(dbApp.BackingServices) != 1 {
		t.Fatalf("expected owned postgres backing service, got %+v", dbApp.BackingServices)
	}

	startedAt := time.Now().UTC().Add(-5 * time.Minute)
	if err := storeState.SyncManagedAppRuntimeStatus(dbApp.ID, &startedAt, &startedAt, []store.ManagedBackingServiceRuntimeStatus{{
		ServiceID:               dbApp.BackingServices[0].ID,
		CurrentRuntimeStartedAt: &startedAt,
		CurrentRuntimeReadyAt:   nil,
	}}); err != nil {
		t.Fatalf("sync runtime status: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/robustness/status", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.RobustnessStatusResponse
	mustDecodeJSON(t, recorder, &response)

	var incident *model.RobustnessIncident
	for index := range response.Status.Incidents {
		if response.Status.Incidents[index].CheckName == "managed_postgres_runtime_ready" {
			incident = &response.Status.Incidents[index]
			break
		}
	}
	if incident == nil {
		t.Fatalf("expected managed postgres runtime incident, got %+v", response.Status.Incidents)
	}
	if incident.Severity != model.RobustnessSeverityDegraded {
		t.Fatalf("expected degraded severity, got %+v", incident)
	}
	if incident.Evidence["owner_app_id"] != dbApp.ID || incident.Evidence["storage_size"] != "5Gi" {
		t.Fatalf("expected postgres evidence, got %+v", incident.Evidence)
	}

	planRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/admin/robustness/incidents/"+incident.ID+"/repair-plan", platformAdminKey, nil)
	if planRecorder.Code != http.StatusOK {
		t.Fatalf("expected repair plan status %d, got %d body=%s", http.StatusOK, planRecorder.Code, planRecorder.Body.String())
	}
	var planResponse model.RobustnessRepairPlanResponse
	mustDecodeJSON(t, planRecorder, &planResponse)
	if !robustnessPlanHasCommand(planResponse.Plan, "fugue app db configure "+dbApp.ID+" --storage-size <larger-than-current> --wait") {
		t.Fatalf("expected storage expansion command in repair plan, got %+v", planResponse.Plan.Actions)
	}
}

func TestRobustnessMetricsExposeGuardiansGenerationAndRepairEvents(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Now().UTC()
	if _, _, err := storeState.UpdateEdgeHeartbeat(model.EdgeNode{
		ID:                 "edge-us-1",
		EdgeGroupID:        "edge-group-country-us",
		Status:             model.EdgeHealthHealthy,
		Healthy:            true,
		RouteBundleVersion: "routegen_live",
		ServingGeneration:  "routegen_live",
		LKGGeneration:      "routegen_live",
		LastHeartbeatAt:    &now,
	}); err != nil {
		t.Fatalf("record edge heartbeat: %v", err)
	}
	if _, err := storeState.UpdateDNSHeartbeat(model.DNSNode{
		ID:                "dns-us-1",
		EdgeGroupID:       "edge-group-country-us",
		Status:            model.EdgeHealthDegraded,
		Healthy:           true,
		PublicIPv4:        "198.51.100.10",
		Zone:              "fugue.pro",
		DNSBundleVersion:  "dnsgen_new",
		ServingGeneration: "dnsgen_lkg",
		LKGGeneration:     "dnsgen_lkg",
		CacheStatus:       "serving-lkg",
		LastHeartbeatAt:   &now,
	}); err != nil {
		t.Fatalf("record dns heartbeat: %v", err)
	}
	if err := storeState.AppendAuditEvent(model.AuditEvent{
		Action:     "robustness.repair.dry_run",
		TargetType: "robustness_incident",
		TargetID:   "robust_test",
		CreatedAt:  now,
	}); err != nil {
		t.Fatalf("append audit event: %v", err)
	}

	recorder := httptest.NewRecorder()
	server.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		`fugue_robustness_guardian_enabled{guardian="route-dns"} 1.000000`,
		`fugue_robustness_node_generation_drift_seconds`,
		`fugue_robustness_lkg_serving{edge_group_id="edge-group-country-us",kind="dns",node_id="dns-us-1"} 1.000000`,
		`fugue_robustness_repair_events_total{outcome="dry_run"} 1.000000`,
		`fugue_robustness_backup_last_success_age_seconds`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, body)
		}
	}
}

func TestRobustnessGenerationDriftIsScopedByEdgeGroup(t *testing.T) {
	t.Parallel()

	edgeExpected := mostCommonNonEmptyEdgeRouteGenerationByGroup([]model.EdgeNode{
		{ID: "edge-us-1", EdgeGroupID: "edge-group-country-us", RouteBundleVersion: "routegen_us"},
		{ID: "edge-de-1", EdgeGroupID: "edge-group-country-de", RouteBundleVersion: "routegen_de"},
	})
	if edgeExpected["edge-group-country-us"] != "routegen_us" || edgeExpected["edge-group-country-de"] != "routegen_de" {
		t.Fatalf("expected route generations to be scoped by edge group, got %+v", edgeExpected)
	}
	if msg := robustnessGenerationDriftMessage("edge", "edge-de-1", edgeExpected["edge-group-country-de"], "routegen_de"); msg != "" {
		t.Fatalf("expected no drift for group-local edge generation, got %q", msg)
	}

	dnsExpected := mostCommonNonEmptyDNSGenerationByGroup([]model.DNSNode{
		{ID: "dns-us-1", EdgeGroupID: "edge-group-country-us", DNSBundleVersion: "dnsgen_us"},
		{ID: "dns-de-1", EdgeGroupID: "edge-group-country-de", DNSBundleVersion: "dnsgen_de"},
	})
	if dnsExpected["edge-group-country-us"] != "dnsgen_us" || dnsExpected["edge-group-country-de"] != "dnsgen_de" {
		t.Fatalf("expected DNS generations to be scoped by edge group, got %+v", dnsExpected)
	}
	if msg := robustnessGenerationDriftMessage("dns", "dns-de-1", dnsExpected["edge-group-country-de"], "dnsgen_de"); msg != "" {
		t.Fatalf("expected no drift for group-local DNS generation, got %q", msg)
	}
}

func TestRobustnessRepairPlanIsReadOnlyUntilSafeAutomationExists(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	configureRobustnessTestPreflight(t, server)

	list := performJSONRequest(t, server, http.MethodGet, "/v1/admin/robustness/incidents", platformAdminKey, nil)
	if list.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, list.Code, list.Body.String())
	}
	var listResponse model.RobustnessIncidentListResponse
	mustDecodeJSON(t, list, &listResponse)
	if len(listResponse.Incidents) == 0 {
		t.Fatalf("expected at least one robustness incident")
	}

	incidentID := listResponse.Incidents[0].ID
	planRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/admin/robustness/incidents/"+incidentID+"/repair-plan", platformAdminKey, nil)
	if planRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, planRecorder.Code, planRecorder.Body.String())
	}
	var planResponse model.RobustnessRepairPlanResponse
	mustDecodeJSON(t, planRecorder, &planResponse)
	if planResponse.Plan.Safe || planResponse.Plan.Status != model.RobustnessRepairPlanStatusManualActionRequired {
		t.Fatalf("expected read-only manual repair plan, got %+v", planResponse.Plan)
	}

	runRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/admin/robustness/incidents/"+incidentID+"/repair", platformAdminKey, model.RobustnessRepairRequest{DryRun: false})
	if runRecorder.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusConflict, runRecorder.Code, runRecorder.Body.String())
	}
}

func configureRobustnessTestPreflight(t *testing.T, server *Server) {
	t.Helper()

	kubeServer := newDNSPreflightKubeServer(t, []string{})
	t.Cleanup(kubeServer.Close)
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}
	server.dnsDelegationProbe = func(_ context.Context, node model.DNSNode, _, _ string) dnsDelegationProbeResult {
		return dnsDelegationProbeResult{
			UDP53Reachable: true,
			TCP53Reachable: true,
			ProbeAnswers:   []string{node.PublicIPv4},
		}
	}
	server.dnsParentNSLookup = func(_ context.Context, _ string) ([]string, error) {
		return []string{"ns1.example.net."}, nil
	}
}

func hasRobustnessCheck(checks []model.RobustnessCheck, name string) bool {
	for _, check := range checks {
		if check.Name == name {
			return true
		}
	}
	return false
}

func robustnessPlanHasCommand(plan model.RobustnessRepairPlan, command string) bool {
	for _, action := range plan.Actions {
		if action.Command == command {
			return true
		}
	}
	return false
}
