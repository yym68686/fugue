package api

import (
	"context"
	"net/http"
	"testing"

	"fugue/internal/model"
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
