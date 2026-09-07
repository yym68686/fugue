package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestBuilderEvidencePrivateDiagnosticsAreNotTenantVisible(t *testing.T) {
	state, server, key, app, op := setupOperationEvidenceAPITest(t)
	_, err := state.RecordOperationEvidence(model.OperationEvidence{TenantID: app.TenantID, AppID: app.ID, OperationID: op.ID, Type: model.OperationEvidenceTypeBuildAttempt, Source: "import_controller", Severity: "error", Confidence: "evidence_backed", Summary: "Build attempt failed", PayloadVersion: 1, Payload: map[string]any{
		"build_attempt":       map[string]any{"attempt": 1, "causes": []string{"ephemeral_storage_limit_exceeded"}},
		"builder_diagnostics": map[string]any{"node": "private-node", "log_tail": "token=secret-value", "address": "10.42.1.1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	response := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/evidence?include_payload=true", key, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("response %d %s", response.Code, response.Body.String())
	}
	for _, private := range []string{"private-node", "10.42.1.1", "secret-value", "builder_diagnostics"} {
		if strings.Contains(response.Body.String(), private) {
			t.Fatalf("tenant evidence leaked %s", private)
		}
	}
	if !strings.Contains(response.Body.String(), "ephemeral_storage_limit_exceeded") {
		t.Fatal("public reason removed")
	}
	timeline := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/timeline?include_payload=true", key, nil)
	if strings.Contains(timeline.Body.String(), "builder_diagnostics") {
		t.Fatal("timeline leaked private evidence")
	}
}
func TestQueuedDeploymentLinkIsExplicitAndScoped(t *testing.T) {
	state, server, key, app, parent := setupOperationEvidenceAPITest(t)
	child, err := state.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec})
	if err != nil {
		t.Fatal(err)
	}
	parent.Type = model.OperationTypeImport
	parent.Status = model.OperationStatusCompleted
	server.attachQueuedDeployOperation(&parent)
	if parent.QueuedDeployOperationID != "" {
		t.Fatal("unlinked deployment inferred")
	}
	_, err = state.RecordOperationEvidence(model.OperationEvidence{TenantID: app.TenantID, AppID: app.ID, OperationID: parent.ID, Type: "deploy_queued", Source: "import_controller", Severity: "info", Confidence: "confirmed", Summary: "Deployment queued", Payload: map[string]any{"queued_deploy_operation_id": child.ID}})
	if err != nil {
		t.Fatal(err)
	}
	server.attachQueuedDeployOperation(&parent)
	if parent.QueuedDeployOperationID != child.ID {
		t.Fatal("durable link missing")
	}
	// Existing operation responses remain readable; optional enrichment does not
	// change the original operation or require a database migration.
	response := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+parent.ID, key, nil)
	var payload map[string]any
	if json.Unmarshal(response.Body.Bytes(), &payload) != nil || response.Code != http.StatusOK {
		t.Fatal("operation response broken")
	}
}
