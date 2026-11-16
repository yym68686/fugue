package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
)

func schedulingAPIFixtureDecision() model.SchedulingDecision {
	return model.SchedulingDecision{ObservedAt: time.Now().UTC(), Stage: "app_placement", Outcome: "rejected", MemoryRequestRatio: .9, CPUOvercommitRatio: 2, Demand: model.SchedulingResources{MemoryBytes: 1600 << 20}, RequiredPodAddresses: 1,
		Nodes: []model.SchedulingNodeDecision{{CandidateID: "candidate-1", RuntimeMatches: true, ResourceInventoryObserved: true, Allocatable: model.SchedulingResources{MemoryBytes: 32 << 30}, Requested: model.SchedulingResources{MemoryBytes: 31 << 30}, EffectiveRequested: model.SchedulingResources{MemoryBytes: 31 << 30}, Budget: model.SchedulingResources{MemoryBytes: 30923764531}, Rejections: []model.SchedulingRejection{{Code: "memory_request_budget_exceeded", Message: "requested=33285996544B + demand=1677721600B > budget=30923764531B (allocatable=34359738368B, ratio=0.9)"}}}}}
}

func TestSchedulingDecisionTenantAPIExplainsRejectionWithoutPlatformDetails(t *testing.T) {
	t.Parallel()
	state, server, key, app, op := setupOperationEvidenceAPITest(t)
	server.auth = auth.New(state, "test-platform-bootstrap")
	d := schedulingAPIFixtureDecision()
	_, err := state.RecordOperationEvidence(model.OperationEvidence{TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID, Type: model.OperationEvidenceTypeSchedulingDecision, Confidence: model.OperationEvidenceConfidenceConfirmed, Severity: model.OperationEvidenceSeverityError, Summary: d.Summary(), Reason: "scheduling_rejected", Payload: map[string]any{
		"scheduling_decision_id": "decision-fixture", "scheduling_decision_part": 1, "scheduling_decision_parts": 1, "scheduling_decision": d,
		"scheduling_diagnostics": map[string]any{"node_name": "private-physical-node", "pv_name": "private-pv-identity", "required_selector": map[string]string{"private-label": "private-label-value"}, "node_affinity": "private-affinity-value"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.FailOperation(op.ID, "no eligible storage attachment node matches runtime/capacity constraints: "+d.Summary()); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "/diagnosis", "/evidence?include_payload=true", "/timeline?include_payload=true", "/debug-bundle"} {
		rec := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+suffix, key, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", suffix, rec.Code, rec.Body.String())
		}
		body := rec.Body.String()
		for _, private := range []string{"private-physical-node", "private-pv-identity", "private-label", "private-affinity-value", "scheduling_diagnostics"} {
			if strings.Contains(body, private) {
				t.Errorf("%s leaked %s: %s", suffix, private, body)
			}
		}
		if !strings.Contains(body, "memory_request_budget_exceeded") {
			t.Errorf("%s lost actionable reason: %s", suffix, body)
		}
		if suffix == "/diagnosis" {
			var out struct {
				Diagnosis model.OperationDiagnosis `json:"diagnosis"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
			if out.Diagnosis.Confidence != "confirmed" || out.Diagnosis.ConfirmedCause == nil || out.Diagnosis.SchedulingDecision == nil || len(out.Diagnosis.MissingEvidence) != 0 {
				t.Fatalf("incorrect diagnosis: %+v", out.Diagnosis)
			}
			if strings.Contains(body, "previous_container_logs") || strings.Contains(body, "inspect application logs") {
				t.Fatalf("pre-Pod rejection misdiagnosed as application failure: %s", body)
			}
		}
	}
	admin := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+"/evidence?include_payload=true", "test-platform-bootstrap", nil)
	if admin.Code != http.StatusOK || !strings.Contains(admin.Body.String(), "private-physical-node") {
		t.Fatalf("administrator lost original inputs: %d %s", admin.Code, admin.Body.String())
	}
	other, _ := state.CreateTenant("Other Scheduling Tenant")
	_, otherKey, _ := state.CreateAPIKey(other.ID, "reader", []string{"app.read"})
	for _, suffix := range []string{"/diagnosis", "/evidence?include_payload=true", "/timeline?include_payload=true", "/debug-bundle"} {
		rec := performJSONRequest(t, server, http.MethodGet, "/v1/operations/"+op.ID+suffix, otherKey, nil)
		if rec.Code != http.StatusForbidden {
			t.Errorf("cross-tenant access %s: %d", suffix, rec.Code)
		}
	}
}

func TestSchedulingDiagnosisNeverReconstructsMissingHistoricalInputs(t *testing.T) {
	t.Parallel()
	_, _, _, _, op := setupOperationEvidenceAPITest(t)
	op.Status, op.ErrorMessage = model.OperationStatusFailed, "no eligible storage attachment node matches runtime/capacity constraints:"
	diagnosis := model.OperationDiagnosis{Category: "failed"}
	if !attachSchedulingDecisionDiagnosis(op, nil, &diagnosis) || diagnosis.Confidence != "insufficient_evidence" || diagnosis.ConfirmedCause != nil || len(diagnosis.MissingEvidence) != 1 || diagnosis.MissingEvidence[0] != "scheduling_decision" {
		t.Fatalf("invented historical cause: %+v", diagnosis)
	}
	if strings.Contains(strings.Join(diagnosis.RecommendedNextActions, " "), "rerun") {
		t.Fatal("missing evidence triggered a blind retry recommendation")
	}
}

func TestSchedulingDiagnosisRequiresCompleteLatestObservation(t *testing.T) {
	t.Parallel()
	d := schedulingAPIFixtureDecision()
	op := model.Operation{Status: model.OperationStatusFailed, ErrorMessage: "no eligible storage attachment node: " + d.Summary()}
	now := time.Now().UTC()
	e := model.OperationEvidence{ID: "evidence-1", Type: model.OperationEvidenceTypeSchedulingDecision, CollectedAt: now, Payload: map[string]any{"scheduling_decision_id": "observation", "scheduling_decision_part": 1, "scheduling_decision_parts": 2, "scheduling_decision": d}}
	diagnosis := model.OperationDiagnosis{}
	attachSchedulingDecisionDiagnosis(op, []model.OperationEvidence{e}, &diagnosis)
	if diagnosis.Confidence != "insufficient_evidence" || diagnosis.ConfirmedCause != nil {
		t.Fatalf("incomplete snapshot claimed a cause: %+v", diagnosis)
	}
	e2 := e
	e2.ID, e2.CollectedAt = "evidence-2", now.Add(time.Second)
	d.Nodes = []model.SchedulingNodeDecision{{CandidateID: "candidate-2"}}
	e2.Payload = map[string]any{"scheduling_decision_id": "observation", "scheduling_decision_part": 2, "scheduling_decision_parts": 2, "scheduling_decision": d}
	merged, _, complete := latestSchedulingDecision([]model.OperationEvidence{e2, e})
	if !complete || len(merged.Nodes) != 2 || merged.Nodes[0].CandidateID != "candidate-1" || merged.Nodes[1].CandidateID != "candidate-2" {
		t.Fatalf("bad chunk reconstruction: %+v %t", merged, complete)
	}
	d.Outcome = "selected"
	e3 := e2
	e3.ID, e3.CollectedAt = "evidence-3", now.Add(2*time.Second)
	e3.Payload = map[string]any{"scheduling_decision_id": "new-observation", "scheduling_decision_part": 1, "scheduling_decision_parts": 1, "scheduling_decision": d}
	diagnosis = model.OperationDiagnosis{}
	attachSchedulingDecisionDiagnosis(op, []model.OperationEvidence{e, e2, e3}, &diagnosis)
	if diagnosis.ConfirmedCause != nil || diagnosis.Confidence != "insufficient_evidence" {
		t.Fatalf("used a superseded admission rejection: %+v", diagnosis)
	}
}
