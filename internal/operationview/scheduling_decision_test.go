package operationview

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func schedulingFixtureDecision() model.SchedulingDecision {
	return model.SchedulingDecision{ObservedAt: time.Now().UTC(), Stage: "app_placement", Outcome: "rejected", MemoryRequestRatio: .9, CPUOvercommitRatio: 2, Demand: model.SchedulingResources{MemoryBytes: 1600 << 20}, RequiredPodAddresses: 1,
		Nodes: []model.SchedulingNodeDecision{{CandidateID: "candidate-1", RuntimeMatches: true, ResourceInventoryObserved: true, Allocatable: model.SchedulingResources{MemoryBytes: 32 << 30}, Requested: model.SchedulingResources{MemoryBytes: 31 << 30}, EffectiveRequested: model.SchedulingResources{MemoryBytes: 31 << 30}, Budget: model.SchedulingResources{MemoryBytes: 30923764531}, Rejections: []model.SchedulingRejection{{Code: "memory_request_budget_exceeded", Message: "requested=33285996544B + demand=1677721600B > budget=30923764531B (allocatable=34359738368B, ratio=0.9)"}}}}}
}

func TestSchedulingDiagnosisNeverReconstructsMissingHistoricalInputs(t *testing.T) {
	t.Parallel()
	op := model.Operation{Status: model.OperationStatusFailed, ErrorMessage: "no eligible storage attachment node matches runtime/capacity constraints:"}
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
	d := schedulingFixtureDecision()
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
