package api

import (
	"encoding/json"
	"strings"

	"fugue/internal/model"
)

type schedulingEvidencePart struct {
	ID       string                    `json:"scheduling_decision_id"`
	Part     int                       `json:"scheduling_decision_part"`
	Parts    int                       `json:"scheduling_decision_parts"`
	Decision *model.SchedulingDecision `json:"scheduling_decision"`
}

// Read only durable observations. A current node inspection cannot fill a
// missing historical input or retroactively prove a scheduling root cause.
func latestSchedulingDecision(items []model.OperationEvidence) (*model.SchedulingDecision, string, bool) {
	var latest model.OperationEvidence
	found := false
	for _, e := range items {
		if e.Type == model.OperationEvidenceTypeSchedulingDecision && (!found || e.CollectedAt.After(latest.CollectedAt) || e.CollectedAt.Equal(latest.CollectedAt) && e.ID > latest.ID) {
			latest, found = e, true
		}
	}
	decode := func(e model.OperationEvidence) schedulingEvidencePart {
		var part schedulingEvidencePart
		b, err := json.Marshal(e.Payload)
		if err == nil {
			_ = json.Unmarshal(b, &part)
		}
		return part
	}
	p := decode(latest)
	if !found || p.ID == "" || p.Decision == nil || p.Parts <= 0 || p.Parts > 1000 {
		return nil, latest.ID, false
	}
	parts := make(map[int]schedulingEvidencePart)
	for _, e := range items {
		if e.Type != model.OperationEvidenceTypeSchedulingDecision {
			continue
		}
		part := decode(e)
		if part.ID == p.ID && part.Parts == p.Parts && part.Part > 0 && part.Part <= part.Parts && part.Decision != nil && part.Decision.ObservedAt.Equal(p.Decision.ObservedAt) && part.Decision.Stage == p.Decision.Stage && part.Decision.Outcome == p.Decision.Outcome {
			parts[part.Part] = part
		}
	}
	out := *p.Decision
	out.Nodes = nil
	for i := 1; i <= p.Parts; i++ {
		part, ok := parts[i]
		if !ok {
			out.MissingEvidence = appendUniqueStrings(out.MissingEvidence, "scheduling_decision_parts")
			continue
		}
		out.Nodes = append(out.Nodes, part.Decision.Nodes...)
	}
	return &out, latest.ID, len(parts) == p.Parts && len(out.MissingEvidence) == 0
}

func attachSchedulingDecisionDiagnosis(op model.Operation, evidence []model.OperationEvidence, diagnosis *model.OperationDiagnosis) bool {
	decision, evidenceID, complete := latestSchedulingDecision(evidence)
	diagnosis.SchedulingDecision = decision
	message := strings.TrimSpace(op.ErrorMessage)
	isSchedulingFailure := op.Status == model.OperationStatusFailed && (strings.Contains(message, "storage placement rejected") ||
		strings.Contains(message, "new storage placement blocked:") || strings.Contains(message, "storage placement has no eligible node") || strings.Contains(message, "no eligible storage attachment node") ||
		strings.Contains(message, "zero-downtime rollout blocked: no ") ||
		strings.Contains(message, "scheduling observation failed:"))
	if !isSchedulingFailure {
		return false
	}
	diagnosis.Category = "scheduling-rejected"
	diagnosis.Confidence = model.OperationEvidenceConfidenceInsufficientEvidence
	diagnosis.ConfirmedCause, diagnosis.ProbableCause = nil, nil
	diagnosis.PrimaryEvidenceID = evidenceID
	diagnosis.MissingEvidence = []string{"scheduling_decision"}
	diagnosis.Hint = "Inspect this operation's scheduling evidence. Preserve existing volumes; do not rebuild or blindly retry without a confirmed placement-state change."
	diagnosis.RecommendedNextActions = []string{"inspect per-candidate resource requests, policy budgets, runtime constraints and storage affinity in operation evidence"}
	if strings.Contains(message, "scheduling observation failed:") {
		diagnosis.Category = "scheduling-observation-failed"
	}
	if decision == nil {
		return true
	}
	diagnosis.MissingEvidence = append([]string(nil), decision.MissingEvidence...)
	if !complete {
		diagnosis.MissingEvidence = appendUniqueStrings(diagnosis.MissingEvidence, "scheduling_decision_parts")
	}
	if decision.Outcome != "rejected" || !complete || !strings.Contains(message, decision.Summary()) {
		if len(diagnosis.MissingEvidence) == 0 {
			diagnosis.MissingEvidence = []string{"matching_scheduling_failure_decision"}
		}
		return true
	}
	diagnosis.Summary = decision.Summary()
	diagnosis.Confidence = model.OperationEvidenceConfidenceConfirmed
	diagnosis.MissingEvidence = nil
	diagnosis.ConfirmedCause = &model.OperationDiagnosisCause{Category: "scheduling_rejected", Source: "scheduling_decision", Reason: "scheduling_rejected", Message: decision.Summary()}
	diagnosis.Evidence = appendUniqueStrings(diagnosis.Evidence, "primary evidence "+evidenceID+" (scheduling_decision): "+decision.Summary())
	return true
}
