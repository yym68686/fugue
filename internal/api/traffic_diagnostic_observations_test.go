package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestTrafficFailureReceiptsBoundedPrivateAndIndependent(t *testing.T) {
	s := &Server{}
	now := time.Now().UTC()
	set := model.PlatformExpectedConsumerSet{ID: "private-expected", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, Revision: 8}
	binding := &platformcontrol.ConsumerReleaseBinding{ReleaseSetID: "private-parent", ArtifactReleaseID: "private-release", ReleaseChannel: "full", FencingToken: 9}
	status := model.PlatformConsumerConvergenceStatus{EvaluatedAt: now, RequiredExpected: 10, RequiredPassing: 1, UnexpectedConsumers: []string{"private-unexpected"}}
	for i := range trafficAssessmentLimit + 3 {
		status.Assessments = append(status.Assessments, model.PlatformConsumerEvidenceAssessment{
			ConsumerID: fmt.Sprintf("private-consumer-%d", i), Required: true, State: model.InvariantEvidenceStateFail,
			Reasons:    []string{"consumer heartbeat is stale", "consumer receipt belongs to another expectation or ReleaseSet", "private-error", "private-error", "consumer compatibility capability is missing: private-capability"},
			FreshUntil: &now, Observed: &model.PlatformConsumerInstance{ReleaseSetID: "private-observed-parent", ExpectedConsumerSetID: "private-observed-set", LastHeartbeatAt: now, FencingToken: 7, GenerationSequence: 42, CredentialID: "private-credential", TokenID: "private-token", Nonce: "private-nonce"},
		})
	}
	r := trafficConvergenceObservation(set, binding, status)
	r.Stage = "required consumers have not converged for edge_route_bundle"
	if len(r.Assessments) != trafficAssessmentLimit || r.Omitted != 3 || r.Unexpected != 1 || r.Publication.FencingToken != 9 || r.Revision != 8 {
		t.Fatal("same-read assessment bounds lost", r)
	}
	row := r.Assessments[0]
	if len(row.Reasons) != 4 || row.Reasons[0] != "heartbeat_stale" || row.Reasons[1] != "publication_mismatch" || row.Fence != 7 || row.Generation != 42 || row.FreshUntil != now || !row.Observed {
		t.Fatal("assessment evidence lost", row)
	}
	// Returned evidence cannot retain mutable consumer facts or bindings.
	status.Assessments[0].Observed.FencingToken = 100
	status.Assessments[0].Reasons[0] = "changed"
	binding.FencingToken = 100
	s.retainTrafficDiagnosticFailure(r)
	for range captureReceiptLimit * 3 {
		_, trace := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
		s.finishCaptureTrace(trace, "ready", nil, 0, 0)
	}
	first := s.captureSnapshot().(map[string]any)["traffic_failures"].(map[string]any)
	rows := first["receipts"].([]trafficDiagnosticFailure)
	if len(rows) != 1 || rows[0].Assessments[0].Fence != 7 || rows[0].Publication.FencingToken != 9 || first["total"] != uint64(1) {
		t.Fatal("unrelated captures displaced or mutated traffic evidence", first)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 8 {
				s.retainTrafficDiagnosticFailure(trafficConvergenceObservation(set, binding, status))
				_ = s.captureSnapshot()
			}
		})
	}
	wg.Wait()
	last := s.captureSnapshot().(map[string]any)["traffic_failures"].(map[string]any)
	if len(last["receipts"].([]trafficDiagnosticFailure)) != trafficFailureLimit || last["total"] != uint64(33) || last["evicted"] != uint64(33-trafficFailureLimit) || rows[0].Sequence != 1 {
		t.Fatal("retention budget, accounting or detached snapshot violated", last)
	}
	raw, err := json.Marshal(last)
	if err != nil || len(raw) > 128<<10 || strings.Contains(string(raw), "private-") {
		t.Fatal("private values or oversized receipts", len(raw), err)
	}
	if trafficDiagnosticState("private-state") != "unrecognized" || trafficDiagnosticReason("private-reason") != "unrecognized" {
		t.Fatal("unrecognized values were retained")
	}
}

func TestTrafficFailureReceiptsPreserveMissingEvidenceAndEmptyRequiredSet(t *testing.T) {
	r := trafficConvergenceObservation(model.PlatformExpectedConsumerSet{}, nil, model.PlatformConsumerConvergenceStatus{
		Assessments: []model.PlatformConsumerEvidenceAssessment{{ConsumerID: "missing", State: model.InvariantEvidenceStateUnknown, Reasons: []string{"consumer heartbeat is missing"}}},
	})
	if r.Publication != nil || len(r.Assessments) != 1 || r.Assessments[0].Observed || r.Required != 0 || r.Passing != 0 || !r.Assessments[0].HeartbeatAt.IsZero() || r.Assessments[0].Reasons[0] != "heartbeat_missing" {
		t.Fatal("missing evidence was fabricated", r)
	}
}
