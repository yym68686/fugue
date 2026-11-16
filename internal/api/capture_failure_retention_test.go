package api

import (
	"context"
	"errors"
	"testing"
)

func TestAgentFailureReceiptsSurviveUnrelatedSuccessfulReads(t *testing.T) {
	s := &Server{}
	_, failure := s.beginCaptureTrace(context.Background(), "agent-request")
	s.finishCaptureTrace(failure, "grant_constraint", errors.New("private-error"), 0, 1)
	for range captureReceiptLimit * 3 {
		_, success := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
		s.finishCaptureTrace(success, "ready", nil, 0, 0)
	}
	snapshot := s.captureSnapshot().(map[string]any)
	for _, row := range snapshot["receipts"].([]captureReceipt) {
		if row.Kind == "agent-request" {
			t.Fatal("fixture did not evict the original shared receipt")
		}
	}
	failures, ok := snapshot["agent_failures"].(map[string]any)
	if !ok {
		t.Fatal("Agent failure evidence was evicted by unrelated DNS reads")
	}
	rows := failures["receipts"].([]captureReceipt)
	if len(rows) != 1 || rows[0].Sequence != failure.receipt.Sequence || rows[0].Outcome != "grant_constraint" || failures["total"] != uint64(1) || failures["evicted"] != uint64(0) {
		t.Fatal("failure evidence lost or misattributed", failures)
	}
	for range captureReceiptLimit + 2 {
		_, next := s.beginCaptureTrace(context.Background(), "agent-request")
		s.finishCaptureTrace(next, "authority", errors.New("private-error"), 0, 1)
	}
	if rows[0].Sequence != failure.receipt.Sequence {
		t.Fatal("later eviction mutated a returned failure snapshot")
	}
	failures = s.captureSnapshot().(map[string]any)["agent_failures"].(map[string]any)
	if len(failures["receipts"].([]captureReceipt)) != captureReceiptLimit || failures["total"] != uint64(captureReceiptLimit+3) || failures["evicted"] != uint64(3) {
		t.Fatal("failure evidence retention is unbounded or unaccounted", failures)
	}
}

func TestAgentFailureReceiptsDoNotClassifyOtherCaptureKindsAsRequests(t *testing.T) {
	s := &Server{}
	for _, kind := range []string{"agent-preview", "agent-internal", "dns-runtime-facts"} {
		_, trace := s.beginCaptureTrace(context.Background(), kind)
		s.finishCaptureTrace(trace, "authority", errors.New("private-error"), 0, 1)
	}
	_, trace := s.beginCaptureTrace(context.Background(), "agent-request")
	s.finishCaptureTrace(trace, "ok", nil, 1, 1)
	failures, ok := s.captureSnapshot().(map[string]any)["agent_failures"].(map[string]any)
	if !ok || failures["total"] != uint64(0) || len(failures["receipts"].([]captureReceipt)) != 0 {
		t.Fatal("preview, internal, DNS or successful reads were counted as Agent failures", failures)
	}
}
