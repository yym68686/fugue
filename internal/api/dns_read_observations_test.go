package api

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDNSReadTimingsAggregateExactWallIntervalsWithoutChangingContext(t *testing.T) {
	s := &Server{}
	ctx, trace := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
	at := trace.receipt.StartedAt
	deadline := at.Add(10 * time.Second)
	trace.observeDNSReadStage(dnsReadInitialSource, dnsReadAssignmentResolution, at.Add(time.Second), at.Add(3*time.Second), deadline, true)
	trace.observeDNSReadStage(dnsReadInitialSource, dnsReadAssignmentResolution, at.Add(4*time.Second), at.Add(11*time.Second), deadline, true)
	// Completion order is not start order; bounds must still retain the first
	// actual start and latest finish, not the last callback's offsets.
	trace.observeDNSReadStage(dnsReadInitialSource, dnsReadAssignmentResolution, at.Add(2*time.Second), at.Add(2500*time.Millisecond), deadline, true)
	trace.observeDNSReadStage(dnsReadAuthorityRecheck, dnsReadConsumerInventory, at.Add(12*time.Second), at.Add(13*time.Second), deadline, true)
	trace.observeDNSReadStage(dnsReadValidation, dnsReadTotal, at.Add(14*time.Second), at.Add(15*time.Second), time.Time{}, false)
	phaseCtx, finish := startDNSReadPhase(ctx, dnsReadBackend)
	if phaseCtx.Done() != ctx.Done() || phaseCtx.Err() != ctx.Err() {
		t.Fatal("phase changed cancellation")
	}
	finish()
	s.finishCaptureTrace(trace, "authority_recheck", errDNSRuntimeFacts, 0, 0)
	rows := captureReceipts(t, s)
	if len(rows) != 1 || len(rows[0].DNSRead) != 4 || len(rows[0].Events) != 0 || rows[0].Dropped != 0 {
		t.Fatal("timings interfered with rejection events or fixed retention", rows)
	}
	want := dnsReadStageObservation{Phase: "initial_source", Stage: "assignment_resolution", Calls: 3,
		ElapsedMillis: 9500, MaxMillis: 7000, HasDeadline: true, FirstStartOffset: 1000,
		LastFinishOffset: 11000, FirstRemainingMillis: 9000, LastRemainingMillis: -1000}
	if rows[0].DNSRead[0] != want {
		t.Fatal("lost exact aggregate timing", rows[0].DNSRead[0], want)
	}
	recheck := rows[0].DNSRead[2]
	if recheck.FirstRemainingMillis != -2000 || recheck.LastRemainingMillis != -3000 || recheck.Calls != 1 || !recheck.HasDeadline {
		t.Fatal("expired budget clamped to zero or confused with no deadline", recheck)
	}
	unbounded := rows[0].DNSRead[3]
	if unbounded.HasDeadline || unbounded.FirstRemainingMillis != 0 || unbounded.LastRemainingMillis != 0 {
		t.Fatal("unbounded context invented a deadline", unbounded)
	}
	before := append([]dnsReadStageObservation(nil), rows[0].DNSRead...)
	trace.observeDNSReadStage(dnsReadInitialSource, dnsReadTotal, at, at.Add(time.Hour), deadline, true)
	if !reflect.DeepEqual(before, rows[0].DNSRead) {
		t.Fatal("late timing rewrote completed receipt")
	}
}

func TestDNSReadTimingsPreserveDeadlineCancellationAndShortCircuit(t *testing.T) {
	s := &Server{}
	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ctx, trace := s.beginCaptureTrace(parent, "dns-runtime-facts")
	ctx, finish := startDNSReadPhase(ctx, dnsReadAuthorityRecheck)
	wantDeadline, _ := parent.Deadline()
	gotDeadline, ok := ctx.Deadline()
	if !ok || gotDeadline != wantDeadline || ctx.Done() != parent.Done() {
		t.Fatal("phase changed request budget")
	}
	cancel()
	if ctx.Err() != context.Canceled {
		t.Fatal("phase hid cancellation")
	}
	finish()
	s.finishCaptureTrace(trace, "authority_recheck", errDNSRuntimeFacts, 0, 0)
	row := captureReceipts(t, s)[0].DNSRead[0]
	if row.Phase != "authority_recheck" || row.Stage != "total" || !row.HasDeadline || row.Calls != 1 {
		t.Fatal("cancellation lost timing", row)
	}
	// No recorder means no context method is invoked, including Err/Deadline.
	counted := &dnsRecheckContext{Context: context.Background(), err: context.DeadlineExceeded}
	startDNSReadStage(counted, dnsReadTotal)()
	if counted.reads != 0 {
		t.Fatal("disabled timing evaluated context.Err")
	}
}

func TestDNSReadTimingsBoundedConcurrentAndRetainedWithFailure(t *testing.T) {
	s := &Server{}
	for range dnsCaptureFailureLimit + 1 {
		ctx, trace := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
		at := trace.receipt.StartedAt
		var workers sync.WaitGroup
		for range 4 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for phase := range dnsReadPhaseCount {
					for stage := range dnsReadStageCount {
						trace.observeDNSReadStage(phase, stage, at, at.Add(time.Millisecond), at.Add(time.Second), true)
					}
				}
			}()
		}
		workers.Wait()
		trace.observeDNSReadStage(dnsReadPhaseCount, dnsReadTotal, at, at, at, true)
		trace.observeDNSReadStage(dnsReadInitialSource, dnsReadStageCount, at, at, at, true)
		for range captureEventLimit + 1 {
			observeCapture(ctx, captureEvent{Stage: "authority_recheck_rejected", Rejection: "context"})
		}
		s.finishCaptureTrace(trace, "authority_recheck", errDNSRuntimeFacts, 0, 0)
	}
	for range captureReceiptLimit + 1 {
		_, trace := s.beginCaptureTrace(context.Background(), "agent-request")
		at := trace.receipt.StartedAt
		trace.observeDNSReadStage(dnsReadInitialSource, dnsReadTotal, at, at, at, true)
		s.finishCaptureTrace(trace, "ready", nil, 0, 0)
	}
	snapshot := s.captureSnapshot().(map[string]any)
	failures := snapshot["dns_failures"].(map[string]any)
	if failures["total"] != uint64(dnsCaptureFailureLimit+1) || failures["evicted"] != uint64(1) {
		t.Fatal("failure accounting changed", failures)
	}
	for _, receipt := range failures["receipts"].([]captureReceipt) {
		if len(receipt.DNSRead) != int(dnsReadPhaseCount)*int(dnsReadStageCount) || len(receipt.Events) != captureEventLimit || receipt.Dropped != 1 {
			t.Fatal("timings consumed rejection-event budget or exceeded bounds", receipt)
		}
		for _, row := range receipt.DNSRead {
			if row.Calls != 4 || row.ElapsedMillis != 4 || row.MaxMillis != 1 {
				t.Fatal("concurrent updates lost", row)
			}
		}
	}
	for _, receipt := range snapshot["receipts"].([]captureReceipt) {
		if len(receipt.DNSRead) != 0 {
			t.Fatal("non-DNS read retained DNS timings")
		}
	}
	raw, err := json.Marshal(snapshot)
	if err != nil || len(raw) > 1<<20 || strings.Contains(string(raw), "private-") {
		t.Fatal("observation exceeded socket budget or privacy boundary", len(raw), err)
	}
}
