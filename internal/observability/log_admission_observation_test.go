package observability

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLogAdmissionObservationReportsExactCapacityBranchWithoutChangingAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"ordinary_slots", Config{Enabled: true, QueueSize: 10, MemoryLimitBytes: 1 << 20}},
		{"queued_bytes", Config{Enabled: true, QueueSize: 100, MemoryLimitBytes: 4096}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed, plain := NewPipeline(tc.cfg, nil), NewPipeline(tc.cfg, nil)
			event := Event{Timestamp: time.Unix(100, 0), Kind: EventKindLog, Source: "synthetic-source", Message: strings.Repeat("private-record", 30)}
			var receipt logAdmissionObservation
			for i := 0; ; i++ {
				if i > 100 {
					t.Fatal("fixture did not reach admission bound")
				}
				got := observed.ingestWithAdmission(t.Context(), event, true, &receipt)
				if want := plain.ingest(t.Context(), event, true); got != want {
					t.Fatal("observation changed admission decision")
				}
				if observed.queuedBytes.Load() != plain.queuedBytes.Load() || observed.ordinaryQueuedSlots.Load() != plain.ordinaryQueuedSlots.Load() || observed.received.Load() != plain.received.Load() || observed.dropped.Load() != plain.dropped.Load() || observed.kubernetesLogAdmissionDeferred.Load() != plain.kubernetesLogAdmissionDeferred.Load() {
					t.Fatal("observation changed queue accounting")
				}
				if !got {
					break
				}
				if receipt.Reason != "" {
					t.Fatal("successful admission fabricated a rejection")
				}
			}
			if receipt.Reason != tc.name || receipt.ObservedAt.IsZero() || receipt.EventBytes <= 0 || receipt.ComparedValue <= receipt.ComparedLimit || receipt.QueueCapacity != tc.cfg.QueueSize || receipt.MemoryLimit != tc.cfg.MemoryLimitBytes {
				t.Fatalf("missing exact rejecting comparison: %+v", receipt)
			}
			if tc.name == "ordinary_slots" && (receipt.ComparedLimit != 8 || receipt.ComparedValue != 9 || receipt.OrdinarySlots != 8) {
				t.Fatalf("slot reservation/refund was not captured exactly: %+v", receipt)
			}
			if tc.name == "queued_bytes" && (receipt.ComparedLimit != 3227 || receipt.ComparedValue != receipt.QueuedBytes+receipt.EventBytes) {
				t.Fatalf("byte reservation/refund was not captured exactly: %+v", receipt)
			}
			beforeBytes, beforeSlots := observed.queuedBytes.Load(), observed.ordinaryQueuedSlots.Load()
			if observed.ingestWithAdmission(t.Context(), event, true, &receipt) || observed.queuedBytes.Load() != beforeBytes || observed.ordinaryQueuedSlots.Load() != beforeSlots || observed.dropped.Load() != 0 {
				t.Fatal("rejected replay leaked a reservation or reported a dropped record")
			}
			raw, _ := json.Marshal(receipt)
			if strings.Contains(string(raw), "private-record") || strings.Contains(string(raw), event.Source) {
				t.Fatal("admission receipt leaked event contents")
			}
		})
	}
}

func TestLogAdmissionObservationDistinguishesContextAndPhysicalQueue(t *testing.T) {
	for _, reason := range []string{"context_canceled", "context_deadline", "queue_full"} {
		t.Run(reason, func(t *testing.T) {
			p := NewPipeline(Config{Enabled: true, QueueSize: 8, MemoryLimitBytes: 1 << 20}, nil)
			// Fill physical slots with critical entries, which do not reserve
			// ordinary slots. This isolates the final select from earlier bounds.
			for i := 0; i < cap(p.queue); i++ {
				p.queue <- queuedEvent{critical: true}
			}
			ctx := t.Context()
			if reason == "context_canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else if reason == "context_deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
			}
			var receipt logAdmissionObservation
			if p.ingestWithAdmission(ctx, Event{Kind: EventKindLog, Source: "test"}, true, &receipt) || receipt.Reason != reason || receipt.EventBytes <= 0 || receipt.ComparedLimit != 0 || receipt.ComparedValue != 0 {
				t.Fatalf("wrong rejection branch: %+v", receipt)
			}
			if p.queuedBytes.Load() != 0 || p.ordinaryQueuedSlots.Load() != 0 || p.dropped.Load() != 0 || p.kubernetesLogAdmissionDeferred.Load() != 1 {
				t.Fatal("diagnostic changed reservation refund or loss semantics")
			}
		})
	}
}

func TestLogAdmissionObservationRetainsIndependentImmutableSourceAndGapEvidence(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	at := time.Now().UTC()
	receipt := logAdmissionObservation{ObservedAt: at, Reason: "queued_bytes", ComparedValue: 12, ComparedLimit: 10}
	p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: at, Outcome: "queue_rejected", LastAdmission: &receipt, GapEvents: 1})
	receipt.Reason = "mutated-input"
	p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: at.Add(time.Second), Outcome: "drained"})
	check := func() {
		t.Helper()
		v, err := p.DiagnosticSources(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		result := v.(map[string]any)
		row := result["sources"].([]logSourceObservation)[0]
		gap := result["gap_history"].(map[string]any)["receipts"].([]logGapObservation)[0]
		if row.Outcome != "drained" || row.LastAdmission.Reason != "queued_bytes" || !row.LastAdmission.ObservedAt.Equal(at) || gap.LastAdmission.Reason != "queued_bytes" {
			t.Fatal("recovery lost evidence or caller mutation rewrote it")
		}
		row.LastAdmission.Reason = "mutated-output"
		gap.LastAdmission.Reason = "mutated-output"
	}
	check()
	check()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: at, Outcome: "drained"})
				v, _ := p.DiagnosticSources(t.Context())
				v.(map[string]any)["sources"].([]logSourceObservation)[0].LastAdmission.Reason = "caller-private"
			}
		}()
	}
	wg.Wait()
	check()
}
