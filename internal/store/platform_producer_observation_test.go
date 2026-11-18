package store

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestProducerGuardObservationAggregatesIntoFixedSlots(t *testing.T) {
	ctx, mutation := startPlatformMutationTrace(context.Background(), "platform-release")
	at := mutation.started
	mutation.now = func() time.Time { return at }
	for i := 0; i < 100; i++ {
		trace := beginPlatformProducerGuardTrace(ctx)
		at = at.Add(5 * time.Millisecond)
		trace.advance(producerGuardParent)
		at = at.Add(7 * time.Millisecond)
		trace.advance(producerGuardDNSOutput)
		at = at.Add(11 * time.Millisecond)
		trace.finish()
	}
	observation := mutation.producerGuardObservation()
	want := []platformMutationStage{{"read_lanes", 500}, {"parent_integrity", 700}, {"dns_output", 1100}}
	if observation.FinalPhase != "dns_output" || !reflect.DeepEqual(observation.Stages, want) {
		t.Fatal("guard samples did not aggregate into fixed slots", observation)
	}
	recorder := &platformMutationRecorder{}
	recorder.record(mutation, ErrConflict)
	receipt := recorder.snapshot()["receipts"].([]platformMutationReceipt)[0]
	if !receipt.Failed || receipt.Failure.ErrorClass != "conflict" || !reflect.DeepEqual(receipt.ProducerGuard, observation) {
		t.Fatal("guard timing lost original operation result", receipt)
	}
	// A retained receipt owns its projection, even if a trace were reused.
	observation.Stages[0].Milliseconds++
	if reflect.DeepEqual(receipt.ProducerGuard, observation) {
		t.Fatal("retained guard observation aliases another projection")
	}
	var disabled *platformProducerGuardTrace
	disabled.advance(producerGuardParent)
	disabled.finish()
	if beginPlatformProducerGuardTrace(context.Background()) != nil {
		t.Fatal("unobserved context unexpectedly enabled a trace")
	}
	_, empty := startPlatformMutationTrace(context.Background(), "platform-release")
	raw, err := json.Marshal(platformMutationReceipt{ProducerGuard: empty.producerGuardObservation()})
	if err != nil || strings.Contains(string(raw), "producer_guard") || empty.producerGuardObservation() != nil {
		t.Fatal("empty guard observation was fabricated", err)
	}
}
