package frontnetwork

import (
	"context"
	"testing"
	"time"
)

func TestPublicFrontObservationBudgetIsBounded(t *testing.T) {
	var budget ObservationBudget
	now := time.Now()
	if delay, ok := budget.reserve("edge-a", now); !ok || delay != 0 {
		t.Fatal("initial observation rejected", delay, ok)
	}
	if delay, ok := budget.reserve("edge-a", now.Add(time.Millisecond)); !ok || delay != time.Second-time.Millisecond {
		t.Fatal("one bounded waiter not reserved", delay, ok)
	}
	if _, ok := budget.reserve("edge-a", now.Add(2*time.Millisecond)); ok {
		t.Fatal("multiple waiters admitted")
	}
	if delay, ok := budget.reserve("edge-a", now.Add(time.Second)); !ok || delay != time.Second {
		t.Fatal("next slot not spaced from the reserved observation", delay, ok)
	}
	for index := 0; index < 256; index++ {
		budget.last[string(rune(index))] = now
	}
	if _, ok := budget.reserve("extra", now); ok {
		t.Fatal("cardinality bound not enforced")
	}
	if delay, ok := budget.reserve("extra", now.Add(2*time.Minute)); !ok || delay != 0 {
		t.Fatal("cardinality bound or expiry failed")
	}
}

func TestPublicFrontObservationWaitHonorsRequestDeadline(t *testing.T) {
	var budget ObservationBudget
	started := time.Now()
	if !budget.Wait(context.Background(), "edge-a") {
		t.Fatal("initial observation rejected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if budget.Wait(ctx, "edge-a") {
		t.Fatal("canceled waiter admitted")
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("wait exceeded request deadline")
	}
	if budget.Wait(context.Background(), "edge-a") {
		t.Fatal("canceled reservation let another caller burst the budget")
	}
}

func TestPublicFrontObservationWaitPreservesSpacing(t *testing.T) {
	var budget ObservationBudget
	if !budget.Wait(context.Background(), "edge-a") {
		t.Fatal("initial observation rejected")
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if !budget.Wait(ctx, "edge-a") || time.Since(started) < 900*time.Millisecond {
		t.Fatal("waiter did not observe the original rate budget")
	}
}
