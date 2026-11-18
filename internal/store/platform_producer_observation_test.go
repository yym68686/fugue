package store

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/platformproducer"
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

func TestProducerGuardObservationDNSOutputIsBoundedAndOwned(t *testing.T) {
	ctx, mutation := startPlatformMutationTrace(context.Background(), "platform-release")
	trace := beginPlatformProducerGuardTrace(ctx)
	trace.advance(producerGuardDNSOutput)
	observe := trace.dnsOutputObserver()
	for range 100 {
		observe(platformproducer.DNSOutputFingerprint, 600*time.Microsecond, platformproducer.DNSOutputCacheNotApplicable)
		observe(platformproducer.DNSOutputCacheLookup, time.Millisecond, platformproducer.DNSOutputCacheHit)
	}
	observe(platformproducer.DNSOutputCacheLookup, 3*time.Millisecond, platformproducer.DNSOutputCacheMiss)
	observe(platformproducer.DNSOutputStageCount, time.Hour, platformproducer.DNSOutputCacheMiss)
	trace.finish()
	receipt := mutation.producerGuardObservation()
	if len(receipt.DNSOutput) != 2 || receipt.DNSOutput[0].Stage != "fingerprint" ||
		receipt.DNSOutput[0].Calls != 100 || receipt.DNSOutput[0].ElapsedMS != 60 ||
		receipt.DNSOutput[1].Stage != "cache_lookup" || receipt.DNSOutput[1].Calls != 101 ||
		receipt.DNSOutput[1].ElapsedMS != 103 {
		t.Fatal("unexpected timing partition", receipt.DNSOutput)
	}
	if *receipt.DNSCache != (platformProducerDNSCacheCounts{Hit: 100, Miss: 1, NotApplicable: 100}) {
		t.Fatal("unexpected cache results", *receipt.DNSCache)
	}
	observe(platformproducer.DNSOutputFingerprint, time.Millisecond, platformproducer.DNSOutputCacheNotApplicable)
	if receipt.DNSOutput[0].Calls != 100 || receipt.DNSCache.NotApplicable != 100 {
		t.Fatal("observation changed after projection")
	}
	var disabled *platformProducerGuardTrace
	if disabled.dnsOutputObserver() != nil {
		t.Fatal("disabled trace installed an observer")
	}
}
