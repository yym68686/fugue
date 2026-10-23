package dnsserver

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/miekg/dns"
)

func availabilityTestPolicy() *model.DNSAvailabilityPolicy {
	return &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-b", "edge-a"}, RecoverySuccesses: 2, RecoveryMinSeconds: 10}
}

func TestAvailabilityRecoversQualityStoppedEndpointWithoutChangingSignedState(t *testing.T) {
	state, now := businessUnavailableServingState(t)
	entry := &state.zones["example.test"].records["target.example.test"][0]
	entry.record.AnswerPolicy.Availability = availabilityTestPolicy()
	for i := range entry.facts {
		at := entry.facts[i].Proof.CheckedAt
		entry.facts[i].ProbeHistory = []time.Time{at.Add(-10 * time.Second), at}
	}
	before, _ := json.Marshal(struct {
		Record model.EdgeDNSRecord
		Facts  []dnsReadinessFact
	}{entry.record, entry.facts})
	query := new(dns.Msg)
	query.SetQuestion("target.example.test.", dns.TypeA)
	receipt := captureDecision(t, state, query, "198.51.100.1:53", now)
	if receipt.RCode != dns.RcodeSuccess || len(receipt.RRSet) != 1 || receipt.Records[0].Answered[0].EdgeID != "edge-a" || receipt.Records[0].SelectionResult != "availability_readiness_failover" {
		t.Fatalf("authorized ready backup was lost to a quality stop: %+v", receipt.Records)
	}
	replay, err := ReplayDNSDecision(receipt)
	if err != nil || !replay.Matched {
		t.Fatalf("fallback is not replayable: %+v %v", replay, err)
	}
	after, _ := json.Marshal(struct {
		Record model.EdgeDNSRecord
		Facts  []dnsReadinessFact
	}{entry.record, entry.facts})
	if !bytes.Equal(before, after) {
		t.Fatal("query changed signed configuration or proof facts")
	}
	// An omitted policy retains the historical, intentionally restrictive result.
	entry.record.AnswerPolicy.Availability = nil
	if got := state.answer(query, "", now); got.Rcode != dns.RcodeServerFailure {
		t.Fatal("old policy was silently activated")
	}
}

func TestAvailabilityCannotBypassMissingMembershipProofOrRecovery(t *testing.T) {
	for _, name := range []string{"not_in_pool", "no_history", "duplicate", "future", "short_window", "expired", "wrong_identity", "negative", "gap", "all_failed"} {
		t.Run(name, func(t *testing.T) {
			state, now := businessUnavailableServingState(t)
			entry := &state.zones["example.test"].records["target.example.test"][0]
			entry.record.AnswerPolicy.Availability = availabilityTestPolicy()
			for i := range entry.facts {
				f := &entry.facts[i]
				at := f.Proof.CheckedAt
				f.ProbeHistory = []time.Time{at.Add(-10 * time.Second), at}
				if f.Proof.EdgeID != "edge-a" {
					continue
				}
				switch name {
				case "not_in_pool":
					entry.record.AnswerPolicy.Availability.OrderedEdgeIDs = []string{"edge-b"}
				case "no_history":
					f.ProbeHistory = nil
				case "duplicate":
					f.ProbeHistory = []time.Time{at, at}
				case "future":
					f.ProbeHistory = []time.Time{at, now.Add(time.Second)}
				case "short_window":
					f.ProbeHistory = []time.Time{at.Add(-time.Second), at}
				case "expired":
					f.Proof.ValidUntil = now.Add(-time.Second)
				case "wrong_identity":
					f.Proof.EdgeID = "foreign"
				case "negative", "all_failed":
					f.Ready = false
				case "gap":
					f.ProbeHistory = []time.Time{at.Add(-24 * time.Hour), at}
				}
			}
			query := new(dns.Msg)
			query.SetQuestion("target.example.test.", dns.TypeA)
			if got := state.answer(query, "", now); got.Rcode != dns.RcodeServerFailure || len(got.Answer) > 0 {
				t.Fatalf("unsafe fallback: %s", got)
			}
		})
	}
}

func TestAvailabilityCanUseAuthorizedCandidateOutsideQualityPool(t *testing.T) {
	state, now := businessUnavailableServingState(t)
	entry := &state.zones["example.test"].records["target.example.test"][0]
	// The record's signed topology and all-path readiness plan authorize edge-c;
	// the independently signed business experiment still contains only a and b.
	for i := range entry.record.Candidates {
		if entry.record.Candidates[i].EdgeID == "edge-a" {
			entry.record.Candidates[i].EdgeID = "edge-c"
		}
	}
	for i := range entry.plan.Records[0].Targets {
		if entry.plan.Records[0].Targets[i].EdgeID == "edge-a" {
			entry.plan.Records[0].Targets[i].EdgeID = "edge-c"
		}
	}
	for i := range entry.plan.Probes {
		if entry.plan.Probes[i].EdgeID == "edge-a" {
			entry.plan.Probes[i].EdgeID = "edge-c"
		}
	}
	for i := range entry.facts {
		f := &entry.facts[i]
		if f.Proof.EdgeID == "edge-a" {
			f.Proof.EdgeID = "edge-c"
		}
		at := f.Proof.CheckedAt
		f.ProbeHistory = []time.Time{at.Add(-10 * time.Second), at}
	}
	entry.record.AnswerPolicy.Availability = availabilityTestPolicy()
	entry.record.AnswerPolicy.Availability.OrderedEdgeIDs = []string{"edge-b", "edge-a", "edge-c"}
	query := new(dns.Msg)
	query.SetQuestion("target.example.test.", dns.TypeA)
	receipt := captureDecision(t, state, query, "", now)
	if receipt.RCode != dns.RcodeSuccess || len(receipt.Records[0].Answered) != 1 || receipt.Records[0].Answered[0].EdgeID != "edge-c" {
		t.Fatal("quality membership incorrectly limited availability pool", receipt.Records)
	}
	if replay, err := ReplayDNSDecision(receipt); err != nil || !replay.Matched {
		t.Fatal("outside-quality-pool fallback did not replay", err)
	}
}

func TestAvailabilityProbeHistoryRequiresDistinctSuccessAfterFailure(t *testing.T) {
	_, _, policy, facts, now := queryExecutionFixture()
	scheduler := &dnsReadinessScheduler{}
	f := facts[0]
	first := f.Proof.CheckedAt
	scheduler.observeProbeHistory(&f, &policy, f.Proof.CheckedAt)
	scheduler.observeProbeHistory(&f, &policy, f.Proof.CheckedAt)
	if len(f.ProbeHistory) != 1 {
		t.Fatal("reused observation advanced recovery")
	}
	f.Proof.CheckedAt = first.Add(10 * time.Second)
	f.Proof.ValidUntil = f.Proof.CheckedAt.Add(time.Minute)
	scheduler.observeProbeHistory(&f, &policy, f.Proof.CheckedAt)
	if !dnsAvailabilityRecovered(availabilityTestPolicy(), f, time.Minute, now.Add(10*time.Second)) {
		t.Fatal("fresh independent probes did not recover")
	}
	failed := f
	failed.Ready = false
	failed.Reason = "probe_unavailable"
	scheduler.observeProbeHistory(&failed, &policy, failed.Proof.CheckedAt)
	f.Proof.CheckedAt = f.Proof.CheckedAt.Add(time.Second)
	scheduler.observeProbeHistory(&f, &policy, f.Proof.CheckedAt)
	if len(f.ProbeHistory) != 1 {
		t.Fatal("failure did not reset recovery")
	}
}

func TestAvailabilityIntermittentSuccessRequiresSustainedRecovery(t *testing.T) {
	_, _, policy, facts, start := queryExecutionFixture()
	scheduler := &dnsReadinessScheduler{}
	fact := facts[0]
	availability := availabilityTestPolicy()
	availability.RecoverySuccesses, availability.RecoveryMinSeconds = 3, 20
	// A limited link can answer occasionally for a long time. Elapsed time
	// and cumulative successes cannot substitute for uninterrupted recovery.
	for i, successful := range []bool{true, false, true, false, true, true, true} {
		at := start.Add(time.Duration(i) * 10 * time.Second)
		fact.Ready, fact.Proof.CheckedAt, fact.Proof.ValidUntil = successful, at, at.Add(time.Minute)
		scheduler.observeProbeHistory(&fact, &policy, at)
		if got := dnsAvailabilityRecovered(availability, fact, time.Minute, at); got != (i == 6) {
			t.Fatalf("intermittent response misclassified at observation %d: %v", i, got)
		}
	}
}

func TestAvailabilityFailedRefreshDoesNotReuseRecoveryProgress(t *testing.T) {
	_, plan, policy, facts, now := queryExecutionFixture()
	for i := range facts {
		at := facts[i].Proof.CheckedAt
		facts[i].ProbeHistory = []time.Time{at.Add(-10 * time.Second), at}
	}
	current := slices.Clone(facts)
	current[0].Ready = false
	current[0].Reason = "probe_unavailable"
	got := retainValidDNSReadinessFacts(&plan, &policy, facts, current, now)
	if !got[0].Ready || got[0].Proof.ValidUntil != facts[0].Proof.ValidUntil || len(got[0].ProbeHistory) != 0 {
		t.Fatal("failed refresh retained availability progress or changed original deadline")
	}
	current[0].Reason = "not_observed"
	got = retainValidDNSReadinessFacts(&plan, &policy, facts, current, now)
	if len(got[0].ProbeHistory) != 2 {
		t.Fatal("scheduler omission invented a failure")
	}
}

func TestAvailabilityControlPlaneLossPreservesProbeContinuityWithoutReusingSuccess(t *testing.T) {
	_, _, policy, facts, now := queryExecutionFixture()
	history := &dnsProbeHistoryStore{}
	serving := (&dnsReadinessScheduler{}).shareHistory(history)
	retained := (&dnsReadinessScheduler{}).shareHistory(history)
	fact := facts[0]
	serving.observeProbeHistory(&fact, &policy, fact.Proof.CheckedAt)
	fact.Proof.CheckedAt = fact.Proof.CheckedAt.Add(10 * time.Second)
	fact.Proof.ValidUntil = fact.Proof.CheckedAt.Add(time.Minute)
	serving.observeProbeHistory(&fact, &policy, fact.Proof.CheckedAt)
	// Another observer consuming the same proof is not another success.
	retained.observeProbeHistory(&fact, &policy, fact.Proof.CheckedAt)
	if len(fact.ProbeHistory) != 2 {
		t.Fatal("scheduler switch duplicated or lost existing observations")
	}
	fact.Proof.CheckedAt = fact.Proof.CheckedAt.Add(time.Second)
	retained.observeProbeHistory(&fact, &policy, fact.Proof.CheckedAt)
	if len(fact.ProbeHistory) != 3 || !dnsAvailabilityRecovered(availabilityTestPolicy(), fact, time.Minute, now.Add(12*time.Second)) {
		t.Fatal("control-plane loss invented recovery downtime")
	}
	failed := fact
	failed.Ready = false
	retained.observeProbeHistory(&failed, &policy, failed.Proof.CheckedAt)
	fact.Proof.CheckedAt = fact.Proof.CheckedAt.Add(time.Second)
	serving.observeProbeHistory(&fact, &policy, fact.Proof.CheckedAt)
	if len(fact.ProbeHistory) != 1 {
		t.Fatal("another scheduler hid an actual failure")
	}
	// A restarted process has no runtime history and must probe afresh.
	restarted := &dnsReadinessScheduler{}
	restarted.observeProbeHistory(&fact, &policy, fact.Proof.CheckedAt)
	if len(fact.ProbeHistory) != 1 || dnsAvailabilityRecovered(availabilityTestPolicy(), fact, time.Minute, now.Add(13*time.Second)) {
		t.Fatal("restart manufactured continuity")
	}
}

func TestAvailabilityDelayedSuccessCannotCrossAnotherSchedulersFailure(t *testing.T) {
	_, _, policy, facts, now := queryExecutionFixture()
	shared := &dnsProbeHistoryStore{}
	serving := (&dnsReadinessScheduler{}).shareHistory(shared)
	retained := (&dnsReadinessScheduler{}).shareHistory(shared)
	stale := facts[0]
	failed := stale
	failed.Ready = false
	failed.Proof.CheckedAt = stale.Proof.CheckedAt.Add(5 * time.Second)
	retained.observeProbeHistory(&failed, &policy, failed.Proof.CheckedAt)
	// A probe started before the failure returns later on another scheduler.
	serving.observeProbeHistory(&stale, &policy, stale.Proof.CheckedAt)
	fresh := facts[0]
	fresh.Proof.CheckedAt = fresh.Proof.CheckedAt.Add(10 * time.Second)
	fresh.Proof.ValidUntil = fresh.Proof.CheckedAt.Add(time.Minute)
	serving.observeProbeHistory(&fresh, &policy, fresh.Proof.CheckedAt)
	if dnsAvailabilityRecovered(availabilityTestPolicy(), fresh, time.Minute, now.Add(10*time.Second)) {
		t.Fatal("delayed success counted across an intervening failure")
	}
}
