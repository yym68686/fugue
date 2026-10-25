package dnsserver

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

func TestRecoveryHistoryObservationExplainsResetWithoutRenewingProof(t *testing.T) {
	scheduler := &dnsReadinessScheduler{}
	policy := &platformconfig.DNSReadinessPolicy{FactFreshnessSeconds: 60}
	start := time.Now().UTC()
	observe := func(delta time.Duration, ready bool, reason string) dnsReadinessFact {
		at := start.Add(delta)
		fact := dnsReadinessFact{ProbeID: "probe", Ready: ready, Reason: reason, Proof: routeprobe.Proof{CheckedAt: at, ValidUntil: at.Add(time.Minute)}}
		scheduler.observeProbeHistoryKey("authorized-history", &fact, policy, at)
		if !fact.Proof.CheckedAt.Equal(at) || !fact.Proof.ValidUntil.Equal(at.Add(time.Minute)) || fact.Ready != ready {
			t.Fatal("observation changed proof or readiness")
		}
		return fact
	}
	first := observe(0, true, "")
	if first.HistoryObservation == nil || first.HistoryObservation.ResetReason != "history_started" || first.HistoryObservation.PreviousSuccessCount != 0 || len(first.ProbeHistory) != 1 {
		t.Fatal("new history provenance missing", first)
	}
	key := first.HistoryObservation.KeyDigest
	if !strings.HasPrefix(key, "sha256:") || len(key) != 71 {
		t.Fatal("history key leaked or lost digest")
	}
	second := observe(10*time.Second, true, "")
	if second.HistoryObservation.ResetReason != "history_started" || !second.HistoryObservation.ResetAt.Equal(start) || len(second.ProbeHistory) != 2 {
		t.Fatal("success rewrote reset history")
	}
	second.HistoryObservation.ResetReason = "caller-mutation"
	failed := observe(20*time.Second, false, "traffic_release_mismatch")
	if failed.HistoryObservation.ResetReason != "negative_observation" || failed.HistoryObservation.ObservationReason != "traffic_release_mismatch" || failed.HistoryObservation.PreviousSuccessCount != 2 || failed.HistoryObservation.PreviousSuccessAt != start.Add(10*time.Second).Format(time.RFC3339Nano) || len(failed.ProbeHistory) != 0 {
		t.Fatal("negative observation cause was lost", failed.HistoryObservation)
	}
	repeated := observe(25*time.Second, false, "probe_unavailable")
	if !repeated.HistoryObservation.ResetAt.Equal(start.Add(20*time.Second)) || repeated.HistoryObservation.PreviousSuccessCount != 2 {
		t.Fatal("repeated failure erased original reset cause")
	}
	recovered := observe(30*time.Second, true, "")
	if recovered.HistoryObservation.ResetReason != "negative_observation" || len(recovered.ProbeHistory) != 1 {
		t.Fatal("first new success hid its failure barrier")
	}
	gap := observe(100*time.Second, true, "")
	if gap.HistoryObservation.ResetReason != "observation_gap" || gap.HistoryObservation.PreviousSuccessCount != 1 || gap.HistoryObservation.PreviousSuccessAt != start.Add(30*time.Second).Format(time.RFC3339Nano) || len(gap.ProbeHistory) != 1 || gap.HistoryObservation.KeyDigest != key {
		t.Fatal("observation gap was not distinguished", gap.HistoryObservation)
	}
	other := dnsReadinessFact{ProbeID: "probe", Ready: true, Proof: gap.Proof}
	scheduler.observeProbeHistoryKey("different-authority-history", &other, policy, start.Add(100*time.Second))
	if other.HistoryObservation.KeyDigest == key || other.HistoryObservation.ResetReason != "history_started" {
		t.Fatal("new authority inherited prior reset provenance")
	}
}

func TestRecoveryHistoryObservationCannotGrantAvailability(t *testing.T) {
	now := time.Now().UTC()
	fact := dnsReadinessFact{Ready: true, Proof: routeprobe.Proof{CheckedAt: now, ValidUntil: now.Add(time.Minute)}, ProbeHistory: []time.Time{now}, HistoryObservation: &dnsProbeHistoryObservation{ResetReason: "history_started", ResetAt: now.Add(-time.Hour), PreviousSuccessCount: 16}}
	if dnsAvailabilityRecovered(availabilityTestPolicy(), fact, time.Minute, now) {
		t.Fatal("diagnostic history granted missing actual recovery")
	}
	fact.ProbeHistory = []time.Time{now.Add(-10 * time.Second), now}
	if !dnsAvailabilityRecovered(availabilityTestPolicy(), fact, time.Minute, now) {
		t.Fatal("diagnostic history changed valid recovery")
	}
	fact.Ready = false
	if dnsAvailabilityRecovered(availabilityTestPolicy(), fact, time.Minute, now) {
		t.Fatal("diagnostics overrode negative proof")
	}
}
