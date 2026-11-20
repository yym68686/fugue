package api

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDNSPreflightReceiptsJoinExactReadsAndStayBounded(t *testing.T) {
	s := &Server{}
	failedCtx, failed := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
	_, ready := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
	s.finishCaptureTrace(ready, "ready", nil, 0, 0)
	traffic := s.retainTrafficDiagnosticFailure(trafficDiagnosticFailure{Stage: "current serving publication unavailable"})
	other := s.retainTrafficDiagnosticFailure(trafficDiagnosticFailure{Stage: "publication changed during observation"})
	authority := captureDNSAuthorityEvidence(
		model.PlatformConsumerAssignment{ExpectedConsumerSetID: "expected-set", FencingToken: 8937},
		model.PlatformConsumerInstance{ExpectedConsumerSetID: "observed-set", FencingToken: 8927},
	)
	observeCapture(failedCtx, captureEvent{
		Stage: "source_assignment_rejected", Rejection: "fact_expected_set", DNSAuthority: authority,
	})
	observeCapture(failedCtx, captureEvent{Stage: "source_rejected", Rejection: "consumer_assignment_unavailable"})
	readFailure := dnsPreflightFactFailureFromReceipt(s.finishCaptureTrace(failed, "source", errDNSRuntimeFacts, 0, 0))
	reads := map[string]dnsInventoryFactRead{
		"private-dns-a": {facts: platformDNSRuntimeFactsResponse{captureSequence: failed.receipt.Sequence, preflightFailure: readFailure}, err: errDNSRuntimeFacts},
		"private-dns-b": {facts: platformDNSRuntimeFactsResponse{captureSequence: ready.receipt.Sequence}},
	}
	response := model.DNSDelegationPreflightResponse{
		GeneratedAt: time.Now().UTC(), Zone: "private-zone", ProbeName: "private-probe",
		Checks: []model.DNSDelegationPreflightCheck{
			{Name: "healthy_dns_nodes", Message: "private-error"},
			{Name: "route_dns_invariant"},
			{Name: "healthy_dns_nodes"},
			{Name: "private-check"},
			{Name: "dns_inventory", Pass: true},
		},
		Nodes: []model.DNSDelegationNodeCheck{
			{PhysicalNodeID: "private-dns-a", ServingObservation: &model.DNSNodeServingObservation{State: "unknown"}, ProbePass: true},
			{PhysicalNodeID: "private-dns-b", ServingObservation: &model.DNSNodeServingObservation{State: "ready"}, Pass: true, ProbePass: true},
			{PhysicalNodeID: "private-dns-c", ServingObservation: &model.DNSNodeServingObservation{State: "unknown"}},
			{PhysicalNodeID: "private-legacy", Message: "private-error"},
			{PhysicalNodeID: "private-dns-b", ServingObservation: &model.DNSNodeServingObservation{State: "unknown"}},
		},
	}
	for range dnsPreflightNodeLimit {
		response.Nodes = append(response.Nodes, model.DNSDelegationNodeCheck{PhysicalNodeID: "private-extra"})
	}
	s.retainDNSPreflightObservation(response, reads, traffic)
	snap := func() map[string]any {
		return s.captureSnapshot().(map[string]any)["dns_preflight_failures"].(map[string]any)
	}
	first := snap()["receipts"].([]dnsPreflightFailure)[0]
	if first.GeneratedAt != response.GeneratedAt || first.TrafficFailureSequence == other || first.TrafficFailureSequence != traffic ||
		!reflect.DeepEqual(first.Checks, []dnsPreflightCheckObservation{{Name: "healthy_dns_nodes"}, {Name: "route_dns_invariant"}, {Name: "dns_inventory", Pass: true}}) ||
		len(first.Nodes) != dnsPreflightNodeLimit || first.NodesOmitted != 5 {
		t.Fatalf("inexact or unbounded preflight receipt: %+v", first)
	}
	want := []dnsPreflightNodeObservation{
		{NodeHash: captureIdentityHash("private-dns-a"), Projection: "read_failed", FactSequence: failed.receipt.Sequence, FactErrorClass: "unavailable", FactFailure: readFailure, ProbePass: true},
		{NodeHash: captureIdentityHash("private-dns-b"), Projection: "ready", FactSequence: ready.receipt.Sequence, Pass: true, ProbePass: true},
		{NodeHash: captureIdentityHash("private-dns-c"), Projection: "not_dispatched"},
		{NodeHash: captureIdentityHash("private-legacy"), Projection: "inventory_only"},
		{NodeHash: captureIdentityHash("private-dns-b"), Projection: "read_not_projected", FactSequence: ready.receipt.Sequence},
	}
	if !reflect.DeepEqual(first.Nodes[:len(want)], want) {
		t.Fatalf("incorrect read or projection evidence: %+v", first.Nodes)
	}
	if got := first.Nodes[0].FactFailure; got == nil || got.Predicate == nil || got.Predicate.Rejection != "consumer_assignment_unavailable" || got.Input == nil ||
		got.Input.Stage != "source_assignment_rejected" || got.Input.Rejection != "fact_expected_set" ||
		got.Input.ExpectedSetHash != captureIdentityHash("expected-set") || got.Input.ObservedSetHash != captureIdentityHash("observed-set") ||
		got.Input.ExpectedFence != 8937 || got.Input.ObservedFence != 8927 || got.Input.ObservedSource != "heartbeat" {
		t.Fatalf("preflight did not retain the exact bounded rejected input: %+v", got)
	}
	for range captureReceiptLimit + 1 {
		_, trace := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
		s.finishCaptureTrace(trace, "source", errDNSRuntimeFacts, 0, 0)
	}
	for _, row := range captureReceipts(t, s) {
		if row.Sequence == failed.receipt.Sequence {
			t.Fatal("original child is still in the common ring")
		}
	}
	for _, row := range s.captureSnapshot().(map[string]any)["dns_failures"].(map[string]any)["receipts"].([]captureReceipt) {
		if row.Sequence == failed.receipt.Sequence {
			t.Fatal("original child is still in the DNS failure ring")
		}
	}
	if got := snap()["receipts"].([]dnsPreflightFailure)[0].Nodes[0].FactFailure; !reflect.DeepEqual(got, readFailure) {
		t.Fatalf("child-ring eviction changed parent evidence: %+v", got)
	}
	for range dnsPreflightFailureLimit * 3 {
		s.retainDNSPreflightObservation(model.DNSDelegationPreflightResponse{Pass: true}, nil, 0)
	}
	if snap()["total"] != uint64(1) || snap()["evicted"] != uint64(0) {
		t.Fatal("successful observations displaced failure evidence")
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			for range dnsPreflightFailureLimit {
				s.retainDNSPreflightObservation(response, reads, traffic)
				_ = snap()
			}
		})
	}
	wg.Wait()
	last := snap()
	if last["total"] != uint64(dnsPreflightFailureLimit*2+1) || last["completed"] != uint64(dnsPreflightFailureLimit*5+1) ||
		last["evicted"] != uint64(dnsPreflightFailureLimit+1) || len(last["receipts"].([]dnsPreflightFailure)) != dnsPreflightFailureLimit || first.Sequence != 1 {
		t.Fatalf("ring accounting or detached snapshot violated: %+v", last)
	}
	raw, err := json.Marshal(last)
	if err != nil || len(raw) > 16<<10 || strings.Contains(string(raw), "private-") {
		t.Fatalf("receipt oversized or contains raw input: bytes=%d err=%v", len(raw), err)
	}
}

func TestDNSPreflightFactWitnessLeavesDroppedTerminalPredicateUnknown(t *testing.T) {
	got := dnsPreflightFactFailureFromReceipt(captureReceipt{Outcome: "source", Dropped: 1, Events: []captureEvent{{
		Stage: "source_rejected", Rejection: "consumer_inventory_read",
	}}})
	if got.Outcome != "source" || got.EventsDropped != 1 || got.Predicate != nil || got.Input != nil {
		t.Fatalf("recorded prefix substituted for a dropped terminal predicate: %+v", got)
	}
}

func TestDNSPreflightFactWitnessPreservesOnlySameAttemptSourceExhaustion(t *testing.T) {
	source := captureDNSSourceExhaustion{Inventory: 4, IdentityFiltered: 2, HeartbeatStale: 2}
	event := captureEvent{Stage: "source_rejected", Rejection: "no_current_source", DNSSource: &source}
	initial := dnsPreflightFactFailureFromReceipt(captureReceipt{Outcome: "source", Events: []captureEvent{event}})
	recheck := dnsPreflightFactFailureFromReceipt(captureReceipt{Outcome: "authority_recheck", Events: []captureEvent{
		event, {Stage: "authority_recheck_rejected", Rejection: "source_unavailable"},
	}})
	for _, got := range []*dnsPreflightFactPredicate{initial.Predicate, recheck.Input} {
		if got == nil || got.DNSSource == nil || got.DNSSource == &source || *got.DNSSource != source {
			t.Fatalf("same-attempt exhaustion not detached: %+v", got)
		}
	}
	unrelated := dnsPreflightFactFailureFromReceipt(captureReceipt{Outcome: "authority_recheck", Events: []captureEvent{
		event, {Stage: "authority_recheck_rejected", Rejection: "context"},
	}})
	dropped := dnsPreflightFactFailureFromReceipt(captureReceipt{Outcome: "source", Dropped: 1, Events: []captureEvent{event}})
	if initial.Input != nil || unrelated.Input != nil || dropped.Predicate != nil || dropped.Input != nil {
		t.Fatal("unrelated or incomplete read inherited exhaustion counts")
	}
}

func TestDNSPreflightFactWitnessRetainsParentDigestComparison(t *testing.T) {
	parent := captureDNSParentEvidence("expected-parent", "reported-parent", "backend-uid")
	got := dnsPreflightFactFailureFromReceipt(captureReceipt{Outcome: "snapshot_validation", Events: []captureEvent{
		{Stage: "source_heartbeat_rejected", Rejection: "consumer_heartbeat_stale"},
		{Stage: "snapshot_validation_rejected", Rejection: "parent_digest", DNSParent: parent},
		{Stage: "snapshot_validation"},
	}})
	if got.Predicate == nil || got.Predicate.Rejection != "parent_digest" ||
		got.Predicate.DNSParent == nil || got.Predicate.DNSParent == parent || *got.Predicate.DNSParent != *parent || got.Input != nil {
		t.Fatalf("preflight did not retain the terminal parent digest comparison: %+v", got)
	}
}
