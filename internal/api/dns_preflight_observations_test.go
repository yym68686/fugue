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
	_, failed := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
	_, ready := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
	s.finishCaptureTrace(ready, "ready", nil, 0, 0)
	s.finishCaptureTrace(failed, "source", context.DeadlineExceeded, 0, 0)
	traffic := s.retainTrafficDiagnosticFailure(trafficDiagnosticFailure{Stage: "current serving publication unavailable"})
	other := s.retainTrafficDiagnosticFailure(trafficDiagnosticFailure{Stage: "publication changed during observation"})
	reads := map[string]dnsInventoryFactRead{
		"private-dns-a": {facts: platformDNSRuntimeFactsResponse{captureSequence: failed.receipt.Sequence}, err: context.DeadlineExceeded},
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
		{NodeHash: captureIdentityHash("private-dns-a"), Projection: "read_failed", FactSequence: failed.receipt.Sequence, FactErrorClass: "deadline", ProbePass: true},
		{NodeHash: captureIdentityHash("private-dns-b"), Projection: "ready", FactSequence: ready.receipt.Sequence, Pass: true, ProbePass: true},
		{NodeHash: captureIdentityHash("private-dns-c"), Projection: "not_dispatched"},
		{NodeHash: captureIdentityHash("private-legacy"), Projection: "inventory_only"},
		{NodeHash: captureIdentityHash("private-dns-b"), Projection: "read_not_projected", FactSequence: ready.receipt.Sequence},
	}
	if !reflect.DeepEqual(first.Nodes[:len(want)], want) {
		t.Fatalf("incorrect read or projection evidence: %+v", first.Nodes)
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
