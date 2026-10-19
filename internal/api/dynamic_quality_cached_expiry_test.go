package api

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDynamicQualityCachedReceiptDoesNotOutliveRouteProof(t *testing.T) {
	receipt, route := businessPublicationFixture(t)
	route.BusinessExploration = nil
	route.PathPrefix = ""
	captured := receipt.Snapshot.CapturedAt
	selection, deadline, err := compileBoundPhysicalQualitySelectionWithDeadline(receipt, captured)
	if err != nil {
		t.Fatal(err)
	}
	if !deadline.After(captured) || !deadline.Before(captured.Add(time.Duration(route.Policy.EvidenceMaxAgeSeconds)*time.Second)) {
		t.Fatalf("proof deadline does not account for shorter route proof lifetime: captured=%s deadline=%s", captured, deadline)
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	entry := dynamicQualityEntry{ContextDigest: "same", Compiled: compiledPhysicalDNSSelection{Selection: selection, Evidence: raw, proofFreshUntil: deadline}}
	later := deadline.Add(time.Nanosecond)
	if reason := dynamicQualityEntryUnavailable(entry, "same", nil, route.Policy, later); reason != "network_evidence_unavailable" {
		t.Fatalf("got reason %q", reason)
	}
}
