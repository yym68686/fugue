package api

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/edgequality"
)

func TestDynamicQualityCachedReceiptDoesNotOutliveRouteProof(t *testing.T) {
	receipt, route := businessPublicationFixture(t)
	route.BusinessExploration = nil
	route.PathPrefix = ""
	captured := receipt.Snapshot.CapturedAt
	selection, err := compileBoundPhysicalQualitySelection(receipt, captured)
	if err != nil {
		t.Fatal(err)
	}
	for i := range receipt.Snapshot.Candidates {
		stale := captured.Add(-time.Duration(route.Policy.EvidenceMaxAgeSeconds)*time.Second + time.Second)
		receipt.Snapshot.Candidates[i].ProofObservedAt = &stale
	}
	receipt, err = edgequality.Capture(receipt.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	entry := dynamicQualityEntry{ContextDigest: "same", Compiled: compiledPhysicalDNSSelection{Selection: selection, Evidence: raw}}
	later := captured.Add(2 * time.Second)
	if reason := dynamicQualityEntryUnavailable(entry, "same", nil, route.Policy, later); reason != "network_evidence_unavailable" {
		t.Fatalf("got reason %q", reason)
	}
}
