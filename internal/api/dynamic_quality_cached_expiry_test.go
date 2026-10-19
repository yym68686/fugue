package api

import (
	"encoding/json"
	"reflect"
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
	wantDeadline := captured.Add(time.Duration(route.Policy.EvidenceMaxAgeSeconds)*time.Second - time.Second)
	if !deadline.Equal(wantDeadline) {
		t.Fatalf("quality evidence deadline=%s want=%s; historical route lease must not shorten quality lifetime", deadline, wantDeadline)
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	entry := dynamicQualityEntry{ContextDigest: "same", Compiled: compiledPhysicalDNSSelection{Selection: selection, Evidence: raw, proofFreshUntil: deadline}}
	for _, at := range []time.Time{captured, captured.Add(31 * time.Second), deadline} {
		if reason := dynamicQualityEntryUnavailable(entry, "same", nil, route.Policy, at); reason != "" {
			t.Fatalf("still-valid historical quality receipt rejected at %s: %s", at, reason)
		}
		verified, err := compileBoundPhysicalQualitySelection(receipt, at)
		if err != nil || !reflect.DeepEqual(verified, selection) {
			t.Fatalf("deadline disagrees with unchanged publication semantics at %s: %v", at, err)
		}
	}
	later := deadline.Add(time.Nanosecond)
	if reason := dynamicQualityEntryUnavailable(entry, "same", nil, route.Policy, later); reason != "network_evidence_unavailable" {
		t.Fatalf("got reason %q", reason)
	}
	if _, err := compileBoundPhysicalQualitySelection(receipt, later); err == nil {
		t.Fatal("expired primary should also fail independent publication validation")
	}
	state := dynamicQualityState{entries: map[string]dynamicQualityEntry{"query": entry}}
	state.finish("query", dynamicQualityEntry{ContextDigest: "same", Failure: "capture failed"}, route.Policy, captured.Add(time.Minute))
	if !reflect.DeepEqual(state.entries["query"].Compiled, entry.Compiled) {
		t.Fatal("failed refresh discarded still-valid quality evidence")
	}
	state.finish("query", dynamicQualityEntry{ContextDigest: "same", Failure: "capture failed"}, route.Policy, later)
	if state.entries["query"].Compiled.Selection != nil {
		t.Fatal("failed refresh extended expired quality evidence")
	}
}
