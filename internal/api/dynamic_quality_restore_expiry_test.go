package api

import (
	"encoding/json"
	"fugue/internal/platformconfig"
	"testing"
	"time"
)

func TestDynamicQualityRestoreRetainsHistoricalProofLifetime(t *testing.T) {
	receipt, route := businessPublicationFixture(t)
	route.BusinessExploration = nil
	route.PathPrefix = ""
	at := receipt.Snapshot.CapturedAt
	selection, err := compileBoundPhysicalQualitySelection(receipt, at)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(receipt)
	projection, _, _, _ := physicalQueryProjectionFixture(t)
	projection.Intent.Generation = "intent-a"
	fact := projection.RuntimeSnapshot.DNSSelections[0]
	fact.PhysicalSelection = selection
	fact.PhysicalEvidence = raw
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: "intent-a", PolicyGeneration: "policy-a", DNSSelections: []platformconfig.DNSSelectionObservation{fact}}
	job := dynamicQualityJob{NodeID: fact.NodeID, Key: fact.NodeID + "\x00" + fact.Hostname, EvidenceHostname: route.Hostname, Route: route}
	s, parent, _ := qualityRestoreInputFixture(t, snapshot)
	s.restorePublishedQualityEvidenceAt(projection, []dynamicQualityJob{job}, parent, func() time.Time { return at.Add(time.Minute) })
	entry := s.dynamicQuality.entries[job.Key]
	if entry.Compiled.Selection == nil || !entry.Compiled.proofFreshUntil.Equal(at.Add(599*time.Second)) {
		t.Fatal("restored receipt lost its quality proof-age bound", entry.Compiled.proofFreshUntil)
	}
	if why := dynamicQualityEntryUnavailable(entry, entry.ContextDigest, nil, route.Policy, at.Add(time.Minute)); why != "" {
		t.Fatal("historical route lease incorrectly limited restored quality evidence", why)
	}
	if why := dynamicQualityEntryUnavailable(entry, entry.ContextDigest, nil, route.Policy, at.Add(599*time.Second+time.Nanosecond)); why != "network_evidence_unavailable" {
		t.Fatal("expired learning proof remained eligible", why)
	}
}
