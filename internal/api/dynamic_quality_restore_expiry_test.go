package api

import (
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
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
	state := store.New(t.TempDir() + "/state.json")
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	job := dynamicQualityJob{NodeID: fact.NodeID, Key: fact.NodeID + "\x00" + fact.Hostname, EvidenceHostname: route.Hostname, Route: route}
	s := &Server{store: state}
	parent := model.PlatformArtifact{Metadata: map[string]string{"input_snapshot_digest": digest, "intent_generation": "intent-a", "policy_generation": "policy-a"}}
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
