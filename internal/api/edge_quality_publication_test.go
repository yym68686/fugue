package api

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
)

func TestPhysicalPublicationReconstructsMeasuredEvidenceRatherThanTrustingScores(t *testing.T) {
	now := time.Now().UTC()
	sample, proof := networkWitnessFixture(now)
	evidence := dnsserver.QualityAnswerEvidence{EdgeID: sample.EdgeID, Hostname: sample.Hostname, Scope: "global",
		Proofs: []dnsserver.QualityRouteProof{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, Hostname: sample.Hostname, Path: sample.PathPrefix, Proof: proof}}}
	snapshot := edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: now, Hostname: sample.Hostname, TrafficClass: sample.TrafficClass, Scope: "global",
		Policy: edgequality.DefaultNetworkPolicy(), Candidates: []edgequality.Candidate{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID}}, NetworkSamples: []model.EdgeNetworkSample{sample}}
	bindPhysicalQualityEvidence(&snapshot, evidence)
	if len(snapshot.Observations) != 1 || validateBoundPhysicalQualitySnapshot(snapshot, evidence) != nil {
		t.Fatal("valid raw observation was not reconstructed", snapshot)
	}
	for _, test := range []struct {
		name string
		edit func(*edgequality.Snapshot)
	}{
		{"invented_cooldown", func(value *edgequality.Snapshot) { since := now.Add(-time.Hour); value.LastSwitchAt = &since }},
		{"wrong_incumbent", func(value *edgequality.Snapshot) { value.CurrentEdgeID = "edge-other" }},
		{"wrong_scope", func(value *edgequality.Snapshot) { value.Scope = "tcp_peer:203.0.113.0/24" }},
		{"forged_value", func(value *edgequality.Snapshot) { changed := 0.1; value.Observations[0].ServiceNetworkMS = &changed }},
		{"raw_missing", func(value *edgequality.Snapshot) { value.NetworkSamples = nil }},
		{"measurement_omitted", func(value *edgequality.Snapshot) { value.Observations = nil }},
		{"measurement_duplicated", func(value *edgequality.Snapshot) {
			value.Observations = append(value.Observations, value.Observations[0])
		}},
		{"invented_throughput", func(value *edgequality.Snapshot) { rate := 1000000.0; value.Observations[0].DownloadBPS = &rate }},
		{"forged_route", func(value *edgequality.Snapshot) { value.Candidates[0].RouteGeneration = "unverified" }},
		{"invented_probe_time", func(value *edgequality.Snapshot) {
			later := now.Add(time.Minute)
			value.Candidates[0].ProofObservedAt = &later
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, _ := json.Marshal(snapshot)
			var changed edgequality.Snapshot
			if err := json.Unmarshal(raw, &changed); err != nil {
				t.Fatal(err)
			}
			test.edit(&changed)
			if validateBoundPhysicalQualitySnapshot(changed, evidence) == nil {
				t.Fatal("accepted reconstructed-evidence mismatch")
			}
		})
	}
	snapshot.Observations = append(snapshot.Observations, edgequality.Observation{ID: "diagnostic", ClientSource: "legacy_unverified", ServiceSource: "legacy_unverified", Diagnostics: map[string]float64{"ttfb_ms": 90000}})
	if err := validateBoundPhysicalQualitySnapshot(snapshot, evidence); err != nil {
		t.Fatal("business diagnostics were treated as network evidence", err)
	}
}

func TestPhysicalAliasKeepsActualQuerySeparateFromMeasuredService(t *testing.T) {
	now := time.Now().UTC()
	sample, proof := networkWitnessFixture(now)
	evidence := dnsserver.QualityAnswerEvidence{EdgeID: sample.EdgeID, Hostname: "alias.example.test", Scope: "global", Proofs: []dnsserver.QualityRouteProof{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, Hostname: sample.Hostname, Path: sample.PathPrefix, Proof: proof}}}
	snapshot := edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: now, Hostname: sample.Hostname, DNSHostname: evidence.Hostname, TrafficClass: sample.TrafficClass, Scope: "global",
		Policy: edgequality.DefaultDeliveryNetworkPolicy(), Candidates: []edgequality.Candidate{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID}}, NetworkSamples: []model.EdgeNetworkSample{sample}}
	bindPhysicalQualityEvidence(&snapshot, evidence)
	if len(snapshot.Observations) != 1 || validateBoundPhysicalQualitySnapshot(snapshot, evidence) != nil || snapshot.NetworkSamples[0].Hostname != sample.Hostname {
		t.Fatal("alias discarded or relabeled its declared service evidence", snapshot)
	}
	receipt, err := edgequality.Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := edgequality.Replay(receipt); err != nil {
		t.Fatal(err)
	}
	snapshot.DNSHostname = "foreign.example.test"
	if validateBoundPhysicalQualitySnapshot(snapshot, evidence) == nil {
		t.Fatal("service measurements rebound to an unrelated DNS query")
	}
	snapshot.DNSHostname = evidence.Hostname
	snapshot.Hostname = "foreign-service.example.test"
	if validateBoundPhysicalQualitySnapshot(snapshot, evidence) == nil {
		t.Fatal("alias admitted an undeclared service")
	}
}

func TestPhysicalPublicationRejectsAbsentActualDNSReceipt(t *testing.T) {
	snapshot := edgequality.Snapshot{Schema: edgequality.Schema, Hostname: "app.example.test", Scope: "global", TrafficClass: "streaming", CapturedAt: time.Now().UTC(), Policy: edgequality.DefaultNetworkPolicy()}
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`{}`), json.RawMessage(`{"write_succeeded":true}`)} {
		snapshot.ActualDNSReceipt = raw
		receipt, err := edgequality.Capture(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := compileBoundPhysicalQualitySelection(receipt, snapshot.CapturedAt); err == nil {
			t.Fatal("unbound shadow receipt became serving authority")
		}
	}
}
