package api

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
)

func TestQualityCandidateOutsideDNSObtainsIndependentProofAndCapacityTarget(t *testing.T) {
	now := time.Now().UTC()
	witness := model.EdgeNetworkSample{ID: "candidate-route", EdgeID: "edge-b", EdgeGroupID: "group-b", Hostname: "app.example.test", PathPrefix: "/", TrafficClass: "streaming", Source: "route_tls_witness_v1",
		RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "candidate-bundle", ObservedAt: now.Add(-time.Second), RouteWitness: &model.EdgeNetworkRouteWitness{Address: "8.8.8.8", ValidUntil: now.Add(time.Minute)}}
	if err := model.ValidateEdgeNetworkSample(witness); err != nil {
		t.Fatal(err)
	}
	evidence := dnsserver.QualityAnswerEvidence{EdgeID: "edge-a", Hostname: witness.Hostname, Scope: "global"}
	snapshot := edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: now, Hostname: witness.Hostname, PathPrefix: "/", TrafficClass: "streaming", Scope: "global", CurrentEdgeID: "edge-a", Policy: edgequality.DefaultPairedContinuityPolicy(), Candidates: []edgequality.Candidate{{EdgeID: "edge-b", EdgeGroupID: "group-b", HardGates: []string{}}}, NetworkSamples: []model.EdgeNetworkSample{witness}}
	bindPhysicalQualityEvidence(&snapshot, evidence)
	if !snapshot.Candidates[0].RouteProofVerified || len(snapshot.Candidates[0].HardGates) != 0 {
		t.Fatal("candidate was required to win DNS before it could be measured", snapshot.Candidates)
	}
	augmented := independentQualityEvidence(snapshot, evidence)
	targets := qualityCapacityTargets([]model.EdgeNode{{ID: "edge-b", EdgeGroupID: "group-b", PublicIPv4: "8.8.8.8"}}, augmented)
	if len(targets) != 1 || targets[0].edgeID != "edge-b" || len(evidence.Candidates) != 0 {
		t.Fatal("measurement eligibility changed DNS evidence or lost independent capacity target", targets)
	}
	if err := validateBoundPhysicalQualitySnapshot(snapshot, evidence); err != nil {
		t.Fatal("independent proof could not be reconstructed", err)
	}
	snapshot.Candidates[0].RouteGeneration = "sha256:" + strings.Repeat("b", 64)
	if err := validateBoundPhysicalQualitySnapshot(snapshot, evidence); err == nil {
		t.Fatal("forged independent proof accepted")
	}
}

func TestIndependentQualityProofRejectsExpiredForeignOrAmbiguousWitness(t *testing.T) {
	now := time.Now().UTC()
	base := model.EdgeNetworkSample{ID: "route-a", EdgeID: "edge-b", EdgeGroupID: "group-b", Hostname: "app.example.test", PathPrefix: "/", TrafficClass: "streaming", Source: "route_tls_witness_v1", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle", ObservedAt: now, RouteWitness: &model.EdgeNetworkRouteWitness{Address: "8.8.8.8", ValidUntil: now.Add(time.Minute)}}
	for _, scenario := range []string{"expired", "foreign_group", "wrong_path", "future", "ambiguous"} {
		sample := base
		witness := *base.RouteWitness
		sample.RouteWitness = &witness
		switch scenario {
		case "expired":
			sample.ObservedAt = now.Add(-time.Hour)
			witness.ValidUntil = now.Add(-time.Second)
		case "foreign_group":
			sample.EdgeGroupID = "other-group"
		case "wrong_path":
			sample.PathPrefix = "/other"
		case "future":
			sample.ObservedAt = now.Add(time.Second)
		case "ambiguous":
			sample.RouteDigest = "sha256:" + strings.Repeat("b", 64)
		}
		snapshot := edgequality.Snapshot{CapturedAt: now, Hostname: base.Hostname, PathPrefix: "/", TrafficClass: "streaming", Policy: edgequality.DefaultPairedContinuityPolicy(), Candidates: []edgequality.Candidate{{EdgeID: "edge-b", EdgeGroupID: "group-b"}}, NetworkSamples: []model.EdgeNetworkSample{sample}}
		if scenario == "ambiguous" {
			snapshot.NetworkSamples = append(snapshot.NetworkSamples, base)
		}
		evidence := independentQualityEvidence(snapshot, dnsserver.QualityAnswerEvidence{})
		if len(evidence.Proofs) != 0 || len(evidence.Candidates) != 0 {
			t.Fatal("unusable independent evidence accepted", scenario, evidence)
		}
	}
}
