package model

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func businessPolicyFixture() BusinessEdgeExplorationPolicy {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return BusinessEdgeExplorationPolicy{Version: BusinessEdgeExplorationVersion, IntentID: "intent-a", Mode: "observe", BaselineEdgeID: "edge-a", CandidateEdgeIDs: []string{"edge-a", "edge-b"},
		InitialDNSPublications: map[string]BusinessDNSPublicationAnchor{"dns-a": {Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", FencingToken: 1}},
		StartsAt:               start, ExpiresAt: start.Add(time.Hour), ExplorationBasisPoints: 100, MaximumCandidateBasisPoints: 1000, MaximumStepBasisPoints: 100,
		BucketSeconds: 60, RequiredBuckets: 3, MinimumConnectionsPerBucket: 3, CooldownSeconds: 60, EvidenceMaxAgeSeconds: 120,
		MinimumCohortCoverageBasisPoints: 9000, RTTAdvantageRatio: .15, DeliveryAdvantageRatio: .2, MaximumRTTRegressionRatio: .1,
		MaximumDeliveryRegressionRatio: .1, MaximumRetransmissionIncrease: .01, StopRTTMS: 3000, StopRetransmissionRate: .2}
}

func TestBusinessDNSBaselinesAreExplicitIndependentAndBounded(t *testing.T) {
	policy := businessPolicyFixture()
	anchor := policy.InitialDNSPublications["dns-a"]
	anchor.BaselineEdgeID = "edge-b"
	policy.InitialDNSPublications["dns-b"] = anchor
	for nodeID, expected := range map[string]string{"dns-a": "edge-a", "dns-b": "edge-b"} {
		resolved, err := BusinessExplorationPolicyForDNS(policy, nodeID)
		if err != nil || resolved.BaselineEdgeID != expected {
			t.Fatal("node baseline differs", nodeID, resolved.BaselineEdgeID, err)
		}
		resolved.CandidateEdgeIDs[0] = "changed"
		delete(resolved.InitialDNSPublications, "dns-a")
	}
	if policy.BaselineEdgeID != "edge-a" || policy.CandidateEdgeIDs[0] != "edge-a" || len(policy.InitialDNSPublications) != 2 {
		t.Fatal("resolution mutated explicit intent")
	}
	if _, err := BusinessExplorationPolicyForDNS(policy, "unknown"); err == nil {
		t.Fatal("undeclared DNS node inherited another node's anchor")
	}
	anchor.BaselineEdgeID = "undeclared"
	policy.InitialDNSPublications["dns-b"] = anchor
	if ValidateBusinessEdgeExplorationPolicy(policy) == nil {
		t.Fatal("undeclared baseline accepted")
	}
	for _, raw := range []string{
		`{"digest":"value","fencing_token":1,"baseline_edge_id":null}`,
		`{"digest":"value","fencing_token":1,"baseline_edge_id":""}`,
		`{"digest":"value","fencing_token":1,"unknown":true}`,
		`{"digest":"value","fencing_token":null}`,
	} {
		if json.Unmarshal([]byte(raw), &anchor) == nil {
			t.Fatal("accepted invalid anchor shape", raw)
		}
	}
}

func TestUnifiedBusinessDNSIdentityCannotChooseQualityBaseline(t *testing.T) {
	policy := businessPolicyFixture()
	policy.Version = BusinessEdgeUnifiedVersion
	policy.MaximumCandidateBasisPoints = 10000
	policy.MinimumConnectionsPerBucket = 1
	policy.Unified = &BusinessUnifiedPolicy{MinimumConnectionsPerCohort: 5, RTTAdvantageMS: 20, PromotionMinimumConnections: 8, PromotionMinimumCoverageBasisPoints: 9000}
	anchor := policy.InitialDNSPublications["dns-a"]
	anchor.BaselineEdgeID = "edge-b"
	policy.InitialDNSPublications["dns-b"] = anchor
	for _, nodeID := range []string{"dns-a", "dns-b"} {
		resolved, err := BusinessExplorationPolicyForDNS(policy, nodeID)
		if err != nil || resolved.BaselineEdgeID != "edge-a" {
			t.Fatal("DNS identity changed business baseline", nodeID, err)
		}
	}
	raw, _ := json.Marshal(policy)
	var decoded BusinessEdgeExplorationPolicy
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*BusinessEdgeExplorationPolicy){
		func(value *BusinessEdgeExplorationPolicy) { value.Unified = nil },
		func(value *BusinessEdgeExplorationPolicy) { value.MaximumCandidateBasisPoints = 1000 },
		func(value *BusinessEdgeExplorationPolicy) { value.Unified.PromotionMinimumConnections = 1 },
		func(value *BusinessEdgeExplorationPolicy) { value.Unified.RTTAdvantageMS = math.NaN() },
	} {
		copy := CloneBusinessEdgeExplorationPolicy(policy)
		mutate(&copy)
		if ValidateBusinessEdgeExplorationPolicy(copy) == nil {
			t.Fatal("accepted invalid unified controls")
		}
	}
}

func TestBusinessExplorationRequiresExplicitBoundedIntent(t *testing.T) {
	valid := businessPolicyFixture()
	raw, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	var decoded BusinessEdgeExplorationPolicy
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for key := range fields {
		saved := fields[key]
		delete(fields, key)
		partial, _ := json.Marshal(fields)
		if json.Unmarshal(partial, &decoded) == nil {
			t.Errorf("accepted omitted field %s", key)
		}
		fields[key] = json.RawMessage("null")
		nullable, _ := json.Marshal(fields)
		if json.Unmarshal(nullable, &decoded) == nil {
			t.Errorf("accepted null field %s", key)
		}
		fields[key] = saved
	}
	for name, change := range map[string]func(*BusinessEdgeExplorationPolicy){
		"unknown mode":    func(policy *BusinessEdgeExplorationPolicy) { policy.Mode = "automatic" },
		"missing anchors": func(policy *BusinessEdgeExplorationPolicy) { policy.InitialDNSPublications = nil },
		"unfenced anchor": func(policy *BusinessEdgeExplorationPolicy) {
			policy.InitialDNSPublications["dns-a"] = BusinessDNSPublicationAnchor{Digest: policy.InitialDNSPublications["dns-a"].Digest}
		},
		"invalid DNS node": func(policy *BusinessEdgeExplorationPolicy) {
			policy.InitialDNSPublications["bad\nnode"] = policy.InitialDNSPublications["dns-a"]
		},
		"unbounded lease": func(policy *BusinessEdgeExplorationPolicy) {
			policy.ExpiresAt = policy.StartsAt.Add(8 * 24 * time.Hour)
		},
		"invalid identity":    func(policy *BusinessEdgeExplorationPolicy) { policy.IntentID = "bad\nidentity" },
		"duplicate candidate": func(policy *BusinessEdgeExplorationPolicy) { policy.CandidateEdgeIDs = []string{"edge-a", "edge-a"} },
		"missing baseline":    func(policy *BusinessEdgeExplorationPolicy) { policy.BaselineEdgeID = "edge-c" },
		"zero connections":    func(policy *BusinessEdgeExplorationPolicy) { policy.MinimumConnectionsPerBucket = 0 },
		"large exploration":   func(policy *BusinessEdgeExplorationPolicy) { policy.ExplorationBasisPoints = 1001 },
		"large adjustment":    func(policy *BusinessEdgeExplorationPolicy) { policy.MaximumStepBasisPoints = 1001 },
		"infinite stop":       func(policy *BusinessEdgeExplorationPolicy) { policy.StopRTTMS = math.Inf(1) },
		"nan advantage":       func(policy *BusinessEdgeExplorationPolicy) { policy.DeliveryAdvantageRatio = math.NaN() },
	} {
		t.Run(name, func(t *testing.T) {
			policy := businessPolicyFixture()
			change(&policy)
			if ValidateBusinessEdgeExplorationPolicy(policy) == nil {
				t.Fatal("accepted unsafe intent")
			}
		})
	}
}

func TestBusinessExplorationPolicyCloneOwnsAnchors(t *testing.T) {
	original := businessPolicyFixture()
	cloned := CloneBusinessEdgeExplorationPolicy(original)
	cloned.CandidateEdgeIDs[0] = "different-edge"
	delete(cloned.InitialDNSPublications, "dns-a")
	if original.CandidateEdgeIDs[0] != "edge-a" || len(original.InitialDNSPublications) != 1 {
		t.Fatal("clone shares mutable intent with original")
	}
}
