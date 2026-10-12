package dnsserver

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"github.com/miekg/dns"
)

func businessSelectionRecord(t *testing.T, now time.Time) model.EdgeDNSRecord {
	t.Helper()
	record := physicalSelectionTestRecord(now)
	selection := record.AnswerPolicy.PhysicalSelection
	policy := model.BusinessEdgeExplorationPolicy{Version: model.BusinessEdgeExplorationVersion, IntentID: "intent-a", Mode: "observe", BaselineEdgeID: "edge-b", CandidateEdgeIDs: []string{"edge-a", "edge-b"},
		InitialDNSPublications: map[string]model.BusinessDNSPublicationAnchor{"dns-a": {Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", FencingToken: 1}},
		StartsAt:               now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour), ExplorationBasisPoints: 100, MaximumCandidateBasisPoints: 1000, MaximumStepBasisPoints: 50,
		BucketSeconds: 300, RequiredBuckets: 3, MinimumConnectionsPerBucket: 3, CooldownSeconds: 600, EvidenceMaxAgeSeconds: 600,
		MinimumCohortCoverageBasisPoints: 9000, RTTAdvantageRatio: .15, DeliveryAdvantageRatio: .2, MaximumRTTRegressionRatio: .1,
		MaximumDeliveryRegressionRatio: .1, MaximumRetransmissionIncrease: .01, StopRTTMS: 3000, StopRetransmissionRate: .2}
	receipt, err := edgequality.CaptureBusinessExploration(edgequality.BusinessExplorationInput{Policy: policy, CapturedAt: now, Hostname: record.Name, PathPrefix: "/", TrafficClass: "dynamic_api",
		Candidates: []edgequality.Candidate{{EdgeID: "edge-a", EdgeGroupID: "shared", RouteGeneration: selection.EvidenceDigest, RouteProofVerified: true, ProofObservedAt: &now},
			{EdgeID: "edge-b", EdgeGroupID: "shared", RouteGeneration: selection.EvidenceDigest, RouteProofVerified: true, ProofObservedAt: &now}}})
	if err != nil {
		t.Fatal(err)
	}
	selection.Version = model.DNSPhysicalBusinessSelectionVersion
	selection.BusinessAllocation = &model.DNSBusinessAllocation{Policy: policy, State: receipt.Decision.State, ValidUntil: receipt.Decision.ValidUntil, ReceiptDigest: receipt.Digest}
	if err := model.ValidateDNSPhysicalSelection(selection); err != nil {
		t.Fatal(err)
	}
	return record
}

func businessBucketHint(value int) dnsGeoHint {
	return dnsGeoHint{decisionEntropy: &dnsDecisionEntropy{replaying: true, Buckets: []dnsDecisionBucket{{Purpose: "business_allocation", Modulus: 10000, Value: value}}}}
}

func TestBusinessDNSWireTTLDoesNotOutliveAllocationLease(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	record := businessSelectionRecord(t, now)
	record.AnswerPolicy.PhysicalSelection.BusinessAllocation.ValidUntil = now.Add(5 * time.Second)
	for _, elapsed := range []time.Duration{0, 4 * time.Second, 4500 * time.Millisecond, 5 * time.Second} {
		at := now.Add(elapsed)
		answers, _, _ := rrForEdgeDNSRecordWithGeoAuditAt(record, record.Name, dns.TypeA, businessBucketHint(0), nil, nil, at)
		if len(answers) != 1 {
			t.Fatal("lost ready baseline", answers)
		}
		if elapsed < 5*time.Second && time.Duration(answers[0].Header().Ttl)*time.Second > 5*time.Second-elapsed {
			t.Fatal("exploration cache exceeds intent lease", elapsed, answers)
		}
		if elapsed == 5*time.Second && answers[0].(*dns.A).A.String() != record.Candidates[1].IP {
			t.Fatal("expiry did not use baseline", answers)
		}
	}
}

func TestBusinessDNSExecutesExactSignedBasisPointAllocation(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	record := businessSelectionRecord(t, now)
	before, _ := json.Marshal(record)
	counts := map[string]int{}
	for bucket := 0; bucket < 10000; bucket++ {
		hint := businessBucketHint(bucket)
		ordered, decision := edgeDNSOrderedCandidatesWithDecision(record, hint, now, false)
		if len(ordered) != 2 || hint.decisionEntropy.err != nil || hint.decisionEntropy.cursor != 1 {
			t.Fatal(ordered, decision, hint.decisionEntropy.err)
		}
		counts[ordered[0].EdgeID]++
		if ordered[0].EdgeID == "edge-a" && edgeDNSSelectionResult(decision, ordered[:1], nil) != "business_allocated_candidate" {
			t.Fatal("exploration was reported as readiness failover", decision)
		}
	}
	if counts["edge-a"] != 100 || counts["edge-b"] != 9900 || counts["unranked"] != 0 {
		t.Fatal(counts)
	}
	after, _ := json.Marshal(record)
	if string(before) != string(after) {
		t.Fatal("mutated signed allocation")
	}
}

func TestBusinessDNSExpiryPreservesReadyBaselineAndNoLegacyExploration(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	record := businessSelectionRecord(t, now)
	expires := record.AnswerPolicy.PhysicalSelection.BusinessAllocation.ValidUntil
	hint := businessBucketHint(0)
	ordered, decision := edgeDNSOrderedCandidatesWithDecision(record, hint, expires, false)
	if len(ordered) != 2 || ordered[0].EdgeID != "edge-b" || decision.ExplorationKind != "" || hint.decisionEntropy.cursor != 0 {
		t.Fatal("expired allocation continued exploring or invalidated LKG", ordered, decision)
	}
	record.AnswerPolicy.ExplorationPercent = 10
	if ordered, _ := edgeDNSOrderedCandidatesWithDecision(record, businessBucketHint(0), now, false); len(ordered) != 0 {
		t.Fatal("legacy exploration overrode signed allocation")
	}
}

func TestBusinessDNSDoesNotRenormalizeUnreadyCandidateWeight(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	record := businessSelectionRecord(t, now)
	record.Candidates[0].RouteReady = false
	ordered, decision := edgeDNSOrderedCandidatesWithDecision(record, businessBucketHint(0), now, false)
	if len(ordered) != 1 || ordered[0].EdgeID != "edge-b" || decision.ExplorationKind != "" {
		t.Fatal("unready candidate received exploration", ordered, decision)
	}
	record.Candidates[1].TLSReady = false
	ordered, _ = edgeDNSOrderedCandidatesWithDecision(record, businessBucketHint(0), now, false)
	if len(ordered) != 0 {
		t.Fatal("unranked endpoint bypassed signed membership", ordered)
	}
}

func TestBusinessDNSRejectsTamperedBudgetAndHistoricalVersion(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for name, mutate := range map[string]func(*model.DNSPhysicalSelection){
		"old version": func(selection *model.DNSPhysicalSelection) { selection.Version = model.DNSPhysicalSelectionVersion },
		"observe budget": func(selection *model.DNSPhysicalSelection) {
			selection.BusinessAllocation.State.BasisPoints = map[string]int{"edge-a": 500, "edge-b": 9500}
		},
		"policy substitution": func(selection *model.DNSPhysicalSelection) {
			selection.BusinessAllocation.Policy.ExplorationBasisPoints = 200
		},
		"lease extension": func(selection *model.DNSPhysicalSelection) {
			selection.BusinessAllocation.ValidUntil = now.Add(24 * time.Hour)
		},
		"scope substitution": func(selection *model.DNSPhysicalSelection) { selection.Scope = "invented" },
	} {
		t.Run(name, func(t *testing.T) {
			record := businessSelectionRecord(t, now)
			mutate(record.AnswerPolicy.PhysicalSelection)
			if ordered, _ := edgeDNSOrderedCandidatesWithDecision(record, businessBucketHint(0), now, false); len(ordered) != 0 {
				t.Fatal("accepted incompatible allocation", ordered)
			}
		})
	}
}

func TestBusinessDNSRealReceiptReplaysAndRetainsBaselineIdentity(t *testing.T) {
	state, now := decisionTestState(t)
	record := businessSelectionRecord(t, now)
	payload := state.payload
	for viewIndex := range payload.Queries {
		for recordIndex := range payload.Queries[viewIndex].Records {
			entry := &payload.Queries[viewIndex].Records[recordIndex]
			if entry.Name == "target.example.test" {
				entry.AnswerPolicy = record.AnswerPolicy
			}
		}
	}
	state, err := buildDNSServingState(state.record, payload, "route", "dns-a", "edge-group-a", state.facts, now)
	if err != nil {
		t.Fatal(err)
	}
	query := new(dns.Msg)
	query.SetQuestion("target.example.test.", dns.TypeA)
	receipt := captureDecision(t, state, query, "198.51.100.2:1234", now)
	replayed, err := ReplayDNSDecision(receipt)
	if err != nil || !replayed.Matched || len(receipt.RRSet) != 1 {
		t.Fatal("business DNS receipt failed replay", replayed, err)
	}
	var input dnsDecisionReplay
	if err := json.Unmarshal(receipt.ReplayInput, &input); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, stage := range input.Stages {
		for _, selection := range stage.Selections {
			for _, bucket := range selection.Entropy.Buckets {
				found = found || bucket.Purpose == "business_allocation" && bucket.Modulus == 10000
			}
		}
	}
	if !found {
		t.Fatal("business allocation entropy was not retained")
	}
	raw, _ := json.Marshal(receipt)
	if strings.Contains(string(raw), "198.51.100.2") {
		t.Fatal("allocation entropy retained raw resolver address")
	}
	evidence, err := QualityEvidenceFromDNSDecision(receipt, receipt.ObservedAt, time.Minute)
	if err != nil || evidence.EdgeID != "edge-b" || evidence.BusinessAllocation == nil {
		t.Fatal("observed exploration answer lost immutable baseline", evidence, err)
	}
}
