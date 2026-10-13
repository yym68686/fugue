package edgequality

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func unifiedBusinessFixture() BusinessExplorationInput {
	input := businessInputFixture()
	input.Policy.Version = model.BusinessEdgeUnifiedVersion
	input.Policy.Mode = "adaptive"
	input.Policy.MaximumCandidateBasisPoints = 10000
	input.Policy.MaximumStepBasisPoints = 200
	input.Policy.MinimumConnectionsPerBucket = 1
	input.Policy.MinimumCohortCoverageBasisPoints = 3000
	input.Policy.Unified = &model.BusinessUnifiedPolicy{MinimumConnectionsPerCohort: 5, RTTAdvantageMS: 20, PromotionMinimumConnections: 8, PromotionMinimumCoverageBasisPoints: 5000}
	for index := range input.NetworkSamples {
		sample := &input.NetworkSamples[index]
		sample.ClientNetwork.Delivery, sample.ClientNetwork.DeliveryBaseline = nil, nil
		if sample.EdgeID == "edge-b" {
			sample.ClientNetwork.RTTMS = number(50)
		}
	}
	return input
}

func advanceUnifiedFixture(input *BusinessExplorationInput, duration time.Duration) {
	input.CapturedAt = input.CapturedAt.Add(duration)
	for index := range input.Candidates {
		at := input.CapturedAt
		input.Candidates[index].ProofObservedAt = &at
	}
	for index := range input.NetworkSamples {
		sample := &input.NetworkSamples[index]
		sample.ObservedAt = sample.ObservedAt.Add(duration)
		sample.ClientNetwork.ObservedAt = sample.ObservedAt
		sample.ClientNetwork.StartedAt = sample.ClientNetwork.StartedAt.Add(duration)
	}
}

func TestUnifiedObserveAndMigrationRetainComparisons(t *testing.T) {
	for _, mode := range []string{"observe", "adaptive"} {
		t.Run(mode, func(t *testing.T) {
			input := unifiedBusinessFixture()
			input.Policy.Mode = mode
			if mode == "adaptive" {
				input.InitialDNSBasisPoints = map[string]map[string]int{"dns-a": {"edge-a": 100, "edge-b": 9900}}
			}
			decision, err := EvaluateBusinessExploration(input)
			if err != nil {
				t.Fatal(err)
			}
			if len(decision.Comparisons) != 1 || decision.Comparisons[0].Verdict != "better" {
				t.Fatal("comparison evidence missing during observation or migration", decision.Comparisons)
			}
			if decision.State.Unified.PrimaryEdgeID != "edge-a" || decision.State.Unified.QualityBasisPoints["edge-b"] != 0 {
				t.Fatal("comparison changed quality allocation before adaptation", decision.State)
			}
			if mode == "adaptive" && !reflect.DeepEqual(decision.State.Unified.MigrationBasisPoints, input.InitialDNSBasisPoints) {
				t.Fatal("migration initialization changed executing weights")
			}
		})
	}
}

func TestUnifiedMigrationCannotOverrideQualifiedQualityWinner(t *testing.T) {
	for _, qualifies := range []bool{false, true} {
		input := unifiedBusinessFixture()
		input.InitialDNSBasisPoints = map[string]map[string]int{"dns-a": {"edge-a": 100, "edge-b": 9900}}
		if !qualifies {
			input.Policy.Unified.PromotionMinimumConnections = 100
		}
		initial, err := EvaluateBusinessExploration(input)
		if err != nil {
			t.Fatal(err)
		}
		input.Previous, input.InitialDNSBasisPoints = &initial.State, nil
		advanceUnifiedFixture(&input, time.Duration(input.Policy.CooldownSeconds)*time.Second)
		next, err := EvaluateBusinessExploration(input)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(next.State.Unified.MigrationBasisPoints, initial.State.Unified.MigrationBasisPoints) {
			t.Fatal("migration moved traffic toward an outperformed historical target")
		}
		if qualifies && (next.Action != "promote" || next.State.Unified.PrimaryEdgeID != "edge-b") {
			t.Fatal("migration blocked replayable quality promotion", next.Action)
		}
		if !qualifies && next.State.Unified.PrimaryEdgeID != "edge-a" {
			t.Fatal("migration promoted without the additional evidence")
		}
	}
}

func TestUnifiedRTTOnlyMovesBeyondOldCeilingAndPromotes(t *testing.T) {
	input := unifiedBusinessFixture()
	promoted := false
	for step := 0; step < 53; step++ {
		receipt, err := CaptureBusinessExploration(input)
		if err != nil {
			t.Fatal(step, err)
		}
		var decoded BusinessExplorationReceipt
		raw, _ := json.Marshal(receipt)
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		if _, err := ReplayBusinessExploration(decoded); err != nil {
			t.Fatal(step, err)
		}
		allocation := &model.DNSBusinessAllocation{ExecutionNodeID: "dns-a", Policy: input.Policy, State: receipt.Decision.State, ValidUntil: receipt.Decision.ValidUntil, ReceiptDigest: receipt.Digest}
		if err := model.ValidateDNSBusinessAllocation(allocation, input.CapturedAt); err != nil {
			t.Fatal(step, err)
		}
		state := receipt.Decision.State
		if step == 0 && state.BasisPoints["edge-b"] != 50 {
			t.Fatal("exploration budget was not independently divided", state)
		}
		if input.Previous != nil && state.BasisPoints["edge-b"]-input.Previous.BasisPoints["edge-b"] > 200 {
			t.Fatal("step budget exceeded", state)
		}
		if step == 7 && state.BasisPoints["edge-b"] <= 1000 {
			t.Fatal("historical candidate ceiling retained", state)
		}
		if receipt.Decision.Action == "promote" {
			promoted = true
		}
		input.Previous = &state
		advanceUnifiedFixture(&input, time.Minute)
	}
	if !promoted || input.Previous.Unified.PrimaryEdgeID != "edge-b" || input.Previous.BasisPoints["edge-b"] != 9950 {
		t.Fatal("winner failed to become full quality primary", input.Previous)
	}
	input.NetworkSamples = nil
	advanceUnifiedFixture(&input, 3*time.Minute)
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.State.Unified.PrimaryEdgeID != "edge-b" {
		t.Fatal("missing evidence resurrected historical primary", err, decision)
	}
}

func TestUnifiedUnknownMetricsAndIndependentStops(t *testing.T) {
	for _, scenario := range []string{"missing_delivery", "bad_rtt", "bad_retransmission"} {
		t.Run(scenario, func(t *testing.T) {
			input := unifiedBusinessFixture()
			for index := range input.NetworkSamples {
				sample := &input.NetworkSamples[index]
				if sample.EdgeID != "edge-b" {
					continue
				}
				if scenario == "bad_rtt" {
					sample.ClientNetwork.RTTMS = number(5000)
				}
				if scenario == "bad_retransmission" {
					sample.ClientNetwork.DeliveryBaseline = &model.EdgeClientDeliveryCounters{ObservedAt: sample.ObservedAt.Add(-time.Second), DataSegmentsOut: 10}
					sample.ClientNetwork.Delivery = &model.EdgeClientDeliveryCounters{ObservedAt: sample.ObservedAt, DataSegmentsOut: 110, RetransmittedSegments: 40, ApplicationLimited: true}
				}
			}
			decision, err := EvaluateBusinessExploration(input)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "missing_delivery" {
				if len(decision.Comparisons) != 1 || decision.Comparisons[0].Verdict != "better" {
					t.Fatal("valid RTT was vetoed by absent delivery", decision)
				}
			} else if decision.State.Stopped["edge-b"] != "observed_transport_stop_threshold" || decision.State.BasisPoints["edge-b"] != 0 {
				t.Fatal("independent stop ignored", decision)
			}
		})
	}
}

func TestUnifiedDeduplicatesAcrossWindowsAndRejectsReusedEvidence(t *testing.T) {
	input := unifiedBusinessFixture()
	initial, err := EvaluateBusinessExploration(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Previous = &initial.State
	advanceUnifiedFixture(&input, time.Minute)
	first, err := EvaluateBusinessExploration(input)
	if err != nil || first.Action != "increase" {
		t.Fatal(first, err)
	}
	input.Previous = &first.State
	input.CapturedAt = input.CapturedAt.Add(5 * time.Second)
	second, err := EvaluateBusinessExploration(input)
	if err != nil || !reflect.DeepEqual(first.State.BasisPoints, second.State.BasisPoints) {
		t.Fatal("one bucket adjusted twice", second, err)
	}
	input = unifiedBusinessFixture()
	for index := range input.NetworkSamples {
		sample := &input.NetworkSamples[index]
		sample.ClientNetwork.ConnectionID = sample.EdgeID
		sample.ClientNetwork.StartedAt = input.CapturedAt.Add(-time.Hour)
	}
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.Comparisons[0].Verdict != "insufficient" {
		t.Fatal("repeated connection inflated samples", decision, err)
	}
}

func TestUnifiedPromotionNeedsAdditionalEvidenceAndExpiryPreservesPrimary(t *testing.T) {
	input := unifiedBusinessFixture()
	input.Policy.Unified.PromotionMinimumConnections = 20
	for step := 0; step < 32; step++ {
		decision, err := EvaluateBusinessExploration(input)
		if err != nil {
			t.Fatal(err)
		}
		if decision.State.Unified.PrimaryEdgeID != "edge-a" || decision.State.BasisPoints["edge-b"] >= decision.State.BasisPoints["edge-a"] {
			t.Fatal("unqualified candidate promoted", decision)
		}
		input.Previous = &decision.State
		advanceUnifiedFixture(&input, time.Minute)
	}
	input.NetworkSamples = nil
	advanceUnifiedFixture(&input, 3*time.Minute)
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.Action != "evidence_expired" || decision.State.BasisPoints["edge-b"] != 50 {
		t.Fatal(decision, err)
	}
}

func TestUnifiedSourcesAreOrderIndependentAndCannotResetPredecessor(t *testing.T) {
	input := unifiedBusinessFixture()
	input.Policy.InitialDNSPublications["dns-b"] = input.Policy.InitialDNSPublications["dns-a"]
	second := input
	second.CapturedAt = second.CapturedAt.Add(time.Second)
	first, err := MergeUnifiedBusinessInputs([]BusinessExplorationInput{input, second})
	if err != nil {
		t.Fatal(err)
	}
	reverse, err := MergeUnifiedBusinessInputs([]BusinessExplorationInput{second, input})
	if err != nil || !reflect.DeepEqual(first, reverse) || len(first.NetworkSamples) != len(input.NetworkSamples) {
		t.Fatal("DNS source order affects quality input", err)
	}
	decision, err := EvaluateBusinessExploration(input)
	if err != nil {
		t.Fatal(err)
	}
	second.Previous = &decision.State
	if _, err := MergeUnifiedBusinessInputs([]BusinessExplorationInput{input, second}); err == nil {
		t.Fatal("mixed initialized/predecessor sources reset shared state")
	}
	if _, err := MergeUnifiedBusinessInputs([]BusinessExplorationInput{input}); err == nil {
		t.Fatal("partial DNS source group accepted")
	}
}

func TestUnifiedMigrationConvergesWithoutInitialTrafficJump(t *testing.T) {
	input := unifiedBusinessFixture()
	input.Policy.InitialDNSPublications["dns-b"] = input.Policy.InitialDNSPublications["dns-a"]
	input.InitialDNSBasisPoints = map[string]map[string]int{"dns-a": {"edge-a": 9900, "edge-b": 100}, "dns-b": {"edge-a": 100, "edge-b": 9900}}
	input.NetworkSamples = nil
	prior := input.InitialDNSBasisPoints
	for step := 0; step < 52; step++ {
		decision, err := EvaluateBusinessExploration(input)
		if err != nil {
			t.Fatal(step, err)
		}
		allocation := &model.DNSBusinessAllocation{ExecutionNodeID: "dns-a", Policy: input.Policy, State: decision.State, ValidUntil: decision.ValidUntil, ReceiptDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
		if err := model.ValidateDNSBusinessAllocation(allocation, input.CapturedAt); err != nil {
			t.Fatal(step, err)
		}
		for _, nodeID := range []string{"dns-a", "dns-b"} {
			weights := decision.State.Unified.MigrationBasisPoints[nodeID]
			if weights == nil {
				weights = decision.State.BasisPoints
			}
			if step == 0 && !equalBusinessWeights(input.Policy, weights, prior[nodeID]) {
				t.Fatal("migration initialization changed production weights")
			}
			for _, edgeID := range input.Policy.CandidateEdgeIDs {
				if abs := weights[edgeID] - prior[nodeID][edgeID]; abs > 200 || abs < -200 {
					t.Fatal("migration exceeded step bound", step, nodeID, prior[nodeID], weights)
				}
			}
			prior[nodeID] = weights
		}
		input.Previous = &decision.State
		input.InitialDNSBasisPoints = nil
		advanceUnifiedFixture(&input, time.Minute)
	}
	if input.Previous.Unified.MigrationBasisPoints != nil {
		t.Fatal("migration did not converge")
	}
}

func TestUnifiedDegradedPrimaryDoesNotBlockBetterCandidate(t *testing.T) {
	input := unifiedBusinessFixture()
	initial, err := EvaluateBusinessExploration(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Previous = &initial.State
	advanceUnifiedFixture(&input, time.Minute)
	for index := range input.NetworkSamples {
		if input.NetworkSamples[index].EdgeID == "edge-a" {
			input.NetworkSamples[index].ClientNetwork.RTTMS = number(5000)
		}
	}
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.Action != "increase" || decision.State.BasisPoints["edge-b"] <= initial.State.BasisPoints["edge-b"] {
		t.Fatal("degraded primary froze quality recovery", decision, err)
	}
	input.Previous = nil
	input.InitialDNSBasisPoints = map[string]map[string]int{"dns-a": {"edge-a": 0, "edge-b": 10000}}
	initial, err = EvaluateBusinessExploration(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Previous = &initial.State
	input.InitialDNSBasisPoints = nil
	advanceUnifiedFixture(&input, time.Minute)
	decision, err = EvaluateBusinessExploration(input)
	if err != nil || !reflect.DeepEqual(decision.State.Unified.MigrationBasisPoints, initial.State.Unified.MigrationBasisPoints) {
		t.Fatal("migration advanced toward degraded target", decision, err)
	}
}
