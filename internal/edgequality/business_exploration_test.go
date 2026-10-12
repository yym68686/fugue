package edgequality

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func businessInputFixture() BusinessExplorationInput {
	now := time.Date(2026, 1, 1, 1, 0, 10, 0, time.UTC)
	policy := model.BusinessEdgeExplorationPolicy{Version: model.BusinessEdgeExplorationVersion, IntentID: "intent-a", Mode: "observe", BaselineEdgeID: "edge-a", CandidateEdgeIDs: []string{"edge-a", "edge-b"},
		InitialDNSPublications: map[string]model.BusinessDNSPublicationAnchor{"dns-a": {Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", FencingToken: 1}},
		StartsAt:               now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour), ExplorationBasisPoints: 100, MaximumCandidateBasisPoints: 1000, MaximumStepBasisPoints: 50,
		BucketSeconds: 60, RequiredBuckets: 3, MinimumConnectionsPerBucket: 3, CooldownSeconds: 60, EvidenceMaxAgeSeconds: 120,
		MinimumCohortCoverageBasisPoints: 9000, RTTAdvantageRatio: .15, DeliveryAdvantageRatio: .2, MaximumRTTRegressionRatio: .1,
		MaximumDeliveryRegressionRatio: .1, MaximumRetransmissionIncrease: .01, StopRTTMS: 3000, StopRetransmissionRate: .2}
	digest := "sha256:" + strings.Repeat("a", 64)
	input := BusinessExplorationInput{Policy: policy, CapturedAt: now, Hostname: "service.example.test", PathPrefix: "/", TrafficClass: "dynamic_api"}
	for _, edgeID := range policy.CandidateEdgeIDs {
		input.Candidates = append(input.Candidates, Candidate{EdgeID: edgeID, EdgeGroupID: "group-a", RouteGeneration: digest, RouteProofVerified: true, ProofObservedAt: &now})
		for bucket := 1; bucket <= 3; bucket++ {
			for connection := 0; connection < 3; connection++ {
				observed := now.Truncate(time.Minute).Add(-time.Duration(bucket)*time.Minute + 30*time.Second)
				acked, rate := uint64(100000), uint64(250000)
				if edgeID == "edge-b" {
					acked, rate = 200000, 500000
				}
				identity := fmt.Sprintf("%s-%d-%d", edgeID, bucket, connection)
				client := &model.EdgeClientNetworkSample{ConnectionID: identity, Slot: "a", Scope: "tcp_peer:203.0.113.0/24", StartedAt: observed.Add(-time.Minute), ObservedAt: observed, TCPInfoAvailable: true, RTTMS: number(100),
					DeliveryBaseline: &model.EdgeClientDeliveryCounters{ObservedAt: observed.Add(-time.Second), BytesAcked: 1000, BusyMicroseconds: 100000, DataSegmentsOut: 10},
					Delivery:         &model.EdgeClientDeliveryCounters{ObservedAt: observed, BytesAcked: 1000 + acked, BusyMicroseconds: 600000, DataSegmentsOut: 110, DeliveryRateBytesPerSecond: rate}}
				input.NetworkSamples = append(input.NetworkSamples, model.EdgeNetworkSample{ID: identity, EdgeID: edgeID, EdgeGroupID: "group-a", Hostname: input.Hostname, PathPrefix: "/", TrafficClass: input.TrafficClass,
					RouteDigest: digest, BundleVersion: "bundle-a", Source: "public_front_tcp_info_v1", ObservedAt: observed, ClientNetwork: client})
			}
		}
	}
	return input
}

func priorBusinessState(t *testing.T, input BusinessExplorationInput) *BusinessExplorationState {
	t.Helper()
	decision, err := EvaluateBusinessExploration(input)
	if err != nil {
		t.Fatal(err)
	}
	decision.State.LastAdjustedAt = input.CapturedAt.Add(-5 * time.Minute)
	decision.State.LastEvaluatedAt = input.CapturedAt.Add(-time.Minute)
	decision.State.EvidenceValidUntil = decision.State.LastEvaluatedAt.Add(time.Duration(input.Policy.EvidenceMaxAgeSeconds) * time.Second)
	return &decision.State
}

func TestBusinessObserveHasFixedBudgetAndDeterministicReplay(t *testing.T) {
	input := businessInputFixture()
	receipt, err := CaptureBusinessExploration(input)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Decision.State.BasisPoints["edge-b"] != 100 || receipt.Decision.ServingAuthority || receipt.Decision.Comparisons[0].Verdict != "better" {
		t.Fatal(receipt.Decision)
	}
	if len(receipt.Decision.Windows) != 6 || receipt.Decision.Windows[0].AcknowledgedBytesMedian != 100000 || receipt.Decision.Windows[0].BusyMicrosecondsMedian != 500000 {
		t.Fatal("selected interval byte and busy-time evidence omitted", receipt.Decision.Windows)
	}
	raw, _ := json.Marshal(receipt)
	var decoded BusinessExplorationReceipt
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, err := ReplayBusinessExploration(decoded); err != nil {
		t.Fatal(err)
	}
	decoded.Decision.State.BasisPoints["edge-b"]++
	if _, err := ReplayBusinessExploration(decoded); err == nil {
		t.Fatal("accepted mutated allocation")
	}
	input.Previous = priorBusinessState(t, input)
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.State.BasisPoints["edge-b"] != 100 {
		t.Fatal("observe promoted candidate", decision, err)
	}
}

func TestBusinessAdaptiveUsesBoundedStepsCooldownAndBudget(t *testing.T) {
	input := businessInputFixture()
	input.Policy.Mode = "adaptive"
	input.Previous = priorBusinessState(t, input)
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.Action != "increase" || decision.State.BasisPoints["edge-b"] != 150 {
		t.Fatal(decision, err)
	}
	input.Previous = &decision.State
	decision, err = EvaluateBusinessExploration(input)
	if err != nil || decision.Action != "hold" || decision.State.BasisPoints["edge-b"] != 150 {
		t.Fatal("cooldown failed", decision, err)
	}
	input.Previous.LastAdjustedAt = input.CapturedAt.Add(-time.Hour)
	input.Previous.LastEvidenceBucket = time.Time{}
	input.Previous.BasisPoints = map[string]int{"edge-a": 9000, "edge-b": 1000}
	decision, err = EvaluateBusinessExploration(input)
	if err != nil || decision.State.BasisPoints["edge-b"] != 1000 {
		t.Fatal("exceeded total candidate budget", decision, err)
	}
}

func TestBusinessDoesNotReuseSameBucketAfterCooldown(t *testing.T) {
	input := businessInputFixture()
	input.Policy.Mode = "adaptive"
	input.Previous = priorBusinessState(t, input)
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.Action != "increase" {
		t.Fatal(decision, err)
	}
	input.Previous = &decision.State
	input.Previous.LastAdjustedAt = input.CapturedAt.Add(-time.Minute)
	input.Previous.LastEvidenceBucket = input.Previous.LastAdjustedAt.Truncate(time.Minute)
	decision, err = EvaluateBusinessExploration(input)
	if err != nil || decision.Action == "increase" {
		t.Fatal("same complete bucket authorized a second step", decision, err)
	}
}

func TestBusinessCohortDisagreementBlocksPromotion(t *testing.T) {
	input := businessInputFixture()
	input.Policy.Mode = "adaptive"
	input.Previous = priorBusinessState(t, input)
	for _, sample := range append([]model.EdgeNetworkSample(nil), input.NetworkSamples...) {
		raw, _ := json.Marshal(sample)
		var copy model.EdgeNetworkSample
		if err := json.Unmarshal(raw, &copy); err != nil {
			t.Fatal(err)
		}
		copy.ID += "-other"
		copy.ClientNetwork.ConnectionID += "-other"
		copy.ClientNetwork.Scope = "tcp_peer:198.51.100.0/24"
		if copy.EdgeID == "edge-b" {
			copy.ClientNetwork.RTTMS = number(200)
		}
		input.NetworkSamples = append(input.NetworkSamples, copy)
	}
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.Action == "increase" || decision.Comparisons[0].Verdict != "inconclusive" {
		t.Fatal("a good cohort concealed a bad cohort", decision, err)
	}
}

func TestBusinessTransportStopsAndDecreases(t *testing.T) {
	input := businessInputFixture()
	input.Policy.Mode = "adaptive"
	input.Previous = priorBusinessState(t, input)
	input.Previous.BasisPoints = map[string]int{"edge-a": 9500, "edge-b": 500}
	for index := range input.NetworkSamples {
		client := input.NetworkSamples[index].ClientNetwork
		if input.NetworkSamples[index].EdgeID == "edge-b" {
			client.Delivery.BytesAcked, client.Delivery.DeliveryRateBytesPerSecond = 51000, 100000
		}
	}
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.Action != "decrease" || decision.State.BasisPoints["edge-b"] != 450 {
		t.Fatal("failed to bound downward step", decision, err)
	}
	for index := range input.NetworkSamples {
		if input.NetworkSamples[index].EdgeID == "edge-b" {
			input.NetworkSamples[index].ClientNetwork.Delivery.RetransmittedSegments = 30
		}
	}
	decision, err = EvaluateBusinessExploration(input)
	if err != nil || decision.State.BasisPoints["edge-b"] != 0 || decision.State.Stopped["edge-b"] != "observed_transport_stop_threshold" {
		t.Fatal("stop condition failed", decision, err)
	}
}

func TestBusinessAdaptiveDoesNotManufactureComparableEvidence(t *testing.T) {
	for name, change := range map[string]func(*BusinessExplorationInput){
		"different cohort": func(input *BusinessExplorationInput) {
			for index := range input.NetworkSamples {
				if input.NetworkSamples[index].EdgeID == "edge-b" {
					input.NetworkSamples[index].ClientNetwork.Scope = "tcp_peer:198.51.100.0/24"
				}
			}
		},
		"one connection per bucket": func(input *BusinessExplorationInput) {
			for index := range input.NetworkSamples {
				input.NetworkSamples[index].ClientNetwork.ConnectionID = "same-connection"
			}
		},
		"missing middle bucket": func(input *BusinessExplorationInput) {
			input.NetworkSamples = append(input.NetworkSamples[:12], input.NetworkSamples[15:]...)
		},
		"application limited": func(input *BusinessExplorationInput) {
			for index := range input.NetworkSamples {
				input.NetworkSamples[index].ClientNetwork.Delivery.ApplicationLimited = true
			}
		},
		"wrong route": func(input *BusinessExplorationInput) {
			for index := range input.NetworkSamples {
				input.NetworkSamples[index].RouteDigest = "sha256:" + strings.Repeat("b", 64)
			}
		},
		"no business traffic": func(input *BusinessExplorationInput) { input.NetworkSamples = nil },
	} {
		t.Run(name, func(t *testing.T) {
			input := businessInputFixture()
			input.Policy.Mode = "adaptive"
			input.Previous = priorBusinessState(t, input)
			change(&input)
			decision, err := EvaluateBusinessExploration(input)
			if err != nil || decision.Action == "increase" || decision.State.BasisPoints["edge-b"] != 100 {
				t.Fatal(decision, err)
			}
		})
	}
}

func TestBusinessStopsAreStickyAndLeaseCannotPromote(t *testing.T) {
	input := businessInputFixture()
	input.Policy.Mode = "adaptive"
	input.Previous = priorBusinessState(t, input)
	input.Candidates[1].HardGates = []string{"maintenance"}
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.State.BasisPoints["edge-b"] != 0 || decision.State.Stopped["edge-b"] == "" || decision.State.BasisPoints["edge-a"] != 10000 {
		t.Fatal(decision, err)
	}
	input.Previous = &decision.State
	input.Candidates[1].HardGates = nil
	decision, err = EvaluateBusinessExploration(input)
	if err != nil || decision.State.BasisPoints["edge-b"] != 0 {
		t.Fatal("heartbeat silently cleared stop", decision, err)
	}
	input = businessInputFixture()
	input.CapturedAt = input.Policy.ExpiresAt
	for index := range input.Candidates {
		input.Candidates[index].ProofObservedAt = &input.CapturedAt
	}
	decision, err = EvaluateBusinessExploration(input)
	if err != nil || decision.Action != "baseline_only" || decision.State.BasisPoints["edge-a"] != 10000 {
		t.Fatal("expired intent explored", decision, err)
	}
}

func TestBusinessRejectsMutatedIntentAndDuplicateSamples(t *testing.T) {
	input := businessInputFixture()
	input.Previous = priorBusinessState(t, input)
	input.Policy.Mode = "adaptive"
	if _, err := EvaluateBusinessExploration(input); err == nil {
		t.Fatal("accepted changed policy with old predecessor")
	}
	input = businessInputFixture()
	input.NetworkSamples = append(input.NetworkSamples, input.NetworkSamples[0])
	if _, err := EvaluateBusinessExploration(input); err == nil {
		t.Fatal("accepted duplicated raw sample")
	}
}

func TestBusinessEvaluationDoesNotMutateInputs(t *testing.T) {
	input := businessInputFixture()
	input.Previous = priorBusinessState(t, input)
	before, _ := json.Marshal(input)
	if _, err := EvaluateBusinessExploration(input); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(input)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("mutated caller evidence")
	}
}

func TestBusinessIncompleteMetricsRemainInCoverageDenominator(t *testing.T) {
	input := businessInputFixture()
	input.Policy.Mode = "adaptive"
	input.Previous = priorBusinessState(t, input)
	for _, sample := range append([]model.EdgeNetworkSample(nil), input.NetworkSamples...) {
		if sample.EdgeID != "edge-b" {
			continue
		}
		raw, _ := json.Marshal(sample)
		var incomplete model.EdgeNetworkSample
		if err := json.Unmarshal(raw, &incomplete); err != nil {
			t.Fatal(err)
		}
		incomplete.ID += "-limited"
		incomplete.ClientNetwork.ConnectionID += "-limited"
		incomplete.ClientNetwork.Scope = "tcp_peer:198.51.100.0/24"
		incomplete.ClientNetwork.Delivery.ApplicationLimited = true
		input.NetworkSamples = append(input.NetworkSamples, incomplete)
	}
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.Action == "increase" || decision.Comparisons[0].CoverageBasisPoints != 5000 {
		t.Fatal("incomplete observations vanished from coverage", decision, err)
	}
	for _, coverage := range decision.Coverage {
		if coverage.EdgeID == "edge-b" && (coverage.RTTRecords != 18 || coverage.AcknowledgedDeltaRecords != 18 || coverage.DeliveryRecords != 9 || coverage.RetransmissionRecords != 18 || coverage.Reasons["delivery_application_limited"] != 9) {
			t.Fatal("application-limited evidence was misreported as absent traffic", coverage)
		}
	}
}

func TestBusinessAdaptiveEvidenceExpirationDoesNotRenewUnknownQuality(t *testing.T) {
	input := businessInputFixture()
	input.Policy.Mode = "adaptive"
	input.Previous = priorBusinessState(t, input)
	input.Previous.BasisPoints = map[string]int{"edge-a": 9500, "edge-b": 500}
	input.Previous.EvidenceValidUntil = input.CapturedAt
	input.NetworkSamples = nil
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.Action != "evidence_expired" || decision.State.BasisPoints["edge-b"] != 100 || len(decision.State.Stopped) != 0 {
		t.Fatal("expired evidence renewed adaptive allocation or became a failure", decision, err)
	}
}

func TestBusinessScheduledIntentStartsWithExplorationFloor(t *testing.T) {
	input := businessInputFixture()
	input.Policy.Mode = "adaptive"
	input.Policy.StartsAt = input.CapturedAt.Add(time.Minute)
	decision, err := EvaluateBusinessExploration(input)
	if err != nil || decision.Action != "baseline_only" {
		t.Fatal(decision, err)
	}
	input.Previous = &decision.State
	input.CapturedAt = input.Policy.StartsAt
	for index := range input.Candidates {
		input.Candidates[index].ProofObservedAt = &input.CapturedAt
	}
	decision, err = EvaluateBusinessExploration(input)
	if err != nil || decision.State.BasisPoints["edge-b"] != 100 || decision.Action != "initialize" {
		t.Fatal("scheduled intent starved its candidate", decision, err)
	}
}
