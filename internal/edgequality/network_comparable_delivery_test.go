package edgequality

import (
	"math"
	"testing"
)

func TestComparableDeliveryUsesCompletedTransfersWithoutInventingRetransmissions(t *testing.T) {
	snapshot := failureAwareFixture()
	for index := range snapshot.Observations {
		observation := &snapshot.Observations[index]
		if observation.EdgeID == "edge-a" {
			observation.ClientRetransmissionRate = number(0.2)
		}
	}
	historical, err := Capture(snapshot)
	if err != nil || historical.Result.Hypothesis != "hold" {
		t.Fatal("historical V5 semantics changed", historical.Result, err)
	}
	if _, err := Replay(historical); err != nil {
		t.Fatal(err)
	}
	snapshot.Policy.Version = ComparableDeliveryPolicyVersion
	result := evaluateTest(t, snapshot)
	if result.Hypothesis != "switch" || result.ProposedEdgeID != "edge-b" || result.SustainedBuckets != snapshot.Policy.RequiredBuckets {
		t.Fatal("missing optional packet counters vetoed measured successful transfers", result)
	}
	if len(result.Comparisons) != 1 || len(result.Comparisons[0].ExcludedMetricCosts) != 1 || result.Comparisons[0].ExcludedMetricCosts[0] != "client_retransmission_rate" || result.Comparisons[0].ComparisonUncertaintyMS != snapshot.Policy.UnknownCostMS {
		t.Fatal("comparison failed to explain its omitted metric and uncertainty", result.Comparisons)
	}
	for index := range snapshot.Observations {
		if snapshot.Observations[index].EdgeID == "edge-b" {
			snapshot.Observations[index].DownloadBPS = nil
		}
	}
	if result := evaluateTest(t, snapshot); result.Hypothesis != "hold" {
		t.Fatal("missing retransmissions were ignored without comparable delivery evidence", result)
	}
}

func TestComparableDeliveryDoesNotUseAnUnsharedRetransmissionPenaltyToWin(t *testing.T) {
	snapshot := failureAwareFixture()
	snapshot.Policy.Version = ComparableDeliveryPolicyVersion
	for index := range snapshot.Observations {
		observation := &snapshot.Observations[index]
		observation.ClientFailureRate, observation.DownloadBPS = number(0), number(1<<20)
		if observation.EdgeID == "edge-a" {
			observation.ClientRetransmissionRate = number(0.9)
		}
	}
	if result := evaluateTest(t, snapshot); result.Hypothesis != "hold" {
		t.Fatal("an optional metric observed on one edge alone manufactured an advantage", result)
	}
	policy := snapshot.Policy
	challenger := Assessment{Upper: 60, Metrics: map[string]Metric{"download_bps": {State: "observed"}}}
	current := Assessment{Lower: 140, Metrics: map[string]Metric{"download_bps": {State: "observed"}, "client_retransmission_rate": {State: "observed", Value: 0.1}}}
	adjustedChallenger, adjustedCurrent := comparableRetransmissionAssessments(challenger, current, policy)
	if math.Abs(adjustedCurrent.Lower-40) > 1e-9 || adjustedChallenger.Upper != 60+policy.UnknownCostMS || current.Lower != 140 || challenger.Upper != 60 {
		t.Fatal("comparison omitted its bounded uncertainty or changed original evidence", adjustedChallenger, adjustedCurrent)
	}
}
