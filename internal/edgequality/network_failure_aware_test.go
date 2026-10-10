package edgequality

import "testing"

func failureAwareFixture() Snapshot {
	snapshot := networkFixture()
	snapshot.Policy.Version = FailureAwareNetworkPolicyVersion
	for index := range snapshot.Observations {
		observation := &snapshot.Observations[index]
		observation.ClientNetworkMS, observation.ServiceNetworkMS = number(180), number(10)
		observation.ClientFailureRate = number(1)
		observation.DownloadBPS = nil
		if observation.EdgeID == "edge-b" {
			observation.ClientFailureRate = number(0)
			observation.DownloadBPS = number(1 << 20)
		}
	}
	return snapshot
}

func TestFailureAwareComparisonDoesNotRequireFailingDownloadsToComplete(t *testing.T) {
	snapshot := failureAwareFixture()
	current := evaluateTest(t, snapshot)
	if current.Hypothesis != "switch" || current.ProposedEdgeID != "edge-b" || current.SustainedBuckets != snapshot.Policy.RequiredBuckets {
		t.Fatal("explicit repeated transfer failures froze a healthy alternative", current)
	}
	snapshot.Policy.Version = DeliveryNetworkPolicyVersion
	historical, err := Capture(snapshot)
	if err != nil || historical.Result.Hypothesis != "hold" {
		t.Fatal("historical V4 semantics changed", historical.Result, err)
	}
	if _, err := Replay(historical); err != nil {
		t.Fatal(err)
	}
	for _, failure := range []*float64{nil, number(0)} {
		snapshot = failureAwareFixture()
		for index := range snapshot.Observations {
			if snapshot.Observations[index].EdgeID == "edge-a" {
				snapshot.Observations[index].ClientFailureRate = failure
			}
		}
		if result := evaluateTest(t, snapshot); result.Hypothesis != "hold" {
			t.Fatal("missing download without explicit excess failure became a switch", result)
		}
	}
}

func TestFailureAwareCohortsBoundUnknownWithoutIgnoringProvenDisagreement(t *testing.T) {
	for _, enough := range []bool{false, true} {
		snapshot := failureAwareFixture()
		additional := []Observation{}
		seen := map[string]bool{}
		for _, observation := range snapshot.Observations {
			if !enough && seen[observation.EdgeID] {
				continue
			}
			seen[observation.EdgeID] = true
			observation.ID += "-different-network"
			observation.ClientCohort = "tcp_peer:198.51.100.0/24"
			observation.ClientFailureRate, observation.DownloadBPS = number(0), number(1<<20)
			observation.ClientNetworkMS = number(10)
			if observation.EdgeID == "edge-b" {
				observation.ClientNetworkMS = number(900)
			}
			additional = append(additional, observation)
		}
		snapshot.Observations = append(snapshot.Observations, additional...)
		result := evaluateTest(t, snapshot)
		if enough && result.Hypothesis != "hold" || !enough && result.Hypothesis != "switch" {
			t.Fatal("cohort evidence was treated as either an invented veto or ignored disagreement", enough, result)
		}
	}
}
