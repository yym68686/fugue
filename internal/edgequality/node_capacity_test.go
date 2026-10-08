package edgequality

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
)

func nodeCapacityFixture(now time.Time) NodeCapacitySample {
	observed := now.Add(-time.Second)
	return NodeCapacitySample{ID: "capacity-a", EdgeID: "edge-a", EdgeGroupID: "shared", Address: "203.0.113.5",
		Capacity: model.EdgeNetworkNodeCapacity{Source: "kubelet_node_allocatable_v1", NodeUID: "node-a", ObservedAt: observed, ValidUntil: observed.Add(2 * time.Minute),
			CPUObservedAt: observed, MemoryObservedAt: observed, CPUUsageNanoCores: 10_000_000, CPUAllocatableMilliCores: 1000, MemoryWorkingSetBytes: 100, MemoryAllocatableBytes: 1000, Pressure: []string{}}}
}

func TestIndependentNodeCapacityCannotInventNetworkEvidence(t *testing.T) {
	snapshot := networkFixture()
	snapshot.Observations = nil
	sample := nodeCapacityFixture(snapshot.CapturedAt)
	snapshot.NodeCapacitySamples = []NodeCapacitySample{sample}
	observation, ok := NodeCapacityObservation(snapshot, snapshot.Candidates[0], sample)
	if !ok || !observation.ObservedAt.Equal(sample.Capacity.ObservedAt) || observation.ClientNetworkMS != nil || observation.ServiceNetworkMS != nil {
		t.Fatal("capacity disguised as network evidence", observation)
	}
	snapshot.Observations = []Observation{observation}
	receipt, err := Capture(snapshot)
	if err != nil || receipt.Result.Hypothesis != "hold" || receipt.Result.PromotionReady {
		t.Fatal("capacity alone authorized routing", err, receipt)
	}
	for _, candidate := range receipt.Result.Candidates {
		if candidate.Ready {
			t.Fatal("missing network evidence became ready")
		}
		if candidate.EdgeID == "edge-a" && (candidate.Metrics["capacity_utilization"].State != "observed" || candidate.Metrics["capacity_utilization"].Value != 0.1) {
			t.Fatal("observed capacity denominator lost", candidate)
		}
	}
	encoded, _ := json.Marshal(receipt)
	var decoded Receipt
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, err := Replay(decoded); err != nil {
		t.Fatal("independent facts did not replay", err)
	}
	decoded.Snapshot.NodeCapacitySamples[0].Capacity.MemoryAllocatableBytes++
	if _, err := Replay(decoded); err == nil {
		t.Fatal("capacity denominator not covered by receipt digest")
	}
}

func TestIndependentCapacityDerivedFieldsAndRawIdentityAreChecked(t *testing.T) {
	for _, scenario := range []string{"missing_raw", "duplicate_node", "foreign_group", "foreign_edge", "future", "wrong_value", "wrong_time", "wrong_source", "wrong_reference", "mixed_witness", "invented_client", "invented_service", "invented_throughput", "invented_failure", "private_address"} {
		t.Run(scenario, func(t *testing.T) {
			snapshot := networkFixture()
			sample := nodeCapacityFixture(snapshot.CapturedAt)
			snapshot.NodeCapacitySamples = []NodeCapacitySample{sample}
			observation, _ := NodeCapacityObservation(snapshot, snapshot.Candidates[0], sample)
			snapshot.Observations = []Observation{observation}
			switch scenario {
			case "missing_raw":
				snapshot.NodeCapacitySamples = nil
			case "duplicate_node":
				snapshot.NodeCapacitySamples = append(snapshot.NodeCapacitySamples, sample)
			case "foreign_group":
				snapshot.NodeCapacitySamples[0].EdgeGroupID = "foreign"
			case "foreign_edge":
				snapshot.NodeCapacitySamples[0].EdgeID = "other"
			case "future":
				snapshot.NodeCapacitySamples[0] = nodeCapacityFixture(snapshot.CapturedAt.Add(time.Minute))
			case "wrong_value":
				snapshot.Observations[0].CapacityUtilization = number(0)
			case "wrong_time":
				snapshot.Observations[0].ObservedAt = snapshot.CapturedAt
			case "wrong_source":
				snapshot.Observations[0].CapacitySource = "active_requests"
			case "wrong_reference":
				snapshot.Observations[0].NodeCapacityID = "other"
			case "mixed_witness":
				snapshot.Observations[0].RouteWitnessID = "witness"
			case "invented_client":
				snapshot.Observations[0].ClientNetworkMS = number(0)
			case "invented_service":
				snapshot.Observations[0].ServiceNetworkMS = number(0)
			case "invented_throughput":
				snapshot.Observations[0].UploadBPS = number(1e6)
			case "invented_failure":
				snapshot.Observations[0].ServiceFailureRate = number(0)
			case "private_address":
				snapshot.NodeCapacitySamples[0].Address = "127.0.0.1"
			}
			if _, err := Capture(snapshot); err == nil {
				t.Fatal("invalid capacity observation accepted")
			}
		})
	}
}

func TestIndependentCapacityExpiryAndPressureRemainObservable(t *testing.T) {
	snapshot := networkFixture()
	sample := nodeCapacityFixture(snapshot.CapturedAt)
	sample.Capacity.Pressure = []string{"MemoryPressure"}
	observation, ok := NodeCapacityObservation(snapshot, snapshot.Candidates[0], sample)
	if !ok || *observation.CapacityUtilization != 1 {
		t.Fatal("pressure hidden as missing or zero")
	}
	sample = nodeCapacityFixture(snapshot.CapturedAt.Add(-3 * time.Minute))
	if ValidateNodeCapacitySample(sample, snapshot.CapturedAt) != nil {
		t.Fatal("historical raw timestamp should remain readable")
	}
	if _, ok := NodeCapacityObservation(snapshot, snapshot.Candidates[0], sample); ok {
		t.Fatal("expired capacity became a fresh observation")
	}
}
