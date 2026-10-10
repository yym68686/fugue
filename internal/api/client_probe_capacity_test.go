package api

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

func TestClientProbeCapacityWitnessPreservesPhysicalProofAndObservationTimes(t *testing.T) {
	now := time.Now().UTC()
	sample, proof := networkWitnessFixture(now)
	node := model.EdgeNode{ID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, PublicIPv4: "203.0.113.5"}
	input := model.EdgeClientProbeRequest{Hostname: sample.Hostname, Path: sample.PathPrefix, TrafficClass: sample.TrafficClass}
	capacity := edgequality.NodeCapacitySample{ID: "capacity", EdgeID: node.ID, EdgeGroupID: node.EdgeGroupID, Address: node.PublicIPv4, Capacity: qualityCapacityFixture(now)}
	witnesses := clientProbeCapacityWitnesses(input, []model.EdgeNode{node}, []routeprobe.Proof{proof}, []edgequality.NodeCapacitySample{capacity}, now)
	if len(witnesses) != 1 || witnesses[0].RouteDigest != proof.Digest || witnesses[0].BundleVersion != proof.Version || !witnesses[0].ObservedAt.Equal(proof.CheckedAt) || witnesses[0].ServiceRTTMS != nil || witnesses[0].ClientNetwork != nil || witnesses[0].ServiceTarget != "" {
		t.Fatal("capacity observation fabricated transport metrics or lost proof identity", witnesses)
	}
	if !reflect.DeepEqual(*witnesses[0].RouteWitness.NodeCapacity, capacity.Capacity) {
		t.Fatal("capacity timestamps or denominators changed")
	}
	for _, scenario := range []string{"foreign_node", "foreign_group", "foreign_address", "expired_capacity", "expired_proof", "stale_proof", "negative_proof", "unpaired_proof"} {
		changedCapacity, changedProof := capacity, proof
		switch scenario {
		case "foreign_node":
			changedCapacity.EdgeID = "other"
		case "foreign_group":
			changedCapacity.EdgeGroupID = "other"
		case "foreign_address":
			changedCapacity.Address = "203.0.113.6"
		case "expired_capacity":
			changedCapacity.Capacity = qualityCapacityFixture(now.Add(-3 * time.Minute))
		case "expired_proof":
			changedProof.ValidUntil = now
		case "stale_proof":
			changedProof.CheckedAt = now.Add(-13 * time.Second)
		case "negative_proof":
			changedProof.State = "excluded"
		case "unpaired_proof":
			changedProof.EdgeID = "other"
		}
		if actual := clientProbeCapacityWitnesses(input, []model.EdgeNode{node}, []routeprobe.Proof{changedProof}, []edgequality.NodeCapacitySample{changedCapacity}, now); len(actual) != 0 {
			t.Fatal("untrusted capacity witness accepted", scenario, actual)
		}
	}
}
