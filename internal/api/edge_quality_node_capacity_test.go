package api

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
)

func qualityCapacityFixture(now time.Time) model.EdgeNetworkNodeCapacity {
	observed := now.Add(-time.Second)
	return model.EdgeNetworkNodeCapacity{Source: "kubelet_node_allocatable_v1", NodeUID: "node-live", ObservedAt: observed, ValidUntil: observed.Add(2 * time.Minute),
		CPUObservedAt: observed, MemoryObservedAt: observed, CPUUsageNanoCores: 10_000_000, CPUAllocatableMilliCores: 1000, MemoryWorkingSetBytes: 100, MemoryAllocatableBytes: 1000, Pressure: []string{}}
}

func TestQualityCapacityTargetsRequireExactPhysicalDNSAndInventory(t *testing.T) {
	now := time.Now().UTC()
	sample, proof := networkWitnessFixture(now)
	for _, scenario := range []string{"matching", "foreign_node", "foreign_group", "foreign_address", "missing_proof", "duplicate_proof", "foreign_host", "duplicate_address"} {
		t.Run(scenario, func(t *testing.T) {
			nodes := []model.EdgeNode{{ID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, PublicIPv4: "203.0.113.5"}}
			evidence := dnsserver.QualityAnswerEvidence{Hostname: sample.Hostname, Candidates: []model.EdgeDNSAnswerCandidate{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, IP: "203.0.113.5"}},
				Proofs: []dnsserver.QualityRouteProof{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, Hostname: sample.Hostname, Path: sample.PathPrefix, Proof: proof}}}
			switch scenario {
			case "foreign_node":
				nodes[0].ID = "other"
			case "foreign_group":
				nodes[0].EdgeGroupID = "other"
			case "foreign_address":
				nodes[0].PublicIPv4 = "203.0.113.6"
			case "missing_proof":
				evidence.Proofs = nil
			case "duplicate_proof":
				evidence.Proofs = append(evidence.Proofs, evidence.Proofs[0])
			case "foreign_host":
				evidence.Proofs[0].Hostname = "other.example.test"
			case "duplicate_address":
				evidence.Candidates = append(evidence.Candidates, evidence.Candidates[0])
			}
			targets := qualityCapacityTargets(nodes, evidence)
			if scenario == "matching" || scenario == "duplicate_address" {
				if len(targets) != 1 || targets[0].edgeID != sample.EdgeID || targets[0].address != "203.0.113.5" {
					t.Fatal(targets)
				}
			} else if len(targets) != 0 {
				t.Fatal("unbound node was selected for observation", targets)
			}
		})
	}
}

func TestQualityCapacityCollectionIsBoundedReadOnlyAndKeepsTimes(t *testing.T) {
	now := time.Now().UTC()
	targets := []qualityCapacityTarget{}
	for index := 0; index < 8; index++ {
		targets = append(targets, qualityCapacityTarget{edgeID: fmt.Sprintf("edge-%d", index), groupID: "group-a", address: fmt.Sprintf("203.0.113.%d", index+1)})
	}
	var active, maximum, calls atomic.Int32
	read := func(ctx context.Context, edgeID, address string) (*model.EdgeNetworkNodeCapacity, error) {
		calls.Add(1)
		current := active.Add(1)
		defer active.Add(-1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		deadline, present := ctx.Deadline()
		if !present || time.Until(deadline) > 2*time.Second {
			t.Error("capacity read lacks total deadline")
		}
		time.Sleep(5 * time.Millisecond)
		if edgeID == "edge-0" {
			return nil, errors.New("unavailable")
		}
		capacity := qualityCapacityFixture(now)
		return &capacity, nil
	}
	samples := captureQualityNodeCapacity(context.Background(), targets, read)
	if len(samples) != 7 || calls.Load() != 8 || maximum.Load() > 4 {
		t.Fatal("capacity read exceeded budget or invented missing facts", len(samples), calls.Load(), maximum.Load())
	}
	for index, sample := range samples {
		if sample.EdgeID != targets[index+1].edgeID || sample.Address != targets[index+1].address || !reflect.DeepEqual(sample.Capacity, qualityCapacityFixture(now)) {
			t.Fatal("node identity, order, original metric timestamp or denominator changed", sample)
		}
	}
	calls.Store(0)
	if samples := captureQualityNodeCapacity(context.Background(), append(targets, targets[0]), read); len(samples) != 0 || calls.Load() != 0 {
		t.Fatal("over-budget node population was partially promoted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if samples := captureQualityNodeCapacity(ctx, targets, read); len(samples) != 0 || calls.Load() != 0 {
		t.Fatal("cancelled capture still read nodes")
	}
}

func TestQualityCapacityDoesNotReadWithoutActualDNSAuthority(t *testing.T) {
	server := &Server{newClusterNodeClient: func() (*clusterNodeClient, error) {
		t.Fatal("unverified DNS receipt authorized Kubernetes observation")
		return nil, errors.New("unexpected read")
	}}
	snapshot := edgequality.Snapshot{Hostname: "app.example.test", Scope: "global", Policy: edgequality.DefaultNetworkPolicy()}
	server.captureQualityCapacity(context.Background(), &snapshot, dnsserver.DNSDecisionReceipt{}, []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "group-a", PublicIPv4: "203.0.113.5"}})
	if len(snapshot.NodeCapacitySamples) != 0 {
		t.Fatal("unbound capture fabricated node capacity")
	}
}

func TestQualityCapacityCancelsBlockedReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	samples := captureQualityNodeCapacity(ctx, []qualityCapacityTarget{{edgeID: "edge-a", groupID: "group-a", address: "203.0.113.5"}}, func(ctx context.Context, _, _ string) (*model.EdgeNetworkNodeCapacity, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if len(samples) != 0 || time.Since(started) > time.Second {
		t.Fatal("failed node read blocked capture or invented capacity")
	}
}

func TestQualityCapacityPublicationReconstructsExactFacts(t *testing.T) {
	now := time.Now().UTC()
	sample, proof := networkWitnessFixture(now)
	capacity := edgequality.NodeCapacitySample{ID: "capacity-a", EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, Address: "203.0.113.5", Capacity: qualityCapacityFixture(now)}
	evidence := dnsserver.QualityAnswerEvidence{EdgeID: sample.EdgeID, Hostname: sample.Hostname, Scope: "global",
		Candidates: []model.EdgeDNSAnswerCandidate{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, IP: capacity.Address}},
		Proofs:     []dnsserver.QualityRouteProof{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, Hostname: sample.Hostname, Path: sample.PathPrefix, Proof: proof}}}
	snapshot := edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: now, Hostname: sample.Hostname, TrafficClass: sample.TrafficClass, Scope: "global", Policy: edgequality.DefaultNetworkPolicy(),
		Candidates: []edgequality.Candidate{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID}}, NodeCapacitySamples: []edgequality.NodeCapacitySample{capacity}}
	bindPhysicalQualityEvidence(&snapshot, evidence)
	if len(snapshot.Observations) != 1 || validateBoundPhysicalQualitySnapshot(snapshot, evidence) != nil {
		t.Fatal("independent capacity did not bind or reconstruct", snapshot.Observations)
	}
	if _, err := edgequality.Capture(snapshot); err != nil {
		t.Fatal(err)
	}
	changed := snapshot
	changed.NodeCapacitySamples = append([]edgequality.NodeCapacitySample(nil), snapshot.NodeCapacitySamples...)
	changed.NodeCapacitySamples[0].Address = "203.0.113.6"
	if validateBoundPhysicalQualitySnapshot(changed, evidence) == nil {
		t.Fatal("capacity from another public identity authorized selection")
	}
	changed = snapshot
	changed.Observations = nil
	if validateBoundPhysicalQualitySnapshot(changed, evidence) == nil {
		t.Fatal("compiler allowed discarding captured capacity facts")
	}
}
