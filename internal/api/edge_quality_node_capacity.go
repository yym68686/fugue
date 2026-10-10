package api

import (
	"context"
	"sort"
	"sync"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
)

type qualityCapacityTarget struct {
	edgeID  string
	groupID string
	address string
}

func qualityCapacityTargets(nodes []model.EdgeNode, evidence dnsserver.QualityAnswerEvidence) []qualityCapacityTarget {
	eligible := map[string]qualityCapacityTarget{}
	for _, node := range nodes {
		for _, candidate := range evidence.Candidates {
			if candidate.EdgeID != node.ID || candidate.EdgeGroupID != node.EdgeGroupID || candidate.IP == "" || candidate.IP != node.PublicIPv4 && candidate.IP != node.PublicIPv6 {
				continue
			}
			proofs := 0
			for _, proof := range evidence.Proofs {
				if proof.EdgeID == node.ID && proof.EdgeGroupID == node.EdgeGroupID && proof.Hostname == evidence.Hostname {
					proofs++
				}
			}
			if proofs == 1 {
				target := qualityCapacityTarget{edgeID: node.ID, groupID: node.EdgeGroupID, address: candidate.IP}
				if old, exists := eligible[node.ID]; !exists || target.address < old.address {
					eligible[node.ID] = target
				}
			}
		}
	}
	if len(eligible) > 8 {
		return nil
	}
	result := make([]qualityCapacityTarget, 0, len(eligible))
	for _, target := range eligible {
		result = append(result, target)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].edgeID < result[right].edgeID })
	return result
}

func captureQualityNodeCapacity(ctx context.Context, targets []qualityCapacityTarget, read func(context.Context, string, string) (*model.EdgeNetworkNodeCapacity, error)) []edgequality.NodeCapacitySample {
	if len(targets) > 8 || len(targets) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	results := make([]*edgequality.NodeCapacitySample, len(targets))
	jobs := make(chan int, len(targets))
	for index := range targets {
		jobs <- index
	}
	close(jobs)
	var workers sync.WaitGroup
	for worker := 0; worker < min(4, len(targets)); worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				if ctx.Err() != nil {
					return
				}
				target := targets[index]
				capacity, err := read(ctx, target.edgeID, target.address)
				if err != nil || capacity == nil {
					continue
				}
				sample := edgequality.NodeCapacitySample{ID: model.NewID("node_capacity"), EdgeID: target.edgeID, EdgeGroupID: target.groupID, Address: target.address, Capacity: *capacity}
				if edgequality.ValidateNodeCapacitySample(sample, time.Now().UTC()) == nil {
					results[index] = &sample
				}
			}
		}()
	}
	workers.Wait()
	out := []edgequality.NodeCapacitySample{}
	for _, sample := range results {
		if sample != nil {
			out = append(out, *sample)
		}
	}
	return out
}

func (s *Server) captureQualityCapacity(ctx context.Context, snapshot *edgequality.Snapshot, receipt dnsserver.DNSDecisionReceipt, nodes []model.EdgeNode) {
	if s.newClusterNodeClient == nil {
		return
	}
	evidence, err := dnsserver.QualityEvidenceFromDNSDecision(receipt, time.Now().UTC(), time.Duration(snapshot.Policy.EvidenceMaxAgeSeconds)*time.Second)
	if err != nil || evidence.Hostname != edgequality.DNSHostname(*snapshot) || evidence.Scope != snapshot.Scope {
		return
	}
	evidence.Hostname = snapshot.Hostname
	targets := qualityCapacityTargets(nodes, evidence)
	if len(targets) == 0 {
		return
	}
	client, err := s.newClusterNodeClient()
	if err != nil {
		return
	}
	defer client.closeIdleConnections()
	snapshot.NodeCapacitySamples = captureQualityNodeCapacity(ctx, targets, func(ctx context.Context, edgeID, address string) (*model.EdgeNetworkNodeCapacity, error) {
		return readNetworkNodeCapacity(ctx, client, edgeID, address, time.Now)
	})
}

func physicalCapacityAddressMatches(sample edgequality.NodeCapacitySample, evidence dnsserver.QualityAnswerEvidence) bool {
	for _, candidate := range evidence.Candidates {
		if sample.EdgeID == candidate.EdgeID && sample.EdgeGroupID == candidate.EdgeGroupID && sample.Address == candidate.IP {
			return true
		}
	}
	return false
}
