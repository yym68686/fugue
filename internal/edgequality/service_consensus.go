package edgequality

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"fugue/internal/clientmeasurement"
)

func ServiceConsensusSnapshot(hostname string, receipts []Receipt) (Snapshot, error) {
	if hostname == "" || len(receipts) < 2 || len(receipts) > 8 {
		return Snapshot{}, errors.New("bounded complete shared-service receipt set required")
	}
	ordered := append([]Receipt(nil), receipts...)
	sort.Slice(ordered, func(left, right int) bool {
		return ServiceEvidenceKey(ordered[left].Snapshot) < ServiceEvidenceKey(ordered[right].Snapshot)
	})
	first := ordered[0].Snapshot
	if !IsDeliveryNetworkPolicy(first.Policy.Version) || len(first.ActualDNSReceipt) == 0 {
		return Snapshot{}, errors.New("shared-service selection requires bound V4 evidence")
	}
	root := Snapshot{Schema: Schema, Hostname: hostname, TrafficClass: first.TrafficClass, Scope: first.Scope, CapturedAt: first.CapturedAt, Policy: first.Policy,
		CurrentEdgeID: first.CurrentEdgeID, LastSwitchAt: first.LastSwitchAt, ActualDNSReceipt: first.ActualDNSReceipt, ServiceReceipts: ordered,
		Candidates: []Candidate{}, Observations: []Observation{}, Blockers: []string{}, Limitations: []string{"shared_dns_alias_requires_all_service_owners", "dns_resolver_scope_is_not_terminal_path"}}
	seen := map[string]bool{}
	nodes := map[string][]Candidate{}
	for _, receipt := range ordered {
		snapshot := receipt.Snapshot
		key := ServiceEvidenceKey(snapshot)
		if len(snapshot.ServiceReceipts) > 0 || seen[key] || snapshot.Hostname == hostname && snapshot.PathPrefix == "" || DNSHostname(snapshot) != hostname || snapshot.Schema != Schema || snapshot.TrafficClass != first.TrafficClass && (snapshot.PathPrefix == "" || first.PathPrefix == "") || snapshot.Scope != first.Scope ||
			snapshot.CurrentEdgeID != first.CurrentEdgeID || !reflect.DeepEqual(snapshot.LastSwitchAt, first.LastSwitchAt) || !reflect.DeepEqual(snapshot.Policy, first.Policy) || !bytes.Equal(snapshot.ActualDNSReceipt, first.ActualDNSReceipt) {
			return Snapshot{}, errors.New("shared-service receipts have different owners, policy or actual DNS authority")
		}
		if _, err := Replay(receipt); err != nil {
			return Snapshot{}, err
		}
		seen[key] = true
		if snapshot.CapturedAt.After(root.CapturedAt) {
			root.CapturedAt = snapshot.CapturedAt
		}
		for _, blocker := range snapshot.Blockers {
			root.Blockers = append(root.Blockers, snapshot.Hostname+":"+blocker)
		}
		for _, candidate := range snapshot.Candidates {
			nodes[candidate.EdgeID] = append(nodes[candidate.EdgeID], candidate)
		}
	}
	for edgeID, candidates := range nodes {
		candidate := Candidate{EdgeID: edgeID, EdgeGroupID: candidates[0].EdgeGroupID, RouteProofVerified: len(candidates) == len(ordered), HardGates: []string{}}
		digests := []string{}
		for _, child := range candidates {
			if child.EdgeGroupID != candidate.EdgeGroupID || !child.RouteProofVerified || child.ProofObservedAt == nil {
				candidate.RouteProofVerified = false
			}
			if child.ProofObservedAt != nil && (candidate.ProofObservedAt == nil || child.ProofObservedAt.Before(*candidate.ProofObservedAt)) {
				at := *child.ProofObservedAt
				candidate.ProofObservedAt = &at
			}
			candidate.HardGates = append(candidate.HardGates, child.HardGates...)
			digests = append(digests, child.RouteGeneration)
		}
		if !candidate.RouteProofVerified {
			candidate.HardGates = append(candidate.HardGates, "shared_service_route_proof_incomplete")
		}
		raw, _ := json.Marshal(digests)
		candidate.RouteGeneration = clientmeasurement.Digest(raw)
		root.Candidates = append(root.Candidates, candidate)
	}
	sort.Slice(root.Candidates, func(left, right int) bool { return root.Candidates[left].EdgeID < root.Candidates[right].EdgeID })
	return root, nil
}

func ServiceEvidenceKey(snapshot Snapshot) string {
	if snapshot.PathPrefix == "" {
		return snapshot.Hostname
	}
	return snapshot.Hostname + "\x00" + snapshot.PathPrefix + "\x00" + snapshot.TrafficClass
}

func evaluateServiceConsensus(snapshot Snapshot) (Result, error) {
	expected, err := ServiceConsensusSnapshot(snapshot.Hostname, snapshot.ServiceReceipts)
	if err != nil {
		return Result{}, err
	}
	actualJSON, _ := json.Marshal(snapshot)
	expectedJSON, _ := json.Marshal(expected)
	if !bytes.Equal(actualJSON, expectedJSON) {
		return Result{}, errors.New("shared-service aggregate differs from captured service receipts")
	}
	result := Result{Mode: "shadow", DNSUnchanged: true, Hypothesis: "hold", ProposedEdgeID: snapshot.CurrentEdgeID, Candidates: []Assessment{}, ProbeEdgeIDs: []string{}, RejectedRecords: map[string]int{},
		Blockers: append([]string{"shadow_only_not_serving_authority"}, snapshot.Blockers...)}
	for _, candidate := range snapshot.Candidates {
		assessment := Assessment{EdgeID: candidate.EdgeID, EdgeGroupID: candidate.EdgeGroupID, HardGates: append([]string{}, candidate.HardGates...), Ready: candidate.RouteProofVerified, Metrics: map[string]Metric{}, Missing: []string{}, BucketCount: snapshot.Policy.RequiredBuckets}
		for _, receipt := range snapshot.ServiceReceipts {
			found := false
			for _, child := range receipt.Result.Candidates {
				if child.EdgeID != candidate.EdgeID {
					continue
				}
				found = true
				assessment.Ready = assessment.Ready && child.Ready
				assessment.Score = max(assessment.Score, child.Score)
				assessment.Lower = max(assessment.Lower, child.Lower)
				assessment.Upper = max(assessment.Upper, child.Upper)
				assessment.RecordCount += child.RecordCount
				assessment.BucketCount = min(assessment.BucketCount, child.BucketCount)
				for name, metric := range child.Metrics {
					assessment.Metrics[ServiceEvidenceKey(receipt.Snapshot)+":"+name] = metric
				}
				for _, missing := range child.Missing {
					assessment.Missing = append(assessment.Missing, ServiceEvidenceKey(receipt.Snapshot)+":"+missing)
				}
				assessment.HardGates = append(assessment.HardGates, child.HardGates...)
			}
			assessment.Ready = assessment.Ready && found
		}
		assessment.Ready = assessment.Ready && len(assessment.HardGates) == 0
		result.Candidates = append(result.Candidates, assessment)
	}
	sort.Slice(result.Candidates, func(left, right int) bool {
		if result.Candidates[left].Score != result.Candidates[right].Score {
			return result.Candidates[left].Score < result.Candidates[right].Score
		}
		return result.Candidates[left].EdgeID < result.Candidates[right].EdgeID
	})
	if len(snapshot.Blockers) > 0 {
		result.Blockers = append(result.Blockers, "shared_service_evidence_incomplete")
		return result, nil
	}
	currentGated := true
	for _, candidate := range result.Candidates {
		if candidate.EdgeID == snapshot.CurrentEdgeID {
			currentGated = len(candidate.HardGates) > 0
		}
	}
	for _, candidate := range result.Candidates {
		if !candidate.Ready || candidate.EdgeID == snapshot.CurrentEdgeID {
			continue
		}
		if currentGated {
			result.Hypothesis, result.ProposedEdgeID = "failover", candidate.EdgeID
			return result, nil
		}
		unanimous, buckets := true, snapshot.Policy.RequiredBuckets
		for _, receipt := range snapshot.ServiceReceipts {
			unanimous = unanimous && receipt.Result.Hypothesis == "switch" && receipt.Result.ProposedEdgeID == candidate.EdgeID
			buckets = min(buckets, receipt.Result.SustainedBuckets)
		}
		if unanimous && buckets >= snapshot.Policy.RequiredBuckets && snapshot.LastSwitchAt != nil && snapshot.CapturedAt.Sub(*snapshot.LastSwitchAt) >= time.Duration(snapshot.Policy.CooldownSeconds)*time.Second {
			result.Hypothesis, result.ProposedEdgeID, result.SustainedBuckets = "switch", candidate.EdgeID, buckets
			return result, nil
		}
	}
	result.Blockers = append(result.Blockers, "shared_service_advantage_not_unanimous")
	return result, nil
}
