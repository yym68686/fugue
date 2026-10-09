package api

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"github.com/miekg/dns"
)

func compileBoundPhysicalQualitySelection(receipt edgequality.Receipt, now time.Time) (*model.DNSPhysicalSelection, error) {
	if !edgequality.IsNetworkPolicy(receipt.Snapshot.Policy.Version) {
		return nil, errors.New("physical publication requires the supported network policy")
	}
	if _, err := edgequality.Replay(receipt); err != nil {
		return nil, err
	}
	var answer dnsserver.DNSDecisionReceipt
	if len(receipt.Snapshot.ActualDNSReceipt) == 0 || json.Unmarshal(receipt.Snapshot.ActualDNSReceipt, &answer) != nil {
		return nil, errors.New("physical publication lacks a captured actual DNS answer")
	}
	if answer.QType != dns.TypeA {
		return nil, errors.New("physical publication requires actual IPv4 DNS evidence")
	}
	evidence, err := dnsserver.QualityEvidenceFromDNSDecision(answer, now, time.Duration(receipt.Snapshot.Policy.EvidenceMaxAgeSeconds)*time.Second)
	if err != nil {
		return nil, err
	}
	if err := validateBoundPhysicalQualitySnapshot(receipt.Snapshot, evidence); err != nil {
		return nil, err
	}
	binding := edgequality.DNSBinding{ReceiptID: evidence.ReceiptID, Hostname: evidence.Hostname, Scope: evidence.Scope,
		CurrentEdgeID: evidence.EdgeID, ObservedAt: evidence.ObservedAt, ReplayMatched: true, WriteSucceeded: true,
		LoadedDigest: evidence.Publication.Digest, PolicyDigest: evidence.Publication.PolicyDigest}
	return edgequality.CompileSelection(receipt, binding, now)
}

func validateBoundPhysicalQualitySnapshot(snapshot edgequality.Snapshot, evidence dnsserver.QualityAnswerEvidence) error {
	if evidence.Hostname != snapshot.Hostname || evidence.Scope != snapshot.Scope || evidence.EdgeID != snapshot.CurrentEdgeID ||
		!sameQualityTime(snapshot.LastSwitchAt, evidence.PrimarySince) {
		return errors.New("physical publication context or cooldown differs from actual DNS answer")
	}
	derived := snapshot
	derived.Observations = nil
	derived.Candidates = append([]edgequality.Candidate(nil), snapshot.Candidates...)
	for index := range derived.Candidates {
		candidate := &derived.Candidates[index]
		candidate.HardGates = append([]string(nil), candidate.HardGates...)
		candidate.RouteGeneration, candidate.RouteProofVerified, candidate.ProofObservedAt = "", false, nil
	}
	bindPhysicalQualityEvidence(&derived, evidence)
	for index, candidate := range snapshot.Candidates {
		bound := derived.Candidates[index]
		if candidate.RouteGeneration != bound.RouteGeneration || candidate.RouteProofVerified != bound.RouteProofVerified || !sameQualityTime(candidate.ProofObservedAt, bound.ProofObservedAt) {
			return errors.New("physical publication candidate differs from captured route proof")
		}
	}
	observations := make(map[string]edgequality.Observation, len(derived.Observations))
	for _, observation := range derived.Observations {
		observations[observation.ID] = observation
	}
	seen := map[string]bool{}
	for _, observation := range snapshot.Observations {
		if observation.ClientSource != "public_tcp_info" && observation.ServiceSource != "service_endpoint_tcp" && observation.CapacitySource != "kubelet_node_allocatable_v1" {
			continue
		}
		bound, found := observations[observation.ID]
		if !found || seen[observation.ID] || !reflect.DeepEqual(observation, bound) {
			return errors.New("physical publication measurement differs from captured network evidence")
		}
		seen[observation.ID] = true
	}
	if len(seen) != len(observations) {
		return errors.New("physical publication omits captured network measurements")
	}
	return nil
}

func sameQualityTime(left, right *time.Time) bool {
	return left == nil && right == nil || left != nil && right != nil && left.Equal(*right)
}
