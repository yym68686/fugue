package edgequality

import (
	"errors"
	"sort"
	"time"

	"fugue/internal/model"
)

type DNSBinding struct {
	ReceiptID      string
	LoadedDigest   string
	PolicyDigest   string
	Hostname       string
	Scope          string
	CurrentEdgeID  string
	ObservedAt     time.Time
	ReplayMatched  bool
	WriteSucceeded bool
}

func DNSHostname(snapshot Snapshot) string {
	if snapshot.DNSHostname != "" {
		return snapshot.DNSHostname
	}
	return snapshot.Hostname
}

func CompileSelection(receipt Receipt, binding DNSBinding, now time.Time) (*model.DNSPhysicalSelection, error) {
	result, err := Replay(receipt)
	if err != nil {
		return nil, err
	}
	snapshot := receipt.Snapshot
	maximumAge := time.Duration(snapshot.Policy.EvidenceMaxAgeSeconds) * time.Second
	if maximumAge <= 0 || maximumAge > time.Duration(snapshot.Policy.WindowSeconds)*time.Second ||
		now.Before(snapshot.CapturedAt) || now.Sub(snapshot.CapturedAt) > maximumAge ||
		binding.ObservedAt.IsZero() || binding.ObservedAt.After(snapshot.CapturedAt) || snapshot.CapturedAt.Sub(binding.ObservedAt) > maximumAge ||
		!binding.ReplayMatched || !binding.WriteSucceeded || binding.Hostname != DNSHostname(snapshot) || binding.Scope != snapshot.Scope || binding.CurrentEdgeID != snapshot.CurrentEdgeID {
		return nil, errors.New("physical selection lacks fresh matching actual DNS evidence")
	}
	if len(snapshot.Blockers) != 0 {
		return nil, errors.New("physical selection evidence is incomplete")
	}
	var primary *Assessment
	for index := range result.Candidates {
		candidate := &result.Candidates[index]
		if candidate.EdgeID == result.ProposedEdgeID {
			primary = candidate
		}
	}
	learning := false
	historical := result.History != nil && result.ProposedEdgeID == result.History.ReusedOrder[0]
	if IsDeliveryNetworkPolicy(snapshot.Policy.Version) && result.Hypothesis == "hold" && result.ProposedEdgeID == snapshot.CurrentEdgeID && primary != nil && !primary.Ready && len(primary.HardGates) == 0 {
		for _, candidate := range snapshot.Candidates {
			learning = learning || candidate.EdgeID == primary.EdgeID && proofFresh(candidate, snapshot.Policy, now)
		}
	}
	if primary == nil || !primary.Ready && !learning && !historical || len(primary.HardGates) != 0 {
		return nil, errors.New("physical selection primary lacks complete network evidence")
	}
	if result.Hypothesis != "hold" && result.Hypothesis != "switch" && result.Hypothesis != "failover" {
		return nil, errors.New("physical selection hypothesis is unsupported")
	}
	selection := &model.DNSPhysicalSelection{Version: model.DNSPhysicalSelectionVersion, PrimaryEdgeID: primary.EdgeID,
		OrderedEdgeIDs: []string{primary.EdgeID}, EvidenceDigest: receipt.Digest, DNSReceiptID: binding.ReceiptID,
		LoadedDigest: binding.LoadedDigest, PolicyDigest: binding.PolicyDigest, Scope: snapshot.Scope, CapturedAt: snapshot.CapturedAt}
	if IsDeliveryNetworkPolicy(snapshot.Policy.Version) {
		selection.QualityState = "measured"
		if learning {
			selection.QualityState = "learning"
		}
	}
	if snapshot.Policy.Version == PairedContinuityPolicyVersion {
		selection.QualityState, selection.QualityObservedAt = result.QualityState, result.QualityObservedAt
		selection.History = model.ClonePhysicalQualityHistoryUse(result.History)
	}
	primarySince := snapshot.CapturedAt
	if primary.EdgeID == snapshot.CurrentEdgeID && snapshot.LastSwitchAt != nil {
		primarySince = *snapshot.LastSwitchAt
	}
	selection.PrimarySince = &primarySince
	for _, candidate := range result.Candidates {
		if candidate.EdgeID != primary.EdgeID && candidate.Ready && len(candidate.HardGates) == 0 {
			selection.OrderedEdgeIDs = append(selection.OrderedEdgeIDs, candidate.EdgeID)
		}
	}
	if IsNetworkPolicy(snapshot.Policy.Version) {
		for _, candidate := range result.Candidates {
			if candidate.EdgeID == primary.EdgeID || candidate.Ready || len(candidate.HardGates) != 0 {
				continue
			}
			for _, input := range snapshot.Candidates {
				if input.EdgeID == candidate.EdgeID && proofFresh(input, snapshot.Policy, now) {
					selection.OrderedEdgeIDs = append(selection.OrderedEdgeIDs, candidate.EdgeID)
					break
				}
			}
		}
	}
	if snapshot.Policy.Version == PairedContinuityPolicyVersion && result.Hypothesis == "hold" && result.QualityState != "measured" {
		eligible := map[string]bool{}
		for _, candidate := range snapshot.Candidates {
			eligible[candidate.EdgeID] = len(candidate.HardGates) == 0 && candidate.ServingBlockedReason == "" && proofFresh(candidate, snapshot.Policy, now)
		}
		order := []string{primary.EdgeID}
		seen := map[string]bool{primary.EdgeID: true}
		if result.QualityState == "retained" && snapshot.PreviousSelection != nil {
			for _, id := range snapshot.PreviousSelection.OrderedEdgeIDs {
				if eligible[id] && !seen[id] {
					order, seen[id] = append(order, id), true
				}
			}
		}
		// Newly measured candidates without comparable client evidence are
		// appended deterministically, never promoted by a missing score of zero.
		ids := []string{}
		for id, allowed := range eligible {
			if allowed && !seen[id] {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		selection.OrderedEdgeIDs = append(order, ids...)
	}
	if historical {
		// Recheck at publication time, not just at capture time. Historical
		// ranking never extends a current route lease or admission grant.
		order := []string{}
		for _, id := range result.History.ReusedOrder {
			for _, candidate := range snapshot.Candidates {
				if candidate.EdgeID == id && len(candidate.HardGates) == 0 && candidate.ServingBlockedReason == "" && proofFresh(candidate, snapshot.Policy, now) {
					order = append(order, id)
				}
			}
		}
		if len(order) == 0 || order[0] != selection.PrimaryEdgeID {
			return nil, errors.New("historical primary lacks current route proof")
		}
		selection.OrderedEdgeIDs = order
		selection.History.ReusedOrder = append([]string(nil), order...)
	}
	if err := model.ValidateDNSPhysicalSelection(selection); err != nil {
		return nil, err
	}
	return selection, nil
}
