package api

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

type compiledPhysicalDNSSelection struct {
	Selection *model.DNSPhysicalSelection
	Evidence  json.RawMessage
	// Cache eligibility only; the publisher still revalidates Evidence.
	proofFreshUntil time.Time
}

func (s *Server) capturePhysicalDNSQueries(ctx context.Context, projection *platformIntentProjectionResponse, policy platformconfig.DNSQueryPolicy) error {
	if len(policy.PhysicalRoutes) == 0 {
		return nil
	}
	selections := map[string]compiledPhysicalDNSSelection{}
	coverage := []map[string]any{}
	for _, route := range policy.PhysicalRoutes {
		matched := false
		unifiedSources := []edgequality.Receipt{}
		for _, fact := range projection.RuntimeSnapshot.DNSSelections {
			if fact.Hostname != route.Hostname {
				continue
			}
			matched = true
			key := fact.NodeID + "\x00" + fact.Hostname
			if selections[key].Selection != nil {
				continue
			}
			captureContext, cancel := context.WithTimeout(ctx, 8*time.Second)
			answer, err := s.observePhysicalDNSAnswer(captureContext, projection.RuntimeSnapshot.DNSConsumers, fact.NodeID, route.Hostname)
			if err != nil {
				cancel()
				return fmt.Errorf("physical DNS actual answer capture: %w", err)
			}
			receipt, err := s.capturePhysicalQualityPathForDNS(captureContext, route.Hostname, route.PathPrefix, route.Hostname, route.TrafficClass, edgeQualityRankScope{}, fact.NodeID, route.Policy, &answer)
			cancel()
			if err != nil {
				return fmt.Errorf("physical DNS evidence capture: %w", err)
			}
			if route.BusinessExploration != nil {
				if route.BusinessExploration.Version == model.BusinessEdgeUnifiedVersion {
					unifiedSources = append(unifiedSources, receipt)
					continue
				}
				compiled, err := captureBusinessPublication(receipt, route, time.Now().UTC())
				if err != nil {
					return fmt.Errorf("business DNS evidence cannot authorize publication: %w", err)
				}
				selections[key] = compiled
				var evidence businessPublicationEvidence
				if err := json.Unmarshal(compiled.Evidence, &evidence); err != nil {
					return err
				}
				entry, err := businessExplorationCoverageFact(route, fact.NodeID, evidence)
				if err != nil {
					return err
				}
				coverage = append(coverage, entry)
				continue
			}
			selection, err := compileBoundPhysicalQualitySelection(receipt, time.Now().UTC())
			if err != nil {
				return fmt.Errorf("physical DNS evidence cannot authorize publication: %w", err)
			}
			raw, err := json.Marshal(receipt)
			if err != nil || len(raw) > 8<<20 {
				return fmt.Errorf("physical DNS evidence exceeds bounded snapshot")
			}
			selections[key] = compiledPhysicalDNSSelection{Selection: selection, Evidence: raw}
		}
		if !matched {
			return fmt.Errorf("physical route %q lacks an owned dynamic DNS query", route.Hostname)
		}
		if len(unifiedSources) > 0 {
			compiled, err := captureUnifiedBusinessPublications(unifiedSources, route, time.Now().UTC())
			if err != nil {
				return fmt.Errorf("unified business publication waiting for coherent source group: %w", err)
			}
			keys := make([]string, 0, len(compiled))
			for key := range compiled {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				selection := compiled[key]
				selections[key] = selection
				var evidence businessPublicationEvidence
				if err := json.Unmarshal(selection.Evidence, &evidence); err != nil {
					return err
				}
				entry, err := businessExplorationCoverageFact(route, strings.SplitN(key, "\x00", 2)[0], evidence)
				if err != nil {
					return err
				}
				coverage = append(coverage, entry)
			}
		}
	}
	if err := applyPhysicalDNSSelections(projection, policy, selections, time.Now().UTC()); err != nil {
		return err
	}
	if len(coverage) > 0 {
		if projection.RuntimeSnapshot.Facts == nil {
			projection.RuntimeSnapshot.Facts = map[string]any{}
		}
		projection.RuntimeSnapshot.Facts["business_exploration_coverage"] = coverage
	}
	return nil
}

func businessExplorationCoverageFact(route platformconfig.PhysicalQualityRoute, nodeID string, evidence businessPublicationEvidence) (map[string]any, error) {
	entry := map[string]any{"hostname": route.Hostname, "dns_node_id": nodeID, "intent_id": route.BusinessExploration.IntentID,
		"source_receipt_digest": evidence.Source.Digest, "allocation_receipt_digest": evidence.Evaluation.Digest,
		"raw_network_records": len(evidence.Source.Snapshot.NetworkSamples), "route_witness_bound_passive_records": len(evidence.Evaluation.Input.NetworkSamples),
		"action": evidence.Evaluation.Decision.Action, "reasons": evidence.Evaluation.Decision.Reasons, "coverage": evidence.Evaluation.Decision.Coverage,
		"comparisons": evidence.Evaluation.Decision.Comparisons, "rejected": evidence.Evaluation.Decision.Rejected, "valid_until": evidence.Evaluation.Decision.ValidUntil}
	if unified := evidence.Evaluation.Decision.State.Unified; unified != nil {
		entry["decision_version"] = route.BusinessExploration.Version
		entry["unified_decision"] = unified
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return nil, err
	}
	var stored map[string]any
	if err := json.Unmarshal(raw, &stored); err != nil {
		return nil, err
	}
	return stored, nil
}

func applyPhysicalDNSSelections(projection *platformIntentProjectionResponse, policy platformconfig.DNSQueryPolicy, selections map[string]compiledPhysicalDNSSelection, now time.Time) error {
	if err := platformconfig.ValidateDNSQueryPolicy(&policy); err != nil {
		return err
	}
	requested := map[string]platformconfig.PhysicalQualityRoute{}
	evidenceHosts := map[string]string{}
	ownerSets := map[string]map[string]bool{}
	serviceSets := map[string]map[string]bool{}
	readRoutes := newPlacementRecordRouteReader(*projection)
	for _, route := range policy.PhysicalRoutes {
		requested[route.Hostname] = route
		ownerSets[route.Hostname] = map[string]bool{}
		serviceSets[route.Hostname] = map[string]bool{}
		owned := false
		for _, record := range projection.Intent.DNS {
			if record.Hostname != route.Hostname {
				continue
			}
			if platformconfig.DNSPlacementOptions(record) == nil {
				if record.Type == "A" || record.Type == "AAAA" || record.Type == "CNAME" {
					return fmt.Errorf("physical route cannot rewrite a static address or alias record")
				}
				continue
			}
			owners, _, err := readRoutes(record)
			if err != nil {
				return err
			}
			for _, owner := range owners {
				if owner.DNSPlacementEdgeGroupID != "" || owner.EdgeGroupMode == model.PlatformRouteEdgeGroupModePinned ||
					owner.Hostname != route.Hostname && policy.DynamicQuality == nil {
					return fmt.Errorf("physical route conflicts with shared or pinned ownership")
				}
				evidenceHosts[route.Hostname] = owner.Hostname
				ownerSets[route.Hostname][owner.Hostname] = true
				serviceSets[route.Hostname][dynamicQualityServiceForRoute(owner).key()] = true
				owned = true
			}
		}
		if !owned {
			return fmt.Errorf("physical route has no dynamic owner")
		}
	}
	facts := append([]platformconfig.DNSSelectionObservation(nil), projection.RuntimeSnapshot.DNSSelections...)
	rules := append([]platformconfig.DNSAnswerRule(nil), projection.Policy.DNSAnswerRules...)
	used := map[string]bool{}
	unifiedDecisions := map[string]*model.DNSBusinessAllocation{}
	unifiedMembers := map[string]map[string]bool{}
	for index, fact := range facts {
		route, requested := requested[fact.Hostname]
		if !requested {
			continue
		}
		compiled := selections[fact.NodeID+"\x00"+fact.Hostname]
		selection := compiled.Selection
		if err := model.ValidateDNSPhysicalSelection(selection); err != nil {
			return err
		}
		if fact.Type != "A" || len(compiled.Evidence) == 0 || len(compiled.Evidence) > 8<<20 || !json.Valid(compiled.Evidence) {
			return fmt.Errorf("physical selection lacks supported address-family evidence")
		}
		if route.BusinessExploration != nil {
			expectedService := fact.Hostname + "\x00" + route.PathPrefix + "\x00" + route.TrafficClass
			if len(serviceSets[fact.Hostname]) != 1 || !serviceSets[fact.Hostname][expectedService] || evidenceHosts[fact.Hostname] != fact.Hostname {
				return fmt.Errorf("business exploration requires one exact owned service path; shared aliases need separate consensus")
			}
			var evidence businessPublicationEvidence
			if err := json.Unmarshal(compiled.Evidence, &evidence); err != nil {
				return err
			}
			verified, err := compileBoundBusinessSelection(evidence, route, now)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(verified, selection) {
				return fmt.Errorf("business DNS selection differs from reconstructed evidence")
			}
			if route.BusinessExploration.Version == model.BusinessEdgeUnifiedVersion {
				var actual struct {
					NodeID string `json:"node_id"`
				}
				if json.Unmarshal(evidence.Source.Snapshot.ActualDNSReceipt, &actual) != nil || actual.NodeID != fact.NodeID {
					return fmt.Errorf("unified DNS execution envelope belongs to another node")
				}
				common := model.CloneDNSBusinessAllocation(selection.BusinessAllocation)
				if common.ExecutionNodeID != fact.NodeID {
					return fmt.Errorf("unified allocation execution identity mismatch")
				}
				common.ExecutionNodeID = ""
				if previous := unifiedDecisions[fact.Hostname]; previous != nil && !reflect.DeepEqual(previous, common) {
					return fmt.Errorf("DNS executors cannot publish different unified business decisions")
				}
				unifiedDecisions[fact.Hostname] = common
				if unifiedMembers[fact.Hostname] == nil {
					unifiedMembers[fact.Hostname] = map[string]bool{}
				}
				unifiedMembers[fact.Hostname][fact.NodeID] = true
			}
		} else if selection.BusinessAllocation != nil {
			return fmt.Errorf("business DNS allocation lacks explicit producer intent")
		} else if evidenceHosts[fact.Hostname] != fact.Hostname || len(ownerSets[fact.Hostname]) > 1 || policy.DynamicQuality != nil {
			var receipt edgequality.Receipt
			if json.Unmarshal(compiled.Evidence, &receipt) != nil || edgequality.DNSHostname(receipt.Snapshot) != fact.Hostname {
				return fmt.Errorf("physical alias evidence differs from frozen service ownership")
			}
			captured := []string{edgequality.ServiceEvidenceKey(receipt.Snapshot)}
			pathBound := receipt.Snapshot.PathPrefix != ""
			if len(receipt.Snapshot.ServiceReceipts) > 0 {
				captured = nil
				for _, service := range receipt.Snapshot.ServiceReceipts {
					captured = append(captured, edgequality.ServiceEvidenceKey(service.Snapshot))
					pathBound = pathBound || service.Snapshot.PathPrefix != ""
				}
			}
			declared := []string{}
			owners := ownerSets[fact.Hostname]
			if pathBound {
				owners = serviceSets[fact.Hostname]
			} else if len(serviceSets[fact.Hostname]) != len(owners) {
				return fmt.Errorf("physical evidence omits declared service paths")
			}
			for hostname := range owners {
				declared = append(declared, hostname)
			}
			sort.Strings(captured)
			sort.Strings(declared)
			if !slices.Equal(captured, declared) {
				return fmt.Errorf("physical alias omits or adds a declared service owner")
			}
			verified, err := compileBoundPhysicalQualitySelection(receipt, now)
			if err != nil {
				return fmt.Errorf("physical alias evidence revalidation failed: %w", err)
			}
			if !reflect.DeepEqual(verified, selection) {
				return fmt.Errorf("physical alias selection differs from exact captured answer and service evidence: fields=%s", physicalSelectionDifferenceFields(verified, selection))
			}
		}
		if selection.CapturedAt.After(now) || now.Sub(selection.CapturedAt) > time.Duration(route.Policy.EvidenceMaxAgeSeconds)*time.Second || selection.Scope != "global" {
			return fmt.Errorf("physical selection has stale or unsupported answer scope")
		}
		available := map[string]bool{}
		for _, candidate := range fact.Candidates {
			available[candidate.EdgeID] = true
		}
		for _, edgeID := range selection.OrderedEdgeIDs {
			if !available[edgeID] {
				return fmt.Errorf("physical order contains an endpoint outside frozen query topology")
			}
		}
		updated := platformconfig.DNSSelectionObservation{NodeID: fact.NodeID, Hostname: fact.Hostname, Type: fact.Type,
			ObservedAt: selection.CapturedAt, PhysicalSelection: model.CloneDNSPhysicalSelection(selection), RankingVersion: selection.Version,
			RankingScope: selection.Scope, Reason: "bound_physical_network_evidence", Candidates: []platformconfig.DNSSelectionCandidate{}}
		if selection.QualityState == "learning" {
			updated.Reason = "physical_quality_learning_current_ready_primary"
		}
		updated.PhysicalEvidence = append(json.RawMessage(nil), compiled.Evidence...)
		for _, candidate := range fact.Candidates {
			updated.Candidates = append(updated.Candidates, platformconfig.DNSSelectionCandidate{IP: candidate.IP, EdgeID: candidate.EdgeID, EdgeGroupID: candidate.EdgeGroupID})
		}
		digest, err := platformconfig.Digest(updated)
		if err != nil {
			return err
		}
		updated.SourceDigest, updated.SourceGeneration = digest, "dns-observation_"+strings.TrimPrefix(digest, "sha256:")
		facts[index] = updated
		matched := false
		for ruleIndex, rule := range rules {
			if rule.NodeID == fact.NodeID && rule.Hostname == fact.Hostname && rule.Type == fact.Type {
				rules[ruleIndex] = platformconfig.DNSAnswerRule{NodeID: rule.NodeID, Hostname: rule.Hostname, Type: rule.Type,
					SelectionMode: model.DNSAnswerPolicyKindPhysicalQuality, TTLSeconds: rule.TTLSeconds}
				matched = true
			}
		}
		if !matched {
			return fmt.Errorf("physical selection has no exact query rule")
		}
		used[fact.Hostname] = true
	}
	if len(used) != len(requested) {
		return fmt.Errorf("physical route opt-in did not match dynamic query facts")
	}
	for hostname, allocation := range unifiedDecisions {
		if len(unifiedMembers[hostname]) != len(allocation.Policy.InitialDNSPublications) {
			return fmt.Errorf("unified business publication omits a declared DNS executor")
		}
		for nodeID := range allocation.Policy.InitialDNSPublications {
			if !unifiedMembers[hostname][nodeID] {
				return fmt.Errorf("unified business publication substitutes a DNS executor")
			}
		}
	}
	updatedPolicy := platformconfig.NormalizePolicySnapshot(projection.Policy)
	updatedPolicy.DNSAnswerRules = rules
	if err := platformconfig.ValidateDNSAnswerRules(rules); err != nil {
		return err
	}
	generation, err := platformconfig.PolicySnapshotGeneration(updatedPolicy)
	if err != nil {
		return err
	}
	updatedPolicy.Generation = generation
	projection.Policy = updatedPolicy
	projection.RuntimeSnapshot.PolicyGeneration = generation
	projection.RuntimeSnapshot.DNSSelections = facts
	projection.CapturedAt = now
	projection.RuntimeSnapshot.CapturedAt = &projection.CapturedAt
	return nil
}

func physicalSelectionDifferenceFields(verified, captured *model.DNSPhysicalSelection) string {
	if verified == nil || captured == nil {
		return "selection"
	}
	left, right := reflect.ValueOf(*verified), reflect.ValueOf(*captured)
	fields := []string{}
	for index := 0; index < left.NumField(); index++ {
		if !reflect.DeepEqual(left.Field(index).Interface(), right.Field(index).Interface()) {
			fields = append(fields, left.Type().Field(index).Name)
		}
	}
	return strings.Join(fields, ",")
}
