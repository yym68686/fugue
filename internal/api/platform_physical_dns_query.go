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
}

func (s *Server) capturePhysicalDNSQueries(ctx context.Context, projection *platformIntentProjectionResponse, policy platformconfig.DNSQueryPolicy) error {
	if len(policy.PhysicalRoutes) == 0 {
		return nil
	}
	selections := map[string]compiledPhysicalDNSSelection{}
	for _, route := range policy.PhysicalRoutes {
		matched := false
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
			receipt, err := s.capturePhysicalQualityWithAnswer(captureContext, route.Hostname, route.TrafficClass, edgeQualityRankScope{}, fact.NodeID, route.Policy, &answer)
			cancel()
			if err != nil {
				return fmt.Errorf("physical DNS evidence capture: %w", err)
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
			return fmt.Errorf("physical route lacks an owned dynamic DNS query")
		}
	}
	return applyPhysicalDNSSelections(projection, policy, selections, time.Now().UTC())
}

func applyPhysicalDNSSelections(projection *platformIntentProjectionResponse, policy platformconfig.DNSQueryPolicy, selections map[string]compiledPhysicalDNSSelection, now time.Time) error {
	if err := platformconfig.ValidateDNSQueryPolicy(&policy); err != nil {
		return err
	}
	requested := map[string]platformconfig.PhysicalQualityRoute{}
	evidenceHosts := map[string]string{}
	ownerSets := map[string]map[string]bool{}
	serviceSets := map[string]map[string]bool{}
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
			owners, _, err := placementRecordRoutes(*projection, record)
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
		if evidenceHosts[fact.Hostname] != fact.Hostname || len(ownerSets[fact.Hostname]) > 1 || policy.DynamicQuality != nil {
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
