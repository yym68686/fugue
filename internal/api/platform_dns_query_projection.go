package api

import (
	"fmt"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Import current signed selection configuration once into a migration draft.
// The executor receives only the compiled snapshot; it never reads these legacy
// publications or business tables. Health/route/TLS flags are deliberately not
// copied into the candidate observations.
func (s *Server) projectDNSQueryRules(result *platformIntentProjectionResponse, nodes []model.DNSNode) error {
	desired := map[string]platformconfig.DNSIntent{}
	for _, record := range result.Intent.DNS {
		if platformconfig.DNSPlacementOptions(record) == nil {
			continue
		}
		routes, _, err := placementRecordRoutes(*result, record)
		if err != nil {
			return err
		}
		enabled := record.Status == "" || record.Status == model.EdgeRouteStatusActive
		for _, route := range routes {
			enabled = enabled && platformconfig.DNSRouteStateAllowed(record, route, result.Policy)
		}
		if enabled {
			desired[record.Hostname] = record
		}
	}
	rules := []platformconfig.DNSAnswerRule{}
	facts := []platformconfig.DNSSelectionObservation{}
	for _, consumer := range result.Intent.DNSConsumers {
		for _, zone := range consumer.Zones {
			var identity *model.DNSNode
			for i := range nodes {
				node := &nodes[i]
				if firstNonEmpty(node.PhysicalNodeID, node.ID) == consumer.NodeID && node.EdgeGroupID == consumer.EdgeGroupID && node.Zone == zone {
					if identity != nil {
						return fmt.Errorf("ambiguous DNS query source zone identity")
					}
					identity = node
				}
			}
			if identity == nil {
				return fmt.Errorf("DNS query source zone is unavailable")
			}
			options, ok := s.edgeDNSBundleOptionsForDNSNode(*identity)
			if !ok {
				return fmt.Errorf("DNS query source endpoint is unavailable")
			}
			now := time.Now().UTC()
			bundle, found, err := s.edgeDNSBundleArtifactForOptions(options, now)
			if err != nil || !found {
				return fmt.Errorf("trusted published DNS query source is unavailable")
			}
			digest, err := platformconfig.Digest(bundle)
			if err != nil {
				return err
			}
			for _, record := range bundle.Records {
				intent, wanted := desired[record.Name]
				if !wanted || (record.Type != "A" && record.Type != "AAAA") {
					continue
				}
				// A nested zone owns its records even if an older parent bundle also
				// contains them. One physical consumer has exactly one rule per RRset.
				owner := ""
				for _, candidate := range consumer.Zones {
					if edgeDNSTargetWithinZone(record.Name, candidate) && len(candidate) > len(owner) {
						owner = candidate
					}
				}
				if owner != zone {
					continue
				}
				options := platformconfig.DNSPlacementOptions(intent)
				if record.Type == "A" && (options.IPv4Policy == "ipv6_only" || options.IPv6Policy == "ipv6_only") {
					continue
				}
				if record.Type == "AAAA" && (options.IPv4Policy == "ipv4_only" || options.IPv6Policy == "ipv4_only") {
					continue
				}
				if len(record.Candidates) == 0 {
					return fmt.Errorf("dynamic DNS source lacks explicit selection candidates")
				}
				policy := record.AnswerPolicy
				scopedMode := ""
				for _, scope := range record.ScopedCandidates {
					if scopedMode != "" && scopedMode != scope.PolicyKind {
						return fmt.Errorf("DNS scopes disagree on query selection mode")
					}
					scopedMode = scope.PolicyKind
				}
				rules = append(rules, platformconfig.DNSAnswerRule{NodeID: consumer.NodeID, Hostname: record.Name, Type: record.Type, SelectionMode: policy.PolicyKind, ScopedSelectionMode: scopedMode, PreferredEdgeGroups: append([]string(nil), policy.PreferredEdgeGroups...), FallbackEdgeGroups: append([]string(nil), policy.FallbackEdgeGroups...), TTLSeconds: record.TTL, ECSEnabled: policy.ECSEnabled, ExplorationPercent: policy.ExplorationPercent, SwitchCooldownSeconds: policy.SwitchCooldownSec})
				fact := platformconfig.DNSSelectionObservation{NodeID: consumer.NodeID, Hostname: record.Name, Type: record.Type, SourceGeneration: bundle.Generation, SourceDigest: digest, ObservedAt: bundle.GeneratedAt, SelectedEdgeGroupID: policy.SelectedEdgeGroupID, ShadowSelectedEdgeGroupID: policy.ShadowSelectedEdgeGroupID, RankingVersion: policy.RankingVersion, RankingScope: policy.RankingScope, Reason: policy.Reason, ShadowReason: policy.ShadowReason, Weight: policy.Weight, Candidates: dnsSelectionCandidates(record.Candidates)}
				for _, scope := range record.ScopedCandidates {
					fact.ScopedCandidates = append(fact.ScopedCandidates, platformconfig.DNSSelectionScope{ScopeKey: scope.ScopeKey, Country: scope.Country, Region: scope.Region, ASN: scope.ASN, SelectedEdgeGroupID: scope.SelectedEdgeGroupID, CooldownUntil: scope.CooldownUntil, Reason: scope.Reason, Candidates: dnsSelectionCandidates(scope.Candidates)})
				}
				facts = append(facts, fact)
			}
		}
	}
	if err := platformconfig.ValidateDNSAnswerRules(rules); err != nil {
		return err
	}
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].NodeID+"\x00"+rules[i].Hostname+"\x00"+rules[i].Type < rules[j].NodeID+"\x00"+rules[j].Hostname+"\x00"+rules[j].Type
	})
	sort.Slice(facts, func(i, j int) bool {
		return facts[i].NodeID+"\x00"+facts[i].Hostname+"\x00"+facts[i].Type < facts[j].NodeID+"\x00"+facts[j].Hostname+"\x00"+facts[j].Type
	})
	policy := result.Policy
	policy.DNSAnswerRules = rules
	generation, err := platformconfig.PolicySnapshotGeneration(policy)
	if err != nil {
		return err
	}
	policy.Generation = generation
	result.Policy = policy
	result.RuntimeSnapshot.PolicyGeneration = generation
	result.RuntimeSnapshot.DNSSelections = facts
	result.CapturedAt = time.Now().UTC()
	result.RuntimeSnapshot.CapturedAt = &result.CapturedAt
	return nil
}

func dnsSelectionCandidates(in []model.EdgeDNSAnswerCandidate) []platformconfig.DNSSelectionCandidate {
	out := make([]platformconfig.DNSSelectionCandidate, 0, len(in))
	for _, c := range in {
		scores := map[string]float64{}
		for k, v := range c.ScoreBreakdown {
			scores[k] = v
		}
		if len(scores) == 0 {
			scores = nil
		}
		out = append(out, platformconfig.DNSSelectionCandidate{IP: c.IP, EdgeID: c.EdgeID, EdgeGroupID: c.EdgeGroupID, Country: c.Country, Region: c.Region, Priority: c.Priority, Weight: c.Weight, Score: c.Score, TrafficClass: c.TrafficClass, Reason: c.Reason, ScoreBreakdown: scores})
	}
	return out
}
