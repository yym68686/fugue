package api

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Only inventory and ranking observations enter this collector. No legacy DNS
// publication, business route table or workload environment is consulted.
func (s *Server) captureDirectDNSQueries(ctx context.Context, result *platformIntentProjectionResponse, policy platformconfig.DNSQueryPolicy) error {
	if err := platformconfig.ValidateDNSQueryPolicy(&policy); err != nil {
		return err
	}
	now := time.Now().UTC()
	nodes, _, err := s.store.ListActiveEdgeNodes("")
	if err != nil {
		return err
	}
	live := s.edgeLiveServingByNode(ctx, now)
	eligible := make([]model.EdgeNode, 0, len(nodes))
	for _, node := range nodes {
		if edgeNodeRouteServingCapableWithLive(node, now, live) && edgeNodeDNSEligible(node) && edgeNodeDNSCacheValid(node) {
			eligible = append(eligible, node)
		}
	}
	catalog := edgeDNSLatencyProfileCatalog{}
	if policy.RankingMode != "disabled" {
		builder, severe, err := s.loadEdgeDNSLatencyProfileBuilder(ctx, now, true)
		if err != nil {
			return err
		}
		decisions, err := s.store.ListEdgeDNSRoutingDecisions("")
		if err != nil {
			return err
		}
		catalog, _ = builder.finishWithCooldown(decisions, now, time.Duration(policy.SwitchCooldownSeconds)*time.Second, true)
		edgeDNSApplySevereDegradeGroupsToCatalog(&catalog, severe)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return projectDirectDNSQueries(result, policy, eligible, catalog, now)
}

func projectDirectDNSQueries(result *platformIntentProjectionResponse, strategy platformconfig.DNSQueryPolicy, nodes []model.EdgeNode, catalog edgeDNSLatencyProfileCatalog, observed time.Time) error {
	if err := platformconfig.ValidateDNSQueryPolicy(&strategy); err != nil {
		return err
	}
	if observed.IsZero() {
		return fmt.Errorf("DNS selection observation time required")
	}
	// Restrict inventory to the already frozen authoritative endpoint topology.
	endpoints := map[string]platformconfig.DNSEdgeEndpoint{}
	for _, e := range result.RuntimeSnapshot.DNSEdgeEndpoints {
		endpoints[e.EdgeID] = e
	}
	candidates := map[string]model.EdgeDNSAnswerCandidate{}
	groups := map[string]bool{}
	for _, node := range nodes {
		e, ok := endpoints[node.ID]
		if !ok {
			continue
		}
		if e.EdgeGroupID != node.EdgeGroupID {
			return fmt.Errorf("DNS selection inventory owner differs from frozen topology")
		}
		for _, pair := range []struct {
			address  string
			declared []string
		}{{node.PublicIPv4, e.A}, {node.PublicIPv6, e.AAAA}} {
			if pair.address == "" {
				continue
			}
			if !stringSliceContains(pair.declared, pair.address) {
				return fmt.Errorf("DNS selection endpoint differs from frozen topology")
			}
			if old, ok := candidates[pair.address]; ok && old.EdgeID != node.ID {
				return fmt.Errorf("DNS selection address has ambiguous owner")
			}
			candidates[pair.address] = edgeDNSAnswerCandidateForNode(pair.address, node, "")
			groups[node.EdgeGroupID] = true
		}
	}
	rules := []platformconfig.DNSAnswerRule{}
	facts := []platformconfig.DNSSelectionObservation{}
	for _, record := range result.Intent.DNS {
		options := platformconfig.DNSPlacementOptions(record)
		if options == nil {
			continue
		}
		owners, _, err := placementRecordRoutes(*result, record)
		if err != nil {
			return err
		}
		enabled := record.Status == "" || record.Status == model.EdgeRouteStatusActive
		for _, r := range owners {
			enabled = enabled && platformconfig.DNSRouteStateAllowed(record, r, result.Policy)
		}
		if !enabled {
			continue
		}
		for _, consumer := range result.Intent.DNSConsumers {
			zone := ""
			for _, z := range consumer.Zones {
				if edgeDNSTargetWithinZone(record.Hostname, z) && len(z) > len(zone) {
					zone = z
				}
			}
			if zone == "" {
				continue
			}
			for _, family := range []string{"A", "AAAA"} {
				if family == "A" && (options.IPv4Policy == "ipv6_only" || options.IPv6Policy == "ipv6_only") || family == "AAAA" && (options.IPv4Policy == "ipv4_only" || options.IPv6Policy == "ipv4_only") {
					continue
				}
				ips := []string{}
				for ip, c := range candidates {
					if (family == "AAAA") != strings.Contains(ip, ":") || !platformconfig.DNSPlacementAllowsEdge(record, owners, c.EdgeID, c.EdgeGroupID) {
						continue
					}
					ips = append(ips, ip)
				}
				sort.Strings(ips)
				if len(ips) == 0 {
					continue
				} // A missing family produces no compiled RRset.
				records := []model.EdgeDNSRecord{}
				seenHosts := map[string]bool{}
				for _, owner := range owners {
					if seenHosts[owner.Hostname] {
						continue
					}
					seenHosts[owner.Hostname] = true
					// Shared target routing chooses among member profiles using the same
					// stable traffic-class ordering as the existing selector.
					primary, fallback := "", ""
					switch {
					case owner.DNSPlacementEdgeGroupID != "":
						primary = owner.DNSPlacementEdgeGroupID
					case owner.EdgeGroupMode == model.PlatformRouteEdgeGroupModePinned:
						primary = owner.EdgeGroupID
					case owner.AppID == "":
						primary = firstNonEmpty(owner.EdgeGroupID, consumer.EdgeGroupID)
					default:
						selected := selectEdgeGroupForRoute(owner.RuntimeEdgeGroupID, groups)
						primary = firstNonEmpty(selected.EdgeGroupID, owner.RuntimeEdgeGroupID)
						if selected.FallbackReason != "" && owner.RuntimeEdgeGroupID != "" && primary != owner.RuntimeEdgeGroupID {
							fallback = primary
						}
					}
					profile := catalog.globalProfile(owner.Hostname)
					active := strategy.RankingMode == "active"
					if strategy.RankingMode == "disabled" {
						profile = nil
					}
					ttl := max(strategy.MinimumTTLSeconds, min(strategy.MaximumTTLSeconds, record.TTL))
					policy := edgeDNSAnswerPolicy(edgeDNSBundleOptions{EdgeGroupID: consumer.EdgeGroupID}, primary, fallback, ips, candidates, profile, ttl, active)
					policy.TTLSeconds, policy.ECSEnabled, policy.ExplorationPercent, policy.SwitchCooldownSec = ttl, strategy.ECSEnabled, strategy.ExplorationPercent, strategy.SwitchCooldownSeconds
					scoped := []model.EdgeDNSScopedAnswerCandidates(nil)
					if active {
						scoped = catalog.scopedProfiles(owner.Hostname, ips, candidates, nil, primary, fallback, true)
					}
					value := model.EdgeDNSRecord{Name: record.Hostname, Type: family, Values: ips, TTL: ttl, RecordKind: record.RecordKind, Status: record.Status, StatusReason: record.StatusReason, AppID: record.AppID, TenantID: record.TenantID, EdgeGroupID: primary, FallbackEdgeGroupID: fallback, AnswerPolicy: policy, Candidates: edgeDNSCandidatesForAnswerIPs(ips, candidates, nil, primary, fallback, profile, active), ScopedCandidates: scoped}
					records = append(records, value)
				}
				if len(records) == 0 {
					return fmt.Errorf("DNS query has no route owner")
				}
				selected := records[0]
				if len(records) > 1 {
					selected = mergeSharedEdgeDNSTargetRecords(records)
				}
				policy := selected.AnswerPolicy
				scopedMode := ""
				for _, scope := range selected.ScopedCandidates {
					if scopedMode != "" && scopedMode != scope.PolicyKind {
						return fmt.Errorf("DNS query scopes disagree on strategy")
					}
					scopedMode = scope.PolicyKind
				}
				rules = append(rules, platformconfig.DNSAnswerRule{NodeID: consumer.NodeID, Hostname: record.Hostname, Type: family, SelectionMode: policy.PolicyKind, ScopedSelectionMode: scopedMode, PreferredEdgeGroups: policy.PreferredEdgeGroups, FallbackEdgeGroups: policy.FallbackEdgeGroups, TTLSeconds: selected.TTL, ECSEnabled: policy.ECSEnabled, ExplorationPercent: policy.ExplorationPercent, SwitchCooldownSeconds: policy.SwitchCooldownSec})
				f := platformconfig.DNSSelectionObservation{NodeID: consumer.NodeID, Hostname: record.Hostname, Type: family, ObservedAt: observed, SelectedEdgeGroupID: policy.SelectedEdgeGroupID, ShadowSelectedEdgeGroupID: policy.ShadowSelectedEdgeGroupID, RankingVersion: policy.RankingVersion, RankingScope: policy.RankingScope, Reason: policy.Reason, ShadowReason: policy.ShadowReason, Weight: policy.Weight, Candidates: dnsSelectionCandidates(selected.Candidates)}
				for _, scope := range selected.ScopedCandidates {
					f.ScopedCandidates = append(f.ScopedCandidates, platformconfig.DNSSelectionScope{ScopeKey: scope.ScopeKey, Country: scope.Country, Region: scope.Region, ASN: scope.ASN, SelectedEdgeGroupID: scope.SelectedEdgeGroupID, CooldownUntil: scope.CooldownUntil, Reason: scope.Reason, Candidates: dnsSelectionCandidates(scope.Candidates)})
				}
				digest, err := platformconfig.Digest(f)
				if err != nil {
					return err
				}
				f.SourceDigest = digest
				f.SourceGeneration = "dns-observation_" + strings.TrimPrefix(digest, "sha256:")
				facts = append(facts, f)
			}
		}
	}
	if err := platformconfig.ValidateDNSAnswerRules(rules); err != nil {
		return err
	}
	sort.Slice(facts, func(i, j int) bool {
		a, b := facts[i], facts[j]
		return a.NodeID+"\x00"+a.Hostname+"\x00"+a.Type < b.NodeID+"\x00"+b.Hostname+"\x00"+b.Type
	})
	policy := result.Policy
	policy.DNSQueryPolicy = &strategy
	policy.DNSAnswerRules = rules
	policy = platformconfig.NormalizePolicySnapshot(policy)
	gen, err := platformconfig.PolicySnapshotGeneration(policy)
	if err != nil {
		return err
	}
	policy.Generation = gen
	result.Policy = policy
	result.RuntimeSnapshot.PolicyGeneration = gen
	result.RuntimeSnapshot.DNSSelections = facts
	result.CapturedAt = observed
	result.RuntimeSnapshot.CapturedAt = &result.CapturedAt
	return nil
}
