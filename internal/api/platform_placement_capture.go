package api

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
)

func (s *Server) capturePlatformPlacements(rctx context.Context, result *platformIntentProjectionResponse) {
	found := false
	for _, record := range result.Intent.DNS {
		found = found || record.Application != nil
	}
	if !found {
		return
	}
	nodes, _, err := s.store.ListEdgeNodes("")
	if err != nil {
		result.Issues = append(result.Issues, platformProjectionIssue{Code: "dns_placement_inventory_unavailable"})
		return
	}
	captureDNSPlacementFacts(rctx, result, nodes, probePlacementRoute)
}

// The inventory is discovery and a finite heartbeat lease. Only independent
// TLS/Host/path proofs matching compiled behavior authorize positive candidates.
func captureDNSPlacementFacts(ctx context.Context, result *platformIntentProjectionResponse, nodes []model.EdgeNode, probe placementRouteProbe) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	issue := func(code, host string) {
		result.Issues = append(result.Issues, platformProjectionIssue{Code: code, Hostname: host})
	}
	if len(nodes) > 4096 {
		issue("dns_placement_inventory_limit", "")
		return
	}
	nodes = append([]model.EdgeNode(nil), nodes...)
	captured := time.Now().UTC()
	nodes = slices.DeleteFunc(nodes, func(node model.EdgeNode) bool { return !placementInventoryEligible(node, result.Policy, captured) })
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	seenIDs, seenAddresses := map[string]bool{}, map[string]bool{}
	for _, node := range nodes {
		if node.ID == "" || seenIDs[node.ID] {
			issue("dns_placement_inventory_ambiguous", "")
			return
		}
		seenIDs[node.ID] = true
		for _, value := range []string{node.PublicIPv4, node.PublicIPv6} {
			if value == "" {
				continue
			}
			ip, err := netip.ParseAddr(value)
			if err != nil {
				continue
			}
			if seenAddresses[ip.String()] {
				issue("dns_placement_inventory_ambiguous", "")
				return
			}
			seenAddresses[ip.String()] = true
		}
	}
	result.CapturedAt = time.Now().UTC()
	result.RuntimeSnapshot.CapturedAt = &result.CapturedAt
	probes := 0
	for _, record := range result.Intent.DNS {
		if record.Application == nil {
			continue
		}
		compiled, projected, err := placementHostnameRoutes(*result, record.Hostname)
		if err != nil {
			issue("dns_placement_route_inputs_invalid", record.Hostname)
			continue
		}
		digest, err := platformconfig.DNSPlacementInputDigest(record, compiled, result.Policy)
		if err != nil {
			issue("dns_placement_route_inputs_invalid", record.Hostname)
			continue
		}
		fact := platformconfig.DNSPlacementObservation{InputDigest: digest, Status: "resolved", TargetTTL: record.TTL, Candidates: []platformconfig.DNSPlacementCandidate{}}
		for _, node := range nodes {
			now := time.Now().UTC()
			// LastSeenAt is not a health observation and cannot renew readiness.
			if !placementInventoryEligible(node, result.Policy, now) {
				continue
			}
			expires := node.LastHeartbeatAt.Add(platformNodeHeartbeatStaleAfter)
			if limit := node.LastHeartbeatAt.Add(time.Duration(result.Policy.MaxStaleSeconds) * time.Second); limit.Before(expires) {
				expires = limit
			}
			if expires.Sub(now) < time.Second {
				continue
			}
			candidate := platformconfig.DNSPlacementCandidate{EdgeID: node.ID, EdgeGroupID: node.EdgeGroupID, ObservedAt: *node.LastHeartbeatAt, ValidUntil: expires, ServingGeneration: node.RouteBundleVersion, Healthy: true, RouteReady: true, TLSReady: true}
			if !placementGroupEligible(record, compiled, node) {
				continue
			}
			for _, family := range []struct {
				value string
				v4    bool
			}{{node.PublicIPv4, true}, {node.PublicIPv6, false}} {
				if family.value == "" {
					continue
				}
				ip, err := netip.ParseAddr(family.value)
				if err != nil || ip.Is4() != family.v4 || !platformconfig.PublicDNSFlattenIP(ip) {
					continue
				}
				valid := true
				addressExpiry := candidate.ValidUntil
				for _, route := range projected {
					if probes >= 256 || ctx.Err() != nil {
						valid = false
						break
					}
					probes++
					expected, err := routeproof.Digest(routebinding.FromIntent(route, node.EdgeGroupID))
					if err != nil {
						valid = false
						break
					}
					proof, err := probe(ctx, record.Hostname, model.NormalizeAppRoutePathPrefix(route.PathPrefix), ip.String())
					if err != nil || proof.Digest != expected || proof.Version != node.RouteBundleVersion || proof.EdgeID != node.ID || proof.GroupID != node.EdgeGroupID || !proof.ValidUntil.After(time.Now()) {
						valid = false
						break
					}
					if proof.ValidUntil.Before(addressExpiry) {
						addressExpiry = proof.ValidUntil
					}
				}
				if !valid {
					continue
				}
				if addressExpiry.Before(candidate.ValidUntil) {
					candidate.ValidUntil = addressExpiry
				}
				if family.v4 {
					candidate.A = append(candidate.A, ip.String())
				} else {
					candidate.AAAA = append(candidate.AAAA, ip.String())
				}
			}
			if len(candidate.A)+len(candidate.AAAA) > 0 {
				fact.Candidates = append(fact.Candidates, candidate)
			}
		}
		fact.CheckedAt = time.Now().UTC()
		result.CapturedAt = fact.CheckedAt
		result.RuntimeSnapshot.DNSPlacements = append(result.RuntimeSnapshot.DNSPlacements, fact)
		filtered := result.Issues[:0]
		for _, existing := range result.Issues {
			if existing.Code != "dns_app_placement_not_projected" || existing.Hostname != record.Hostname {
				filtered = append(filtered, existing)
			}
		}
		result.Issues = filtered
		if _, err := platformconfig.ResolveDNSPlacements(platformconfig.PlatformIntent{DNS: []platformconfig.DNSIntent{record}}, compiled, platformconfig.RuntimeSnapshot{CapturedAt: &result.CapturedAt, DNSPlacements: []platformconfig.DNSPlacementObservation{fact}}, result.Policy); err != nil {
			issue("dns_placement_evidence_requires_repair", record.Hostname)
		}
		if probes >= 256 || ctx.Err() != nil {
			issue("dns_placement_capture_limit", record.Hostname)
		}
	}
	sort.Slice(result.RuntimeSnapshot.DNSPlacements, func(i, j int) bool {
		return result.RuntimeSnapshot.DNSPlacements[i].InputDigest < result.RuntimeSnapshot.DNSPlacements[j].InputDigest
	})
	// Later network probes may consume an earlier hostname's remaining lease.
	// Re-evaluate all facts at the final fixed snapshot time without recapture.
	for _, record := range result.Intent.DNS {
		if record.Application == nil {
			continue
		}
		compiled, _, err := placementHostnameRoutes(*result, record.Hostname)
		if err == nil {
			digest, digestErr := platformconfig.DNSPlacementInputDigest(record, compiled, result.Policy)
			if digestErr != nil {
				err = digestErr
			} else {
				facts := []platformconfig.DNSPlacementObservation{}
				for _, fact := range result.RuntimeSnapshot.DNSPlacements {
					if fact.InputDigest == digest {
						facts = append(facts, fact)
					}
				}
				_, err = platformconfig.ResolveDNSPlacements(platformconfig.PlatformIntent{DNS: []platformconfig.DNSIntent{record}}, compiled, platformconfig.RuntimeSnapshot{CapturedAt: &result.CapturedAt, DNSPlacements: facts}, result.Policy)
			}
		}
		if err != nil {
			existing := slices.ContainsFunc(result.Issues, func(issue platformProjectionIssue) bool {
				return issue.Hostname == record.Hostname && (issue.Code == "dns_placement_route_inputs_invalid" || issue.Code == "dns_placement_evidence_requires_repair")
			})
			if !existing {
				issue("dns_placement_evidence_requires_repair", record.Hostname)
			}
		}
	}
}

func placementInventoryEligible(node model.EdgeNode, policy platformconfig.PolicySnapshot, now time.Time) bool {
	if node.LastHeartbeatAt == nil || node.LastHeartbeatAt.IsZero() || node.LastHeartbeatAt.After(now) || !node.Healthy || node.Draining || node.Status != model.EdgeHealthHealthy || node.RouteBundleVersion == "" || node.EdgeGroupID == "" {
		return false
	}
	age := now.Sub(*node.LastHeartbeatAt)
	return age < platformNodeHeartbeatStaleAfter && age.Seconds() < float64(policy.MaxStaleSeconds)
}

func placementGroupEligible(record platformconfig.DNSIntent, routes []platformconfig.CompiledRoute, node model.EdgeNode) bool {
	if record.EdgeGroupID != "" && record.EdgeGroupID != node.EdgeGroupID && record.FallbackEdgeGroupID != node.EdgeGroupID {
		return false
	}
	for _, r := range routes {
		if !r.Enabled || (r.Status != "" && r.Status != model.EdgeRouteStatusActive) || (r.RoutePolicy != "" && !model.EdgeRoutePolicyAllowsTraffic(r.RoutePolicy)) || slices.Contains(r.ExcludedEdgeIDs, node.ID) || slices.Contains(r.ExcludedEdgeGroupIDs, node.EdgeGroupID) || (r.EdgeGroupMode == model.PlatformRouteEdgeGroupModePinned && r.EdgeGroupID != node.EdgeGroupID) {
			return false
		}
	}
	return true
}

// Compile only the hostname's dependency closure for diagnosis. The original
// full policy is retained in the placement input digest; unrelated invalid
// releases still prevent full-platform compilation and migration readiness.
func placementHostnameRoutes(result platformIntentProjectionResponse, host string) ([]platformconfig.CompiledRoute, []model.EdgeRouteIntent, error) {
	intent := platformconfig.NormalizePlatformIntent(result.Intent)
	policy := platformconfig.NormalizePolicySnapshot(result.Policy)
	routes := []platformconfig.RouteIntent{}
	originRefs, apps, releaseIDs := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, r := range intent.Routes {
		if r.Hostname == host {
			routes = append(routes, r)
			originRefs[r.OriginRef] = true
			apps[r.AppID] = true
		}
	}
	if len(routes) == 0 {
		return nil, nil, fmt.Errorf("hostname has no routes")
	}
	filteredPolicy := policy
	filteredPolicy.RouteConstraints = nil
	filteredPolicy.TrafficConstraints = nil
	for _, r := range policy.RouteConstraints {
		if r.Hostname == host {
			filteredPolicy.RouteConstraints = append(filteredPolicy.RouteConstraints, r)
		}
	}
	for _, r := range policy.TrafficConstraints {
		if apps[r.AppID] {
			filteredPolicy.TrafficConstraints = append(filteredPolicy.TrafficConstraints, r)
			releaseIDs[r.StableReleaseID] = true
			releaseIDs[r.CandidateReleaseID] = true
		}
	}
	snapshot := result.RuntimeSnapshot
	snapshot.Origins = nil
	snapshot.Releases = nil
	for _, r := range result.RuntimeSnapshot.Origins {
		if originRefs[r.Ref] {
			snapshot.Origins = append(snapshot.Origins, r)
		}
	}
	for _, r := range result.RuntimeSnapshot.Releases {
		if releaseIDs[r.ID] {
			snapshot.Releases = append(snapshot.Releases, r)
		}
	}
	compiled, err := platformconfig.ResolveRouteOrigins(routes, snapshot, filteredPolicy)
	if err != nil {
		return nil, nil, err
	}
	compiled, err = platformconfig.ApplyRoutePolicyConstraints(compiled, filteredPolicy)
	if err != nil {
		return nil, nil, err
	}
	compiled, err = platformconfig.ApplyTrafficPolicyConstraints(compiled, filteredPolicy, snapshot)
	if err != nil {
		return nil, nil, err
	}
	projection, err := projectPlatformRouteArtifact(model.PlatformArtifact{Generation: result.SourceGeneration, Content: map[string]any{"routes": compiled, "policy": policy, "cache_policies": intent.CachePolicies}})
	return compiled, projection.Routes, err
}
