package api

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
)

func (s *Server) capturePlatformPlacements(rctx context.Context, result *platformIntentProjectionResponse) {
	found := false
	for _, record := range result.Intent.DNS {
		found = found || platformconfig.DNSPlacementOptions(record) != nil
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
	if len(nodes) > 4096 || len(result.Intent.DNS) > 10000 {
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
	// Workers read one detached snapshot. Only the caller merges their facts
	// and issues, preserving intent order irrespective of network completion.
	snapshot := *result
	snapshot.CapturedAt = captured
	snapshot.RuntimeSnapshot.CapturedAt = &snapshot.CapturedAt
	records := []platformconfig.DNSIntent{}
	for _, record := range result.Intent.DNS {
		if platformconfig.DNSPlacementOptions(record) != nil {
			records = append(records, record)
		}
	}
	type observation struct {
		fact       platformconfig.DNSPlacementObservation
		inputError bool
		limited    bool
	}
	observations := make([]observation, len(records))
	jobs := make(chan int)
	var workers sync.WaitGroup
	var budget atomic.Int64
	budget.Store(4096)
	for range min(8, len(records)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				fact, limited, err := captureDNSPlacementRecord(ctx, snapshot, records[index], nodes, probe, &budget)
				observations[index] = observation{fact: fact, limited: limited, inputError: err != nil}
			}
		}()
	}
	for index := range records {
		jobs <- index
	}
	close(jobs)
	workers.Wait()

	result.CapturedAt = time.Now().UTC()
	result.RuntimeSnapshot.CapturedAt = &result.CapturedAt
	result.RuntimeSnapshot.DNSPlacements = nil
	for index, record := range records {
		observation := observations[index]
		if observation.inputError {
			issue("dns_placement_route_inputs_invalid", record.Hostname)
			continue
		}
		fact := observation.fact
		result.RuntimeSnapshot.DNSPlacements = append(result.RuntimeSnapshot.DNSPlacements, fact)
		result.Issues = slices.DeleteFunc(result.Issues, func(existing platformProjectionIssue) bool {
			return existing.Code == "dns_app_placement_not_projected" && existing.Hostname == record.Hostname
		})
		// Compilation and evidence freshness are rechecked at the final fixed
		// time. A slow peer cannot silently extend an earlier hostname's lease.
		compiled, _, err := placementRecordRoutes(*result, record)
		if err == nil {
			_, err = platformconfig.ResolveDNSPlacements(platformconfig.PlatformIntent{DNS: []platformconfig.DNSIntent{record}}, compiled, platformconfig.RuntimeSnapshot{CapturedAt: &result.CapturedAt, DNSPlacements: []platformconfig.DNSPlacementObservation{fact}}, result.Policy)
		}
		if err != nil {
			issue("dns_placement_evidence_requires_repair", record.Hostname)
		}
		if observation.limited {
			issue("dns_placement_capture_limit", record.Hostname)
		}
	}
	sort.Slice(result.RuntimeSnapshot.DNSPlacements, func(i, j int) bool {
		return result.RuntimeSnapshot.DNSPlacements[i].InputDigest < result.RuntimeSnapshot.DNSPlacements[j].InputDigest
	})
}

func captureDNSPlacementRecord(ctx context.Context, snapshot platformIntentProjectionResponse, record platformconfig.DNSIntent, nodes []model.EdgeNode, probe placementRouteProbe, budget *atomic.Int64) (platformconfig.DNSPlacementObservation, bool, error) {
	compiled, projected, err := placementRecordRoutes(snapshot, record)
	if err != nil {
		return platformconfig.DNSPlacementObservation{}, false, err
	}
	digest, err := platformconfig.DNSPlacementInputDigest(record, compiled, snapshot.Policy)
	if err != nil {
		return platformconfig.DNSPlacementObservation{}, false, err
	}
	fact := platformconfig.DNSPlacementObservation{InputDigest: digest, Status: "resolved", TargetTTL: record.TTL, Candidates: []platformconfig.DNSPlacementCandidate{}}
	limited := false
	for _, node := range nodes {
		now := time.Now().UTC()
		// LastSeenAt is not a health observation and cannot renew readiness.
		if !placementInventoryEligible(node, snapshot.Policy, now) || !placementGroupEligible(record, compiled, node) {
			continue
		}
		expires := node.LastHeartbeatAt.Add(platformNodeHeartbeatStaleAfter)
		if limit := node.LastHeartbeatAt.Add(time.Duration(snapshot.Policy.MaxStaleSeconds) * time.Second); limit.Before(expires) {
			expires = limit
		}
		if expires.Sub(now) < time.Second {
			continue
		}
		candidate := platformconfig.DNSPlacementCandidate{EdgeID: node.ID, EdgeGroupID: node.EdgeGroupID, ObservedAt: *node.LastHeartbeatAt, ValidUntil: expires, ServingGeneration: node.RouteBundleVersion, Healthy: true, RouteReady: true, TLSReady: true}
		for _, family := range []struct {
			value string
			v4    bool
		}{{node.PublicIPv4, true}, {node.PublicIPv6, false}} {
			ip, err := netip.ParseAddr(family.value)
			if err != nil || ip.Is4() != family.v4 || !platformconfig.PublicDNSFlattenIP(ip) {
				continue
			}
			valid := true
			addressExpiry := candidate.ValidUntil
			for _, route := range projected {
				if ctx.Err() != nil || budget.Add(-1) < 0 {
					limited, valid = true, false
					break
				}
				expected, err := routeproof.Digest(routebinding.FromIntent(route, node.EdgeGroupID))
				if err != nil {
					valid = false
					break
				}
				proof, err := probe(ctx, route.Hostname, model.NormalizeAppRoutePathPrefix(route.PathPrefix), ip.String())
				if err != nil || proof.Digest != expected || proof.Version != node.RouteBundleVersion || proof.EdgeID != node.ID || proof.GroupID != node.EdgeGroupID || !proof.ValidUntil.After(time.Now()) {
					valid = false
					if ctx.Err() != nil {
						limited = true
					}
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
	return fact, limited, nil
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
	compiled, err = platformconfig.ApplyRoutePolicyConstraintsForPlacement(compiled, filteredPolicy)
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

func placementRecordRoutes(result platformIntentProjectionResponse, record platformconfig.DNSIntent) ([]platformconfig.CompiledRoute, []model.EdgeRouteIntent, error) {
	compiled := []platformconfig.CompiledRoute{}
	projected := []model.EdgeRouteIntent{}
	for _, host := range platformconfig.DNSPlacementHostnames(record) {
		c, p, err := placementHostnameRoutes(result, host)
		if err != nil {
			return nil, nil, err
		}
		compiled = append(compiled, c...)
		projected = append(projected, p...)
	}
	return compiled, projected, nil
}
