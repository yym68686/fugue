package platformconfig

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

type DNSPlacementCandidate struct {
	EdgeID            string    `json:"edge_id"`
	EdgeGroupID       string    `json:"edge_group_id"`
	ServingGeneration string    `json:"serving_generation"`
	ObservedAt        time.Time `json:"observed_at"`
	ValidUntil        time.Time `json:"valid_until"`
	Healthy           bool      `json:"healthy"`
	RouteReady        bool      `json:"route_ready"`
	TLSReady          bool      `json:"tls_ready"`
	A                 []string  `json:"a,omitempty"`
	AAAA              []string  `json:"aaaa,omitempty"`
}

type DNSPlacementObservation struct {
	InputDigest string                  `json:"input_digest"`
	CheckedAt   time.Time               `json:"checked_at"`
	Status      string                  `json:"status"`
	TargetTTL   int                     `json:"target_ttl"`
	Candidates  []DNSPlacementCandidate `json:"candidates"`
}

func normalizeDNSPlacementObservations(in []DNSPlacementObservation) []DNSPlacementObservation {
	out := append([]DNSPlacementObservation(nil), in...)
	for i := range out {
		out[i].Candidates = append([]DNSPlacementCandidate(nil), in[i].Candidates...)
		for j := range out[i].Candidates {
			out[i].Candidates[j].A = normalizeDNSValues("A", out[i].Candidates[j].A)
			out[i].Candidates[j].AAAA = normalizeDNSValues("AAAA", out[i].Candidates[j].AAAA)
		}
		sort.Slice(out[i].Candidates, func(a, b int) bool { return out[i].Candidates[a].EdgeID < out[i].Candidates[b].EdgeID })
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InputDigest < out[j].InputDigest })
	return out
}

// DNSPlacementInputDigest binds one record, every path at its hostname and
// the policy. Routes include resolved origins and weighted upstreams. Changes
// to ownership, runtime placement or constraints invalidate old readiness facts.
func DNSPlacementInputDigest(record DNSIntent, routes []CompiledRoute, policy PolicySnapshot) (string, error) {
	normalized := NormalizePlatformIntent(PlatformIntent{DNS: []DNSIntent{record}})
	hostRoutes := []CompiledRoute{}
	for _, route := range routes {
		if normalizedImportHostname(route.Hostname) == normalized.DNS[0].Hostname {
			route.RouteIntent = NormalizePlatformIntent(PlatformIntent{Routes: []RouteIntent{route.RouteIntent}}).Routes[0]
			route.ExcludedEdgeIDs = uniqueSorted(route.ExcludedEdgeIDs)
			route.ExcludedEdgeGroupIDs = uniqueSorted(route.ExcludedEdgeGroupIDs)
			hostRoutes = append(hostRoutes, route)
		}
	}
	sort.Slice(hostRoutes, func(i, j int) bool {
		return model.NormalizeAppRoutePathPrefix(hostRoutes[i].PathPrefix) < model.NormalizeAppRoutePathPrefix(hostRoutes[j].PathPrefix)
	})
	raw, err := json.Marshal(map[string]any{"dns": normalized.DNS[0], "routes": hostRoutes, "policy": NormalizePolicySnapshot(policy)})
	if err != nil {
		return "", err
	}
	var material map[string]any
	if err = json.Unmarshal(raw, &material); err != nil {
		return "", err
	}
	return Digest(material)
}

func validateDNSPlacementObservation(fact DNSPlacementObservation, captured *time.Time) error {
	digest, err := hex.DecodeString(strings.TrimPrefix(fact.InputDigest, "sha256:"))
	if err != nil || len(digest) != 32 || len(fact.InputDigest) != 71 || !strings.HasPrefix(fact.InputDigest, "sha256:") {
		return fmt.Errorf("DNS placement requires a valid input digest")
	}
	if captured == nil || captured.IsZero() || fact.CheckedAt.IsZero() || fact.CheckedAt.After(*captured) {
		return fmt.Errorf("DNS placement requires a fixed captured_at at or after checked_at")
	}
	if fact.Status != "resolved" && fact.Status != "stale" {
		return fmt.Errorf("DNS placement status is invalid")
	}
	if fact.TargetTTL < 0 || fact.TargetTTL > 2147483647 || len(fact.Candidates) > 4096 {
		return fmt.Errorf("DNS placement TTL or candidate count is invalid")
	}
	seenEdges, seenIPs := map[string]bool{}, map[string]bool{}
	for _, c := range fact.Candidates {
		for _, identity := range []string{c.EdgeID, c.EdgeGroupID, c.ServingGeneration} {
			if identity == "" || identity != strings.TrimSpace(identity) {
				return fmt.Errorf("DNS placement candidate identity is invalid")
			}
		}
		if seenEdges[c.EdgeID] {
			return fmt.Errorf("duplicate DNS placement edge")
		}
		seenEdges[c.EdgeID] = true
		if c.ObservedAt.IsZero() || c.ValidUntil.IsZero() || c.ObservedAt.After(fact.CheckedAt) || !c.ValidUntil.After(c.ObservedAt) {
			return fmt.Errorf("DNS placement candidate time is invalid")
		}
		if len(c.A) > 16 || len(c.AAAA) > 16 {
			return fmt.Errorf("DNS placement address count exceeds limit")
		}
		for _, family := range []struct {
			v4     bool
			values []string
		}{{true, c.A}, {false, c.AAAA}} {
			for _, value := range family.values {
				ip, err := netip.ParseAddr(value)
				if err != nil || ip.Is4() != family.v4 || !PublicDNSFlattenIP(ip) {
					return fmt.Errorf("DNS placement requires public addresses of the correct family")
				}
				address := ip.String()
				if seenIPs[address] {
					return fmt.Errorf("DNS placement address has ambiguous edge ownership")
				}
				seenIPs[address] = true
			}
		}
	}
	return nil
}

// ResolveDNSPlacements is a pure transformation. Required route/TLS readiness
// and finite address leases are safety invariants, independent of soft gates.
func ResolveDNSPlacements(intent PlatformIntent, routes []CompiledRoute, snapshot RuntimeSnapshot, policy PolicySnapshot) ([]DNSIntent, error) {
	if len(snapshot.DNSPlacements) > 10000 {
		return nil, fmt.Errorf("too many DNS placement observations")
	}
	byDigest := make(map[string]DNSPlacementObservation, len(snapshot.DNSPlacements))
	for _, fact := range snapshot.DNSPlacements {
		if err := validateDNSPlacementObservation(fact, snapshot.CapturedAt); err != nil {
			return nil, err
		}
		if _, exists := byDigest[fact.InputDigest]; exists {
			return nil, fmt.Errorf("duplicate DNS placement observation")
		}
		byDigest[fact.InputDigest] = fact
	}
	byHost := map[string][]CompiledRoute{}
	for _, route := range routes {
		host := normalizedImportHostname(route.Hostname)
		byHost[host] = append(byHost[host], route)
	}
	out := make([]DNSIntent, 0, len(intent.DNS))
	matched := map[string]bool{}
	for _, record := range intent.DNS {
		if record.Application == nil {
			out = append(out, record)
			continue
		}
		if err := validateDNSApplicationConfiguration(record); err != nil {
			return nil, err
		}
		owners := byHost[record.Hostname]
		if len(owners) == 0 {
			return nil, fmt.Errorf("DNS placement requires hostname routes")
		}
		digest, err := DNSPlacementInputDigest(record, routes, policy)
		if err != nil {
			return nil, err
		}
		fact, exists := byDigest[digest]
		if !exists {
			return nil, fmt.Errorf("DNS application bindings require fixed placement resolution matching intent and policy")
		}
		matched[digest] = true
		ready := record.Status == "" || record.Status == model.EdgeRouteStatusActive
		minimum := policy.MinimumHealthyEdges
		for _, route := range owners {
			if route.AppID != record.AppID || route.TenantID != record.TenantID {
				return nil, fmt.Errorf("DNS placement route ownership differs")
			}
			if !route.Enabled || (route.Status != "" && route.Status != model.EdgeRouteStatusActive) || (route.RoutePolicy != "" && !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy)) {
				ready = false
			}
			minimum = max(minimum, route.MinHealthyEdgeNodes)
		}
		if !ready {
			continue
		}
		p := record.Application
		if fact.Status == "stale" && p.FallbackPolicy != "stale_if_error" {
			return nil, fmt.Errorf("DNS placement stale observation is not permitted by fallback policy")
		}
		ttl := record.TTL
		if p.TTLPolicy != "record" {
			targetRemaining := fact.TargetTTL - int(snapshot.CapturedAt.Sub(fact.CheckedAt).Seconds())
			if fact.Status == "stale" {
				targetRemaining = fact.TargetTTL
			}
			if targetRemaining < 1 {
				return nil, fmt.Errorf("DNS placement TTL policy requires usable target TTL")
			}
			if p.TTLPolicy == "target" {
				ttl = targetRemaining
			} else {
				ttl = min(ttl, targetRemaining)
			}
		}
		allow4 := p.IPv4Policy != "ipv6_only" && p.IPv6Policy != "ipv6_only"
		allow6 := p.IPv4Policy != "ipv4_only" && p.IPv6Policy != "ipv4_only"
		dual := p.IPv4Policy == "dual_stack_required" || p.IPv6Policy == "dual_stack_required"
		values4, values6 := []string{}, []string{}
		expires4, expires6 := map[string]time.Time{}, map[string]time.Time{}
		quorumExpiries, v4Expiries, v6Expiries := []time.Time{}, []time.Time{}, []time.Time{}
		count, count4, count6 := 0, 0, 0
		for _, c := range fact.Candidates {
			expiry := c.ValidUntil
			if float64(policy.MaxStaleSeconds) < expiry.Sub(c.ObservedAt).Seconds() {
				expiry = c.ObservedAt.Add(time.Duration(policy.MaxStaleSeconds) * time.Second)
			}
			if !c.Healthy || !c.RouteReady || !c.TLSReady || expiry.Sub(*snapshot.CapturedAt) < time.Second {
				continue
			}
			eligible := record.EdgeGroupID == "" || c.EdgeGroupID == record.EdgeGroupID || c.EdgeGroupID == record.FallbackEdgeGroupID
			for _, route := range owners {
				if slices.Contains(route.ExcludedEdgeIDs, c.EdgeID) || slices.Contains(route.ExcludedEdgeGroupIDs, c.EdgeGroupID) {
					eligible = false
				}
				if route.EdgeGroupMode == model.PlatformRouteEdgeGroupModePinned && route.EdgeGroupID != c.EdgeGroupID {
					eligible = false
				}
			}
			if !eligible {
				continue
			}
			has4, has6 := allow4 && len(c.A) > 0, allow6 && len(c.AAAA) > 0
			if !has4 && !has6 {
				continue
			}
			count++
			quorumExpiries = append(quorumExpiries, expiry)
			if has4 {
				count4++
				v4Expiries = append(v4Expiries, expiry)
				for _, ip := range c.A {
					values4 = append(values4, ip)
					expires4[ip] = expiry
				}
			}
			if has6 {
				count6++
				v6Expiries = append(v6Expiries, expiry)
				for _, ip := range c.AAAA {
					values6 = append(values6, ip)
					expires6[ip] = expiry
				}
			}
		}
		if count < minimum || (dual && (count4 < minimum || count6 < minimum)) {
			return nil, fmt.Errorf("DNS placement has insufficient route-ready and TLS-ready edges")
		}
		quorumUntil := dnsPlacementQuorumUntil(quorumExpiries, minimum)
		if dual {
			for _, deadlines := range [][]time.Time{v4Expiries, v6Expiries} {
				if until := dnsPlacementQuorumUntil(deadlines, minimum); until.Before(quorumUntil) {
					quorumUntil = until
				}
			}
		}
		for _, family := range []struct {
			kind    string
			values  []string
			expires map[string]time.Time
		}{{"A", values4, expires4}, {"AAAA", values6, expires6}} {
			if len(family.values) == 0 {
				continue
			}
			resolved := record
			resolved.Application = nil
			resolved.Type = family.kind
			resolved.Values = family.values
			resolved.ValueExpirations = family.expires
			for value, expiry := range resolved.ValueExpirations {
				if quorumUntil.Before(expiry) {
					resolved.ValueExpirations[value] = quorumUntil
				}
			}
			resolved.TTL = ttl
			out = append(out, resolved)
		}
	}
	if len(matched) != len(byDigest) {
		return nil, fmt.Errorf("DNS placement observations reference configuration outside intent")
	}
	return NormalizePlatformIntent(PlatformIntent{DNS: out}).DNS, nil
}

func dnsPlacementQuorumUntil(expiries []time.Time, minimum int) time.Time {
	sort.Slice(expiries, func(i, j int) bool { return expiries[i].After(expiries[j]) })
	return expiries[minimum-1]
}
