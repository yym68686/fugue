package platformconfig

import (
	"encoding/hex"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
)

type DNSReadinessPolicy struct {
	ProbeIntervalSeconds int `json:"probe_interval_seconds"`
	ProbeTimeoutSeconds  int `json:"probe_timeout_seconds"`
	FactFreshnessSeconds int `json:"fact_freshness_seconds"`
	MaxConcurrency       int `json:"max_concurrency"`
	MaxProbes            int `json:"max_probes"`
}

type DNSEdgeEndpoint struct {
	EdgeID      string    `json:"edge_id"`
	EdgeGroupID string    `json:"edge_group_id"`
	ObservedAt  time.Time `json:"observed_at"`
	A           []string  `json:"a,omitempty"`
	AAAA        []string  `json:"aaaa,omitempty"`
}

type DNSReadinessProbe struct {
	ID          string `json:"id"`
	EdgeID      string `json:"edge_id"`
	EdgeGroupID string `json:"edge_group_id"`
	Address     string `json:"address"`
	Hostname    string `json:"hostname"`
	Path        string `json:"path"`
	RouteDigest string `json:"route_digest"`
	State       string `json:"state,omitempty"`
}

type DNSReadinessTarget struct {
	EdgeID      string   `json:"edge_id"`
	EdgeGroupID string   `json:"edge_group_id"`
	Address     string   `json:"address"`
	Family      string   `json:"family"`
	ProbeIDs    []string `json:"probe_ids"`
}

type DNSReadinessRecord struct {
	Hostname            string               `json:"hostname"`
	MinimumHealthyEdges int                  `json:"minimum_healthy_edges"`
	RequireDualStack    bool                 `json:"require_dual_stack"`
	Targets             []DNSReadinessTarget `json:"targets"`
}

type DNSReadinessPlan struct {
	Probes  []DNSReadinessProbe  `json:"probes"`
	Records []DNSReadinessRecord `json:"records"`
}

func ValidateDNSReadinessPolicy(p *DNSReadinessPolicy) error {
	if p == nil {
		return nil
	}
	if p.ProbeIntervalSeconds < 10 || p.ProbeIntervalSeconds > 300 || p.ProbeTimeoutSeconds < 1 || p.ProbeTimeoutSeconds > 10 || p.FactFreshnessSeconds < 20 || p.FactFreshnessSeconds > 600 || p.FactFreshnessSeconds < p.ProbeIntervalSeconds+p.ProbeTimeoutSeconds || p.MaxConcurrency < 1 || p.MaxConcurrency > 16 || p.MaxProbes < 1 || p.MaxProbes > 4096 {
		return fmt.Errorf("DNS readiness policy is outside bounded limits")
	}
	return nil
}

func normalizeDNSEdgeEndpoints(in []DNSEdgeEndpoint) []DNSEdgeEndpoint {
	out := append([]DNSEdgeEndpoint(nil), in...)
	for i := range out {
		out[i].A = normalizeDNSValues("A", in[i].A)
		out[i].AAAA = normalizeDNSValues("AAAA", in[i].AAAA)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EdgeID < out[j].EdgeID })
	return out
}

func validateDNSEdgeEndpoints(endpoints []DNSEdgeEndpoint, captured *time.Time) error {
	if len(endpoints) > 4096 {
		return fmt.Errorf("too many DNS edge endpoints")
	}
	nodes, owners := map[string]bool{}, map[string]string{}
	for _, e := range endpoints {
		if !validDNSConsumerIdentity(e.EdgeID) || !platformRouteArtifactGroupID.MatchString(e.EdgeGroupID) || nodes[e.EdgeID] || captured == nil || captured.IsZero() || e.ObservedAt.IsZero() || e.ObservedAt.After(*captured) || len(e.A)+len(e.AAAA) == 0 || len(e.A) > 16 || len(e.AAAA) > 16 {
			return fmt.Errorf("invalid or ambiguous DNS edge endpoint")
		}
		nodes[e.EdgeID] = true
		for _, family := range []struct {
			v4     bool
			values []string
		}{{true, e.A}, {false, e.AAAA}} {
			for _, address := range family.values {
				ip, err := netip.ParseAddr(address)
				if err != nil || ip.Is4() != family.v4 || ip.String() != address || !PublicDNSFlattenIP(ip) {
					return fmt.Errorf("DNS readiness requires canonical public endpoints")
				}
				if owner, ok := owners[address]; ok && owner != e.EdgeID {
					return fmt.Errorf("DNS readiness address has multiple edge owners")
				}
				owners[address] = e.EdgeID
			}
		}
	}
	return nil
}

// Requirements authorize observation of exact routes, not an answer. They are
// stable when heartbeat/probe leases lapse. Only separately collected runtime
// facts can establish current readiness, and no fact changes content expiry.
func CompileDNSReadiness(intent PlatformIntent, routes []CompiledRoute, snapshot RuntimeSnapshot, policy PolicySnapshot) (*DNSReadinessPlan, error) {
	if policy.DNSReadiness == nil {
		if len(snapshot.DNSEdgeEndpoints) > 0 {
			return nil, fmt.Errorf("DNS edge observations require an explicit readiness policy")
		}
		return nil, nil
	}
	if err := ValidateDNSReadinessPolicy(policy.DNSReadiness); err != nil {
		return nil, err
	}
	if err := validateDNSEdgeEndpoints(snapshot.DNSEdgeEndpoints, snapshot.CapturedAt); err != nil {
		return nil, err
	}
	projection, err := ProjectRouteArtifact(model.PlatformArtifact{Generation: intent.Generation, Content: map[string]any{"routes": routes, "policy": policy, "cache_policies": intent.CachePolicies}})
	if err != nil {
		return nil, err
	}
	byHost := map[string][]CompiledRoute{}
	projected := map[string][]model.EdgeRouteIntent{}
	for _, r := range routes {
		byHost[r.Hostname] = append(byHost[r.Hostname], r)
	}
	for _, r := range projection.Routes {
		projected[r.Hostname] = append(projected[r.Hostname], r)
	}
	plan := &DNSReadinessPlan{Probes: []DNSReadinessProbe{}, Records: []DNSReadinessRecord{}}
	probes := map[string]DNSReadinessProbe{}
	for _, record := range intent.DNS {
		options := DNSPlacementOptions(record)
		if options == nil {
			continue
		}
		owners := []CompiledRoute{}
		dependencies := []model.EdgeRouteIntent{}
		for _, hostname := range DNSPlacementHostnames(record) {
			owners = append(owners, byHost[hostname]...)
			dependencies = append(dependencies, projected[hostname]...)
		}
		if len(owners) == 0 || len(dependencies) == 0 {
			return nil, fmt.Errorf("DNS readiness requires route dependencies")
		}
		if err := ValidateDNSRouteOwners(record, owners); err != nil {
			return nil, err
		}
		enabled := record.Status == "" || record.Status == model.EdgeRouteStatusActive
		minimum := policy.MinimumHealthyEdges
		for _, owner := range owners {
			enabled = enabled && DNSRouteStateAllowed(record, owner, policy)
			minimum = max(minimum, owner.MinHealthyEdgeNodes)
		}
		if !enabled {
			continue
		}
		r := DNSReadinessRecord{Hostname: record.Hostname, MinimumHealthyEdges: minimum, RequireDualStack: options.IPv4Policy == "dual_stack_required" || options.IPv6Policy == "dual_stack_required", Targets: []DNSReadinessTarget{}}
		for _, endpoint := range snapshot.DNSEdgeEndpoints {
			if !DNSPlacementAllowsEdge(record, owners, endpoint.EdgeID, endpoint.EdgeGroupID) {
				continue
			}
			for _, family := range []struct {
				kind    string
				ips     []string
				allowed bool
			}{
				{"A", endpoint.A, options.IPv4Policy != "ipv6_only" && options.IPv6Policy != "ipv6_only"},
				{"AAAA", endpoint.AAAA, options.IPv4Policy != "ipv4_only" && options.IPv6Policy != "ipv4_only"},
			} {
				if !family.allowed {
					continue
				}
				for _, ip := range family.ips {
					target := DNSReadinessTarget{EdgeID: endpoint.EdgeID, EdgeGroupID: endpoint.EdgeGroupID, Address: ip, Family: family.kind, ProbeIDs: []string{}}
					for _, route := range dependencies {
						binding := routebinding.FromIntent(route, endpoint.EdgeGroupID)
						digest, err := routeproof.Digest(binding)
						if err != nil {
							return nil, err
						}
						probe := DNSReadinessProbe{EdgeID: endpoint.EdgeID, EdgeGroupID: endpoint.EdgeGroupID, Address: ip, Hostname: route.Hostname, Path: model.NormalizeAppRoutePathPrefix(route.PathPrefix), RouteDigest: digest}
						if binding.Status == model.EdgeRouteStatusDisabled || binding.Status == model.EdgeRouteStatusUnavailable {
							probe.State = binding.Status
						}
						probe.ID, err = DNSReadinessProbeID(probe)
						if err != nil {
							return nil, err
						}
						probes[probe.ID] = probe
						target.ProbeIDs = append(target.ProbeIDs, probe.ID)
						if len(probes) > policy.DNSReadiness.MaxProbes {
							return nil, fmt.Errorf("DNS readiness probe budget exceeded")
						}
					}
					target.ProbeIDs = uniqueSorted(target.ProbeIDs)
					r.Targets = append(r.Targets, target)
				}
			}
		}
		sort.Slice(r.Targets, func(i, j int) bool {
			return r.Targets[i].EdgeID+"\x00"+r.Targets[i].Address < r.Targets[j].EdgeID+"\x00"+r.Targets[j].Address
		})
		plan.Records = append(plan.Records, r)
	}
	for _, p := range probes {
		plan.Probes = append(plan.Probes, p)
	}
	sort.Slice(plan.Probes, func(i, j int) bool { return plan.Probes[i].ID < plan.Probes[j].ID })
	sort.Slice(plan.Records, func(i, j int) bool { return plan.Records[i].Hostname < plan.Records[j].Hostname })
	if err := ValidateDNSReadinessPlan(plan, policy.DNSReadiness); err != nil {
		return nil, err
	}
	return plan, nil
}

func DNSReadinessProbeID(p DNSReadinessProbe) (string, error) { p.ID = ""; return Digest(p) }

func ValidateDNSReadinessPlan(plan *DNSReadinessPlan, policy *DNSReadinessPolicy) error {
	if plan == nil {
		if policy != nil {
			return fmt.Errorf("DNS readiness plan missing")
		}
		return nil
	}
	if policy == nil {
		return fmt.Errorf("DNS readiness plan has no policy")
	}
	if err := ValidateDNSReadinessPolicy(policy); err != nil {
		return err
	}
	if len(plan.Probes) > policy.MaxProbes || len(plan.Records) > 10000 {
		return fmt.Errorf("DNS readiness plan exceeds limits")
	}
	probes := map[string]DNSReadinessProbe{}
	owners := map[string]string{}
	for _, p := range plan.Probes {
		id, err := DNSReadinessProbeID(p)
		digestBytes, digestErr := hex.DecodeString(strings.TrimPrefix(p.RouteDigest, "sha256:"))
		ip, ipErr := netip.ParseAddr(p.Address)
		if digestErr != nil || len(digestBytes) != 32 || strings.ToLower(p.RouteDigest) != p.RouteDigest || err != nil || id != p.ID || p.ID == "" || probes[p.ID].ID != "" || !validDNSConsumerIdentity(p.EdgeID) || !platformRouteArtifactGroupID.MatchString(p.EdgeGroupID) || ipErr != nil || !PublicDNSFlattenIP(ip) || ip.String() != p.Address || !validDNSConsumerZone(p.Hostname) || p.Path != model.NormalizeAppRoutePathPrefix(p.Path) || !strings.HasPrefix(p.RouteDigest, "sha256:") || len(p.RouteDigest) != 71 || (p.State != "" && p.State != "disabled" && p.State != "unavailable") {
			return fmt.Errorf("invalid DNS route-proof requirement")
		}
		if owner, ok := owners[p.Address]; ok && owner != p.EdgeID+"\x00"+p.EdgeGroupID {
			return fmt.Errorf("ambiguous readiness probe address owner")
		}
		owners[p.Address] = p.EdgeID + "\x00" + p.EdgeGroupID
		probes[p.ID] = p
	}
	seen, used := map[string]bool{}, map[string]bool{}
	for _, r := range plan.Records {
		if !validDNSConsumerZone(r.Hostname) || seen[r.Hostname] || r.MinimumHealthyEdges < 1 || r.MinimumHealthyEdges > 4096 || len(r.Targets) > 8192 {
			return fmt.Errorf("invalid DNS readiness record")
		}
		seen[r.Hostname] = true
		targets := map[string]bool{}
		for _, target := range r.Targets {
			key := target.EdgeID + "\x00" + target.Address
			ip, err := netip.ParseAddr(target.Address)
			if targets[key] || err != nil || (target.Family != "A" && target.Family != "AAAA") || ip.Is4() != (target.Family == "A") || len(target.ProbeIDs) == 0 || len(target.ProbeIDs) > 4096 {
				return fmt.Errorf("invalid DNS readiness target")
			}
			targets[key] = true
			refs := map[string]bool{}
			for _, id := range target.ProbeIDs {
				p, ok := probes[id]
				if !ok || refs[id] || p.EdgeID != target.EdgeID || p.EdgeGroupID != target.EdgeGroupID || p.Address != target.Address {
					return fmt.Errorf("DNS readiness target proof binding mismatch")
				}
				refs[id] = true
				used[id] = true
			}
		}
	}
	if len(used) != len(probes) {
		return fmt.Errorf("orphan DNS readiness probe")
	}
	return nil
}
