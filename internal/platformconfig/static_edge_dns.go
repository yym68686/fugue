package platformconfig

import (
	"crypto/sha256"
	"fmt"
	"fugue/internal/model"
	"net/netip"
	"slices"
)

type StaticEdgeDNSRoute struct {
	AppID    string                     `json:"app_id"`
	TenantID string                     `json:"tenant_id"`
	Hostname string                     `json:"hostname"`
	Routing  model.AppStaticEdgeRouting `json:"routing"`
}

// Separate proof protocol; this is never a Fugue edge release observation.
type StaticEdgeDNSProbe struct {
	StatusCode int `json:"status_code"`
}

func StaticEdgeDNSGroup(edgeID string) string {
	digest := sha256.Sum256([]byte(edgeID))
	return fmt.Sprintf("edge-group-static-%x", digest[:12])
}

func validateStaticEdgeDNSRoutes(p PolicySnapshot) error {
	if len(p.StaticEdgeRoutes) == 0 {
		return nil
	}
	if len(p.StaticEdgeRoutes) > 256 || p.PublicationRole != "" || p.DNSPlacementMode != DNSPlacementConsumerReadiness || p.DNSReadiness == nil || p.DNSQueryPolicy == nil {
		return fmt.Errorf("static DNS routing requires global consumer readiness publication")
	}
	hosts := map[string]bool{}
	for _, r := range p.StaticEdgeRoutes {
		if r.AppID == "" || r.TenantID == "" || !validDNSConsumerZone(r.Hostname) || hosts[r.Hostname] || model.ValidateAppStaticEdgeRouting(&r.Routing) != nil {
			return fmt.Errorf("invalid static edge DNS route authority")
		}
		hosts[r.Hostname] = true
		for _, c := range r.Routing.Candidates {
			for _, value := range c.Addresses {
				ip, _ := netip.ParseAddr(value)
				if !PublicDNSFlattenIP(ip) {
					return fmt.Errorf("static edge address must be public")
				}
			}
		}
		if !slices.Equal(r.Routing.Availability.OrderedEdgeIDs, DNSAvailabilityOrder(p.DNSQueryPolicy, r.Hostname)) {
			return fmt.Errorf("static edge routing differs from signed availability")
		}
	}
	return nil
}

func DNSAvailabilityOrder(p *DNSQueryPolicy, host string) []string {
	if value := DNSAvailabilityForHost(p, host); value != nil {
		return value.OrderedEdgeIDs
	}
	return nil
}

func appendStaticEdgeDNSTargets(out *DNSReadinessRecord, record DNSIntent, owners []CompiledRoute, policy PolicySnapshot, probes map[string]DNSReadinessProbe) error {
	for _, r := range policy.StaticEdgeRoutes {
		if r.Hostname != record.Hostname {
			continue
		}
		if len(owners) != 1 || owners[0].AppID != r.AppID || owners[0].TenantID != r.TenantID || owners[0].Hostname != r.Hostname || model.NormalizeAppRoutePathPrefix(owners[0].PathPrefix) != "/" || out.MinimumHealthyEdges != 1 {
			return fmt.Errorf("static edge DNS requires one app root route and minimum one healthy edge")
		}
		options := DNSPlacementOptions(record)
		for _, c := range r.Routing.Candidates {
			if !DNSPlacementAllowsEdge(record, owners, c.EdgeID, StaticEdgeDNSGroup(c.EdgeID)) {
				return fmt.Errorf("static edge candidate conflicts with route placement constraints")
			}
			for _, address := range c.Addresses {
				for _, existing := range probes {
					if existing.Address == address && existing.StaticEdge == nil {
						return fmt.Errorf("static endpoint overlaps a managed edge")
					}
				}
				ip, _ := netip.ParseAddr(address)
				family := "AAAA"
				if ip.Is4() {
					family = "A"
				}
				if family == "A" && (options.IPv4Policy == "ipv6_only" || options.IPv6Policy == "ipv6_only") || family == "AAAA" && (options.IPv4Policy == "ipv4_only" || options.IPv6Policy == "ipv4_only") {
					continue
				}
				probe := DNSReadinessProbe{EdgeID: c.EdgeID, EdgeGroupID: StaticEdgeDNSGroup(c.EdgeID), Address: address, Hostname: r.Hostname, Path: c.ProbePath, RouteDigest: c.RouteDigest, StaticEdge: &StaticEdgeDNSProbe{StatusCode: c.StatusCode}}
				var err error
				probe.ID, err = DNSReadinessProbeID(probe)
				if err != nil {
					return err
				}
				probes[probe.ID] = probe
				if len(probes) > policy.DNSReadiness.MaxProbes {
					return fmt.Errorf("static edge DNS probe budget exceeded")
				}
				out.Targets = append(out.Targets, DNSReadinessTarget{EdgeID: c.EdgeID, EdgeGroupID: probe.EdgeGroupID, Address: address, Family: family, ProbeIDs: []string{probe.ID}})
			}
		}
	}
	return nil
}
