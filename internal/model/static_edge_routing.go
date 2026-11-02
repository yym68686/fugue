package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"strings"
)

const StaticEdgeDNSVersion = "static-edge-dns-v1"

type StaticEdgeDNSCandidate struct {
	RegistrationID string   `json:"registration_id"`
	EdgeID         string   `json:"edge_id"`
	Addresses      []string `json:"addresses"`
	ProbePath      string   `json:"probe_path"`
	RouteDigest    string   `json:"route_digest"`
	StatusCode     int      `json:"status_code"`
}

type AppStaticEdgeRouting struct {
	Version      string                   `json:"version"`
	Candidates   []StaticEdgeDNSCandidate `json:"candidates"`
	Availability DNSAvailabilityPolicy    `json:"availability"`
}

var staticDNSID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)
var staticDNSDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func ValidateAppStaticEdgeRouting(r *AppStaticEdgeRouting) error {
	if r == nil {
		return nil
	}
	if r.Version != StaticEdgeDNSVersion || len(r.Candidates) < 1 || len(r.Candidates) > 8 || ValidateDNSAvailabilityPolicy(&r.Availability) != nil {
		return fmt.Errorf("invalid static edge DNS routing or availability policy")
	}
	ids, ips := map[string]bool{}, map[string]bool{}
	for _, c := range r.Candidates {
		if !staticDNSID.MatchString(c.EdgeID) || c.RegistrationID == "" || ids[c.EdgeID] || !slices.Contains(r.Availability.OrderedEdgeIDs, c.EdgeID) || len(c.Addresses) < 1 || len(c.Addresses) > 8 || !staticDNSDigest.MatchString(c.RouteDigest) || c.ProbePath != NormalizeAppRoutePathPrefix(c.ProbePath) || strings.ContainsAny(c.ProbePath, "?#\\\r\n") || c.StatusCode < 200 || c.StatusCode > 399 {
			return fmt.Errorf("invalid static edge DNS candidate")
		}
		ids[c.EdgeID] = true
		for _, value := range c.Addresses {
			ip, err := netip.ParseAddr(value)
			if err != nil || ip.String() != value || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ips[value] {
				return fmt.Errorf("static edge addresses must be unique canonical public IPs")
			}
			ips[value] = true
		}
	}
	if len(r.Availability.OrderedEdgeIDs) <= len(ids) {
		return fmt.Errorf("static edge routing requires a managed fallback")
	}
	return nil
}

func CloneAppStaticEdgeRouting(r *AppStaticEdgeRouting) *AppStaticEdgeRouting {
	if r == nil {
		return nil
	}
	out := *r
	out.Candidates = slices.Clone(r.Candidates)
	for i := range out.Candidates {
		out.Candidates[i].Addresses = slices.Clone(r.Candidates[i].Addresses)
	}
	out.Availability = *CloneDNSAvailabilityPolicy(&r.Availability)
	return &out
}

// The revision covers the entire primary route, so a rename cannot reuse an old
// DNS authorization. It does not depend on unrelated deployment timestamps.
func AppRouteRevision(r *AppRoute) string {
	raw, _ := json.Marshal(r)
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:])
}
