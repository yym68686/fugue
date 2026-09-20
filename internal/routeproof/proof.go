// Package routeproof defines the bounded Edge route observation protocol.
package routeproof

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	RequestHeader = "X-Fugue-Route-Probe"
	NonceHeader   = "X-Fugue-Route-Probe-Nonce"
	DigestHeader  = "X-Fugue-Route-Proof"
	VersionHeader = "X-Fugue-Route-Bundle-Version"
	ExpiryHeader  = "X-Fugue-Route-Valid-Until"
	EdgeHeader    = "X-Fugue-Route-Edge-Id"
	GroupHeader   = "X-Fugue-Route-Edge-Group"
	StateHeader   = "X-Fugue-Route-Probe-State"
	TrafficHeader = "X-Fugue-Traffic-Release"
	// AppTrafficHeader carries a digest of the release identities, upstream
	// metadata and weights actually loaded by Edge for an application route.
	// The payload is intentionally never returned.
	AppTrafficHeader = "X-Fugue-App-Traffic-Proof"
)

func ValidNonce(nonce string) bool {
	decoded, err := hex.DecodeString(nonce)
	return err == nil && len(decoded) == 16 && nonce == strings.ToLower(nonce)
}

// Digest binds all route fields except publication bookkeeping and diagnostic
// prose. Weighted upstream order is significant to the executor and preserved.
// It does not expose the payload or change the caller's slices.
func Digest(route model.EdgeRouteBinding) (string, error) {
	route.Hostname = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(route.Hostname)), ".")
	route.PathPrefix = model.NormalizeAppRoutePathPrefix(route.PathPrefix)
	route.RouteGeneration, route.DecisionID = "", ""
	route.CreatedAt, route.UpdatedAt = time.Time{}, time.Time{}
	route.StatusReason, route.SelectionReason, route.FallbackReason = "", "", ""
	route.ExclusionReason = ""
	route.HealthyEdgeNodeCount = 0
	route.EdgeRedundancyStatus, route.EdgeRedundancyReason = "", ""
	route.ExcludedEdgeIDs = canonicalSet(route.ExcludedEdgeIDs)
	route.ExcludedEdgeGroupIDs = canonicalSet(route.ExcludedEdgeGroupIDs)
	route.Upstreams = append([]model.EdgeRouteUpstream(nil), route.Upstreams...)
	for i := range route.Upstreams {
		route.Upstreams[i].StatusReason = ""
	}
	raw, err := json.Marshal(struct {
		Schema string                 `json:"schema"`
		Route  model.EdgeRouteBinding `json:"route"`
	}{"fugue.edge.route-proof/v1", route})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

// AppTrafficDigest binds the application release routing that an Edge worker
// has loaded. It is deliberately narrower than Digest: publication metadata,
// edge placement and TLS/cache details are proved by the route proof itself,
// while this digest lets a rollout gate verify the exact release/weight split.
// Upstream order is preserved because it is part of the weighted selector's
// canonical behavior. A valid application proof always has explicit release
// identities and weights totaling 100.
func AppTrafficDigest(route model.EdgeRouteBinding) (string, error) {
	if strings.TrimSpace(route.Hostname) == "" || strings.TrimSpace(route.AppID) == "" || strings.TrimSpace(route.TenantID) == "" || len(route.Upstreams) == 0 || len(route.Upstreams) > 16 {
		return "", errInvalidAppTraffic
	}
	type upstream struct {
		Role                 string `json:"role,omitempty"`
		ReleaseID            string `json:"release_id"`
		Weight               int    `json:"weight"`
		UpstreamKind         string `json:"upstream_kind,omitempty"`
		UpstreamScope        string `json:"upstream_scope,omitempty"`
		UpstreamURL          string `json:"upstream_url"`
		ServicePort          int    `json:"service_port,omitempty"`
		RuntimeID            string `json:"runtime_id,omitempty"`
		DeploymentGeneration string `json:"deployment_generation,omitempty"`
	}
	upstreams := make([]upstream, 0, len(route.Upstreams))
	seen := map[string]struct{}{}
	total := 0
	for _, item := range route.Upstreams {
		item.ReleaseID = strings.TrimSpace(item.ReleaseID)
		item.Role = strings.TrimSpace(item.Role)
		item.UpstreamURL = strings.TrimSpace(item.UpstreamURL)
		item.RuntimeID = strings.TrimSpace(item.RuntimeID)
		item.DeploymentGeneration = strings.TrimSpace(item.DeploymentGeneration)
		if item.ReleaseID == "" || item.UpstreamURL == "" || item.Weight <= 0 || item.Weight > 100 {
			return "", errInvalidAppTraffic
		}
		if _, ok := seen[item.ReleaseID]; ok {
			return "", errInvalidAppTraffic
		}
		seen[item.ReleaseID] = struct{}{}
		total += item.Weight
		upstreams = append(upstreams, upstream{Role: item.Role, ReleaseID: item.ReleaseID, Weight: item.Weight, UpstreamKind: strings.TrimSpace(item.UpstreamKind), UpstreamScope: strings.TrimSpace(item.UpstreamScope), UpstreamURL: item.UpstreamURL, ServicePort: item.ServicePort, RuntimeID: item.RuntimeID, DeploymentGeneration: item.DeploymentGeneration})
	}
	if total != 100 {
		return "", errInvalidAppTraffic
	}
	payload := struct {
		Schema    string     `json:"schema"`
		Hostname  string     `json:"hostname"`
		Path      string     `json:"path"`
		AppID     string     `json:"app_id"`
		TenantID  string     `json:"tenant_id"`
		Upstreams []upstream `json:"upstreams"`
	}{
		Schema:   "fugue.edge.app-traffic-proof/v1",
		Hostname: strings.TrimSuffix(strings.ToLower(strings.TrimSpace(route.Hostname)), "."),
		Path:     model.NormalizeAppRoutePathPrefix(route.PathPrefix),
		AppID:    strings.TrimSpace(route.AppID), TenantID: strings.TrimSpace(route.TenantID), Upstreams: upstreams,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

var errInvalidAppTraffic = errors.New("invalid application traffic route")

func canonicalSet(values []string) []string {
	out := append([]string(nil), values...)
	slices.Sort(out)
	return slices.Compact(out)
}
