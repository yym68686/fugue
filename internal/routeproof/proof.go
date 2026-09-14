// Package routeproof defines the bounded Edge route observation protocol.
package routeproof

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

func canonicalSet(values []string) []string {
	out := append([]string(nil), values...)
	slices.Sort(out)
	return slices.Compact(out)
}
