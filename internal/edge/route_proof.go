package edge

import (
	"net/http"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeproof"
)

// A proof uses exactly one published index. It cannot execute an origin, read
// business state, renew a lease, or turn an emergency expired LKG into new evidence.
func (s *Service) handleRouteProof(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodHead || len(r.Header.Values(routeproof.RequestHeader)) != 1 ||
		r.Header.Get(routeproof.RequestHeader) != "1" || len(r.Header.Values(routeproof.NonceHeader)) != 1 ||
		!routeproof.ValidNonce(r.Header.Get(routeproof.NonceHeader)) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	index := s.currentRouteIndex()
	// Only Host selects the route; client-provided internal forwarding headers
	// cannot obtain a proof for a different TLS hostname.
	route, ok, fallback, version, _ := index.routeForRequest(normalizeRouteHost(r.Host), r.URL.Path)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if index.publication.Candidate || fallback || route.Status != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) ||
		strings.TrimSpace(s.Config.EdgeID) == "" || strings.TrimSpace(s.Config.EdgeGroupID) == "" || version == "" ||
		!index.validUntil.After(time.Now()) || slices.Contains(route.ExcludedEdgeIDs, s.Config.EdgeID) ||
		slices.Contains(route.ExcludedEdgeGroupIDs, s.Config.EdgeGroupID) {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	digest, err := routeproof.Digest(route)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.Header().Set(routeproof.DigestHeader, digest)
	w.Header().Set(routeproof.NonceHeader, r.Header.Get(routeproof.NonceHeader))
	w.Header().Set(routeproof.VersionHeader, version)
	w.Header().Set(routeproof.ExpiryHeader, index.validUntil.UTC().Format(time.RFC3339Nano))
	w.Header().Set(routeproof.EdgeHeader, s.Config.EdgeID)
	w.Header().Set(routeproof.GroupHeader, s.Config.EdgeGroupID)
	w.WriteHeader(http.StatusNoContent)
}
