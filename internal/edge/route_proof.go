package edge

import (
	"encoding/base64"
	"encoding/json"
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
	expectedState := r.Header.Get(routeproof.StateHeader)
	if len(r.Header.Values(routeproof.StateHeader)) > 1 || (len(r.Header.Values(routeproof.StateHeader)) == 1 && expectedState != model.EdgeRouteStatusDisabled && expectedState != model.EdgeRouteStatusUnavailable) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
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
	stateMatches := !fallback && route.Status == model.EdgeRouteStatusActive
	if expectedState != "" {
		// Inactive index entries use the error-page fallback path even when
		// local. Check actual ownership and forbid any remaining upstream.
		stateMatches = route.Status == expectedState && routeMatchesCurrentEdgeGroup(route, s.Config.EdgeGroupID) && route.UpstreamURL == "" && len(route.Upstreams) == 0
	}
	if index.publication.Candidate || !stateMatches || !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) ||
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
	if expectedState == "" && s.appTrafficProofApplied(index) {
		active := true
		for _, upstream := range route.Upstreams {
			if upstream.Status != "" && upstream.Status != model.EdgeRouteStatusActive {
				active = false
			}
		}
		if digest, err := routeproof.AppTrafficDigest(route); active && err == nil {
			w.Header().Set(routeproof.AppTrafficHeader, digest)
		}
	}
	if index.trafficRelease != nil {
		raw, err := json.Marshal(index.trafficRelease)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set(routeproof.TrafficHeader, base64.RawURLEncoding.EncodeToString(raw))
	}
	if expectedState != "" {
		w.Header().Set(routeproof.StateHeader, expectedState)
	}
	w.WriteHeader(http.StatusNoContent)
}

// Check the exact immutable index against the applied proxy snapshot under the
// same lock used by bundle publication. A loaded candidate or a pending/failed
// Caddy apply cannot attest that application traffic has migrated.
func (s *Service) appTrafficProofApplied(index *edgeRouteIndex) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Config.CaddyEnabled && index != nil && !index.publication.Candidate &&
		s.routeIndex.Load() == index && s.bundle != nil && s.bundle.Version == index.bundleVersion &&
		index.validUntil.After(time.Now()) && s.snapshot.Healthy && !s.snapshot.StaleCache && !s.snapshot.MaxStaleExceeded &&
		s.snapshot.CaddyAppliedVersion == index.bundleVersion && s.snapshot.CaddyLastError == "" && s.snapshot.LastError == ""
}
