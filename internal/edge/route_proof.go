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

// A proof uses exactly one published index. Retained observations require an
// explicit mode and cannot become candidate/application rollout evidence.
func (s *Service) handleRouteProof(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	mode, hasMode := r.Header[http.CanonicalHeaderKey(routeproof.ModeHeader)]
	if hasMode && (len(mode) != 1 || mode[0] != routeproof.RetainedLKGMode) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	expectedState := r.Header.Get(routeproof.StateHeader)
	if len(r.Header.Values(routeproof.StateHeader)) > 1 || (len(r.Header.Values(routeproof.StateHeader)) == 1 && expectedState != model.EdgeRouteStatusDisabled && expectedState != model.EdgeRouteStatusUnavailable && expectedState != routeproof.StateExcluded) {
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
	excluded := slices.Contains(route.ExcludedEdgeIDs, s.Config.EdgeID) || slices.Contains(route.ExcludedEdgeGroupIDs, s.Config.EdgeGroupID)
	stateMatches := !fallback && route.Status == model.EdgeRouteStatusActive && !excluded
	if expectedState == routeproof.StateExcluded {
		// This proves the loaded exclusion only, never serving eligibility.
		stateMatches = excluded && routeMatchesCurrentEdgeGroup(route, s.Config.EdgeGroupID)
	} else if expectedState != "" {
		// Inactive index entries use the error-page fallback path even when
		// local. Check actual ownership and forbid any remaining upstream.
		stateMatches = !excluded && route.Status == expectedState && routeMatchesCurrentEdgeGroup(route, s.Config.EdgeGroupID) && route.UpstreamURL == "" && len(route.Upstreams) == 0
	}
	now := time.Now().UTC()
	until := index.validUntil
	retained := hasMode && !until.IsZero() && !until.After(now)
	if retained {
		until = s.retainedRouteProofUntil(index, now)
	}
	if index.publication.Candidate || !stateMatches || !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) ||
		strings.TrimSpace(s.Config.EdgeID) == "" || strings.TrimSpace(s.Config.EdgeGroupID) == "" || version == "" ||
		!until.After(now) {
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
	w.Header().Set(routeproof.ExpiryHeader, until.UTC().Format(time.RFC3339Nano))
	if retained {
		w.Header().Set(routeproof.ModeHeader, routeproof.RetainedLKGMode)
		w.Header().Set(routeproof.BundleExpiryHeader, index.validUntil.UTC().Format(time.RFC3339Nano))
	}
	w.Header().Set(routeproof.EdgeHeader, s.Config.EdgeID)
	w.Header().Set(routeproof.GroupHeader, s.Config.EdgeGroupID)
	if !retained && expectedState == "" && s.appTrafficProofApplied(index) {
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

func (s *Service) retainedRouteProofUntil(index *edgeRouteIndex, now time.Time) time.Time {
	if index == nil || index.publication.Candidate || index.validUntil.IsZero() || s.Config.MaxStale <= 0 {
		return time.Time{}
	}
	until := index.validUntil.Add(s.Config.MaxStale)
	if !until.After(now) {
		return time.Time{}
	}
	s.mu.Lock()
	if !s.Config.CaddyEnabled || s.bundle == nil || s.routeIndex.Load() != index || s.bundle.Version != index.bundleVersion ||
		s.snapshot.CaddyAppliedVersion != index.bundleVersion || s.snapshot.CaddyLastError != "" || !s.snapshot.Healthy {
		s.mu.Unlock()
		return time.Time{}
	}
	bundle := *s.bundle
	s.mu.Unlock()
	if (!s.edgeControlRouteSourceEnabled() && strings.TrimSpace(s.Config.BundleSigningKey) == "" && strings.TrimSpace(s.Config.BundleSigningPreviousKey) == "") ||
		(bundle.Signature == "" && len(bundle.Signatures) == 0) {
		return time.Time{}
	}
	// Recheck signatures and current trust, including revoked keys. This never
	// reads the API, rewrites the bundle, renews CachedAt or extends MaxStale.
	if s.verifyCachedBundle(cacheFile{Bundle: bundle}, now) != nil {
		return time.Time{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.routeIndex.Load() != index || s.snapshot.CaddyAppliedVersion != index.bundleVersion || s.snapshot.CaddyLastError != "" || !s.snapshot.Healthy {
		return time.Time{}
	}
	return until
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
