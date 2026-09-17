package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
)

type platformRouteMigrationComparison struct {
	ArtifactID          string                             `json:"artifact_id"`
	ArtifactDigest      string                             `json:"artifact_digest"`
	ArtifactGeneration  string                             `json:"artifact_generation"`
	SourceGeneration    string                             `json:"source_generation"`
	CapturedAt          time.Time                          `json:"captured_at"`
	SourceRouteCount    int                                `json:"source_route_count"`
	ArtifactRouteCount  int                                `json:"artifact_route_count"`
	MatchingRouteCount  int                                `json:"matching_route_count"`
	Equivalent          bool                               `json:"equivalent"`
	Differences         []platformRouteMigrationDifference `json:"differences"`
	SnapshotDifferences []string                           `json:"snapshot_differences"`
}

type platformRouteMigrationDifference struct {
	Hostname   string   `json:"hostname"`
	PathPrefix string   `json:"path_prefix"`
	Kind       string   `json:"kind"`
	Fields     []string `json:"fields"`
}

// This endpoint deliberately bypasses the serving-source selector: migration
// must compare the captured legacy source even if a route LKG already exists.
// It neither persists this observation as intent nor authorizes promotion.
func (s *Server) handleComparePlatformRouteMigration(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("artifact_id"))
	if id == "" {
		httpx.WriteError(w, http.StatusBadRequest, "artifact_id is required")
		return
	}
	artifact, err := s.store.GetPlatformArtifact(id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpx.WriteError(w, http.StatusNotFound, "artifact not found")
		} else {
			httpx.WriteError(w, http.StatusServiceUnavailable, "artifact storage unavailable")
		}
		return
	}
	if artifact.ID != id || artifact.ArtifactKind != model.PlatformArtifactKindEdgeRouteBundle ||
		artifact.ScopeKey != "global" || artifact.Status != model.PlatformArtifactStatusValidated ||
		!platformsafety.EvaluateArtifactIntegrity(artifact, s.bundleKeyring()).Pass {
		httpx.WriteError(w, http.StatusConflict, "comparison requires an exact trusted validated global route artifact")
		return
	}
	projected, err := projectPlatformRouteArtifact(artifact)
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, "route artifact projection is incompatible")
		return
	}
	source, err := s.deriveEdgeRouteIntentSnapshot(r, s.store)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "business route projection unavailable")
		return
	}
	if err := validateEdgeRouteIntentSnapshotForDiagnostics(source); err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "business route projection is invalid")
		return
	}
	comparison, err := comparePlatformRouteSnapshots(source, projected)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "route projections cannot be compared unambiguously")
		return
	}
	comparison.ArtifactID = artifact.ID
	comparison.ArtifactDigest = artifact.ContentHash
	comparison.ArtifactGeneration = artifact.Generation
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, comparison)
}

// Compare all route fields except generation and record timestamps. Those
// identify different representations, not different traffic behavior. Report
// field names only, so upstream URLs and tenant metadata are not duplicated in
// diagnostic evidence. Route ordering is immaterial; nested rule order is not.
func comparePlatformRouteSnapshots(source, artifact model.EdgeRouteIntentSnapshot) (platformRouteMigrationComparison, error) {
	result := platformRouteMigrationComparison{
		SourceGeneration: source.Generation, CapturedAt: source.GeneratedAt,
		SourceRouteCount: len(source.Routes), ArtifactRouteCount: len(artifact.Routes),
		Differences: []platformRouteMigrationDifference{}, SnapshotDifferences: []string{},
	}
	left, err := routeMigrationIndex(source.Routes)
	if err != nil {
		return result, err
	}
	right, err := routeMigrationIndex(artifact.Routes)
	if err != nil {
		return result, err
	}
	keys := make([]string, 0, len(left)+len(right))
	for key := range left {
		keys = append(keys, key)
	}
	for key := range right {
		if _, exists := left[key]; !exists {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		host, path, _ := strings.Cut(key, "\x00")
		difference := platformRouteMigrationDifference{Hostname: host, PathPrefix: path, Fields: []string{}}
		a, hasLeft := left[key]
		b, hasRight := right[key]
		switch {
		case !hasRight:
			difference.Kind = "missing_from_artifact"
		case !hasLeft:
			difference.Kind = "extra_in_artifact"
		default:
			for field, value := range a {
				if !bytes.Equal(value, b[field]) {
					difference.Fields = append(difference.Fields, field)
				}
			}
			for field := range b {
				if _, exists := a[field]; !exists {
					difference.Fields = append(difference.Fields, field)
				}
			}
			if len(difference.Fields) == 0 {
				result.MatchingRouteCount++
				continue
			}
			sort.Strings(difference.Fields)
			difference.Kind = "changed"
		}
		result.Differences = append(result.Differences, difference)
	}
	// A route-only match must not hide a lost TLS allowlist or cache policy.
	leftCache, err := cachePoliciesForMigrationComparison(source.CachePolicies)
	if err != nil {
		return result, err
	}
	rightCache, err := cachePoliciesForMigrationComparison(artifact.CachePolicies)
	if err != nil {
		return result, err
	}
	for _, field := range []struct {
		name        string
		left, right any
	}{
		{"tls_allowlist", append([]model.EdgeTLSAllowlistEntry{}, source.TLSAllowlist...), append([]model.EdgeTLSAllowlistEntry{}, artifact.TLSAllowlist...)},
		{"cache_policies", leftCache, rightCache},
	} {
		a, err := json.Marshal(field.left)
		if err != nil {
			return result, err
		}
		b, err := json.Marshal(field.right)
		if err != nil {
			return result, err
		}
		if !bytes.Equal(a, b) {
			result.SnapshotDifferences = append(result.SnapshotDifferences, field.name)
		}
	}
	result.Equivalent = len(result.Differences) == 0 && len(result.SnapshotDifferences) == 0
	return result, nil
}

type migrationCachePolicies struct {
	Policies          []model.CachePolicy `json:"policies"`
	HTMLFallbackOrder []string            `json:"html_fallback_order"`
}

func cachePoliciesForMigrationComparison(policies []model.CachePolicy) (migrationCachePolicies, error) {
	result := migrationCachePolicies{Policies: append([]model.CachePolicy{}, policies...), HTMLFallbackOrder: []string{}}
	seen := make(map[string]bool, len(policies))
	for _, policy := range policies {
		key := strings.ToLower(strings.TrimSpace(policy.ID))
		if key == "" || policy.ID != strings.TrimSpace(policy.ID) || seen[key] {
			return result, fmt.Errorf("duplicate or empty cache policy identity")
		}
		seen[key] = true
		// Edge resolves the explicitly referenced policy by ID, then appends
		// HTML document policies in bundle order. Preserve that precedence;
		// only collection order without executor meaning may be normalized.
		if strings.EqualFold(strings.TrimSpace(policy.Kind), model.CachePolicyKindHTMLDocuments) {
			result.HTMLFallbackOrder = append(result.HTMLFallbackOrder, policy.ID)
		}
	}
	sort.Slice(result.Policies, func(i, j int) bool { return result.Policies[i].ID < result.Policies[j].ID })
	return result, nil
}

func routeMigrationIndex(routes []model.EdgeRouteIntent) (map[string]map[string]json.RawMessage, error) {
	index := make(map[string]map[string]json.RawMessage, len(routes))
	for _, route := range routes {
		route.Hostname = normalizeExternalAppDomain(route.Hostname)
		route.PathPrefix = model.NormalizeAppRoutePathPrefix(route.PathPrefix)
		key := route.Hostname + "\x00" + route.PathPrefix
		if _, exists := index[key]; exists || route.Hostname == "" {
			return nil, fmt.Errorf("duplicate or empty route identity")
		}
		raw, err := json.Marshal(route)
		if err != nil {
			return nil, err
		}
		fields := map[string]json.RawMessage{}
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, err
		}
		delete(fields, "generation")
		delete(fields, "created_at")
		delete(fields, "updated_at")
		// No exclusion has no lifecycle to execute. Preserve all non-clear
		// states and every list/expiry/reason field; this only normalizes the
		// legacy omission of an explicitly empty exclusion state.
		if len(route.ExcludedEdgeIDs) == 0 && len(route.ExcludedEdgeGroupIDs) == 0 && route.ExclusionLifecycle == model.EdgeExclusionLifecycleClear {
			delete(fields, "exclusion_lifecycle")
		}
		index[key] = fields
	}
	return index, nil
}
