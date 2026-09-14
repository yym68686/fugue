package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

type platformIntentProjectionResponse struct {
	Intent               platformconfig.PlatformIntent  `json:"intent"`
	RuntimeSnapshot      platformconfig.RuntimeSnapshot `json:"runtime_snapshot"`
	SourceGeneration     string                         `json:"source_generation"`
	CapturedAt           time.Time                      `json:"captured_at"`
	RouteCount           int                            `json:"route_count"`
	OmittedRuntimeFields []string                       `json:"omitted_runtime_fields"`
}

// handleProjectPlatformIntent is read-only: it writes no intent, artifact,
// release, LKG or business record.
func (s *Server) handleProjectPlatformIntent(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	snapshot, err := s.deriveEdgeRouteIntentSnapshot(r, s.store)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "business route projection unavailable")
		return
	}
	apps, err := s.store.ListAppsMetadata("", true)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "business app projection unavailable")
		return
	}
	appByID := make(map[string]model.App, len(apps))
	for _, app := range apps {
		appByID[strings.TrimSpace(app.ID)] = app
	}
	intent, facts, err := s.projectBusinessRouteIntent(r, snapshot, appByID)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "business projection cannot form a canonical PlatformIntent")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, platformIntentProjectionResponse{Intent: intent, RuntimeSnapshot: facts, SourceGeneration: snapshot.Generation, CapturedAt: *facts.CapturedAt, RouteCount: len(intent.Routes), OmittedRuntimeFields: []string{"healthy_edge_node_count", "selected_edge_group", "decision_id", "fallback_reason", "exclusion_evidence"}})
}

func (s *Server) projectBusinessRouteIntent(r *http.Request, snapshot model.EdgeRouteIntentSnapshot, apps map[string]model.App) (platformconfig.PlatformIntent, platformconfig.RuntimeSnapshot, error) {
	captured := time.Now().UTC()
	routes := make([]platformconfig.RouteIntent, 0, len(snapshot.Routes))
	origins := make([]platformconfig.OriginObservation, 0, len(snapshot.Routes))
	for i, source := range snapshot.Routes {
		host := strings.Trim(strings.ToLower(strings.TrimSpace(source.Hostname)), ".")
		app := apps[strings.TrimSpace(source.AppID)]
		upstreamURL := strings.TrimSpace(source.UpstreamURL)
		if upstreamURL == "" && app.ID != "" {
			upstreamURL = s.serviceURLForApp(r.Context(), app)
		}
		if upstreamURL == "" {
			return platformconfig.PlatformIntent{}, platformconfig.RuntimeSnapshot{}, fmt.Errorf("route %q has no desired upstream", host)
		}
		enabled := source.OriginStatus != model.EdgeRouteStatusDisabled
		if app.ID != "" && app.Spec.Replicas == 0 {
			enabled = false
		}
		originRef := ""
		if source.RuntimeID != "" {
			originRef = "origin-" + strconv.Itoa(i)
			status := source.OriginStatus
			if status == "" {
				status = model.EdgeRouteStatusActive
			}
			origins = append(origins, platformconfig.OriginObservation{Ref: originRef, ObservedAt: captured, Status: status, StatusReason: source.OriginStatusReason, RuntimeID: source.RuntimeID, RuntimeType: source.RuntimeType, RuntimeEdgeGroupID: source.RuntimeEdgeGroupID, RuntimeClusterNode: source.RuntimeClusterNode})
		}
		upstreams := make([]platformconfig.UpstreamIntent, 0, len(source.Upstreams))
		for _, upstream := range source.Upstreams {
			upstreams = append(upstreams, platformconfig.UpstreamIntent{Role: upstream.Role, ReleaseID: upstream.ReleaseID, Weight: upstream.Weight, UpstreamKind: upstream.UpstreamKind, UpstreamScope: upstream.UpstreamScope, UpstreamURL: upstream.UpstreamURL, ServicePort: upstream.ServicePort, RuntimeID: upstream.RuntimeID, DeploymentGeneration: upstream.DeploymentGeneration})
		}
		routes = append(routes, platformconfig.RouteIntent{Hostname: host, PathPrefix: source.PathPrefix, Kind: source.RouteKind, AppID: source.AppID, TenantID: source.TenantID, RuntimeID: source.RuntimeID, OriginRef: originRef, UpstreamKind: source.UpstreamKind, UpstreamScope: source.UpstreamScope, UpstreamURL: upstreamURL, Upstreams: upstreams, ServicePort: source.ServicePort, TLSPolicy: source.TLSPolicy, RoutePolicy: source.RoutePolicy, CachePolicyID: source.CachePolicyID, CacheNamespace: source.CacheNamespace, DeploymentGeneration: source.DeploymentGeneration, RequestBodyPolicies: model.CloneEdgeRequestBodyPolicies(source.RequestBodyPolicies), Streaming: boolPointerProjection(source.Streaming), Enabled: enabled, EdgeGroupMode: source.TargetGroupMode, EdgeGroupID: source.PinnedEdgeGroupID})
	}
	intent := platformconfig.NormalizePlatformIntent(platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: platformconfig.GlobalScopeKey, Routes: routes, CachePolicies: platformconfig.CloneCachePolicies(snapshot.CachePolicies)})
	generation, err := platformconfig.PlatformIntentGeneration(intent)
	if err != nil {
		return platformconfig.PlatformIntent{}, platformconfig.RuntimeSnapshot{}, err
	}
	intent.Generation = generation
	return intent, platformconfig.RuntimeSnapshot{IntentGeneration: generation, CapturedAt: &captured, Origins: origins}, nil
}

func boolPointerProjection(value bool) *bool { return &value }
