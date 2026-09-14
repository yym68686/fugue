package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/runtime"
)

type platformProjectionIssue struct {
	Code       string `json:"code"`
	Hostname   string `json:"hostname,omitempty"`
	PathPrefix string `json:"path_prefix,omitempty"`
}

type platformIntentProjectionResponse struct {
	Intent               platformconfig.PlatformIntent  `json:"intent"`
	RuntimeSnapshot      platformconfig.RuntimeSnapshot `json:"runtime_snapshot"`
	SourceGeneration     string                         `json:"source_generation"`
	CapturedAt           time.Time                      `json:"captured_at"`
	RouteCount           int                            `json:"route_count"`
	OmittedRuntimeFields []string                       `json:"omitted_runtime_fields"`
	MigrationReady       bool                           `json:"migration_ready"`
	Issues               []platformProjectionIssue      `json:"issues"`
}

type platformProjectionSource struct {
	edgeRouteIntentSource
	apps map[string]model.App
}

func (source *platformProjectionSource) ListAppsMetadata(tenant string, admin bool) ([]model.App, error) {
	apps, err := source.edgeRouteIntentSource.ListAppsMetadata(tenant, admin)
	if err != nil {
		return nil, err
	}
	// Keep the desired source detached from the observation overlay.
	raw, err := json.Marshal(apps)
	if err != nil {
		return nil, err
	}
	var original []model.App
	if err := json.Unmarshal(raw, &original); err != nil {
		return nil, err
	}
	source.apps = make(map[string]model.App, len(original))
	for _, app := range original {
		source.apps[app.ID] = app
	}
	return apps, nil
}

func (s *Server) handleProjectPlatformIntent(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	source := &platformProjectionSource{edgeRouteIntentSource: s.store}
	observed := map[string]model.App{}
	snapshot, err := s.deriveEdgeRouteIntentSnapshotWithObservations(r, source, func(apps []model.App) {
		for _, app := range apps {
			observed[app.ID] = app
		}
	})
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "business route projection unavailable")
		return
	}
	projection, err := projectBusinessRouteDraft(snapshot, source.apps, observed, s.platformRoutes)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "business route draft cannot be captured")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, projection)
}

// This diagnostic does not turn runtime-selected targets into desired intent.
// Unsupported migration semantics are explicit issues until a complete
// business/constraint/fact snapshot can be frozen and compared.
func projectBusinessRouteDraft(snapshot model.EdgeRouteIntentSnapshot, apps, observed map[string]model.App, platformRoutes []model.PlatformRoute) (platformIntentProjectionResponse, error) {
	result := platformIntentProjectionResponse{SourceGeneration: snapshot.Generation, CapturedAt: snapshot.GeneratedAt,
		Issues:               []platformProjectionIssue{{Code: "transaction_snapshot_not_frozen"}, {Code: "constraint_policy_not_projected"}, {Code: "dns_tls_not_projected"}},
		OmittedRuntimeFields: []string{"selected_edge_group", "decision_id", "exclusion_evidence"},
	}
	addIssue := func(code string, route model.EdgeRouteIntent) {
		result.Issues = append(result.Issues, platformProjectionIssue{Code: code, Hostname: route.Hostname, PathPrefix: model.NormalizeAppRoutePathPrefix(route.PathPrefix)})
	}
	platformByHost := map[string]model.PlatformRoute{}
	for _, route := range platformRoutes {
		platformByHost[normalizeExternalAppDomain(route.Hostname)] = route
	}
	intent := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: platformconfig.GlobalScopeKey, CachePolicies: platformconfig.CloneCachePolicies(snapshot.CachePolicies)}
	facts := platformconfig.RuntimeSnapshot{CapturedAt: &result.CapturedAt}
	for _, source := range snapshot.Routes {
		host, path := normalizeExternalAppDomain(source.Hostname), model.NormalizeAppRoutePathPrefix(source.PathPrefix)
		if host == "" {
			return result, fmt.Errorf("empty route hostname")
		}
		streaming := source.Streaming
		route := platformconfig.RouteIntent{Hostname: host, PathPrefix: path, Kind: source.RouteKind, AppID: source.AppID, TenantID: source.TenantID,
			UpstreamKind: source.UpstreamKind, UpstreamScope: source.UpstreamScope, ServicePort: source.ServicePort,
			TLSPolicy: source.TLSPolicy, RoutePolicy: source.RoutePolicy, CachePolicyID: source.CachePolicyID, CacheNamespace: source.CacheNamespace,
			DeploymentGeneration: source.DeploymentGeneration, RequestBodyPolicies: model.CloneEdgeRequestBodyPolicies(source.RequestBodyPolicies), Streaming: &streaming,
			EdgeGroupMode: model.PlatformRouteEdgeGroupModeAllHealthy,
		}
		if source.TargetGroupMode == model.EdgeRouteIntentGroupModePinnedGroup {
			route.EdgeGroupMode, route.EdgeGroupID = model.PlatformRouteEdgeGroupModePinned, source.PinnedEdgeGroupID
		} else if source.TargetGroupMode != model.EdgeRouteIntentGroupModeAllGroups {
			addIssue("unknown_group_mode", source)
		}
		if source.AppID != "" {
			app, exists := apps[source.AppID]
			if !exists || app.TenantID != source.TenantID {
				return result, fmt.Errorf("route owner is missing")
			}
			route.Enabled, route.RuntimeID = app.Spec.Replicas > 0, strings.TrimSpace(app.Spec.RuntimeID)
			route.UpstreamURL = "http://" + appServiceHost(runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppServiceName(app)) + ":" + strconv.Itoa(source.ServicePort)
			if source.ServicePort <= 0 {
				addIssue("desired_service_port_missing", source)
			}
			ref, err := platformconfig.Digest([]string{host, path, source.AppID, route.RuntimeID})
			if err != nil {
				return result, err
			}
			route.OriginRef = "origin_" + strings.TrimPrefix(ref, "sha256:")
			observation := platformconfig.OriginObservation{Ref: route.OriginRef, Status: source.OriginStatus, StatusReason: source.OriginStatusReason,
				RuntimeID: source.RuntimeID, RuntimeType: source.RuntimeType, RuntimeEdgeGroupID: source.RuntimeEdgeGroupID, RuntimeClusterNode: source.RuntimeClusterNode}
			if evidence := observed[source.AppID].ObservedStatus; evidence != nil {
				observation.ObservedAt = evidence.ObservedAt
				if !evidence.Fresh {
					addIssue("origin_observation_not_fresh", source)
				}
			}
			if observation.ObservedAt.IsZero() {
				addIssue("origin_observation_time_missing", source)
			}
			if observation.Status == "" {
				observation.Status = model.EdgeRouteStatusUnavailable
				addIssue("origin_status_missing", source)
			}
			if observation.Status == model.EdgeRouteStatusRuntimeMissing {
				observation.Status = model.EdgeRouteStatusUnavailable
			}
			if route.RuntimeID == "" || route.RuntimeID != observation.RuntimeID {
				addIssue("desired_runtime_not_resolved", source)
			}
			if source.UpstreamURL != "" && source.UpstreamURL != route.UpstreamURL {
				addIssue("resolved_upstream_differs", source)
			}
			if len(source.Upstreams) > 0 {
				addIssue("release_weights_not_projected", source)
			}
			facts.Origins = append(facts.Origins, observation)
		} else {
			configured, exists := platformByHost[host]
			if !exists {
				return result, fmt.Errorf("platform route configuration missing")
			}
			route.UpstreamURL, route.Status, route.StatusReason = configured.UpstreamURL, configured.Status, configured.StatusReason
			route.Enabled = configured.Status != model.EdgeRouteStatusDisabled
		}
		intent.Routes = append(intent.Routes, route)
	}
	intent = platformconfig.NormalizePlatformIntent(intent)
	generation, err := platformconfig.PlatformIntentGeneration(intent)
	if err != nil {
		return result, err
	}
	intent.Generation = generation
	if err := platformconfig.ValidatePlatformIntent(intent); err != nil {
		result.Issues = append(result.Issues, platformProjectionIssue{Code: "intent_requires_validation_repair"})
	}
	facts.IntentGeneration = generation
	sort.Slice(facts.Origins, func(i, j int) bool { return facts.Origins[i].Ref < facts.Origins[j].Ref })
	sort.Slice(result.Issues, func(i, j int) bool {
		a, b := result.Issues[i], result.Issues[j]
		return a.Code+"\x00"+a.Hostname+"\x00"+a.PathPrefix < b.Code+"\x00"+b.Hostname+"\x00"+b.PathPrefix
	})
	result.Intent, result.RuntimeSnapshot, result.RouteCount = intent, facts, len(intent.Routes)
	return result, nil
}
