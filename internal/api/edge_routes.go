package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

const defaultEdgeGroupID = "edge-group-default"

type edgeRouteBundleOptions struct {
	EdgeID      string
	EdgeGroupID string
}

func (s *Server) handleEdgeRoutes(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeEdgeToken(w, r) {
		return
	}

	options := edgeRouteBundleOptions{
		EdgeID:      strings.TrimSpace(r.URL.Query().Get("edge_id")),
		EdgeGroupID: strings.TrimSpace(r.URL.Query().Get("edge_group_id")),
	}
	bundle, err := s.deriveEdgeRouteBundle(r, options)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	etag := edgeRouteBundleETag(bundle.Version)
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("X-Fugue-Route-Bundle-Version", bundle.Version)
	if edgeRouteBundleETagMatches(r.Header.Get("If-None-Match"), bundle.Version) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, bundle)
}

func (s *Server) deriveEdgeRouteBundle(r *http.Request, options edgeRouteBundleOptions) (model.EdgeRouteBundle, error) {
	apps, err := s.store.ListAppsMetadata("", true)
	if err != nil {
		return model.EdgeRouteBundle{}, err
	}
	domains, err := s.store.ListVerifiedAppDomains()
	if err != nil {
		return model.EdgeRouteBundle{}, err
	}
	runtimes, err := s.store.ListRuntimes("", true)
	if err != nil {
		return model.EdgeRouteBundle{}, err
	}

	runtimeByID := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		runtimeByID[strings.TrimSpace(runtimeObj.ID)] = runtimeObj
	}
	appByID := make(map[string]model.App, len(apps))
	for _, app := range apps {
		app = s.overlayManagedAppStatusCached(app)
		appByID[strings.TrimSpace(app.ID)] = app
	}

	routes := make([]model.EdgeRouteBinding, 0, len(apps)+len(domains))
	for _, app := range appByID {
		if app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "" {
			continue
		}
		binding := s.deriveEdgeRouteBinding(r, app, strings.TrimSpace(app.Route.Hostname), model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, app.CreatedAt, app.UpdatedAt, runtimeByID)
		if edgeRouteMatchesSelector(binding, options) {
			routes = append(routes, binding)
		}
	}

	tlsAllowlist := make([]model.EdgeTLSAllowlistEntry, 0, len(domains))
	for _, domain := range domains {
		hostname := normalizeExternalAppDomain(domain.Hostname)
		if hostname == "" || !s.managedEdgeCustomDomain(hostname) {
			continue
		}
		app, ok := appByID[strings.TrimSpace(domain.AppID)]
		if !ok {
			continue
		}
		binding := s.deriveEdgeRouteBinding(r, app, hostname, model.EdgeRouteKindCustomDomain, model.EdgeRouteTLSPolicyCustomDomain, domain.CreatedAt, domain.UpdatedAt, runtimeByID)
		if edgeRouteMatchesSelector(binding, options) {
			routes = append(routes, binding)
			tlsAllowlist = append(tlsAllowlist, model.EdgeTLSAllowlistEntry{
				Hostname:  hostname,
				AppID:     domain.AppID,
				TenantID:  domain.TenantID,
				Status:    domain.Status,
				TLSStatus: domain.TLSStatus,
			})
		}
	}

	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Hostname != routes[j].Hostname {
			return routes[i].Hostname < routes[j].Hostname
		}
		return routes[i].RouteKind < routes[j].RouteKind
	})
	sort.Slice(tlsAllowlist, func(i, j int) bool {
		return tlsAllowlist[i].Hostname < tlsAllowlist[j].Hostname
	})

	bundle := model.EdgeRouteBundle{
		GeneratedAt:  time.Now().UTC(),
		EdgeID:       options.EdgeID,
		EdgeGroupID:  options.EdgeGroupID,
		Routes:       routes,
		TLSAllowlist: tlsAllowlist,
	}
	bundle.Version = edgeRouteBundleVersion(bundle)
	return bundle, nil
}

func (s *Server) deriveEdgeRouteBinding(r *http.Request, app model.App, hostname, routeKind, tlsPolicy string, createdAt, updatedAt time.Time, runtimeByID map[string]model.Runtime) model.EdgeRouteBinding {
	runtimeID := appProxyRuntimeID(app)
	runtimeObj, runtimeFound := runtimeByID[runtimeID]
	edgeGroupID := derivedEdgeGroupIDForRuntime(runtimeObj, runtimeFound)
	fallbackEdgeGroupID := ""
	if edgeGroupID != defaultEdgeGroupID {
		fallbackEdgeGroupID = defaultEdgeGroupID
	}
	status, reason := edgeRouteStatus(app, runtimeID, runtimeFound)
	servicePort := edgeServicePortForApp(app)

	binding := model.EdgeRouteBinding{
		Hostname:            normalizeExternalAppDomain(hostname),
		RouteKind:           routeKind,
		AppID:               app.ID,
		TenantID:            app.TenantID,
		RuntimeID:           runtimeID,
		EdgeGroupID:         edgeGroupID,
		FallbackEdgeGroupID: fallbackEdgeGroupID,
		RoutePolicy:         model.EdgeRoutePolicyPrimary,
		UpstreamKind:        model.EdgeRouteUpstreamKindKubernetesService,
		ServicePort:         servicePort,
		TLSPolicy:           tlsPolicy,
		Streaming:           true,
		Status:              status,
		StatusReason:        reason,
		CreatedAt:           createdAt,
		UpdatedAt:           updatedAt,
	}
	if binding.Status == model.EdgeRouteStatusActive {
		binding.UpstreamURL = s.serviceURLForApp(r.Context(), app)
	}
	binding.RouteGeneration = edgeRouteGeneration(binding)
	return binding
}

func edgeRouteStatus(app model.App, runtimeID string, runtimeFound bool) (string, string) {
	switch {
	case app.Spec.Replicas == 0:
		return model.EdgeRouteStatusDisabled, "desired replicas is 0"
	case strings.TrimSpace(runtimeID) == "":
		return model.EdgeRouteStatusRuntimeMissing, "app has no runtime id"
	case !runtimeFound:
		return model.EdgeRouteStatusRuntimeMissing, "runtime not found"
	case app.Status.CurrentReplicas == 0:
		return model.EdgeRouteStatusUnavailable, appRouteUnavailableMessage(app)
	default:
		return model.EdgeRouteStatusActive, ""
	}
}

func edgeServicePortForApp(app model.App) int {
	if app.Route != nil && app.Route.ServicePort > 0 {
		return app.Route.ServicePort
	}
	if len(app.Spec.Ports) > 0 && app.Spec.Ports[0] > 0 {
		return app.Spec.Ports[0]
	}
	return 80
}

func edgeRouteMatchesSelector(binding model.EdgeRouteBinding, options edgeRouteBundleOptions) bool {
	if options.EdgeGroupID != "" {
		return binding.EdgeGroupID == options.EdgeGroupID || binding.FallbackEdgeGroupID == options.EdgeGroupID
	}
	if edgeGroupID := edgeGroupIDFromEdgeID(options.EdgeID); edgeGroupID != "" {
		return binding.EdgeGroupID == edgeGroupID || binding.FallbackEdgeGroupID == edgeGroupID
	}
	return true
}

func edgeGroupIDFromEdgeID(edgeID string) string {
	edgeID = strings.TrimSpace(edgeID)
	if strings.HasPrefix(edgeID, "edge-group-") {
		return edgeID
	}
	return ""
}

func derivedEdgeGroupIDForRuntime(runtimeObj model.Runtime, runtimeFound bool) string {
	if !runtimeFound {
		return defaultEdgeGroupID
	}
	if country := firstRuntimeLabelValue(runtimeObj.Labels, runtimepkg.LocationCountryCodeLabelKey, "country_code", "countryCode"); country != "" {
		if slug := edgeRouteSlug(country); slug != "" {
			return "edge-group-country-" + slug
		}
	}
	if region := firstRuntimeLabelValue(runtimeObj.Labels, runtimepkg.RegionLabelKey, runtimepkg.LegacyRegionLabelKey, "region"); region != "" {
		if slug := edgeRouteSlug(region); slug != "" {
			return "edge-group-region-" + slug
		}
	}
	return defaultEdgeGroupID
}

func firstRuntimeLabelValue(labels map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(labels[key]); value != "" {
			return value
		}
	}
	return ""
}

func edgeRouteSlug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	lastDash := false
	for _, char := range value {
		switch {
		case char >= 'a' && char <= 'z':
			builder.WriteRune(char)
			lastDash = false
		case char >= '0' && char <= '9':
			builder.WriteRune(char)
			lastDash = false
		case !lastDash:
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

func edgeRouteGeneration(binding model.EdgeRouteBinding) string {
	payload, _ := json.Marshal(edgeRouteVersionMaterialFromBinding(binding))
	sum := sha256.Sum256(payload)
	return "routegen_" + hex.EncodeToString(sum[:])[:16]
}

type edgeRouteVersionMaterial struct {
	Hostname            string `json:"hostname"`
	RouteKind           string `json:"route_kind"`
	AppID               string `json:"app_id"`
	TenantID            string `json:"tenant_id"`
	RuntimeID           string `json:"runtime_id"`
	EdgeGroupID         string `json:"edge_group_id"`
	FallbackEdgeGroupID string `json:"fallback_edge_group_id,omitempty"`
	RoutePolicy         string `json:"route_policy"`
	UpstreamKind        string `json:"upstream_kind"`
	UpstreamURL         string `json:"upstream_url,omitempty"`
	ServicePort         int    `json:"service_port"`
	TLSPolicy           string `json:"tls_policy"`
	Streaming           bool   `json:"streaming"`
	Status              string `json:"status"`
	StatusReason        string `json:"status_reason,omitempty"`
}

type edgeRouteBundleVersionMaterial struct {
	Routes       []edgeRouteVersionMaterial    `json:"routes"`
	TLSAllowlist []model.EdgeTLSAllowlistEntry `json:"tls_allowlist"`
}

func edgeRouteBundleVersion(bundle model.EdgeRouteBundle) string {
	routes := make([]edgeRouteVersionMaterial, len(bundle.Routes))
	for index, route := range bundle.Routes {
		routes[index] = edgeRouteVersionMaterialFromBinding(route)
	}
	material := edgeRouteBundleVersionMaterial{
		Routes:       routes,
		TLSAllowlist: append([]model.EdgeTLSAllowlistEntry(nil), bundle.TLSAllowlist...),
	}
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return "routegen_" + hex.EncodeToString(sum[:])[:16]
}

func edgeRouteVersionMaterialFromBinding(binding model.EdgeRouteBinding) edgeRouteVersionMaterial {
	return edgeRouteVersionMaterial{
		Hostname:            binding.Hostname,
		RouteKind:           binding.RouteKind,
		AppID:               binding.AppID,
		TenantID:            binding.TenantID,
		RuntimeID:           binding.RuntimeID,
		EdgeGroupID:         binding.EdgeGroupID,
		FallbackEdgeGroupID: binding.FallbackEdgeGroupID,
		RoutePolicy:         binding.RoutePolicy,
		UpstreamKind:        binding.UpstreamKind,
		UpstreamURL:         binding.UpstreamURL,
		ServicePort:         binding.ServicePort,
		TLSPolicy:           binding.TLSPolicy,
		Streaming:           binding.Streaming,
		Status:              binding.Status,
		StatusReason:        binding.StatusReason,
	}
}

func edgeRouteBundleETag(version string) string {
	return strconv.Quote(strings.TrimSpace(version))
}

func edgeRouteBundleETagMatches(headerValue, version string) bool {
	version = strings.TrimSpace(version)
	if version == "" {
		return false
	}
	for _, candidate := range strings.Split(headerValue, ",") {
		candidate = strings.TrimSpace(candidate)
		candidate = strings.TrimPrefix(candidate, "W/")
		if unquoted, err := strconv.Unquote(candidate); err == nil {
			candidate = unquoted
		}
		if candidate == version {
			return true
		}
	}
	return false
}
