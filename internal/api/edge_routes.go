package api

import (
	"context"
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
	authContext, ok := s.authorizeEdgeRequest(w, r)
	if !ok {
		return
	}

	options := edgeRouteBundleOptions{
		EdgeID:      strings.TrimSpace(r.URL.Query().Get("edge_id")),
		EdgeGroupID: strings.TrimSpace(r.URL.Query().Get("edge_group_id")),
	}
	if err := authContext.constrain(&options.EdgeID, &options.EdgeGroupID); err != nil {
		httpx.WriteError(w, http.StatusForbidden, err.Error())
		return
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
	policies, err := s.store.ListEdgeRoutePolicies()
	if err != nil {
		return model.EdgeRouteBundle{}, err
	}
	healthyEdgeGroups, err := s.edgeRouteHealthyEdgeGroups()
	if err != nil {
		return model.EdgeRouteBundle{}, err
	}

	runtimeByID := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		runtimeByID[strings.TrimSpace(runtimeObj.ID)] = runtimeObj
	}
	runtimeNodeLabelsByID := s.edgeRouteRuntimeNodeLabels(r.Context())
	appByID := make(map[string]model.App, len(apps))
	for _, app := range apps {
		app = s.overlayManagedAppStatusCached(app)
		appByID[strings.TrimSpace(app.ID)] = app
	}
	policyByHostname := edgeRoutePolicyByHostname(policies)

	routes := make([]model.EdgeRouteBinding, 0, len(apps)+len(domains))
	for _, app := range appByID {
		if app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "" {
			continue
		}
		binding := s.deriveEdgeRouteBinding(r, app, strings.TrimSpace(app.Route.Hostname), model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, app.CreatedAt, app.UpdatedAt, runtimeByID, runtimeNodeLabelsByID)
		binding = applyEdgeRoutePolicy(binding, policyByHostname, healthyEdgeGroups)
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
		binding := s.deriveEdgeRouteBinding(r, app, hostname, model.EdgeRouteKindCustomDomain, model.EdgeRouteTLSPolicyCustomDomain, domain.CreatedAt, domain.UpdatedAt, runtimeByID, runtimeNodeLabelsByID)
		binding = applyEdgeRoutePolicy(binding, policyByHostname, healthyEdgeGroups)
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

func (s *Server) deriveEdgeRouteBinding(r *http.Request, app model.App, hostname, routeKind, tlsPolicy string, createdAt, updatedAt time.Time, runtimeByID map[string]model.Runtime, runtimeNodeLabelsByID map[string]map[string]string) model.EdgeRouteBinding {
	runtimeID := appProxyRuntimeID(app)
	runtimeObj, runtimeFound := runtimeByID[runtimeID]
	edgeGroupID := derivedEdgeGroupIDForRuntime(runtimeObj, runtimeFound, runtimeNodeLabelsByID[runtimeID])
	fallbackEdgeGroupID := ""
	if edgeGroupID != defaultEdgeGroupID {
		fallbackEdgeGroupID = defaultEdgeGroupID
	}
	status, reason := edgeRouteStatus(app, runtimeID, runtimeFound)
	servicePort := edgeServicePortForApp(app)
	upstream := s.edgeRouteUpstream(r.Context(), app, runtimeObj, runtimeFound)
	if status == model.EdgeRouteStatusActive && upstream.Status != model.EdgeRouteStatusActive {
		status = upstream.Status
		reason = upstream.StatusReason
	}

	binding := model.EdgeRouteBinding{
		Hostname:            normalizeExternalAppDomain(hostname),
		RouteKind:           routeKind,
		AppID:               app.ID,
		TenantID:            app.TenantID,
		RuntimeID:           runtimeID,
		RuntimeEdgeGroupID:  edgeGroupID,
		EdgeGroupID:         edgeGroupID,
		FallbackEdgeGroupID: fallbackEdgeGroupID,
		RoutePolicy:         model.EdgeRoutePolicyRouteAOnly,
		UpstreamKind:        upstream.Kind,
		UpstreamScope:       upstream.Scope,
		ServicePort:         servicePort,
		TLSPolicy:           tlsPolicy,
		Streaming:           true,
		Status:              status,
		StatusReason:        reason,
		CreatedAt:           createdAt,
		UpdatedAt:           updatedAt,
	}
	if runtimeFound {
		binding.RuntimeType = strings.TrimSpace(runtimeObj.Type)
		binding.RuntimeClusterNode = strings.TrimSpace(runtimeObj.ClusterNodeName)
	}
	if binding.Status == model.EdgeRouteStatusActive {
		binding.UpstreamURL = upstream.URL
	}
	binding.RouteGeneration = edgeRouteGeneration(binding)
	return binding
}

func (s *Server) edgeRouteRuntimeNodeLabels(ctx context.Context) map[string]map[string]string {
	snapshots, err := s.loadClusterNodeInventory(ctx)
	if err != nil {
		if s.log != nil {
			s.log.Printf("edge route bundle continuing without cluster node location labels: %v", err)
		}
		return nil
	}
	out := make(map[string]map[string]string)
	for _, snapshot := range snapshots {
		runtimeID := strings.TrimSpace(snapshot.runtimeID)
		if runtimeID == "" {
			continue
		}
		out[runtimeID] = cloneStringMap(snapshot.labels)
	}
	return out
}

type edgeRouteUpstream struct {
	Kind         string
	Scope        string
	URL          string
	Status       string
	StatusReason string
}

func (s *Server) edgeRouteUpstream(ctx context.Context, app model.App, runtimeObj model.Runtime, runtimeFound bool) edgeRouteUpstream {
	out := edgeRouteUpstream{
		Kind:   model.EdgeRouteUpstreamKindKubernetesService,
		Scope:  model.EdgeRouteUpstreamScopeLocalService,
		Status: model.EdgeRouteStatusActive,
	}
	if !runtimeFound {
		return out
	}
	switch strings.TrimSpace(runtimeObj.Type) {
	case "", model.RuntimeTypeManagedShared, model.RuntimeTypeManagedOwned:
		out.URL = s.serviceURLForApp(ctx, app)
		return out
	case model.RuntimeTypeExternalOwned:
		out.Kind = model.EdgeRouteUpstreamKindMesh
		out.Scope = model.EdgeRouteUpstreamScopeMesh
		out.Status = model.EdgeRouteStatusUnavailable
		out.StatusReason = "external-owned runtime requires mesh upstream"
		return out
	default:
		out.Status = model.EdgeRouteStatusUnavailable
		out.StatusReason = "runtime type is not supported by edge upstream"
		return out
	}
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

func edgeRoutePolicyByHostname(policies []model.EdgeRoutePolicy) map[string]model.EdgeRoutePolicy {
	out := make(map[string]model.EdgeRoutePolicy, len(policies))
	for _, policy := range policies {
		hostname := normalizeExternalAppDomain(policy.Hostname)
		if hostname == "" {
			continue
		}
		out[hostname] = policy
	}
	return out
}

func applyEdgeRoutePolicy(binding model.EdgeRouteBinding, policies map[string]model.EdgeRoutePolicy, healthyEdgeGroups map[string]bool) model.EdgeRouteBinding {
	if len(policies) == 0 {
		binding.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
		binding.RouteGeneration = edgeRouteGeneration(binding)
		return binding
	}
	policy, ok := policies[normalizeExternalAppDomain(binding.Hostname)]
	if !ok || strings.TrimSpace(policy.AppID) != strings.TrimSpace(binding.AppID) {
		binding.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
		binding.RouteGeneration = edgeRouteGeneration(binding)
		return binding
	}
	binding.PolicyEdgeGroupID = strings.TrimSpace(policy.EdgeGroupID)
	binding.RoutePolicy = model.NormalizeEdgeRoutePolicy(policy.RoutePolicy)
	if binding.RoutePolicy == "" {
		binding.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
	}
	if model.EdgeRoutePolicyAllowsTraffic(binding.RoutePolicy) && strings.TrimSpace(policy.EdgeGroupID) != "" {
		policyEdgeGroupID := strings.TrimSpace(policy.EdgeGroupID)
		runtimeEdgeGroupID := strings.TrimSpace(firstNonEmpty(binding.RuntimeEdgeGroupID, binding.EdgeGroupID))
		if !strings.EqualFold(policyEdgeGroupID, runtimeEdgeGroupID) {
			binding.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
			binding.Status = model.EdgeRouteStatusUnavailable
			binding.StatusReason = "edge policy target edge group does not match runtime edge group"
			binding.UpstreamURL = ""
			binding.RouteGeneration = edgeRouteGeneration(binding)
			return binding
		}
		if !healthyEdgeGroups[policyEdgeGroupID] {
			binding.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
			binding.Status = model.EdgeRouteStatusUnavailable
			binding.StatusReason = "edge group has no healthy edge nodes"
			binding.UpstreamURL = ""
			binding.RouteGeneration = edgeRouteGeneration(binding)
			return binding
		}
		binding.EdgeGroupID = policyEdgeGroupID
		binding.RuntimeEdgeGroupID = runtimeEdgeGroupID
		binding.FallbackEdgeGroupID = ""
	} else {
		binding.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
	}
	binding.RouteGeneration = edgeRouteGeneration(binding)
	return binding
}

func (s *Server) edgeRouteHealthyEdgeGroups() (map[string]bool, error) {
	nodes, _, err := s.store.ListEdgeNodes("")
	if err != nil {
		return nil, err
	}
	healthy := make(map[string]bool)
	for _, node := range nodes {
		groupID := strings.TrimSpace(node.EdgeGroupID)
		if groupID == "" {
			continue
		}
		if node.Healthy && !node.Draining && strings.EqualFold(strings.TrimSpace(node.Status), model.EdgeHealthHealthy) {
			healthy[groupID] = true
		} else if _, ok := healthy[groupID]; !ok {
			healthy[groupID] = false
		}
	}
	return healthy, nil
}

func edgeGroupIDFromEdgeID(edgeID string) string {
	edgeID = strings.TrimSpace(edgeID)
	if strings.HasPrefix(edgeID, "edge-group-") {
		return edgeID
	}
	return ""
}

func derivedEdgeGroupIDForRuntime(runtimeObj model.Runtime, runtimeFound bool, nodeLabels map[string]string) string {
	if !runtimeFound {
		return defaultEdgeGroupID
	}
	if edgeGroupID := derivedEdgeGroupIDForLabels(runtimeObj.Labels); edgeGroupID != defaultEdgeGroupID {
		return edgeGroupID
	}
	return derivedEdgeGroupIDForLabels(nodeLabels)
}

func derivedEdgeGroupIDForLabels(labels map[string]string) string {
	if country := firstRuntimeLabelValue(labels, runtimepkg.LocationCountryCodeLabelKey, "country_code", "countryCode"); country != "" {
		if slug := edgeRouteSlug(country); slug != "" {
			return "edge-group-country-" + slug
		}
	}
	if region := firstRuntimeLabelValue(labels, runtimepkg.RegionLabelKey, runtimepkg.LegacyRegionLabelKey, "region"); region != "" {
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
	RuntimeType         string `json:"runtime_type,omitempty"`
	RuntimeEdgeGroupID  string `json:"runtime_edge_group_id,omitempty"`
	RuntimeClusterNode  string `json:"runtime_cluster_node,omitempty"`
	EdgeGroupID         string `json:"edge_group_id"`
	FallbackEdgeGroupID string `json:"fallback_edge_group_id,omitempty"`
	PolicyEdgeGroupID   string `json:"policy_edge_group_id,omitempty"`
	RoutePolicy         string `json:"route_policy"`
	UpstreamKind        string `json:"upstream_kind"`
	UpstreamScope       string `json:"upstream_scope,omitempty"`
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
		RuntimeType:         binding.RuntimeType,
		RuntimeEdgeGroupID:  binding.RuntimeEdgeGroupID,
		RuntimeClusterNode:  binding.RuntimeClusterNode,
		EdgeGroupID:         binding.EdgeGroupID,
		FallbackEdgeGroupID: binding.FallbackEdgeGroupID,
		PolicyEdgeGroupID:   binding.PolicyEdgeGroupID,
		RoutePolicy:         binding.RoutePolicy,
		UpstreamKind:        binding.UpstreamKind,
		UpstreamScope:       binding.UpstreamScope,
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
