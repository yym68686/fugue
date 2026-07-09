package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/releaseflow"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

const defaultEdgeGroupID = "edge-group-default"

const (
	defaultStaticAssetCachePolicyID  = "static-assets-immutable-v1"
	defaultHTMLDocumentCachePolicyID = "html-documents-short-v1"
)

var defaultHTMLDocumentVaryAllowlist = []string{
	"Accept-Encoding",
	"RSC",
	"Next-Router-State-Tree",
	"Next-Router-Prefetch",
	"Next-Router-Segment-Prefetch",
}

const edgeCaddyRestartCooldown = 2 * time.Minute

type edgeLiveServingState struct {
	Serving bool
	Reason  string
}

type edgeRouteBundleOptions struct {
	EdgeID              string
	AuthenticatedEdgeID string
	EdgeGroupID         string
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
	if authContext.Scoped {
		options.AuthenticatedEdgeID = authContext.EdgeID
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

	// Route bundles carry signed validity windows. Re-send unchanged content so
	// edge nodes can refresh valid_until instead of going stale behind a 304.
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
	projectRouteTables, err := s.store.ListProjectRouteTables("", true)
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
	releases, err := s.store.ListAppReleases(model.AppReleaseFilter{PlatformAdmin: true, ActiveOnly: true})
	if err != nil {
		return model.EdgeRouteBundle{}, err
	}
	trafficPolicies, err := s.store.ListAppTrafficPolicies("", true)
	if err != nil {
		return model.EdgeRouteBundle{}, err
	}
	healthyEdgeGroups, healthyEdgeNodeIDsByGroup, expectedNonEmptyEdgeGroups, expectedMinTrafficRoutes, err := s.edgeRouteGroupInventory()
	if err != nil {
		return model.EdgeRouteBundle{}, err
	}
	requestedEdgeGroupID := strings.TrimSpace(options.EdgeGroupID)
	if requestedEdgeGroupID == "" {
		requestedEdgeGroupID = edgeGroupIDFromEdgeID(options.EdgeID)
	}
	if requestedEdgeGroupID != "" && !healthyEdgeGroups[requestedEdgeGroupID] {
		nodes, _, err := s.store.ListEdgeNodes(requestedEdgeGroupID)
		if err != nil {
			return model.EdgeRouteBundle{}, err
		}
		now := time.Now().UTC()
		for _, node := range nodes {
			if edgeNodeRouteBootstrapCapable(node, now) {
				healthyEdgeGroups[requestedEdgeGroupID] = true
				healthyEdgeNodeIDsByGroup[requestedEdgeGroupID] = appendUniqueString(healthyEdgeNodeIDsByGroup[requestedEdgeGroupID], node.ID)
				break
			}
		}
	}

	runtimeByID := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		runtimeByID[strings.TrimSpace(runtimeObj.ID)] = runtimeObj
	}
	runtimeNodeLabelsByID := s.edgeRouteRuntimeNodeLabels(r.Context())
	apps = s.overlayManagedAppStatusesForEdgeRoutesCached(apps, runtimeByID)
	appByID := make(map[string]model.App, len(apps))
	for _, app := range apps {
		appByID[strings.TrimSpace(app.ID)] = app
	}
	appsByProjectID := make(map[string][]model.App)
	for _, app := range apps {
		appsByProjectID[strings.TrimSpace(app.ProjectID)] = append(appsByProjectID[strings.TrimSpace(app.ProjectID)], app)
	}
	domainByHostname := make(map[string]model.AppDomain, len(domains))
	for _, domain := range domains {
		if hostname := normalizeExternalAppDomain(domain.Hostname); hostname != "" {
			domainByHostname[hostname] = domain
		}
	}
	policyByHostname := edgeRoutePolicyByHostname(policies)
	releaseByID := appReleaseByID(releases)
	trafficPolicyByApp := appTrafficPolicyByApp(trafficPolicies)

	now := time.Now().UTC()
	// Route publication is intentionally global unless a hostname explicitly
	// excludes an edge or edge group. Edge group IDs steer DNS, policy, and
	// telemetry, but every non-excluded edge retains host routes so stale or
	// alternate DNS answers do not 404.
	routes := make([]model.EdgeRouteBinding, 0, len(apps)+len(domains)+len(s.platformRoutes))
	explicitlyExcludedRoutes := 0
	for _, app := range appByID {
		if app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "" {
			continue
		}
		binding := s.deriveEdgeRouteBinding(r, app, strings.TrimSpace(app.Route.Hostname), model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, app.CreatedAt, app.UpdatedAt, runtimeByID, runtimeNodeLabelsByID)
		binding = applyEdgeRoutePolicy(binding, policyByHostname, healthyEdgeGroups, healthyEdgeNodeIDsByGroup, now)
		binding = applyAppReleaseTraffic(binding, trafficPolicyByApp, releaseByID)
		for _, platformBinding := range expandDefaultPlatformEdgeBindings(binding, healthyEdgeGroups, healthyEdgeNodeIDsByGroup) {
			if !edgeRouteBindingAllowedForRequest(platformBinding, options) {
				explicitlyExcludedRoutes++
				continue
			}
			routes = append(routes, platformBinding)
		}
	}

	for _, platformRoute := range s.platformRoutes {
		for _, binding := range edgeRouteBindingsForPlatformRoute(platformRoute, healthyEdgeGroups, healthyEdgeNodeIDsByGroup) {
			routes = append(routes, binding)
		}
	}

	tlsAllowlist := make([]model.EdgeTLSAllowlistEntry, 0, len(domains))
	for _, domain := range domains {
		hostname := normalizeExternalAppDomain(domain.Hostname)
		if hostname == "" {
			continue
		}
		app, ok := appByID[strings.TrimSpace(domain.AppID)]
		if !ok {
			continue
		}
		routeKind := model.EdgeRouteKindCustomDomain
		tlsPolicy := model.EdgeRouteTLSPolicyCustomDomain
		switch {
		case s.isPlatformOwnedDomainBinding(hostname):
			routeKind = model.EdgeRouteKindPlatformDomain
			tlsPolicy = model.EdgeRouteTLSPolicyPlatform
		case s.managedEdgeCustomDomain(hostname):
		default:
			continue
		}
		binding := s.deriveEdgeRouteBinding(r, app, hostname, routeKind, tlsPolicy, domain.CreatedAt, domain.UpdatedAt, runtimeByID, runtimeNodeLabelsByID)
		binding = applyEdgeRoutePolicy(binding, policyByHostname, healthyEdgeGroups, healthyEdgeNodeIDsByGroup, now)
		binding = applyCustomDomainReadiness(binding, domain)
		binding = applyAppReleaseTraffic(binding, trafficPolicyByApp, releaseByID)
		addedRoute := false
		for _, expandedBinding := range expandDefaultPlatformEdgeBindings(binding, healthyEdgeGroups, healthyEdgeNodeIDsByGroup) {
			if !edgeRouteBindingAllowedForRequest(expandedBinding, options) {
				explicitlyExcludedRoutes++
				continue
			}
			routes = append(routes, expandedBinding)
			addedRoute = true
		}
		if addedRoute {
			tlsAllowlist = append(tlsAllowlist, model.EdgeTLSAllowlistEntry{
				Hostname:  hostname,
				AppID:     domain.AppID,
				TenantID:  domain.TenantID,
				Status:    domain.Status,
				TLSStatus: domain.TLSStatus,
			})
		}
	}

	for _, table := range projectRouteTables {
		bindings := store.TryCompileProjectRouteTableBindings(table, appsByProjectID[strings.TrimSpace(table.ProjectID)])
		for _, routeBinding := range bindings {
			app, ok := appByID[strings.TrimSpace(routeBinding.AppID)]
			if !ok {
				continue
			}
			hostname := normalizeExternalAppDomain(routeBinding.Hostname)
			if hostname == "" {
				continue
			}
			routeKind, tlsPolicy, domain, ok := s.projectRouteEdgePolicy(hostname, domainByHostname)
			if !ok {
				continue
			}
			binding := s.deriveEdgeRouteBindingForRoute(r, app, hostname, routeBinding.PathPrefix, routeBinding.ServicePort, routeKind, tlsPolicy, table.CreatedAt, table.UpdatedAt, runtimeByID, runtimeNodeLabelsByID)
			binding = applyEdgeRoutePolicy(binding, policyByHostname, healthyEdgeGroups, healthyEdgeNodeIDsByGroup, now)
			if domain != nil {
				binding = applyCustomDomainReadiness(binding, *domain)
			}
			binding = applyAppReleaseTraffic(binding, trafficPolicyByApp, releaseByID)
			for _, expandedBinding := range expandDefaultPlatformEdgeBindings(binding, healthyEdgeGroups, healthyEdgeNodeIDsByGroup) {
				if !edgeRouteBindingAllowedForRequest(expandedBinding, options) {
					explicitlyExcludedRoutes++
					continue
				}
				routes = append(routes, expandedBinding)
			}
		}
	}
	routes = dedupeEdgeRouteBindings(routes)

	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Hostname != routes[j].Hostname {
			return routes[i].Hostname < routes[j].Hostname
		}
		if routes[i].PathPrefix != routes[j].PathPrefix {
			return routes[i].PathPrefix < routes[j].PathPrefix
		}
		if routes[i].RouteKind != routes[j].RouteKind {
			return routes[i].RouteKind < routes[j].RouteKind
		}
		return routes[i].EdgeGroupID < routes[j].EdgeGroupID
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
	if edgeRouteBundleUsesCachePolicies(routes) {
		bundle.CachePolicies = defaultEdgeCachePolicies()
	}
	bundle.Version = edgeRouteBundleVersion(bundle)
	bundle.Generation = bundle.Version
	if err := validateEdgeRouteBundleForPublish(bundle, edgeRouteBundleInvariantInput{
		Apps:                       apps,
		Domains:                    domains,
		PlatformRoutes:             s.platformRoutes,
		HealthyEdgeGroups:          healthyEdgeGroups,
		ExpectedNonEmptyEdgeGroups: expectedNonEmptyEdgeGroups,
		ExpectedMinTrafficRoutes:   expectedMinTrafficRoutes,
		ExplicitlyExcludedRoutes:   explicitlyExcludedRoutes,
		Options:                    options,
	}); err != nil {
		return model.EdgeRouteBundle{}, err
	}
	bundle = signEdgeRouteBundle(bundle, s.bundleKeyring(), s.discoveryBundleTTL())
	return bundle, nil
}

func (s *Server) deriveEdgeRouteBinding(r *http.Request, app model.App, hostname, routeKind, tlsPolicy string, createdAt, updatedAt time.Time, runtimeByID map[string]model.Runtime, runtimeNodeLabelsByID map[string]map[string]string) model.EdgeRouteBinding {
	pathPrefix := "/"
	if routeKind == model.EdgeRouteKindPlatform && app.Route != nil {
		pathPrefix = model.NormalizeAppRoutePathPrefix(app.Route.PathPrefix)
	}
	return s.deriveEdgeRouteBindingForRoute(r, app, hostname, pathPrefix, 0, routeKind, tlsPolicy, createdAt, updatedAt, runtimeByID, runtimeNodeLabelsByID)
}

func (s *Server) deriveEdgeRouteBindingForRoute(r *http.Request, app model.App, hostname, pathPrefix string, servicePort int, routeKind, tlsPolicy string, createdAt, updatedAt time.Time, runtimeByID map[string]model.Runtime, runtimeNodeLabelsByID map[string]map[string]string) model.EdgeRouteBinding {
	runtimeID := appProxyRuntimeID(app)
	runtimeObj, runtimeFound := runtimeByID[runtimeID]
	edgeGroupID := derivedEdgeGroupIDForRuntime(runtimeObj, runtimeFound, runtimeNodeLabelsByID[runtimeID])
	fallbackEdgeGroupID := ""
	if edgeGroupID != defaultEdgeGroupID {
		fallbackEdgeGroupID = defaultEdgeGroupID
	}
	status, reason := edgeRouteStatus(app, runtimeID, runtimeFound)
	if servicePort <= 0 {
		servicePort = edgeServicePortForApp(app)
	}
	upstream := s.edgeRouteUpstream(r.Context(), app, runtimeObj, runtimeFound)
	if status == model.EdgeRouteStatusActive && upstream.Status != model.EdgeRouteStatusActive {
		status = upstream.Status
		reason = upstream.StatusReason
	}

	binding := model.EdgeRouteBinding{
		Hostname:             normalizeExternalAppDomain(hostname),
		PathPrefix:           model.NormalizeAppRoutePathPrefix(pathPrefix),
		RouteKind:            routeKind,
		AppID:                app.ID,
		TenantID:             app.TenantID,
		RuntimeID:            runtimeID,
		RuntimeEdgeGroupID:   edgeGroupID,
		RuntimeEdgeGroup:     edgeGroupID,
		EdgeGroupID:          edgeGroupID,
		SelectedEdgeGroup:    edgeGroupID,
		FallbackEdgeGroupID:  fallbackEdgeGroupID,
		RoutePolicy:          model.EdgeRoutePolicyRouteAOnly,
		UpstreamKind:         upstream.Kind,
		UpstreamScope:        upstream.Scope,
		ServicePort:          servicePort,
		TLSPolicy:            tlsPolicy,
		CachePolicyID:        edgeRouteCachePolicyIDForKind(routeKind),
		DeploymentGeneration: edgeRouteDeploymentGeneration(app),
		Streaming:            true,
		Status:               status,
		StatusReason:         reason,
		CreatedAt:            createdAt,
		UpdatedAt:            updatedAt,
	}
	if binding.CachePolicyID != "" {
		binding.CacheNamespace = edgeRouteCacheNamespace(app.ID, binding.DeploymentGeneration)
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

func (s *Server) projectRouteEdgePolicy(hostname string, domainByHostname map[string]model.AppDomain) (string, string, *model.AppDomain, bool) {
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" {
		return "", "", nil, false
	}
	if s.projectRouteHostnameUsesPlatformTLS(hostname) {
		if s.isPlatformOwnedDomainBinding(hostname) {
			return model.EdgeRouteKindPlatformDomain, model.EdgeRouteTLSPolicyPlatform, nil, true
		}
		return model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, nil, true
	}
	domain, ok := domainByHostname[hostname]
	if !ok || !s.managedEdgeCustomDomain(hostname) {
		return "", "", nil, false
	}
	return model.EdgeRouteKindCustomDomain, model.EdgeRouteTLSPolicyCustomDomain, &domain, true
}

func dedupeEdgeRouteBindings(routes []model.EdgeRouteBinding) []model.EdgeRouteBinding {
	if len(routes) < 2 {
		return routes
	}
	indexByKey := make(map[string]int, len(routes))
	out := make([]model.EdgeRouteBinding, 0, len(routes))
	for _, route := range routes {
		key := normalizeExternalAppDomain(route.Hostname) + "\x00" + model.NormalizeAppRoutePathPrefix(route.PathPrefix) + "\x00" + strings.TrimSpace(route.EdgeGroupID)
		if index, exists := indexByKey[key]; exists {
			out[index] = route
			continue
		}
		indexByKey[key] = len(out)
		out = append(out, route)
	}
	return out
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
	case appUsesKnownNonHTTPRouteProtocol(app):
		return model.EdgeRouteStatusUnavailable, "app source exposes a non-HTTP service protocol"
	default:
		return model.EdgeRouteStatusActive, ""
	}
}

func appUsesKnownNonHTTPRouteProtocol(app model.App) bool {
	if app.Source != nil && sourceimport.ImageUsesNonHTTPServiceProtocol(app.Source.ImageRef) {
		return true
	}
	origin := model.AppOriginSource(app)
	return origin != nil && sourceimport.ImageUsesNonHTTPServiceProtocol(origin.ImageRef)
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

func appReleaseByID(releases []model.AppRelease) map[string]model.AppRelease {
	return releaseflow.AppReleaseByID(releases)
}

func appTrafficPolicyByApp(policies []model.AppTrafficPolicy) map[string]model.AppTrafficPolicy {
	return releaseflow.AppTrafficPolicyByApp(policies)
}

func applyAppReleaseTraffic(binding model.EdgeRouteBinding, policies map[string]model.AppTrafficPolicy, releases map[string]model.AppRelease) model.EdgeRouteBinding {
	return releaseflow.ApplyAppReleaseTraffic(binding, policies, releases)
}

func appReleaseCanReceiveEdgeTraffic(release model.AppRelease) bool {
	return releaseflow.AppReleaseCanReceiveEdgeTraffic(release)
}

func applyEdgeRoutePolicy(binding model.EdgeRouteBinding, policies map[string]model.EdgeRoutePolicy, healthyEdgeGroups map[string]bool, healthyEdgeNodeIDsByGroup map[string][]string, now time.Time) model.EdgeRouteBinding {
	runtimeEdgeGroupID := strings.TrimSpace(firstNonEmpty(binding.RuntimeEdgeGroupID, binding.EdgeGroupID))
	policy, ok := policies[normalizeExternalAppDomain(binding.Hostname)]
	policyMatches := ok && edgeRoutePolicyMatchesBinding(policy, binding)
	exclusions := edgeRoutePolicyActiveExclusions(policy, now)
	if !policyMatches {
		exclusions = edgeRouteExclusions{}
	}
	if policyMatches && !exclusions.Empty() {
		binding.ExcludedEdgeIDs = exclusions.EdgeIDs
		binding.ExcludedEdgeGroupIDs = exclusions.EdgeGroupIDs
		binding.ExclusionReason = strings.TrimSpace(policy.ExclusionReason)
		binding.ExclusionExpiresAt = policy.ExclusionExpiresAt
	}
	effectiveHealthyEdgeGroups := edgeRouteHealthyGroupsAfterExclusions(healthyEdgeGroups, healthyEdgeNodeIDsByGroup, exclusions)
	healthyEdgeNodeCount := edgeRouteHealthyNodeCountAfterExclusions(healthyEdgeNodeIDsByGroup, exclusions)
	if policyMatches && strings.TrimSpace(policy.EdgeGroupID) != "" {
		healthyEdgeNodeCount = edgeRouteHealthyNodeCountForGroupsAfterExclusions(healthyEdgeNodeIDsByGroup, []string{policy.EdgeGroupID}, exclusions)
	}
	selection := selectEdgeGroupForRoute(runtimeEdgeGroupID, effectiveHealthyEdgeGroups)
	servingEdgeGroupID := selection.EdgeGroupID
	minHealthyEdgeNodes := defaultMinHealthyEdgeNodesForBinding(binding)
	if policyMatches && policy.MinHealthyEdgeNodes > 0 {
		minHealthyEdgeNodes = policy.MinHealthyEdgeNodes
	}
	binding.MinHealthyEdgeNodes = minHealthyEdgeNodes
	binding.HealthyEdgeNodeCount = healthyEdgeNodeCount
	if model.EdgeRoutePolicyAllowsTraffic(binding.RoutePolicy) || policyMatches || isDefaultEdgeRouteKind(binding.RouteKind) {
		binding.EdgeRedundancyStatus = "ok"
		if minHealthyEdgeNodes > 0 && healthyEdgeNodeCount < minHealthyEdgeNodes {
			binding.EdgeRedundancyStatus = "at_risk"
			binding.EdgeRedundancyReason = fmt.Sprintf("healthy edge nodes %d below minimum %d", healthyEdgeNodeCount, minHealthyEdgeNodes)
		}
	}
	binding.RuntimeEdgeGroupID = runtimeEdgeGroupID
	binding.RuntimeEdgeGroup = runtimeEdgeGroupID
	binding.SelectedEdgeGroup = servingEdgeGroupID
	binding.SelectionReason = selection.Reason
	binding.FallbackReason = selection.FallbackReason
	if !policyMatches {
		if isDefaultEdgeRouteKind(binding.RouteKind) {
			binding.RoutePolicy = model.EdgeRoutePolicyEnabled
			if servingEdgeGroupID != "" {
				binding.EdgeGroupID = servingEdgeGroupID
				binding.FallbackEdgeGroupID = ""
				if selection.FallbackReason != "" && runtimeEdgeGroupID != "" && servingEdgeGroupID != runtimeEdgeGroupID {
					binding.FallbackEdgeGroupID = servingEdgeGroupID
				}
			} else {
				binding.Status = model.EdgeRouteStatusUnavailable
				binding.StatusReason = "edge group has no healthy edge nodes"
				binding.UpstreamURL = ""
				binding.FallbackEdgeGroupID = ""
			}
		} else {
			binding.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
		}
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
		if servingEdgeGroupID == "" {
			binding.Status = model.EdgeRouteStatusUnavailable
			binding.StatusReason = "edge group has no healthy non-excluded edge nodes"
			binding.FallbackReason = firstNonEmpty(selection.FallbackReason, "no healthy edge group")
			binding.UpstreamURL = ""
			binding.RouteGeneration = edgeRouteGeneration(binding)
			return binding
		}
		if !effectiveHealthyEdgeGroups[policyEdgeGroupID] {
			binding.Status = model.EdgeRouteStatusUnavailable
			if exclusions.ExcludesEdgeGroup(policyEdgeGroupID) {
				binding.StatusReason = "policy edge group is excluded for this hostname"
			} else {
				binding.StatusReason = "edge group has no healthy non-excluded edge nodes"
			}
			binding.SelectedEdgeGroup = servingEdgeGroupID
			binding.FallbackReason = firstNonEmpty(selection.FallbackReason, "policy edge group unhealthy")
			binding.UpstreamURL = ""
			binding.RouteGeneration = edgeRouteGeneration(binding)
			return binding
		}
		binding.EdgeGroupID = policyEdgeGroupID
		binding.SelectedEdgeGroup = policyEdgeGroupID
		binding.SelectionReason = "policy edge group is healthy"
		if runtimeEdgeGroupID != "" && !strings.EqualFold(policyEdgeGroupID, runtimeEdgeGroupID) {
			binding.FallbackReason = "policy edge group overrides runtime locality"
		}
		binding.FallbackEdgeGroupID = ""
	} else if model.EdgeRoutePolicyAllowsTraffic(binding.RoutePolicy) {
		if servingEdgeGroupID != "" {
			binding.EdgeGroupID = servingEdgeGroupID
			binding.FallbackEdgeGroupID = ""
			if selection.FallbackReason != "" && runtimeEdgeGroupID != "" && servingEdgeGroupID != runtimeEdgeGroupID {
				binding.FallbackEdgeGroupID = servingEdgeGroupID
			}
		} else {
			binding.Status = model.EdgeRouteStatusUnavailable
			binding.StatusReason = "edge group has no healthy non-excluded edge nodes"
			binding.UpstreamURL = ""
			binding.FallbackEdgeGroupID = ""
		}
	} else {
		binding.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
	}
	binding.RouteGeneration = edgeRouteGeneration(binding)
	return binding
}

func edgeRoutePolicyMatchesBinding(policy model.EdgeRoutePolicy, binding model.EdgeRouteBinding) bool {
	if strings.TrimSpace(policy.AppID) == strings.TrimSpace(binding.AppID) {
		return true
	}
	policyTenantID := strings.TrimSpace(policy.TenantID)
	bindingTenantID := strings.TrimSpace(binding.TenantID)
	return policyTenantID != "" && bindingTenantID != "" && policyTenantID == bindingTenantID
}

func expandDefaultPlatformEdgeBindings(binding model.EdgeRouteBinding, healthyEdgeGroups map[string]bool, healthyEdgeNodeIDsByGroup map[string][]string) []model.EdgeRouteBinding {
	if !isDefaultEdgeRouteKind(binding.RouteKind) ||
		binding.RoutePolicy != model.EdgeRoutePolicyEnabled ||
		strings.TrimSpace(binding.PolicyEdgeGroupID) != "" {
		return []model.EdgeRouteBinding{binding}
	}
	groups := sortedHealthyEdgeGroups(edgeRouteHealthyGroupsForBinding(healthyEdgeGroups, healthyEdgeNodeIDsByGroup, binding))
	if len(groups) == 0 {
		return []model.EdgeRouteBinding{binding}
	}
	out := make([]model.EdgeRouteBinding, 0, len(groups))
	for _, edgeGroupID := range groups {
		candidate := binding
		candidate.EdgeGroupID = edgeGroupID
		candidate.FallbackEdgeGroupID = ""
		candidate.RouteGeneration = edgeRouteGeneration(candidate)
		out = append(out, candidate)
	}
	return out
}

type edgeRouteExclusions struct {
	EdgeIDs      []string
	EdgeGroupIDs []string
	edgeIDSet    map[string]struct{}
	groupIDSet   map[string]struct{}
}

func edgeRoutePolicyActiveExclusions(policy model.EdgeRoutePolicy, now time.Time) edgeRouteExclusions {
	if policy.ExclusionExpiresAt != nil && !policy.ExclusionExpiresAt.After(now) {
		return edgeRouteExclusions{}
	}
	return newEdgeRouteExclusions(policy.ExcludedEdgeIDs, policy.ExcludedEdgeGroupIDs)
}

func edgeRouteExclusionsFromBinding(binding model.EdgeRouteBinding) edgeRouteExclusions {
	return newEdgeRouteExclusions(binding.ExcludedEdgeIDs, binding.ExcludedEdgeGroupIDs)
}

func newEdgeRouteExclusions(edgeIDs, edgeGroupIDs []string) edgeRouteExclusions {
	exclusions := edgeRouteExclusions{
		EdgeIDs:      normalizeEdgeRouteExclusionIDs(edgeIDs),
		EdgeGroupIDs: normalizeEdgeRouteExclusionIDs(edgeGroupIDs),
	}
	if len(exclusions.EdgeIDs) > 0 {
		exclusions.edgeIDSet = make(map[string]struct{}, len(exclusions.EdgeIDs))
		for _, edgeID := range exclusions.EdgeIDs {
			exclusions.edgeIDSet[edgeID] = struct{}{}
		}
	}
	if len(exclusions.EdgeGroupIDs) > 0 {
		exclusions.groupIDSet = make(map[string]struct{}, len(exclusions.EdgeGroupIDs))
		for _, edgeGroupID := range exclusions.EdgeGroupIDs {
			exclusions.groupIDSet[edgeGroupID] = struct{}{}
		}
	}
	return exclusions
}

func normalizeEdgeRouteExclusionIDs(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(strings.ToLower(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil
	}
	return out
}

func (exclusions edgeRouteExclusions) Empty() bool {
	return len(exclusions.EdgeIDs) == 0 && len(exclusions.EdgeGroupIDs) == 0
}

func (exclusions edgeRouteExclusions) ExcludesEdge(edgeID string) bool {
	edgeID = strings.TrimSpace(strings.ToLower(edgeID))
	if edgeID == "" || len(exclusions.edgeIDSet) == 0 {
		return false
	}
	_, ok := exclusions.edgeIDSet[edgeID]
	return ok
}

func (exclusions edgeRouteExclusions) ExcludesEdgeGroup(edgeGroupID string) bool {
	edgeGroupID = strings.TrimSpace(strings.ToLower(edgeGroupID))
	if edgeGroupID == "" || len(exclusions.groupIDSet) == 0 {
		return false
	}
	_, ok := exclusions.groupIDSet[edgeGroupID]
	return ok
}

func edgeRouteHealthyGroupsForBinding(healthyEdgeGroups map[string]bool, healthyEdgeNodeIDsByGroup map[string][]string, binding model.EdgeRouteBinding) map[string]bool {
	return edgeRouteHealthyGroupsAfterExclusions(healthyEdgeGroups, healthyEdgeNodeIDsByGroup, edgeRouteExclusionsFromBinding(binding))
}

func edgeRouteHealthyGroupsAfterExclusions(healthyEdgeGroups map[string]bool, healthyEdgeNodeIDsByGroup map[string][]string, exclusions edgeRouteExclusions) map[string]bool {
	if exclusions.Empty() {
		return healthyEdgeGroups
	}
	out := make(map[string]bool, len(healthyEdgeGroups))
	for edgeGroupID, healthy := range healthyEdgeGroups {
		edgeGroupID = strings.TrimSpace(edgeGroupID)
		if !healthy || edgeGroupID == "" || exclusions.ExcludesEdgeGroup(edgeGroupID) {
			out[edgeGroupID] = false
			continue
		}
		nodes := healthyEdgeNodeIDsByGroup[edgeGroupID]
		if len(nodes) > 0 {
			hasAllowedNode := false
			for _, edgeID := range nodes {
				if !exclusions.ExcludesEdge(edgeID) {
					hasAllowedNode = true
					break
				}
			}
			if !hasAllowedNode {
				out[edgeGroupID] = false
				continue
			}
		}
		out[edgeGroupID] = true
	}
	return out
}

func edgeRouteHealthyNodeCountAfterExclusions(healthyEdgeNodeIDsByGroup map[string][]string, exclusions edgeRouteExclusions) int {
	return edgeRouteHealthyNodeCountForGroupsAfterExclusions(healthyEdgeNodeIDsByGroup, nil, exclusions)
}

func edgeRouteHealthyNodeCountForGroupsAfterExclusions(healthyEdgeNodeIDsByGroup map[string][]string, allowedGroups []string, exclusions edgeRouteExclusions) int {
	if len(healthyEdgeNodeIDsByGroup) == 0 {
		return 0
	}
	allowed := map[string]struct{}{}
	for _, groupID := range allowedGroups {
		groupID = strings.TrimSpace(groupID)
		if groupID != "" {
			allowed[groupID] = struct{}{}
		}
	}
	seen := map[string]struct{}{}
	for edgeGroupID, nodes := range healthyEdgeNodeIDsByGroup {
		if len(allowed) > 0 {
			if _, ok := allowed[strings.TrimSpace(edgeGroupID)]; !ok {
				continue
			}
		}
		if exclusions.ExcludesEdgeGroup(edgeGroupID) {
			continue
		}
		for _, edgeID := range nodes {
			edgeID = strings.TrimSpace(edgeID)
			if edgeID == "" || exclusions.ExcludesEdge(edgeID) {
				continue
			}
			seen[edgeID] = struct{}{}
		}
	}
	return len(seen)
}

func edgeRouteBindingAllowedForRequest(binding model.EdgeRouteBinding, options edgeRouteBundleOptions) bool {
	exclusions := edgeRouteExclusionsFromBinding(binding)
	if exclusions.Empty() {
		return true
	}
	edgeID := strings.TrimSpace(options.AuthenticatedEdgeID)
	if edgeID == "" {
		edgeID = strings.TrimSpace(options.EdgeID)
	}
	if exclusions.ExcludesEdge(edgeID) {
		return false
	}
	edgeGroupID := strings.TrimSpace(options.EdgeGroupID)
	if edgeGroupID == "" {
		edgeGroupID = edgeGroupIDFromEdgeID(options.EdgeID)
	}
	if exclusions.ExcludesEdgeGroup(edgeGroupID) {
		return false
	}
	return true
}

func isDefaultEdgeRouteKind(routeKind string) bool {
	return routeKind == model.EdgeRouteKindPlatform ||
		routeKind == model.EdgeRouteKindPlatformDomain ||
		routeKind == model.EdgeRouteKindCustomDomain
}

func defaultMinHealthyEdgeNodesForPolicyHostname(s *Server, hostname string) int {
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" {
		return 1
	}
	if s != nil {
		for _, route := range s.platformRoutes {
			if normalizeExternalAppDomain(route.Hostname) != hostname {
				continue
			}
			if platformRouteRequiresRedundantEdges(route.Kind) {
				return 2
			}
		}
		if normalizeExternalAppDomain(s.apiPublicDomain) == hostname {
			return 2
		}
	}
	return 1
}

func defaultMinHealthyEdgeNodesForBinding(binding model.EdgeRouteBinding) int {
	if platformRouteRequiresRedundantEdges(binding.RouteKind) {
		return 2
	}
	return 1
}

func platformRouteRequiresRedundantEdges(routeKind string) bool {
	switch strings.TrimSpace(routeKind) {
	case model.EdgeRouteKindControlPlaneAPI, model.EdgeRouteKindPlatformRoute:
		return true
	default:
		return false
	}
}

func applyCustomDomainReadiness(binding model.EdgeRouteBinding, domain model.AppDomain) model.EdgeRouteBinding {
	if binding.RouteKind != model.EdgeRouteKindCustomDomain {
		return binding
	}
	if domain.Status == model.AppDomainStatusVerified && domain.DNSStatus == model.AppDomainDNSStatusReady && domain.TLSStatus == model.AppDomainTLSStatusReady {
		return binding
	}
	binding.RoutePolicy = model.EdgeRoutePolicyEnabled
	binding.Status = model.EdgeRouteStatusUnavailable
	binding.StatusReason = "custom domain ownership, DNS, or TLS verification is pending"
	binding.UpstreamURL = ""
	binding.RouteGeneration = edgeRouteGeneration(binding)
	return binding
}

func edgeRouteBindingsForPlatformRoute(route model.PlatformRoute, healthyEdgeGroups map[string]bool, healthyEdgeNodeIDsByGroup map[string][]string) []model.EdgeRouteBinding {
	base := model.EdgeRouteBinding{
		Hostname:      route.Hostname,
		PathPrefix:    "/",
		RouteKind:     route.Kind,
		RoutePolicy:   route.RoutePolicy,
		UpstreamKind:  route.UpstreamKind,
		UpstreamScope: route.UpstreamScope,
		UpstreamURL:   route.UpstreamURL,
		TLSPolicy:     route.TLSPolicy,
		Streaming:     true,
		Status:        route.Status,
		StatusReason:  route.StatusReason,
	}
	minHealthyEdgeNodes := defaultMinHealthyEdgeNodesForBinding(base)
	healthyEdgeNodeCount := edgeRouteHealthyNodeCountAfterExclusions(healthyEdgeNodeIDsByGroup, edgeRouteExclusions{})
	if base.Status != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(base.RoutePolicy) {
		base.UpstreamURL = ""
	}

	switch route.EdgeGroupMode {
	case model.PlatformRouteEdgeGroupModePinned:
		base.EdgeGroupID = strings.TrimSpace(route.EdgeGroupID)
		healthyEdgeNodeCount = edgeRouteHealthyNodeCountForGroupsAfterExclusions(healthyEdgeNodeIDsByGroup, []string{base.EdgeGroupID}, edgeRouteExclusions{})
		base = applyEdgeRouteRedundancyStatus(base, healthyEdgeNodeCount, minHealthyEdgeNodes)
		if base.EdgeGroupID == "" || !healthyEdgeGroups[base.EdgeGroupID] {
			base.Status = model.EdgeRouteStatusUnavailable
			base.StatusReason = "edge group has no healthy edge nodes"
			base.UpstreamURL = ""
		}
		base.RouteGeneration = edgeRouteGeneration(base)
		return []model.EdgeRouteBinding{base}
	default:
		groups := sortedHealthyEdgeGroups(healthyEdgeGroups)
		if len(groups) == 0 {
			base.Status = model.EdgeRouteStatusUnavailable
			base.StatusReason = "no healthy edge groups"
			base.UpstreamURL = ""
			base.RouteGeneration = edgeRouteGeneration(base)
			return []model.EdgeRouteBinding{base}
		}
		out := make([]model.EdgeRouteBinding, 0, len(groups))
		for _, edgeGroupID := range groups {
			candidate := base
			candidate.EdgeGroupID = edgeGroupID
			candidate = applyEdgeRouteRedundancyStatus(candidate, healthyEdgeNodeCount, minHealthyEdgeNodes)
			candidate.RouteGeneration = edgeRouteGeneration(candidate)
			out = append(out, candidate)
		}
		return out
	}
}

func applyEdgeRouteRedundancyStatus(binding model.EdgeRouteBinding, healthyEdgeNodeCount, minHealthyEdgeNodes int) model.EdgeRouteBinding {
	binding.MinHealthyEdgeNodes = minHealthyEdgeNodes
	binding.HealthyEdgeNodeCount = healthyEdgeNodeCount
	if minHealthyEdgeNodes <= 0 {
		binding.EdgeRedundancyStatus = ""
		binding.EdgeRedundancyReason = ""
		return binding
	}
	binding.EdgeRedundancyStatus = "ok"
	binding.EdgeRedundancyReason = ""
	if healthyEdgeNodeCount < minHealthyEdgeNodes {
		binding.EdgeRedundancyStatus = "at_risk"
		binding.EdgeRedundancyReason = fmt.Sprintf("healthy edge nodes %d below minimum %d", healthyEdgeNodeCount, minHealthyEdgeNodes)
	}
	return binding
}

type edgeGroupSelection struct {
	EdgeGroupID    string
	Reason         string
	FallbackReason string
}

func selectEdgeGroupForRoute(runtimeEdgeGroupID string, healthyEdgeGroups map[string]bool) edgeGroupSelection {
	runtimeEdgeGroupID = strings.TrimSpace(runtimeEdgeGroupID)
	if runtimeEdgeGroupID != "" && healthyEdgeGroups[runtimeEdgeGroupID] {
		return edgeGroupSelection{
			EdgeGroupID: runtimeEdgeGroupID,
			Reason:      "runtime edge group is healthy",
		}
	}
	candidates := sortedHealthyEdgeGroups(healthyEdgeGroups)
	if len(candidates) == 0 {
		return edgeGroupSelection{
			Reason:         "no healthy edge group",
			FallbackReason: "no healthy edge group",
		}
	}
	fallbackReason := ""
	if runtimeEdgeGroupID != "" {
		fallbackReason = "runtime edge group is not healthy"
	}
	return edgeGroupSelection{
		EdgeGroupID:    candidates[0],
		Reason:         "first healthy edge group by stable policy order",
		FallbackReason: fallbackReason,
	}
}

func sortedHealthyEdgeGroups(healthyEdgeGroups map[string]bool) []string {
	candidates := make([]string, 0, len(healthyEdgeGroups))
	for edgeGroupID, healthy := range healthyEdgeGroups {
		if healthy {
			candidates = append(candidates, edgeGroupID)
		}
	}
	sort.Strings(candidates)
	return candidates
}

func (s *Server) edgeRouteHealthyEdgeGroups() (map[string]bool, error) {
	healthy, _, _, _, err := s.edgeRouteGroupInventory()
	return healthy, err
}

func (s *Server) edgeRouteHealthyEdgeGroupInventory() (map[string]bool, map[string][]string, error) {
	healthy, healthyNodeIDsByGroup, _, _, err := s.edgeRouteGroupInventory()
	return healthy, healthyNodeIDsByGroup, err
}

func (s *Server) edgeRouteGroupInventory() (map[string]bool, map[string][]string, map[string]bool, map[string]int, error) {
	nodes, _, err := s.store.ListEdgeNodes("")
	if err != nil {
		return nil, nil, nil, nil, err
	}
	healthy := make(map[string]bool)
	healthyNodeIDsByGroup := make(map[string][]string)
	expectedNonEmpty := make(map[string]bool)
	expectedMinTrafficRoutes := make(map[string]int)
	now := time.Now().UTC()
	liveServingByNode := s.edgeLiveServingByNode(context.Background(), now)
	quarantineByNode := s.activeNodeQuarantineByName()
	for _, node := range nodes {
		groupID := strings.TrimSpace(node.EdgeGroupID)
		if groupID == "" {
			continue
		}
		if edgeNodeHasRouteState(node) {
			expectedNonEmpty[groupID] = true
		}
		if node.CaddyRouteCount > expectedMinTrafficRoutes[groupID] && edgeNodeHasRouteState(node) {
			expectedMinTrafficRoutes[groupID] = node.CaddyRouteCount
		}
		if edgeNodeQuarantined(node, quarantineByNode) {
			if _, ok := healthy[groupID]; !ok {
				healthy[groupID] = false
			}
			continue
		}
		if edgeNodeRouteServingCapableWithLive(node, now, liveServingByNode) {
			healthy[groupID] = true
			healthyNodeIDsByGroup[groupID] = appendUniqueString(healthyNodeIDsByGroup[groupID], node.ID)
		} else if edgeNodeHeartbeatFresh(node, now) {
			if _, ok := healthy[groupID]; !ok {
				healthy[groupID] = false
			}
		}
	}
	for groupID := range healthyNodeIDsByGroup {
		sort.Strings(healthyNodeIDsByGroup[groupID])
	}
	return healthy, healthyNodeIDsByGroup, expectedNonEmpty, expectedMinTrafficRoutes, nil
}

func edgeNodeQuarantined(node model.EdgeNode, quarantineByNode map[string]model.NodeDeepHealthResult) bool {
	if len(quarantineByNode) == 0 {
		return false
	}
	for _, key := range []string{node.ID, node.PublicHostname, node.MeshIP} {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if quarantine, ok := quarantineByNode[key]; ok && strings.TrimSpace(quarantine.QuarantineState) != model.NodeQuarantineStateClear {
			return true
		}
	}
	return false
}

func (s *Server) edgeLiveServingByNode(ctx context.Context, now time.Time) map[string]edgeLiveServingState {
	if s == nil {
		return nil
	}
	namespace := strings.TrimSpace(s.controlPlaneNamespace)
	if namespace == "" {
		return nil
	}
	releaseInstance := strings.TrimSpace(s.controlPlaneReleaseInstance)
	cacheKey := namespace + "/" + releaseInstance
	states, err := s.edgeLiveServingCache.do(cacheKey, func() (map[string]edgeLiveServingState, error) {
		clientFactory := s.newClusterNodeClient
		if clientFactory == nil {
			clientFactory = newClusterNodeClient
		}
		client, err := clientFactory()
		if err != nil {
			return nil, err
		}
		defer client.closeIdleConnections()
		pods, err := client.listControlPlaneNamespacePods(ctx, namespace)
		if err != nil {
			return nil, err
		}
		out := make(map[string]edgeLiveServingState)
		for _, pod := range pods {
			if !controlPlanePodMatchesRelease(pod, releaseInstance) || !controlPlanePodIsEdge(pod) {
				continue
			}
			nodeName := strings.TrimSpace(pod.Spec.NodeName)
			if nodeName == "" {
				continue
			}
			state := edgePodLiveServingState(pod, now)
			if existing, ok := out[nodeName]; ok && !existing.Serving {
				continue
			}
			out[nodeName] = state
		}
		return out, nil
	})
	if err != nil {
		if s.log != nil {
			s.log.Printf("edge live serving inventory unavailable: %v", err)
		}
		return nil
	}
	return states
}

func controlPlanePodIsEdge(pod kubePodInfo) bool {
	component := strings.TrimSpace(pod.Metadata.Labels["app.kubernetes.io/component"])
	return component == "edge" || strings.HasPrefix(component, "edge-")
}

func edgePodLiveServingState(pod kubePodInfo, now time.Time) edgeLiveServingState {
	if !strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Running") {
		return edgeLiveServingState{Serving: false, Reason: "pod is not running"}
	}
	caddyExpected := false
	for _, container := range pod.Spec.Containers {
		if strings.TrimSpace(container.Name) == "caddy" {
			caddyExpected = true
			break
		}
	}
	caddySeen := false
	for _, status := range pod.Status.ContainerStatuses {
		if strings.TrimSpace(status.Name) != "caddy" {
			continue
		}
		caddySeen = true
		if status.State.Running == nil || !status.Ready {
			return edgeLiveServingState{Serving: false, Reason: "caddy container is not ready"}
		}
		if terminated := status.LastState.Terminated; terminated != nil && terminated.FinishedAt != nil {
			if now.Sub((*terminated.FinishedAt).UTC()) < edgeCaddyRestartCooldown {
				return edgeLiveServingState{Serving: false, Reason: "caddy container restarted recently"}
			}
		}
	}
	if caddyExpected && !caddySeen {
		return edgeLiveServingState{Serving: false, Reason: "caddy container status is missing"}
	}
	if !caddySeen {
		return edgeLiveServingState{Serving: true}
	}
	return edgeLiveServingState{Serving: true}
}

func edgeNodeRouteServingCapableWithLive(node model.EdgeNode, now time.Time, liveServingByNode map[string]edgeLiveServingState) bool {
	if !edgeNodeRouteServingCapable(node, now) {
		return false
	}
	if len(liveServingByNode) == 0 {
		return true
	}
	state, ok := liveServingByNode[strings.TrimSpace(node.ID)]
	if !ok {
		return true
	}
	return state.Serving
}

func edgeNodeHasRouteState(node model.EdgeNode) bool {
	return node.CaddyRouteCount > 0 ||
		strings.TrimSpace(node.RouteBundleVersion) != "" ||
		strings.TrimSpace(node.ServingGeneration) != "" ||
		strings.TrimSpace(node.LKGGeneration) != ""
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
	if edgeGroupID := firstRuntimeLabelValue(labels, runtimepkg.EdgeGroupIDLabelKey, "edge_group_id", "edgeGroupID"); edgeGroupID != "" {
		if strings.HasPrefix(edgeGroupID, "edge-group-") {
			return edgeGroupID
		}
	}
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
	return releaseflow.EdgeRouteGeneration(binding)
}

type edgeRouteVersionMaterial struct {
	Hostname             string                    `json:"hostname"`
	PathPrefix           string                    `json:"path_prefix,omitempty"`
	RouteKind            string                    `json:"route_kind"`
	AppID                string                    `json:"app_id"`
	TenantID             string                    `json:"tenant_id"`
	RuntimeID            string                    `json:"runtime_id"`
	RuntimeType          string                    `json:"runtime_type,omitempty"`
	RuntimeEdgeGroupID   string                    `json:"runtime_edge_group_id,omitempty"`
	RuntimeClusterNode   string                    `json:"runtime_cluster_node,omitempty"`
	RuntimeEdgeGroup     string                    `json:"runtime_edge_group,omitempty"`
	SelectedEdgeGroup    string                    `json:"selected_edge_group,omitempty"`
	EdgeGroupID          string                    `json:"edge_group_id"`
	FallbackEdgeGroupID  string                    `json:"fallback_edge_group_id,omitempty"`
	PolicyEdgeGroupID    string                    `json:"policy_edge_group_id,omitempty"`
	ExcludedEdgeIDs      []string                  `json:"excluded_edge_ids,omitempty"`
	ExcludedEdgeGroupIDs []string                  `json:"excluded_edge_group_ids,omitempty"`
	ExclusionReason      string                    `json:"exclusion_reason,omitempty"`
	ExclusionExpiresAt   *time.Time                `json:"exclusion_expires_at,omitempty"`
	MinHealthyEdgeNodes  int                       `json:"min_healthy_edge_nodes,omitempty"`
	HealthyEdgeNodeCount int                       `json:"healthy_edge_node_count,omitempty"`
	EdgeRedundancyStatus string                    `json:"edge_redundancy_status,omitempty"`
	EdgeRedundancyReason string                    `json:"edge_redundancy_reason,omitempty"`
	RoutePolicy          string                    `json:"route_policy"`
	SelectionReason      string                    `json:"selection_reason,omitempty"`
	FallbackReason       string                    `json:"fallback_reason,omitempty"`
	UpstreamKind         string                    `json:"upstream_kind"`
	UpstreamScope        string                    `json:"upstream_scope,omitempty"`
	UpstreamURL          string                    `json:"upstream_url,omitempty"`
	Upstreams            []model.EdgeRouteUpstream `json:"upstreams,omitempty"`
	ServicePort          int                       `json:"service_port"`
	TLSPolicy            string                    `json:"tls_policy"`
	CachePolicyID        string                    `json:"cache_policy_id,omitempty"`
	CacheNamespace       string                    `json:"cache_namespace,omitempty"`
	DeploymentGeneration string                    `json:"deployment_generation,omitempty"`
	Streaming            bool                      `json:"streaming"`
	Status               string                    `json:"status"`
	StatusReason         string                    `json:"status_reason,omitempty"`
}

type edgeRouteBundleVersionMaterial struct {
	Routes        []edgeRouteVersionMaterial    `json:"routes"`
	TLSAllowlist  []model.EdgeTLSAllowlistEntry `json:"tls_allowlist"`
	CachePolicies []model.CachePolicy           `json:"cache_policies,omitempty"`
}

func edgeRouteBundleVersion(bundle model.EdgeRouteBundle) string {
	routes := make([]edgeRouteVersionMaterial, len(bundle.Routes))
	for index, route := range bundle.Routes {
		routes[index] = edgeRouteVersionMaterialFromBinding(route)
	}
	material := edgeRouteBundleVersionMaterial{
		Routes:        routes,
		TLSAllowlist:  append([]model.EdgeTLSAllowlistEntry(nil), bundle.TLSAllowlist...),
		CachePolicies: append([]model.CachePolicy(nil), bundle.CachePolicies...),
	}
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return "routegen_" + hex.EncodeToString(sum[:])[:16]
}

func edgeRouteVersionMaterialFromBinding(binding model.EdgeRouteBinding) edgeRouteVersionMaterial {
	return edgeRouteVersionMaterial{
		Hostname:             binding.Hostname,
		PathPrefix:           model.NormalizeAppRoutePathPrefix(binding.PathPrefix),
		RouteKind:            binding.RouteKind,
		AppID:                binding.AppID,
		TenantID:             binding.TenantID,
		RuntimeID:            binding.RuntimeID,
		RuntimeType:          binding.RuntimeType,
		RuntimeEdgeGroupID:   binding.RuntimeEdgeGroupID,
		RuntimeClusterNode:   binding.RuntimeClusterNode,
		RuntimeEdgeGroup:     binding.RuntimeEdgeGroup,
		SelectedEdgeGroup:    binding.SelectedEdgeGroup,
		EdgeGroupID:          binding.EdgeGroupID,
		FallbackEdgeGroupID:  binding.FallbackEdgeGroupID,
		PolicyEdgeGroupID:    binding.PolicyEdgeGroupID,
		ExcludedEdgeIDs:      append([]string(nil), binding.ExcludedEdgeIDs...),
		ExcludedEdgeGroupIDs: append([]string(nil), binding.ExcludedEdgeGroupIDs...),
		ExclusionReason:      binding.ExclusionReason,
		ExclusionExpiresAt:   binding.ExclusionExpiresAt,
		MinHealthyEdgeNodes:  binding.MinHealthyEdgeNodes,
		HealthyEdgeNodeCount: binding.HealthyEdgeNodeCount,
		EdgeRedundancyStatus: binding.EdgeRedundancyStatus,
		EdgeRedundancyReason: binding.EdgeRedundancyReason,
		RoutePolicy:          binding.RoutePolicy,
		SelectionReason:      binding.SelectionReason,
		FallbackReason:       binding.FallbackReason,
		UpstreamKind:         binding.UpstreamKind,
		UpstreamScope:        binding.UpstreamScope,
		UpstreamURL:          binding.UpstreamURL,
		Upstreams:            append([]model.EdgeRouteUpstream(nil), binding.Upstreams...),
		ServicePort:          binding.ServicePort,
		TLSPolicy:            binding.TLSPolicy,
		CachePolicyID:        binding.CachePolicyID,
		CacheNamespace:       binding.CacheNamespace,
		DeploymentGeneration: binding.DeploymentGeneration,
		Streaming:            binding.Streaming,
		Status:               binding.Status,
		StatusReason:         binding.StatusReason,
	}
}

func edgeRouteBundleUsesCachePolicies(routes []model.EdgeRouteBinding) bool {
	for _, route := range routes {
		if strings.TrimSpace(route.CachePolicyID) != "" {
			return true
		}
	}
	return false
}

func edgeRouteCachePolicyIDForKind(routeKind string) string {
	if isDefaultEdgeRouteKind(routeKind) {
		return defaultStaticAssetCachePolicyID
	}
	return ""
}

func edgeRouteDeploymentGeneration(app model.App) string {
	if value := strings.TrimSpace(app.Status.LastOperationID); value != "" {
		return value
	}
	if value := strings.TrimSpace(app.Spec.Image); value != "" {
		sum := sha256.Sum256([]byte(value))
		return "image_" + hex.EncodeToString(sum[:])[:16]
	}
	if app.Status.CurrentReleaseReadyAt != nil && !app.Status.CurrentReleaseReadyAt.IsZero() {
		return "release_" + app.Status.CurrentReleaseReadyAt.UTC().Format("20060102T150405Z")
	}
	if !app.Status.UpdatedAt.IsZero() {
		return "status_" + app.Status.UpdatedAt.UTC().Format("20060102T150405Z")
	}
	if !app.UpdatedAt.IsZero() {
		return "app_" + app.UpdatedAt.UTC().Format("20060102T150405Z")
	}
	return "app_" + strings.TrimSpace(app.ID)
}

func edgeRouteCacheNamespace(appID, deploymentGeneration string) string {
	appID = strings.TrimSpace(appID)
	deploymentGeneration = strings.TrimSpace(deploymentGeneration)
	switch {
	case appID != "" && deploymentGeneration != "":
		return appID + "_" + deploymentGeneration
	case appID != "":
		return appID
	default:
		return deploymentGeneration
	}
}

func defaultEdgeCachePolicies() []model.CachePolicy {
	return []model.CachePolicy{
		{
			ID:                    defaultStaticAssetCachePolicyID,
			Kind:                  model.CachePolicyKindStaticAssets,
			PathPatterns:          []string{"/_next/static/*", "/assets/*", "/static/*", "*.js", "*.css", "*.woff", "*.woff2", "*.ttf", "*.otf", "*.png", "*.jpg", "*.jpeg", "*.webp", "*.svg", "*.ico"},
			MethodAllowlist:       []string{http.MethodGet, http.MethodHead},
			StatusAllowlist:       []int{http.StatusOK},
			TTLSeconds:            31536000,
			BrowserCacheControl:   "public, max-age=31536000, immutable",
			EdgeCacheControl:      "public, max-age=31536000, immutable",
			BypassOnAuthorization: true,
			VaryAllowlist:         []string{"Accept-Encoding"},
			PurgeMode:             model.CachePolicyPurgeModeGeneration,
		},
		{
			ID:                          defaultHTMLDocumentCachePolicyID,
			Kind:                        model.CachePolicyKindHTMLDocuments,
			PathPatterns:                []string{"/", "/index.html", "*.html"},
			MethodAllowlist:             []string{http.MethodGet, http.MethodHead},
			StatusAllowlist:             []int{http.StatusOK},
			TTLSeconds:                  60,
			StaleWhileRevalidateSeconds: 300,
			BrowserCacheControl:         "public, max-age=30, stale-while-revalidate=300",
			EdgeCacheControl:            "public, max-age=60, stale-while-revalidate=300",
			BypassOnAuthorization:       true,
			BypassOnCookie:              true,
			VaryAllowlist:               append([]string(nil), defaultHTMLDocumentVaryAllowlist...),
			PurgeMode:                   model.CachePolicyPurgeModeGeneration,
		},
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
