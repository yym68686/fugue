package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/auth"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

type edgeRouteIntentSource interface {
	EdgeRoutePolicyTime() (time.Time, error)
	ListAppsMetadata(string, bool) ([]model.App, error)
	ListVerifiedAppDomains() ([]model.AppDomain, error)
	ListProjectRouteTables(string, bool) ([]model.ProjectRouteTable, error)
	ListRuntimes(string, bool) ([]model.Runtime, error)
	ListEdgeRoutePolicies() ([]model.EdgeRoutePolicy, error)
	ListAppReleases(model.AppReleaseFilter) ([]model.AppRelease, error)
	ListAppTrafficPolicies(string, bool) ([]model.AppTrafficPolicy, error)
}

func (s *Server) handleEdgeRouteIntents(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.PlatformComponentIdentityFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, http.StatusInternalServerError, "verified platform component identity missing")
		return
	}
	if !edgeRouteIntentClaimsAllowed(claims) {
		httpx.WriteError(w, http.StatusForbidden, "platform component identity is not authorized for edge route intents")
		return
	}
	snapshot, err := s.deriveEdgeRouteIntentSnapshot(r, s.store)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	w.Header().Set("ETag", edgeRouteBundleETag(snapshot.Generation))
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("X-Fugue-Route-Intent-Generation", snapshot.Generation)
	httpx.WriteJSON(w, http.StatusOK, snapshot)
}

func edgeRouteIntentClaimsAllowed(claims platformcontrol.PlatformComponentIdentityClaims) bool {
	return strings.EqualFold(strings.TrimSpace(claims.Component), model.PlatformConsumerComponentEdgeControl) &&
		strings.EqualFold(strings.TrimSpace(claims.ScopeKey), "global") &&
		len(claims.ArtifactKinds) == 1 &&
		strings.EqualFold(strings.TrimSpace(claims.ArtifactKinds[0]), model.PlatformArtifactKindEdgeRouteIntent)
}

func (s *Server) deriveEdgeRouteIntentSnapshot(r *http.Request, source edgeRouteIntentSource) (model.EdgeRouteIntentSnapshot, error) {
	apps, err := source.ListAppsMetadata("", true)
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	domains, err := source.ListVerifiedAppDomains()
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	projectRouteTables, err := source.ListProjectRouteTables("", true)
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	runtimes, err := source.ListRuntimes("", true)
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	policies, err := source.ListEdgeRoutePolicies()
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	releases, err := source.ListAppReleases(model.AppReleaseFilter{PlatformAdmin: true, ActiveOnly: true})
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	trafficPolicies, err := source.ListAppTrafficPolicies("", true)
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	policyTime, err := source.EdgeRoutePolicyTime()
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}

	runtimeByID := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		runtimeByID[strings.TrimSpace(runtimeObj.ID)] = runtimeObj
	}
	runtimeNodeLabelsByID := s.edgeRouteRuntimeNodeLabels(r.Context())
	apps, _ = s.overlayManagedAppStatusesForEdgeRoutesCachedWithProvenance(apps, runtimeByID)
	sort.Slice(apps, func(i, j int) bool { return strings.TrimSpace(apps[i].ID) < strings.TrimSpace(apps[j].ID) })
	sort.Slice(domains, func(i, j int) bool {
		left, right := normalizeExternalAppDomain(domains[i].Hostname), normalizeExternalAppDomain(domains[j].Hostname)
		if left != right {
			return left < right
		}
		return strings.TrimSpace(domains[i].AppID) < strings.TrimSpace(domains[j].AppID)
	})
	sort.Slice(projectRouteTables, func(i, j int) bool {
		left := strings.TrimSpace(projectRouteTables[i].TenantID) + "\x00" + strings.TrimSpace(projectRouteTables[i].ProjectID)
		right := strings.TrimSpace(projectRouteTables[j].TenantID) + "\x00" + strings.TrimSpace(projectRouteTables[j].ProjectID)
		return left < right
	})
	platformRoutes := append([]model.PlatformRoute(nil), s.platformRoutes...)
	sort.Slice(platformRoutes, func(i, j int) bool {
		left := normalizeExternalAppDomain(platformRoutes[i].Hostname) + "\x00" + strings.TrimSpace(platformRoutes[i].Kind) + "\x00" + strings.TrimSpace(platformRoutes[i].UpstreamURL)
		right := normalizeExternalAppDomain(platformRoutes[j].Hostname) + "\x00" + strings.TrimSpace(platformRoutes[j].Kind) + "\x00" + strings.TrimSpace(platformRoutes[j].UpstreamURL)
		return left < right
	})
	appByID := make(map[string]model.App, len(apps))
	appsByProjectID := make(map[string][]model.App)
	for _, app := range apps {
		appByID[strings.TrimSpace(app.ID)] = app
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

	intents := make([]model.EdgeRouteIntent, 0, len(apps)+len(domains)+len(s.platformRoutes))
	tlsAllowlist := make([]model.EdgeTLSAllowlistEntry, 0, len(domains))
	appendBinding := func(binding model.EdgeRouteBinding) {
		binding = applyAppReleaseTraffic(binding, trafficPolicyByApp, releaseByID)
		intents = append(intents, edgeRouteIntentFromBinding(binding, policyByHostname[normalizeExternalAppDomain(binding.Hostname)], policyTime))
	}
	for _, app := range apps {
		if app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "" {
			continue
		}
		appendBinding(s.compileTrafficEpochRouteBinding(r.Context(), app, strings.TrimSpace(app.Route.Hostname), model.EdgeRouteKindPlatform, model.EdgeRouteTLSPolicyPlatform, app.CreatedAt, app.UpdatedAt, runtimeByID, runtimeNodeLabelsByID))
	}
	for _, route := range platformRoutes {
		intents = append(intents, edgeRouteIntentFromPlatformRoute(route))
	}
	for _, domain := range domains {
		hostname := normalizeExternalAppDomain(domain.Hostname)
		app, ok := appByID[strings.TrimSpace(domain.AppID)]
		if hostname == "" || !ok {
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
		binding := s.compileTrafficEpochRouteBinding(r.Context(), app, hostname, routeKind, tlsPolicy, domain.CreatedAt, domain.UpdatedAt, runtimeByID, runtimeNodeLabelsByID)
		binding = applyCustomDomainReadiness(binding, domain)
		appendBinding(binding)
		tlsAllowlist = append(tlsAllowlist, model.EdgeTLSAllowlistEntry{Hostname: hostname, AppID: domain.AppID, TenantID: domain.TenantID, Status: domain.Status, TLSStatus: domain.TLSStatus})
	}
	for _, table := range projectRouteTables {
		for _, routeBinding := range store.TryCompileProjectRouteTableBindings(table, appsByProjectID[strings.TrimSpace(table.ProjectID)]) {
			app, ok := appByID[strings.TrimSpace(routeBinding.AppID)]
			if !ok {
				continue
			}
			hostname := normalizeExternalAppDomain(routeBinding.Hostname)
			routeKind, tlsPolicy, domain, ok := s.projectRouteEdgePolicy(hostname, domainByHostname)
			if !ok {
				continue
			}
			binding := s.compileTrafficEpochRouteBindingForRoute(r.Context(), app, hostname, routeBinding.PathPrefix, routeBinding.ServicePort, routeKind, tlsPolicy, table.CreatedAt, table.UpdatedAt, runtimeByID, runtimeNodeLabelsByID)
			if domain != nil {
				binding = applyCustomDomainReadiness(binding, *domain)
			}
			appendBinding(binding)
		}
	}
	intents = dedupeEdgeRouteIntents(intents)
	sort.Slice(intents, func(i, j int) bool {
		if intents[i].Hostname != intents[j].Hostname {
			return intents[i].Hostname < intents[j].Hostname
		}
		if intents[i].PathPrefix != intents[j].PathPrefix {
			return intents[i].PathPrefix < intents[j].PathPrefix
		}
		return intents[i].RouteKind < intents[j].RouteKind
	})
	sort.Slice(tlsAllowlist, func(i, j int) bool { return tlsAllowlist[i].Hostname < tlsAllowlist[j].Hostname })
	snapshot := model.EdgeRouteIntentSnapshot{SchemaVersion: model.EdgeRouteIntentSchemaVersionV1, GeneratedAt: time.Now().UTC(), Routes: intents, TLSAllowlist: tlsAllowlist}
	for _, intent := range intents {
		if strings.TrimSpace(intent.CachePolicyID) != "" {
			snapshot.CachePolicies = defaultEdgeCachePolicies()
			break
		}
	}
	snapshot.Generation = edgeRouteIntentSnapshotGeneration(snapshot)
	return snapshot, nil
}

func edgeRouteIntentFromBinding(binding model.EdgeRouteBinding, policy model.EdgeRoutePolicy, policyTime time.Time) model.EdgeRouteIntent {
	policyMatches := edgeRoutePolicyMatchesBinding(policy, binding)
	routePolicy := model.EdgeRoutePolicyRouteAOnly
	if isDefaultEdgeRouteKind(binding.RouteKind) {
		routePolicy = model.EdgeRoutePolicyEnabled
	}
	targetMode := model.EdgeRouteIntentGroupModeAllGroups
	pinnedGroupID := ""
	minHealthy := defaultMinHealthyEdgeNodesForBinding(binding)
	if policyMatches {
		if normalized := model.NormalizeEdgeRoutePolicy(policy.RoutePolicy); normalized != "" {
			routePolicy = normalized
		}
		if policy.MinHealthyEdgeNodes > 0 {
			minHealthy = policy.MinHealthyEdgeNodes
		}
	}
	intent := model.EdgeRouteIntent{
		Hostname: binding.Hostname, PathPrefix: model.NormalizeAppRoutePathPrefix(binding.PathPrefix), RouteKind: binding.RouteKind,
		AppID: binding.AppID, TenantID: binding.TenantID, RuntimeID: binding.RuntimeID, RuntimeType: binding.RuntimeType,
		RuntimeEdgeGroupID: binding.RuntimeEdgeGroupID, RuntimeClusterNode: binding.RuntimeClusterNode,
		TargetGroupMode: targetMode, PinnedEdgeGroupID: pinnedGroupID, MinHealthyEdgeNodes: minHealthy, RoutePolicy: routePolicy,
		UpstreamKind: binding.UpstreamKind, UpstreamScope: binding.UpstreamScope, UpstreamURL: binding.UpstreamURL,
		Upstreams: append([]model.EdgeRouteUpstream(nil), binding.Upstreams...), ServicePort: binding.ServicePort, TLSPolicy: binding.TLSPolicy,
		CachePolicyID: binding.CachePolicyID, CacheNamespace: binding.CacheNamespace, DeploymentGeneration: binding.DeploymentGeneration,
		RequestBodyPolicies: model.CloneEdgeRequestBodyPolicies(binding.RequestBodyPolicies), Streaming: binding.Streaming,
		OriginStatus: binding.Status, OriginStatusReason: binding.StatusReason, CreatedAt: binding.CreatedAt, UpdatedAt: binding.UpdatedAt,
	}
	if policyMatches {
		intent.ExcludedEdgeIDs = normalizeEdgeRouteExclusionIDs(policy.ExcludedEdgeIDs)
		intent.ExcludedEdgeGroupIDs = normalizeEdgeRouteExclusionIDs(policy.ExcludedEdgeGroupIDs)
		intent.ExclusionReason = strings.TrimSpace(policy.ExclusionReason)
		intent.ExclusionExpiresAt = policy.ExclusionExpiresAt
		intent.ExclusionLifecycle = model.EdgeRoutePolicyExclusionLifecycleAt(policy, policyTime)
	}
	intent.Generation = edgeRouteIntentGeneration(intent)
	return intent
}

// edgeRouteIntentDiagnosticBindings projects Core-owned route intent into the
// existing admin response shape without inventing Edge Control materialization
// facts such as the selected group, healthy instance count, or decision ID.
func edgeRouteIntentDiagnosticBindings(intents []model.EdgeRouteIntent) []model.EdgeRouteBinding {
	bindings := make([]model.EdgeRouteBinding, 0, len(intents))
	for _, intent := range intents {
		status := strings.TrimSpace(intent.OriginStatus)
		if status == "" {
			status = model.EdgeRouteStatusActive
		}
		binding := model.EdgeRouteBinding{
			Hostname:             normalizeExternalAppDomain(intent.Hostname),
			PathPrefix:           model.NormalizeAppRoutePathPrefix(intent.PathPrefix),
			RouteKind:            strings.TrimSpace(intent.RouteKind),
			AppID:                strings.TrimSpace(intent.AppID),
			TenantID:             strings.TrimSpace(intent.TenantID),
			RuntimeID:            strings.TrimSpace(intent.RuntimeID),
			RuntimeType:          strings.TrimSpace(intent.RuntimeType),
			RuntimeEdgeGroup:     strings.TrimSpace(intent.RuntimeEdgeGroupID),
			RuntimeEdgeGroupID:   strings.TrimSpace(intent.RuntimeEdgeGroupID),
			RuntimeClusterNode:   strings.TrimSpace(intent.RuntimeClusterNode),
			ExcludedEdgeIDs:      normalizeEdgeRouteExclusionIDs(intent.ExcludedEdgeIDs),
			ExcludedEdgeGroupIDs: normalizeEdgeRouteExclusionIDs(intent.ExcludedEdgeGroupIDs),
			ExclusionReason:      strings.TrimSpace(intent.ExclusionReason),
			ExclusionExpiresAt:   intent.ExclusionExpiresAt,
			MinHealthyEdgeNodes:  intent.MinHealthyEdgeNodes,
			RoutePolicy:          model.NormalizeEdgeRoutePolicy(intent.RoutePolicy),
			UpstreamKind:         strings.TrimSpace(intent.UpstreamKind),
			UpstreamScope:        strings.TrimSpace(intent.UpstreamScope),
			UpstreamURL:          strings.TrimSpace(intent.UpstreamURL),
			Upstreams:            append([]model.EdgeRouteUpstream(nil), intent.Upstreams...),
			ServicePort:          intent.ServicePort,
			TLSPolicy:            strings.TrimSpace(intent.TLSPolicy),
			CachePolicyID:        strings.TrimSpace(intent.CachePolicyID),
			CacheNamespace:       strings.TrimSpace(intent.CacheNamespace),
			DeploymentGeneration: strings.TrimSpace(intent.DeploymentGeneration),
			RequestBodyPolicies:  model.CloneEdgeRequestBodyPolicies(intent.RequestBodyPolicies),
			Streaming:            intent.Streaming,
			Status:               status,
			StatusReason:         strings.TrimSpace(intent.OriginStatusReason),
			RouteGeneration:      strings.TrimSpace(intent.Generation),
			CreatedAt:            intent.CreatedAt,
			UpdatedAt:            intent.UpdatedAt,
		}
		if strings.TrimSpace(intent.TargetGroupMode) == model.EdgeRouteIntentGroupModePinnedGroup {
			binding.PolicyEdgeGroupID = strings.TrimSpace(intent.PinnedEdgeGroupID)
		}
		if binding.Status != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(binding.RoutePolicy) {
			binding.UpstreamURL = ""
			binding.Upstreams = nil
		}
		bindings = append(bindings, binding)
	}
	return bindings
}

func validateEdgeRouteIntentSnapshotForDiagnostics(snapshot model.EdgeRouteIntentSnapshot) error {
	if snapshot.SchemaVersion != model.EdgeRouteIntentSchemaVersionV1 {
		return fmt.Errorf("route intent schema is %q", snapshot.SchemaVersion)
	}
	if strings.TrimSpace(snapshot.Generation) == "" || edgeRouteIntentSnapshotGeneration(snapshot) != snapshot.Generation {
		return fmt.Errorf("route intent snapshot generation is invalid")
	}
	for _, intent := range snapshot.Routes {
		hostname := normalizeExternalAppDomain(intent.Hostname)
		if hostname == "" || strings.TrimSpace(intent.RouteKind) == "" || strings.TrimSpace(intent.Generation) == "" || edgeRouteIntentGeneration(intent) != intent.Generation {
			return fmt.Errorf("route intent identity is invalid for hostname %q", hostname)
		}
		switch strings.TrimSpace(intent.TargetGroupMode) {
		case model.EdgeRouteIntentGroupModeAllGroups:
		case model.EdgeRouteIntentGroupModePinnedGroup:
			if strings.TrimSpace(intent.PinnedEdgeGroupID) == "" {
				return fmt.Errorf("pinned route intent is missing edge group for hostname %q", hostname)
			}
		default:
			return fmt.Errorf("route intent target group mode is invalid for hostname %q", hostname)
		}
		policy := model.NormalizeEdgeRoutePolicy(intent.RoutePolicy)
		if policy == "" {
			return fmt.Errorf("route intent policy is invalid for hostname %q", hostname)
		}
		status := strings.TrimSpace(intent.OriginStatus)
		if status == "" {
			status = model.EdgeRouteStatusActive
		}
		if status == model.EdgeRouteStatusActive && model.EdgeRoutePolicyAllowsTraffic(policy) && strings.TrimSpace(intent.UpstreamURL) == "" && len(intent.Upstreams) == 0 {
			return fmt.Errorf("active route intent has no upstream for hostname %q", hostname)
		}
	}
	return nil
}

func edgeRouteIntentFromPlatformRoute(route model.PlatformRoute) model.EdgeRouteIntent {
	routePolicy := model.NormalizeEdgeRoutePolicy(route.RoutePolicy)
	if routePolicy == "" {
		routePolicy = model.EdgeRoutePolicyRouteAOnly
	}
	mode := model.EdgeRouteIntentGroupModeAllGroups
	pinned := ""
	if route.EdgeGroupMode == model.PlatformRouteEdgeGroupModePinned {
		mode = model.EdgeRouteIntentGroupModePinnedGroup
		pinned = strings.TrimSpace(route.EdgeGroupID)
	}
	status := strings.TrimSpace(route.Status)
	if status == "" {
		status = model.EdgeRouteStatusActive
	}
	upstream := strings.TrimSpace(route.UpstreamURL)
	if status != model.EdgeRouteStatusActive || !model.EdgeRoutePolicyAllowsTraffic(routePolicy) {
		upstream = ""
	}
	intent := model.EdgeRouteIntent{
		Hostname: normalizeExternalAppDomain(route.Hostname), PathPrefix: "/", RouteKind: route.Kind,
		TargetGroupMode: mode, PinnedEdgeGroupID: pinned, MinHealthyEdgeNodes: defaultMinHealthyEdgeNodesForBinding(model.EdgeRouteBinding{RouteKind: route.Kind}),
		RoutePolicy: routePolicy, UpstreamKind: route.UpstreamKind, UpstreamScope: route.UpstreamScope, UpstreamURL: upstream,
		TLSPolicy: route.TLSPolicy, Streaming: true, OriginStatus: status, OriginStatusReason: route.StatusReason,
	}
	intent.Generation = edgeRouteIntentGeneration(intent)
	return intent
}

func dedupeEdgeRouteIntents(intents []model.EdgeRouteIntent) []model.EdgeRouteIntent {
	indexByKey := make(map[string]int, len(intents))
	out := make([]model.EdgeRouteIntent, 0, len(intents))
	for _, intent := range intents {
		key := normalizeExternalAppDomain(intent.Hostname) + "\x00" + model.NormalizeAppRoutePathPrefix(intent.PathPrefix)
		if index, ok := indexByKey[key]; ok {
			out[index] = intent
			continue
		}
		indexByKey[key] = len(out)
		out = append(out, intent)
	}
	return out
}

func edgeRouteIntentGeneration(intent model.EdgeRouteIntent) string {
	intent.Generation = ""
	payload, _ := json.Marshal(intent)
	sum := sha256.Sum256(payload)
	return "routeintent_" + hex.EncodeToString(sum[:])
}

func edgeRouteIntentSnapshotGeneration(snapshot model.EdgeRouteIntentSnapshot) string {
	material := struct {
		SchemaVersion string                        `json:"schema_version"`
		Routes        []model.EdgeRouteIntent       `json:"routes"`
		TLSAllowlist  []model.EdgeTLSAllowlistEntry `json:"tls_allowlist"`
		CachePolicies []model.CachePolicy           `json:"cache_policies,omitempty"`
	}{snapshot.SchemaVersion, snapshot.Routes, snapshot.TLSAllowlist, snapshot.CachePolicies}
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return "routeintents_" + hex.EncodeToString(sum[:])
}
