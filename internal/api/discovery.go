package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/routeartifact"
)

func (s *Server) handleDiscoveryBundle(w http.ResponseWriter, r *http.Request) {
	principal := discoveryBundlePrincipal()
	bundle, err := s.deriveDiscoveryBundle(r, principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	etag := edgeRouteBundleETag(bundle.Generation)
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("X-Fugue-Discovery-Bundle-Version", bundle.Generation)
	if edgeRouteBundleETagMatches(r.Header.Get("If-None-Match"), bundle.Generation) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, bundle)
}

func (s *Server) deriveDiscoveryBundle(r *http.Request, principal model.Principal) (model.DiscoveryBundle, error) {
	now := time.Now().UTC()
	edgeNodes, edgeGroups, err := s.store.ListActiveEdgeNodes("")
	if err != nil {
		return model.DiscoveryBundle{}, err
	}
	dnsNodes, err := s.store.ListDNSNodes("")
	if err != nil {
		return model.DiscoveryBundle{}, err
	}
	nodePolicies, err := s.loadClusterNodePolicyStatuses(r.Context(), principal)
	if err != nil {
		if s.log != nil {
			s.log.Printf("discovery bundle continuing without node policy inventory: %v", err)
		}
		nodePolicies = nil
	}
	edgeNodes = activeEdgeNodesForPolicy(edgeNodes, nodePolicies)
	edgeNodes = eligibleDiscoveryEdgeNodes(edgeNodes, now, s.activeNodeQuarantineByName())
	dnsNodes = activeDNSNodesForPolicy(dnsNodes, nodePolicies)
	edgeGroups = activeEdgeGroupsForInventory(edgeGroups, edgeNodes, dnsNodes)
	edgeGroups = dedupeEdgeGroups(edgeGroups)
	sort.Slice(edgeNodes, func(i, j int) bool { return edgeNodes[i].ID < edgeNodes[j].ID })
	sort.Slice(dnsNodes, func(i, j int) bool { return dnsNodes[i].ID < dnsNodes[j].ID })
	sort.Slice(nodePolicies, func(i, j int) bool { return nodePolicies[i].NodeName < nodePolicies[j].NodeName })

	platformRoutes, err := s.publishedDiscoveryPlatformRoutes()
	if err != nil {
		return model.DiscoveryBundle{}, err
	}
	apiURL := s.publicAPIURL(r)
	bundle := model.DiscoveryBundle{
		SchemaVersion:       model.BundleSchemaVersionV1,
		GeneratedAt:         now,
		ValidUntil:          now.Add(s.discoveryBundleTTL()),
		Issuer:              model.BundleIssuerFugue,
		APIEndpoints:        s.discoveryAPIEndpoints(apiURL),
		Kubernetes:          s.discoveryKubernetesEndpoints(),
		Registry:            s.discoveryRegistryEndpoints(),
		EdgeGroups:          edgeGroups,
		EdgeNodes:           edgeNodes,
		EdgeSelectionPolicy: model.DefaultEdgeSelectionPolicy(),
		DNSNodes:            dnsNodes,
		PlatformRoutes:      platformRoutes,
		PublicRuntimeEnv:    s.discoveryRuntimeEnv(apiURL),
	}
	bundle.Generation = discoveryBundleGeneration(bundle, nodePolicies, nil)
	bundle = signDiscoveryBundle(bundle, s.bundleKeyring(), s.discoveryBundleTTL())
	return bundle, nil
}

func (s *Server) discoveryBundleTTL() time.Duration {
	if s.bundleValidFor > 0 {
		return s.bundleValidFor
	}
	return 15 * time.Minute
}

func (s *Server) publicAPIURL(r *http.Request) string {
	if strings.TrimSpace(s.apiPublicDomain) != "" {
		return "https://" + strings.TrimSpace(s.apiPublicDomain)
	}
	if r == nil {
		return ""
	}
	host := firstForwardedValue(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = strings.TrimSpace(r.Host)
	}
	if host == "" {
		return ""
	}
	scheme := firstForwardedValue(r.Header.Get("X-Forwarded-Proto"))
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	return scheme + "://" + host
}

func (s *Server) discoveryAPIEndpoints(apiURL string) []model.DiscoveryEndpoint {
	out := []model.DiscoveryEndpoint{}
	if strings.TrimSpace(apiURL) != "" {
		out = append(out, model.DiscoveryEndpoint{Name: "public", URL: apiURL, EndpointMode: controlPlaneEndpointModeSingle})
	}
	if strings.TrimSpace(s.clusterJoinServer) != "" {
		out = append(out, model.DiscoveryEndpoint{Name: "cluster-join", URL: s.clusterJoinServer, EndpointMode: discoveryEndpointModeForFallbacks(s.clusterJoinServerFallbacks)})
	}
	return out
}

func discoveryEndpointModeForFallbacks(fallbacks []string) string {
	if len(nonEmptyDiscoveryStrings(fallbacks)) > 0 {
		return controlPlaneEndpointModeMultiAddress
	}
	return controlPlaneEndpointModeSingle
}

func nonEmptyDiscoveryStrings(values []string) []string {
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func discoveryBundlePrincipal() model.Principal {
	return model.Principal{
		ActorType: "system",
		ActorID:   "discovery-bundle",
		Scopes: map[string]struct{}{
			"platform.admin": {},
		},
	}
}

func firstForwardedValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if idx := strings.IndexByte(value, ','); idx >= 0 {
		value = value[:idx]
	}
	return strings.TrimSpace(value)
}

func (s *Server) discoveryKubernetesEndpoints() []model.DiscoveryKubernetesEndpoint {
	endpoint := model.DiscoveryKubernetesEndpoint{
		Name:            "cluster-join",
		Server:          s.clusterJoinServer,
		FallbackServers: append([]string(nil), s.clusterJoinServerFallbacks...),
		EndpointMode:    discoveryEndpointModeForFallbacks(s.clusterJoinServerFallbacks),
		CAHash:          s.clusterJoinCAHash,
	}
	if strings.TrimSpace(s.clusterJoinRegistryEndpoint) != "" {
		endpoint.RegistryEndpoint = s.clusterJoinRegistryEndpoint
	}
	return []model.DiscoveryKubernetesEndpoint{endpoint}
}

func (s *Server) discoveryRegistryEndpoints() []model.DiscoveryRegistryEndpoint {
	return []model.DiscoveryRegistryEndpoint{
		{
			Name:     "registry",
			PushBase: s.registryPushBase,
			PullBase: s.registryPullBase,
			Mirror:   s.clusterJoinRegistryEndpoint,
		},
	}
}

func (s *Server) discoveryRuntimeEnv(apiURL string) map[string]string {
	env := map[string]string{
		"FUGUE_API_URL":                        apiURL,
		"FUGUE_API_PUBLIC_DOMAIN":              s.apiPublicDomain,
		"FUGUE_APP_BASE_DOMAIN":                s.appBaseDomain,
		"FUGUE_REGISTRY_PUSH_BASE":             s.registryPushBase,
		"FUGUE_REGISTRY_PULL_BASE":             s.registryPullBase,
		"FUGUE_CLUSTER_JOIN_SERVER":            s.clusterJoinServer,
		"FUGUE_CLUSTER_JOIN_SERVER_FALLBACKS":  strings.Join(s.clusterJoinServerFallbacks, ","),
		"FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT": s.clusterJoinRegistryEndpoint,
	}
	if zone := strings.TrimSpace(s.customDomainBaseDomain); zone != "" {
		env["FUGUE_DNS_ZONE"] = zone
	}
	return env
}

func discoveryBundleGeneration(bundle model.DiscoveryBundle, nodePolicies []model.ClusterNodePolicyStatus, edgeRoutePolicies []model.EdgeRoutePolicy) string {
	payload := struct {
		model.DiscoveryBundle
		NodePolicies      []model.ClusterNodePolicyStatus `json:"node_policies"`
		EdgeRoutePolicies []model.EdgeRoutePolicy         `json:"edge_route_policies"`
	}{
		DiscoveryBundle:   bundle,
		NodePolicies:      nodePolicies,
		EdgeRoutePolicies: edgeRoutePolicies,
	}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return "discovery_" + hex.EncodeToString(sum[:])[:16]
}

func dedupeEdgeGroups(groups []model.EdgeGroup) []model.EdgeGroup {
	if len(groups) == 0 {
		return groups
	}
	seen := make(map[string]struct{}, len(groups))
	out := make([]model.EdgeGroup, 0, len(groups))
	for _, group := range groups {
		id := strings.TrimSpace(group.ID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, group)
	}
	return out
}

// Discovery carries a route summary for node bootstrap, but it is derived only
// from the verified global TrafficReleaseSet. It cannot reintroduce environment
// route configuration as a serving source.
func (s *Server) publishedDiscoveryPlatformRoutes() ([]model.PlatformRoute, error) {
	parent, found, err := s.verifiedPlatformArtifactForScope(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil {
		return nil, err
	}
	if !found {
		// Discovery remains available for bootstrap metadata, but carries no
		// serving routes until a verified TrafficReleaseSet exists.
		return []model.PlatformRoute{}, nil
	}
	child, err := consumerAssignmentChild(parent, model.PlatformArtifactKindEdgeRouteBundle, s.store.GetPlatformArtifact)
	if err != nil {
		return nil, err
	}
	snapshot, err := routeartifact.Project(child)
	if err != nil {
		return nil, err
	}
	out := make([]model.PlatformRoute, 0, len(snapshot.Routes))
	for _, route := range snapshot.Routes {
		upstream := route.UpstreamURL
		if upstream == "" && len(route.Upstreams) > 0 {
			upstream = route.Upstreams[0].UpstreamURL
		}
		status := route.OriginStatus
		if status == "" {
			status = model.EdgeRouteStatusActive
		}
		out = append(out, model.PlatformRoute{Hostname: route.Hostname, Kind: route.RouteKind, UpstreamKind: route.UpstreamKind, UpstreamScope: route.UpstreamScope, UpstreamURL: upstream, TLSPolicy: route.TLSPolicy, RoutePolicy: route.RoutePolicy, EdgeGroupMode: route.TargetGroupMode, EdgeGroupID: route.PinnedEdgeGroupID, Status: status, StatusReason: route.OriginStatusReason})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hostname < out[j].Hostname })
	return out, nil
}
