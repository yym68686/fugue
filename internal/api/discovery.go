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
	edgeNodes, edgeGroups, err := s.store.ListEdgeNodes("")
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
	edgeRoutes, err := s.store.ListEdgeRoutePolicies()
	if err != nil {
		return model.DiscoveryBundle{}, err
	}
	edgeGroups = dedupeEdgeGroups(edgeGroups)
	sort.Slice(edgeNodes, func(i, j int) bool { return edgeNodes[i].ID < edgeNodes[j].ID })
	sort.Slice(dnsNodes, func(i, j int) bool { return dnsNodes[i].ID < dnsNodes[j].ID })
	sort.Slice(nodePolicies, func(i, j int) bool { return nodePolicies[i].NodeName < nodePolicies[j].NodeName })
	sort.Slice(edgeRoutes, func(i, j int) bool { return edgeRoutes[i].Hostname < edgeRoutes[j].Hostname })

	apiURL := s.publicAPIURL(r)
	bundle := model.DiscoveryBundle{
		SchemaVersion:    model.BundleSchemaVersionV1,
		GeneratedAt:      now,
		ValidUntil:       now.Add(s.discoveryBundleTTL()),
		Issuer:           model.BundleIssuerFugue,
		APIEndpoints:     s.discoveryAPIEndpoints(apiURL),
		Kubernetes:       s.discoveryKubernetesEndpoints(),
		Registry:         s.discoveryRegistryEndpoints(),
		EdgeGroups:       edgeGroups,
		EdgeNodes:        edgeNodes,
		DNSNodes:         dnsNodes,
		PlatformRoutes:   s.platformRoutes,
		PublicRuntimeEnv: s.discoveryRuntimeEnv(apiURL),
	}
	bundle.Generation = discoveryBundleGeneration(bundle, nodePolicies, edgeRoutes)
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
		out = append(out, model.DiscoveryEndpoint{Name: "public", URL: apiURL})
	}
	if strings.TrimSpace(s.clusterJoinServer) != "" {
		out = append(out, model.DiscoveryEndpoint{Name: "cluster-join", URL: s.clusterJoinServer})
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
