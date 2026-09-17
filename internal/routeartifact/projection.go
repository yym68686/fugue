// Package routeartifact projects immutable platform route artifacts into executor
// inputs without consulting business tables or live runtime state. Callers verify
// artifact integrity and release authorization before using these projections.
package routeartifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

var platformRouteArtifactGroupID = regexp.MustCompile(`^edge-group-[a-z0-9]+(?:-[a-z0-9]+)*$`)

// Project preserves the established RouteIntent wire semantics for a verified
// artifact. It never reads business state or grants serving authorization.
func Project(artifact model.PlatformArtifact) (model.EdgeRouteIntentSnapshot, error) {
	if schema, ok := artifact.Content["schema_version"]; ok && schema != platformconfig.SchemaVersion {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("unsupported route artifact payload schema")
	}
	if artifact.Content["routes"] == nil {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("route artifact routes are missing")
	}
	raw, err := json.Marshal(artifact.Content["routes"])
	if err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	var routes []struct {
		platformconfig.CompiledRoute
		Enabled *bool `json:"enabled"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&routes); err != nil {
		return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("decode verified route artifact: %w", err)
	}
	var cachePolicies []model.CachePolicy
	if content, exists := artifact.Content["cache_policies"]; exists {
		raw, err := json.Marshal(content)
		if err != nil {
			return model.EdgeRouteIntentSnapshot{}, err
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cachePolicies); err != nil {
			return model.EdgeRouteIntentSnapshot{}, err
		}
	}
	typedRoutes := make([]platformconfig.RouteIntent, 0, len(routes))
	for _, route := range routes {
		typedRoutes = append(typedRoutes, route.RouteIntent)
	}
	if err := platformconfig.ValidateRouteBehavior(typedRoutes, cachePolicies); err != nil {
		return model.EdgeRouteIntentSnapshot{}, err
	}
	disabledCacheIDs := map[string]bool{}
	cacheIDs := map[string]string{}
	for _, policy := range cachePolicies {
		disabledCacheIDs[strings.ToLower(policy.ID)] = policy.Kind == model.CachePolicyKindDisabled
		cacheIDs[strings.ToLower(policy.ID)] = policy.ID
	}
	minimumHealthy := 1
	compiledTraffic := map[string]platformconfig.TrafficPolicyConstraint{}
	if value, exists := artifact.Content["policy"]; exists {
		rawPolicy, err := json.Marshal(value)
		if err != nil {
			return model.EdgeRouteIntentSnapshot{}, err
		}
		var policy platformconfig.PolicySnapshot
		if err := json.Unmarshal(rawPolicy, &policy); err != nil {
			return model.EdgeRouteIntentSnapshot{}, err
		}
		if err := platformconfig.ValidatePolicySnapshot(policy); err != nil {
			return model.EdgeRouteIntentSnapshot{}, err
		}
		if policy.MinimumHealthyEdges > 0 {
			minimumHealthy = policy.MinimumHealthyEdges
		}
		for _, rule := range policy.TrafficConstraints {
			compiledTraffic[rule.AppID] = rule
		}
	}
	intents := make([]model.EdgeRouteIntent, 0, len(routes))
	seen := make(map[string]bool, len(routes))
	for _, route := range routes {
		hostname := normalizeHostname(route.Hostname)
		path := model.NormalizeAppRoutePathPrefix(route.PathPrefix)
		key := hostname + "\x00" + path
		if hostname == "" || route.Enabled == nil || seen[key] {
			return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("route artifact requires unique hostname/path and explicit enabled state")
		}
		if route.PathPrefix != "" && route.PathPrefix != path || route.ServicePort < 0 || route.ServicePort > 65535 {
			return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("route artifact has invalid path or service port")
		}
		if err := platformconfig.ValidateUpstreamIntents(route.Upstreams); err != nil {
			return model.EdgeRouteIntentSnapshot{}, err
		}
		seen[key] = true
		legacy := model.PlatformRoute{
			Hostname: hostname, Kind: route.Kind, UpstreamKind: route.UpstreamKind,
			UpstreamScope: route.UpstreamScope, UpstreamURL: strings.TrimSpace(route.UpstreamURL),
			TLSPolicy: route.TLSPolicy, RoutePolicy: route.RoutePolicy, EdgeGroupMode: route.EdgeGroupMode,
			EdgeGroupID: route.EdgeGroupID, Status: route.Status, StatusReason: route.StatusReason, TTL: route.TTL,
		}
		// Earlier compiler payloads represented pinned placement by ID alone.
		if legacy.EdgeGroupMode == "" && legacy.EdgeGroupID != "" {
			legacy.EdgeGroupMode = model.PlatformRouteEdgeGroupModePinned
		}
		if !*route.Enabled {
			legacy.Status = model.EdgeRouteStatusDisabled
			legacy.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
		}
		legacy, ok := NormalizePlatformRoute(legacy)
		if !ok {
			return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("route artifact has invalid platform route semantics")
		}
		if legacy.EdgeGroupMode == model.PlatformRouteEdgeGroupModePinned && !platformRouteArtifactGroupID.MatchString(legacy.EdgeGroupID) {
			return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("route artifact has invalid pinned edge group")
		}
		if *route.Enabled {
			parsed, err := url.Parse(legacy.UpstreamURL)
			if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
				return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("enabled route artifact has invalid HTTP upstream")
			}
		}
		intent := IntentFromPlatformRoute(legacy, minimumHealthy)
		intent.AppID, intent.TenantID, intent.RuntimeID = route.AppID, route.TenantID, route.RuntimeID
		intent.RuntimeType, intent.RuntimeEdgeGroupID, intent.RuntimeClusterNode = route.RuntimeType, route.RuntimeEdgeGroupID, route.RuntimeClusterNode
		if intent.OriginStatus == model.EdgeRouteStatusActive && model.EdgeRoutePolicyAllowsTraffic(intent.RoutePolicy) {
			intent.Upstreams = platformconfig.ProjectUpstreamIntents(route.Upstreams)
			if rule, resolved := compiledTraffic[route.AppID]; resolved {
				// The traffic compiler emits only targets whose fixed release facts
				// passed owner, freshness and availability checks. Preserve that
				// eligibility in the executor projection; desired upstreams without
				// a compiled traffic policy still carry no runtime status.
				for i := range intent.Upstreams {
					upstream := &intent.Upstreams[i]
					if rule.TenantID != route.TenantID || upstream.ReleaseID == "" ||
						(upstream.ReleaseID != rule.StableReleaseID && upstream.ReleaseID != rule.CandidateReleaseID) || upstream.Weight <= 0 {
						return model.EdgeRouteIntentSnapshot{}, fmt.Errorf("compiled release upstream does not match traffic policy")
					}
					upstream.Status = model.EdgeRouteStatusActive
				}
			}
		}
		intent.CachePolicyID, intent.CacheNamespace = cacheIDs[strings.ToLower(route.CachePolicyID)], route.CacheNamespace
		if disabledCacheIDs[strings.ToLower(route.CachePolicyID)] {
			intent.CachePolicyID = ""
		}
		intent.DeploymentGeneration = route.DeploymentGeneration
		intent.RequestBodyPolicies = model.CloneEdgeRequestBodyPolicies(route.RequestBodyPolicies)
		intent.PathPrefix = path
		intent.ServicePort = route.ServicePort
		if route.Streaming != nil {
			intent.Streaming = *route.Streaming
		}
		intent.MinHealthyEdgeNodes = minimumHealthy
		intent.ExcludedEdgeIDs = append([]string(nil), route.ExcludedEdgeIDs...)
		intent.ExcludedEdgeGroupIDs = append([]string(nil), route.ExcludedEdgeGroupIDs...)
		intent.ExclusionReason, intent.ExclusionExpiresAt = route.ExclusionReason, route.ExclusionExpiresAt
		if route.MinHealthyEdgeNodes > 0 {
			intent.MinHealthyEdgeNodes = route.MinHealthyEdgeNodes
		}
		intent.Generation = IntentGeneration(intent)
		intents = append(intents, intent)
	}
	sort.Slice(intents, func(i, j int) bool {
		if intents[i].Hostname != intents[j].Hostname {
			return intents[i].Hostname < intents[j].Hostname
		}
		return intents[i].PathPrefix < intents[j].PathPrefix
	})
	snapshot := model.EdgeRouteIntentSnapshot{SchemaVersion: model.EdgeRouteIntentSchemaVersionV1, Generation: artifact.Generation, GeneratedAt: artifact.CreatedAt, Routes: intents, TLSAllowlist: []model.EdgeTLSAllowlistEntry{}, CachePolicies: cachePolicies}
	return snapshot, nil
}

// NormalizePlatformRoute retains the legacy platform-route defaults shared by
// the environment importer and immutable artifact projection during migration.
func NormalizePlatformRoute(route model.PlatformRoute) (model.PlatformRoute, bool) {
	route.Hostname = normalizeHostname(route.Hostname)
	route.Kind = strings.TrimSpace(route.Kind)
	route.UpstreamKind = strings.TrimSpace(route.UpstreamKind)
	route.UpstreamScope = strings.TrimSpace(route.UpstreamScope)
	route.UpstreamURL = strings.TrimSpace(route.UpstreamURL)
	route.TLSPolicy = strings.TrimSpace(route.TLSPolicy)
	rawRoutePolicy := strings.TrimSpace(route.RoutePolicy)
	if rawRoutePolicy == "" {
		route.RoutePolicy = model.EdgeRoutePolicyEnabled
	} else {
		route.RoutePolicy = model.NormalizeEdgeRoutePolicy(rawRoutePolicy)
	}
	route.EdgeGroupMode = strings.TrimSpace(route.EdgeGroupMode)
	route.EdgeGroupID = strings.TrimSpace(route.EdgeGroupID)
	route.Status = strings.TrimSpace(route.Status)
	route.StatusReason = strings.TrimSpace(route.StatusReason)

	if route.Hostname == "" || route.UpstreamURL == "" {
		return model.PlatformRoute{}, false
	}
	if route.Kind == "" {
		route.Kind = model.EdgeRouteKindPlatformRoute
	}
	if route.UpstreamKind == "" {
		route.UpstreamKind = model.EdgeRouteUpstreamKindKubernetesService
	}
	if route.UpstreamScope == "" {
		route.UpstreamScope = model.EdgeRouteUpstreamScopeCluster
	}
	if route.TLSPolicy == "" {
		route.TLSPolicy = model.EdgeRouteTLSPolicyPlatform
	}
	if route.RoutePolicy == "" {
		return model.PlatformRoute{}, false
	}
	if route.EdgeGroupMode == "" {
		route.EdgeGroupMode = model.PlatformRouteEdgeGroupModeAllHealthy
	}
	switch route.EdgeGroupMode {
	case model.PlatformRouteEdgeGroupModeAllHealthy, model.PlatformRouteEdgeGroupModeRegionAware:
		route.EdgeGroupID = ""
	case model.PlatformRouteEdgeGroupModePinned:
		if route.EdgeGroupID == "" {
			return model.PlatformRoute{}, false
		}
	default:
		return model.PlatformRoute{}, false
	}
	if route.Status == "" {
		route.Status = model.EdgeRouteStatusActive
	}
	switch route.Status {
	case model.EdgeRouteStatusActive, model.EdgeRouteStatusDisabled, model.EdgeRouteStatusUnavailable:
	default:
		return model.PlatformRoute{}, false
	}
	if route.TTL <= 0 {
		route.TTL = 60
	}
	return route, true
}

// IntentFromPlatformRoute converts an already normalized route. Callers supply
// the health threshold from their explicit policy source.
func IntentFromPlatformRoute(route model.PlatformRoute, minimumHealthy int) model.EdgeRouteIntent {
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
		Hostname: normalizeHostname(route.Hostname), PathPrefix: "/", RouteKind: route.Kind,
		TargetGroupMode: mode, PinnedEdgeGroupID: pinned, MinHealthyEdgeNodes: minimumHealthy,
		RoutePolicy: routePolicy, UpstreamKind: route.UpstreamKind, UpstreamScope: route.UpstreamScope, UpstreamURL: upstream,
		TLSPolicy: route.TLSPolicy, Streaming: true, OriginStatus: status, OriginStatusReason: route.StatusReason,
	}
	intent.Generation = IntentGeneration(intent)
	return intent
}

func IntentGeneration(intent model.EdgeRouteIntent) string {
	intent.Generation = ""
	payload, _ := json.Marshal(intent)
	sum := sha256.Sum256(payload)
	return "routeintent_" + hex.EncodeToString(sum[:])
}

func normalizeHostname(raw string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(raw)), ".")
}
