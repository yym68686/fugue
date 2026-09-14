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
	"fugue/internal/store"
)

type platformProjectionIssue struct {
	Code       string `json:"code"`
	Hostname   string `json:"hostname,omitempty"`
	PathPrefix string `json:"path_prefix,omitempty"`
}

type platformIntentProjectionResponse struct {
	DNSExclusions            []platformDNSExclusion         `json:"dns_exclusions"`
	Intent                   platformconfig.PlatformIntent  `json:"intent"`
	Policy                   platformconfig.PolicySnapshot  `json:"policy"`
	RuntimeSnapshot          platformconfig.RuntimeSnapshot `json:"runtime_snapshot"`
	SourceGeneration         string                         `json:"source_generation"`
	CapturedAt               time.Time                      `json:"captured_at"`
	RouteCount               int                            `json:"route_count"`
	OmittedRuntimeFields     []string                       `json:"omitted_runtime_fields"`
	MigrationReady           bool                           `json:"migration_ready"`
	Issues                   []platformProjectionIssue      `json:"issues"`
	BusinessSnapshotRevision string                         `json:"business_snapshot_revision"`
	BusinessSnapshotAt       time.Time                      `json:"business_snapshot_at"`
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
	business, err := s.store.CaptureRouteBusinessSnapshot(r.Context())
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "business route snapshot unavailable")
		return
	}
	source := &platformProjectionSource{edgeRouteIntentSource: routeBusinessSource{business}}
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
	projection, err := projectBusinessRouteDraft(snapshot, source.apps, observed, s.platformRoutes, business.RoutePolicies, business.TrafficPolicies, business.Releases, business.HostedZones, business.DNSRecords, s.dnsStaticRecords)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "business route draft cannot be captured")
		return
	}
	projection.BusinessSnapshotRevision = business.Revision
	projection.BusinessSnapshotAt = business.CapturedAt
	if err := projectACMEChallengeIntents(&projection, business.ACMEChallenges); err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "ACME migration configuration invalid")
		return
	}
	resolver := newHostedDNSFlattenResolver()
	captureDNSFlattenFacts(r.Context(), &projection, resolver.resolve)
	s.capturePlatformPlacements(r.Context(), &projection)
	issues := projection.Issues[:0]
	for _, issue := range projection.Issues {
		if issue.Code != "transaction_snapshot_not_frozen" {
			issues = append(issues, issue)
		}
	}
	projection.Issues = issues
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, projection)
}

// Only the captured data is visible through this adapter. The compiler-side
// projection cannot accidentally read a newer business row between methods.
type routeBusinessSource struct{ store.RouteBusinessSnapshot }

func (source routeBusinessSource) EdgeRoutePolicyTime() (time.Time, error) {
	return source.CapturedAt, nil
}
func (source routeBusinessSource) ListAppsMetadata(tenant string, admin bool) ([]model.App, error) {
	if !admin || tenant != "" {
		return nil, store.ErrInvalidInput
	}
	return source.Apps, nil
}
func (source routeBusinessSource) ListVerifiedAppDomains() ([]model.AppDomain, error) {
	return source.Domains, nil
}
func (source routeBusinessSource) ListProjectRouteTables(tenant string, admin bool) ([]model.ProjectRouteTable, error) {
	if !admin || tenant != "" {
		return nil, store.ErrInvalidInput
	}
	return source.RouteTables, nil
}
func (source routeBusinessSource) ListRuntimes(tenant string, admin bool) ([]model.Runtime, error) {
	if !admin || tenant != "" {
		return nil, store.ErrInvalidInput
	}
	return source.Runtimes, nil
}
func (source routeBusinessSource) ListEdgeRoutePolicies() ([]model.EdgeRoutePolicy, error) {
	return source.RoutePolicies, nil
}
func (source routeBusinessSource) ListAppTrafficPolicies(tenant string, admin bool) ([]model.AppTrafficPolicy, error) {
	if !admin || tenant != "" {
		return nil, store.ErrInvalidInput
	}
	return source.TrafficPolicies, nil
}
func (source routeBusinessSource) ListAppReleases(filter model.AppReleaseFilter) ([]model.AppRelease, error) {
	if !filter.PlatformAdmin || filter.TenantID != "" || filter.AppID != "" || filter.Role != "" {
		return nil, store.ErrInvalidInput
	}
	releases := make([]model.AppRelease, 0, len(source.Releases))
	for _, release := range source.Releases {
		if !filter.IncludeRetired && release.Role == model.AppReleaseRoleRetired {
			continue
		}
		if filter.ActiveOnly {
			switch release.Role {
			case model.AppReleaseRoleStable, model.AppReleaseRoleCandidate, model.AppReleaseRolePrevious:
			default:
				continue
			}
			switch release.Status {
			case model.AppReleaseStatusReady, model.AppReleaseStatusServing, model.AppReleaseStatusDraining:
			default:
				continue
			}
		}
		releases = append(releases, release)
	}
	return releases, nil
}

// This diagnostic does not turn runtime-selected targets into desired intent.
// Unsupported migration semantics are explicit issues until a complete
// business/constraint/fact snapshot can be frozen and compared.
func projectBusinessRouteDraft(snapshot model.EdgeRouteIntentSnapshot, apps, observed map[string]model.App, platformRoutes []model.PlatformRoute, routePolicies []model.EdgeRoutePolicy, trafficPolicies []model.AppTrafficPolicy, releases []model.AppRelease, hostedZones []model.HostedZone, dnsRecords []model.DNSRecord, staticRecords []model.EdgeDNSRecord) (platformIntentProjectionResponse, error) {
	result := platformIntentProjectionResponse{SourceGeneration: snapshot.Generation, CapturedAt: snapshot.GeneratedAt,
		Issues:               []platformProjectionIssue{{Code: "transaction_snapshot_not_frozen"}, {Code: "dns_route_placement_not_projected"}, {Code: "dns_acme_not_projected"}},
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
	referencedReleases := make(map[string]bool)
	for _, traffic := range trafficPolicies {
		for _, id := range []string{traffic.StableReleaseID, traffic.CandidateReleaseID} {
			if id != "" {
				referencedReleases[id] = true
			}
		}
	}
	for _, release := range releases {
		if !referencedReleases[release.ID] {
			continue
		}
		status := model.EdgeRouteStatusUnavailable
		if release.Status == model.AppReleaseStatusReady || release.Status == model.AppReleaseStatusServing {
			status = model.EdgeRouteStatusActive
		}
		facts.Releases = append(facts.Releases, platformconfig.ReleaseObservation{ID: release.ID, AppID: release.AppID, TenantID: release.TenantID, ObservedAt: release.UpdatedAt, Status: status, StatusReason: release.StatusReason, UpstreamURL: release.UpstreamURL, RuntimeID: release.RuntimeID, DeploymentGeneration: firstNonEmpty(release.ResolvedImageRef, release.SourceRef)})
	}
	sort.Slice(facts.Releases, func(i, j int) bool { return facts.Releases[i].ID < facts.Releases[j].ID })
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
	intent.DNS, result.DNSExclusions = projectBusinessDNSDraft(&result, apps, hostedZones, dnsRecords, staticRecords)
	// TLS policy is desired route configuration and can be projected without
	// copying certificate readiness or other runtime facts.
	tlsByHost := make(map[string]platformconfig.TLSIntent, len(intent.Routes))
	for _, route := range intent.Routes {
		if strings.TrimSpace(route.TLSPolicy) == "" {
			continue
		}
		tlsByHost[route.Hostname] = platformconfig.TLSIntent{Hostname: route.Hostname, Policy: strings.TrimSpace(route.TLSPolicy)}
	}
	for _, tls := range tlsByHost {
		intent.TLS = append(intent.TLS, tls)
	}
	sort.Slice(intent.TLS, func(i, j int) bool { return intent.TLS[i].Hostname < intent.TLS[j].Hostname })
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
	policy, err := platformconfig.ProjectPolicySnapshot(platformconfig.PolicySnapshot{SchemaVersion: platformconfig.SchemaVersion, Scope: platformconfig.GlobalScopeKey, MinimumHealthyEdges: 1, MaxStaleSeconds: 86400}, routePolicies, trafficPolicies, "policy-draft")
	if err != nil {
		return result, err
	}
	policy.Generation, err = platformconfig.PolicySnapshotGeneration(policy)
	if err != nil {
		return result, err
	}
	facts.PolicyGeneration = policy.Generation
	if len(policy.TrafficConstraints) > 0 {
		// Capturing facts is not equivalent to proving the legacy serving output.
		result.Issues = append(result.Issues, platformProjectionIssue{Code: "release_target_equivalence_not_verified"})
		compiled := make([]platformconfig.CompiledRoute, len(intent.Routes))
		for i, route := range intent.Routes {
			compiled[i].RouteIntent = route
		}
		if _, err := platformconfig.ApplyTrafficPolicyConstraints(compiled, policy, facts); err != nil {
			result.Issues = append(result.Issues, platformProjectionIssue{Code: "release_observations_require_repair"})
		}
	}
	result.Intent, result.Policy, result.RuntimeSnapshot, result.RouteCount = intent, policy, facts, len(intent.Routes)
	return result, nil
}
