package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
)

const (
	safeRolloutEdgeBundleWaitTimeout   = 90 * time.Second
	safeRolloutEdgeBundlePollInterval  = 3 * time.Second
	safeRolloutDrainMetricsLookback    = 20 * time.Minute
	safeRolloutDrainMetricsQueryLimit  = 1000
	safeRolloutDrainMetricsFinalReason = "idle"
)

type safeRolloutEdgeBundleObserver interface {
	WaitForSafeRolloutEdgeRouteBundle(ctx context.Context, app model.App, release model.AppRelease, targetCandidateWeight int, since time.Time) (safeRolloutEdgeBundleObservation, error)
}

type safeRolloutEdgeBundleObservation struct {
	Ready           bool
	RequiredNodes   int
	ReadyNodes      int
	ObservedNodes   int
	WaitingNodes    []string
	ServingVersions []string
	Summary         map[string]any
}

type safeRolloutTrafficStore interface {
	GetAppTrafficPolicy(tenantID string, platformAdmin bool, appID string) (model.AppTrafficPolicy, error)
	ListAppReleases(filter model.AppReleaseFilter) ([]model.AppRelease, error)
}

type safeRolloutRouteProbe func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error)

type safeRolloutDrainMetricsQuerier interface {
	QuerySafeRolloutDrainMetrics(ctx context.Context, app model.App, previous model.AppRelease, since time.Time) (safeRolloutDrainMetrics, error)
}

type safeRolloutDrainMetrics struct {
	Ready                bool
	ActiveConnections    int
	MaxActiveConnections int
	SampleCount          int
	FinalCount           int
	ObserverErrors       int
	Source               string
	ObservedAt           time.Time
	Summary              map[string]any
}

type storeSafeRolloutEdgeBundleObserver struct {
	Store    edgeNodeLister
	Timeout  time.Duration
	Interval time.Duration
	Sleep    func(context.Context, time.Duration) error
	Now      func() time.Time
	Traffic  safeRolloutTrafficStore
	Probe    safeRolloutRouteProbe
}

type edgeNodeLister interface {
	ListActiveEdgeNodes(edgeGroupID string) ([]model.EdgeNode, []model.EdgeGroup, error)
}

func (o storeSafeRolloutEdgeBundleObserver) WaitForSafeRolloutEdgeRouteBundle(ctx context.Context, app model.App, release model.AppRelease, targetCandidateWeight int, since time.Time) (safeRolloutEdgeBundleObservation, error) {
	if o.Store == nil {
		return safeRolloutEdgeBundleObservation{}, fmt.Errorf("edge node store is not configured")
	}
	timeout := o.Timeout
	if timeout <= 0 {
		timeout = safeRolloutEdgeBundleWaitTimeout
	}
	interval := o.Interval
	if interval <= 0 {
		interval = safeRolloutEdgeBundlePollInterval
	}
	deadline := o.now().Add(timeout)
	var last safeRolloutEdgeBundleObservation
	for {
		observation, err := o.observe(ctx, app, release, targetCandidateWeight, since)
		if err != nil {
			return observation, err
		}
		last = observation
		if observation.Ready || !o.now().Before(deadline) {
			return observation, nil
		}
		if err := o.sleep(ctx, interval); err != nil {
			return last, err
		}
	}
}

func (o storeSafeRolloutEdgeBundleObserver) observe(ctx context.Context, app model.App, release model.AppRelease, targetCandidateWeight int, since time.Time) (safeRolloutEdgeBundleObservation, error) {
	nodes, _, err := o.Store.ListActiveEdgeNodes("")
	if err != nil {
		return safeRolloutEdgeBundleObservation{}, err
	}
	now := o.now()
	relevant := make([]model.EdgeNode, 0, len(nodes))
	for _, node := range nodes {
		if safeRolloutEdgeNodeRelevant(node, now) {
			relevant = append(relevant, node)
		}
	}
	if len(relevant) == 0 {
		return safeRolloutEdgeBundleObservation{Ready: true, ObservedNodes: len(nodes), WaitingNodes: []string{}, Summary: map[string]any{"release_id": release.ID, "app_id": app.ID, "reason": "no active edge route nodes require bundle confirmation"}}, nil
	}
	trafficDigest := ""
	if o.Traffic != nil {
		var err error
		trafficDigest, err = o.expectedAppTrafficDigest(app, release, targetCandidateWeight)
		if err != nil {
			return safeRolloutEdgeBundleObservation{}, err
		}
	}
	observation := safeRolloutEdgeBundleObservation{
		Ready:         true,
		WaitingNodes:  []string{},
		Summary:       map[string]any{"release_id": release.ID, "app_id": app.ID, "target_candidate_weight": targetCandidateWeight, "expected_app_traffic_digest": trafficDigest},
		ObservedNodes: len(nodes),
		RequiredNodes: len(relevant),
	}
	versions := map[string]struct{}{}
	for _, node := range relevant {
		ready, reason := safeRolloutEdgeNodeBundleApplied(node, since)
		version := firstNonEmptyString(strings.TrimSpace(node.RouteBundleVersion), strings.TrimSpace(node.ServingGeneration), strings.TrimSpace(node.CaddyAppliedVersion))
		if version != "" {
			versions[version] = struct{}{}
		}
		if ready && trafficDigest != "" {
			proof, probeErr := o.probeNode(ctx, app, node)
			if probeErr != nil {
				ready, reason = false, "app_traffic_probe_failed"
			} else if proof.AppTrafficDigest != trafficDigest {
				ready, reason = false, "app_traffic_proof_mismatch"
			} else if proof.EdgeID != node.ID || proof.GroupID != node.EdgeGroupID {
				ready, reason = false, "app_traffic_identity_mismatch"
			} else if strings.TrimSpace(node.RouteBundleVersion) != "" && proof.Version != strings.TrimSpace(node.RouteBundleVersion) {
				ready, reason = false, "app_traffic_bundle_mismatch"
			}
		}
		if ready {
			observation.ReadyNodes++
			continue
		}
		observation.Ready = false
		observation.WaitingNodes = append(observation.WaitingNodes, node.ID+":"+reason)
	}
	if observation.RequiredNodes == 0 {
		observation.Ready = true
		observation.Summary["reason"] = "no active edge route nodes require bundle confirmation"
	}
	observation.ServingVersions = sortedStringSet(versions)
	observation.Summary["required_nodes"] = observation.RequiredNodes
	observation.Summary["ready_nodes"] = observation.ReadyNodes
	observation.Summary["waiting_nodes"] = observation.WaitingNodes
	observation.Summary["serving_versions"] = observation.ServingVersions
	return observation, nil
}

func (o storeSafeRolloutEdgeBundleObserver) expectedAppTrafficDigest(app model.App, release model.AppRelease, targetCandidateWeight int) (string, error) {
	if o.Traffic == nil {
		return "", fmt.Errorf("traffic policy store is not configured")
	}
	if app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "" {
		return "", fmt.Errorf("safe rollout app route is missing")
	}
	policy, err := o.Traffic.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil {
		return "", fmt.Errorf("read app traffic policy for edge proof: %w", err)
	}
	if targetCandidateWeight < 0 || targetCandidateWeight > 100 || policy.CandidateWeight != targetCandidateWeight {
		return "", fmt.Errorf("app traffic policy candidate weight does not match rollout target")
	}
	if targetCandidateWeight > 0 && (policy.CandidateReleaseID != release.ID || policy.StableReleaseID == "" || policy.StableWeight+policy.CandidateWeight != 100) {
		return "", fmt.Errorf("app traffic policy release identities do not match candidate rollout")
	}
	if targetCandidateWeight == 0 && (policy.StableReleaseID != release.ID || policy.StableWeight != 100 || policy.CandidateReleaseID != "") {
		return "", fmt.Errorf("app traffic policy does not identify promoted stable release")
	}
	if policy.StickyHeader != "" || (policy.StickyCookie != "" && policy.StickyCookie != "Fugue-Release-Stickiness") {
		return "", fmt.Errorf("app traffic policy uses unsupported sticky semantics")
	}
	releases, err := o.Traffic.ListAppReleases(model.AppReleaseFilter{TenantID: app.TenantID, AppID: app.ID, IncludeRetired: false, PlatformAdmin: true})
	if err != nil {
		return "", fmt.Errorf("read app releases for edge proof: %w", err)
	}
	byID := make(map[string]model.AppRelease, len(releases))
	for _, item := range releases {
		byID[item.ID] = item
	}
	upstreams := make([]model.EdgeRouteUpstream, 0, 2)
	for _, target := range []struct {
		id, role string
		weight   int
	}{{policy.StableReleaseID, model.AppReleaseRoleStable, policy.StableWeight}, {policy.CandidateReleaseID, model.AppReleaseRoleCandidate, policy.CandidateWeight}} {
		if target.weight <= 0 {
			continue
		}
		item, ok := byID[target.id]
		if !ok || item.AppID != app.ID || item.TenantID != app.TenantID || strings.TrimSpace(item.UpstreamURL) == "" {
			return "", fmt.Errorf("app traffic policy references an unavailable release")
		}
		upstreams = append(upstreams, model.EdgeRouteUpstream{Role: target.role, ReleaseID: item.ID, Weight: target.weight, UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService, UpstreamURL: item.UpstreamURL, ServicePort: app.Route.ServicePort, RuntimeID: item.RuntimeID, DeploymentGeneration: firstNonEmptyString(item.ResolvedImageRef, item.SourceRef)})
	}
	route := model.EdgeRouteBinding{Hostname: app.Route.Hostname, PathPrefix: app.Route.PathPrefix, AppID: app.ID, TenantID: app.TenantID, Upstreams: upstreams}
	digest, err := routeproof.AppTrafficDigest(route)
	if err != nil {
		return "", fmt.Errorf("build expected app traffic proof: %w", err)
	}
	return digest, nil
}

func (o storeSafeRolloutEdgeBundleObserver) probeNode(ctx context.Context, app model.App, node model.EdgeNode) (routeprobe.Proof, error) {
	if app.Route == nil {
		return routeprobe.Proof{}, fmt.Errorf("safe rollout app route is missing")
	}
	address := strings.TrimSpace(node.PublicIPv4)
	if address == "" {
		address = strings.TrimSpace(node.PublicIPv6)
	}
	if address == "" {
		return routeprobe.Proof{}, fmt.Errorf("edge node has no public probe address")
	}
	probe := o.Probe
	if probe == nil {
		probe = routeprobe.Probe
	}
	return probe(ctx, app.Route.Hostname, model.NormalizeAppRoutePathPrefix(app.Route.PathPrefix), address, "", 5*time.Second)
}

func safeRolloutEdgeNodeRelevant(node model.EdgeNode, now time.Time) bool {
	if strings.TrimSpace(node.ID) == "" || node.Draining {
		return false
	}
	if !node.Healthy || strings.TrimSpace(node.Status) != model.EdgeHealthHealthy {
		return false
	}
	if node.CaddyRouteCount <= 0 &&
		strings.TrimSpace(node.RouteBundleVersion) == "" &&
		strings.TrimSpace(node.ServingGeneration) == "" &&
		strings.TrimSpace(node.CaddyAppliedVersion) == "" {
		return false
	}
	if node.LastHeartbeatAt == nil {
		return false
	}
	return now.Sub(node.LastHeartbeatAt.UTC()) <= 2*time.Minute
}

func safeRolloutEdgeNodeBundleApplied(node model.EdgeNode, since time.Time) (bool, string) {
	version := firstNonEmptyString(strings.TrimSpace(node.RouteBundleVersion), strings.TrimSpace(node.ServingGeneration), strings.TrimSpace(node.CaddyAppliedVersion))
	if version == "" {
		return false, "missing_route_generation"
	}
	if strings.TrimSpace(node.CaddyLastError) != "" || strings.TrimSpace(node.LastError) != "" {
		return false, "edge_reports_error"
	}
	if strings.TrimSpace(node.ServingGeneration) != "" &&
		strings.TrimSpace(node.LKGGeneration) != "" &&
		strings.TrimSpace(node.RouteBundleVersion) != "" &&
		strings.TrimSpace(node.ServingGeneration) == strings.TrimSpace(node.LKGGeneration) &&
		!safeRolloutEdgePublicationMatchesServingGeneration(node) {
		return false, "serving_lkg"
	}
	if !since.IsZero() && node.LastHeartbeatAt != nil && node.LastHeartbeatAt.UTC().Before(since.UTC()) {
		return false, "heartbeat_before_promotion"
	}
	return true, ""
}

func safeRolloutEdgePublicationMatchesServingGeneration(node model.EdgeNode) bool {
	version := strings.TrimSpace(node.RouteBundleVersion)
	generation := strings.TrimSpace(node.ServingGeneration)
	if version == generation {
		return true
	}
	// Group authority publications identify both the content generation and
	// the publication/recovery sequence. Edge serving/LKG status reports only
	// the content generation. Accept that representation only when the proxy
	// confirms the exact publication and its suffix has the canonical format
	// emitted by edgecontrol.groupPublicationVersion.
	if generation == "" || strings.TrimSpace(node.CaddyAppliedVersion) != version {
		return false
	}
	suffix, ok := strings.CutPrefix(version, generation+".p")
	if !ok {
		return false
	}
	publication, recovery, ok := strings.Cut(suffix, ".r")
	if !ok {
		return false
	}
	sequence, sequenceErr := strconv.ParseUint(publication, 10, 64)
	epoch, epochErr := strconv.ParseUint(recovery, 10, 64)
	return sequenceErr == nil && epochErr == nil && sequence > 0 &&
		version == fmt.Sprintf("%s.p%d.r%d", generation, sequence, epoch)
}

func (o storeSafeRolloutEdgeBundleObserver) sleep(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	if o.Sleep != nil {
		return o.Sleep(ctx, delay)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (o storeSafeRolloutEdgeBundleObserver) now() time.Time {
	if o.Now != nil {
		return o.Now().UTC()
	}
	return time.Now().UTC()
}

type lokiSafeRolloutDrainMetricsQuerier struct {
	Config observability.Config
	Client *http.Client
	Now    func() time.Time
}

func controllerSafeRolloutDrainMetricsQuerier(cfg config.ControllerConfig) safeRolloutDrainMetricsQuerier {
	observabilityConfig := cfg.Observability.Normalize()
	if strings.TrimSpace(observabilityConfig.LokiURL) == "" {
		return nil
	}
	return lokiSafeRolloutDrainMetricsQuerier{Config: observabilityConfig}
}

func (q lokiSafeRolloutDrainMetricsQuerier) QuerySafeRolloutDrainMetrics(ctx context.Context, app model.App, previous model.AppRelease, since time.Time) (safeRolloutDrainMetrics, error) {
	cfg := q.Config.Normalize()
	endpoint, err := safeRolloutNormalizeLokiQueryRangeURL(cfg.LokiURL)
	if err != nil {
		return safeRolloutDrainMetrics{}, err
	}
	until := q.now()
	if since.IsZero() {
		since = until.Add(-safeRolloutDrainMetricsLookback)
	}
	if until.Sub(since) > safeRolloutDrainMetricsLookback {
		since = until.Add(-safeRolloutDrainMetricsLookback)
	}
	values := url.Values{}
	values.Set("query", safeRolloutDrainLogQL(app.ID))
	values.Set("start", strconv.FormatInt(since.UTC().UnixNano(), 10))
	values.Set("end", strconv.FormatInt(until.UTC().UnixNano(), 10))
	values.Set("limit", strconv.Itoa(safeRolloutDrainMetricsQueryLimit))
	endpoint.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return safeRolloutDrainMetrics{}, fmt.Errorf("build Loki drain query request: %w", err)
	}
	client := q.Client
	if client == nil {
		client = &http.Client{Timeout: cfg.ExportTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return safeRolloutDrainMetrics{}, fmt.Errorf("query Loki drain metrics: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, cfg.MaxPayloadBytes))
	if err != nil {
		return safeRolloutDrainMetrics{}, fmt.Errorf("read Loki drain metrics response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return safeRolloutDrainMetrics{}, fmt.Errorf("query Loki drain metrics returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var payload safeRolloutLokiQueryRangeResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return safeRolloutDrainMetrics{}, fmt.Errorf("decode Loki drain metrics response: %w", err)
	}
	if payload.Status != "" && payload.Status != "success" {
		return safeRolloutDrainMetrics{}, fmt.Errorf("query Loki drain metrics failed: %s", firstNonEmptyString(payload.Error, payload.Status))
	}
	metrics := safeRolloutDrainMetrics{
		Source:     "loki",
		ObservedAt: until,
		Summary: map[string]any{
			"release_id":          previous.ID,
			"previous_deployment": previous.DeploymentName,
		},
	}
	for _, stream := range payload.Data.Result {
		for _, value := range stream.Values {
			if len(value) < 2 {
				continue
			}
			safeRolloutApplyDrainLogLine(&metrics, value[1])
		}
	}
	unsafeFinalReason, _ := metrics.Summary["unsafe_final_reason"].(bool)
	metrics.Ready = metrics.FinalCount > 0 && metrics.ActiveConnections == 0 && !unsafeFinalReason
	metrics.Summary["ready"] = metrics.Ready
	metrics.Summary["active_connections"] = metrics.ActiveConnections
	metrics.Summary["max_active_connections"] = metrics.MaxActiveConnections
	metrics.Summary["sample_count"] = metrics.SampleCount
	metrics.Summary["final_count"] = metrics.FinalCount
	metrics.Summary["observer_errors"] = metrics.ObserverErrors
	return metrics, nil
}

func (q lokiSafeRolloutDrainMetricsQuerier) now() time.Time {
	if q.Now != nil {
		return q.Now().UTC()
	}
	return time.Now().UTC()
}

type safeRolloutLokiQueryRangeResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		Result []struct {
			Stream map[string]string `json:"stream"`
			Values [][2]string       `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func safeRolloutDrainLogQL(appID string) string {
	labels := []string{`app_id="` + safeRolloutEscapeLogQLString(appID) + `"`}
	return "{" + strings.Join(labels, ",") + `} |= "fugue_drain_"`
}

func safeRolloutEscapeLogQLString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func safeRolloutNormalizeLokiQueryRangeURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid Loki URL")
	}
	cleanPath := strings.TrimRight(parsed.Path, "/")
	if !strings.HasSuffix(cleanPath, "/loki/api/v1/query_range") {
		if strings.HasSuffix(cleanPath, "/loki/api/v1/push") {
			cleanPath = strings.TrimSuffix(cleanPath, "/push") + "/query_range"
		} else {
			cleanPath = strings.TrimRight(cleanPath, "/") + "/loki/api/v1/query_range"
		}
	}
	parsed.Path = cleanPath
	parsed.RawQuery = ""
	return parsed, nil
}

func safeRolloutApplyDrainLogLine(metrics *safeRolloutDrainMetrics, message string) {
	if metrics == nil {
		return
	}
	message = strings.TrimSpace(message)
	if !strings.Contains(message, "fugue_drain_") {
		return
	}
	values := safeRolloutParseDrainKeyValues(message)
	if strings.Contains(message, "fugue_drain_sample") {
		metrics.SampleCount++
		if active, ok := parseSafeRolloutInt(values["active_connections"]); ok {
			metrics.ActiveConnections = active
			if active > metrics.MaxActiveConnections {
				metrics.MaxActiveConnections = active
			}
		}
	}
	if strings.Contains(message, "fugue_drain_complete") {
		metrics.FinalCount++
		if active, ok := parseSafeRolloutInt(values["active_connections"]); ok {
			metrics.ActiveConnections = active
		}
		if maxActive, ok := parseSafeRolloutInt(values["max_active_connections"]); ok && maxActive > metrics.MaxActiveConnections {
			metrics.MaxActiveConnections = maxActive
		}
		if observerErrors, ok := parseSafeRolloutInt(values["observer_errors"]); ok {
			metrics.ObserverErrors += observerErrors
		}
		if reason := strings.TrimSpace(values["reason"]); reason != "" {
			metrics.Summary["final_reason"] = reason
			if reason != safeRolloutDrainMetricsFinalReason && reason != "observer_error_open" {
				metrics.Summary["unsafe_final_reason"] = true
				metrics.Ready = false
			}
		}
	}
}

func safeRolloutParseDrainKeyValues(message string) map[string]string {
	out := map[string]string{}
	for _, field := range strings.Fields(message) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"`)
		if key != "" {
			out[key] = value
		}
	}
	return out
}

func parseSafeRolloutInt(raw string) (int, bool) {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, false
	}
	return value, true
}

func sortedStringSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}
