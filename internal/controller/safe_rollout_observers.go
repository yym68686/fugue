package controller

import (
	"context"
	"encoding/json"
	"errors"
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
	Store      edgeNodeLister
	Timeout    time.Duration
	Interval   time.Duration
	Sleep      func(context.Context, time.Duration) error
	Now        func() time.Time
	Traffic    safeRolloutTrafficStore
	Probe      safeRolloutRouteProbe
	Membership safeRolloutEdgeMembership
}

type safeRolloutEdgeMembership interface {
	ListMachines(tenantID string, platformAdmin bool) ([]model.Machine, error)
}

// Historical heartbeats do not declare membership. Explicit machine policy can
// remove an Edge role; an unhealthy or absent declared Edge remains required.
func (o storeSafeRolloutEdgeBundleObserver) requiredNodes() ([]model.EdgeNode, error) {
	nodes, _, err := o.Store.ListActiveEdgeNodes("")
	if err != nil || o.Membership == nil {
		return nodes, err
	}
	machines, err := o.Membership.ListMachines("", true)
	if err != nil {
		return nil, fmt.Errorf("read desired Edge membership: %w", err)
	}
	policies := make(map[string]bool, len(machines))
	for _, machine := range machines {
		id := strings.TrimSpace(machine.ClusterNodeName)
		if id == "" {
			continue
		}
		if _, duplicate := policies[id]; duplicate {
			return nil, fmt.Errorf("ambiguous desired Edge membership")
		}
		policies[id] = machine.Policy.AllowEdge
	}
	present := make(map[string]bool, len(nodes))
	out := make([]model.EdgeNode, 0, len(nodes))
	for _, node := range nodes {
		if enabled, explicit := policies[node.ID]; explicit && !enabled {
			continue
		}
		present[node.ID] = true
		out = append(out, node)
	}
	for id, enabled := range policies {
		if enabled && !present[id] {
			return nil, fmt.Errorf("declared Edge %s is missing from inventory", id)
		}
	}
	return out, nil
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
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	deadline := o.now().Add(timeout)
	wait := safeRolloutEdgeWait{groups: map[string]string{}}
	var last safeRolloutEdgeBundleObservation
	for {
		observation, err := o.observeWait(ctx, app, release, targetCandidateWeight, since, &wait)
		if err != nil {
			if (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) && last.Summary != nil {
				return last, err
			}
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

// A wait can add newly serving nodes but cannot forget a node merely because
// it became unhealthy, stopped heartbeating, drained or vanished from inventory.
// These expectations live only for this bounded observation, not in a new ledger.
type safeRolloutEdgeWait struct {
	groups        map[string]string
	trafficDigest string
}

func (o storeSafeRolloutEdgeBundleObserver) observe(ctx context.Context, app model.App, release model.AppRelease, weight int, since time.Time) (safeRolloutEdgeBundleObservation, error) {
	return o.observeWait(ctx, app, release, weight, since, &safeRolloutEdgeWait{groups: map[string]string{}})
}

func (o storeSafeRolloutEdgeBundleObserver) observeWait(ctx context.Context, app model.App, release model.AppRelease, weight int, since time.Time, wait *safeRolloutEdgeWait) (safeRolloutEdgeBundleObservation, error) {
	observation := safeRolloutEdgeBundleObservation{WaitingNodes: []string{}, Summary: map[string]any{"release_id": release.ID, "app_id": app.ID, "target_candidate_weight": weight}}
	digest, err := o.expectedAppTrafficDigest(app, release, weight)
	if err != nil {
		return observation, err
	}
	if wait.trafficDigest == "" {
		wait.trafficDigest = digest
	}
	if digest != wait.trafficDigest {
		return observation, fmt.Errorf("app traffic target changed during edge confirmation")
	}
	observation.Summary["expected_app_traffic_digest"] = digest
	nodes, err := o.requiredNodes()
	if err != nil {
		return observation, err
	}
	observation.ObservedNodes = len(nodes)
	current := make(map[string]model.EdgeNode, len(nodes))
	for _, node := range nodes {
		if node.ID == "" || node.EdgeGroupID == "" {
			return observation, fmt.Errorf("edge inventory identity is missing")
		}
		if _, duplicate := current[node.ID]; duplicate {
			return observation, fmt.Errorf("edge inventory identity is ambiguous")
		}
		current[node.ID] = node
		if _, expected := wait.groups[node.ID]; !expected && !node.Draining {
			wait.groups[node.ID] = node.EdgeGroupID
		}
	}
	ids := make([]string, 0, len(wait.groups))
	for id := range wait.groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	observation.RequiredNodes = len(ids)
	versions := map[string]struct{}{}
	proofs := make(map[string]routeprobe.Proof, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return observation, err
		}
		node, exists := current[id]
		reason := ""
		switch {
		case !exists:
			reason = "edge_missing"
		case node.EdgeGroupID != wait.groups[id]:
			reason = "edge_group_changed"
		case node.Draining:
			reason = "edge_draining"
		case !node.Healthy || node.Status != model.EdgeHealthHealthy:
			reason = "edge_unhealthy"
		case node.LastHeartbeatAt == nil || o.now().Sub(*node.LastHeartbeatAt) > 2*time.Minute:
			reason = "edge_heartbeat_stale"
		case node.LastHeartbeatAt.After(o.now().Add(5 * time.Second)):
			reason = "edge_heartbeat_future"
		default:
			_, reason = safeRolloutEdgeNodeBundleApplied(node, since)
			if reason == "" && (node.RouteBundleVersion == "" || node.CaddyAppliedVersion != node.RouteBundleVersion) {
				reason = "caddy_bundle_not_applied"
			}
		}
		if reason == "" {
			started := o.now()
			proof, probeErr := o.probeNode(ctx, app, node)
			switch {
			case probeErr != nil:
				reason = "app_traffic_probe_failed"
			case proof.AppTrafficDigest != digest:
				reason = "app_traffic_proof_mismatch"
			case proof.EdgeID != id || proof.GroupID != wait.groups[id]:
				reason = "app_traffic_identity_mismatch"
			case !safeRolloutProofPublicationAtLeast(proof.Version, node.RouteBundleVersion):
				reason = "app_traffic_bundle_mismatch"
			case proof.State != "" || proof.CheckedAt.Before(started) || proof.CheckedAt.After(o.now()) || !proof.ValidUntil.After(o.now()):
				reason = "app_traffic_proof_stale"
			default:
				proofs[id] = proof
				versions[proof.Version] = struct{}{}
			}
		}
		if reason != "" {
			observation.WaitingNodes = append(observation.WaitingNodes, id+":"+reason)
		}
	}
	if err := ctx.Err(); err != nil {
		return observation, err
	}
	// Recheck desired traffic after the network calls; no ACK may be reused after
	// the policy or target upstream changed while evidence was being collected.
	currentDigest, err := o.expectedAppTrafficDigest(app, release, weight)
	if err != nil {
		return observation, err
	}
	if currentDigest != digest {
		return observation, fmt.Errorf("app traffic target changed during edge probes")
	}
	// Confirm that the inventory used for this batch still describes serving
	// nodes. A topology change requires another complete observation batch.
	after, err := o.requiredNodes()
	if err != nil {
		return observation, err
	}
	afterByID := make(map[string]model.EdgeNode, len(after))
	for _, node := range after {
		if node.ID == "" || node.EdgeGroupID == "" {
			return observation, fmt.Errorf("edge inventory identity is missing")
		}
		if _, duplicate := afterByID[node.ID]; duplicate {
			return observation, fmt.Errorf("edge inventory identity is ambiguous")
		}
		afterByID[node.ID] = node
		if _, expected := wait.groups[node.ID]; !expected && !node.Draining {
			wait.groups[node.ID] = node.EdgeGroupID
			observation.WaitingNodes = append(observation.WaitingNodes, node.ID+":edge_joined_during_probes")
		}
	}
	observation.RequiredNodes = len(wait.groups)
	for id, proof := range proofs {
		node, exists := afterByID[id]
		before := current[id]
		if !exists || node.EdgeGroupID != wait.groups[id] || node.Draining || !node.Healthy || node.Status != model.EdgeHealthHealthy ||
			!safeRolloutProofPublicationAtLeast(proof.Version, node.RouteBundleVersion) ||
			!safeRolloutProofPublicationAtLeast(node.RouteBundleVersion, before.RouteBundleVersion) ||
			node.CaddyAppliedVersion != node.RouteBundleVersion || node.PublicIPv4 != before.PublicIPv4 || node.PublicIPv6 != before.PublicIPv6 ||
			node.LastHeartbeatAt == nil || node.LastHeartbeatAt.Before(o.now().Add(-2*time.Minute)) || node.LastHeartbeatAt.After(o.now().Add(5*time.Second)) || node.CaddyLastError != "" || node.LastError != "" {
			delete(proofs, id)
			observation.WaitingNodes = append(observation.WaitingNodes, id+":edge_changed_during_probes")
		}
	}
	// A slow later probe must not make an earlier expired proof count as success.
	for _, id := range ids {
		if proof, ok := proofs[id]; ok {
			if proof.ValidUntil.After(o.now()) {
				observation.ReadyNodes++
			} else {
				observation.WaitingNodes = append(observation.WaitingNodes, id+":app_traffic_proof_expired")
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return observation, err
	}
	sort.Strings(observation.WaitingNodes)
	observation.Ready = observation.RequiredNodes > 0 && observation.ReadyNodes == observation.RequiredNodes
	if observation.RequiredNodes == 0 {
		observation.Summary["reason"] = "no edge serving evidence available"
	}
	observation.ServingVersions = sortedStringSet(versions)
	observation.Summary["required_nodes"] = observation.RequiredNodes
	observation.Summary["ready_nodes"] = observation.ReadyNodes
	observation.Summary["waiting_nodes"] = observation.WaitingNodes
	observation.Summary["serving_versions"] = observation.ServingVersions
	return observation, nil
}

// The nonce-bound HTTPS app proof attests the exact locally applied index.
// A healthy inventory heartbeat can lag a newer group publication, even when
// the application's release/weight/upstream digest is unchanged. Never accept
// an older proof, a recovery epoch change, or unordered legacy versions.
func safeRolloutProofPublicationAtLeast(proofVersion, inventoryVersion string) bool {
	if proofVersion == inventoryVersion {
		return proofVersion != ""
	}
	proofSequence, proofEpoch, proofOK := safeRolloutGroupPublicationOrder(proofVersion)
	inventorySequence, inventoryEpoch, inventoryOK := safeRolloutGroupPublicationOrder(inventoryVersion)
	return proofOK && inventoryOK && proofEpoch == inventoryEpoch && proofSequence > inventorySequence
}

func safeRolloutGroupPublicationOrder(version string) (uint64, uint64, bool) {
	const prefix = "edgegroupbundle_"
	if !strings.HasPrefix(version, prefix) || len(version) <= len(prefix)+64 {
		return 0, 0, false
	}
	digest := version[len(prefix) : len(prefix)+64]
	for _, c := range digest {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return 0, 0, false
		}
	}
	suffix, ok := strings.CutPrefix(version[len(prefix)+64:], ".p")
	if !ok {
		return 0, 0, false
	}
	publication, recovery, ok := strings.Cut(suffix, ".r")
	if !ok {
		return 0, 0, false
	}
	sequence, sequenceErr := strconv.ParseUint(publication, 10, 64)
	epoch, epochErr := strconv.ParseUint(recovery, 10, 64)
	return sequence, epoch, sequenceErr == nil && epochErr == nil && sequence > 0 &&
		version == fmt.Sprintf("%s%s.p%d.r%d", prefix, digest, sequence, epoch)
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
	if policy.AppID != app.ID || policy.TenantID != app.TenantID || release.AppID != app.ID || release.TenantID != app.TenantID {
		return "", fmt.Errorf("app traffic policy or target release owner differs")
	}
	if targetCandidateWeight > 0 && policy.Mode != model.AppTrafficModeCanary && policy.Mode != model.AppTrafficModeWeighted {
		return "", fmt.Errorf("app traffic policy mode does not allow candidate traffic")
	}
	if targetCandidateWeight == 0 && policy.Mode != model.AppTrafficModeSingle {
		return "", fmt.Errorf("app traffic policy mode does not identify a promoted stable")
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
	target, ok := byID[release.ID]
	if !ok || target.UpstreamURL != release.UpstreamURL || target.RuntimeID != release.RuntimeID || firstNonEmptyString(target.ResolvedImageRef, target.SourceRef) != firstNonEmptyString(release.ResolvedImageRef, release.SourceRef) {
		return "", fmt.Errorf("app release target changed from rollout state")
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
