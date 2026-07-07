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
)

const (
	safeRolloutEdgeBundleWaitTimeout   = 90 * time.Second
	safeRolloutEdgeBundlePollInterval  = 3 * time.Second
	safeRolloutDrainMetricsLookback    = 20 * time.Minute
	safeRolloutDrainMetricsQueryLimit  = 1000
	safeRolloutDrainMetricsFinalReason = "idle"
)

type safeRolloutEdgeBundleObserver interface {
	WaitForSafeRolloutEdgeRouteBundle(ctx context.Context, app model.App, release model.AppRelease, since time.Time) (safeRolloutEdgeBundleObservation, error)
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
}

type edgeNodeLister interface {
	ListEdgeNodes(edgeGroupID string) ([]model.EdgeNode, []model.EdgeGroup, error)
}

func (o storeSafeRolloutEdgeBundleObserver) WaitForSafeRolloutEdgeRouteBundle(ctx context.Context, app model.App, release model.AppRelease, since time.Time) (safeRolloutEdgeBundleObservation, error) {
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
		observation, err := o.observe(app, release, since)
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

func (o storeSafeRolloutEdgeBundleObserver) observe(app model.App, release model.AppRelease, since time.Time) (safeRolloutEdgeBundleObservation, error) {
	nodes, _, err := o.Store.ListEdgeNodes("")
	if err != nil {
		return safeRolloutEdgeBundleObservation{}, err
	}
	observation := safeRolloutEdgeBundleObservation{
		Ready:         true,
		WaitingNodes:  []string{},
		Summary:       map[string]any{"release_id": release.ID, "app_id": app.ID},
		ObservedNodes: len(nodes),
	}
	versions := map[string]struct{}{}
	now := o.now()
	for _, node := range nodes {
		if !safeRolloutEdgeNodeRelevant(node, now) {
			continue
		}
		observation.RequiredNodes++
		ready, reason := safeRolloutEdgeNodeBundleApplied(node, since)
		version := firstNonEmptyString(strings.TrimSpace(node.RouteBundleVersion), strings.TrimSpace(node.ServingGeneration), strings.TrimSpace(node.CaddyAppliedVersion))
		if version != "" {
			versions[version] = struct{}{}
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
		strings.TrimSpace(node.RouteBundleVersion) != strings.TrimSpace(node.ServingGeneration) {
		return false, "serving_lkg"
	}
	if !since.IsZero() && node.LastHeartbeatAt != nil && node.LastHeartbeatAt.UTC().Before(since.UTC()) {
		return false, "heartbeat_before_promotion"
	}
	return true, ""
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
