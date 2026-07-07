package releaseflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	DefaultAppReleaseProbeTimeout    = 10 * time.Second
	DefaultAppReleaseGateWindow      = 10 * time.Minute
	DefaultAppReleaseGateMinRequests = 1
)

type ReleaseGateMetricsQuerier interface {
	QueryReleaseGateMetrics(ctx context.Context, appID, releaseID, releaseRole string, window time.Duration) (map[string]any, error)
}

type ReleaseGateRawMetricsQuerier interface {
	QueryReleaseGateRawMetrics(ctx context.Context, appID, releaseID, releaseRole string, window time.Duration) (map[string]any, error)
}

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type ReleaseGateEvaluator struct {
	MetricsQuerier ReleaseGateMetricsQuerier
	HTTPClient     HTTPDoer
	Now            func() time.Time
}

func (e ReleaseGateEvaluator) NormalizePolicy(raw *model.AppReleaseGatePolicy) model.AppReleaseGatePolicy {
	policy := model.AppReleaseGatePolicy{
		WindowSeconds:              int(DefaultAppReleaseGateWindow.Seconds()),
		MinCandidateRequests:       DefaultAppReleaseGateMinRequests,
		Max5xxRate:                 0.01,
		MaxEdgeUpstreamErrorRate:   0.005,
		MaxP95TTFBMilliseconds:     2000,
		MaxP99DurationMilliseconds: 30000,
	}
	if raw == nil {
		return policy
	}
	if raw.WindowSeconds > 0 {
		policy.WindowSeconds = raw.WindowSeconds
	}
	if raw.MinCandidateRequests > 0 {
		policy.MinCandidateRequests = raw.MinCandidateRequests
	}
	if raw.Max5xxRate > 0 {
		policy.Max5xxRate = raw.Max5xxRate
	}
	if raw.MaxEdgeUpstreamErrorRate > 0 {
		policy.MaxEdgeUpstreamErrorRate = raw.MaxEdgeUpstreamErrorRate
	}
	if raw.MaxP95TTFBMilliseconds > 0 {
		policy.MaxP95TTFBMilliseconds = raw.MaxP95TTFBMilliseconds
	}
	if raw.MaxP99DurationMilliseconds > 0 {
		policy.MaxP99DurationMilliseconds = raw.MaxP99DurationMilliseconds
	}
	if len(raw.Probes) > 0 {
		policy.Probes = raw.Probes
	}
	return policy
}

func (e ReleaseGateEvaluator) Evaluate(ctx context.Context, app model.App, release model.AppRelease, policy model.AppReleaseGatePolicy) model.AppReleaseGateResult {
	gate := model.AppReleaseGateResult{
		Status:      model.AppReleaseGateStatusPass,
		ReleaseID:   release.ID,
		Role:        release.Role,
		Evidence:    []string{},
		Warnings:    []string{},
		Failures:    []string{},
		Metrics:     map[string]any{},
		EvaluatedAt: e.now(),
	}
	window := time.Duration(policy.WindowSeconds) * time.Second
	if window <= 0 {
		window = DefaultAppReleaseGateWindow
	}
	gate.Window = window.String()
	if e.MetricsQuerier != nil {
		if metrics, err := e.MetricsQuerier.QueryReleaseGateMetrics(ctx, app.ID, release.ID, release.Role, window); err == nil {
			gate.Metrics = metrics
			gate.Evidence = append(gate.Evidence, ReleaseGateMetricEvidence(metrics)...)
			gate.Failures = append(gate.Failures, ReleaseGateMetricFailures(metrics, policy)...)
		} else {
			gate.Warnings = append(gate.Warnings, "passive release metrics unavailable: "+err.Error())
		}
	} else {
		gate.Warnings = append(gate.Warnings, "passive release metrics unavailable: metrics querier is not configured")
	}
	if len(policy.Probes) > 0 {
		probeResults := e.RunProbes(ctx, release, policy.Probes)
		gate.ProbeResults = probeResults
		for _, result := range probeResults {
			if result.Status == model.AppReleaseGateStatusFail {
				gate.Failures = append(gate.Failures, fmt.Sprintf("probe %s failed: %s", firstNonEmpty(result.Name, result.Path), result.Error))
			}
		}
	}
	if len(gate.Failures) > 0 {
		gate.Status = model.AppReleaseGateStatusFail
	}
	return gate
}

func (e ReleaseGateEvaluator) RunProbes(ctx context.Context, release model.AppRelease, probes []model.AppReleaseProbe) []model.AppReleaseProbeResult {
	if len(probes) == 0 {
		probes = DefaultReleaseProbes()
	}
	results := make([]model.AppReleaseProbeResult, 0, len(probes))
	for _, probe := range probes {
		results = append(results, e.RunProbe(ctx, release, probe))
	}
	return results
}

func (e ReleaseGateEvaluator) RunProbe(ctx context.Context, release model.AppRelease, probe model.AppReleaseProbe) model.AppReleaseProbeResult {
	result := model.AppReleaseProbeResult{Name: probe.Name, Path: probe.Path, Status: model.AppReleaseGateStatusFail}
	base := strings.TrimRight(strings.TrimSpace(release.UpstreamURL), "/")
	if base == "" {
		result.Error = "release upstream_url is empty"
		return result
	}
	timeout := time.Duration(probe.TimeoutMilliseconds) * time.Millisecond
	if timeout <= 0 {
		timeout = DefaultAppReleaseProbeTimeout
	}
	method := strings.TrimSpace(probe.Method)
	if method == "" {
		method = http.MethodGet
	}
	body := io.Reader(nil)
	if probe.Body != "" {
		body = bytes.NewBufferString(probe.Body)
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, method, base+"/"+strings.TrimLeft(probe.Path, "/"), body)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	for key, value := range probe.Headers {
		if strings.TrimSpace(key) != "" {
			req.Header.Set(strings.TrimSpace(key), value)
		}
	}
	if probe.Body != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	started := e.now()
	var firstByteAt time.Time
	trace := &httptrace.ClientTrace{
		GotFirstResponseByte: func() {
			firstByteAt = e.now()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	client := e.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		result.DurationMillis = e.now().Sub(started).Milliseconds()
		result.Error = err.Error()
		return result
	}
	defer resp.Body.Close()
	result.StatusCode = resp.StatusCode
	expected := probe.ExpectedStatus
	if expected == 0 {
		expected = http.StatusOK
	}
	limit := int64(4096)
	if strings.EqualFold(probe.Kind, model.AppReleaseProbeKindHTTPStream) || probe.ExpectStreamEventBeforeMillis > 0 {
		limit = 1
	}
	payload, readErr := io.ReadAll(io.LimitReader(resp.Body, limit))
	result.DurationMillis = e.now().Sub(started).Milliseconds()
	if !firstByteAt.IsZero() {
		result.TTFBMillis = firstByteAt.Sub(started).Milliseconds()
	}
	if readErr != nil {
		result.Error = readErr.Error()
		return result
	}
	if resp.StatusCode != expected {
		result.Error = fmt.Sprintf("expected status %d, got %d", expected, resp.StatusCode)
		return result
	}
	if probe.ExpectedContentType != "" && !strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), strings.ToLower(probe.ExpectedContentType)) {
		result.Error = "response content-type did not match"
		return result
	}
	if probe.ExpectedBodyContains != "" && !strings.Contains(string(payload), probe.ExpectedBodyContains) {
		result.Error = "response body did not contain expected text"
		return result
	}
	if probe.MaxTTFBMilliseconds > 0 && result.TTFBMillis > int64(probe.MaxTTFBMilliseconds) {
		result.Error = fmt.Sprintf("ttfb %dms exceeded %dms", result.TTFBMillis, probe.MaxTTFBMilliseconds)
		return result
	}
	if probe.MaxDurationMilliseconds > 0 && result.DurationMillis > int64(probe.MaxDurationMilliseconds) {
		result.Error = fmt.Sprintf("duration %dms exceeded %dms", result.DurationMillis, probe.MaxDurationMilliseconds)
		return result
	}
	if probe.ExpectStreamEventBeforeMillis > 0 && result.TTFBMillis > int64(probe.ExpectStreamEventBeforeMillis) {
		result.Error = fmt.Sprintf("stream first byte %dms exceeded %dms", result.TTFBMillis, probe.ExpectStreamEventBeforeMillis)
		return result
	}
	result.Status = model.AppReleaseGateStatusPass
	result.Evidence = fmt.Sprintf("status=%d ttfb_ms=%d duration_ms=%d", result.StatusCode, result.TTFBMillis, result.DurationMillis)
	return result
}

func DefaultReleaseProbes() []model.AppReleaseProbe {
	return []model.AppReleaseProbe{
		{Name: "health", Kind: model.AppReleaseProbeKindHTTP, Method: http.MethodGet, Path: "/v1/health", ExpectedStatus: http.StatusOK, TimeoutMilliseconds: 3000, MaxDurationMilliseconds: 3000},
	}
}

func ReleaseGateMetricEvidence(metrics map[string]any) []string {
	if len(metrics) == 0 {
		return nil
	}
	return []string{fmt.Sprintf("release requests=%0.f 5xx_rate=%.4f upstream_error_rate=%.4f p95_ttfb_ms=%.0f p99_duration_ms=%.0f",
		FloatMetric(metrics, "request_count"),
		FloatMetric(metrics, "error_5xx_rate"),
		FloatMetric(metrics, "edge_upstream_error_rate"),
		FloatMetric(metrics, "p95_ttfb_ms"),
		FloatMetric(metrics, "p99_duration_ms"),
	)}
}

func ReleaseGateMetricFailures(metrics map[string]any, policy model.AppReleaseGatePolicy) []string {
	failures := []string{}
	if policy.MinCandidateRequests > 0 && FloatMetric(metrics, "request_count") < float64(policy.MinCandidateRequests) {
		failures = append(failures, fmt.Sprintf("release request count %.0f is below minimum %d", FloatMetric(metrics, "request_count"), policy.MinCandidateRequests))
	}
	if policy.Max5xxRate > 0 && FloatMetric(metrics, "error_5xx_rate") > policy.Max5xxRate {
		failures = append(failures, fmt.Sprintf("5xx rate %.4f exceeds %.4f", FloatMetric(metrics, "error_5xx_rate"), policy.Max5xxRate))
	}
	if policy.MaxEdgeUpstreamErrorRate > 0 && FloatMetric(metrics, "edge_upstream_error_rate") > policy.MaxEdgeUpstreamErrorRate {
		failures = append(failures, fmt.Sprintf("edge upstream error rate %.4f exceeds %.4f", FloatMetric(metrics, "edge_upstream_error_rate"), policy.MaxEdgeUpstreamErrorRate))
	}
	if policy.MaxP95TTFBMilliseconds > 0 && FloatMetric(metrics, "p95_ttfb_ms") > float64(policy.MaxP95TTFBMilliseconds) {
		failures = append(failures, fmt.Sprintf("p95 ttfb %.0fms exceeds %dms", FloatMetric(metrics, "p95_ttfb_ms"), policy.MaxP95TTFBMilliseconds))
	}
	if policy.MaxP99DurationMilliseconds > 0 && FloatMetric(metrics, "p99_duration_ms") > float64(policy.MaxP99DurationMilliseconds) {
		failures = append(failures, fmt.Sprintf("p99 duration %.0fms exceeds %dms", FloatMetric(metrics, "p99_duration_ms"), policy.MaxP99DurationMilliseconds))
	}
	return failures
}

func BuildReleaseGateComparisonMetrics(candidateMetrics, stableMetrics map[string]any) map[string]any {
	comparison := map[string]any{}
	for key, value := range candidateMetrics {
		comparison["candidate_"+key] = value
	}
	for key, value := range stableMetrics {
		comparison["stable_"+key] = value
	}
	comparison["request_count"] = FloatMetric(candidateMetrics, "request_count")
	comparison["error_5xx_rate"] = FloatMetric(candidateMetrics, "error_5xx_rate")
	comparison["edge_upstream_error_rate"] = FloatMetric(candidateMetrics, "edge_upstream_error_rate")
	comparison["p95_ttfb_ms"] = FloatMetric(candidateMetrics, "p95_ttfb_ms")
	comparison["p99_duration_ms"] = FloatMetric(candidateMetrics, "p99_duration_ms")
	comparison["has_status_class_counts"] = BoolMetric(candidateMetrics, "has_status_class_counts") && BoolMetric(stableMetrics, "has_status_class_counts")
	return comparison
}

func ReleaseGateComparisonEvidence(metrics map[string]any) []string {
	if len(metrics) == 0 {
		return nil
	}
	return []string{fmt.Sprintf("release comparison candidate_requests=%.0f stable_requests=%.0f candidate_5xx_rate=%.4f stable_5xx_rate=%.4f candidate_2xx_rate=%.4f stable_2xx_rate=%.4f candidate_p95_ttfb_ms=%.0f stable_p95_ttfb_ms=%.0f",
		FloatMetric(metrics, "candidate_request_count"),
		FloatMetric(metrics, "stable_request_count"),
		FloatMetric(metrics, "candidate_error_5xx_rate"),
		FloatMetric(metrics, "stable_error_5xx_rate"),
		FloatMetric(metrics, "candidate_status_2xx_rate"),
		FloatMetric(metrics, "stable_status_2xx_rate"),
		FloatMetric(metrics, "candidate_p95_ttfb_ms"),
		FloatMetric(metrics, "stable_p95_ttfb_ms"),
	)}
}

func ReleaseGateComparisonFailures(metrics map[string]any, policy model.AppReleaseGatePolicy) []string {
	failures := []string{}
	candidateRequests := FloatMetric(metrics, "candidate_request_count")
	stableRequests := FloatMetric(metrics, "stable_request_count")
	if candidateRequests <= 0 {
		return failures
	}
	if stableRequests <= 0 {
		failures = append(failures, "stable baseline request count 0 is below minimum 1 for comparative release gate")
		return failures
	}
	stable5xx := FloatMetric(metrics, "stable_error_5xx_rate")
	candidate5xx := FloatMetric(metrics, "candidate_error_5xx_rate")
	if allowed := stable5xx + maxFloat64(0.005, stable5xx*0.5); candidate5xx > allowed {
		failures = append(failures, fmt.Sprintf("candidate 5xx rate %.4f is worse than stable %.4f by more than allowed delta", candidate5xx, stable5xx))
	}
	stableUpstream := FloatMetric(metrics, "stable_edge_upstream_error_rate")
	candidateUpstream := FloatMetric(metrics, "candidate_edge_upstream_error_rate")
	if allowed := stableUpstream + maxFloat64(0.003, stableUpstream*0.5); candidateUpstream > allowed {
		failures = append(failures, fmt.Sprintf("candidate upstream error rate %.4f is worse than stable %.4f by more than allowed delta", candidateUpstream, stableUpstream))
	}
	if BoolMetric(metrics, "has_status_class_counts") {
		stable2xx := FloatMetric(metrics, "stable_status_2xx_rate")
		candidate2xx := FloatMetric(metrics, "candidate_status_2xx_rate")
		if stable2xx >= 0.80 && candidate2xx+0.05 < stable2xx {
			failures = append(failures, fmt.Sprintf("candidate 2xx rate %.4f is lower than stable %.4f by more than 0.0500", candidate2xx, stable2xx))
		}
	}
	stableP95 := FloatMetric(metrics, "stable_p95_ttfb_ms")
	candidateP95 := FloatMetric(metrics, "candidate_p95_ttfb_ms")
	if stableP95 > 0 && candidateP95 > stableP95*1.75+250 {
		failures = append(failures, fmt.Sprintf("candidate p95 ttfb %.0fms is worse than stable %.0fms", candidateP95, stableP95))
	}
	stableP99 := FloatMetric(metrics, "stable_p99_duration_ms")
	candidateP99 := FloatMetric(metrics, "candidate_p99_duration_ms")
	if stableP99 > 0 && candidateP99 > stableP99*1.75+1000 {
		failures = append(failures, fmt.Sprintf("candidate p99 duration %.0fms is worse than stable %.0fms", candidateP99, stableP99))
	}
	return failures
}

func FloatMetric(metrics map[string]any, key string) float64 {
	switch value := metrics[key].(type) {
	case float64:
		return value
	case float32:
		return float64(value)
	case int:
		return float64(value)
	case int64:
		return float64(value)
	case json.Number:
		out, _ := value.Float64()
		return out
	default:
		return 0
	}
}

func BoolMetric(metrics map[string]any, key string) bool {
	switch value := metrics[key].(type) {
	case bool:
		return value
	case string:
		return strings.EqualFold(strings.TrimSpace(value), "true")
	default:
		return false
	}
}

func maxFloat64(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}

func (e ReleaseGateEvaluator) now() time.Time {
	if e.Now != nil {
		return e.Now().UTC()
	}
	return time.Now().UTC()
}
