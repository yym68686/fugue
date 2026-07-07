package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/releaseflow"
	"fugue/internal/store"
)

const (
	defaultAppReleaseProbeTimeout    = 10 * time.Second
	defaultAppReleaseGateWindow      = 10 * time.Minute
	defaultAppReleaseGateMinRequests = 1
)

type appReleaseCreateRequest struct {
	Role             string         `json:"role,omitempty"`
	SourceRef        string         `json:"source_ref,omitempty"`
	ResolvedImageRef string         `json:"resolved_image_ref,omitempty"`
	UpstreamURL      string         `json:"upstream_url,omitempty"`
	RuntimeID        string         `json:"runtime_id,omitempty"`
	DeploymentName   string         `json:"deployment_name,omitempty"`
	ServiceName      string         `json:"service_name,omitempty"`
	Status           string         `json:"status,omitempty"`
	StatusReason     string         `json:"status_reason,omitempty"`
	SpecSnapshot     *model.AppSpec `json:"spec_snapshot,omitempty"`
}

type appReleaseListResponse struct {
	AppID    string                  `json:"app_id"`
	Releases []model.AppRelease      `json:"releases"`
	Traffic  *model.AppTrafficPolicy `json:"traffic,omitempty"`
}

type appReleaseResponse struct {
	AppID   string           `json:"app_id"`
	Release model.AppRelease `json:"release"`
}

type appTrafficPatchRequest struct {
	Mode               string `json:"mode,omitempty"`
	StableReleaseID    string `json:"stable_release_id,omitempty"`
	CandidateReleaseID string `json:"candidate_release_id,omitempty"`
	StableWeight       *int   `json:"stable_weight,omitempty"`
	CandidateWeight    *int   `json:"candidate_weight,omitempty"`
	StickyHeader       string `json:"sticky_header,omitempty"`
	StickyCookie       string `json:"sticky_cookie,omitempty"`
}

type appTrafficResponse struct {
	AppID    string                 `json:"app_id"`
	Traffic  model.AppTrafficPolicy `json:"traffic"`
	Releases []model.AppRelease     `json:"releases,omitempty"`
}

type appReleasePromoteRequest struct {
	CandidateWeight *int `json:"candidate_weight,omitempty"`
}

type appReleaseAbortRequest struct {
	MarkFailed bool   `json:"mark_failed,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

type appReleaseProbeRequest struct {
	Probes []model.AppReleaseProbe `json:"probes,omitempty"`
}

type appReleaseProbeResponse struct {
	AppID   string                        `json:"app_id"`
	Release model.AppRelease              `json:"release"`
	Results []model.AppReleaseProbeResult `json:"results"`
	Status  string                        `json:"status"`
}

type appReleaseGateRequest struct {
	Policy *model.AppReleaseGatePolicy `json:"policy,omitempty"`
}

type appReleaseGateResponse struct {
	AppID  string                     `json:"app_id"`
	Gate   model.AppReleaseGateResult `json:"gate"`
	Policy model.AppReleaseGatePolicy `json:"policy"`
}

func (s *Server) appReleaseService() releaseflow.AppReleaseService {
	return releaseflow.AppReleaseService{
		Store:            s.store,
		ServiceURLForApp: s.serviceURLForApp,
	}
}

func (s *Server) appReleaseGateEvaluator() releaseflow.ReleaseGateEvaluator {
	return releaseflow.ReleaseGateEvaluator{
		MetricsQuerier: apiReleaseGateMetricsQuerier{server: s},
	}
}

type apiReleaseGateMetricsQuerier struct {
	server *Server
}

func (q apiReleaseGateMetricsQuerier) QueryReleaseGateMetrics(ctx context.Context, appID, releaseID, releaseRole string, window time.Duration) (map[string]any, error) {
	if q.server == nil {
		return nil, fmt.Errorf("api server is nil")
	}
	until := time.Now().UTC()
	obsWindow := appObservabilityWindow{Since: until.Add(-window).Format(time.RFC3339), Until: until.Format(time.RFC3339)}
	return q.server.queryAppReleaseGateMetrics(ctx, appID, releaseID, releaseRole, obsWindow)
}

func (s *Server) handleListAppReleases(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	releases, err := s.store.ListAppReleases(model.AppReleaseFilter{
		TenantID:      principal.TenantID,
		AppID:         app.ID,
		PlatformAdmin: principal.IsPlatformAdmin(),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var traffic *model.AppTrafficPolicy
	if policy, err := s.store.GetAppTrafficPolicy(principal.TenantID, principal.IsPlatformAdmin(), app.ID); err == nil {
		traffic = &policy
	} else if !errors.Is(err, store.ErrNotFound) {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, appReleaseListResponse{AppID: app.ID, Releases: releases, Traffic: traffic})
}

func (s *Server) handleCreateAppRelease(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	var req appReleaseCreateRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	release, err := s.appReleaseService().CreateRelease(r.Context(), app, releaseflow.CreateReleaseRequest{
		Role:             req.Role,
		SourceRef:        req.SourceRef,
		ResolvedImageRef: req.ResolvedImageRef,
		UpstreamURL:      req.UpstreamURL,
		RuntimeID:        req.RuntimeID,
		DeploymentName:   req.DeploymentName,
		ServiceName:      req.ServiceName,
		Status:           req.Status,
		StatusReason:     req.StatusReason,
		SpecSnapshot:     req.SpecSnapshot,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.release.create", "app_release", release.ID, app.TenantID, map[string]string{
		"app_id": app.ID,
		"role":   release.Role,
		"status": release.Status,
	})
	httpx.WriteJSON(w, http.StatusCreated, appReleaseResponse{AppID: app.ID, Release: release})
}

func (s *Server) handleGetAppTrafficPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	policy, err := s.ensureAppStableTrafficPolicy(r.Context(), principal, app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	releases, err := s.store.ListAppReleases(model.AppReleaseFilter{TenantID: principal.TenantID, AppID: app.ID, PlatformAdmin: principal.IsPlatformAdmin()})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, appTrafficResponse{AppID: app.ID, Traffic: policy, Releases: releases})
}

func (s *Server) handlePatchAppTrafficPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	var req appTrafficPatchRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	policy, err := s.appReleaseService().PatchTrafficPolicy(r.Context(), principal, app, releaseflow.TrafficPatch{
		Mode:               req.Mode,
		StableReleaseID:    req.StableReleaseID,
		CandidateReleaseID: req.CandidateReleaseID,
		StableWeight:       req.StableWeight,
		CandidateWeight:    req.CandidateWeight,
		StickyHeader:       req.StickyHeader,
		StickyCookie:       req.StickyCookie,
	})
	if err != nil {
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "belongs to another app") || strings.Contains(err.Error(), "upstream_url") {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.release.traffic.update", "app", app.ID, app.TenantID, map[string]string{
		"stable_weight":    fmt.Sprintf("%d", policy.StableWeight),
		"candidate_weight": fmt.Sprintf("%d", policy.CandidateWeight),
		"mode":             policy.Mode,
	})
	httpx.WriteJSON(w, http.StatusOK, appTrafficResponse{AppID: app.ID, Traffic: policy})
}

func (s *Server) handleProbeAppRelease(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, release, ok := s.loadAuthorizedAppRelease(w, r, principal)
	if !ok {
		return
	}
	var req appReleaseProbeRequest
	if r.ContentLength != 0 {
		if err := httpx.DecodeJSON(r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	probes := req.Probes
	if len(probes) == 0 {
		probes = defaultAppReleaseProbes()
	}
	results := s.appReleaseGateEvaluator().RunProbes(r.Context(), release, probes)
	status := model.AppReleaseGateStatusPass
	for _, result := range results {
		if result.Status == model.AppReleaseGateStatusFail {
			status = model.AppReleaseGateStatusFail
			break
		}
	}
	s.appendAudit(principal, "app.release.probe", "app_release", release.ID, app.TenantID, map[string]string{
		"app_id": app.ID,
		"status": status,
	})
	httpx.WriteJSON(w, http.StatusOK, appReleaseProbeResponse{AppID: app.ID, Release: release, Results: results, Status: status})
}

func (s *Server) handleEvaluateAppReleaseGate(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, release, ok := s.loadAuthorizedAppRelease(w, r, principal)
	if !ok {
		return
	}
	var req appReleaseGateRequest
	if r.ContentLength != 0 {
		if err := httpx.DecodeJSON(r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	policy := normalizeAppReleaseGatePolicy(req.Policy)
	gate := s.appReleaseGateEvaluator().Evaluate(r.Context(), app, release, policy)
	s.appendAudit(principal, "app.release.gate.evaluate", "app_release", release.ID, app.TenantID, map[string]string{
		"app_id": app.ID,
		"status": gate.Status,
	})
	httpx.WriteJSON(w, http.StatusOK, appReleaseGateResponse{AppID: app.ID, Gate: gate, Policy: policy})
}

func (s *Server) handlePromoteAppRelease(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, release, ok := s.loadAuthorizedAppRelease(w, r, principal)
	if !ok {
		return
	}
	var req appReleasePromoteRequest
	if r.ContentLength != 0 {
		if err := httpx.DecodeJSON(r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	weight := 100
	if req.CandidateWeight != nil {
		weight = *req.CandidateWeight
	}
	policy, err := s.appReleaseService().PromoteRelease(r.Context(), principal, app, release, weight)
	if err != nil {
		if strings.Contains(err.Error(), "candidate_weight") {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.release.promote", "app_release", release.ID, app.TenantID, map[string]string{"app_id": app.ID, "weight": "100"})
	httpx.WriteJSON(w, http.StatusOK, appTrafficResponse{AppID: app.ID, Traffic: policy})
}

func (s *Server) handleAbortAppRelease(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	app, release, ok := s.loadAuthorizedAppRelease(w, r, principal)
	if !ok {
		return
	}
	var req appReleaseAbortRequest
	if r.ContentLength != 0 {
		if err := httpx.DecodeJSON(r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	policy, err := s.appReleaseService().AbortRelease(r.Context(), principal, app, release, req.MarkFailed, req.Reason)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.release.abort", "app_release", release.ID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusOK, appTrafficResponse{AppID: app.ID, Traffic: policy})
}

func (s *Server) loadAuthorizedAppRelease(w http.ResponseWriter, r *http.Request, principal model.Principal) (model.App, model.AppRelease, bool) {
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return model.App{}, model.AppRelease{}, false
	}
	releaseID := strings.TrimSpace(r.PathValue("release_id"))
	if releaseID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "release_id is required")
		return model.App{}, model.AppRelease{}, false
	}
	release, err := s.store.GetAppRelease(principal.TenantID, principal.IsPlatformAdmin(), releaseID)
	if err != nil {
		s.writeStoreError(w, err)
		return model.App{}, model.AppRelease{}, false
	}
	if strings.TrimSpace(release.AppID) != strings.TrimSpace(app.ID) {
		httpx.WriteError(w, http.StatusNotFound, "release not found")
		return model.App{}, model.AppRelease{}, false
	}
	return app, release, true
}

func (s *Server) ensureAppStableTrafficPolicy(ctx context.Context, principal model.Principal, app model.App) (model.AppTrafficPolicy, error) {
	return s.appReleaseService().EnsureStableTrafficPolicy(ctx, principal, app)
}

func (s *Server) ensureAppStableRelease(ctx context.Context, app model.App) (model.AppRelease, error) {
	return s.appReleaseService().EnsureStableRelease(ctx, app)
}

func (s *Server) validateAppTrafficPolicyReferences(principal model.Principal, app model.App, policy model.AppTrafficPolicy) error {
	return s.appReleaseService().ValidateTrafficPolicyReferences(principal, app, policy)
}

func (s *Server) evaluateAppReleaseGate(ctx context.Context, app model.App, release model.AppRelease, policy model.AppReleaseGatePolicy) model.AppReleaseGateResult {
	return s.appReleaseGateEvaluator().Evaluate(ctx, app, release, policy)
}

func (s *Server) queryAppReleaseGateMetrics(ctx context.Context, appID, releaseID, releaseRole string, window appObservabilityWindow) (map[string]any, error) {
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return nil, err
	}
	releaseCondition := appReleaseGateMetricReleaseCondition(releaseID, releaseRole)
	queryText := "SELECT " +
		"count() AS request_count, " +
		"countIf(status_code >= 500) AS error_5xx_count, " +
		"countIf(error_type = 'upstream_error') AS edge_upstream_error_count, " +
		"quantileTDigest(0.95)(toFloat64(ttfb_ms)) AS p95_ttfb_ms, " +
		"quantileTDigestIf(0.99)(toFloat64(duration_ms), NOT " + appObservabilityStreamingSummaryPredicate() + ") AS p99_duration_ms " +
		"FROM request_facts WHERE app_id = " + quoteClickHouseString(appID) +
		" AND ts >= " + clickHouseDateTime64Literal(since) +
		" AND ts <= " + clickHouseDateTime64Literal(until) +
		" AND " + releaseCondition +
		" FORMAT JSONEachRow"
	rows, err := s.queryAppObservabilityClickHouse(ctx, queryText)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return map[string]any{}, nil
	}
	row := rows[0]
	requestCount := finiteFloatField(row, "request_count")
	error5xxCount := finiteFloatField(row, "error_5xx_count")
	upstreamErrorCount := finiteFloatField(row, "edge_upstream_error_count")
	error5xxRate := 0.0
	upstreamErrorRate := 0.0
	if requestCount > 0 {
		error5xxRate = error5xxCount / requestCount
		upstreamErrorRate = upstreamErrorCount / requestCount
	}
	return map[string]any{
		"request_count":             requestCount,
		"error_5xx_count":           error5xxCount,
		"edge_upstream_error_count": upstreamErrorCount,
		"error_5xx_rate":            error5xxRate,
		"edge_upstream_error_rate":  upstreamErrorRate,
		"p95_ttfb_ms":               finiteFloatField(row, "p95_ttfb_ms"),
		"p99_duration_ms":           finiteFloatField(row, "p99_duration_ms"),
	}, nil
}

func appReleaseGateMetricReleaseCondition(releaseID, releaseRole string) string {
	releaseID = strings.TrimSpace(releaseID)
	if releaseID != "" {
		return "JSONExtractString(summary_json, 'release_id') = " + quoteClickHouseString(releaseID)
	}
	return "JSONExtractString(summary_json, 'release_role') = " + quoteClickHouseString(strings.TrimSpace(releaseRole))
}

func appReleaseGateMetricEvidence(metrics map[string]any) []string {
	if len(metrics) == 0 {
		return nil
	}
	return []string{fmt.Sprintf("release requests=%0.f 5xx_rate=%.4f upstream_error_rate=%.4f p95_ttfb_ms=%.0f p99_duration_ms=%.0f",
		floatMetric(metrics, "request_count"),
		floatMetric(metrics, "error_5xx_rate"),
		floatMetric(metrics, "edge_upstream_error_rate"),
		floatMetric(metrics, "p95_ttfb_ms"),
		floatMetric(metrics, "p99_duration_ms"),
	)}
}

func appReleaseGateMetricFailures(metrics map[string]any, policy model.AppReleaseGatePolicy) []string {
	failures := []string{}
	if policy.MinCandidateRequests > 0 && floatMetric(metrics, "request_count") < float64(policy.MinCandidateRequests) {
		failures = append(failures, fmt.Sprintf("release request count %.0f is below minimum %d", floatMetric(metrics, "request_count"), policy.MinCandidateRequests))
	}
	if policy.Max5xxRate > 0 && floatMetric(metrics, "error_5xx_rate") > policy.Max5xxRate {
		failures = append(failures, fmt.Sprintf("5xx rate %.4f exceeds %.4f", floatMetric(metrics, "error_5xx_rate"), policy.Max5xxRate))
	}
	if policy.MaxEdgeUpstreamErrorRate > 0 && floatMetric(metrics, "edge_upstream_error_rate") > policy.MaxEdgeUpstreamErrorRate {
		failures = append(failures, fmt.Sprintf("edge upstream error rate %.4f exceeds %.4f", floatMetric(metrics, "edge_upstream_error_rate"), policy.MaxEdgeUpstreamErrorRate))
	}
	if policy.MaxP95TTFBMilliseconds > 0 && floatMetric(metrics, "p95_ttfb_ms") > float64(policy.MaxP95TTFBMilliseconds) {
		failures = append(failures, fmt.Sprintf("p95 ttfb %.0fms exceeds %dms", floatMetric(metrics, "p95_ttfb_ms"), policy.MaxP95TTFBMilliseconds))
	}
	if policy.MaxP99DurationMilliseconds > 0 && floatMetric(metrics, "p99_duration_ms") > float64(policy.MaxP99DurationMilliseconds) {
		failures = append(failures, fmt.Sprintf("p99 duration %.0fms exceeds %dms", floatMetric(metrics, "p99_duration_ms"), policy.MaxP99DurationMilliseconds))
	}
	return failures
}

func (s *Server) runAppReleaseProbes(ctx context.Context, release model.AppRelease, probes []model.AppReleaseProbe) []model.AppReleaseProbeResult {
	return s.appReleaseGateEvaluator().RunProbes(ctx, release, probes)
}

func runAppReleaseProbe(ctx context.Context, release model.AppRelease, probe model.AppReleaseProbe) model.AppReleaseProbeResult {
	result := model.AppReleaseProbeResult{Name: probe.Name, Path: probe.Path, Status: model.AppReleaseGateStatusFail}
	base := strings.TrimRight(strings.TrimSpace(release.UpstreamURL), "/")
	if base == "" {
		result.Error = "release upstream_url is empty"
		return result
	}
	timeout := time.Duration(probe.TimeoutMilliseconds) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultAppReleaseProbeTimeout
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
	started := time.Now()
	var firstByteAt time.Time
	trace := &httptrace.ClientTrace{
		GotFirstResponseByte: func() {
			firstByteAt = time.Now()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		result.DurationMillis = time.Since(started).Milliseconds()
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
	result.DurationMillis = time.Since(started).Milliseconds()
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

func normalizeAppReleaseGatePolicy(raw *model.AppReleaseGatePolicy) model.AppReleaseGatePolicy {
	return releaseflow.ReleaseGateEvaluator{}.NormalizePolicy(raw)
}

func defaultAppReleaseProbes() []model.AppReleaseProbe {
	return releaseflow.DefaultReleaseProbes()
}

func appReleaseSourceRef(app model.App) string {
	return releaseflow.AppReleaseSourceRef(app)
}

func appReleaseFirstNonNilTime(values ...*time.Time) *time.Time {
	return releaseflow.FirstNonNilTime(values...)
}

func floatMetric(metrics map[string]any, key string) float64 {
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
