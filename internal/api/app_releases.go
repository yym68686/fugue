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
	role := store.NormalizeAppReleaseRole(req.Role)
	if role == "" {
		role = model.AppReleaseRoleCandidate
	}
	status := store.NormalizeAppReleaseStatus(req.Status)
	upstreamURL := strings.TrimSpace(req.UpstreamURL)
	if role == model.AppReleaseRoleStable && upstreamURL == "" {
		upstreamURL = s.serviceURLForApp(r.Context(), app)
	}
	if status == model.AppReleaseStatusReady && upstreamURL == "" && role == model.AppReleaseRoleCandidate {
		status = model.AppReleaseStatusCreating
	}
	var readyAt *time.Time
	if status == model.AppReleaseStatusReady || status == model.AppReleaseStatusServing {
		now := time.Now().UTC()
		readyAt = &now
	}
	specSnapshot := req.SpecSnapshot
	if specSnapshot == nil && role == model.AppReleaseRoleStable {
		spec := app.Spec
		specSnapshot = &spec
	}
	release, err := s.store.CreateAppRelease(model.AppRelease{
		TenantID:         app.TenantID,
		AppID:            app.ID,
		Role:             role,
		SourceRef:        firstNonEmpty(req.SourceRef, appReleaseSourceRef(app)),
		ResolvedImageRef: strings.TrimSpace(req.ResolvedImageRef),
		UpstreamURL:      upstreamURL,
		RuntimeID:        firstNonEmpty(req.RuntimeID, app.Spec.RuntimeID),
		DeploymentName:   strings.TrimSpace(req.DeploymentName),
		ServiceName:      strings.TrimSpace(req.ServiceName),
		Status:           status,
		StatusReason:     strings.TrimSpace(req.StatusReason),
		SpecSnapshot:     specSnapshot,
		ReadyAt:          readyAt,
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
	current, err := s.ensureAppStableTrafficPolicy(r.Context(), principal, app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if req.Mode != "" {
		current.Mode = req.Mode
	}
	if req.StableReleaseID != "" {
		current.StableReleaseID = strings.TrimSpace(req.StableReleaseID)
	}
	if req.CandidateReleaseID != "" {
		current.CandidateReleaseID = strings.TrimSpace(req.CandidateReleaseID)
	}
	if req.StableWeight != nil {
		current.StableWeight = *req.StableWeight
	}
	if req.CandidateWeight != nil {
		current.CandidateWeight = *req.CandidateWeight
	}
	if req.StickyHeader != "" {
		current.StickyHeader = strings.TrimSpace(req.StickyHeader)
	}
	if req.StickyCookie != "" {
		current.StickyCookie = strings.TrimSpace(req.StickyCookie)
	}
	current.UpdatedByType = principal.ActorType
	current.UpdatedByID = principal.ActorID
	if err := s.validateAppTrafficPolicyReferences(principal, app, current); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	policy, err := s.store.UpsertAppTrafficPolicy(current)
	if err != nil {
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
	results := s.runAppReleaseProbes(r.Context(), release, probes)
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
	gate := s.evaluateAppReleaseGate(r.Context(), app, release, policy)
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
	if weight < 0 || weight > 100 {
		httpx.WriteError(w, http.StatusBadRequest, "candidate_weight must be between 0 and 100")
		return
	}
	policy, err := s.ensureAppStableTrafficPolicy(r.Context(), principal, app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if weight < 100 {
		policy.Mode = model.AppTrafficModeCanary
		policy.CandidateReleaseID = release.ID
		policy.StableWeight = 100 - weight
		policy.CandidateWeight = weight
		policy.UpdatedByType = principal.ActorType
		policy.UpdatedByID = principal.ActorID
		policy, err = s.store.UpsertAppTrafficPolicy(policy)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, appTrafficResponse{AppID: app.ID, Traffic: policy})
		return
	}
	now := time.Now().UTC()
	oldStableID := policy.StableReleaseID
	if oldStableID != "" && oldStableID != release.ID {
		if oldStable, err := s.store.GetAppRelease(app.TenantID, true, oldStableID); err == nil {
			oldStable.Role = model.AppReleaseRolePrevious
			_, _ = s.store.UpdateAppRelease(oldStable)
		}
	}
	release.Role = model.AppReleaseRoleStable
	release.Status = model.AppReleaseStatusServing
	release.PromotedAt = &now
	release.ReadyAt = appReleaseFirstNonNilTime(release.ReadyAt, &now)
	if _, err := s.store.UpdateAppRelease(release); err != nil {
		s.writeStoreError(w, err)
		return
	}
	policy.Mode = model.AppTrafficModeSingle
	policy.StableReleaseID = release.ID
	policy.CandidateReleaseID = ""
	policy.StableWeight = 100
	policy.CandidateWeight = 0
	policy.UpdatedByType = principal.ActorType
	policy.UpdatedByID = principal.ActorID
	policy, err = s.store.UpsertAppTrafficPolicy(policy)
	if err != nil {
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
	policy, err := s.ensureAppStableTrafficPolicy(r.Context(), principal, app)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	policy.Mode = model.AppTrafficModeSingle
	policy.CandidateReleaseID = release.ID
	policy.StableWeight = 100
	policy.CandidateWeight = 0
	policy.UpdatedByType = principal.ActorType
	policy.UpdatedByID = principal.ActorID
	policy, err = s.store.UpsertAppTrafficPolicy(policy)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if req.MarkFailed {
		release.Status = model.AppReleaseStatusFailed
		release.StatusReason = strings.TrimSpace(req.Reason)
		if _, err := s.store.UpdateAppRelease(release); err != nil {
			s.writeStoreError(w, err)
			return
		}
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
	if policy, err := s.store.GetAppTrafficPolicy(principal.TenantID, principal.IsPlatformAdmin(), app.ID); err == nil {
		return policy, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return model.AppTrafficPolicy{}, err
	}
	stable, err := s.ensureAppStableRelease(ctx, app)
	if err != nil {
		return model.AppTrafficPolicy{}, err
	}
	return s.store.UpsertAppTrafficPolicy(model.AppTrafficPolicy{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		Mode:            model.AppTrafficModeSingle,
		StableReleaseID: stable.ID,
		StableWeight:    100,
		CandidateWeight: 0,
		StickyCookie:    "Fugue-Release-Stickiness",
		UpdatedByType:   principal.ActorType,
		UpdatedByID:     principal.ActorID,
	})
}

func (s *Server) ensureAppStableRelease(ctx context.Context, app model.App) (model.AppRelease, error) {
	releases, err := s.store.ListAppReleases(model.AppReleaseFilter{
		TenantID:      app.TenantID,
		AppID:         app.ID,
		Role:          model.AppReleaseRoleStable,
		PlatformAdmin: true,
	})
	if err != nil {
		return model.AppRelease{}, err
	}
	if len(releases) > 0 {
		return releases[0], nil
	}
	status := model.AppReleaseStatusReady
	if app.Status.CurrentReplicas <= 0 {
		status = model.AppReleaseStatusCreating
	}
	now := time.Now().UTC()
	spec := app.Spec
	return s.store.CreateAppRelease(model.AppRelease{
		TenantID:         app.TenantID,
		AppID:            app.ID,
		Role:             model.AppReleaseRoleStable,
		SourceRef:        appReleaseSourceRef(app),
		ResolvedImageRef: app.Spec.Image,
		UpstreamURL:      s.serviceURLForApp(ctx, app),
		RuntimeID:        app.Spec.RuntimeID,
		Status:           status,
		SpecSnapshot:     &spec,
		ReadyAt:          &now,
	})
}

func (s *Server) validateAppTrafficPolicyReferences(principal model.Principal, app model.App, policy model.AppTrafficPolicy) error {
	if _, err := s.store.GetAppRelease(principal.TenantID, principal.IsPlatformAdmin(), policy.StableReleaseID); err != nil {
		return fmt.Errorf("stable_release_id is invalid")
	}
	if policy.CandidateReleaseID != "" {
		release, err := s.store.GetAppRelease(principal.TenantID, principal.IsPlatformAdmin(), policy.CandidateReleaseID)
		if err != nil {
			return fmt.Errorf("candidate_release_id is invalid")
		}
		if release.AppID != app.ID {
			return fmt.Errorf("candidate_release_id belongs to another app")
		}
		if policy.CandidateWeight > 0 && strings.TrimSpace(release.UpstreamURL) == "" {
			return fmt.Errorf("candidate release has no upstream_url")
		}
	}
	return nil
}

func (s *Server) evaluateAppReleaseGate(ctx context.Context, app model.App, release model.AppRelease, policy model.AppReleaseGatePolicy) model.AppReleaseGateResult {
	gate := model.AppReleaseGateResult{
		Status:      model.AppReleaseGateStatusPass,
		ReleaseID:   release.ID,
		Role:        release.Role,
		Evidence:    []string{},
		Warnings:    []string{},
		Failures:    []string{},
		Metrics:     map[string]any{},
		EvaluatedAt: time.Now().UTC(),
	}
	window := time.Duration(policy.WindowSeconds) * time.Second
	if window <= 0 {
		window = defaultAppReleaseGateWindow
	}
	until := time.Now().UTC()
	obsWindow := appObservabilityWindow{Since: until.Add(-window).Format(time.RFC3339), Until: until.Format(time.RFC3339)}
	gate.Window = window.String()
	if metrics, err := s.queryAppReleaseGateMetrics(ctx, app.ID, release.ID, release.Role, obsWindow); err == nil {
		gate.Metrics = metrics
		gate.Evidence = append(gate.Evidence, appReleaseGateMetricEvidence(metrics)...)
		gate.Failures = append(gate.Failures, appReleaseGateMetricFailures(metrics, policy)...)
	} else {
		gate.Warnings = append(gate.Warnings, "passive release metrics unavailable: "+err.Error())
	}
	probeResults := s.runAppReleaseProbes(ctx, release, policy.Probes)
	gate.ProbeResults = probeResults
	for _, result := range probeResults {
		if result.Status == model.AppReleaseGateStatusFail {
			gate.Failures = append(gate.Failures, fmt.Sprintf("probe %s failed: %s", firstNonEmpty(result.Name, result.Path), result.Error))
		}
	}
	if len(gate.Failures) > 0 {
		gate.Status = model.AppReleaseGateStatusFail
	}
	return gate
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
		"quantileTDigest(0.99)(toFloat64(duration_ms)) AS p99_duration_ms " +
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
	if len(probes) == 0 {
		probes = defaultAppReleaseProbes()
	}
	results := make([]model.AppReleaseProbeResult, 0, len(probes))
	for _, probe := range probes {
		results = append(results, runAppReleaseProbe(ctx, release, probe))
	}
	return results
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
	policy := model.AppReleaseGatePolicy{
		WindowSeconds:              int(defaultAppReleaseGateWindow.Seconds()),
		MinCandidateRequests:       defaultAppReleaseGateMinRequests,
		Max5xxRate:                 0.01,
		MaxEdgeUpstreamErrorRate:   0.005,
		MaxP95TTFBMilliseconds:     2000,
		MaxP99DurationMilliseconds: 30000,
		Probes:                     defaultAppReleaseProbes(),
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

func defaultAppReleaseProbes() []model.AppReleaseProbe {
	return []model.AppReleaseProbe{
		{Name: "health", Kind: model.AppReleaseProbeKindHTTP, Method: http.MethodGet, Path: "/v1/health", ExpectedStatus: http.StatusOK, TimeoutMilliseconds: 3000, MaxDurationMilliseconds: 3000},
	}
}

func appReleaseSourceRef(app model.App) string {
	if app.Source != nil {
		return firstNonEmpty(app.Source.ImageRef, app.Source.RepoURL, app.Spec.Image)
	}
	return app.Spec.Image
}

func appReleaseFirstNonNilTime(values ...*time.Time) *time.Time {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
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
