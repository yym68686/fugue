package api

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/releaseflow"
	fuguestore "fugue/internal/store"
)

func (s *Server) handleGetOperationEvidence(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	op, err := s.store.GetOperation(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && op.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "operation is not visible to this tenant")
		return
	}
	filter := model.OperationEvidenceFilter{
		TenantID:      principal.TenantID,
		PlatformAdmin: principal.IsPlatformAdmin(),
		OperationID:   op.ID,
		Types:         splitQueryCSV(r.URL.Query()["type"]),
		Severities:    splitQueryCSV(r.URL.Query()["severity"]),
		Limit:         queryIntDefault(r, "limit", 200),
	}
	if since := strings.TrimSpace(r.URL.Query().Get("since")); since != "" {
		parsed, parseErr := time.Parse(time.RFC3339, since)
		if parseErr != nil {
			httpx.WriteError(w, http.StatusBadRequest, "invalid since timestamp")
			return
		}
		filter.Since = &parsed
	}
	evidence, err := s.store.ListOperationEvidence(filter)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !queryBool(r, "include_payload") {
		for idx := range evidence {
			evidence[idx].Payload = nil
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"evidence": evidence})
}

func (s *Server) handleGetOperationTimeline(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	timeline, err := s.store.ListOperationTimeline(principal.TenantID, principal.IsPlatformAdmin(), r.PathValue("id"), queryBool(r, "include_payload"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"timeline": timeline})
}

func (s *Server) handleGetOperationDebugBundle(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	bundle, err := s.operationDebugBundle(r, principal, r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("format")), "zip") {
		writeDebugBundleZip(w, "operation-debug-bundle.json", bundle)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"bundle": bundle})
}

func (s *Server) operationDebugBundle(r *http.Request, principal model.Principal, operationID string) (model.OperationDebugBundle, error) {
	op, err := s.store.GetOperation(operationID)
	if err != nil {
		return model.OperationDebugBundle{}, err
	}
	if !principal.IsPlatformAdmin() && op.TenantID != principal.TenantID {
		return model.OperationDebugBundle{}, fuguestore.ErrNotFound
	}
	evidence, err := s.store.ListOperationEvidence(model.OperationEvidenceFilter{
		TenantID:      principal.TenantID,
		PlatformAdmin: principal.IsPlatformAdmin(),
		OperationID:   op.ID,
		Limit:         1000,
	})
	if err != nil {
		return model.OperationDebugBundle{}, err
	}
	timeline, err := s.store.ListOperationTimeline(principal.TenantID, principal.IsPlatformAdmin(), op.ID, true)
	if err != nil {
		return model.OperationDebugBundle{}, err
	}
	diagnosis, _ := s.diagnoseOperation(r.Context(), op)
	_ = s.attachOperationEvidenceDiagnosis(r.Context(), op, &diagnosis)
	var diagnosisPtr *model.OperationDiagnosis
	if strings.TrimSpace(diagnosis.Category) != "" {
		diagnosisPtr = &diagnosis
	}
	app, appFound, _ := s.getDiagnosisApp(op.AppID)
	var appPtr *model.App
	if appFound {
		appCopy := redactAppForDebugBundle(app)
		appPtr = &appCopy
	}
	trackingPtr, trackingChecks := s.appImageTrackingDebugBundleState(principal, appPtr)
	metricsSummary := s.debugBundleMetricsSummaryForApp(principal, op.AppID, queryBool(r, "include_global_summary"))
	redactedOperation := redactOperationForDebugBundle(op)
	view := releaseflow.ReleaseEvidenceView{
		Metadata: map[string]any{
			"generated_at": time.Now().UTC().Format(time.RFC3339),
			"kind":         "operation_debug_bundle",
			"operation_id": op.ID,
			"redacted":     true,
		},
		Operation:           &redactedOperation,
		App:                 appPtr,
		ImageTracking:       trackingPtr,
		ImageTrackingChecks: trackingChecks,
		MetricsSummary:      metricsSummary,
		Diagnosis:           diagnosisPtr,
		Timeline:            timeline,
		Evidence:            evidence,
	}
	for _, item := range evidence {
		if strings.TrimSpace(item.ReleaseAttemptID) == "" {
			continue
		}
		if attempt, err := s.store.GetReleaseAttempt(item.ReleaseAttemptID); err == nil {
			view.ReleaseAttempt = &attempt
			if releaseTimeline, err := s.store.ListReleaseTimeline(principal.TenantID, principal.IsPlatformAdmin(), attempt.ID); err == nil {
				view.ReleaseTimeline = releaseTimeline
				view.GateResults = releaseflow.GateResultsFromReleaseTimeline(releaseTimeline)
			}
			view.AppReleases, view.TrafficPolicies = s.releaseDebugSections(principal, attempt.AppID)
		}
		break
	}
	bundle := releaseflow.ReleaseEvidenceViewBuilder{}.OperationBundle(view)
	return bundle, nil
}

func (s *Server) handleListAppReleaseAttempts(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, err := s.store.GetApp(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && app.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "app is not visible to this tenant")
		return
	}
	attempts, err := s.store.ListReleaseAttempts(model.ReleaseAttemptFilter{
		TenantID:      principal.TenantID,
		PlatformAdmin: principal.IsPlatformAdmin(),
		AppID:         app.ID,
		Status:        strings.TrimSpace(r.URL.Query().Get("status")),
		Limit:         queryIntDefault(r, "limit", 50),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"release_attempts": attempts})
}

func (s *Server) handleGetAppReleaseAttempt(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	attempt, ok := s.getVisibleReleaseAttempt(w, principal, r.PathValue("id"), r.PathValue("attempt_id"))
	if !ok {
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"release_attempt": attempt})
}

func (s *Server) handleGetAppReleaseAttemptTimeline(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	attempt, ok := s.getVisibleReleaseAttempt(w, principal, r.PathValue("id"), r.PathValue("attempt_id"))
	if !ok {
		return
	}
	timeline, err := s.store.ListReleaseTimeline(principal.TenantID, principal.IsPlatformAdmin(), attempt.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"timeline": timeline})
}

func (s *Server) handleGetAppReleaseAttemptEvidence(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	attempt, ok := s.getVisibleReleaseAttempt(w, principal, r.PathValue("id"), r.PathValue("attempt_id"))
	if !ok {
		return
	}
	evidence, err := s.store.ListOperationEvidence(model.OperationEvidenceFilter{
		TenantID:         principal.TenantID,
		PlatformAdmin:    principal.IsPlatformAdmin(),
		ReleaseAttemptID: attempt.ID,
		Limit:            queryIntDefault(r, "limit", 1000),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !queryBool(r, "include_payload") {
		for idx := range evidence {
			evidence[idx].Payload = nil
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"evidence": evidence})
}

func (s *Server) handleGetAppReleaseAttemptDebugBundle(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	attempt, ok := s.getVisibleReleaseAttempt(w, principal, r.PathValue("id"), r.PathValue("attempt_id"))
	if !ok {
		return
	}
	timeline, err := s.store.ListReleaseTimeline(principal.TenantID, principal.IsPlatformAdmin(), attempt.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	evidence, err := s.store.ListOperationEvidence(model.OperationEvidenceFilter{
		TenantID:         principal.TenantID,
		PlatformAdmin:    principal.IsPlatformAdmin(),
		ReleaseAttemptID: attempt.ID,
		Limit:            1000,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var appPtr *model.App
	if app, err := s.store.GetApp(attempt.AppID); err == nil {
		appCopy := redactAppForDebugBundle(app)
		appPtr = &appCopy
	}
	trackingPtr, trackingChecks := s.appImageTrackingDebugBundleState(principal, appPtr)
	metricsSummary := s.debugBundleMetricsSummaryForApp(principal, attempt.AppID, queryBool(r, "include_global_summary"))
	appReleases, trafficPolicies := s.releaseDebugSections(principal, attempt.AppID)
	view := releaseflow.ReleaseEvidenceView{
		Metadata: map[string]any{
			"generated_at":       time.Now().UTC().Format(time.RFC3339),
			"kind":               "release_debug_bundle",
			"release_attempt_id": attempt.ID,
			"redacted":           true,
		},
		ReleaseAttempt:      &attempt,
		App:                 appPtr,
		ImageTracking:       trackingPtr,
		ImageTrackingChecks: trackingChecks,
		MetricsSummary:      metricsSummary,
		ReleaseTimeline:     timeline,
		Evidence:            evidence,
		AppReleases:         appReleases,
		TrafficPolicies:     trafficPolicies,
		GateResults:         releaseflow.GateResultsFromReleaseTimeline(timeline),
	}
	bundle := releaseflow.ReleaseEvidenceViewBuilder{}.ReleaseBundle(view)
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("format")), "zip") {
		writeDebugBundleZip(w, "release-debug-bundle.json", bundle)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"bundle": bundle})
}

func (s *Server) releaseDebugSections(principal model.Principal, appID string) ([]model.AppRelease, []model.AppTrafficPolicy) {
	appReleases, err := s.store.ListAppReleases(model.AppReleaseFilter{
		TenantID:      principal.TenantID,
		PlatformAdmin: principal.IsPlatformAdmin(),
		AppID:         appID,
		ActiveOnly:    true,
	})
	if err != nil {
		appReleases = nil
	}
	policies := []model.AppTrafficPolicy{}
	if policy, err := s.store.GetAppTrafficPolicy(principal.TenantID, principal.IsPlatformAdmin(), appID); err == nil {
		policies = append(policies, policy)
	}
	return appReleases, policies
}

func (s *Server) debugBundleMetricsSummaryForApp(principal model.Principal, appID string, includeGlobal bool) map[string]any {
	appID = strings.TrimSpace(appID)
	summary := map[string]any{
		"scope":                  "app",
		"app_id":                 appID,
		"include_global_summary": includeGlobal,
	}
	if appID == "" {
		summary["scope"] = "operation"
	}
	if appID != "" {
		attempts, err := s.store.ListReleaseAttempts(model.ReleaseAttemptFilter{
			TenantID:      principal.TenantID,
			PlatformAdmin: principal.IsPlatformAdmin(),
			AppID:         appID,
			Limit:         500,
		})
		if err != nil {
			summary["release_attempt_error"] = err.Error()
		} else {
			summary["release_attempt_count"] = len(attempts)
			summary["release_attempts_by_status"] = releaseAttemptStatusSummary(attempts)
		}
		releases, err := s.store.ListAppReleases(model.AppReleaseFilter{
			TenantID:      principal.TenantID,
			PlatformAdmin: principal.IsPlatformAdmin(),
			AppID:         appID,
			ActiveOnly:    true,
		})
		if err != nil {
			summary["app_release_error"] = err.Error()
		} else {
			summary["active_release_count"] = len(releases)
			summary["active_releases_by_role"] = appReleaseRoleSummary(releases)
		}
	}
	evidence, err := s.store.ListOperationEvidence(model.OperationEvidenceFilter{
		TenantID:      principal.TenantID,
		PlatformAdmin: principal.IsPlatformAdmin(),
		AppID:         appID,
		Limit:         1000,
	})
	if err != nil {
		summary["operation_evidence_error"] = err.Error()
	} else {
		summary["operation_evidence_count"] = len(evidence)
		summary["operation_evidence_by_type"] = operationEvidenceTypeSummary(evidence)
	}
	if includeGlobal {
		summary["global"] = s.debugBundleMetricsSummary()
	}
	return summary
}

func releaseAttemptStatusSummary(attempts []model.ReleaseAttempt) map[string]int {
	out := map[string]int{}
	for _, attempt := range attempts {
		key := strings.TrimSpace(attempt.Status)
		if key == "" {
			key = "unknown"
		}
		out[key]++
	}
	return out
}

func appReleaseRoleSummary(releases []model.AppRelease) map[string]int {
	out := map[string]int{}
	for _, release := range releases {
		key := strings.TrimSpace(release.Role)
		if key == "" {
			key = "unknown"
		}
		out[key]++
	}
	return out
}

func operationEvidenceTypeSummary(evidence []model.OperationEvidence) map[string]int {
	out := map[string]int{}
	for _, item := range evidence {
		key := strings.TrimSpace(item.Type)
		if key == "" {
			key = "unknown"
		}
		out[key]++
	}
	return out
}

func (s *Server) debugBundleMetricsSummary() map[string]any {
	records, captures, rollouts, err := s.store.CountOperationEvidenceMetricGroups()
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	releaseAttempts, err := s.store.CountReleaseAttemptMetricGroups()
	if err != nil {
		return map[string]any{"operation_evidence_records": records, "evidence_capture_results": captures, "rollout_failures": rollouts, "release_attempt_error": err.Error()}
	}
	releaseResearch, migrationEvidence, err := s.store.CountReleaseEvidenceResearchGroups()
	if err != nil {
		return map[string]any{
			"operation_evidence_records": records,
			"evidence_capture_results":   captures,
			"rollout_failures":           rollouts,
			"release_attempts":           releaseAttempts,
			"release_research_error":     err.Error(),
		}
	}
	return map[string]any{
		"operation_evidence_records": records,
		"evidence_capture_results":   captures,
		"rollout_failures":           rollouts,
		"release_attempts":           releaseAttempts,
		"release_research": map[string]any{
			"env_patch_tracking_sync": releaseResearch,
			"migration_log_evidence":  migrationEvidence,
		},
	}
}

func (s *Server) appImageTrackingDebugBundleState(principal model.Principal, app *model.App) (*model.AppImageTracking, []model.AppImageTrackingCheck) {
	if app == nil {
		return nil, nil
	}
	tracking, err := s.store.GetAppImageTracking(principal.TenantID, principal.IsPlatformAdmin(), app.ID)
	if err != nil {
		return nil, nil
	}
	checks, err := s.store.ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter{
		TenantID:      principal.TenantID,
		PlatformAdmin: principal.IsPlatformAdmin(),
		AppID:         app.ID,
		TrackingID:    tracking.ID,
		Limit:         20,
	})
	if err != nil {
		checks = nil
	}
	return &tracking, checks
}

func (s *Server) getVisibleReleaseAttempt(w http.ResponseWriter, principal model.Principal, appID, attemptID string) (model.ReleaseAttempt, bool) {
	app, err := s.store.GetApp(appID)
	if err != nil {
		s.writeStoreError(w, err)
		return model.ReleaseAttempt{}, false
	}
	if !principal.IsPlatformAdmin() && app.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "app is not visible to this tenant")
		return model.ReleaseAttempt{}, false
	}
	attempt, err := s.store.GetReleaseAttempt(attemptID)
	if err != nil {
		s.writeStoreError(w, err)
		return model.ReleaseAttempt{}, false
	}
	if strings.TrimSpace(attempt.AppID) != strings.TrimSpace(app.ID) {
		httpx.WriteError(w, http.StatusNotFound, "release attempt not found")
		return model.ReleaseAttempt{}, false
	}
	if !principal.IsPlatformAdmin() && attempt.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "release attempt is not visible to this tenant")
		return model.ReleaseAttempt{}, false
	}
	return attempt, true
}

func writeDebugBundleZip(w http.ResponseWriter, name string, payload any) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	file, err := zw.Create(name)
	if err == nil {
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		err = encoder.Encode(payload)
	}
	closeErr := zw.Close()
	if err != nil || closeErr != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "failed to build debug bundle")
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", strings.TrimSuffix(name, ".json")+".zip"))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

func splitQueryCSV(values []string) []string {
	out := []string{}
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out
}

func queryBool(r *http.Request, key string) bool {
	value := strings.ToLower(strings.TrimSpace(r.URL.Query().Get(key)))
	return value == "1" || value == "true" || value == "yes"
}

func queryIntDefault(r *http.Request, key string, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
