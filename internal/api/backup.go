package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/observability"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

const (
	backupSchedulerInterval  = time.Minute
	backupRunTimeout         = 30 * time.Minute
	backupRunLeaseTTL        = 2 * time.Minute
	backupRunHeartbeatPeriod = 30 * time.Second
	backupBackendProbeTTL    = 30 * time.Second
	backupRunMaxRetries      = 3
)

type backupBackendRequest struct {
	TenantID     string                       `json:"tenant_id,omitempty"`
	Name         string                       `json:"name"`
	Provider     string                       `json:"provider"`
	Bucket       string                       `json:"bucket,omitempty"`
	Region       string                       `json:"region,omitempty"`
	Endpoint     string                       `json:"endpoint,omitempty"`
	BaseURL      string                       `json:"base_url,omitempty"`
	Prefix       string                       `json:"prefix,omitempty"`
	Credentials  model.DataBackendCredentials `json:"credentials,omitempty"`
	FugueManaged bool                         `json:"fugue_managed,omitempty"`
	Billable     *bool                        `json:"billable,omitempty"`
	RotateOnly   bool                         `json:"rotate_only,omitempty"`
}

type backupPolicyRequest struct {
	ID          string                      `json:"id,omitempty"`
	TenantID    string                      `json:"tenant_id,omitempty"`
	ProjectID   string                      `json:"project_id,omitempty"`
	AppID       string                      `json:"app_id,omitempty"`
	Name        string                      `json:"name,omitempty"`
	Target      model.BackupTarget          `json:"target"`
	BackendID   string                      `json:"backend_id,omitempty"`
	Enabled     *bool                       `json:"enabled,omitempty"`
	Schedule    string                      `json:"schedule,omitempty"`
	RetainCount int                         `json:"retain_count,omitempty"`
	Retention   model.BackupRetentionPolicy `json:"retention,omitempty"`
	Version     string                      `json:"version,omitempty"`
}

type backupRunRequest struct {
	PolicyID  string             `json:"policy_id,omitempty"`
	Target    model.BackupTarget `json:"target,omitempty"`
	BackendID string             `json:"backend_id,omitempty"`
	Trigger   string             `json:"trigger,omitempty"`
	Version   string             `json:"version,omitempty"`
	Wait      bool               `json:"wait,omitempty"`
}

type backupRestorePlanRequest struct {
	ArtifactID string             `json:"artifact_id"`
	Target     model.BackupTarget `json:"target,omitempty"`
	Mode       string             `json:"mode,omitempty"`
}

type backupRestoreRunRequest struct {
	PlanID string `json:"plan_id"`
	Mode   string `json:"mode,omitempty"`
}

func (s *Server) StartBackgroundBackups(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(backupSchedulerInterval)
	defer ticker.Stop()
	s.enqueueDueBackups(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.enqueueDueBackups(ctx)
		}
	}
}

func (s *Server) enqueueDueBackups(ctx context.Context) {
	s.recoverStaleBackupRuns(ctx)
	s.enqueueDueBackupRetries(ctx)
	policies, err := s.store.ListDueBackupPolicies(time.Now().UTC(), 25)
	if err != nil {
		if s.log != nil {
			s.log.Printf("backup scheduler list due policies failed: %v", err)
		}
		return
	}
	for _, policy := range policies {
		run, err := s.store.CreateBackupRun(model.BackupRun{
			PolicyID:        policy.ID,
			TenantID:        policy.TenantID,
			ProjectID:       policy.ProjectID,
			AppID:           policy.AppID,
			Target:          policy.Target,
			BackendID:       policy.BackendID,
			Trigger:         model.BackupRunTriggerScheduled,
			Version:         policy.Version,
			Status:          model.BackupRunStatusPending,
			RequestedByType: "system",
			RequestedByID:   "backup-scheduler",
		})
		if errors.Is(err, store.ErrConflict) {
			continue
		}
		if err != nil {
			if s.log != nil {
				s.log.Printf("backup scheduler enqueue policy=%s failed: %v", policy.ID, err)
			}
			continue
		}
		go s.executeBackupRun(contextWithoutCancel(ctx), run.ID)
	}
}

func (s *Server) recoverStaleBackupRuns(ctx context.Context) {
	now := time.Now().UTC()
	for _, status := range []string{model.BackupRunStatusRunning, model.BackupRunStatusPending} {
		runs, err := s.store.ListBackupRuns(store.BackupRunFilter{Status: status, PlatformAdmin: true, Limit: 500})
		if err != nil {
			if s.log != nil {
				s.log.Printf("backup scheduler list %s runs for recovery failed: %v", status, err)
			}
			continue
		}
		for _, run := range runs {
			if !backupRunIsStale(run, now) {
				continue
			}
			failed := model.BackupRunStatusFailed
			code := "backup_run_lost"
			message := "backup run lease expired before completion; worker likely restarted or lost ownership"
			finishedAt := now
			updated, err := s.store.UpdateBackupRun(run.ID, store.BackupRunUpdate{
				Status:       &failed,
				ErrorCode:    &code,
				ErrorMessage: &message,
				FinishedAt:   timePtrPtr(&finishedAt),
				HeartbeatAt:  timePtrPtr(&now),
			})
			if err != nil {
				if s.log != nil {
					s.log.Printf("backup scheduler recover stale run=%s failed: %v", run.ID, err)
				}
				continue
			}
			if s.log != nil {
				s.log.Printf("backup scheduler recovered stale run=%s target=%s retry_count=%d", updated.ID, updated.Target.Type, updated.RetryCount)
			}
			s.scheduleBackupRetry(contextWithoutCancel(ctx), updated)
		}
	}
}

func backupRunIsStale(run model.BackupRun, now time.Time) bool {
	run = model.NormalizeBackupRun(run)
	if run.Status != model.BackupRunStatusRunning && run.Status != model.BackupRunStatusPending {
		return false
	}
	if run.Status == model.BackupRunStatusPending && run.Trigger == model.BackupRunTriggerRetry && run.NextRetryAt != nil && run.NextRetryAt.After(now) {
		return false
	}
	if run.LockedUntil != nil {
		return run.LockedUntil.Before(now)
	}
	lastSeen := run.UpdatedAt
	if run.HeartbeatAt != nil {
		lastSeen = *run.HeartbeatAt
	}
	if lastSeen.IsZero() {
		lastSeen = run.CreatedAt
	}
	return !lastSeen.IsZero() && lastSeen.Add(backupRunLeaseTTL).Before(now)
}

func (s *Server) enqueueDueBackupRetries(ctx context.Context) {
	runs, err := s.store.ListBackupRuns(store.BackupRunFilter{Status: model.BackupRunStatusPending, PlatformAdmin: true, Limit: 100})
	if err != nil {
		if s.log != nil {
			s.log.Printf("backup scheduler list pending retries failed: %v", err)
		}
		return
	}
	now := time.Now().UTC()
	for _, run := range runs {
		if run.Trigger != model.BackupRunTriggerRetry || run.NextRetryAt == nil || run.NextRetryAt.After(now) {
			continue
		}
		go s.executeBackupRun(contextWithoutCancel(ctx), run.ID)
	}
}

func (s *Server) handleListBackupBackends(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.read scope")
		return
	}
	backends, err := s.store.ListBackupBackends(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"backends": backends})
}

func (s *Server) handleCreateBackupBackend(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.write scope")
		return
	}
	var req backupBackendRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tenantID := req.TenantID
	if !principal.IsPlatformAdmin() {
		tenantID = principal.TenantID
	}
	billable := false
	if req.Billable != nil {
		billable = *req.Billable
	}
	if req.FugueManaged && model.NormalizeDataBackendProvider(req.Provider) == model.DataBackendProviderCloudflareR2 && req.Billable == nil {
		billable = true
	}
	if req.RotateOnly {
		backend, err := s.store.RotateBackupBackendCredentials(req.Name, tenantID, principal.IsPlatformAdmin(), req.Credentials)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		s.appendAudit(principal, "backup.backend.rotate", "backup_backend", backend.ID, backend.TenantID, map[string]string{"name": backend.Name})
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"backend": backend})
		return
	}
	backend, err := s.store.CreateBackupBackend(model.BackupBackend{
		TenantID:     tenantID,
		Name:         req.Name,
		Provider:     req.Provider,
		Bucket:       req.Bucket,
		Region:       req.Region,
		Endpoint:     req.Endpoint,
		BaseURL:      req.BaseURL,
		Prefix:       req.Prefix,
		Credentials:  req.Credentials,
		FugueManaged: req.FugueManaged,
		Billable:     billable,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "backup.backend.create", "backup_backend", backend.ID, backend.TenantID, map[string]string{
		"name":     backend.Name,
		"provider": backend.Provider,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"backend": backend})
}

func (s *Server) handleGetBackupBackend(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.read scope")
		return
	}
	backend, err := s.store.GetBackupBackend(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"backend": backend})
}

func (s *Server) handleDeleteBackupBackend(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.write scope")
		return
	}
	backend, err := s.store.DeleteBackupBackend(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "backup.backend.delete", "backup_backend", backend.ID, backend.TenantID, map[string]string{"name": backend.Name})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"backend": backend})
}

func (s *Server) handleTestBackupBackend(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.write scope")
		return
	}
	backendID := r.PathValue("id")
	pass, message := s.testBackupBackend(r.Context(), backendID, principal)
	backend, err := s.store.RecordBackupBackendTest(backendID, principal.TenantID, principal.IsPlatformAdmin(), pass, message)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	status := "failed"
	if pass {
		status = "ok"
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"status": status, "message": message, "backend": backend})
}

func (s *Server) handleListBackupPolicies(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.read scope")
		return
	}
	policies, err := s.store.ListBackupPolicies(store.BackupPolicyFilter{
		TenantID:        principal.TenantID,
		ProjectID:       strings.TrimSpace(r.URL.Query().Get("project_id")),
		AppID:           strings.TrimSpace(r.URL.Query().Get("app_id")),
		TargetType:      strings.TrimSpace(r.URL.Query().Get("target_type")),
		IncludeDisabled: parseBackupBoolQuery(r, "include_disabled"),
		PlatformAdmin:   principal.IsPlatformAdmin(),
		Limit:           parseLimitQuery(r, 100),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policies": policies})
}

func (s *Server) handleUpsertBackupPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.write scope")
		return
	}
	var req backupPolicyRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	policy, ok := s.backupPolicyFromRequest(w, principal, req, nil)
	if !ok {
		return
	}
	saved, err := s.store.UpsertBackupPolicy(policy)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "backup.policy.upsert", "backup_policy", saved.ID, saved.TenantID, map[string]string{
		"name":        saved.Name,
		"target_type": saved.Target.Type,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": saved})
}

func (s *Server) handleGetBackupPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.read scope")
		return
	}
	policy, err := s.store.GetBackupPolicy(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": policy})
}

func (s *Server) handlePatchBackupPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.write scope")
		return
	}
	current, err := s.store.GetBackupPolicy(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var req backupPolicyRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Enabled != nil && req.Name == "" && req.BackendID == "" && req.Schedule == "" && req.RetainCount == 0 && req.Version == "" && req.Target.Type == "" {
		policy, err := s.store.SetBackupPolicyEnabled(current.ID, principal.TenantID, principal.IsPlatformAdmin(), *req.Enabled, "disabled by user")
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": policy})
		return
	}
	policy, ok := s.backupPolicyFromRequest(w, principal, req, &current)
	if !ok {
		return
	}
	policy.ID = current.ID
	policy.CreatedAt = current.CreatedAt
	saved, err := s.store.UpsertBackupPolicy(policy)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": saved})
}

func (s *Server) handleListBackupRuns(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.read scope")
		return
	}
	runs, err := s.store.ListBackupRuns(store.BackupRunFilter{
		TenantID:      principal.TenantID,
		ProjectID:     strings.TrimSpace(r.URL.Query().Get("project_id")),
		AppID:         strings.TrimSpace(r.URL.Query().Get("app_id")),
		PolicyID:      strings.TrimSpace(r.URL.Query().Get("policy_id")),
		TargetType:    strings.TrimSpace(r.URL.Query().Get("target_type")),
		Status:        strings.TrimSpace(r.URL.Query().Get("status")),
		PlatformAdmin: principal.IsPlatformAdmin(),
		Limit:         parseLimitQuery(r, 100),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) handleCreateBackupRun(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.write scope")
		return
	}
	var req backupRunRequest
	if r.Body != nil {
		if err := httpx.DecodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	run := model.BackupRun{
		PolicyID:        strings.TrimSpace(req.PolicyID),
		Target:          model.NormalizeBackupTarget(req.Target),
		BackendID:       strings.TrimSpace(req.BackendID),
		Trigger:         firstNonEmptyString(req.Trigger, model.BackupRunTriggerManual),
		Version:         strings.TrimSpace(req.Version),
		Status:          model.BackupRunStatusPending,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
	}
	if !principal.IsPlatformAdmin() {
		run.TenantID = principal.TenantID
		if run.Target.TenantID == "" {
			run.Target.TenantID = principal.TenantID
		}
	}
	created, err := s.store.CreateBackupRun(run)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if req.Wait {
		s.executeBackupRun(r.Context(), created.ID)
		finalRun, err := s.store.GetBackupRun(created.ID, principal.TenantID, principal.IsPlatformAdmin())
		if err == nil {
			created = finalRun
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"run": created})
		return
	}
	go s.executeBackupRun(contextWithoutCancel(r.Context()), created.ID)
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"run": created})
}

func (s *Server) handleGetBackupRun(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.read scope")
		return
	}
	run, err := s.store.GetBackupRun(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	artifacts, _ := s.store.ListBackupArtifacts(store.BackupArtifactFilter{
		TenantID:      principal.TenantID,
		RunID:         run.ID,
		PlatformAdmin: principal.IsPlatformAdmin(),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"run": run, "artifacts": artifacts})
}

func (s *Server) handleListBackupArtifacts(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.read scope")
		return
	}
	artifacts, err := s.store.ListBackupArtifacts(store.BackupArtifactFilter{
		TenantID:      principal.TenantID,
		ProjectID:     strings.TrimSpace(r.URL.Query().Get("project_id")),
		AppID:         strings.TrimSpace(r.URL.Query().Get("app_id")),
		PolicyID:      strings.TrimSpace(r.URL.Query().Get("policy_id")),
		RunID:         strings.TrimSpace(r.URL.Query().Get("run_id")),
		TargetType:    strings.TrimSpace(r.URL.Query().Get("target_type")),
		ActiveOnly:    !parseBackupBoolQuery(r, "include_deleted"),
		PlatformAdmin: principal.IsPlatformAdmin(),
		Limit:         parseLimitQuery(r, 100),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"artifacts": artifacts})
}

func (s *Server) handleGetBackupArtifact(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.read scope")
		return
	}
	artifact, err := s.store.GetBackupArtifact(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"artifact": artifact})
}

func (s *Server) handleDeleteBackupArtifact(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.write scope")
		return
	}
	artifact, err := s.store.MarkBackupArtifactDeleted(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "backup.artifact.delete", "backup_artifact", artifact.ID, artifact.TenantID, map[string]string{
		"target_type": artifact.Target.Type,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"artifact": artifact})
}

func (s *Server) handleCreateBackupRestorePlan(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.restore") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.restore scope")
		return
	}
	var req backupRestorePlanRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	plan, err := s.store.CreateBackupRestorePlan(model.BackupRestorePlan{
		ArtifactID:    req.ArtifactID,
		Target:        req.Target,
		Mode:          req.Mode,
		Status:        model.BackupRestoreStatusPlanned,
		CreatedByType: principal.ActorType,
		CreatedByID:   principal.ActorID,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "backup.restore.plan", "backup_restore_plan", plan.ID, plan.TenantID, map[string]string{
		"artifact_id": plan.ArtifactID,
		"mode":        plan.Mode,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"plan": plan})
}

func (s *Server) handleListBackupRestorePlans(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.restore") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.restore scope")
		return
	}
	plans, err := s.store.ListBackupRestorePlans(principal.TenantID, principal.IsPlatformAdmin(), parseLimitQuery(r, 100))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"plans": plans})
}

func (s *Server) handleCreateBackupRestoreRun(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.restore") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.restore scope")
		return
	}
	var req backupRestoreRunRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	run, err := s.store.CreateBackupRestoreRun(model.BackupRestoreRun{
		PlanID:          req.PlanID,
		Mode:            req.Mode,
		Status:          model.BackupRestoreStatusPlanned,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "backup.restore.run", "backup_restore_run", run.ID, run.TenantID, map[string]string{
		"plan_id": run.PlanID,
		"mode":    run.Mode,
	})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"run": run})
}

func (s *Server) handleListBackupRestoreRuns(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.restore") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.restore scope")
		return
	}
	runs, err := s.store.ListBackupRestoreRuns(principal.TenantID, principal.IsPlatformAdmin(), parseLimitQuery(r, 100))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) handleGetBackupUsage(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.read scope")
		return
	}
	usage, err := s.store.BackupUsage(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"usage": usage})
}

func (s *Server) handleGetAdminBackupStatus(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.admin") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.admin scope")
		return
	}
	policies, err := s.store.ListBackupPolicies(store.BackupPolicyFilter{IncludeDisabled: true, PlatformAdmin: true})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	runs, err := s.store.ListBackupRuns(store.BackupRunFilter{PlatformAdmin: true, Limit: 10})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	usage, err := s.store.BackupUsage("", true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	posture := s.platformBackupPosture(policies, usage)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"policies": policies,
		"runs":     runs,
		"usage":    usage,
		"posture":  posture,
	})
}

func (s *Server) handleGetAppBackupStatus(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.read") && !principal.HasScope("app.read") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.read or app.read scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	policies, err := s.store.ListBackupPolicies(store.BackupPolicyFilter{
		TenantID:        principal.TenantID,
		AppID:           app.ID,
		IncludeDisabled: true,
		PlatformAdmin:   principal.IsPlatformAdmin(),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	artifacts, err := s.store.ListBackupArtifacts(store.BackupArtifactFilter{
		TenantID:      principal.TenantID,
		AppID:         app.ID,
		ActiveOnly:    true,
		PlatformAdmin: principal.IsPlatformAdmin(),
		Limit:         10,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"app":       sanitizeAppForAPI(app),
		"policies":  policies,
		"artifacts": artifacts,
		"posture":   appBackupPosture(app, policies, artifacts),
	})
}

func (s *Server) handleCreateAppBackupPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.write scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	var req backupPolicyRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Target.Type == "" {
		req.Target.Type = model.BackupTargetAppDatabase
	}
	req.TenantID = app.TenantID
	req.ProjectID = app.ProjectID
	req.AppID = app.ID
	req.Target.TenantID = app.TenantID
	req.Target.ProjectID = app.ProjectID
	req.Target.AppID = app.ID
	req.Target.Name = app.Name
	target, ok := resolveAppBackupTarget(w, app, req.Target)
	if !ok {
		return
	}
	req.Target = target
	policy, ok := s.backupPolicyFromRequest(w, principal, req, nil)
	if !ok {
		return
	}
	saved, err := s.store.UpsertBackupPolicy(policy)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "app.backup.policy.upsert", "backup_policy", saved.ID, app.TenantID, map[string]string{
		"app_id":      app.ID,
		"target_type": saved.Target.Type,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": saved})
}

func resolveAppBackupTarget(w http.ResponseWriter, app model.App, target model.BackupTarget) (model.BackupTarget, bool) {
	target = model.NormalizeBackupTarget(target)
	target.TenantID = app.TenantID
	target.ProjectID = app.ProjectID
	target.AppID = app.ID
	target.Name = firstNonEmptyString(target.Name, app.Name)
	if target.Type != model.BackupTargetAppDatabase {
		return target, true
	}
	postgres := app.Spec.Postgres
	if postgres == nil {
		for idx := range app.BackingServices {
			service := app.BackingServices[idx]
			if service.Type != model.BackingServiceTypePostgres || service.Provisioner != model.BackingServiceProvisionerManaged || service.Status == model.BackingServiceStatusDeleted || service.Spec.Postgres == nil {
				continue
			}
			postgres = service.Spec.Postgres
			break
		}
	}
	if postgres == nil {
		httpx.WriteError(w, http.StatusBadRequest, "app has no managed postgres backup target")
		return model.BackupTarget{}, false
	}
	target.Database = firstNonEmptyString(target.Database, postgres.Database)
	target.RuntimeID = firstNonEmptyString(target.RuntimeID, postgres.RuntimeID, app.Spec.RuntimeID)
	target.ServiceName = firstNonEmptyString(target.ServiceName, postgres.ServiceName)
	return target, true
}

func (s *Server) handleCreateAppBackupRun(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() && !principal.HasScope("backup.write") {
		httpx.WriteError(w, http.StatusForbidden, "missing backup.write scope")
		return
	}
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	var req backupRunRequest
	if r.Body != nil {
		if err := httpx.DecodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if req.PolicyID == "" {
		policies, err := s.store.ListBackupPolicies(store.BackupPolicyFilter{
			TenantID:        app.TenantID,
			AppID:           app.ID,
			IncludeDisabled: false,
			PlatformAdmin:   true,
			Limit:           1,
		})
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if len(policies) == 0 {
			httpx.WriteError(w, http.StatusConflict, "app backup is disabled")
			return
		}
		req.PolicyID = policies[0].ID
	}
	run, err := s.store.CreateBackupRun(model.BackupRun{
		PolicyID:        req.PolicyID,
		TenantID:        app.TenantID,
		ProjectID:       app.ProjectID,
		AppID:           app.ID,
		Target:          req.Target,
		BackendID:       req.BackendID,
		Trigger:         firstNonEmptyString(req.Trigger, model.BackupRunTriggerManual),
		Version:         req.Version,
		Status:          model.BackupRunStatusPending,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	go s.executeBackupRun(contextWithoutCancel(r.Context()), run.ID)
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"run": run})
}

func (s *Server) backupPolicyFromRequest(w http.ResponseWriter, principal model.Principal, req backupPolicyRequest, current *model.BackupPolicy) (model.BackupPolicy, bool) {
	policy := model.BackupPolicy{}
	if current != nil {
		policy = *current
	}
	if req.ID != "" {
		policy.ID = strings.TrimSpace(req.ID)
	}
	if req.Name != "" {
		policy.Name = strings.TrimSpace(req.Name)
	}
	if policy.Name == "" {
		policy.Name = defaultBackupPolicyName(req.Target)
	}
	if req.Target.Type != "" {
		policy.Target = model.NormalizeBackupTarget(req.Target)
	}
	if policy.Target.Type == "" {
		httpx.WriteError(w, http.StatusBadRequest, "target.type is required")
		return model.BackupPolicy{}, false
	}
	if req.TenantID != "" || current == nil {
		policy.TenantID = strings.TrimSpace(req.TenantID)
	}
	if !principal.IsPlatformAdmin() {
		policy.TenantID = principal.TenantID
		if policy.Target.TenantID == "" {
			policy.Target.TenantID = principal.TenantID
		}
	}
	if req.ProjectID != "" || current == nil {
		policy.ProjectID = strings.TrimSpace(req.ProjectID)
	}
	if req.AppID != "" || current == nil {
		policy.AppID = strings.TrimSpace(req.AppID)
	}
	if req.BackendID != "" || current == nil {
		policy.BackendID = strings.TrimSpace(req.BackendID)
	}
	if req.Enabled != nil {
		policy.Enabled = *req.Enabled
	} else if current == nil {
		policy.Enabled = true
	}
	if req.Schedule != "" || current == nil {
		policy.Schedule = strings.TrimSpace(req.Schedule)
	}
	if policy.Schedule == "" {
		policy.Schedule = model.BackupDefaultSchedule
	}
	if req.RetainCount > 0 || current == nil {
		policy.RetainCount = req.RetainCount
	}
	if policy.RetainCount <= 0 {
		policy.RetainCount = model.BackupDefaultRetainCount
	}
	if req.Retention.RetainCount > 0 || req.Retention.RetainDays > 0 || req.Retention.ProtectLatest > 0 {
		policy.Retention = req.Retention
	}
	if policy.Retention.RetainCount <= 0 {
		policy.Retention.RetainCount = policy.RetainCount
	}
	if req.Version != "" || current == nil {
		policy.Version = strings.TrimSpace(req.Version)
	}
	if policy.BackendID == "" {
		policy.Status = model.BackupPolicyStatusBlockedNoBackend
		policy.DisabledReason = "backup backend is not configured"
	} else if policy.Enabled {
		policy.Status = model.BackupPolicyStatusActive
		policy.DisabledReason = ""
	} else {
		policy.Status = model.BackupPolicyStatusDisabled
	}
	policy.Scope = model.NormalizeBackupScope(policy.Scope, policy.Target)
	policy.CreatedBy = firstNonEmptyString(policy.CreatedBy, principal.ActorID)
	return model.NormalizeBackupPolicy(policy), true
}

func defaultBackupPolicyName(target model.BackupTarget) string {
	target = model.NormalizeBackupTarget(target)
	switch target.Type {
	case model.BackupTargetControlPlaneDatabase:
		return "control-plane-database"
	case model.BackupTargetAppDatabase:
		return "app-database"
	case model.BackupTargetPersistentStorage:
		return "persistent-storage"
	case model.BackupTargetDataWorkspace:
		return "data-workspace"
	default:
		return "backup-policy"
	}
}

func (s *Server) executeBackupRun(parent context.Context, runID string) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, backupRunTimeout)
	defer cancel()
	run, err := s.store.GetBackupRun(runID, "", true)
	if err != nil {
		if s.log != nil {
			s.log.Printf("load backup run %s failed: %v", runID, err)
		}
		return
	}
	now := time.Now().UTC()
	startedAt := now
	lockedUntil := now.Add(backupRunLeaseTTL)
	status := model.BackupRunStatusRunning
	leaseOwner := backupLeaseOwner()
	_, _ = s.store.UpdateBackupRun(run.ID, store.BackupRunUpdate{
		Status:      &status,
		LeaseOwner:  &leaseOwner,
		LockedUntil: timePtrPtr(&lockedUntil),
		StartedAt:   timePtrPtr(&startedAt),
		HeartbeatAt: timePtrPtr(&startedAt),
	})
	stopHeartbeat := s.startBackupRunHeartbeat(ctx, run.ID)
	runner := s.backupRunner
	if runner == nil {
		runner = s.runBackup
	}
	artifacts, err := runner(ctx, run)
	finishedAt := time.Now().UTC()
	stopHeartbeat()
	if err != nil {
		status = model.BackupRunStatusFailed
		code := backupErrorCode(err)
		message := err.Error()
		_, _ = s.store.UpdateBackupRun(run.ID, store.BackupRunUpdate{
			Status:       &status,
			ErrorCode:    &code,
			ErrorMessage: &message,
			LockedUntil:  timePtrPtr(nil),
			FinishedAt:   timePtrPtr(&finishedAt),
			HeartbeatAt:  timePtrPtr(&finishedAt),
		})
		if s.log != nil {
			s.log.Printf("backup run %s failed: %v", run.ID, err)
		}
		s.scheduleBackupRetry(contextWithoutCancel(parent), run)
		return
	}
	var bytesWritten int64
	for _, artifact := range artifacts {
		bytesWritten += artifact.SizeBytes
	}
	status = model.BackupRunStatusSucceeded
	count := len(artifacts)
	_, _ = s.store.UpdateBackupRun(run.ID, store.BackupRunUpdate{
		Status:        &status,
		LockedUntil:   timePtrPtr(nil),
		BytesWritten:  &bytesWritten,
		ArtifactCount: &count,
		FinishedAt:    timePtrPtr(&finishedAt),
		HeartbeatAt:   timePtrPtr(&finishedAt),
	})
}

func (s *Server) startBackupRunHeartbeat(ctx context.Context, runID string) func() {
	if ctx == nil {
		ctx = context.Background()
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		ticker := time.NewTicker(backupRunHeartbeatPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				now := time.Now().UTC()
				lockedUntil := now.Add(backupRunLeaseTTL)
				_, _ = s.store.UpdateBackupRun(runID, store.BackupRunUpdate{
					LockedUntil: timePtrPtr(&lockedUntil),
					HeartbeatAt: timePtrPtr(&now),
				})
			}
		}
	}()
	return func() {
		once.Do(func() {
			close(stop)
			<-done
		})
	}
}

func backupLeaseOwner() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return "fugue-api"
	}
	return "fugue-api/" + strings.TrimSpace(hostname)
}

func timePtrPtr(value *time.Time) **time.Time {
	return &value
}

func (s *Server) scheduleBackupRetry(ctx context.Context, run model.BackupRun) {
	if run.PolicyID == "" || run.RetryCount >= backupRunMaxRetries || run.Trigger == model.BackupRunTriggerManual {
		return
	}
	delay := backupRetryDelay(run.RetryCount + 1)
	nextRetryAt := time.Now().UTC().Add(delay)
	retryRun, err := s.store.CreateBackupRun(model.BackupRun{
		PolicyID:        run.PolicyID,
		TenantID:        run.TenantID,
		ProjectID:       run.ProjectID,
		AppID:           run.AppID,
		Target:          run.Target,
		BackendID:       run.BackendID,
		Trigger:         model.BackupRunTriggerRetry,
		Version:         run.Version,
		Status:          model.BackupRunStatusPending,
		Attempt:         run.Attempt + 1,
		RetryCount:      run.RetryCount + 1,
		RequestedByType: "system",
		RequestedByID:   "backup-retry",
		NextRetryAt:     &nextRetryAt,
	})
	if err != nil {
		if s.log != nil {
			s.log.Printf("backup retry enqueue for run %s failed: %v", run.ID, err)
		}
		return
	}
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.executeBackupRun(contextWithoutCancel(ctx), retryRun.ID)
		}
	}()
}

func backupRetryDelay(retryCount int) time.Duration {
	switch {
	case retryCount <= 1:
		return 5 * time.Minute
	case retryCount == 2:
		return 15 * time.Minute
	default:
		return 30 * time.Minute
	}
}

func (s *Server) runBackup(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
	switch model.NormalizeBackupTargetType(run.Target.Type) {
	case model.BackupTargetControlPlaneDatabase:
		return s.runControlPlaneDatabaseBackup(ctx, run)
	case model.BackupTargetAppDatabase:
		return s.runAppDatabaseBackup(ctx, run)
	case model.BackupTargetPersistentStorage:
		return nil, fmt.Errorf("unsupported_target: persistent storage backup worker is disabled by default")
	case model.BackupTargetDataWorkspace:
		return nil, fmt.Errorf("unsupported_target: data workspace backup maps to snapshot integration and is disabled by default")
	case model.BackupTargetRegistry:
		return nil, fmt.Errorf("unsupported_target: bundled registry backup is not enabled")
	default:
		return nil, fmt.Errorf("unsupported_target: %s", run.Target.Type)
	}
}

func (s *Server) runControlPlaneDatabaseBackup(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
	if strings.TrimSpace(run.BackendID) == "" {
		return nil, fmt.Errorf("backup_backend_missing: backup backend is not configured")
	}
	backend, err := s.store.GetBackupBackendForUse(run.BackendID, "", true)
	if err != nil {
		return nil, err
	}
	objectBackend, err := newDataObjectBackend(model.BackupBackendAsDataBackend(backend))
	if err != nil {
		return nil, err
	}
	databaseURL := s.controlPlaneBackupDatabaseURL()
	if databaseURL == "" {
		return nil, fmt.Errorf("database_url_missing: control-plane database URL is not configured")
	}
	tmpDir, err := os.MkdirTemp("", "fugue-control-plane-backup-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)
	dumpPath := path.Join(tmpDir, "control-plane.dump")
	pgDump := strings.TrimSpace(os.Getenv("FUGUE_PG_DUMP_BIN"))
	if pgDump == "" {
		pgDump = "pg_dump"
	}
	cmd := exec.CommandContext(ctx, pgDump, "--format=custom", "--no-owner", "--no-privileges", "--file", dumpPath, databaseURL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("pg_dump failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	size, sha256sum, err := fileSizeAndSHA256(dumpPath)
	if err != nil {
		return nil, err
	}
	version := strings.TrimSpace(run.Version)
	if version == "" {
		version = "v" + time.Now().UTC().Format("20060102-150405")
	}
	baseKey := path.Join("control-plane", time.Now().UTC().Format("2006/01/02/15"), run.ID)
	dumpKey := baseKey + "/control-plane.dump"
	manifestKey := baseKey + "/manifest.json"
	file, err := os.Open(dumpPath)
	if err != nil {
		return nil, err
	}
	if err := objectBackend.putObject(ctx, dumpKey, file, size); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("upload control-plane dump: %w", err)
	}
	_ = file.Close()
	if err := verifyBackupObjectSHA256(ctx, objectBackend, dumpKey, size, sha256sum); err != nil {
		return nil, fmt.Errorf("verify control-plane dump: %w", err)
	}
	storeStatus, statusErr := s.controlPlaneStoreStatus()
	invariants := map[string]string{}
	if statusErr == nil {
		invariants["store_generation"] = storeStatus.StoreGeneration
		invariants["restore_readiness"] = storeStatus.RestoreReadiness
		invariants["permission_verification_status"] = storeStatus.PermissionVerificationStatus
	}
	manifest := model.NormalizeBackupManifest(model.BackupManifest{
		RunID:             run.ID,
		PolicyID:          run.PolicyID,
		Target:            run.Target,
		Kind:              model.BackupArtifactKindControlPlanePGDump,
		Version:           version,
		Format:            "pg_dump-custom",
		ObjectKey:         dumpKey,
		ManifestObjectKey: manifestKey,
		SizeBytes:         size,
		LogicalBytes:      size,
		SHA256:            sha256sum,
		StoreFingerprint:  storeStatus.SourceFingerprint,
		Invariants:        invariants,
		Metadata: map[string]string{
			"backend_kind":      s.store.BackendKind(),
			"pg_dump":           pgDump,
			"status_error":      errorString(statusErr),
			"cnpg_integration":  "logical-pg-dump",
			"restore_drill":     "plan-available",
			"offline_restore":   "supported",
			"online_restore":    "plan-only",
			"retention_default": strconv.Itoa(model.BackupDefaultRetainCount),
		},
		CreatedAt: time.Now().UTC(),
	})
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := objectBackend.putObject(ctx, manifestKey, bytes.NewReader(manifestBytes), int64(len(manifestBytes))); err != nil {
		return nil, fmt.Errorf("upload control-plane manifest: %w", err)
	}
	artifact, err := s.store.CreateBackupArtifact(model.BackupArtifact{
		RunID:             run.ID,
		PolicyID:          run.PolicyID,
		TenantID:          run.TenantID,
		ProjectID:         run.ProjectID,
		AppID:             run.AppID,
		Target:            run.Target,
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindControlPlanePGDump,
		Version:           version,
		ObjectKey:         dumpKey,
		ManifestObjectKey: manifestKey,
		SHA256:            sha256sum,
		SizeBytes:         size,
		LogicalBytes:      size,
		Status:            model.BackupArtifactStatusActive,
		Billable:          backend.Billable,
		BillingClass:      backupBillingClass(backend),
		Manifest:          manifest,
	})
	if err != nil {
		return nil, err
	}
	return []model.BackupArtifact{artifact}, nil
}

func (s *Server) runAppDatabaseBackup(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
	if strings.TrimSpace(run.BackendID) == "" {
		return nil, fmt.Errorf("backup_backend_missing: backup backend is not configured")
	}
	if strings.TrimSpace(run.AppID) == "" && strings.TrimSpace(run.Target.AppID) == "" {
		return nil, fmt.Errorf("app_id_missing: app database backup requires an app id")
	}
	appID := firstNonEmptyString(run.AppID, run.Target.AppID)
	app, err := s.store.GetApp(appID)
	if err != nil {
		return nil, err
	}
	postgres := store.OwnedManagedPostgresSpec(app)
	if postgres == nil {
		return nil, fmt.Errorf("managed_postgres_missing: app has no Fugue-managed PostgreSQL database")
	}
	serviceName := firstNonEmptyString(run.Target.ServiceName, postgres.ServiceName)
	database := firstNonEmptyString(run.Target.Database, postgres.Database)
	user := strings.TrimSpace(postgres.User)
	password := strings.TrimSpace(postgres.Password)
	runtimeID := firstNonEmptyString(run.Target.RuntimeID, postgres.RuntimeID, app.Spec.RuntimeID)
	if serviceName == "" || database == "" || user == "" || password == "" {
		return nil, fmt.Errorf("managed_postgres_incomplete: service, database, user, and password are required")
	}
	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	host := model.PostgresRWServiceName(serviceName)
	if host == "" {
		return nil, fmt.Errorf("managed_postgres_incomplete: service name is required")
	}
	databaseURL := (&url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   host + "." + namespace + ".svc.cluster.local:5432",
		Path:   "/" + database,
		RawQuery: url.Values{
			"sslmode": []string{"disable"},
		}.Encode(),
	}).String()

	backend, err := s.store.GetBackupBackendForUse(run.BackendID, app.TenantID, false)
	if err != nil {
		backend, err = s.store.GetBackupBackendForUse(run.BackendID, "", true)
		if err != nil {
			return nil, err
		}
	}
	objectBackend, err := newDataObjectBackend(model.BackupBackendAsDataBackend(backend))
	if err != nil {
		return nil, err
	}
	tmpDir, err := os.MkdirTemp("", "fugue-app-db-backup-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)
	dumpPath := path.Join(tmpDir, "database.dump")
	pgDump := strings.TrimSpace(os.Getenv("FUGUE_PG_DUMP_BIN"))
	if pgDump == "" {
		pgDump = "pg_dump"
	}
	cmd := exec.CommandContext(ctx, pgDump, "--format=custom", "--no-owner", "--no-privileges", "--file", dumpPath, databaseURL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("pg_dump failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	size, sha256sum, err := fileSizeAndSHA256(dumpPath)
	if err != nil {
		return nil, err
	}
	version := strings.TrimSpace(run.Version)
	if version == "" {
		version = "v" + time.Now().UTC().Format("20060102-150405")
	}
	target := model.NormalizeBackupTarget(run.Target)
	target.Type = model.BackupTargetAppDatabase
	target.TenantID = app.TenantID
	target.ProjectID = app.ProjectID
	target.AppID = app.ID
	target.Name = firstNonEmptyString(target.Name, app.Name)
	target.Database = database
	target.RuntimeID = runtimeID
	target.ServiceName = serviceName
	baseKey := path.Join("apps", app.TenantID, app.ProjectID, app.ID, run.ID)
	dumpKey := baseKey + "/database.dump"
	manifestKey := baseKey + "/manifest.json"
	file, err := os.Open(dumpPath)
	if err != nil {
		return nil, err
	}
	if err := objectBackend.putObject(ctx, dumpKey, file, size); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("upload app database dump: %w", err)
	}
	_ = file.Close()
	if err := verifyBackupObjectSHA256(ctx, objectBackend, dumpKey, size, sha256sum); err != nil {
		return nil, fmt.Errorf("verify app database dump: %w", err)
	}
	manifest := model.NormalizeBackupManifest(model.BackupManifest{
		RunID:             run.ID,
		PolicyID:          run.PolicyID,
		Target:            target,
		Kind:              model.BackupArtifactKindAppPGDump,
		Version:           version,
		Format:            "pg_dump-custom",
		ObjectKey:         dumpKey,
		ManifestObjectKey: manifestKey,
		SizeBytes:         size,
		LogicalBytes:      size,
		SHA256:            sha256sum,
		Metadata: map[string]string{
			"app_id":        app.ID,
			"app_name":      app.Name,
			"tenant_id":     app.TenantID,
			"project_id":    app.ProjectID,
			"runtime_id":    runtimeID,
			"namespace":     namespace,
			"service_name":  serviceName,
			"database":      database,
			"cnpg_cluster":  serviceName,
			"pg_dump":       pgDump,
			"restore_modes": "plan-only,clone,replace",
		},
		CreatedAt: time.Now().UTC(),
	})
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := objectBackend.putObject(ctx, manifestKey, bytes.NewReader(manifestBytes), int64(len(manifestBytes))); err != nil {
		return nil, fmt.Errorf("upload app database manifest: %w", err)
	}
	artifact, err := s.store.CreateBackupArtifact(model.BackupArtifact{
		RunID:             run.ID,
		PolicyID:          run.PolicyID,
		TenantID:          app.TenantID,
		ProjectID:         app.ProjectID,
		AppID:             app.ID,
		Target:            target,
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindAppPGDump,
		Version:           version,
		ObjectKey:         dumpKey,
		ManifestObjectKey: manifestKey,
		SHA256:            sha256sum,
		SizeBytes:         size,
		LogicalBytes:      size,
		Status:            model.BackupArtifactStatusActive,
		Billable:          backend.Billable,
		BillingClass:      backupBillingClass(backend),
		Manifest:          manifest,
	})
	if err != nil {
		return nil, err
	}
	return []model.BackupArtifact{artifact}, nil
}

func (s *Server) testBackupBackend(ctx context.Context, backendID string, principal model.Principal) (bool, string) {
	ctx, cancel := context.WithTimeout(ctx, backupBackendProbeTTL)
	defer cancel()
	backend, err := s.store.GetBackupBackendForUse(backendID, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return false, err.Error()
	}
	objectBackend, err := newDataObjectBackend(model.BackupBackendAsDataBackend(backend))
	if err != nil {
		return false, err.Error()
	}
	key := path.Join(".fugue-backup-probe", model.NewID("probe")+".txt")
	payload := []byte("fugue backup backend probe\n")
	if err := objectBackend.putObject(ctx, key, bytes.NewReader(payload), int64(len(payload))); err != nil {
		return false, "write probe failed: " + err.Error()
	}
	if exists, err := objectBackend.headObject(ctx, key); err != nil {
		return false, "head probe failed: " + err.Error()
	} else if !exists {
		return false, "head probe did not find written object"
	}
	objects, err := objectBackend.listObjects(ctx, ".fugue-backup-probe")
	if err != nil {
		return false, "list probe failed: " + err.Error()
	}
	found := false
	for _, object := range objects {
		if strings.HasSuffix(object.Key, path.Base(key)) {
			found = true
			break
		}
	}
	if !found {
		return false, "list probe did not include written object"
	}
	body, _, err := objectBackend.getObject(ctx, key)
	if err != nil {
		return false, "read probe failed: " + err.Error()
	}
	readPayload, readErr := io.ReadAll(io.LimitReader(body, 1024))
	_ = body.Close()
	if readErr != nil {
		return false, "read probe failed: " + readErr.Error()
	}
	if string(readPayload) != string(payload) {
		return false, "read probe returned different payload"
	}
	if err := objectBackend.deleteObjects(ctx, []string{objectBackend.objectKey(key)}); err != nil {
		return false, "delete probe failed: " + err.Error()
	}
	return true, "write/read/list/head/delete probe succeeded"
}

func (s *Server) controlPlaneBackupDatabaseURL() string {
	if strings.TrimSpace(s.controlPlaneDatabaseURL) != "" {
		return strings.TrimSpace(s.controlPlaneDatabaseURL)
	}
	if value := strings.TrimSpace(os.Getenv("FUGUE_DATABASE_URL")); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv("DATABASE_URL"))
}

func fileSizeAndSHA256(path string) (int64, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return 0, "", err
	}
	return size, hex.EncodeToString(hash.Sum(nil)), nil
}

func verifyBackupObjectSHA256(ctx context.Context, backend *dataObjectBackend, key string, expectedSize int64, expectedSHA256 string) error {
	if backend == nil {
		return fmt.Errorf("backup object backend is not configured")
	}
	body, objectSize, err := backend.getObject(ctx, key)
	if err != nil {
		return err
	}
	defer body.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, body)
	if err != nil {
		return err
	}
	if expectedSize >= 0 && size != expectedSize {
		return fmt.Errorf("size mismatch: expected %d bytes, got %d bytes", expectedSize, size)
	}
	if objectSize > 0 && expectedSize >= 0 && objectSize != expectedSize {
		return fmt.Errorf("object metadata size mismatch: expected %d bytes, got %d bytes", expectedSize, objectSize)
	}
	actualSHA256 := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(strings.TrimSpace(expectedSHA256), actualSHA256) {
		return fmt.Errorf("sha256 mismatch: expected %s, got %s", expectedSHA256, actualSHA256)
	}
	return nil
}

func backupBillingClass(backend model.BackupBackend) string {
	if backend.Billable && model.NormalizeDataBackendProvider(backend.Provider) == model.DataBackendProviderCloudflareR2 {
		return "cloudflare-r2-standard-storage-plus-5pct"
	}
	return ""
}

func backupErrorCode(err error) string {
	message := err.Error()
	if idx := strings.Index(message, ":"); idx > 0 {
		code := strings.TrimSpace(message[:idx])
		if strings.Contains(code, "_") || strings.Contains(code, "-") {
			return code
		}
	}
	return "backup_failed"
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func parseBackupBoolQuery(r *http.Request, key string) bool {
	value := strings.TrimSpace(strings.ToLower(r.URL.Query().Get(key)))
	return value == "1" || value == "true" || value == "yes"
}

func parseLimitQuery(r *http.Request, fallback int) int {
	value := strings.TrimSpace(r.URL.Query().Get("limit"))
	if value == "" {
		return fallback
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit <= 0 {
		return fallback
	}
	if limit > 500 {
		return 500
	}
	return limit
}

func contextWithoutCancel(parent context.Context) context.Context {
	if parent == nil {
		return context.Background()
	}
	return context.WithoutCancel(parent)
}

func (s *Server) platformBackupPosture(policies []model.BackupPolicy, usage model.BackupUsage) []model.BackupPosture {
	var out []model.BackupPosture
	for _, policy := range policies {
		if policy.Target.Type != model.BackupTargetControlPlaneDatabase {
			continue
		}
		status := "ready"
		message := ""
		if policy.Status == model.BackupPolicyStatusBlockedNoBackend {
			status = "blocked"
			message = policy.DisabledReason
		}
		out = append(out, model.BackupPosture{
			Target:               policy.Target,
			Status:               status,
			Message:              message,
			PolicyID:             policy.ID,
			LastSuccessfulRunID:  policy.LastSuccessfulRunID,
			LastSuccessfulAt:     policy.LastSuccessfulAt,
			BillableBytes:        usage.BillableBytes,
			CNPGBackupIntegrated: false,
			RestoreDrillStatus:   "plan-available",
		})
	}
	return out
}

func (s *Server) writeBackupMetrics(w io.Writer) {
	policies, err := s.store.ListBackupPolicies(store.BackupPolicyFilter{IncludeDisabled: true, PlatformAdmin: true, Limit: 500})
	if err == nil {
		policyCounts := map[string]float64{}
		for _, policy := range policies {
			key := strings.Join([]string{policy.Status, policy.Scope, policy.Target.Type}, "\x00")
			policyCounts[key]++
		}
		observability.WriteMetricHeader(w, "fugue_backup_policies", "Backup policies by status, scope, and target type.", "gauge")
		for key, count := range policyCounts {
			parts := strings.Split(key, "\x00")
			observability.WriteMetricSample(w, "fugue_backup_policies", map[string]string{
				"status":      parts[0],
				"scope":       parts[1],
				"target_type": parts[2],
			}, count)
		}
	}
	runs, err := s.store.ListBackupRuns(store.BackupRunFilter{PlatformAdmin: true, Limit: 500})
	if err == nil {
		runCounts := map[string]float64{}
		for _, run := range runs {
			key := strings.Join([]string{run.Status, run.Target.Type}, "\x00")
			runCounts[key]++
		}
		observability.WriteMetricHeader(w, "fugue_backup_runs", "Backup runs by status and target type.", "gauge")
		for key, count := range runCounts {
			parts := strings.Split(key, "\x00")
			observability.WriteMetricSample(w, "fugue_backup_runs", map[string]string{
				"status":      parts[0],
				"target_type": parts[1],
			}, count)
		}
	}
	artifacts, err := s.store.ListBackupArtifacts(store.BackupArtifactFilter{PlatformAdmin: true, Limit: 500})
	if err == nil {
		artifactCounts := map[string]float64{}
		artifactBytes := map[string]float64{}
		for _, artifact := range artifacts {
			key := strings.Join([]string{artifact.Status, boolLabel(artifact.Billable), artifact.Target.Type}, "\x00")
			artifactCounts[key]++
			artifactBytes[key] += float64(artifact.SizeBytes)
		}
		observability.WriteMetricHeader(w, "fugue_backup_artifacts", "Backup artifacts by status, billing flag, and target type.", "gauge")
		observability.WriteMetricHeader(w, "fugue_backup_artifact_bytes", "Backup artifact bytes by status, billing flag, and target type.", "gauge")
		for key, count := range artifactCounts {
			parts := strings.Split(key, "\x00")
			labels := map[string]string{"status": parts[0], "billable": parts[1], "target_type": parts[2]}
			observability.WriteMetricSample(w, "fugue_backup_artifacts", labels, count)
			observability.WriteMetricSample(w, "fugue_backup_artifact_bytes", labels, artifactBytes[key])
		}
	}
	restoreRuns, err := s.store.ListBackupRestoreRuns("", true, 500)
	if err == nil {
		restoreCounts := map[string]float64{}
		for _, run := range restoreRuns {
			key := strings.Join([]string{run.Status, run.Mode}, "\x00")
			restoreCounts[key]++
		}
		observability.WriteMetricHeader(w, "fugue_backup_restore_runs", "Backup restore runs by status and mode.", "gauge")
		for key, count := range restoreCounts {
			parts := strings.Split(key, "\x00")
			observability.WriteMetricSample(w, "fugue_backup_restore_runs", map[string]string{
				"status": parts[0],
				"mode":   parts[1],
			}, count)
		}
	}
	usage, err := s.store.BackupUsage("", true)
	if err == nil {
		observability.WriteGaugeMetric(w, "fugue_backup_billable_bytes", "Billable backup storage bytes metered by Fugue.", map[string]string{
			"provider":       usage.Provider,
			"markup_percent": strconv.Itoa(usage.MarkupPercent),
		}, float64(usage.BillableBytes))
	}
}

func boolLabel(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func appBackupPosture(app model.App, policies []model.BackupPolicy, artifacts []model.BackupArtifact) []model.BackupPosture {
	targets := []model.BackupTarget{
		{Type: model.BackupTargetAppDatabase, TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, Name: app.Name},
		{Type: model.BackupTargetPersistentStorage, TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, Name: app.Name},
	}
	out := make([]model.BackupPosture, 0, len(targets))
	for _, target := range targets {
		posture := model.BackupPosture{Target: target, Status: "disabled", Message: "backup is disabled by default"}
		for _, policy := range policies {
			if policy.Target.Type != target.Type {
				continue
			}
			posture.PolicyID = policy.ID
			posture.Status = policy.Status
			if !policy.Enabled {
				posture.Status = "disabled"
				posture.Message = policy.DisabledReason
			}
			posture.LastSuccessfulRunID = policy.LastSuccessfulRunID
			posture.LastSuccessfulAt = policy.LastSuccessfulAt
			break
		}
		for _, artifact := range artifacts {
			if artifact.Target.Type == target.Type {
				posture.BillableBytes += artifact.SizeBytes
			}
		}
		out = append(out, posture)
	}
	return out
}
