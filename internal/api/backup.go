package api

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/backupschedule"
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

var (
	errBackupTargetNotAuthorized  = errors.New("tenant-scoped backup target does not belong to tenant")
	errBackupBackendNotAuthorized = errors.New("tenant-scoped backup backend does not belong to tenant")
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
	runs, err := s.store.ListStaleBackupRuns(now, backupRunLeaseTTL, 500)
	if err != nil {
		if s.log != nil {
			s.log.Printf("backup scheduler list stale runs for recovery failed: %v", err)
		}
		return
	}
	for _, run := range runs {
		updated, err := s.store.RecoverStaleBackupRun(run, now, backupRunLeaseTTL)
		if errors.Is(err, store.ErrConflict) {
			continue
		}
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

func backupRunIsStale(run model.BackupRun, now time.Time) bool {
	return store.BackupRunIsStale(run, now, backupRunLeaseTTL)
}

func (s *Server) enqueueDueBackupRetries(ctx context.Context) {
	now := time.Now().UTC()
	runs, err := s.store.ListDueBackupRetryRuns(now, 100)
	if err != nil {
		if s.log != nil {
			s.log.Printf("backup scheduler list pending retries failed: %v", err)
		}
		return
	}
	for _, run := range runs {
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
		if !s.authorizeBackupBackendMutation(w, principal, req.Name) {
			return
		}
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
	backendID := r.PathValue("id")
	if !s.authorizeBackupBackendMutation(w, principal, backendID) {
		return
	}
	backend, err := s.store.DeleteBackupBackend(backendID, principal.TenantID, principal.IsPlatformAdmin())
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
	if !s.authorizeBackupBackendMutation(w, principal, backendID) {
		return
	}
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

func (s *Server) authorizeBackupBackendMutation(w http.ResponseWriter, principal model.Principal, idOrName string) bool {
	backend, err := s.store.GetBackupBackend(idOrName, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return false
	}
	if !principal.IsPlatformAdmin() && (principal.TenantID == "" || backend.TenantID != principal.TenantID) {
		httpx.WriteError(w, http.StatusForbidden, "platform backup backends require platform administrator access for mutation")
		return false
	}
	return true
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
	policy, ok := s.backupPolicyUpsertFromRequest(w, principal, req)
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
	if err := s.authorizeTenantBackupPolicy(principal, current); err != nil {
		httpx.WriteError(w, http.StatusForbidden, "backup policy is not available to this tenant")
		return
	}
	var req backupPolicyRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.ID) != "" {
		httpx.WriteError(w, http.StatusBadRequest, "id is read-only; use the policy PATCH endpoint to update an existing policy")
		return
	}
	if backupPolicyRequestOnlyEnabled(req) {
		if *req.Enabled && strings.TrimSpace(current.BackendID) != "" {
			if _, ok := s.backupBackendIDForScope(w, principal, current.TenantID, current.BackendID); !ok {
				return
			}
		}
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

func backupPolicyRequestOnlyEnabled(req backupPolicyRequest) bool {
	return req.Enabled != nil &&
		strings.TrimSpace(req.ID) == "" &&
		strings.TrimSpace(req.TenantID) == "" &&
		strings.TrimSpace(req.ProjectID) == "" &&
		strings.TrimSpace(req.AppID) == "" &&
		strings.TrimSpace(req.Name) == "" &&
		backupTargetRequestIsEmpty(req.Target) &&
		strings.TrimSpace(req.BackendID) == "" &&
		strings.TrimSpace(req.Schedule) == "" &&
		req.RetainCount == 0 &&
		req.Retention.RetainCount == 0 &&
		req.Retention.RetainDays == 0 &&
		req.Retention.ProtectLatest == 0 &&
		strings.TrimSpace(req.Version) == ""
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
		run.Target.TenantID = principal.TenantID
		requestedTargetType := strings.TrimSpace(req.Target.Type)
		if run.PolicyID == "" && requestedTargetType == "" {
			httpx.WriteError(w, http.StatusBadRequest, "policy_id or target.type is required")
			return
		}
		if run.PolicyID == "" && requestedTargetType != "" && backupTargetRequiresPlatformAdmin(run.Target.Type) {
			httpx.WriteError(w, http.StatusForbidden, "platform backup targets require platform administrator access")
			return
		}
		if run.PolicyID == "" && requestedTargetType != "" {
			if err := s.validateTenantBackupTarget(principal.TenantID, "", "", run.Target); err != nil {
				httpx.WriteError(w, http.StatusForbidden, "backup target is not available to this tenant")
				return
			}
		}
		if run.PolicyID != "" {
			policy, err := s.store.GetBackupPolicy(run.PolicyID, principal.TenantID, false)
			if err != nil {
				s.writeStoreError(w, err)
				return
			}
			if err := s.authorizeTenantBackupPolicy(principal, policy); err != nil {
				httpx.WriteError(w, http.StatusForbidden, "backup policy is not available to this tenant")
				return
			}
			if run.BackendID == "" && policy.BackendID != "" {
				run.BackendID = policy.BackendID
			}
			run.PolicyID = policy.ID
			run.ProjectID = policy.ProjectID
			run.AppID = policy.AppID
			run.Target = policy.Target
		}
	}
	if principal.IsPlatformAdmin() && run.PolicyID != "" {
		policy, err := s.store.GetBackupPolicy(run.PolicyID, "", true)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if run.BackendID == "" && policy.BackendID != "" {
			run.BackendID = policy.BackendID
		}
		run.PolicyID = policy.ID
		run.TenantID = policy.TenantID
		run.ProjectID = policy.ProjectID
		run.AppID = policy.AppID
		run.Target = policy.Target
	}
	if principal.IsPlatformAdmin() {
		var err error
		run, err = s.applyDefaultControlPlaneBackupPolicy(run)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
	}
	if run.BackendID != "" {
		backendID, ok := s.backupBackendIDForScope(w, principal, firstNonEmptyString(run.TenantID, run.Target.TenantID), run.BackendID)
		if !ok {
			return
		}
		run.BackendID = backendID
	}
	created, err := s.store.CreateBackupRun(run)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if backupRunTerminalStatus(created.Status) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"run": created})
		return
	}
	if req.Wait {
		s.executeBackupRun(contextWithoutCancel(r.Context()), created.ID)
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

func backupRunTerminalStatus(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case model.BackupRunStatusSucceeded, model.BackupRunStatusFailed, model.BackupRunStatusCanceled, model.BackupRunStatusBlocked:
		return true
	default:
		return false
	}
}

func (s *Server) applyDefaultControlPlaneBackupPolicy(run model.BackupRun) (model.BackupRun, error) {
	run = model.NormalizeBackupRun(run)
	if run.PolicyID != "" || run.BackendID != "" || run.Target.Type != model.BackupTargetControlPlaneDatabase {
		return run, nil
	}
	if run.Target.TenantID != "" || run.Target.ProjectID != "" || run.Target.AppID != "" {
		return run, nil
	}
	policy, err := s.store.GetBackupPolicy(s.store.DefaultControlPlaneBackupPolicyID(), "", true)
	if errors.Is(err, store.ErrNotFound) {
		return run, nil
	}
	if err != nil {
		return run, err
	}
	policy = model.NormalizeBackupPolicy(policy)
	if !policy.Enabled || policy.Status != model.BackupPolicyStatusActive || policy.BackendID == "" {
		return run, nil
	}
	run.PolicyID = policy.ID
	return run, nil
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
	artifact, err := s.store.MarkBackupArtifactDeleted(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
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
	artifact, err := s.store.GetBackupArtifact(req.ArtifactID, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if artifact.Status != model.BackupArtifactStatusActive {
		httpx.WriteError(w, http.StatusConflict, "backup artifact is not active")
		return
	}
	if !principal.IsPlatformAdmin() && (artifact.TenantID == "" || artifact.TenantID != principal.TenantID) {
		httpx.WriteError(w, http.StatusForbidden, "backup artifact is not available to this tenant")
		return
	}
	target := req.Target
	if backupTargetRequestIsEmpty(target) {
		target = artifact.Target
	}
	target = model.NormalizeBackupTarget(target)
	if !principal.IsPlatformAdmin() {
		if err := s.validateTenantBackupTarget(principal.TenantID, target.ProjectID, target.AppID, target); err != nil {
			httpx.WriteError(w, http.StatusForbidden, "backup restore target is not available to this tenant")
			return
		}
		target.TenantID = principal.TenantID
	}
	if target.Type != model.NormalizeBackupTarget(artifact.Target).Type {
		httpx.WriteError(w, http.StatusBadRequest, "backup artifact target type is not compatible with the restore target")
		return
	}
	planTenantID := firstNonEmptyString(target.TenantID, artifact.TenantID)
	planProjectID := firstNonEmptyString(target.ProjectID, artifact.ProjectID)
	planAppID := firstNonEmptyString(target.AppID, artifact.AppID)
	plan, err := s.store.CreateBackupRestorePlan(model.BackupRestorePlan{
		ArtifactID:    req.ArtifactID,
		TenantID:      planTenantID,
		ProjectID:     planProjectID,
		AppID:         planAppID,
		Target:        target,
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

func backupTargetRequestIsEmpty(target model.BackupTarget) bool {
	return strings.TrimSpace(target.Type) == "" &&
		strings.TrimSpace(target.TenantID) == "" &&
		strings.TrimSpace(target.ProjectID) == "" &&
		strings.TrimSpace(target.AppID) == "" &&
		strings.TrimSpace(target.WorkspaceID) == "" &&
		strings.TrimSpace(target.RuntimeID) == "" &&
		strings.TrimSpace(target.Name) == "" &&
		strings.TrimSpace(target.ServiceName) == "" &&
		strings.TrimSpace(target.Database) == "" &&
		strings.TrimSpace(target.Component) == ""
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
	plan, err := s.store.GetBackupRestorePlan(req.PlanID, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	mode := model.NormalizeBackupRestoreMode(firstNonEmptyString(req.Mode, plan.Mode))
	phases := append([]model.BackupRestorePhase(nil), plan.Phases...)
	if mode == model.BackupRestoreModeReplace {
		protectiveRun, err := s.createProtectiveBackupRunForRestore(principal, plan)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if protectiveRun.Status == model.BackupRunStatusBlocked {
			httpx.WriteError(w, http.StatusConflict, protectiveRun.ErrorMessage)
			return
		}
		phases = annotateProtectiveBackupPhase(phases, protectiveRun.ID)
		go s.executeBackupRun(contextWithoutCancel(r.Context()), protectiveRun.ID)
	}
	run, err := s.store.CreateBackupRestoreRun(model.BackupRestoreRun{
		PlanID:          req.PlanID,
		Mode:            mode,
		Status:          model.BackupRestoreStatusPlanned,
		Phases:          phases,
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
		"posture":   s.appBackupPosture(app, policies, artifacts),
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
	if strings.TrimSpace(req.ID) != "" {
		current, err := s.store.GetBackupPolicy(req.ID, app.TenantID, principal.IsPlatformAdmin())
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if !backupPolicyTargetsApp(current, app) {
			httpx.WriteError(w, http.StatusForbidden, "backup policy is not available for this app")
			return
		}
	}
	policy, ok := s.backupPolicyUpsertFromRequest(w, principal, req)
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
	if target.Type == model.BackupTargetPersistentStorage {
		storage, _ := appPersistentStorageBackupSpec(app)
		if storage == nil {
			httpx.WriteError(w, http.StatusBadRequest, "app has no persistent storage backup target")
			return model.BackupTarget{}, false
		}
		target.RuntimeID = firstNonEmptyString(target.RuntimeID, app.Spec.RuntimeID)
		return target, true
	}
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
		requestedTargetType := model.BackupTargetAppDatabase
		if strings.TrimSpace(req.Target.Type) != "" {
			requestedTargetType = model.NormalizeBackupTargetType(req.Target.Type)
		}
		policies, err := s.store.ListBackupPolicies(store.BackupPolicyFilter{
			TenantID:        app.TenantID,
			AppID:           app.ID,
			TargetType:      requestedTargetType,
			IncludeDisabled: false,
			PlatformAdmin:   principal.IsPlatformAdmin(),
			Limit:           100,
		})
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		policy, found := preferredBackupPolicy(policies)
		if !found {
			httpx.WriteError(w, http.StatusConflict, "app backup is disabled")
			return
		}
		req.PolicyID = policy.ID
	}
	policy, err := s.store.GetBackupPolicy(req.PolicyID, app.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !backupPolicyTargetsApp(policy, app) {
		httpx.WriteError(w, http.StatusForbidden, "backup policy is not available for this app")
		return
	}
	if err := s.authorizeTenantBackupPolicy(principal, policy); err != nil {
		httpx.WriteError(w, http.StatusForbidden, "backup policy is not available to this tenant")
		return
	}
	backendID := strings.TrimSpace(req.BackendID)
	if backendID == "" {
		backendID = policy.BackendID
	}
	if backendID != "" {
		var ok bool
		backendID, ok = s.backupBackendIDForScope(w, principal, app.TenantID, backendID)
		if !ok {
			return
		}
	}
	run, err := s.store.CreateBackupRun(model.BackupRun{
		PolicyID:        policy.ID,
		TenantID:        app.TenantID,
		ProjectID:       app.ProjectID,
		AppID:           app.ID,
		Target:          policy.Target,
		BackendID:       backendID,
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

func (s *Server) backupPolicyUpsertFromRequest(w http.ResponseWriter, principal model.Principal, req backupPolicyRequest) (model.BackupPolicy, bool) {
	var current *model.BackupPolicy
	if id := strings.TrimSpace(req.ID); id != "" {
		policy, err := s.store.GetBackupPolicy(id, principal.TenantID, principal.IsPlatformAdmin())
		if err != nil {
			s.writeStoreError(w, err)
			return model.BackupPolicy{}, false
		}
		if err := s.authorizeTenantBackupPolicy(principal, policy); err != nil {
			httpx.WriteError(w, http.StatusForbidden, "backup policy is not available to this tenant")
			return model.BackupPolicy{}, false
		}
		current = &policy
	} else if principal.IsPlatformAdmin() && req.Name == "" &&
		model.NormalizeBackupTargetType(req.Target.Type) == model.BackupTargetControlPlaneDatabase &&
		strings.TrimSpace(req.TenantID) == "" && strings.TrimSpace(req.ProjectID) == "" && strings.TrimSpace(req.AppID) == "" &&
		strings.TrimSpace(req.Target.TenantID) == "" && strings.TrimSpace(req.Target.ProjectID) == "" && strings.TrimSpace(req.Target.AppID) == "" {
		policy, err := s.store.GetBackupPolicy(s.store.DefaultControlPlaneBackupPolicyID(), "", true)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			s.writeStoreError(w, err)
			return model.BackupPolicy{}, false
		}
		if err == nil {
			current = &policy
		}
	}

	policy, ok := s.backupPolicyFromRequest(w, principal, req, current)
	if !ok {
		return model.BackupPolicy{}, false
	}
	if current == nil {
		policies, err := s.store.ListBackupPolicies(store.BackupPolicyFilter{
			TenantID:        policy.TenantID,
			IncludeDisabled: true,
			PlatformAdmin:   principal.IsPlatformAdmin(),
		})
		if err != nil {
			s.writeStoreError(w, err)
			return model.BackupPolicy{}, false
		}
		for idx := range policies {
			candidate := model.NormalizeBackupPolicy(policies[idx])
			if candidate.TenantID != policy.TenantID || candidate.ProjectID != policy.ProjectID || candidate.AppID != policy.AppID || candidate.Slug != policy.Slug {
				continue
			}
			if err := s.authorizeTenantBackupPolicy(principal, candidate); err != nil {
				httpx.WriteError(w, http.StatusForbidden, "backup policy is not available to this tenant")
				return model.BackupPolicy{}, false
			}
			current = &candidate
			policy, ok = s.backupPolicyFromRequest(w, principal, req, current)
			if !ok {
				return model.BackupPolicy{}, false
			}
			break
		}
	}
	if current != nil {
		policy.ID = current.ID
		policy.CreatedAt = current.CreatedAt
	}
	return policy, true
}

func mergeBackupTargetDefaults(target, defaults model.BackupTarget) model.BackupTarget {
	target = model.NormalizeBackupTarget(target)
	defaults = model.NormalizeBackupTarget(defaults)
	target.TenantID = firstNonEmptyString(target.TenantID, defaults.TenantID)
	target.ProjectID = firstNonEmptyString(target.ProjectID, defaults.ProjectID)
	target.AppID = firstNonEmptyString(target.AppID, defaults.AppID)
	target.WorkspaceID = firstNonEmptyString(target.WorkspaceID, defaults.WorkspaceID)
	target.RuntimeID = firstNonEmptyString(target.RuntimeID, defaults.RuntimeID)
	target.Name = firstNonEmptyString(target.Name, defaults.Name)
	target.ServiceName = firstNonEmptyString(target.ServiceName, defaults.ServiceName)
	target.Database = firstNonEmptyString(target.Database, defaults.Database)
	target.Component = firstNonEmptyString(target.Component, defaults.Component)
	return model.NormalizeBackupTarget(target)
}

func (s *Server) backupPolicyFromRequest(w http.ResponseWriter, principal model.Principal, req backupPolicyRequest, current *model.BackupPolicy) (model.BackupPolicy, bool) {
	policy := model.BackupPolicy{}
	if current != nil {
		policy = *current
	}
	if req.Name != "" {
		policy.Name = strings.TrimSpace(req.Name)
	}
	if policy.Name == "" {
		policy.Name = defaultBackupPolicyName(req.Target)
	}
	if req.Target.Type != "" {
		requestedTarget := model.NormalizeBackupTarget(req.Target)
		if current != nil && requestedTarget.Type == model.NormalizeBackupTarget(current.Target).Type {
			requestedTarget = mergeBackupTargetDefaults(requestedTarget, current.Target)
		}
		policy.Target = requestedTarget
	}
	if policy.Target.Type == "" {
		httpx.WriteError(w, http.StatusBadRequest, "target.type is required")
		return model.BackupPolicy{}, false
	}
	if !principal.IsPlatformAdmin() && backupTargetRequiresPlatformAdmin(policy.Target.Type) {
		httpx.WriteError(w, http.StatusForbidden, "platform backup targets require platform administrator access")
		return model.BackupPolicy{}, false
	}
	if req.TenantID != "" || current == nil {
		policy.TenantID = strings.TrimSpace(req.TenantID)
	}
	if !principal.IsPlatformAdmin() {
		policy.TenantID = principal.TenantID
		policy.Target.TenantID = principal.TenantID
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
	if policy.Enabled {
		if err := backupschedule.Validate(policy.Schedule); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "invalid backup schedule: "+err.Error())
			return model.BackupPolicy{}, false
		}
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
	if err := s.authorizeTenantBackupPolicy(principal, policy); err != nil {
		httpx.WriteError(w, http.StatusForbidden, "backup target is not available to this tenant")
		return model.BackupPolicy{}, false
	}
	if policy.BackendID != "" {
		backendID, ok := s.backupBackendIDForScope(w, principal, policy.TenantID, policy.BackendID)
		if !ok {
			return model.BackupPolicy{}, false
		}
		policy.BackendID = backendID
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

func backupTargetRequiresPlatformAdmin(targetType string) bool {
	switch model.NormalizeBackupTargetType(targetType) {
	case model.BackupTargetControlPlaneDatabase, model.BackupTargetRegistry, model.BackupTargetPlatformComponent:
		return true
	default:
		return false
	}
}

func (s *Server) authorizeTenantBackupPolicy(principal model.Principal, policy model.BackupPolicy) error {
	if principal.IsPlatformAdmin() {
		return nil
	}
	tenantID := strings.TrimSpace(principal.TenantID)
	if tenantID == "" || strings.TrimSpace(policy.TenantID) != tenantID {
		return errBackupTargetNotAuthorized
	}
	return s.validateTenantBackupTarget(tenantID, policy.ProjectID, policy.AppID, policy.Target)
}

func backupPolicyTargetsApp(policy model.BackupPolicy, app model.App) bool {
	policy = model.NormalizeBackupPolicy(policy)
	if strings.TrimSpace(policy.TenantID) != strings.TrimSpace(app.TenantID) {
		return false
	}
	if policy.ProjectID != "" && policy.ProjectID != app.ProjectID {
		return false
	}
	if policy.Target.TenantID != "" && policy.Target.TenantID != app.TenantID {
		return false
	}
	if policy.Target.ProjectID != "" && policy.Target.ProjectID != app.ProjectID {
		return false
	}
	if policy.AppID != "" && policy.AppID != app.ID {
		return false
	}
	if policy.Target.AppID != "" && policy.Target.AppID != app.ID {
		return false
	}
	return policy.AppID != "" || policy.Target.AppID != ""
}

func (s *Server) backupBackendIDForPrincipal(w http.ResponseWriter, principal model.Principal, idOrName string) (string, bool) {
	return s.backupBackendIDForScope(w, principal, principal.TenantID, idOrName)
}

func (s *Server) backupBackendIDForScope(w http.ResponseWriter, principal model.Principal, tenantID, idOrName string) (string, bool) {
	tenantID = strings.TrimSpace(tenantID)
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" {
		return "", true
	}
	backend, err := s.store.GetBackupBackend(idOrName, tenantID, false)
	if err != nil {
		if !principal.IsPlatformAdmin() && errors.Is(err, store.ErrNotFound) {
			httpx.WriteError(w, http.StatusForbidden, "backup backend is not available to this tenant")
		} else {
			s.writeStoreError(w, err)
		}
		return "", false
	}
	if backend.TenantID != "" && backend.TenantID != tenantID {
		httpx.WriteError(w, http.StatusForbidden, "backup backend is not available to this tenant")
		return "", false
	}
	return backend.ID, true
}

func (s *Server) validateTenantBackupBackend(tenantID, idOrName string) error {
	tenantID = strings.TrimSpace(tenantID)
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" {
		return nil
	}
	if tenantID == "" {
		return errBackupBackendNotAuthorized
	}
	backend, err := s.store.GetBackupBackend(idOrName, tenantID, false)
	if err != nil || backend.TenantID != "" && backend.TenantID != tenantID {
		return errBackupBackendNotAuthorized
	}
	return nil
}

func (s *Server) validateTenantBackupTarget(tenantID, projectID, appID string, target model.BackupTarget) error {
	tenantID = strings.TrimSpace(tenantID)
	projectID = strings.TrimSpace(projectID)
	appID = strings.TrimSpace(appID)
	target = model.NormalizeBackupTarget(target)
	if tenantID == "" || backupTargetRequiresPlatformAdmin(target.Type) {
		return errBackupTargetNotAuthorized
	}
	if target.TenantID != "" && target.TenantID != tenantID {
		return errBackupTargetNotAuthorized
	}
	if projectID != "" && target.ProjectID != "" && projectID != target.ProjectID {
		return errBackupTargetNotAuthorized
	}
	if appID != "" && target.AppID != "" && appID != target.AppID {
		return errBackupTargetNotAuthorized
	}

	effectiveProjectID := firstNonEmptyString(projectID, target.ProjectID)
	if effectiveProjectID != "" {
		project, err := s.store.GetProject(effectiveProjectID)
		if err != nil || project.TenantID != tenantID {
			return errBackupTargetNotAuthorized
		}
	}

	effectiveAppID := firstNonEmptyString(appID, target.AppID)
	if effectiveAppID != "" {
		app, err := s.store.GetApp(effectiveAppID)
		if err != nil || app.TenantID != tenantID {
			return errBackupTargetNotAuthorized
		}
		if effectiveProjectID != "" && app.ProjectID != effectiveProjectID {
			return errBackupTargetNotAuthorized
		}
	}
	if target.RuntimeID != "" {
		visible, err := s.store.RuntimeVisibleToTenant(target.RuntimeID, tenantID, false)
		if err != nil || !visible {
			return errBackupTargetNotAuthorized
		}
	}

	workspaceID := strings.TrimSpace(target.WorkspaceID)
	if target.Type == model.BackupTargetDataWorkspace {
		workspaceID = firstNonEmptyString(workspaceID, target.Name)
	}
	if workspaceID != "" {
		workspace, err := s.store.GetDataWorkspace(workspaceID, tenantID, false)
		if err != nil || workspace.TenantID != tenantID {
			return errBackupTargetNotAuthorized
		}
		if effectiveProjectID != "" && workspace.ProjectID != effectiveProjectID {
			return errBackupTargetNotAuthorized
		}
	}
	return nil
}

func (s *Server) executeBackupRun(parent context.Context, runID string) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, backupRunTimeout)
	defer cancel()
	now := time.Now().UTC()
	leaseOwner := backupLeaseOwner()
	run, err := s.store.ClaimBackupRun(runID, leaseOwner, now, backupRunLeaseTTL)
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			return
		}
		if s.log != nil {
			s.log.Printf("claim backup run %s failed: %v", runID, err)
		}
		return
	}
	stopHeartbeat := s.startBackupRunHeartbeat(ctx, cancel, run.ID, leaseOwner)
	runner := s.backupRunner
	if runner == nil {
		runner = s.runBackup
	}
	artifacts, err := runner(ctx, run)
	finishedAt := time.Now().UTC()
	stopHeartbeat()
	status := model.BackupRunStatusSucceeded
	if err != nil {
		status = model.BackupRunStatusFailed
		code := backupErrorCode(err)
		message := err.Error()
		finished, finishErr := s.store.FinishBackupRun(run.ID, leaseOwner, store.BackupRunFinish{
			Status:       status,
			ErrorCode:    code,
			ErrorMessage: message,
			FinishedAt:   finishedAt,
		})
		if s.log != nil {
			s.log.Printf("backup run %s failed: %v", run.ID, err)
		}
		if finishErr != nil {
			if s.log != nil && !errors.Is(finishErr, store.ErrConflict) {
				s.log.Printf("finish failed backup run %s failed: %v", run.ID, finishErr)
			}
			return
		}
		s.scheduleBackupRetry(contextWithoutCancel(parent), finished)
		return
	}
	var bytesWritten int64
	for _, artifact := range artifacts {
		bytesWritten += artifact.SizeBytes
	}
	count := len(artifacts)
	_, finishErr := s.store.FinishBackupRun(run.ID, leaseOwner, store.BackupRunFinish{
		Status:        status,
		BytesWritten:  bytesWritten,
		ArtifactCount: count,
		FinishedAt:    finishedAt,
	})
	if finishErr != nil && s.log != nil && !errors.Is(finishErr, store.ErrConflict) {
		s.log.Printf("finish successful backup run %s failed: %v", run.ID, finishErr)
	}
}

func (s *Server) startBackupRunHeartbeat(ctx context.Context, cancel context.CancelFunc, runID, leaseOwner string) func() {
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
				if _, err := s.store.HeartbeatBackupRun(runID, leaseOwner, now, backupRunLeaseTTL); err != nil {
					if errors.Is(err, store.ErrConflict) {
						if cancel != nil {
							cancel()
						}
						return
					}
					if s.log != nil {
						s.log.Printf("heartbeat backup run %s failed: %v", runID, err)
					}
				}
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
	if strings.TrimSpace(run.TenantID) != "" {
		if err := s.validateTenantBackupTarget(run.TenantID, run.ProjectID, run.AppID, run.Target); err != nil {
			return nil, errBackupTargetNotAuthorized
		}
		if err := s.validateTenantBackupBackend(run.TenantID, run.BackendID); err != nil {
			return nil, errBackupBackendNotAuthorized
		}
	}
	switch model.NormalizeBackupTargetType(run.Target.Type) {
	case model.BackupTargetControlPlaneDatabase:
		return s.runControlPlaneDatabaseBackup(ctx, run)
	case model.BackupTargetAppDatabase:
		return s.runAppDatabaseBackup(ctx, run)
	case model.BackupTargetPersistentStorage:
		return s.runPersistentStorageBackup(ctx, run)
	case model.BackupTargetDataWorkspace:
		return s.runDataWorkspaceBackup(ctx, run)
	case model.BackupTargetRegistry:
		return s.runRegistryBackup(ctx, run)
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
			"cnpg_integration":  s.controlPlaneCNPGBackupIntegrationLabel(),
			"cnpg_backup_name":  s.controlPlaneCNPGBackupName,
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
	artifact, err := s.store.CreateBackupArtifactForRun(model.BackupArtifact{
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
	}, run.LeaseOwner)
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
		return nil, err
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
	artifact, err := s.store.CreateBackupArtifactForRun(model.BackupArtifact{
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
	}, run.LeaseOwner)
	if err != nil {
		return nil, err
	}
	return []model.BackupArtifact{artifact}, nil
}

func (s *Server) runPersistentStorageBackup(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
	if strings.TrimSpace(run.BackendID) == "" {
		return nil, fmt.Errorf("backup_backend_missing: backup backend is not configured")
	}
	appID := firstNonEmptyString(run.AppID, run.Target.AppID)
	if appID == "" {
		return nil, fmt.Errorf("app_id_missing: persistent storage backup requires an app id")
	}
	app, err := s.store.GetApp(appID)
	if err != nil {
		return nil, err
	}
	storage, storageKind := appPersistentStorageBackupSpec(app)
	if storage == nil {
		return nil, fmt.Errorf("persistent_storage_missing: app has no persistent storage or workspace backup target")
	}
	sourceRoot, ok := persistentStorageBackupSourceRoot(app)
	if !ok {
		return nil, fmt.Errorf("persistent_storage_source_unavailable: FUGUE_BACKUP_PERSISTENT_STORAGE_ROOT is not mounted for the file-level worker")
	}
	info, err := os.Stat(sourceRoot)
	if err != nil {
		return nil, fmt.Errorf("stat persistent storage source: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("persistent_storage_source_invalid: %s is not a directory", sourceRoot)
	}
	backend, objectBackend, err := s.backupObjectBackendForRun(run, app.TenantID, false)
	if err != nil {
		return nil, err
	}
	tmpDir, err := os.MkdirTemp("", "fugue-persistent-storage-backup-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)
	archivePath := filepath.Join(tmpDir, "persistent-storage.tar.gz")
	files, logicalBytes, err := writeFileArchive(sourceRoot, archivePath)
	if err != nil {
		return nil, err
	}
	size, sha256sum, err := fileSizeAndSHA256(archivePath)
	if err != nil {
		return nil, err
	}
	version := backupVersionLabel(run.Version)
	target := model.NormalizeBackupTarget(run.Target)
	target.Type = model.BackupTargetPersistentStorage
	target.TenantID = app.TenantID
	target.ProjectID = app.ProjectID
	target.AppID = app.ID
	target.Name = firstNonEmptyString(target.Name, app.Name)
	target.RuntimeID = firstNonEmptyString(target.RuntimeID, app.Spec.RuntimeID)
	baseKey := path.Join("apps", app.TenantID, app.ProjectID, app.ID, run.ID, "persistent-storage")
	archiveKey := baseKey + "/persistent-storage.tar.gz"
	manifestKey := baseKey + "/manifest.json"
	file, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	if err := objectBackend.putObject(ctx, archiveKey, file, size); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("upload persistent storage archive: %w", err)
	}
	_ = file.Close()
	if err := verifyBackupObjectSHA256(ctx, objectBackend, archiveKey, size, sha256sum); err != nil {
		return nil, fmt.Errorf("verify persistent storage archive: %w", err)
	}
	manifest := model.NormalizeBackupManifest(model.BackupManifest{
		RunID:             run.ID,
		PolicyID:          run.PolicyID,
		Target:            target,
		Kind:              model.BackupArtifactKindFileArchive,
		Version:           version,
		Format:            "tar+gzip",
		Compression:       "gzip",
		ObjectKey:         archiveKey,
		ManifestObjectKey: manifestKey,
		SizeBytes:         size,
		LogicalBytes:      logicalBytes,
		SHA256:            sha256sum,
		Metadata: map[string]string{
			"app_id":                    app.ID,
			"app_name":                  app.Name,
			"tenant_id":                 app.TenantID,
			"project_id":                app.ProjectID,
			"runtime_id":                target.RuntimeID,
			"storage_kind":              storageKind,
			"storage_mode":              storage.Mode,
			"storage_class":             storage.StorageClassName,
			"claim_name":                storage.ClaimName,
			"backup_strategy":           "file-archive",
			"source_root":               sourceRoot,
			"restore_target":            "new-pvc",
			"restore_verification":      "manifest-checksum-tar-headers",
			"cutover":                   "normal-deploy-operation",
			"rollback":                  "retain-old-pvc-until-explicit-delete",
			"volume_snapshot_supported": boolStringFromBool(detectCSIVolumeSnapshotSupport()),
			"pvc_clone_supported":       boolStringFromBool(storageSupportsPVCClone(*storage)),
		},
		Files:     files,
		CreatedAt: time.Now().UTC(),
	})
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := objectBackend.putObject(ctx, manifestKey, bytes.NewReader(manifestBytes), int64(len(manifestBytes))); err != nil {
		return nil, fmt.Errorf("upload persistent storage manifest: %w", err)
	}
	artifact, err := s.store.CreateBackupArtifactForRun(model.BackupArtifact{
		RunID:             run.ID,
		PolicyID:          run.PolicyID,
		TenantID:          app.TenantID,
		ProjectID:         app.ProjectID,
		AppID:             app.ID,
		Target:            target,
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindFileArchive,
		Version:           version,
		ObjectKey:         archiveKey,
		ManifestObjectKey: manifestKey,
		SHA256:            sha256sum,
		SizeBytes:         size,
		LogicalBytes:      logicalBytes,
		Status:            model.BackupArtifactStatusActive,
		Billable:          backend.Billable,
		BillingClass:      backupBillingClass(backend),
		Manifest:          manifest,
	}, run.LeaseOwner)
	if err != nil {
		return nil, err
	}
	return []model.BackupArtifact{artifact}, nil
}

func (s *Server) runDataWorkspaceBackup(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
	if strings.TrimSpace(run.BackendID) == "" {
		return nil, fmt.Errorf("backup_backend_missing: backup backend is not configured")
	}
	workspaceID := firstNonEmptyString(run.Target.WorkspaceID, run.Target.Name)
	if workspaceID == "" {
		return nil, fmt.Errorf("workspace_id_missing: data workspace backup requires a workspace id")
	}
	workspace, err := s.store.GetDataWorkspace(workspaceID, run.TenantID, run.TenantID == "")
	if err != nil {
		return nil, err
	}
	snapshot, err := s.resolveDataWorkspaceBackupSnapshot(workspace, run)
	if err != nil {
		return nil, err
	}
	backend, objectBackend, err := s.backupObjectBackendForRun(run, workspace.TenantID, false)
	if err != nil {
		return nil, err
	}
	version := backupVersionLabel(firstNonEmptyString(run.Version, snapshot.Version))
	target := model.NormalizeBackupTarget(run.Target)
	target.Type = model.BackupTargetDataWorkspace
	target.TenantID = workspace.TenantID
	target.ProjectID = workspace.ProjectID
	target.WorkspaceID = workspace.ID
	target.Name = workspace.Name
	baseKey := path.Join("data-workspaces", workspace.TenantID, workspace.ProjectID, workspace.ID, run.ID)
	manifestKey := baseKey + "/manifest.json"
	manifest := model.NormalizeBackupManifest(model.BackupManifest{
		RunID:             run.ID,
		PolicyID:          run.PolicyID,
		Target:            target,
		Kind:              model.BackupArtifactKindDataSnapshot,
		Version:           version,
		Format:            "data-workspace-snapshot-reference",
		ObjectKey:         snapshot.ID,
		ManifestObjectKey: manifestKey,
		SizeBytes:         snapshot.TotalBytes,
		LogicalBytes:      snapshot.TotalBytes,
		SHA256:            snapshot.ManifestDigest,
		Metadata: map[string]string{
			"workspace_id":           workspace.ID,
			"workspace_name":         workspace.Name,
			"snapshot_id":            snapshot.ID,
			"snapshot_version":       snapshot.Version,
			"manifest_digest":        snapshot.ManifestDigest,
			"file_count":             strconv.Itoa(snapshot.FileCount),
			"asset_count":            strconv.Itoa(snapshot.AssetCount),
			"total_bytes":            strconv.FormatInt(snapshot.TotalBytes, 10),
			"storage_backend_id":     workspace.StorageBackendID,
			"retention_protection":   "protected-backup-artifacts-are-not-expired",
			"restore_plan":           "data-workspace-snapshot-materialization",
			"blob_retention_policy":  "do-not-delete-blobs-referenced-by-protected-snapshots",
			"snapshot_creation_mode": dataWorkspaceSnapshotCreationMode(run, snapshot),
		},
		CreatedAt: time.Now().UTC(),
	})
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := objectBackend.putObject(ctx, manifestKey, bytes.NewReader(manifestBytes), int64(len(manifestBytes))); err != nil {
		return nil, fmt.Errorf("upload data workspace backup manifest: %w", err)
	}
	artifact, err := s.store.CreateBackupArtifactForRun(model.BackupArtifact{
		RunID:             run.ID,
		PolicyID:          run.PolicyID,
		TenantID:          workspace.TenantID,
		ProjectID:         workspace.ProjectID,
		Target:            target,
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindDataSnapshot,
		Version:           version,
		ObjectKey:         snapshot.ID,
		ManifestObjectKey: manifestKey,
		SHA256:            snapshot.ManifestDigest,
		SizeBytes:         int64(len(manifestBytes)),
		LogicalBytes:      snapshot.TotalBytes,
		Status:            model.BackupArtifactStatusActive,
		Protected:         true,
		Billable:          backend.Billable,
		BillingClass:      backupBillingClass(backend),
		ManifestDigest:    snapshot.ManifestDigest,
		Manifest:          manifest,
	}, run.LeaseOwner)
	if err != nil {
		return nil, err
	}
	return []model.BackupArtifact{artifact}, nil
}

func (s *Server) runRegistryBackup(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
	if strings.TrimSpace(run.BackendID) == "" {
		return nil, fmt.Errorf("backup_backend_missing: backup backend is not configured")
	}
	backend, objectBackend, err := s.backupObjectBackendForRun(run, "", true)
	if err != nil {
		return nil, err
	}
	version := backupVersionLabel(run.Version)
	target := model.NormalizeBackupTarget(run.Target)
	target.Type = model.BackupTargetRegistry
	target.Component = firstNonEmptyString(target.Component, "registry")
	baseKey := path.Join("platform", "registry", run.ID)
	if s.registryIsExternalized() {
		manifestKey := baseKey + "/manifest.json"
		manifest := model.NormalizeBackupManifest(model.BackupManifest{
			RunID:             run.ID,
			PolicyID:          run.PolicyID,
			Target:            target,
			Kind:              model.BackupArtifactKindRegistryArchive,
			Version:           version,
			Format:            "externalized-registry-reference",
			ManifestObjectKey: manifestKey,
			Metadata: map[string]string{
				"registry_push_base": s.registryPushBase,
				"registry_pull_base": s.registryPullBase,
				"registry_mirror":    s.clusterJoinRegistryEndpoint,
				"externalized":       "true",
				"backup_strategy":    "external-provider-backup",
			},
			CreatedAt: time.Now().UTC(),
		})
		manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(manifestBytes)
		sha256sum := hex.EncodeToString(sum[:])
		manifest.SHA256 = sha256sum
		if err := objectBackend.putObject(ctx, manifestKey, bytes.NewReader(manifestBytes), int64(len(manifestBytes))); err != nil {
			return nil, fmt.Errorf("upload registry reference manifest: %w", err)
		}
		artifact, err := s.store.CreateBackupArtifactForRun(model.BackupArtifact{
			RunID:             run.ID,
			PolicyID:          run.PolicyID,
			Target:            target,
			BackendID:         backend.ID,
			Kind:              model.BackupArtifactKindRegistryArchive,
			Version:           version,
			ManifestObjectKey: manifestKey,
			SHA256:            sha256sum,
			SizeBytes:         int64(len(manifestBytes)),
			LogicalBytes:      0,
			Status:            model.BackupArtifactStatusActive,
			Billable:          backend.Billable,
			BillingClass:      backupBillingClass(backend),
			Manifest:          manifest,
		}, run.LeaseOwner)
		if err != nil {
			return nil, err
		}
		return []model.BackupArtifact{artifact}, nil
	}
	sourceRoot := strings.TrimSpace(os.Getenv("FUGUE_BACKUP_REGISTRY_ROOT"))
	if sourceRoot == "" {
		return nil, fmt.Errorf("registry_source_unavailable: bundled registry backup requires FUGUE_BACKUP_REGISTRY_ROOT to be mounted")
	}
	if gcRunning := strings.TrimSpace(os.Getenv("FUGUE_BACKUP_REGISTRY_GC_RUNNING")); strings.EqualFold(gcRunning, "true") {
		return nil, fmt.Errorf("registry_gc_running: registry backup is blocked while registry GC is running")
	}
	tmpDir, err := os.MkdirTemp("", "fugue-registry-backup-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)
	archivePath := filepath.Join(tmpDir, "registry.tar.gz")
	files, logicalBytes, err := writeFileArchive(sourceRoot, archivePath)
	if err != nil {
		return nil, err
	}
	size, sha256sum, err := fileSizeAndSHA256(archivePath)
	if err != nil {
		return nil, err
	}
	archiveKey := baseKey + "/registry.tar.gz"
	manifestKey := baseKey + "/manifest.json"
	file, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	if err := objectBackend.putObject(ctx, archiveKey, file, size); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("upload registry archive: %w", err)
	}
	_ = file.Close()
	if err := verifyBackupObjectSHA256(ctx, objectBackend, archiveKey, size, sha256sum); err != nil {
		return nil, fmt.Errorf("verify registry archive: %w", err)
	}
	manifest := model.NormalizeBackupManifest(model.BackupManifest{
		RunID:             run.ID,
		PolicyID:          run.PolicyID,
		Target:            target,
		Kind:              model.BackupArtifactKindRegistryArchive,
		Version:           version,
		Format:            "tar+gzip",
		Compression:       "gzip",
		ObjectKey:         archiveKey,
		ManifestObjectKey: manifestKey,
		SizeBytes:         size,
		LogicalBytes:      logicalBytes,
		SHA256:            sha256sum,
		Metadata: map[string]string{
			"backup_strategy":        "file-archive",
			"registry_gc_lease_name": s.registryGCLeaseName,
			"gc_coordination":        "blocked-when-FUGUE_BACKUP_REGISTRY_GC_RUNNING=true",
			"source_root":            sourceRoot,
		},
		Files:     files,
		CreatedAt: time.Now().UTC(),
	})
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := objectBackend.putObject(ctx, manifestKey, bytes.NewReader(manifestBytes), int64(len(manifestBytes))); err != nil {
		return nil, fmt.Errorf("upload registry manifest: %w", err)
	}
	artifact, err := s.store.CreateBackupArtifactForRun(model.BackupArtifact{
		RunID:             run.ID,
		PolicyID:          run.PolicyID,
		Target:            target,
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindRegistryArchive,
		Version:           version,
		ObjectKey:         archiveKey,
		ManifestObjectKey: manifestKey,
		SHA256:            sha256sum,
		SizeBytes:         size,
		LogicalBytes:      logicalBytes,
		Status:            model.BackupArtifactStatusActive,
		Billable:          backend.Billable,
		BillingClass:      backupBillingClass(backend),
		Manifest:          manifest,
	}, run.LeaseOwner)
	if err != nil {
		return nil, err
	}
	return []model.BackupArtifact{artifact}, nil
}

func (s *Server) backupObjectBackendForRun(run model.BackupRun, tenantID string, platformAdmin bool) (model.BackupBackend, *dataObjectBackend, error) {
	backend, err := s.store.GetBackupBackendForUse(run.BackendID, tenantID, platformAdmin)
	if err != nil {
		return model.BackupBackend{}, nil, err
	}
	objectBackend, err := newDataObjectBackend(model.BackupBackendAsDataBackend(backend))
	if err != nil {
		return model.BackupBackend{}, nil, err
	}
	return backend, objectBackend, nil
}

func backupVersionLabel(raw string) string {
	version := strings.TrimSpace(raw)
	if version == "" {
		version = "v" + time.Now().UTC().Format("20060102-150405")
	}
	return version
}

func appPersistentStorageBackupSpec(app model.App) (*model.AppPersistentStorageSpec, string) {
	if app.Spec.PersistentStorage != nil {
		storage := *app.Spec.PersistentStorage
		return &storage, "persistent_storage"
	}
	if app.Spec.Workspace != nil {
		workspace := *app.Spec.Workspace
		mountPath, err := model.NormalizeAppWorkspaceMountPath(workspace.MountPath)
		if err != nil {
			mountPath = model.DefaultAppWorkspaceMountPath
		}
		storage := model.AppPersistentStorageSpec{
			Mode:             model.AppPersistentStorageModeMovableRWO,
			StoragePath:      workspace.StoragePath,
			StorageSize:      workspace.StorageSize,
			StorageClassName: workspace.StorageClassName,
			ResetToken:       workspace.ResetToken,
			Mounts: []model.AppPersistentStorageMount{{
				Kind: model.AppPersistentStorageMountKindDirectory,
				Path: mountPath,
				Mode: 0o755,
			}},
		}
		return &storage, "workspace"
	}
	return nil, ""
}

func persistentStorageBackupSourceRoot(app model.App) (string, bool) {
	root := strings.TrimSpace(os.Getenv("FUGUE_BACKUP_PERSISTENT_STORAGE_ROOT"))
	if root == "" {
		return "", false
	}
	replacements := map[string]string{
		"{tenant_id}":  app.TenantID,
		"{project_id}": app.ProjectID,
		"{app_id}":     app.ID,
		"{app_name}":   app.Name,
	}
	for key, value := range replacements {
		root = strings.ReplaceAll(root, key, value)
	}
	return filepath.Clean(root), true
}

func writeFileArchive(sourceRoot, archivePath string) ([]model.BackupManifestFile, int64, error) {
	sourceRoot = filepath.Clean(sourceRoot)
	out, err := os.Create(archivePath)
	if err != nil {
		return nil, 0, err
	}
	defer out.Close()
	gw := gzip.NewWriter(out)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()
	var files []model.BackupManifestFile
	var logicalBytes int64
	err = filepath.WalkDir(sourceRoot, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if current == sourceRoot {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(sourceRoot, current)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." || rel == "" {
			return nil
		}
		linkTarget := ""
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, _ = os.Readlink(current)
		}
		header, err := tar.FileInfoHeader(info, linkTarget)
		if err != nil {
			return err
		}
		header.Name = rel
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		manifestFile := model.BackupManifestFile{
			Path:  rel,
			Size:  info.Size(),
			MTime: info.ModTime().UTC(),
		}
		switch {
		case entry.Type().IsDir():
			manifestFile.Kind = "directory"
		case info.Mode()&os.ModeSymlink != 0:
			manifestFile.Kind = "symlink"
		case info.Mode().IsRegular():
			manifestFile.Kind = "file"
			file, err := os.Open(current)
			if err != nil {
				return err
			}
			hasher := sha256.New()
			written, copyErr := io.Copy(io.MultiWriter(tw, hasher), file)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			if written != info.Size() {
				return fmt.Errorf("archive file %s wrote %d bytes, expected %d", rel, written, info.Size())
			}
			logicalBytes += written
			manifestFile.SHA256 = hex.EncodeToString(hasher.Sum(nil))
		default:
			manifestFile.Kind = "special"
		}
		files = append(files, manifestFile)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return files, logicalBytes, nil
}

func (s *Server) resolveDataWorkspaceBackupSnapshot(workspace model.DataWorkspace, run model.BackupRun) (model.DataSnapshot, error) {
	version := strings.TrimSpace(run.Version)
	if version != "" {
		if snapshot, err := s.store.GetDataSnapshot(workspace.ID, version); err == nil {
			return snapshot, nil
		}
	}
	latest, err := s.store.LatestDataSnapshot(workspace.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return s.store.CreateDataSnapshot(model.DataSnapshot{
				WorkspaceID: workspace.ID,
				Version:     firstNonEmptyString(version, backupVersionLabel("")),
				Message:     "created by Fugue backup policy because no data workspace snapshot existed",
				Manifest: model.DataManifest{
					WorkspaceID: workspace.ID,
					Entries:     []model.DataManifestEntry{},
				},
				CreatedBy: "backup-worker",
			})
		}
		return model.DataSnapshot{}, err
	}
	if version == "" || version == latest.Version {
		return latest, nil
	}
	return s.store.CreateDataSnapshot(model.DataSnapshot{
		WorkspaceID: workspace.ID,
		Version:     version,
		Message:     "created by Fugue backup policy from latest data workspace snapshot " + latest.ID,
		Manifest:    latest.Manifest,
		CreatedBy:   "backup-worker",
	})
}

func dataWorkspaceSnapshotCreationMode(run model.BackupRun, snapshot model.DataSnapshot) string {
	if strings.TrimSpace(run.Version) == "" || strings.TrimSpace(run.Version) == snapshot.Version {
		return "referenced-existing-snapshot"
	}
	return "created-versioned-snapshot-reference"
}

func detectCSIVolumeSnapshotSupport() bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv("FUGUE_BACKUP_CSI_VOLUME_SNAPSHOT_SUPPORTED")))
	return value == "1" || value == "true" || value == "yes"
}

func storageSupportsPVCClone(storage model.AppPersistentStorageSpec) bool {
	mode := strings.TrimSpace(storage.Mode)
	return mode == model.AppPersistentStorageModeDedicatedPVC || mode == model.AppPersistentStorageModeMovableRWO
}

func boolStringFromBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func (s *Server) registryIsExternalized() bool {
	for _, value := range []string{s.registryPushBase, s.registryPullBase, s.clusterJoinRegistryEndpoint} {
		if looksBundledRegistryEndpoint(value) {
			return false
		}
	}
	return strings.TrimSpace(s.registryPushBase) != "" || strings.TrimSpace(s.registryPullBase) != "" || strings.TrimSpace(s.clusterJoinRegistryEndpoint) != ""
}

func looksBundledRegistryEndpoint(raw string) bool {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "" {
		return false
	}
	return strings.Contains(value, "svc.cluster.local") ||
		strings.Contains(value, "fugue-registry") ||
		strings.Contains(value, "registry.fugue.internal") ||
		strings.HasPrefix(value, "127.") ||
		strings.HasPrefix(value, "localhost") ||
		strings.HasPrefix(value, "10.") ||
		strings.HasPrefix(value, "100.64.")
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
			CNPGBackupIntegrated: s.controlPlaneCNPGBackupIntegrated(),
			RestoreDrillStatus:   "plan-available",
		})
	}
	out = append(out, s.registryBackupPosture())
	out = append(out, s.platformComponentBackupPosture("headscale"))
	out = append(out, s.platformComponentBackupPosture("dns"))
	out = append(out, s.platformComponentBackupPosture("edge"))
	return out
}

func (s *Server) createProtectiveBackupRunForRestore(principal model.Principal, plan model.BackupRestorePlan) (model.BackupRun, error) {
	artifact, err := s.store.GetBackupArtifact(plan.ArtifactID, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return model.BackupRun{}, err
	}
	return s.store.CreateBackupRun(model.BackupRun{
		TenantID:        plan.TenantID,
		ProjectID:       plan.ProjectID,
		AppID:           plan.AppID,
		Target:          plan.Target,
		BackendID:       artifact.BackendID,
		Trigger:         "pre-restore-protective",
		Version:         "protective-" + time.Now().UTC().Format("20060102-150405"),
		Status:          model.BackupRunStatusPending,
		RequestedByType: principal.ActorType,
		RequestedByID:   principal.ActorID,
	})
}

func annotateProtectiveBackupPhase(phases []model.BackupRestorePhase, runID string) []model.BackupRestorePhase {
	if len(phases) == 0 {
		phases = []model.BackupRestorePhase{{Name: "protective-backup", Status: model.BackupRestoreStatusPlanned}}
	}
	found := false
	for idx := range phases {
		if phases[idx].Name == "protective-backup" {
			phases[idx].Message = strings.TrimSpace(phases[idx].Message + " queued protective backup run " + runID)
			found = true
			break
		}
	}
	if !found {
		phases = append([]model.BackupRestorePhase{{
			Name:    "protective-backup",
			Status:  model.BackupRestoreStatusPlanned,
			Message: "queued protective backup run " + runID,
		}}, phases...)
	}
	return phases
}

func (s *Server) controlPlaneCNPGBackupIntegrated() bool {
	return s.controlPlaneCNPGBackupEnabled || strings.TrimSpace(s.controlPlaneCNPGBackupName) != ""
}

func (s *Server) controlPlaneCNPGBackupIntegrationLabel() string {
	if s.controlPlaneCNPGBackupIntegrated() {
		return "cnpg-scheduled-backup:" + strings.TrimSpace(s.controlPlaneCNPGBackupName)
	}
	return "logical-pg-dump"
}

func (s *Server) registryBackupPosture() model.BackupPosture {
	externalized := s.registryIsExternalized()
	status := "blocked"
	message := "registry endpoint is not configured"
	if strings.TrimSpace(s.registryPushBase) != "" || strings.TrimSpace(s.registryPullBase) != "" || strings.TrimSpace(s.clusterJoinRegistryEndpoint) != "" {
		if externalized {
			status = "externalized"
			message = "registry endpoints are externalized; Fugue records endpoint health and relies on provider registry backup"
		} else {
			status = "needs-backup"
			message = "bundled registry requires PVC/object backup coordinated with registry GC"
		}
	}
	return model.BackupPosture{
		Target: model.BackupTarget{
			Type:      model.BackupTargetRegistry,
			Component: "registry",
			Name:      firstNonEmptyString(s.registryPullBase, s.registryPushBase, s.clusterJoinRegistryEndpoint),
		},
		Status:             status,
		Message:            message,
		Externalized:       externalized,
		ExternallyBackedUp: externalized,
	}
}

func (s *Server) platformComponentBackupPosture(component string) model.BackupPosture {
	component = strings.TrimSpace(strings.ToLower(component))
	posture := model.BackupPosture{
		Target: model.BackupTarget{
			Type:      model.BackupTargetPlatformComponent,
			Component: component,
			Name:      component,
		},
		Status:  "recorded-in-control-plane",
		Message: "authoritative state is backed up with the control-plane database",
	}
	switch component {
	case "headscale":
		provider := strings.TrimSpace(strings.ToLower(s.clusterJoinMeshProvider))
		loginServer := strings.TrimSpace(s.clusterJoinMeshLoginServer)
		if provider == "" {
			posture.Status = "disabled"
			posture.Message = "mesh provider is not configured"
			return posture
		}
		posture.Target.Name = firstNonEmptyString(loginServer, provider)
		posture.Externalized = provider != "headscale" || looksExternalPlatformEndpoint(loginServer)
		posture.ExternallyBackedUp = posture.Externalized
		if posture.Externalized {
			posture.Status = "externalized"
			posture.Message = "mesh control state is externalized; provider backup is authoritative"
		} else {
			posture.Status = "needs-backup"
			posture.Message = "bundled headscale uses persistent state; back up the headscale PVC or externalize it"
		}
	case "dns":
		nodes, err := s.store.ListDNSNodes("")
		if err != nil {
			posture.Status = "unknown"
			posture.Message = "dns inventory unavailable: " + err.Error()
			return posture
		}
		posture.Message = fmt.Sprintf("control-plane DNS records and ACME challenges are backed up; %d DNS node caches are reconstructable from signed bundles", len(nodes))
	case "edge":
		nodes, _, err := s.store.ListEdgeNodes("")
		if err != nil {
			posture.Status = "unknown"
			posture.Message = "edge inventory unavailable: " + err.Error()
			return posture
		}
		posture.Message = fmt.Sprintf("route policy and node inventory are backed up; %d edge node caches are reconstructable, but node-local Caddy data should be externalized or backed up", len(nodes))
	}
	return posture
}

func looksExternalPlatformEndpoint(raw string) bool {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "" {
		return false
	}
	return !strings.Contains(value, "svc.cluster.local") &&
		!strings.Contains(value, ".svc") &&
		!strings.Contains(value, "localhost") &&
		!strings.HasPrefix(value, "127.") &&
		!strings.HasPrefix(value, "10.") &&
		!strings.HasPrefix(value, "100.64.")
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

func (s *Server) persistentStoragePostureMessage(app model.App) (string, bool) {
	storage, kind := appPersistentStorageBackupSpec(app)
	if storage == nil {
		return "persistent storage is not configured", true
	}
	runtimeID := strings.TrimSpace(app.Spec.RuntimeID)
	runtimeType := ""
	if runtimeID != "" {
		if runtime, err := s.store.GetRuntime(runtimeID); err == nil {
			runtimeType = runtime.Type
		}
	}
	if runtimeType != "" && !model.RuntimeSupportsPersistentWorkspace(runtimeType) {
		return fmt.Sprintf("%s backup blocked: runtime %s does not support persistent workspaces", kind, runtimeType), true
	}
	strategy := "file-archive"
	if detectCSIVolumeSnapshotSupport() {
		strategy = "csi-snapshot"
	} else if storageSupportsPVCClone(*storage) {
		strategy = "pvc-clone-or-file-archive"
	}
	workerRoot := "not-mounted"
	if _, ok := persistentStorageBackupSourceRoot(app); ok {
		workerRoot = "mounted"
	}
	return fmt.Sprintf("%s backup strategy=%s csi_snapshot=%t pvc_clone=%t file_worker=%s", kind, strategy, detectCSIVolumeSnapshotSupport(), storageSupportsPVCClone(*storage), workerRoot), false
}

func (s *Server) appBackupPosture(app model.App, policies []model.BackupPolicy, artifacts []model.BackupArtifact) []model.BackupPosture {
	targets := []model.BackupTarget{
		{Type: model.BackupTargetAppDatabase, TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, Name: app.Name},
		{Type: model.BackupTargetPersistentStorage, TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, Name: app.Name},
	}
	out := make([]model.BackupPosture, 0, len(targets))
	for _, target := range targets {
		posture := model.BackupPosture{Target: target, Status: "disabled", Message: "backup is disabled by default"}
		if policy, ok := preferredBackupPolicyForTarget(policies, target.Type); ok {
			posture.PolicyID = policy.ID
			posture.Status = policy.Status
			posture.Message = policy.DisabledReason
			if !policy.Enabled {
				posture.Status = "disabled"
			}
			if policy.Enabled && policy.Status == model.BackupPolicyStatusActive {
				posture.Message = ""
			}
			posture.LastSuccessfulRunID = policy.LastSuccessfulRunID
			posture.LastSuccessfulAt = policy.LastSuccessfulAt
		}
		for _, artifact := range artifacts {
			if artifact.Target.Type == target.Type {
				posture.BillableBytes += artifact.SizeBytes
			}
		}
		if target.Type == model.BackupTargetPersistentStorage {
			storageMessage, blocked := s.persistentStoragePostureMessage(app)
			posture.Message = firstNonEmptyString(posture.Message, storageMessage)
			if blocked && posture.PolicyID != "" && posture.Status == model.BackupPolicyStatusActive {
				posture.Status = "blocked"
				posture.Message = storageMessage
			}
		}
		out = append(out, posture)
	}
	return out
}

func preferredBackupPolicyForTarget(policies []model.BackupPolicy, targetType string) (model.BackupPolicy, bool) {
	filtered := make([]model.BackupPolicy, 0, len(policies))
	for _, policy := range policies {
		if policy.Target.Type == targetType {
			filtered = append(filtered, policy)
		}
	}
	return preferredBackupPolicy(filtered)
}

func preferredBackupPolicy(policies []model.BackupPolicy) (model.BackupPolicy, bool) {
	var selected model.BackupPolicy
	selectedPriority := -1
	for _, policy := range policies {
		priority := backupPolicyPosturePriority(policy)
		if priority > selectedPriority || priority == selectedPriority && backupPolicyPreferredOnTie(policy, selected) {
			selected = policy
			selectedPriority = priority
		}
	}
	return selected, selectedPriority >= 0
}

func backupPolicyPreferredOnTie(candidate, current model.BackupPolicy) bool {
	if candidate.LastSuccessfulAt != nil || current.LastSuccessfulAt != nil {
		if candidate.LastSuccessfulAt == nil {
			return false
		}
		if current.LastSuccessfulAt == nil {
			return true
		}
		if !candidate.LastSuccessfulAt.Equal(*current.LastSuccessfulAt) {
			return candidate.LastSuccessfulAt.After(*current.LastSuccessfulAt)
		}
	}
	if !candidate.UpdatedAt.Equal(current.UpdatedAt) {
		return candidate.UpdatedAt.After(current.UpdatedAt)
	}
	return candidate.ID < current.ID
}

func backupPolicyPosturePriority(policy model.BackupPolicy) int {
	if !policy.Enabled || policy.Status == model.BackupPolicyStatusDisabled {
		return 0
	}
	switch policy.Status {
	case model.BackupPolicyStatusActive:
		return 3
	case model.BackupPolicyStatusBlockedNoBackend, model.BackupPolicyStatusError:
		return 2
	default:
		return 1
	}
}
