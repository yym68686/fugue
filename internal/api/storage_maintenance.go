package api

import (
	"context"
	"errors"
	"fmt"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
	"net/http"
	"time"
)

func (s *Server) handleListStorageMaintenanceJobs(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	jobs, err := s.store.ListStorageMaintenanceJobs()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, 200, map[string]any{"jobs": jobs})
}
func (s *Server) handleGetStorageMaintenanceJob(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	job, err := s.store.GetStorageMaintenanceJob(r.PathValue("job_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, 200, job)
}
func (s *Server) handleCreateStorageMaintenanceJob(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() {
		httpx.WriteError(w, 403, "platform administrator required")
		return
	}
	var req struct {
		Kind    string `json:"kind"`
		ScopeID string `json:"scope_id"`
		Execute bool   `json:"execute"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, 400, err.Error())
		return
	}
	switch req.Kind {
	case "retention":
		if req.ScopeID != "" {
			if _, err := s.store.GetBackupPolicy(req.ScopeID, "", true); err != nil {
				s.writeStoreError(w, err)
				return
			}
		}
	case "longhorn-reconcile", "backup-cleanup", "transfer-expiry":
		if req.ScopeID != "" {
			httpx.WriteError(w, 400, "this job kind has no scope")
			return
		}
	case "data-gc":
		if _, err := s.store.GetDataWorkspace(req.ScopeID, "", true); err != nil {
			s.writeStoreError(w, err)
			return
		}
	default:
		httpx.WriteError(w, 400, "unknown maintenance kind")
		return
	}
	job, err := s.store.CreateStorageMaintenanceJob(model.StorageMaintenanceJob{Kind: req.Kind, ScopeID: req.ScopeID, Execute: req.Execute})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(p, "storage.maintenance.create", "storage_job", job.ID, "", map[string]string{"kind": req.Kind, "scope_id": req.ScopeID, "execute": fmt.Sprint(req.Execute)})
	httpx.WriteJSON(w, 202, job)
}

// Each pass is bounded and serialized across replicas. Running jobs survive a
// restart; retry_at is a worker lease and is separate from progress evidence.
func (s *Server) sweepStorageMaintenance(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	_, err := s.store.WithAdvisoryLock(ctx, "fugue-storage-maintenance-v1", func() error {
		jobs, err := s.store.ListStorageMaintenanceJobs()
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		for _, kind := range []string{"retention", "longhorn-reconcile", "transfer-expiry"} {
			recent := false
			for _, j := range jobs {
				if j.Kind == kind && j.ScopeID == "" && (j.Execute && now.Sub(j.CreatedAt) < 15*time.Minute || j.Status == "queued" || j.Status == "running" || j.Status == "retrying") {
					recent = true
					break
				}
			}
			if !recent {
				j, e := s.store.CreateStorageMaintenanceJob(model.StorageMaintenanceJob{Kind: kind, Execute: true})
				if e != nil {
					return e
				}
				jobs = append(jobs, j)
			}
		}
		workspaces, e := s.store.ListDataWorkspaces("", "", true)
		if e != nil {
			return e
		}
		namespaces := map[string]bool{}
		workspaceNamespaces := map[string]string{}
		for _, w := range workspaces {
			b, e := s.store.GetDataBackend(w.StorageBackendID, "", true)
			if e != nil {
				return e
			}
			workspaceNamespaces[w.ID] = dataPhysicalBucket(b) + "/" + b.Prefix
		}
		for _, w := range workspaces {
			key := workspaceNamespaces[w.ID]
			if namespaces[key] {
				continue
			}
			namespaces[key] = true
			recent := false
			for _, j := range jobs {
				if j.Kind == "data-gc" && workspaceNamespaces[j.ScopeID] == key && (j.Execute && now.Sub(j.CreatedAt) < 24*time.Hour || j.Status == "queued" || j.Status == "running" || j.Status == "retrying") {
					recent = true
					break
				}
			}
			if !recent {
				j, e := s.store.CreateStorageMaintenanceJob(model.StorageMaintenanceJob{Kind: "data-gc", ScopeID: w.ID, Execute: true})
				if e != nil {
					return e
				}
				jobs = append(jobs, j)
			}
		}
		for _, job := range jobs {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if job.Status != "queued" && job.Status != "running" && job.Status != "retrying" {
				continue
			}
			if job.RetryAt != nil && job.RetryAt.After(now) {
				continue
			}
			job.Status = "running"
			job.Attempts++
			lease := time.Now().UTC().Add(5 * time.Minute)
			job.RetryAt = &lease
			if err := s.store.SaveStorageMaintenanceJob(job, job.UpdatedAt); err != nil {
				return err
			}
			job, err = s.store.GetStorageMaintenanceJob(job.ID)
			if err != nil {
				return err
			}
			result, runErr := s.runStorageMaintenance(ctx, job)
			job.Result = result
			job.Error = ""
			job.RetryAt = nil
			job.Status = "succeeded"
			if runErr != nil {
				job.Error = runErr.Error()
				job.Status = "retrying"
				retry := time.Now().UTC().Add(time.Duration(min(job.Attempts, 5)) * time.Minute)
				job.RetryAt = &retry
				if job.Attempts >= 5 && !errors.Is(runErr, errLonghornCleanupPending) {
					job.Status = "failed"
					job.RetryAt = nil
				}
			}
			if err := s.store.SaveStorageMaintenanceJob(job, job.UpdatedAt); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil && s.log != nil && !errors.Is(err, context.Canceled) {
		s.log.Printf("storage maintenance: %v", err)
	}
}

func (s *Server) runStorageMaintenance(ctx context.Context, job model.StorageMaintenanceJob) (map[string]any, error) {
	switch job.Kind {
	case "retention":
		if !job.Execute {
			return s.previewBackupRetention(job.ScopeID)
		}
		if job.ScopeID != "" {
			return map[string]any{"policy_id": job.ScopeID}, s.store.ReconcileBackupRetention(job.ScopeID)
		}
		return map[string]any{"enabled_policies_only": true}, s.store.ReconcileEnabledBackupRetention()
	case "longhorn-reconcile":
		return s.reconcileCompletedLonghornBackups(ctx, job.Execute)
	case "backup-cleanup":
		if job.Execute {
			s.sweepDeletedBackupArtifacts(ctx)
			s.sweepFailedBackupRunObjects(ctx)
			s.sweepLonghornErrorBackups(ctx)
		}
		candidates, err := s.store.ListBackupArtifactCleanupCandidates(store.BackupArtifactCleanupFilter{Before: time.Now().UTC().Add(-backupArtifactGCGrace), Limit: 501})
		if err == nil && job.Execute && len(candidates) > 0 {
			err = errLonghornCleanupPending
		}
		return map[string]any{"remaining_candidates": len(candidates), "truncated": len(candidates) == 501, "physical_reclamation": "engine-owned; inspect artifact receipts and repository measurements"}, err
	case "data-gc":
		w, err := s.store.GetDataWorkspace(job.ScopeID, "", true)
		if err != nil {
			return nil, err
		}
		gc, err := s.sweepDataNamespaceGC(ctx, w, 7, !job.Execute)
		return map[string]any{"gc": gc}, err
	case "transfer-expiry":
		return s.retireIdleDataTransfers(ctx, job.Execute)
	}
	return nil, store.ErrInvalidInput
}

func (s *Server) previewBackupRetention(scope string) (map[string]any, error) {
	policies, err := s.store.ListBackupPolicies(store.BackupPolicyFilter{PlatformAdmin: true, IncludeDisabled: scope != "", Limit: 10001})
	if err != nil {
		return nil, err
	}
	result := []map[string]any{}
	for _, p := range policies {
		if scope != "" && p.ID != scope {
			continue
		}
		candidates, e := s.store.PreviewBackupRetention(p.ID)
		if e != nil {
			return nil, e
		}
		result = append(result, map[string]any{"policy_id": p.ID, "enabled": p.Enabled, "retention": p.Retention, "retain_count": p.RetainCount, "candidate_artifact_ids": candidates})
	}
	return map[string]any{"policies": result, "preview": true, "protected_and_restoring": "excluded", "manual_history": "kept until explicit retirement"}, nil
}
