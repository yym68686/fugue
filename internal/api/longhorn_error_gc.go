package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
)

// Only failed metadata is collected here. Longhorn's finalizer retains control
// of deletion; Fugue never removes finalizers or repository shared blocks.
func (s *Server) sweepLonghornErrorBackups(parent context.Context) {
	if s == nil || s.store == nil || !longhornBackupEnabled() {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, backupLonghornErrorGCTimeout)
	defer cancel()
	_, err := s.store.WithAdvisoryLock(ctx, backupLonghornErrorGCLock, func() error {
		client, err := newKubeLogsClient("")
		if err != nil {
			return err
		}
		defer client.closeIdleConnections()
		return s.reconcileLonghornErrorBackups(ctx, client, time.Now().UTC())
	})
	if err != nil && s.log != nil && !errors.Is(err, context.Canceled) {
		s.log.Printf("Longhorn Error backup GC sweep failed: %v", err)
	}
}

func (s *Server) reconcileLonghornErrorBackups(ctx context.Context, client *kubeLogsClient, now time.Time) error {
	namespace := firstNonEmptyString(getenv(longhornBackupNamespaceEnv), "longhorn-system")
	if !longhornResourceName.MatchString(namespace) {
		return fmt.Errorf("invalid Longhorn namespace %q", namespace)
	}
	query := url.Values{"limit": {"500"}, "labelSelector": {"fugue.pro/backup-run"}}
	requested := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var list struct {
			Items    []map[string]any `json:"items"`
			Metadata struct {
				Continue string `json:"continue"`
			} `json:"metadata"`
		}
		apiPath := "/apis/longhorn.io/v1beta2/namespaces/" + url.PathEscape(namespace) + "/backups?" + query.Encode()
		if err := client.doJSON(ctx, http.MethodGet, apiPath, &list); err != nil {
			return err
		}
		for _, backup := range list.Items {
			if !longhornErrorBackupEligible(backup, now, backupLonghornErrorGCGrace) {
				continue
			}
			metadata, _ := backup["metadata"].(map[string]any)
			labels, _ := metadata["labels"].(map[string]any)
			runID := longhornStringValue(labels["fugue.pro/backup-run"])
			run, err := s.store.GetBackupRun(runID, "", true)
			if err != nil || run.Status != model.BackupRunStatusFailed || !backupRunHasLonghornLockConflict(run) ||
				run.FinishedAt == nil || run.FinishedAt.After(now.Add(-backupLonghornErrorGCGrace)) {
				continue
			}
			if exists, err := s.store.BackupArtifactExistsForRun(runID); err != nil || exists {
				continue
			}
			if s.longhornErrorBackupHasActiveNewerRun(run) {
				continue
			}
			name := longhornObjectName(backup)
			if err := deleteLonghornErrorBackup(ctx, client, namespace, name, backup, runID, now, backupLonghornErrorGCGrace); err != nil {
				if s.log != nil {
					s.log.Printf("Longhorn Error backup GC deferred run=%s backup=%s: %v", runID, name, err)
				}
				continue
			}
			if s.log != nil {
				s.log.Printf("Longhorn Error backup GC deletion requested run=%s backup=%s uid=%s", runID, name, longhornStringValue(metadata["uid"]))
			}
			requested++
			if requested >= backupLonghornErrorGCLimit {
				return nil
			}
		}
		if list.Metadata.Continue == "" {
			return nil
		}
		if list.Metadata.Continue == query.Get("continue") {
			return errors.New("Longhorn Error backup GC inventory repeated continuation token")
		}
		query.Set("continue", list.Metadata.Continue)
	}
}

func (s *Server) longhornErrorBackupHasActiveNewerRun(run model.BackupRun) bool {
	if run.PolicyID == "" {
		return false
	}
	policy, err := s.store.GetBackupPolicy(run.PolicyID, "", true)
	if err != nil {
		return true
	}
	if policy.LastRunID == "" || policy.LastRunID == run.ID {
		return false
	}
	newer, err := s.store.GetBackupRun(policy.LastRunID, "", true)
	if err != nil {
		return true
	}
	return newer.Status == model.BackupRunStatusPending || newer.Status == model.BackupRunStatusRunning
}

func longhornErrorBackupEligible(backup map[string]any, now time.Time, grace time.Duration) bool {
	metadata, _ := backup["metadata"].(map[string]any)
	labels, _ := metadata["labels"].(map[string]any)
	if longhornStringValue(labels["fugue.pro/backup-run"]) == "" || longhornStringValue(metadata["deletionTimestamp"]) != "" {
		return false
	}
	status, _ := backup["status"].(map[string]any)
	if !strings.EqualFold(longhornStringValue(status["state"]), "Error") || !isLonghornBackupLockConflictMessage(longhornStringValue(status["error"])) {
		return false
	}
	for _, key := range []string{"url", "volumeName", "backupCreatedAt", "size", "newlyUploadDataSize", "reUploadedDataSize"} {
		if value, exists := status[key]; exists && value != "" && value != "0" && value != float64(0) && value != int(0) {
			return false
		}
	}
	if value, exists := status["progress"]; exists && value != float64(0) && value != int(0) {
		return false
	}
	createdAt, err := time.Parse(time.RFC3339Nano, longhornStringValue(metadata["creationTimestamp"]))
	return err == nil && !createdAt.IsZero() && !createdAt.After(now.Add(-grace))
}
