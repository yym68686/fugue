package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestLonghornErrorGCPaginatesAndPreservesArtifactsAndActiveRuns(t *testing.T) {
	now := time.Now().UTC()
	finished := now.Add(-48 * time.Hour)
	message := "failed to acquire lock backupstore/volumes/aa/bb when performing backup create/restore"
	db := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	backend, err := db.CreateBackupBackend(model.BackupBackend{Name: "backup-store", Provider: model.DataBackendProviderCloudflareR2, Bucket: "backup-bucket"})
	if err != nil {
		t.Fatal(err)
	}
	run, err := db.CreateBackupRun(model.BackupRun{Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Engine: model.BackupEngineLonghornSnapshot}, BackendID: backend.ID, Trigger: model.BackupRunTriggerManual, Status: model.BackupRunStatusFailed, ErrorMessage: message, FinishedAt: &finished})
	if err != nil {
		t.Fatal(err)
	}
	object := map[string]any{
		"metadata": map[string]any{"name": "empty-error", "uid": "error-uid", "resourceVersion": "12", "finalizers": []any{"longhorn.io"}, "creationTimestamp": finished.Format(time.RFC3339Nano), "labels": map[string]any{"fugue.pro/backup-run": run.ID}},
		"spec":     map[string]any{"snapshotName": "source-snapshot"},
		"status":   map[string]any{"state": "Error", "error": message},
	}
	deletes, pages := 0, 0
	snapshotPresent := false
	deleted := false
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/backups") {
			pages++
			if r.URL.Query().Get("continue") == "" {
				json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "metadata": map[string]any{"continue": "next-page"}})
				return
			}
			items := []any{}
			if !deleted {
				items = append(items, object)
			}
			json.NewEncoder(w).Encode(map[string]any{"items": items})
			return
		}
		if strings.Contains(r.URL.Path, "/snapshots/") {
			if snapshotPresent {
				json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "source-snapshot"}})
			} else {
				http.NotFound(w, r)
			}
			return
		}
		if r.Method == http.MethodDelete {
			var options struct {
				Preconditions struct{ UID, ResourceVersion string }
			}
			if err := json.NewDecoder(r.Body).Decode(&options); err != nil {
				t.Error(err)
			}
			if options.Preconditions.UID != "error-uid" || options.Preconditions.ResourceVersion != "12" {
				t.Errorf("missing identity fence: %+v", options)
			}
			deletes++
			deleted = true
			w.WriteHeader(http.StatusAccepted)
			return
		}
		json.NewEncoder(w).Encode(object)
	}))
	defer remote.Close()
	s := NewServer(db, auth.New(db, ""), nil, ServerConfig{})
	client := &kubeLogsClient{baseURL: remote.URL, client: remote.Client()}
	reconcile := func() {
		t.Helper()
		if err := s.reconcileLonghornErrorBackups(context.Background(), client, now); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	if deletes != 1 || pages != 2 {
		t.Fatalf("pagination or eligible metadata deletion failed: deletes=%d pages=%d", deletes, pages)
	}
	deleted = false
	snapshotPresent = true
	reconcile()
	if deletes != 1 {
		t.Fatal("source snapshot still present but Error CR was deleted")
	}
	deleted = false
	snapshotPresent = false
	if _, err := db.CreateBackupArtifact(model.BackupArtifact{RunID: run.ID, Target: run.Target, Kind: model.BackupArtifactKindLonghornSnapshot}); err != nil {
		t.Fatal(err)
	}
	reconcile()
	if deletes != 1 {
		t.Fatal("artifact reference did not block Error CR deletion")
	}
	stored, err := db.GetBackupRun(run.ID, "", true)
	if err != nil || stored.Status != model.BackupRunStatusFailed || stored.ErrorMessage != message {
		t.Fatalf("failed audit history changed: %+v %v", stored, err)
	}
}
