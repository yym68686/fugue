package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestLonghornCleanupWaitsForEngineAndRejectsForeignBackups(t *testing.T) {
	for _, scenario := range []string{"completed", "deleting", "absent", "foreign", "protected", "wrong-repository", "missing-finalizer", "provider-error"} {
		t.Run(scenario, func(t *testing.T) {
			targetURL := "s3://test-bucket@auto/backups/engine"
			backupURL := targetURL + "?backup=backup-one&volume=volume-one"
			a := model.BackupArtifact{RunID: "run-one", Kind: model.BackupArtifactKindLonghornSnapshot, Status: model.BackupArtifactStatusDeleted, Manifest: model.BackupManifest{Metadata: map[string]string{
				"longhorn_backup_name": "backup-one", "volume_name": "volume-one", "backup_target_name": "target-one", "backup_target_url": targetURL, "longhorn_backup_url": backupURL,
			}}}
			if scenario == "protected" {
				a.Protected = true
			}
			if scenario == "wrong-repository" {
				a.Manifest.Metadata["backup_target_url"] = "s3://other-bucket@auto/backups/engine"
			}
			deletes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "backuptargets") {
					json.NewEncoder(w).Encode(map[string]any{"spec": map[string]string{"backupTargetURL": targetURL}})
					return
				}
				if scenario == "absent" {
					w.WriteHeader(404)
					return
				}
				if scenario == "provider-error" {
					w.WriteHeader(403)
					return
				}
				if r.Method == "DELETE" {
					deletes++
					var options struct {
						Preconditions map[string]string `json:"preconditions"`
					}
					json.NewDecoder(r.Body).Decode(&options)
					if options.Preconditions["uid"] != "uid-one" || options.Preconditions["resourceVersion"] != "7" {
						t.Error("missing exact deletion preconditions")
					}
					json.NewEncoder(w).Encode(map[string]string{"status": "Success"})
					return
				}
				metadata := map[string]any{"name": "backup-one", "uid": "uid-one", "resourceVersion": "7", "finalizers": []string{"longhorn.io"}, "labels": map[string]string{"fugue.pro/backup-run": "run-one"}}
				if scenario == "foreign" {
					metadata["labels"] = map[string]string{"fugue.pro/backup-run": "other-run"}
				}
				if scenario == "deleting" {
					metadata["deletionTimestamp"] = "2026-01-01T00:00:00Z"
				}
				if scenario == "missing-finalizer" {
					metadata["finalizers"] = []string{}
				}
				json.NewEncoder(w).Encode(map[string]any{"metadata": metadata, "status": map[string]string{"volumeName": "volume-one", "backupTargetName": "target-one", "url": backupURL, "state": "Completed"}})
			}))
			defer server.Close()
			err := cleanupLonghornBackup(context.Background(), &kubeLogsClient{baseURL: server.URL, client: server.Client()}, a, model.DataBackend{Bucket: "test-bucket", Prefix: "backups"})
			if scenario == "absent" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("cleanup completed before engine confirmation")
			}
			if scenario == "completed" || scenario == "deleting" {
				if !errors.Is(err, errLonghornCleanupPending) {
					t.Fatal(err)
				}
			}
			want := 0
			if scenario == "completed" {
				want = 1
			}
			if deletes != want {
				t.Fatalf("DELETE calls %d want %d", deletes, want)
			}
		})
	}
}
