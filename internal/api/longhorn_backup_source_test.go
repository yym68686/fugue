package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestLonghornSourceSurvivesEveryCreateWindow(t *testing.T) {
	for _, mode := range []string{"fresh", "snapshot-only", "completed-remote", "error-remote-gone-source", "pending-remote", "pending-wrong-volume", "duplicate", "wrong-volume", "deleting", "truncated", "lost-create-response"} {
		t.Run(mode, func(t *testing.T) {
			posts, deletes := 0, 0
			snapshot := map[string]any{"metadata": map[string]any{"name": "point", "uid": "point-uid", "labels": map[string]any{"fugue.pro/backup-run": "run"}}, "spec": map[string]any{"volume": "volume"}}
			if mode == "wrong-volume" || mode == "pending-wrong-volume" {
				snapshot["spec"].(map[string]any)["volume"] = "other"
			}
			if mode == "deleting" {
				snapshot["metadata"].(map[string]any)["deletionTimestamp"] = "now"
			}
			created := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/snapshots/point") {
					if mode == "completed-remote" || mode == "error-remote-gone-source" {
						t.Error("terminal remote status must not require its old local source")
					}
					_ = json.NewEncoder(w).Encode(snapshot)
					return
				}
				if r.Method == http.MethodDelete {
					deletes++
					t.Error("source acquisition must not delete anything")
				}
				if r.Method == http.MethodPost {
					posts++
					var body map[string]any
					_ = json.NewDecoder(r.Body).Decode(&body)
					if longhornObjectName(body) != longhornRunObjectName("run") {
						t.Error("create must use deterministic run identity")
					}
					created = true
					if mode == "lost-create-response" {
						http.Error(w, "response lost after commit", 500)
						return
					}
					_ = json.NewEncoder(w).Encode(snapshot)
					return
				}
				items := []any{}
				meta := map[string]any{}
				if strings.HasSuffix(r.URL.Path, "/backups") && (mode == "completed-remote" || strings.HasPrefix(mode, "pending-") || mode == "error-remote-gone-source") {
					state, remoteVolume := "Completed", "volume"
					if strings.HasPrefix(mode, "pending-") {
						state, remoteVolume = "InProgress", ""
					}
					if mode == "error-remote-gone-source" {
						state, remoteVolume = "Error", ""
					}
					items = append(items, map[string]any{"metadata": map[string]any{"name": "remote", "uid": "remote-uid", "labels": map[string]any{"fugue.pro/backup-run": "run"}},
						"spec": map[string]any{"snapshotName": "point"}, "status": map[string]any{"state": state, "snapshotName": "point", "volumeName": remoteVolume}})
				}
				if strings.HasSuffix(r.URL.Path, "/snapshots") && (mode != "fresh" && mode != "lost-create-response" || created) {
					items = append(items, snapshot)
					if mode == "duplicate" {
						items = append(items, snapshot)
					}
					if mode == "truncated" {
						meta["continue"] = "more"
					}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"metadata": meta, "items": items})
			}))
			defer server.Close()
			client := &kubeLogsClient{baseURL: server.URL, client: server.Client()}
			point, remote, err := ensureLonghornBackupSource(context.Background(), client, "storage", "run", "volume")
			bad := mode == "duplicate" || mode == "wrong-volume" || mode == "pending-wrong-volume" || mode == "deleting" || mode == "truncated" || mode == "lost-create-response"
			if (err != nil) != bad {
				t.Fatalf("unexpected result: %s %s %v", point, remote, err)
			}
			if mode == "lost-create-response" {
				point, _, err = ensureLonghornBackupSource(context.Background(), client, "storage", "run", "volume")
				if err != nil || point != "point" || posts != 1 {
					t.Fatalf("restart duplicated accepted create: point=%s posts=%d err=%v", point, posts, err)
				}
			}
			if mode == "completed-remote" && (point != "point" || remote != "remote") {
				t.Fatal("must reuse remote even without a local snapshot")
			}
			if mode != "fresh" && mode != "lost-create-response" && posts != 0 || deletes != 0 {
				t.Fatal("resume unexpectedly mutated storage")
			}
		})
	}
}

func TestLonghornTenantBackupSharesCleanupFence(t *testing.T) {
	for _, engine := range []string{model.BackupEngineLonghornSnapshot, "logical-pgdump"} {
		run := model.BackupRun{Target: model.BackupTarget{Type: model.BackupTargetAppDatabase, Engine: engine}, Status: model.BackupRunStatusPending}
		want := engine == model.BackupEngineLonghornSnapshot
		if backupRunNeedsCoordinationLease(run) != want || backupRunWaitingForCoordinationLease(run) != want {
			t.Fatal("tenant Longhorn backups must coordinate with cleanup without changing logical backups")
		}
	}
}
