package api

import (
	"context"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"fugue/internal/store"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCompletedLonghornReconcileVerifiesBlocksBeforeAdoption(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			t.Setenv(longhornBackupEngineEnabledEnv, "true")
			created := time.Now().Add(-48 * time.Hour).UTC().Truncate(time.Second)
			id := "backup_run_fixture"
			checksum := strings.Repeat("a", 64)
			backupURL := "s3://bucket@auto/root/longhorn?backup=backup-test&volume=volume-test"
			config := map[string]any{"Name": "backup-test", "VolumeName": "volume-test", "SnapshotName": "snapshot-test", "CreatedTime": created.Format(time.RFC3339), "SnapshotCreatedAt": created.Add(-time.Hour).Format(time.RFC3339), "Size": "2097152", "CompressionMethod": "lz4", "Parameters": map[string]string{"backup-block-size": "2097152"}, "Blocks": []map[string]any{{"Offset": 0, "BlockChecksum": checksum}}}
			data, _ := json.Marshal(config)
			s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "HEAD" && r.Method != "GET" {
					t.Errorf("provider mutation %s", r.Method)
				}
				if r.URL.Query().Get("list-type") == "2" {
					fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated>")
					if !missing {
						fmt.Fprintf(w, "<Contents><Key>%saa/aa/%s.blk</Key><Size>100</Size></Contents>", r.URL.Query().Get("prefix"), checksum)
					}
					fmt.Fprint(w, "</ListBucketResult>")
					return
				}
				if !strings.HasSuffix(r.URL.Path, ".cfg") {
					w.WriteHeader(404)
					return
				}
				w.Header().Set("ETag", `"cfg-version"`)
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				if r.Method == "GET" {
					w.Write(data)
				}
			}))
			defer s3.Close()
			remote := map[string]any{"metadata": map[string]any{"name": "backup-test", "labels": map[string]string{"fugue.pro/backup-run": id}}, "spec": map[string]string{"snapshotName": "snapshot-test"}, "status": map[string]string{"state": "Completed", "url": backupURL, "volumeName": "volume-test", "volumeSize": "4194304", "size": "100", "backupTargetName": "default", "snapshotName": "snapshot-test"}}
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("kubernetes mutation %s", r.Method)
				}
				if strings.Contains(r.URL.Path, "backuptargets") {
					json.NewEncoder(w).Encode(map[string]any{"spec": map[string]string{"backupTargetURL": "s3://bucket@auto/root/longhorn"}, "status": map[string]bool{"available": true}})
					return
				}
				if strings.HasSuffix(r.URL.Path, "/backups") {
					json.NewEncoder(w).Encode(map[string]any{"items": []any{remote}})
					return
				}
				json.NewEncoder(w).Encode(remote)
			}))
			defer kube.Close()
			db := store.New(filepath.Join(t.TempDir(), "state.json"))
			if e := db.Init(); e != nil {
				t.Fatal(e)
			}
			backend, e := db.CreateBackupBackend(model.BackupBackend{Name: "backup", Provider: model.DataBackendProviderCloudflareR2, Bucket: "bucket", Prefix: "root", Endpoint: s3.URL, Credentials: model.DataBackendCredentials{AccessKeyID: "key", SecretAccessKey: "secret"}})
			if e != nil {
				t.Fatal(e)
			}
			run, e := db.CreateBackupRun(model.BackupRun{ID: id, BackendID: backend.ID, Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Engine: model.BackupEngineLonghornSnapshot}, Status: model.BackupRunStatusFailed, FinishedAt: &created, ErrorMessage: "lease expired"})
			if e != nil {
				t.Fatal(e)
			}
			server := NewServer(db, nil, nil, ServerConfig{})
			server.newBackupLonghornClient = func(string) (*kubeLogsClient, error) {
				return &kubeLogsClient{baseURL: kube.URL, client: kube.Client()}, nil
			}
			result, e := server.reconcileCompletedLonghornBackups(context.Background(), false)
			if e != nil {
				t.Fatal(e)
			}
			if exists, _ := db.BackupArtifactExistsForRun(id); exists {
				t.Fatal("preview wrote artifact")
			}
			result, e = server.reconcileCompletedLonghornBackups(context.Background(), true)
			if e != nil {
				t.Fatal(e)
			}
			if result["adopted"].(int) != map[bool]int{true: 0, false: 1}[missing] {
				t.Fatalf("result %+v", result)
			}
			updated, _ := db.GetBackupRun(id, "", true)
			if updated.Status != run.Status || updated.ErrorMessage != run.ErrorMessage {
				t.Fatal("failure history changed")
			}
			if !missing {
				result, e = server.reconcileCompletedLonghornBackups(context.Background(), true)
				if e != nil || result["adopted"].(int) != 0 {
					t.Fatalf("duplicate adoption %+v %v", result, e)
				}
			}
		})
	}
}
