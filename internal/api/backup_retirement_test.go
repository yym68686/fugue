package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestRecoveredBackupRetirementAPI(t *testing.T) {
	const repo = "s3://fixture@auto/backups/longhorn"
	remoteURL := func(name string) string { return repo + "?backup=" + name + "&volume=fixture-volume" }
	var missingRemote atomic.Bool
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected Kubernetes write: %s", r.Method)
			w.WriteHeader(500)
			return
		}
		if strings.Contains(r.URL.Path, "/backuptargets/") {
			fmt.Fprintf(w, `{"spec":{"backupTargetURL":%q},"status":{"available":true}}`, repo)
			return
		}
		name := filepath.Base(r.URL.Path)
		if missingRemote.Load() && name == "backup-retained-two" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		short := strings.TrimPrefix(name, "backup-")
		fmt.Fprintf(w, `{"metadata":{"labels":{"fugue.pro/backup-run":%q}},"spec":{"snapshotName":%q},"status":{"state":"Completed","volumeName":"fixture-volume","url":%q}}`, "run-"+short, "snapshot-"+short, remoteURL(name))
	}))
	defer kube.Close()
	r2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead || !strings.HasSuffix(r.URL.Path, ".cfg") {
			t.Errorf("unexpected provider operation: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("ETag", `"fixture-config-etag"`)
	}))
	defer r2.Close()
	db := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	backend, err := db.CreateBackupBackend(model.BackupBackend{Name: "fixture", Provider: model.DataBackendProviderCloudflareR2, Bucket: "fixture", Prefix: "backups", Endpoint: r2.URL, Credentials: model.DataBackendCredentials{AccessKeyID: "test", SecretAccessKey: "secret"}})
	if err != nil {
		t.Fatal(err)
	}
	target := model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Engine: model.BackupEngineLonghornSnapshot}
	policy, err := db.UpsertBackupPolicy(model.BackupPolicy{Name: "fixture", Target: target, BackendID: backend.ID, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	create := func(suffix, status, provenance string, age time.Duration) {
		t.Helper()
		run, err := db.CreateBackupRun(model.BackupRun{ID: "run-" + suffix, PolicyID: policy.ID, BackendID: backend.ID, Target: target, Status: status})
		if err != nil {
			t.Fatal(err)
		}
		m := model.BackupManifest{RunID: run.ID, Kind: model.BackupArtifactKindLonghornSnapshot, Format: "longhorn-remote-backup", Metadata: map[string]string{
			"reconciliation_source": provenance, "volume_name": "fixture-volume", "snapshot_name": "snapshot-" + suffix,
			"longhorn_backup_name": "backup-" + suffix, "backup_target_name": "default", "backup_target_url": repo,
			"longhorn_backup_url": remoteURL("backup-" + suffix),
		}}
		if _, err := db.CreateBackupArtifact(model.BackupArtifact{ID: suffix, PolicyID: policy.ID, BackendID: backend.ID, RunID: run.ID, Target: target, Kind: m.Kind, Manifest: m, Protected: provenance != "", CreatedAt: time.Now().Add(-age)}); err != nil {
			t.Fatal(err)
		}
	}
	create("old", model.BackupRunStatusFailed, "completed-longhorn-orphan-manifest", 30*24*time.Hour)
	create("new", model.BackupRunStatusSucceeded, "", time.Hour)
	create("retained-one", model.BackupRunStatusSucceeded, "", 2*time.Hour)
	create("retained-two", model.BackupRunStatusSucceeded, "", 3*time.Hour)
	api := NewServer(db, auth.New(db, "fixture-admin"), nil, ServerConfig{})
	api.newBackupLonghornClient = func(string) (*kubeLogsClient, error) {
		return &kubeLogsClient{baseURL: kube.URL, client: kube.Client()}, nil
	}
	handler := api.Handler()
	call := func(token string, req model.RecoveredBackupRetirementRequest, status int) model.RecoveredBackupRetirement {
		t.Helper()
		payload, _ := json.Marshal(req)
		request := httptest.NewRequest(http.MethodPost, "/v1/backups/artifacts/old/retire-recovered", bytes.NewReader(payload))
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, request)
		if rec.Code != status {
			t.Fatalf("expected %d, got %d %s", status, rec.Code, rec.Body.String())
		}
		if status == http.StatusUnauthorized {
			return model.RecoveredBackupRetirement{}
		}
		var result model.RecoveredBackupRetirement
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	req := model.RecoveredBackupRetirementRequest{ReplacementArtifactID: "new", Reason: "retire historical point"}
	call("", req, http.StatusUnauthorized)
	preview := call("fixture-admin", req, http.StatusOK)
	if preview.Confirmation == "" || preview.Retired || len(preview.Blockers) != 0 || preview.RestoreEvidenceStatus != "not_provided" || len(preview.RetainedArtifactIDs) != 3 {
		t.Fatalf("preview not ready: %+v", preview)
	}
	req.Confirmation = preview.Confirmation
	missingRemote.Store(true)
	blocked := call("fixture-admin", req, http.StatusConflict)
	if blocked.Retired || blocked.Confirmation != "" || len(blocked.Blockers) != 1 {
		t.Fatalf("changed remote backup was not blocked: %+v", blocked)
	}
	missingRemote.Store(false)
	retired := call("fixture-admin", req, http.StatusOK)
	if !retired.Retired || retired.AuditID == "" {
		t.Fatalf("missing retirement receipt: %+v", retired)
	}
}
