package api

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestOrphanManifestLogicalKeyAllowed(t *testing.T) {
	tests := []struct {
		name string
		run  model.BackupRun
		key  string
		want bool
	}{
		{
			name: "current control plane layout",
			run:  model.BackupRun{ID: "backup_run_1", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}},
			key:  "control-plane/backup_run_1/manifest.json", want: true,
		},
		{
			name: "legacy control plane component layout",
			run:  model.BackupRun{ID: "backup_run_1", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Component: "control-plane-postgres"}},
			key:  "apps/control-plane-postgres/backup_run_1/manifest.json", want: true,
		},
		{
			name: "app database exact target",
			run:  model.BackupRun{ID: "backup_run_1", TenantID: "tenant_1", ProjectID: "project_1", AppID: "app_1", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase}},
			key:  "apps/tenant_1/project_1/app_1/backup_run_1/manifest.json", want: true,
		},
		{
			name: "different app",
			run:  model.BackupRun{ID: "backup_run_1", TenantID: "tenant_1", ProjectID: "project_1", AppID: "app_1", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase}},
			key:  "apps/tenant_1/project_1/app_other/backup_run_1/manifest.json",
		},
		{
			name: "arbitrary app prefix for platform run",
			run:  model.BackupRun{ID: "backup_run_1", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}},
			key:  "apps/not-the-target/backup_run_1/manifest.json",
		},
		{
			name: "path traversal",
			run:  model.BackupRun{ID: "backup_run_1", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}},
			key:  "control-plane/../backup_run_1/manifest.json",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := orphanManifestLogicalKeyAllowed(test.run, test.key); got != test.want {
				t.Fatalf("orphanManifestLogicalKeyAllowed()=%t, want %t", got, test.want)
			}
		})
	}
}

func TestOrphanManifestCleanupSafetyFlow(t *testing.T) {
	for _, mode := range []string{"safe", "recent", "bytes", "referenced", "additional-object", "wrong-run", "changed-etag", "changed-before-delete", "worker-lease", "succeeded"} {
		t.Run(mode, func(t *testing.T) {
			clearBackupUsageSeedEnv(t)
			const logicalKey = "control-plane/backup_run_manifest/manifest.json"
			const physicalKey = "backup-root/" + logicalKey
			manifest := model.BackupManifest{SchemaVersion: "v1", RunID: "backup_run_manifest", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}, Kind: model.BackupArtifactKindControlPlanePGDump, ManifestObjectKey: logicalKey}
			if mode == "wrong-run" {
				manifest.RunID = "backup_run_other"
			}
			body, _ := json.Marshal(manifest)
			deleted, heads := 0, 0
			fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
					if r.URL.Query().Get("prefix") != strings.TrimSuffix(physicalKey, "manifest.json") || r.URL.Query().Get("max-keys") != "2" {
						t.Errorf("unbounded or wrong-prefix listing: %s", r.URL)
					}
					w.Header().Set("Content-Type", "application/xml")
					extra := ""
					if mode == "additional-object" {
						extra = "<Contents><Key>" + strings.TrimSuffix(physicalKey, "manifest.json") + "database.dump</Key><Size>100</Size></Contents>"
					}
					_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>%s</Key><Size>%d</Size></Contents>%s</ListBucketResult>`, physicalKey, len(body), extra)
				case r.URL.Path != "/bucket/"+physicalKey:
					t.Errorf("unexpected object target %s", r.URL)
					w.WriteHeader(http.StatusNotFound)
				case r.Method == http.MethodHead:
					heads++
					w.Header().Set("ETag", `"manifest-etag"`)
					if mode == "changed-before-delete" && heads >= 3 {
						w.Header().Set("ETag", `"replacement-etag"`)
					}
					w.Header().Set("Content-Length", fmt.Sprint(len(body)))
				case r.Method == http.MethodGet:
					if r.Header.Get("If-Match") != `"manifest-etag"` {
						t.Error("manifest read is not bound to the HEAD observation")
					}
					w.Header().Set("ETag", `"manifest-etag"`)
					_, _ = w.Write(body)
				case r.Method == http.MethodDelete:
					deleted++
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected provider mutation %s %s", r.Method, r.URL)
				}
			}))
			defer fake.Close()
			db := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := db.Init(); err != nil {
				t.Fatal(err)
			}
			backend, err := db.CreateBackupBackend(model.BackupBackend{Name: "cleanup-r2", Provider: model.DataBackendProviderCloudflareR2, Bucket: "bucket", Endpoint: fake.URL, Prefix: "backup-root", Credentials: model.DataBackendCredentials{AccessKeyID: "test", SecretAccessKey: "test-secret"}})
			if err != nil {
				t.Fatal(err)
			}
			finished := time.Now().Add(-48 * time.Hour).UTC()
			if mode == "recent" {
				finished = time.Now().UTC()
			}
			run := model.BackupRun{ID: manifest.RunID, BackendID: backend.ID, Status: model.BackupRunStatusFailed, FinishedAt: &finished, Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}}
			run.ID = "backup_run_manifest"
			if mode == "bytes" {
				run.BytesWritten = 100
			}
			if mode == "succeeded" {
				run.Status = model.BackupRunStatusSucceeded
			}
			if mode == "worker-lease" {
				until := time.Now().Add(time.Hour)
				run.LockedUntil = &until
			}
			if _, err := db.CreateBackupRun(run); err != nil {
				t.Fatal(err)
			}
			if mode == "referenced" {
				if _, err := db.CreateBackupArtifact(model.BackupArtifact{RunID: run.ID, BackendID: backend.ID, Target: run.Target, Kind: manifest.Kind, ManifestObjectKey: logicalKey, Status: model.BackupArtifactStatusActive}); err != nil {
					t.Fatal(err)
				}
			}
			s := NewServer(db, auth.New(db, "bootstrap-test"), nil, ServerConfig{})
			request := orphanManifestCleanupRequest{RunID: run.ID, ObjectKey: physicalKey}
			preview, err := s.inspectOrphanManifest(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			wantDelete := mode == "safe" || mode == "changed-etag" || mode == "changed-before-delete"
			if preview.CanDelete != wantDelete || deleted != 0 {
				t.Fatalf("preview=%+v deletions=%d", preview, deleted)
			}
			request.Execute, request.ExpectedETag = true, preview.ETag
			if mode == "changed-etag" {
				request.ExpectedETag = "outdated-etag"
			}
			payload, _ := json.Marshal(request)
			req := httptest.NewRequest(http.MethodPost, "/v1/admin/storage/orphan-manifests/cleanup", strings.NewReader(string(payload)))
			req.Header.Set("Authorization", "Bearer bootstrap-test")
			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(rec, req)
			if mode == "safe" {
				if rec.Code != http.StatusOK || deleted != 1 || !strings.Contains(rec.Body.String(), `"executed":true`) {
					t.Fatalf("safe execute status=%d deleted=%d body=%s", rec.Code, deleted, rec.Body)
				}
			} else if rec.Code != http.StatusConflict || deleted != 0 {
				t.Fatalf("unsafe execute status=%d deleted=%d body=%s", rec.Code, deleted, rec.Body)
			}
		})
	}
}

func TestOrphanManifestCleanupRequiresPlatformAdmin(t *testing.T) {
	db := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := db.CreateTenant("test")
	_, token, _ := db.CreateAPIKey(tenant.ID, "storage-admin", []string{"storage.admin"})
	s := NewServer(db, auth.New(db, ""), nil, ServerConfig{})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/storage/orphan-manifests/cleanup", strings.NewReader(`{"run_id":"backup_run_manifest","object_key":"backup-root/manifest.json"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
	}
}

func TestInspectOrphanLonghornRemoteBlocksCompletedBackup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/backups") {
			t.Fatalf("unexpected Longhorn inventory request: %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"backup-completed","labels":{"fugue.pro/backup-run":"backup_run_1"}},"status":{"state":"Completed"}}]}`))
	}))
	defer server.Close()
	state, exists, err := inspectOrphanLonghornRemoteWithClient(context.Background(), &kubeLogsClient{baseURL: server.URL, client: server.Client()}, "longhorn-system", "backup_run_1")
	if err != nil || !exists || state != "Completed" {
		t.Fatalf("Longhorn remote check state=%q exists=%t err=%v", state, exists, err)
	}
}

func TestInspectOrphanLonghornRemoteRequiresCompleteInventory(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "unlabeled result", body: `{"items":[{"metadata":{"name":"ambiguous"},"status":{"state":"Completed"}}]}`},
		{name: "truncated forever", body: `{"items":[],"metadata":{"continue":"next"}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			state, exists, err := inspectOrphanLonghornRemoteWithClient(context.Background(), &kubeLogsClient{baseURL: server.URL, client: server.Client()}, "longhorn-system", "backup_run_1")
			if err == nil || exists || state != "" {
				t.Fatalf("incomplete Longhorn inventory was trusted state=%q exists=%t err=%v", state, exists, err)
			}
			if test.name == "truncated forever" && calls < 2 {
				t.Fatalf("pagination continuation was not followed, calls=%d", calls)
			}
		})
	}
}

func TestOrphanLonghornRepositoryRequiresVerifiedAbsence(t *testing.T) {
	for _, mode := range []string{"absent", "recoverable", "unknown-layout", "provider-error", "wrong-repository"} {
		t.Run(mode, func(t *testing.T) {
			requests := 0
			fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != http.MethodHead {
					t.Errorf("repository check attempted a mutation: %s", r.Method)
				}
				const base = "/bucket/backup-root/longhorn/backupstore/volumes/b2/a3/volume-test/"
				switch r.URL.Path {
				case base + "volume.cfg":
					if mode == "unknown-layout" {
						w.WriteHeader(http.StatusNotFound)
					}
				case base + "backups/backup_backup-test.cfg":
					if mode == "provider-error" {
						w.WriteHeader(http.StatusForbidden)
					} else if mode != "recoverable" {
						w.WriteHeader(http.StatusNotFound)
					}
				default:
					t.Errorf("wrong repository key: %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer fake.Close()
			backend, err := newDataObjectBackend(model.DataBackend{Provider: model.DataBackendProviderCloudflareR2, Bucket: "bucket", Endpoint: fake.URL, Prefix: "backup-root", Credentials: model.DataBackendCredentials{AccessKeyID: "test", SecretAccessKey: "test-secret"}})
			if err != nil {
				t.Fatal(err)
			}
			manifest := model.BackupManifest{Kind: model.BackupArtifactKindLonghornSnapshot, Metadata: map[string]string{
				"volume_name": "volume-test", "longhorn_backup_name": "backup-test",
				"backup_target_url":   "s3://bucket@auto/backup-root/longhorn",
				"longhorn_backup_url": "s3://bucket@auto/backup-root/longhorn?backup=backup-test&volume=volume-test",
			}}
			if mode == "wrong-repository" {
				manifest.Metadata["longhorn_backup_url"] = "s3://bucket@auto/elsewhere?backup=backup-test&volume=volume-test"
			}
			err = verifyOrphanLonghornRepositoryAbsent(context.Background(), backend, manifest)
			if (err == nil) != (mode == "absent") {
				t.Fatalf("mode=%s err=%v", mode, err)
			}
			if mode == "wrong-repository" && requests != 0 {
				t.Fatalf("invalid repository identity triggered %d provider requests", requests)
			}
		})
	}
}
