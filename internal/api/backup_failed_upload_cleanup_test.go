package api

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestBackupFailedRunObjectKeyAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  model.BackupRun
		key  string
		want bool
	}{
		{
			name: "control-plane dump",
			run:  model.BackupRun{ID: "backup_run_cp", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}},
			key:  "control-plane/2026/07/29/21/backup_run_cp/control-plane.dump",
			want: true,
		},
		{
			name: "control-plane malformed hour",
			run:  model.BackupRun{ID: "backup_run_cp", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}},
			key:  "control-plane/2026/07/29/hour/backup_run_cp/control-plane.dump",
		},
		{
			name: "app database manifest",
			run:  model.BackupRun{ID: "backup_run_app", TenantID: "tenant-1", ProjectID: "project-1", AppID: "app-1", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase}},
			key:  "apps/tenant-1/project-1/app-1/backup_run_app/manifest.json",
			want: true,
		},
		{
			name: "app database cross app",
			run:  model.BackupRun{ID: "backup_run_app", TenantID: "tenant-1", ProjectID: "project-1", AppID: "app-1", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase}},
			key:  "apps/tenant-1/project-1/app-2/backup_run_app/database.dump",
		},
		{
			name: "app database missing tenant",
			run:  model.BackupRun{ID: "backup_run_app", ProjectID: "project-1", AppID: "app-1", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase}},
			key:  "apps/project-1/app-1/backup_run_app/database.dump",
		},
		{
			name: "persistent storage archive",
			run:  model.BackupRun{ID: "backup_run_files", TenantID: "tenant-1", ProjectID: "project-1", AppID: "app-1", Target: model.BackupTarget{Type: model.BackupTargetPersistentStorage}},
			key:  "apps/tenant-1/project-1/app-1/backup_run_files/persistent-storage/persistent-storage.tar.gz",
			want: true,
		},
		{
			name: "data workspace manifest",
			run:  model.BackupRun{ID: "backup_run_data", TenantID: "tenant-1", ProjectID: "project-1", Target: model.BackupTarget{Type: model.BackupTargetDataWorkspace, WorkspaceID: "workspace-1"}},
			key:  "data-workspaces/tenant-1/project-1/workspace-1/backup_run_data/manifest.json",
			want: true,
		},
		{
			name: "registry archive",
			run:  model.BackupRun{ID: "backup_run_registry", Target: model.BackupTarget{Type: model.BackupTargetRegistry}},
			key:  "platform/registry/backup_run_registry/registry.tar.gz",
			want: true,
		},
		{
			name: "unexpected filename",
			run:  model.BackupRun{ID: "backup_run_registry", Target: model.BackupTarget{Type: model.BackupTargetRegistry}},
			key:  "platform/registry/backup_run_registry/catalog.json",
		},
		{
			name: "path traversal",
			run:  model.BackupRun{ID: "backup_run_registry", Target: model.BackupTarget{Type: model.BackupTargetRegistry}},
			key:  "platform/registry/backup_run_registry/../manifest.json",
		},
		{
			name: "noncanonical leading slash",
			run:  model.BackupRun{ID: "backup_run_registry", Target: model.BackupTarget{Type: model.BackupTargetRegistry}},
			key:  "/platform/registry/backup_run_registry/manifest.json",
		},
		{
			name: "unsafe run id segment",
			run:  model.BackupRun{ID: "../backup_run_registry", Target: model.BackupTarget{Type: model.BackupTargetRegistry}},
			key:  "platform/registry/backup_run_registry/manifest.json",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := backupFailedRunObjectKeyAllowed(test.run, test.key); got != test.want {
				t.Fatalf("backupFailedRunObjectKeyAllowed()=%t, want %t", got, test.want)
			}
		})
	}
}

func TestBackupObjectUploadTransactionCleansAttemptedPutAfterFailure(t *testing.T) {
	fake := newFailedUploadS3(t)
	backend := newTestDataObjectBackend(t, fake.URL)
	backend.backend.Prefix = "backup-root"
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	run := model.BackupRun{ID: "backup_run_cleanup", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}}
	tx := newBackupObjectUploadTransaction(&Server{store: stateStore}, run, backend)
	key := "control-plane/2026/07/29/21/backup_run_cleanup/control-plane.dump"
	if err := tx.putObject(context.Background(), key, bytes.NewReader([]byte("dump")), 4); err != nil {
		t.Fatalf("put object: %v", err)
	}
	rootErr := errors.New("verify failed")
	retErr := rootErr
	tx.cleanupOnError(context.Background(), &retErr)
	if !errors.Is(retErr, rootErr) {
		t.Fatalf("cleanup replaced root error: %v", retErr)
	}
	if fake.has("backup-root/" + key) {
		t.Fatal("failed upload object remained after synchronous cleanup")
	}
	if fake.deleteCalls() != 1 {
		t.Fatalf("expected one delete call, got %d", fake.deleteCalls())
	}
}

func TestBackupObjectUploadTransactionCleansAmbiguousFailedPut(t *testing.T) {
	fake := newFailedUploadS3(t)
	fake.setFailPutsAfterStore(true)
	backend := newTestDataObjectBackend(t, fake.URL)
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	run := model.BackupRun{ID: "backup_run_ambiguous_put", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}}
	tx := newBackupObjectUploadTransaction(&Server{store: stateStore}, run, backend)
	key := "control-plane/2026/07/29/21/backup_run_ambiguous_put/control-plane.dump"
	putErr := tx.putObject(context.Background(), key, bytes.NewReader([]byte("dump")), 4)
	if putErr == nil {
		t.Fatal("expected ambiguous put to return an error")
	}
	if !fake.has(key) {
		t.Fatal("ambiguous put fixture did not persist the object before returning an error")
	}
	tx.cleanupOnError(context.Background(), &putErr)
	if fake.has(key) {
		t.Fatal("ambiguous failed put left its uploaded object behind")
	}
	if fake.deleteCalls() != 1 {
		t.Fatalf("expected one delete call, got %d", fake.deleteCalls())
	}
}

func TestBackupObjectUploadTransactionPreservesCommittedArtifact(t *testing.T) {
	fake := newFailedUploadS3(t)
	backend := newTestDataObjectBackend(t, fake.URL)
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	run := model.BackupRun{ID: "backup_run_committed", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}}
	key := "control-plane/2026/07/29/21/backup_run_committed/control-plane.dump"
	tx := newBackupObjectUploadTransaction(&Server{store: stateStore}, run, backend)
	if err := tx.putObject(context.Background(), key, bytes.NewReader([]byte("dump")), 4); err != nil {
		t.Fatalf("put object: %v", err)
	}
	if _, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
		RunID:     run.ID,
		Target:    run.Target,
		Kind:      model.BackupArtifactKindControlPlanePGDump,
		ObjectKey: key,
		Status:    model.BackupArtifactStatusActive,
	}); err != nil {
		t.Fatalf("create committed artifact: %v", err)
	}
	retErr := errors.New("ambiguous artifact commit")
	tx.cleanupOnError(context.Background(), &retErr)
	if !fake.has(key) {
		t.Fatal("cleanup deleted an object referenced by a committed artifact")
	}
	if fake.deleteCalls() != 0 {
		t.Fatalf("expected no delete call, got %d", fake.deleteCalls())
	}
}

func TestControlPlaneBackupVerificationFailureDeletesUploadedDump(t *testing.T) {
	fake := newFailedUploadS3(t)
	fake.setCorruptGets(true)
	stateStore, backend := newFailedUploadBackupStore(t, fake.URL, "")
	pgDump := writeFailedUploadPGDump(t)
	t.Setenv("FUGUE_PG_DUMP_BIN", pgDump)
	server := &Server{store: stateStore, controlPlaneDatabaseURL: "postgres://backup.invalid/fugue"}
	run := model.BackupRun{
		ID:        "backup_run_verify_control_plane",
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
	}

	if _, err := server.runControlPlaneDatabaseBackup(context.Background(), run); err == nil || !strings.Contains(err.Error(), "verify control-plane dump") {
		t.Fatalf("expected verification failure, got %v", err)
	}
	if fake.objectCount() != 0 {
		t.Fatalf("verification failure left %d uploaded objects", fake.objectCount())
	}
}

func TestControlPlaneBackupLeaseConflictDeletesDumpAndManifest(t *testing.T) {
	fake := newFailedUploadS3(t)
	stateStore, backend := newFailedUploadBackupStore(t, fake.URL, "")
	pgDump := writeFailedUploadPGDump(t)
	t.Setenv("FUGUE_PG_DUMP_BIN", pgDump)
	server := &Server{store: stateStore, controlPlaneDatabaseURL: "postgres://backup.invalid/fugue"}
	// The worker carries a lease owner, but the run is deliberately absent from
	// the store. Artifact creation therefore returns the same conflict used for
	// a lost/expired lease after both objects have been uploaded.
	run := model.BackupRun{
		ID:         "backup_run_lease_control_plane",
		Target:     model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID:  backend.ID,
		LeaseOwner: "worker-that-lost-its-lease",
	}

	if _, err := server.runControlPlaneDatabaseBackup(context.Background(), run); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected lease conflict, got %v", err)
	}
	if fake.objectCount() != 0 {
		t.Fatalf("lease conflict left %d uploaded objects", fake.objectCount())
	}
	if fake.deleteCalls() != 1 {
		t.Fatalf("expected dump and manifest in one delete call, got %d", fake.deleteCalls())
	}
}

func TestAppDatabaseBackupVerificationFailureDeletesUploadedDump(t *testing.T) {
	fake := newFailedUploadS3(t)
	fake.setCorruptGets(true)
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Failed Upload Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "database-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: runtime.ID,
		Replicas:  1,
		Postgres:  &model.AppPostgresSpec{Database: "appdb"},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: tenant.ID,
		Name:     "cleanup-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: fake.URL,
		Region:   "us-east-1",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backup backend: %v", err)
	}
	pgDump := writeFailedUploadPGDump(t)
	t.Setenv("FUGUE_PG_DUMP_BIN", pgDump)
	server := &Server{store: stateStore}
	run := model.BackupRun{
		ID:        "backup_run_verify_app",
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		AppID:     app.ID,
		Target: model.BackupTarget{
			Type:      model.BackupTargetAppDatabase,
			TenantID:  tenant.ID,
			ProjectID: project.ID,
			AppID:     app.ID,
		},
		BackendID: backend.ID,
	}

	if _, err := server.runAppDatabaseBackup(context.Background(), run); err == nil || !strings.Contains(err.Error(), "verify app database dump") {
		t.Fatalf("expected app verification failure, got %v", err)
	}
	if fake.objectCount() != 0 {
		t.Fatalf("app verification failure left %d uploaded objects", fake.objectCount())
	}
}

func TestSweepFailedBackupRunObjectsDeletesOnlyExactUnreferencedRunObjects(t *testing.T) {
	fake := newFailedUploadS3(t)
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "cleanup-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: fake.URL,
		Region:   "us-east-1",
		Prefix:   "backup-root",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backup backend: %v", err)
	}
	finishedAt := time.Now().UTC().Add(-2 * time.Hour)
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		ID:         "backup_run_orphan",
		Target:     model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID:  backend.ID,
		Status:     model.BackupRunStatusFailed,
		FinishedAt: &finishedAt,
	})
	if err != nil {
		t.Fatalf("create failed backup run: %v", err)
	}
	orphanKey := "backup-root/control-plane/2026/07/29/20/" + run.ID + "/control-plane.dump"
	unrelatedKey := "backup-root/control-plane/2026/07/29/20/backup_run_other/control-plane.dump"
	referencedRun, err := stateStore.CreateBackupRun(model.BackupRun{
		ID:         "backup_run_referenced",
		Target:     model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID:  backend.ID,
		Status:     model.BackupRunStatusFailed,
		FinishedAt: &finishedAt,
	})
	if err != nil {
		t.Fatalf("create referenced failed backup run: %v", err)
	}
	referencedKey := "backup-root/control-plane/2026/07/29/20/" + referencedRun.ID + "/control-plane.dump"
	if _, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
		RunID:     referencedRun.ID,
		Target:    referencedRun.Target,
		BackendID: backend.ID,
		Kind:      model.BackupArtifactKindControlPlanePGDump,
		ObjectKey: strings.TrimPrefix(referencedKey, "backup-root/"),
		Status:    model.BackupArtifactStatusActive,
	}); err != nil {
		t.Fatalf("create artifact reference for failed run: %v", err)
	}
	fake.put(orphanKey, []byte("orphan"))
	fake.put(unrelatedKey, []byte("preserve"))
	fake.put(referencedKey, []byte("referenced"))

	server := &Server{store: stateStore}
	server.sweepFailedBackupRunObjects(context.Background())
	if fake.has(orphanKey) {
		t.Fatal("durable failed-run sweep left the orphan object behind")
	}
	if !fake.has(unrelatedKey) {
		t.Fatal("durable failed-run sweep deleted an unrelated object")
	}
	if !fake.has(referencedKey) {
		t.Fatal("durable failed-run sweep deleted an artifact-referenced object")
	}
	candidates, err := stateStore.ListFailedBackupRunObjectCleanupCandidates(store.BackupRunObjectCleanupFilter{
		Before: time.Now().UTC(),
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("list cleanup candidates: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("expected durable cleanup marker to suppress retry, got %+v", candidates)
	}
}

func TestBackupFailedRunObjectKeysFailsClosedOnUnexpectedSameRunKey(t *testing.T) {
	backend := &dataObjectBackend{backend: model.DataBackend{Prefix: "backup-root"}}
	run := model.BackupRun{ID: "backup_run_shape", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}}
	objects := []dataObjectInfo{
		{Key: "backup-root/control-plane/2026/07/29/20/backup_run_shape/control-plane.dump", Size: 10},
		{Key: "backup-root/control-plane/2026/07/29/20/backup_run_shape/unknown.bin", Size: 20},
	}
	keys, bytes, err := backupFailedRunObjectKeys(run, backend, objects)
	if err == nil {
		t.Fatal("expected unexpected same-run key to fail closed")
	}
	if len(keys) != 0 || bytes != 0 {
		t.Fatalf("failed-closed matcher returned deletable objects: keys=%v bytes=%d", keys, bytes)
	}
}

type failedUploadS3 struct {
	*httptest.Server
	mu                 sync.Mutex
	objects            map[string][]byte
	deletes            int
	corrupt            bool
	failPutsAfterStore bool
}

func newFailedUploadS3(t *testing.T) *failedUploadS3 {
	t.Helper()
	fake := &failedUploadS3{objects: map[string][]byte{}}
	fake.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		key = strings.TrimPrefix(key, "bucket/")
		switch {
		case r.Method == http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read failed", http.StatusInternalServerError)
				return
			}
			fake.put(key, body)
			fake.mu.Lock()
			failPut := fake.failPutsAfterStore
			fake.mu.Unlock()
			if failPut {
				http.Error(w, "synthetic ambiguous put failure", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			prefix := r.URL.Query().Get("prefix")
			fake.mu.Lock()
			contents := make([]failedUploadS3Object, 0, len(fake.objects))
			for objectKey, body := range fake.objects {
				if strings.HasPrefix(objectKey, prefix) {
					contents = append(contents, failedUploadS3Object{Key: objectKey, Size: int64(len(body))})
				}
			}
			fake.mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(failedUploadS3List{IsTruncated: false, Contents: contents})
		case r.Method == http.MethodGet:
			fake.mu.Lock()
			body, ok := fake.objects[key]
			corrupt := fake.corrupt
			fake.mu.Unlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			body = append([]byte(nil), body...)
			if corrupt {
				body = append(body, '!')
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
			_, _ = w.Write(body)
		case r.Method == http.MethodPost && hasQueryKey(r, "delete"):
			var request struct {
				Objects []struct {
					Key string `xml:"Key"`
				} `xml:"Object"`
			}
			if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, "decode failed", http.StatusBadRequest)
				return
			}
			fake.mu.Lock()
			fake.deletes++
			for _, object := range request.Objects {
				delete(fake.objects, object.Key)
			}
			fake.mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`)
		default:
			http.Error(w, fmt.Sprintf("unexpected request %s", r.Method), http.StatusBadRequest)
		}
	}))
	t.Cleanup(fake.Close)
	return fake
}

type failedUploadS3Object struct {
	Key  string `xml:"Key"`
	Size int64  `xml:"Size"`
}

type failedUploadS3List struct {
	XMLName     xml.Name               `xml:"ListBucketResult"`
	Xmlns       string                 `xml:"xmlns,attr,omitempty"`
	IsTruncated bool                   `xml:"IsTruncated"`
	Contents    []failedUploadS3Object `xml:"Contents"`
}

func (f *failedUploadS3) put(key string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = append([]byte(nil), body...)
}

func (f *failedUploadS3) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}

func (f *failedUploadS3) deleteCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deletes
}

func (f *failedUploadS3) objectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

func (f *failedUploadS3) setCorruptGets(corrupt bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.corrupt = corrupt
}

func (f *failedUploadS3) setFailPutsAfterStore(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failPutsAfterStore = fail
}

func newFailedUploadBackupStore(t *testing.T, endpoint, tenantID string) (*store.Store, model.BackupBackend) {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: tenantID,
		Name:     "cleanup-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: endpoint,
		Region:   "us-east-1",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backup backend: %v", err)
	}
	return stateStore, backend
}

func writeFailedUploadPGDump(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pg_dump")
	script := `#!/bin/sh
set -eu
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--file" ]; then
    shift
    printf 'synthetic backup dump' > "$1"
    exit 0
  fi
  shift
done
exit 2
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake pg_dump: %v", err)
	}
	return path
}
