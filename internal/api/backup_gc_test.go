package api

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestBackupArtifactObjectKeysForDeletion(t *testing.T) {
	t.Parallel()

	valid := model.BackupArtifact{
		RunID:             "backup_run_test",
		Kind:              model.BackupArtifactKindAppPGDump,
		ObjectKey:         "apps/tenant/project/app/backup_run_test/database.dump",
		ManifestObjectKey: "apps/tenant/project/app/backup_run_test/manifest.json",
		Status:            model.BackupArtifactStatusDeleted,
	}
	keys, err := backupArtifactObjectKeysForDeletion(valid)
	if err != nil {
		t.Fatalf("valid artifact rejected: %v", err)
	}
	if len(keys) != 2 || keys[0] != valid.ObjectKey || keys[1] != valid.ManifestObjectKey {
		t.Fatalf("unexpected deletion keys: %#v", keys)
	}

	snapshot := valid
	snapshot.Kind = model.BackupArtifactKindDataSnapshot
	snapshot.ObjectKey = "snapshot-owned-by-workspace"
	keys, err = backupArtifactObjectKeysForDeletion(snapshot)
	if err != nil {
		t.Fatalf("data snapshot artifact rejected: %v", err)
	}
	if len(keys) != 1 || keys[0] != snapshot.ManifestObjectKey {
		t.Fatalf("snapshot blob would be deleted: %#v", keys)
	}

	unsafe := valid
	unsafe.ObjectKey = "apps/tenant/project/app/other-run/../backup_run_test/database.dump"
	if _, err := backupArtifactObjectKeysForDeletion(unsafe); err == nil {
		t.Fatal("unsafe path was accepted")
	}

	wrongRun := valid
	wrongRun.ObjectKey = "apps/tenant/project/app/another-run/database.dump"
	if _, err := backupArtifactObjectKeysForDeletion(wrongRun); err == nil {
		t.Fatal("object outside the artifact run was accepted")
	}

	missingKeys := valid
	missingKeys.ObjectKey = ""
	missingKeys.ManifestObjectKey = ""
	if _, err := backupArtifactObjectKeysForDeletion(missingKeys); err == nil {
		t.Fatal("artifact without object keys was treated as successfully deletable")
	}
}

func TestBackupArtifactCleanupCandidatesHonorRestoreInterlocksAndMarker(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	newArtifact := func(id string) model.BackupArtifact {
		t.Helper()
		artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
			ID:                id,
			RunID:             "run-" + id,
			TenantID:          "tenant-a",
			Target:            model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant-a"},
			Kind:              model.BackupArtifactKindAppPGDump,
			ObjectKey:         "apps/tenant-a/project/app/run-" + id + "/database.dump",
			ManifestObjectKey: "apps/tenant-a/project/app/run-" + id + "/manifest.json",
			Status:            model.BackupArtifactStatusActive,
		})
		if err != nil {
			t.Fatalf("create artifact %s: %v", id, err)
		}
		return artifact
	}

	protectedByPlan := newArtifact("with-plan")
	if _, err := stateStore.CreateBackupRestorePlan(model.BackupRestorePlan{
		ID:         "plan-with-artifact",
		ArtifactID: protectedByPlan.ID,
		TenantID:   protectedByPlan.TenantID,
		Target:     protectedByPlan.Target,
		Mode:       model.BackupRestoreModePlanOnly,
		Status:     model.BackupRestoreStatusPlanned,
	}); err != nil {
		t.Fatalf("create restore plan: %v", err)
	}
	if _, err := stateStore.MarkBackupArtifactDeleted(protectedByPlan.ID, protectedByPlan.TenantID, false); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("delete artifact with restore plan error = %v, want conflict", err)
	}
	stillActive, err := stateStore.GetBackupArtifact(protectedByPlan.ID, protectedByPlan.TenantID, false)
	if err != nil {
		t.Fatalf("get restore-protected artifact: %v", err)
	}
	if stillActive.Status != model.BackupArtifactStatusActive || stillActive.DeletedAt != nil {
		t.Fatalf("restore-protected artifact changed: %+v", stillActive)
	}

	collectable := newArtifact("collectable")
	if _, err := stateStore.MarkBackupArtifactDeleted(collectable.ID, collectable.TenantID, false); err != nil {
		t.Fatalf("delete collectable artifact: %v", err)
	}

	candidates, err := stateStore.ListBackupArtifactCleanupCandidates(store.BackupArtifactCleanupFilter{
		Before: time.Now().UTC().Add(time.Minute),
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("list cleanup candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].ID != collectable.ID {
		t.Fatalf("unexpected candidates: %#v", candidates)
	}
	if err := stateStore.MarkBackupArtifactPhysicalDeleted(collectable.ID, time.Now().UTC()); err != nil {
		t.Fatalf("mark physical deletion: %v", err)
	}
	candidates, err = stateStore.ListBackupArtifactCleanupCandidates(store.BackupArtifactCleanupFilter{
		Before: time.Now().UTC().Add(time.Minute),
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("list candidates after marker: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("marked artifact remained a candidate: %#v", candidates)
	}
	if err := stateStore.MarkBackupArtifactPhysicalDeleted("missing", time.Now().UTC()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing marker error = %v, want not found", err)
	}

	alreadyDeleted := newArtifact("deleted-cannot-plan")
	if _, err := stateStore.MarkBackupArtifactDeleted(alreadyDeleted.ID, alreadyDeleted.TenantID, false); err != nil {
		t.Fatalf("delete artifact before restore plan: %v", err)
	}
	if _, err := stateStore.CreateBackupRestorePlan(model.BackupRestorePlan{
		ID:         "plan-after-delete",
		ArtifactID: alreadyDeleted.ID,
		TenantID:   alreadyDeleted.TenantID,
		Target:     alreadyDeleted.Target,
		Mode:       model.BackupRestoreModePlanOnly,
		Status:     model.BackupRestoreStatusPlanned,
	}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("restore plan for deleted artifact error = %v, want conflict", err)
	}
}

func TestSweepDeletedBackupArtifactsDeletesObjectsAndPersistsMarker(t *testing.T) {
	t.Parallel()

	var deleteBody string
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !hasQueryKey(r, "delete") {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		deleteBody = string(body)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`))
	}))
	defer s3Server.Close()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "gc-test",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: s3Server.URL,
		Region:   "us-east-1",
		Prefix:   "backup-root",
		Status:   "active",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
		Capabilities: model.DataBackendCapabilitiesForProvider(model.DataBackendProviderS3),
	})
	if err != nil {
		t.Fatalf("create backup backend: %v", err)
	}
	deletedAt := time.Now().UTC().Add(-2 * backupArtifactGCGrace)
	artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
		ID:                "artifact-gc",
		RunID:             "backup_run_gc",
		Target:            model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID:         backend.ID,
		Kind:              model.BackupArtifactKindControlPlanePGDump,
		ObjectKey:         "control-plane/2026/07/29/backup_run_gc/control-plane.dump",
		ManifestObjectKey: "control-plane/2026/07/29/backup_run_gc/manifest.json",
		Status:            model.BackupArtifactStatusDeleted,
		DeletedAt:         &deletedAt,
	})
	if err != nil {
		t.Fatalf("create deleted artifact: %v", err)
	}

	server := &Server{store: stateStore}
	server.sweepDeletedBackupArtifacts(t.Context())
	for _, key := range []string{
		"backup-root/" + artifact.ObjectKey,
		"backup-root/" + artifact.ManifestObjectKey,
	} {
		if !strings.Contains(deleteBody, "<Key>"+key+"</Key>") {
			t.Fatalf("delete request missing %s: %s", key, deleteBody)
		}
	}
	candidates, err := stateStore.ListBackupArtifactCleanupCandidates(store.BackupArtifactCleanupFilter{
		Before: time.Now().UTC(),
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("list candidates after sweep: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("sweep did not persist the physical deletion marker: %#v", candidates)
	}
}
