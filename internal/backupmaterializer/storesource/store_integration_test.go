package storesource

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"fugue/internal/backupadapter"
	"fugue/internal/backupmaterializer/httpapi"
	"fugue/internal/backupmaterializer/legacysource"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestRealJSONStoreReadAdapterIsRedactedAndImmutable(t *testing.T) {
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "materializer-store-source-encryption-key")
	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("initialize store source integration store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name: "store source backend", Provider: model.DataBackendProviderS3,
		Bucket: "private-store-source-bucket", Region: "region-1", Endpoint: "https://s3.example.test", Prefix: "backups",
		Credentials: model.DataBackendCredentials{
			AccessKeyID: "store-source-access-key", SecretAccessKey: "store-source-secret-key", Token: "store-source-session-token",
		},
	})
	if err != nil {
		t.Fatalf("create store source backend: %v", err)
	}
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		ID: "run-store-source-real-1", BackendID: backend.ID, Trigger: model.BackupRunTriggerManual,
		Target: model.BackupTarget{Type: model.BackupTargetRegistry},
	})
	if err != nil {
		t.Fatalf("create store source run: %v", err)
	}
	backendObservation, err := stateStore.GetBackupBackendObservation(backend.ID, "", true)
	if err != nil {
		t.Fatalf("read expected backend generation: %v", err)
	}
	expected, err := backupadapter.BuildShadowSpec(run, backendObservation.Generation)
	if err != nil {
		t.Fatalf("build expected source spec: %v", err)
	}
	before, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read store before source: %v", err)
	}
	reader, err := New(stateStore)
	if err != nil {
		t.Fatalf("construct real store source: %v", err)
	}
	source, err := legacysource.New(reader.ReadSnapshot)
	if err != nil {
		t.Fatalf("construct real legacy source: %v", err)
	}
	input, err := source.ReadDesiredInput(context.Background(), httpapi.ReadRequest{RunID: run.ID, CellKey: expected.CellKey})
	if err != nil || input.Spec != expected || input.TenantID != run.TenantID {
		t.Fatalf("real store desired input drifted: input=%+v err=%v", input, err)
	}
	after, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read store after source: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("read-only store source mutated the JSON store")
	}
	snapshot, err := reader.ReadSnapshot(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("read redacted snapshot: %v", err)
	}
	if snapshot.BackendGeneration != backendObservation.Generation || snapshot.Run.ID != run.ID {
		t.Fatalf("redacted snapshot drifted: %+v", snapshot)
	}
}
