package store

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestGetBackupBackendObservationIsReadOnlyRedactedAndRotationBound(t *testing.T) {
	clearDefaultDataBackendEnv(t)
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "backup-observation-test-key")
	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: "tenant-1", Name: "observer backend", Provider: model.DataBackendProviderS3,
		Bucket: "private-bucket", Region: "region-1", Endpoint: "https://s3.example.test", Prefix: "backups",
		Credentials: model.DataBackendCredentials{AccessKeyID: "access-key-1", SecretAccessKey: "secret-key-1", Token: "session-token-1"},
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	if _, err := stateStore.GetBackupBackendObservation(backend.ID, "tenant-other", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant observation error = %v, want not found", err)
	}
	beforeRead, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read store before observation: %v", err)
	}
	first, err := stateStore.GetBackupBackendObservation(backend.ID, "tenant-1", false)
	if err != nil {
		t.Fatalf("get backend observation: %v", err)
	}
	afterRead, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read store after observation: %v", err)
	}
	if !reflect.DeepEqual(beforeRead, afterRead) {
		t.Fatal("read-only backend observation mutated the JSON store")
	}
	assertBackupBackendObservationRedacted(t, first, "access-key-1", "secret-key-1", "session-token-1", "private-bucket", "s3.example.test")
	firstGeneration := first.Generation

	if _, err := stateStore.RecordBackupBackendTest(backend.ID, "tenant-1", false, false, "transient secret-shaped health detail"); err != nil {
		t.Fatalf("record backend health: %v", err)
	}
	afterHealth, err := stateStore.GetBackupBackendObservation(backend.ID, "tenant-1", false)
	if err != nil {
		t.Fatalf("get observation after health: %v", err)
	}
	afterHealthGeneration := afterHealth.Generation
	if afterHealthGeneration != firstGeneration || !reflect.DeepEqual(afterHealth, first) {
		t.Fatalf("health noise churned observation: first=%+v after=%+v firstGeneration=%q afterGeneration=%q", first, afterHealth, firstGeneration, afterHealthGeneration)
	}

	if _, err := stateStore.RotateBackupBackendCredentials(backend.ID, "tenant-1", false, model.DataBackendCredentials{
		AccessKeyID: "access-key-2", SecretAccessKey: "secret-key-2", Token: "session-token-2",
	}); err != nil {
		t.Fatalf("rotate backend credentials: %v", err)
	}
	afterRotation, err := stateStore.GetBackupBackendObservation(backend.ID, "tenant-1", false)
	if err != nil {
		t.Fatalf("get observation after rotation: %v", err)
	}
	assertBackupBackendObservationRedacted(t, afterRotation, "access-key-2", "secret-key-2", "session-token-2", "private-bucket", "s3.example.test")
	rotationGeneration := afterRotation.Generation
	if rotationGeneration == firstGeneration {
		t.Fatalf("credential rotation did not advance generation: first=%q rotated=%q", firstGeneration, rotationGeneration)
	}
	if err := stateStore.withLockedState(true, func(state *model.State) error {
		state.BackupBackendSecrets = nil
		return nil
	}); err != nil {
		t.Fatalf("corrupt backend secret fixture: %v", err)
	}
	if _, err := stateStore.GetBackupBackendObservation(backend.ID, "tenant-1", false); !errors.Is(err, ErrConflict) {
		t.Fatalf("missing declared backend secret error = %v, want conflict", err)
	}
}

func TestGetBackupBackendObservationSupportsCredentialFreeBackend(t *testing.T) {
	clearDefaultDataBackendEnv(t)
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name: "credential-free", Provider: model.DataBackendProviderMinIO,
		Bucket: "public-test", Endpoint: "https://minio.example.test",
	})
	if err != nil {
		t.Fatalf("create credential-free backend: %v", err)
	}
	observation, err := stateStore.GetBackupBackendObservation(backend.ID, "", true)
	if err != nil {
		t.Fatalf("get credential-free observation: %v", err)
	}
	assertBackupBackendObservationRedacted(t, observation)
}

func TestPGGetBackupBackendObservationUsesOneRedactedSnapshotQuery(t *testing.T) {
	stateStore, mock := newBackupSchedulePGTestStore(t)
	now := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	backend := model.NormalizeBackupBackend(model.BackupBackend{
		ID: "backend-pg-1", TenantID: "tenant-1", Name: "postgres observer", Slug: "postgres-observer",
		Provider: model.DataBackendProviderS3, Bucket: "private-bucket", Region: "region-1",
		Endpoint: "https://s3.example.test", Prefix: "backups", Status: "active",
		Credentials:        model.DataBackendCredentials{AccessKeyID: "pg-access-key"},
		CredentialSecretID: "secret-pg-1", CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
	})
	secret := model.BackupBackendSecret{
		ID: "secret-pg-1", TenantID: backend.TenantID, BackendID: backend.ID,
		Ciphertext: "encrypted-secret-payload", KeyID: "encryption-key-1",
		CreatedAt: now.Add(-time.Hour), UpdatedAt: now, LastRotated: now,
	}
	query := backupBackendObservationSelectSQL() + ` WHERE (b.id = $1 OR b.name = $1 OR b.slug = $2) AND (b.tenant_id IS NULL OR b.tenant_id = $3)`
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(backend.ID, model.Slugify(backend.ID), backend.TenantID).
		WillReturnRows(backupBackendObservationRows(t, backend, &secret))

	observation, err := stateStore.GetBackupBackendObservation(backend.ID, backend.TenantID, false)
	if err != nil {
		t.Fatalf("get postgres observation: %v", err)
	}
	assertBackupBackendObservationRedacted(t, observation, "pg-access-key", secret.Ciphertext, secret.KeyID, secret.ID, backend.Bucket, backend.Endpoint)
	assertBackupSchedulePGExpectations(t, mock)
}

func assertBackupBackendObservationRedacted(t *testing.T, observation BackupBackendObservation, forbidden ...string) {
	t.Helper()
	if observation.BackendID == "" || !strings.HasPrefix(observation.Generation, "sha256:") || len(observation.Generation) != len("sha256:")+64 {
		t.Fatalf("invalid backend observation: %+v", observation)
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		t.Fatalf("marshal observation: %v", err)
	}
	for _, value := range forbidden {
		if value != "" && strings.Contains(string(encoded), value) {
			t.Fatalf("backend observation leaked %q: %s", value, encoded)
		}
	}
}

func backupBackendObservationRows(t *testing.T, backend model.BackupBackend, secret *model.BackupBackendSecret) *sqlmock.Rows {
	t.Helper()
	capabilities, err := json.Marshal(backend.Capabilities)
	if err != nil {
		t.Fatalf("marshal capabilities: %v", err)
	}
	credentials, err := json.Marshal(backend.Credentials)
	if err != nil {
		t.Fatalf("marshal credentials: %v", err)
	}
	values := []driver.Value{
		backend.ID, backend.TenantID, backend.Name, backend.Slug, backend.Provider, backend.Bucket,
		backend.Region, backend.Endpoint, backend.BaseURL, backend.Prefix, backend.Status,
		capabilities, credentials, backend.CredentialSecretID, backend.FugueManaged, backend.Billable,
		nil, backend.LastTestResult, backend.ErrorMessage, backend.CreatedAt, backend.UpdatedAt,
		nil, nil, nil, nil, nil, nil, nil, nil,
	}
	if secret != nil {
		values[21] = secret.ID
		values[22] = secret.TenantID
		values[23] = secret.BackendID
		values[24] = secret.Ciphertext
		values[25] = secret.KeyID
		values[26] = secret.CreatedAt
		values[27] = secret.UpdatedAt
		values[28] = secret.LastRotated
	}
	return sqlmock.NewRows(backupBackendObservationColumns()).AddRow(values...)
}

func backupBackendObservationColumns() []string {
	return []string{
		"id", "tenant_id", "name", "slug", "provider", "bucket", "region", "endpoint", "base_url",
		"prefix", "status", "capabilities_json", "credentials_json", "credential_secret_id",
		"fugue_managed", "billable", "last_tested_at", "last_test_result", "error_message", "created_at", "updated_at",
		"secret_id", "secret_tenant_id", "secret_backend_id", "ciphertext", "key_id",
		"secret_created_at", "secret_updated_at", "secret_last_rotated_at",
	}
}
