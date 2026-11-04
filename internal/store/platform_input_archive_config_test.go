package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestManagedArchiveConfigurationLoadsSharedReaderAndWriterWithoutEnvironment(t *testing.T) {
	s := archiveTestStore(t)
	// This bounded store fixture deliberately has no tenant catalog. Install
	// only the independent singleton configuration DDL from the real migration.
	if _, err := s.db.ExecContext(t.Context(), strings.Split(schemamigrate.ObjectStorageSQL, "CREATE TABLE IF NOT EXISTS fugue_object_stores")[0]); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "synthetic-managed-archive-test-key")
	old, oldErr := s.GetObjectStorageConfig()
	t.Cleanup(func() {
		if oldErr == nil {
			_ = s.SaveObjectStorageConfig(old)
		} else {
			_, _ = s.db.Exec(`DELETE FROM fugue_object_storage_config WHERE id='platform'`)
		}
	})
	reader, err := s.SealObjectStorageSecret(model.DataBackendCredentials{AccessKeyID: "reader-key", SecretAccessKey: "reader-secret"})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := s.SealObjectStorageSecret(model.DataBackendCredentials{AccessKeyID: "writer-key", SecretAccessKey: "writer-secret"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	account := strings.Repeat("a", 32)
	config := model.ObjectStorageConfig{AccountID: account, CompilerInputArchive: &model.CompilerInputArchiveBackend{ID: "platform_archive_fixture", AccountID: account, Bucket: "isolated-archive", Prefix: "inputs", State: "provisioning", Reader: reader, Writer: writer, ReaderConfigured: true, WriterConfigured: true, ProtectionVerifiedAt: &now, RetentionPolicySHA256: strings.Repeat("b", 64)}}
	if err = s.SaveObjectStorageConfig(config); err != nil {
		t.Fatal(err)
	}
	if _, configured, err := resolveCompilerInputArchiveConfig(t.Context(), s.db, false); err != nil || configured {
		t.Fatal("provisioning backend activated", err)
	}
	// A process started before platform activation already holds reader leases.
	if err = s.ConfigureCompilerInputArchiveFromEnv(); err != nil || !s.compilerInputArchiveReadLeaseEnabled {
		t.Fatal("reader barrier disabled before activation", err)
	}
	config.CompilerInputArchive.State = "active"
	if err = s.SaveObjectStorageConfig(config); err != nil {
		t.Fatal(err)
	}
	r, configured, err := resolveCompilerInputArchiveConfig(t.Context(), s.db, false)
	if err != nil || !configured || r.AccessKeyID != "reader-key" || r.SecretAccessKey != "reader-secret" {
		t.Fatal("shared read configuration unavailable", err)
	}
	w, configured, err := resolveCompilerInputArchiveConfig(t.Context(), s.db, true)
	if err != nil || !configured || w.AccessKeyID != "writer-key" {
		t.Fatal("writer role unavailable", err)
	}
	rh, _ := r.Fingerprint()
	wh, _ := w.Fingerprint()
	if rh != wh {
		t.Fatal("role changes physical backend binding")
	}
	tx, finish, err := s.BeginCompilerInputReadLease(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	r, configured, err = resolveCompilerInputArchiveConfig(t.Context(), tx, false)
	if err != nil || !configured || r.AccessKeyID != "reader-key" {
		t.Fatal("transaction-bound resolver failed", err)
	}
}
