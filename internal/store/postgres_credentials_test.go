package store

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestPostgresCredentialAssignmentPersistsAndRejectsStaleIntent(t *testing.T) {
	f := newManagedPostgresPlacementFixture(t, false)
	original := f.service
	next, err := f.store.AssignManagedPostgresCredentialSecret(original, "stable-credential")
	if err != nil {
		t.Fatal(err)
	}
	if next.Spec.Postgres.Password != original.Spec.Postgres.Password || next.Spec.Postgres.ServiceName != original.Spec.Postgres.ServiceName {
		t.Fatal("assignment changed database configuration")
	}
	if _, err = f.store.AssignManagedPostgresCredentialSecret(original, "stable-credential"); err != nil {
		t.Fatal("idempotent assignment", err)
	}
	if _, err = f.store.AssignManagedPostgresCredentialSecret(original, "another-credential"); !errors.Is(err, ErrConflict) {
		t.Fatal("changed identity was accepted", err)
	}
	omitted := *model.CloneAppPostgresSpec(next.Spec.Postgres)
	omitted.CredentialSecretName = ""
	updated, err := f.store.UpdateBackingServiceSpec(next.ID, model.BackingServiceSpec{Postgres: &omitted})
	if err != nil || updated.Spec.Postgres.CredentialSecretName != "stable-credential" {
		t.Fatal("ordinary update erased persisted identity", err)
	}
	stale := cloneBackingService(original)
	stale.Spec.Postgres.Password = "different"
	if _, err = f.store.AssignManagedPostgresCredentialSecret(stale, "stable-credential"); !errors.Is(err, ErrConflict) {
		t.Fatal("stale password intent accepted", err)
	}
	changed := *model.CloneAppPostgresSpec(next.Spec.Postgres)
	changed.CredentialSecretName = "foreign-credential"
	if _, err = f.store.UpdateBackingServiceSpec(next.ID, model.BackingServiceSpec{Postgres: &changed}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("caller changed server-owned identity", err)
	}
	if err := reconcileManagedPostgresRuntimeResources(&changed, nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("caller supplied new identity", err)
	}
}

func TestPostgresCredentialAssignmentLiveStore(t *testing.T) {
	address := os.Getenv("FUGUE_CREDENTIAL_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("requires disposable local credential-test database")
	}
	parsed, err := url.Parse(address)
	if err != nil || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost") || parsed.Path != "/fugue_credential_test" {
		t.Fatal("requires exact local test database")
	}
	s := New(filepath.Join(t.TempDir(), "state.json"), address)
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateEdgeInstanceFencing(context.Background(), address); err != nil {
		t.Fatal(err)
	}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("credential transaction")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "credential-test", "")
	if err != nil {
		t.Fatal(err)
	}
	service, err := s.CreateBackingService(tenant.ID, project.ID, "database", "", model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{Password: "unchanged-test-password", StorageSize: "1Gi"}})
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.AssignManagedPostgresCredentialSecret(service, "persisted-credential")
	if err != nil {
		t.Fatal(err)
	}
	stored, err := s.GetBackingService(service.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Postgres.CredentialSecretName != next.Spec.Postgres.CredentialSecretName || stored.Spec.Postgres.Password != service.Spec.Postgres.Password {
		t.Fatal("database intent was not preserved")
	}
	stale := cloneBackingService(service)
	stale.Spec.Postgres.Password = "stale-password"
	if _, err = s.AssignManagedPostgresCredentialSecret(stale, "persisted-credential"); !errors.Is(err, ErrConflict) {
		t.Fatal("stale SQL transaction accepted", err)
	}
	if _, err = s.AssignManagedPostgresCredentialSecret(service, "persisted-credential"); err != nil {
		t.Fatal("idempotent SQL retry failed", err)
	}
}
