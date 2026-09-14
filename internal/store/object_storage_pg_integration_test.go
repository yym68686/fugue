package store

import (
	"context"
	"errors"
	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestObjectStoragePostgresIntegration(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set disposable loopback FUGUE_TEST_DATABASE_URL")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "synthetic-object-storage-pg-key")
	s := New("", address)
	if err = s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	// Exercise the independent migration twice over the baseline schema.
	for i := 0; i < 2; i++ {
		if err = schemamigrate.MigrateObjectStorage(context.Background(), address); err != nil {
			t.Fatal(err)
		}
	}
	tenant, err := s.CreateTenant(model.NewID("s3test"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec("DELETE FROM fugue_tenants WHERE id=$1", tenant.ID)
	project, err := s.CreateProject(tenant.ID, "analytics", "")
	if err != nil {
		t.Fatal(err)
	}
	v := model.ObjectStore{ID: model.NewID("objects"), TenantID: tenant.ID, ProjectID: project.ID, Name: "events", Status: "active", CreatedAt: time.Now().UTC()}
	if err = s.SaveObjectStore(v); err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec("DELETE FROM fugue_object_stores WHERE id=$1", v.ID)
	secret, err := s.SealObjectStorageSecret(model.DataBackendCredentials{SecretAccessKey: "synthetic-secret"})
	if err != nil {
		t.Fatal(err)
	}
	rec := model.ObjectStorageCredentialRecord{Credential: model.ObjectStorageCredential{ID: model.NewID("grant"), StoreID: v.ID, Name: "reader", Status: "active"}, Secret: secret}
	if err = s.SaveObjectStorageCredential(rec); err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec("DELETE FROM fugue_object_storage_credentials WHERE id=$1", rec.Credential.ID)
	rows, err := s.ListObjectStores(tenant.ID, project.ID)
	if err != nil || len(rows) != 1 {
		t.Fatal("missing own store", err)
	}
	rows, err = s.ListObjectStores("another-tenant", project.ID)
	if err != nil || len(rows) != 0 {
		t.Fatal("tenant filter leaked store", err)
	}
	grants, err := s.ListObjectStorageCredentials(v.ID)
	if err != nil || len(grants) != 1 {
		t.Fatal("missing grant", err)
	}
	decoded, err := s.OpenObjectStorageSecret(grants[0].Secret)
	if err != nil || decoded.SecretAccessKey != "synthetic-secret" {
		t.Fatal("encrypted roundtrip failed", err)
	}
	duplicate := v
	duplicate.ID = model.NewID("duplicate")
	if err = s.SaveObjectStore(duplicate); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate name not rejected", err)
	}
	if _, err = s.DeleteTenant(tenant.ID); err == nil {
		t.Fatal("deleted owner while object store exists")
	}
}
