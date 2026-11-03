package store

import (
	"context"
	"errors"
	"fugue/internal/model"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStorageMaintenanceDurabilityCASAndDedup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := New(path)
	if e := s.Init(); e != nil {
		t.Fatal(e)
	}
	testMaintenanceStore(t, s)
	fresh := New(path)
	if e := fresh.Init(); e != nil {
		t.Fatal(e)
	}
	jobs, e := fresh.ListStorageMaintenanceJobs()
	if e != nil || len(jobs) != 1 || jobs[0].Status != "succeeded" {
		t.Fatalf("restart lost evidence: %+v %v", jobs, e)
	}
}
func testMaintenanceStore(t *testing.T, s *Store) {
	t.Helper()
	j, e := s.CreateStorageMaintenanceJob(model.StorageMaintenanceJob{Kind: "retention"})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.CreateStorageMaintenanceJob(model.StorageMaintenanceJob{Kind: "retention"}); !errors.Is(e, ErrConflict) {
		t.Fatalf("duplicate actionable job: %v", e)
	}
	stale := j.UpdatedAt
	j.Status = "succeeded"
	if e = s.SaveStorageMaintenanceJob(j, stale); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveStorageMaintenanceJob(j, stale); !errors.Is(e, ErrConflict) {
		t.Fatalf("stale worker overwrote result: %v", e)
	}
}
func TestStorageMaintenancePostgresIntegration(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable database required")
	}
	s := New("", dsn)
	if e := s.Init(); e != nil {
		t.Fatal(e)
	}
	defer s.db.Close()
	testMaintenanceStore(t, s)
	tenant, e := s.CreateTenant(model.NewID("tenant"))
	if e != nil {
		t.Fatal(e)
	}
	w, e := s.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "data"})
	if e != nil {
		t.Fatal(e)
	}
	manifest := model.DataManifest{Entries: []model.DataManifestEntry{{Kind: "file", SHA256: "abcd", Size: 1, AssetName: "asset", RelativePath: "file"}}}
	transfer, e := s.CreateDataTransfer(model.DataTransfer{WorkspaceID: w.ID, Direction: "upload", Status: "running", Manifest: manifest})
	if e != nil {
		t.Fatal(e)
	}
	if _, e := s.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: w.ID, Version: "v1", Manifest: manifest}); e != nil {
		t.Fatal(e)
	}
	refs, e := s.ListDataGCReferences(context.Background())
	if e != nil || len(refs) == 0 {
		t.Fatalf("references: %+v %v", refs, e)
	}
	stale := transfer
	transfer.Status = "canceled"
	if _, e = s.UpdateDataTransfer(transfer); e != nil {
		t.Fatal(e)
	}
	if _, e = s.UpdateDataTransfer(stale); !errors.Is(e, ErrConflict) {
		t.Fatalf("resurrected terminal transfer: %v", e)
	}
	if _, e = s.ListIdleDataTransferIDs(context.Background(), time.Now(), time.Now()); e != nil {
		t.Fatal(e)
	}
}
