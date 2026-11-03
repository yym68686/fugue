package store

import (
	"context"
	"errors"
	"fugue/internal/model"
	"path/filepath"
	"testing"
	"time"
)

func TestDataGCReferencesFenceWritersAndPreserveExpiredAuthorization(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("tenant")
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "data"})
	if err != nil {
		t.Fatal(err)
	}
	expired := time.Now().Add(-24 * time.Hour)
	manifest := model.DataManifest{Entries: []model.DataManifestEntry{{Kind: "file", SHA256: "abcd", RelativePath: "file", AssetName: "asset", Size: 1}}}
	_, err = s.CreateDataTransfer(model.DataTransfer{WorkspaceID: w.ID, Direction: "upload", Status: "running", Manifest: manifest, ExpiresAt: &expired})
	if err != nil {
		t.Fatal(err)
	}
	refs, err := s.ListDataGCReferences(context.Background())
	if err != nil || len(refs) == 0 {
		t.Fatalf("expired authorization lost live references: %+v %v", refs, err)
	}
	acquired, err := s.WithAdvisoryLock(context.Background(), DataReferenceLock, func() error {
		_, e := s.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: w.ID, Version: "one", Manifest: manifest})
		if !errors.Is(e, ErrConflict) {
			t.Fatalf("writer bypassed GC fence: %v", e)
		}
		return nil
	})
	if !acquired || err != nil {
		t.Fatal(acquired, err)
	}
}
