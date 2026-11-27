package store

import (
	"context"
	"errors"
	"fugue/internal/model"
	"testing"
	"time"
)

func retirementTestStore(t *testing.T) (*Store, model.DataWorkspace) {
	t.Helper()
	s := newStoreTestDatabase(t, "")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("retirement-fixture")
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.CreateDataWorkspace(model.DataWorkspace{TenantID: tenant.ID, Name: "dataset"})
	if err != nil {
		t.Fatal(err)
	}
	return s, w
}

func TestWorkspaceRetirementWaitsForObservedCacheCleanupAndReleaseClock(t *testing.T) {
	s, w := retirementTestStore(t)
	_, err := s.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: w.ID, Version: "one"})
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.CreateDataTransfer(model.DataTransfer{WorkspaceID: w.ID, Direction: "prewarm", Status: "completed", Cache: &model.DataPrewarmCache{State: "ready"}})
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{EvictCaches: true}, "api-key", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReleaseDataWorkspaceRetirement(j); !errors.Is(err, ErrConflict) {
		t.Fatalf("cache cleanup not awaited: %v", err)
	}
	if j.WorkspaceRetirement.Phase != "evicting" || j.WorkspaceRetirement.ReclaimAfter != nil {
		t.Fatal("failed release changed caller's durable phase")
	}
	tr, err = s.GetDataTransfer(tr.ID)
	if err != nil {
		t.Fatal(err)
	}
	tr.Cache.State = "removed"
	if _, err = s.UpdateDataTransfer(tr); err != nil {
		t.Fatalf("cleanup completion blocked by freeze: %v", err)
	}
	start := time.Now()
	j, err = s.ReleaseDataWorkspaceRetirement(j)
	if err != nil {
		t.Fatal(err)
	}
	if j.WorkspaceRetirement.ReclaimAfter == nil || j.WorkspaceRetirement.ReclaimAfter.Before(start.Add(DataRetirementGrace)) {
		t.Fatal("grace did not start at reference release")
	}
	if err = s.FinalizeDataWorkspaceRetirement(j); !errors.Is(err, ErrConflict) {
		t.Fatalf("early finalization: %v", err)
	}
	snaps, err := s.ListDataSnapshots(w.ID, false)
	if err != nil || len(snaps) != 0 {
		t.Fatal(snaps, err)
	}
	stored, err := s.GetStorageMaintenanceJob(j.ID)
	if err != nil || stored.WorkspaceRetirement.ReleasedAt == nil {
		t.Fatal("release and clock not durable", stored, err)
	}
}

func TestWorkspaceRetirementRunningTransfersRemainBlockers(t *testing.T) {
	s, w := retirementTestStore(t)
	_, err := s.CreateDataTransfer(model.DataTransfer{WorkspaceID: w.ID, Direction: "upload", Status: "running"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{CancelPlannedTransfers: true, RevokeGrants: true, EvictCaches: true}, "api-key", "fixture")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("running upload canceled implicitly: %v", err)
	}
}

func TestWorkspaceRetirementPreservesReferencesDuringGrace(t *testing.T) {
	s, w := retirementTestStore(t)
	m := model.DataManifest{Entries: []model.DataManifestEntry{{Kind: "file", SHA256: "abcd", Size: 1}}}
	_, err := s.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: w.ID, Version: "one", Manifest: m})
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{}, "api-key", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	j, err = s.ReleaseDataWorkspaceRetirement(j)
	if err != nil {
		t.Fatal(err)
	}
	refs, err := s.ListDataGCReferences(context.Background())
	if err != nil || len(refs) == 0 {
		t.Fatal("global GC lost retiring tombstone", refs, err)
	}
}
