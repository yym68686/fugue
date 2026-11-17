package store

import (
	"context"
	"errors"
	"fugue/internal/model"
	"path/filepath"
	"testing"
	"time"
)

func retirementTestStore(t *testing.T) (*Store, model.DataWorkspace) {
	t.Helper()
	s := New(filepath.Join(t.TempDir(), "state.json"))
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

func TestWorkspaceRetirementExplicitOptionsAndReferenceFreeze(t *testing.T) {
	s, w := retirementTestStore(t)
	m := model.DataManifest{Entries: []model.DataManifestEntry{{Kind: "file", SHA256: "abcd", Size: 1}}}
	_, err := s.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: w.ID, Version: "one", Manifest: m})
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.CreateDataTransfer(model.DataTransfer{WorkspaceID: w.ID, Direction: "upload", Manifest: m})
	if err != nil {
		t.Fatal(err)
	}
	g, _, err := s.CreateDataGrant(model.DataGrant{WorkspaceID: w.ID, Mode: "read-only"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{}, "api-key", "fixture"); !errors.Is(err, ErrConflict) {
		t.Fatalf("unapproved cleanup: %v", err)
	}
	opt := model.DataWorkspaceRetirementOptions{CancelPlannedTransfers: true, RevokeGrants: true}
	j, err := s.BeginDataWorkspaceRetirement(w, opt, "api-key", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.BeginDataWorkspaceRetirement(w, opt, "api-key", "fixture")
	if err != nil || j.ID != again.ID {
		t.Fatal("not idempotent", again, err)
	}
	tr, err = s.GetDataTransfer(tr.ID)
	if err != nil || tr.Status != "canceled" {
		t.Fatal(tr, err)
	}
	g, err = s.GetDataGrant(g.ID)
	if err != nil || g.Status != "revoked" {
		t.Fatal(g, err)
	}
	if _, err = s.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: w.ID, Version: "two", Manifest: m}); !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatalf("snapshot bypassed freeze: %v", err)
	}
	if _, err = s.CreateDataTransfer(model.DataTransfer{WorkspaceID: w.ID, Direction: "upload"}); !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatalf("transfer bypassed freeze: %v", err)
	}
	if _, _, err = s.CreateDataGrant(model.DataGrant{WorkspaceID: w.ID, Mode: "read-only"}); !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatalf("grant bypassed freeze: %v", err)
	}
	if _, err = s.UpdateDataWorkspace(w.ID, w.TenantID, false, DataWorkspaceUpdate{}); !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatalf("backend mutation bypassed freeze: %v", err)
	}
	if _, err = s.DeleteDataWorkspace(w.ID, w.TenantID, false); !errors.Is(err, ErrConflict) {
		t.Fatalf("metadata disappeared before reclamation: %v", err)
	}
	if err = s.withLockedState(false, func(st *model.State) error {
		return validateDataReferencesState(st, w.TenantID, &model.AppSpec{Data: &model.AppDataMaterializationSpec{Workspaces: []model.AppDataWorkspaceMaterialization{{WorkspaceID: w.ID}}}})
	}); !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatalf("app reference bypassed freeze: %v", err)
	}
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

func TestWorkspaceRetirementProtectsRegisteredBackupReferences(t *testing.T) {
	s, w := retirementTestStore(t)
	target := model.BackupTarget{Type: model.BackupTargetDataWorkspace, WorkspaceID: w.ID, TenantID: w.TenantID}
	p, err := s.UpsertBackupPolicy(model.BackupPolicy{TenantID: w.TenantID, Name: "dataset-policy", Target: target, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{}, "api-key", "fixture"); !errors.Is(err, ErrConflict) {
		t.Fatal("enabled backup policy ignored", err)
	}
	if _, err = s.SetBackupPolicyEnabled(p.ID, w.TenantID, false, false, "retire"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateBackupArtifact(model.BackupArtifact{TenantID: w.TenantID, Target: target, Kind: model.BackupArtifactKindDataSnapshot, Status: "active"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{}, "api-key", "fixture"); !errors.Is(err, ErrConflict) {
		t.Fatal("reference-only backup ignored", err)
	}
	if err = s.withLockedState(true, func(st *model.State) error {
		for i := range st.BackupArtifacts {
			if st.BackupArtifacts[i].Target.WorkspaceID == w.ID {
				st.BackupArtifacts[i].Status = "deleted"
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginDataWorkspaceRetirement(w, model.DataWorkspaceRetirementOptions{}, "api-key", "fixture"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetBackupPolicyEnabled(p.ID, w.TenantID, false, true, ""); !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatal("backup re-enabled after freeze", err)
	}
	if _, err = s.CreateBackupArtifact(model.BackupArtifact{TenantID: w.TenantID, Target: target, Kind: model.BackupArtifactKindDataSnapshot, Status: "active"}); !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatal("artifact bypassed freeze", err)
	}
	if err = s.checkDataBackupTargetActive(target, "different-tenant"); !errors.Is(err, ErrDataWorkspaceRetiring) {
		t.Fatal("legacy target tenant mismatch bypassed exact-ID freeze", err)
	}
}
