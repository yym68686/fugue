package store

import (
	"errors"
	"fugue/internal/model"
	"testing"
	"time"
)

func TestDataDeletionProtectsReferences(t *testing.T) {
	ws := model.DataWorkspace{ID: "data_ws_test", TenantID: "tenant_test", Name: "training", Slug: "training"}
	snapshot := model.DataSnapshot{ID: "snapshot_test", WorkspaceID: ws.ID, Version: "v1"}
	future := time.Now().Add(time.Hour)
	for name, state := range map[string]model.State{
		"transfer":      {DataTransfers: []model.DataTransfer{{WorkspaceID: ws.ID, Status: "running"}}},
		"grant":         {DataGrants: []model.DataGrant{{WorkspaceID: ws.ID, SnapshotID: snapshot.ID, Status: "active", ExpiresAt: &future}}},
		"app":           {Apps: []model.App{{TenantID: ws.TenantID, Spec: model.AppSpec{Data: &model.AppDataMaterializationSpec{Workspaces: []model.AppDataWorkspaceMaterialization{{WorkspaceID: ws.ID, Version: "v1"}}}}}}},
		"queued intent": {Operations: []model.Operation{{TenantID: ws.TenantID, Status: "pending", DesiredSpec: &model.AppSpec{Data: &model.AppDataMaterializationSpec{Workspaces: []model.AppDataWorkspaceMaterialization{{Workspace: ws.Name, Version: "latest"}}}}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := guardDataDeletion(&state, ws, &snapshot); !errors.Is(err, ErrConflict) {
				t.Fatalf("wanted conflict: %v", err)
			}
		})
	}
	state := model.State{DataSnapshots: []model.DataSnapshot{snapshot}}
	if err := guardDataDeletion(&state, ws, nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("workspace retained snapshots: %v", err)
	}
	if err := guardDataDeletion(&state, ws, &snapshot); err != nil {
		t.Fatal(err)
	}
}
func TestDataDeletionIgnoresUnrelatedAndExpiredReferences(t *testing.T) {
	ws := model.DataWorkspace{ID: "data_ws_test", TenantID: "tenant_test", Name: "shared-name"}
	snapshot := model.DataSnapshot{ID: "snapshot_test", Version: "v1"}
	past := time.Now().Add(-time.Hour)
	state := model.State{DataGrants: []model.DataGrant{{WorkspaceID: ws.ID, Status: "active", ExpiresAt: &past}}, DataTransfers: []model.DataTransfer{{WorkspaceID: ws.ID, Status: "completed"}}, Apps: []model.App{{TenantID: "tenant_other", Spec: model.AppSpec{Data: &model.AppDataMaterializationSpec{Workspaces: []model.AppDataWorkspaceMaterialization{{Workspace: ws.Name}}}}}}}
	if err := guardDataDeletion(&state, ws, &snapshot); err != nil {
		t.Fatal(err)
	}
}
