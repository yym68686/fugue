package store

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fugue/internal/appimages"
	"fugue/internal/imageretention"
	"fugue/internal/model"
)

func TestMaintenanceOperationsPreserveImageProtectionWithoutInlineFiles(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	app := model.App{ID: "app-a", TenantID: "tenant-a", Spec: model.AppSpec{Image: "registry.example/team/app:live"}}
	ops := []model.Operation{
		{ID: "historical", TenantID: app.TenantID, AppID: app.ID, Status: model.OperationStatusCompleted, CreatedAt: now.Add(-24 * time.Hour), CompletedAt: &now,
			DesiredSpec:   &model.AppSpec{Image: "registry.example/team/app:old", Files: []model.AppFile{{Path: "/config", Content: "large private configuration"}}},
			DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/team/app:old"}},
		{ID: "active", TenantID: app.TenantID, AppID: app.ID, Status: model.OperationStatusWaitingAgent, CreatedAt: now},
		{ID: "foreign", TenantID: "tenant-b", AppID: "app-b", Status: model.OperationStatusRunning, CreatedAt: now},
	}
	if err := s.withLockedState(true, func(state *model.State) error { state.Operations = ops; return nil }); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListImageRetentionOperations(app.TenantID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "historical" || got[1].ID != "active" || len(got[0].DesiredSpec.Files) != 0 {
		t.Fatalf("unexpected projection: %+v", got)
	}
	wantRefs := appimages.ManagedImageRefs(app, ops[:2], "registry.example", "registry.example")
	if refs := appimages.ManagedImageRefs(app, got, "registry.example", "registry.example"); !reflect.DeepEqual(refs, wantRefs) {
		t.Fatalf("image references changed: %v != %v", refs, wantRefs)
	}
	images := []model.Image{{ID: "image-active", AppID: app.ID, SourceOperationID: "active"}, {ID: "image-old", AppID: app.ID, SourceOperationID: "historical"}}
	if plan := imageretention.Plan(app, images, got, nil, nil, now); !reflect.DeepEqual(plan, imageretention.Plan(app, images, ops[:2], nil, nil, now)) {
		t.Fatal("retention decisions changed")
	}
	active, err := s.ListActiveOperationLifecycles()
	if err != nil || len(active) != 2 {
		t.Fatalf("active summaries: %v %v", active, err)
	}
	for _, op := range active {
		if op.DesiredSpec != nil || op.DesiredSource != nil {
			t.Fatal("active summary leaked config")
		}
	}
	full, err := s.ListOperations(app.TenantID, false)
	if err != nil || len(full[0].DesiredSpec.Files) != 1 {
		t.Fatal("projection mutated stored configuration")
	}
}
