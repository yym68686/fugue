package api

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestManagedAppStoreSnapshotMatchesPointReads(t *testing.T) {
	state := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := state.CreateTenant("snapshot tenant")
	if err != nil {
		t.Fatal(err)
	}
	project, err := state.CreateProject(tenant.ID, "snapshot project", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := state.CreateApp(tenant.ID, project.ID, "snapshot app", "", model.AppSpec{
		Image: "registry.example/fugue-apps/demo:current", Ports: []int{8080}, Replicas: 1,
		RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	release, err := state.CreateAppRelease(model.AppRelease{
		TenantID: tenant.ID, AppID: app.ID, Role: model.AppReleaseRoleStable,
		ResolvedImageRef: app.Spec.Image, DeploymentName: "stable-deployment", Status: model.AppReleaseStatusServing,
		SpecSnapshot: &model.AppSpec{Env: map[string]string{"SECRET": "not-needed-for-observation"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = state.UpsertAppTrafficPolicy(model.AppTrafficPolicy{
		TenantID: tenant.ID, AppID: app.ID, Mode: model.AppTrafficModeSingle,
		StableReleaseID: release.ID, StableWeight: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{store: state}
	snapshot, err := s.loadManagedAppStoreSnapshot(context.Background(), app.ID)
	if err != nil {
		t.Fatal(err)
	}
	want, wantFound := s.servingReleaseTrafficTarget(app)
	if want.SpecSnapshot == nil {
		t.Fatal("full release unexpectedly omitted config")
	}
	want.SpecSnapshot = nil
	got, gotFound := s.servingReleaseTrafficTargetWithSnapshot(app, snapshot)
	if !wantFound || gotFound != wantFound || !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot serving target differs from point read: got=%+v found=%t want=%+v found=%t", got, gotFound, want, wantFound)
	}
	// A complete snapshot remains internally stable during a concurrent policy
	// change; the next refresh must observe that change rather than retain it.
	_, err = state.UpsertAppTrafficPolicy(model.AppTrafficPolicy{
		TenantID: tenant.ID, AppID: app.ID, Mode: model.AppTrafficModeSingle,
		StableReleaseID: release.ID, StableWeight: 90, CandidateWeight: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, found := s.servingReleaseTrafficTargetWithSnapshot(app, snapshot); !found {
		t.Fatal("concurrent policy write mutated an already loaded snapshot")
	}
	fresh, err := s.loadManagedAppStoreSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := s.servingReleaseTrafficTargetWithSnapshot(app, fresh); found {
		t.Fatal("new snapshot reused an outdated serving policy")
	}
}

func TestManagedAppStoreSnapshotImageEvidenceRetainsIdentityAndPrecedence(t *testing.T) {
	app := model.App{ID: "app", TenantID: "tenant", Spec: model.AppSpec{Image: "registry.example/fugue-apps/demo:current"}}
	now := time.Now().UTC()
	base := model.ImageLocation{AppID: app.ID, TenantID: app.TenantID, RuntimeID: "target", ImageRef: app.Spec.Image, UpdatedAt: now}
	for _, test := range []struct {
		name    string
		rows    []model.ImageLocation
		present *bool
		status  string
	}{
		{name: "absence is unknown"},
		{name: "fresh missing", rows: []model.ImageLocation{{Status: model.ImageLocationStatusMissing}}, present: boolForSnapshotTest(false), status: model.ImageLocationStatusMissing},
		{name: "pending overrides negative", rows: []model.ImageLocation{{Status: model.ImageLocationStatusMissing}, {Status: model.ImageLocationStatusPulling}}, status: model.ImageLocationStatusPulling},
		{name: "present overrides pending", rows: []model.ImageLocation{{Status: model.ImageLocationStatusPulling}, {Status: model.ImageLocationStatusPresent}}, present: boolForSnapshotTest(true), status: model.ImageLocationStatusPresent},
		{name: "source runtime is not target evidence", rows: []model.ImageLocation{{Status: model.ImageLocationStatusPresent, RuntimeID: "source"}}},
		{name: "other tenant is not evidence", rows: []model.ImageLocation{{Status: model.ImageLocationStatusPresent, TenantID: "other"}}},
		{name: "expired evidence is unknown", rows: []model.ImageLocation{{Status: model.ImageLocationStatusPresent, UpdatedAt: now.Add(-24 * time.Hour)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows := make([]model.ImageLocation, 0, len(test.rows))
			for _, override := range test.rows {
				row := base
				row.Status = override.Status
				if override.RuntimeID != "" {
					row.RuntimeID = override.RuntimeID
				}
				if override.TenantID != "" {
					row.TenantID = override.TenantID
				}
				if !override.UpdatedAt.IsZero() {
					row.UpdatedAt = override.UpdatedAt
				}
				rows = append(rows, row)
			}
			snapshot := &managedAppStoreSnapshot{locations: map[string][]model.ImageLocation{app.ID: rows}}
			s := &Server{store: store.New(filepath.Join(t.TempDir(), "unused.json"))}
			present, observation, err := s.currentManagedImagePresenceWithStoreSnapshot(app, app.Spec.Image, "target", snapshot)
			if err != nil || !reflect.DeepEqual(present, test.present) || observation.status != test.status {
				t.Fatalf("got present=%v observation=%+v err=%v", present, observation, err)
			}
		})
	}
}

func boolForSnapshotTest(value bool) *bool { return &value }

func TestManagedAppStoreSnapshotLoadsNegativeAndPendingLocations(t *testing.T) {
	state := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	s := &Server{store: state}
	for _, status := range []string{model.ImageLocationStatusPresent, model.ImageLocationStatusPulling, model.ImageLocationStatusMissing, model.ImageLocationStatusFailed} {
		app := model.App{ID: "app_" + status, TenantID: "tenant", Spec: model.AppSpec{Image: "registry.example/demo:" + status}}
		_, err := state.UpsertImageLocation(model.ImageLocation{TenantID: app.TenantID, AppID: app.ID, ImageRef: app.Spec.Image, RuntimeID: "target", Status: status})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := s.loadManagedAppStoreSnapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		want, wantObservation, err := s.currentManagedImagePresenceWithObservation(app, app.Spec.Image, "target")
		if err != nil {
			t.Fatal(err)
		}
		got, gotObservation, err := s.currentManagedImagePresenceWithStoreSnapshot(app, app.Spec.Image, "target", snapshot)
		if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotObservation, wantObservation) {
			t.Fatalf("status %s changed in batch: got=%v %+v want=%v %+v err=%v", status, got, gotObservation, want, wantObservation, err)
		}
	}
}
