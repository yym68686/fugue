package store

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestAppReleaseWorkloadBinding(t *testing.T) {
	s, _, _, app := newAppImageTrackingTestStore(t)
	testAppReleaseWorkloadBinding(t, s, app)
}

func TestAppReleaseWorkloadBindingPostgres(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateAppReleaseWorkload(context.Background(), os.Getenv("FUGUE_TEST_DATABASE_URL")); err != nil {
		t.Fatal(err)
	}
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "revision", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "revision", "", model.AppSpec{Image: "registry.example/app:v1", Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	testAppReleaseWorkloadBinding(t, s, app)
}

func testAppReleaseWorkloadBinding(t *testing.T, s *Store, app model.App) {
	t.Helper()
	op, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec, ExecutionMode: model.ExecutionModeManaged})
	if err != nil {
		t.Fatal(err)
	}
	op, claimed, err := s.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatalf("claim: %v %t", err, claimed)
	}
	release, err := s.CreateAppRelease(model.AppRelease{TenantID: app.TenantID, AppID: app.ID, Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusCreating, RuntimeID: "runtime", ResolvedImageRef: app.Spec.Image, DeploymentName: "revision", ServiceName: "revision", SpecSnapshot: &app.Spec})
	if err != nil {
		t.Fatal(err)
	}
	workload := model.AppReleaseWorkload{OperationID: op.ID, Namespace: "tenant", DeploymentName: "revision", DeploymentUID: "deployment-uid", DeploymentGeneration: 1, ServiceName: "revision", ServiceUID: "service-uid", ReleaseKey: "key", RuntimeID: "runtime", ImageRef: app.Spec.Image}
	for _, field := range []string{"operation", "image", "target", "owner", "stale"} {
		expected, w := release, workload
		switch field {
		case "operation":
			w.OperationID = "missing"
		case "image":
			w.ImageRef = "wrong"
		case "target":
			w.DeploymentName = "canonical"
		case "owner":
			expected.TenantID = "other"
		case "stale":
			expected.UpdatedAt = expected.UpdatedAt.Add(-1)
		}
		if _, err := s.BindAppReleaseWorkload(context.Background(), expected, w); err == nil {
			t.Fatal("invalid bind accepted", field)
		}
	}
	injected := release
	injected.RevisionWorkload = &workload
	if _, err := s.UpdateAppRelease(injected); err == nil {
		t.Fatal("ordinary update bound a workload")
	}
	if _, err := s.CreateAppRelease(injected); err == nil {
		t.Fatal("ordinary create bound a workload")
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, uid := range []string{"first", "second"} {
		wg.Add(1)
		go func(uid string) {
			defer wg.Done()
			w := workload
			w.DeploymentUID = uid
			_, err := s.BindAppReleaseWorkload(context.Background(), release, w)
			results <- err
		}(uid)
	}
	wg.Wait()
	close(results)
	wins, conflicts := 0, 0
	for err := range results {
		if err == nil {
			wins++
		} else if errors.Is(err, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("concurrent bind: wins=%d conflicts=%d", wins, conflicts)
	}
	bound, err := s.GetAppRelease(app.TenantID, false, release.ID)
	if err != nil || bound.RevisionWorkload == nil {
		t.Fatal("binding missing", err)
	}
	identity := *bound.RevisionWorkload
	w := identity
	w.BoundAt = workload.BoundAt
	if repeated, err := s.BindAppReleaseWorkload(context.Background(), release, w); err != nil || !reflect.DeepEqual(repeated.RevisionWorkload, bound.RevisionWorkload) {
		t.Fatal("idempotent bind failed", err)
	}
	// A stale writer that predates the binding still updates routing/status.
	release.DeploymentName = "canonical"
	release.ServiceName = "canonical"
	release.Status = model.AppReleaseStatusServing
	release.Role = model.AppReleaseRoleStable
	updated, err := s.UpdateAppRelease(release)
	if err != nil || !reflect.DeepEqual(updated.RevisionWorkload, &identity) {
		t.Fatal("canonical update lost identity", err)
	}
	bad := updated
	copy := identity
	copy.DeploymentUID = "replacement"
	bad.RevisionWorkload = &copy
	if _, err := s.UpdateAppRelease(bad); err == nil {
		t.Fatal("binding replaced by status update")
	}
	bad = updated
	bad.TenantID = "other"
	if _, err := s.UpdateAppRelease(bad); err == nil {
		t.Fatal("binding moved to another tenant")
	}
	metadata, err := s.ListAppReleaseMetadata(model.AppReleaseFilter{TenantID: app.TenantID, AppID: app.ID})
	if err != nil || len(metadata) != 1 || metadata[0].SpecSnapshot != nil || !reflect.DeepEqual(metadata[0].RevisionWorkload, &identity) {
		t.Fatal("metadata projection lost binding", err)
	}
	full, err := s.GetAppRelease(app.TenantID, false, release.ID)
	if err != nil || full.SpecSnapshot == nil {
		t.Fatal("metadata read lost executable snapshot", err)
	}
}
