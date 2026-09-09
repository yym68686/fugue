package store

import (
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestReleaseMetadataPostgresPreservesFactsAndFullConfig(t *testing.T) {
	s := billingBatchPGStore(t)
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "release", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "release", "", model.AppSpec{Image: "registry.example/release:v1", RuntimeID: model.DefaultManagedRuntimeID, Ports: []int{8080}, Replicas: 1})
	if err != nil {
		t.Fatal(err)
	}
	release, err := s.CreateAppRelease(model.AppRelease{TenantID: tenant.ID, AppID: app.ID, Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusServing, ResolvedImageRef: app.Spec.Image, DeploymentName: "release", SpecSnapshot: &model.AppSpec{Env: map[string]string{"PRIVATE": "config"}}})
	if err != nil {
		t.Fatal(err)
	}
	filter := model.AppReleaseFilter{TenantID: tenant.ID, AppID: app.ID, ActiveOnly: true}
	full, err := s.ListAppReleases(filter)
	if err != nil || len(full) != 1 {
		t.Fatal(err)
	}
	metadata, err := s.ListAppReleaseMetadata(filter)
	if err != nil || len(metadata) != 1 {
		t.Fatal(err)
	}
	want := full[0]
	want.SpecSnapshot = nil
	if !reflect.DeepEqual(metadata[0], want) {
		t.Fatal("metadata changed release facts")
	}
	stored, err := s.GetAppRelease(tenant.ID, false, release.ID)
	if err != nil || stored.SpecSnapshot.Env["PRIVATE"] != "config" {
		t.Fatal("metadata reader altered rollback config")
	}
}
