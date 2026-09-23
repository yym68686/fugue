package runtime

import (
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestPostgresCredentialIdentitySeparatesSameNamesAndSurvivesRename(t *testing.T) {
	build := func(id string) model.App {
		return model.App{ID: "app_" + id, TenantID: "tenant_demo", ProjectID: "project_" + id, Name: "same-name", BackingServices: []model.BackingService{{ID: "service_" + id, OwnerAppID: "app_" + id, Name: "same-name", Type: "postgres", Provisioner: "managed", Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{ServiceName: "database-" + id, User: "demo", Password: id}}}}, Bindings: []model.ServiceBinding{{ServiceID: "service_" + id}}}
	}
	secret := func(app model.App) string {
		objects := BuildManagedAppChildObjects(app, SchedulingConstraints{}, nil)
		name := ""
		for _, obj := range objects {
			if obj["kind"] == "Secret" {
				name = obj["metadata"].(map[string]any)["name"].(string)
			}
		}
		for _, obj := range objects {
			if obj["kind"] == "Cluster" {
				ref := obj["spec"].(map[string]any)["managed"].(map[string]any)["roles"].([]map[string]any)[0]["passwordSecret"].(map[string]any)["name"]
				if ref != name {
					t.Fatal("cluster references a different credential identity")
				}
			}
		}
		return name
	}
	a, b := build("first"), build("second")
	x, y := secret(a), secret(b)
	if x == "" || x == y || len(x) > 63 {
		t.Fatalf("invalid resource isolation: %q / %q", x, y)
	}
	a.Name = strings.Repeat("renamed", 30)
	a.BackingServices[0].Name = "renamed"
	if secret(a) != x {
		t.Fatal("display name changed credential identity")
	}
	a.BackingServices[0].Spec.Postgres.CredentialSecretName = "retained-legacy-credential"
	if secret(a) != "retained-legacy-credential" {
		t.Fatal("persisted reference was ignored")
	}
	a.ID = "consumer"
	for _, obj := range BuildManagedAppChildObjects(a, SchedulingConstraints{}, nil) {
		if obj["kind"] == "Cluster" || obj["kind"] == "Secret" {
			t.Fatal("bound consumer rendered provider credential resources")
		}
	}
}
