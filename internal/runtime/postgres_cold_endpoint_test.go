package runtime

import (
	"fugue/internal/model"
	"testing"
)

func TestColdPostgresEndpointSurvivesEveryRender(t *testing.T) {
	pg := model.AppPostgresSpec{ServiceName: "restored-db", EndpointServiceName: "original-db", CredentialSecretName: "unchanged-secret", Database: "demo", User: "demo", Password: "fixture", StorageSize: "4Gi", StorageClassName: "cloneable"}
	service := model.BackingService{ID: "service_test", Name: "database", Type: model.BackingServiceTypePostgres, Provisioner: model.BackingServiceProvisionerManaged, Spec: model.BackingServiceSpec{Postgres: &pg}}
	app := model.App{ID: "app_test", Name: "api", TenantID: "tenant_test", Spec: model.AppSpec{Image: "example.invalid/app:v1", Replicas: 0, Env: map[string]string{"DATABASE_URL": "postgresql://custom:unchanged@original-db-rw:5432/demo?sslmode=require"}}, BackingServices: []model.BackingService{service}, Bindings: []model.ServiceBinding{{ServiceID: service.ID, AppID: "app_test", Alias: "postgres"}}}
	seen := false
	for _, obj := range BuildManagedAppChildObjects(app, SchedulingConstraints{}, nil) {
		if obj["kind"] != "Service" {
			continue
		}
		m := obj["metadata"].(map[string]any)
		if m["name"] != "original-db" {
			continue
		}
		seen = true
		selector := obj["spec"].(map[string]any)["selector"].(map[string]string)
		if selector["cnpg.io/cluster"] != "restored-db" || selector["cnpg.io/instanceRole"] != "primary" {
			t.Fatal(selector)
		}
	}
	if !seen {
		t.Fatal("stable Service omitted")
	}
	if defaultRuntimePostgresEnv(pg)["DB_HOST"] != "original-db" {
		t.Fatal("binding hostname changed")
	}
	if env := mergedRuntimeEnv(app); env["DATABASE_URL"] != "postgresql://custom:unchanged@original-db:5432/demo?sslmode=require" || env["DB_HOST"] != "original-db" {
		t.Fatal("rendered consumer environment does not follow the stable endpoint")
	}
	if app.Spec.Env["DATABASE_URL"] != "postgresql://custom:unchanged@original-db-rw:5432/demo?sslmode=require" {
		t.Fatal("rendering changed stored application intent")
	}
}
