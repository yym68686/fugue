package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func TestAlignedReleaseRevisionPostgresDiscovery(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateAppReleaseWorkload(context.Background(), os.Getenv("FUGUE_TEST_DATABASE_URL")); err != nil {
		t.Fatal(err)
	}
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "aligned", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "aligned", "", model.AppSpec{Image: "registry.example/app:v1", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	binding, _ := json.Marshal(model.AppReleaseWorkload{DeploymentName: "original", ServiceName: "original"})
	// Seed immutable historical provenance without manufacturing a running
	// operation; discovery must depend on facts, not executable snapshots.
	if _, err := s.db.Exec(`INSERT INTO fugue_app_releases(id,tenant_id,app_id,role,status,deployment_name,service_name,revision_workload_json,created_at,updated_at) VALUES($1,$2,$3,'stable','serving','canonical','canonical',$4,now(),now())`, model.NewID("aligned"), tenant.ID, app.ID, binding); err != nil {
		t.Fatal(err)
	}
	targets, err := s.ListAppReleaseRetirementTargets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, target := range targets {
		if target.AppID == app.ID && target.TenantID == tenant.ID {
			count++
		}
	}
	if count != 1 {
		t.Fatal("Postgres omitted aligned stable original workload", targets)
	}
	if _, err := s.db.Exec(`UPDATE fugue_app_releases SET service_name='original' WHERE app_id=$1`, app.ID); err != nil {
		t.Fatal(err)
	}
	targets, err = s.ListAppReleaseRetirementTargets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		if target.AppID == app.ID {
			t.Fatal("Postgres queued a release whose Service still uses the original workload")
		}
	}
}
