package store

import (
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestAppWorkloadIdentityPostgresPreservesAttributionAndTombstones(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "workloads", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "worker", "", model.AppSpec{Image: "registry.example/worker:v1", Replicas: 1, Ports: []int{8080}, RuntimeID: model.DefaultManagedRuntimeID})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, phase, currentRuntime string
		desired, current            int
	}{
		{"worker", "running", "observed-runtime", 1, 1},
		{"worker-deleted", "running", "observed-runtime", 1, 1},
		{"worker-deleted", "unknown", "", 0, 0},
		{"worker", "deleted", "", 1, 0},
		{"worker", "failed", "", 0, 0},
	} {
		spec := app.Spec
		spec.Replicas = test.desired
		spec.Env = map[string]string{"PRIVATE": "not needed for attribution"}
		spec.Files = []model.AppFile{{Path: "/config", Content: "not needed", Secret: true}}
		specJSON, _ := json.Marshal(spec)
		statusJSON, _ := json.Marshal(model.AppStatus{Phase: test.phase, CurrentReplicas: test.current, CurrentRuntimeID: test.currentRuntime})
		if _, err := s.db.Exec(`UPDATE fugue_apps SET name=$2,spec_json=$3,status_json=$4 WHERE id=$1`, app.ID, test.name, specJSON, statusJSON); err != nil {
			t.Fatal(err)
		}
		full, err := s.ListApps(tenant.ID, false)
		if err != nil {
			t.Fatal(err)
		}
		identities, _, err := s.ListAppWorkloadIdentitiesWithTiming(tenant.ID, false)
		if err != nil {
			t.Fatal(err)
		}
		type identity struct{ ID, TenantID, ProjectID, Name, DesiredRuntime, CurrentRuntime string }
		project := func(apps []model.App) []identity {
			result := []identity{}
			for _, a := range apps {
				result = append(result, identity{a.ID, a.TenantID, a.ProjectID, a.Name, a.Spec.RuntimeID, a.Status.CurrentRuntimeID})
			}
			return result
		}
		if !reflect.DeepEqual(project(full), project(identities)) {
			t.Fatalf("workload attribution changed for %+v", test)
		}
		for _, a := range identities {
			if len(a.Spec.Env) > 0 || len(a.Spec.Files) > 0 || a.Source != nil || len(a.BackingServices) > 0 {
				t.Fatal("identity included unrelated configuration")
			}
		}
	}
	foreign, _, err := s.ListAppWorkloadIdentitiesWithTiming("foreign", false)
	if err != nil || len(foreign) != 0 {
		t.Fatal("identity lookup crossed tenant boundary")
	}
}
