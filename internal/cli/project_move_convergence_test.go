package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/model"
)

func TestProjectMovePhysicalNodeLocalizesDatabaseAndRerunsIdempotently(t *testing.T) {
	service := model.BackingService{ID: "service_db", TenantID: "tenant_test", ProjectID: "project_test", Name: "database", Type: "postgres", Provisioner: "managed", Status: "active", Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "source", Instances: 3}}, RuntimeStatus: &model.BackingServiceRuntimeStatus{Phase: "active", DesiredInstances: 3, ReadyInstances: 3}}
	localizations, verifications := 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var result any
		switch r.URL.Path {
		case "/v1/tenants":
			result = map[string]any{"tenants": []model.Tenant{{ID: "tenant_test", Name: "tenant", Slug: "tenant"}}}
		case "/v1/projects":
			result = map[string]any{"projects": []model.Project{{ID: "project_test", TenantID: "tenant_test", Name: "sample", Slug: "sample"}}}
		case "/v1/runtimes":
			result = map[string]any{"runtimes": []model.Runtime{{ID: "destination", Name: "node-b", Type: model.RuntimeTypeManagedOwned, Status: "active", ClusterNodeName: "node-b"}}}
		case "/v1/apps":
			result = map[string]any{"apps": []model.App{}}
		case "/v1/backing-services":
			result = map[string]any{"backing_services": []model.BackingService{service}}
		case "/v1/backing-services/service_db/migrate":
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["dry_run"] != true {
				t.Fatal("physical node move used switchover that leaves source replicas")
			}
			result = map[string]any{"backing_service": service, "dry_run": true}
		case "/v1/backing-services/service_db/localize":
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["target_node_name"] != "node-b" || body["target_runtime_id"] != "destination" {
				t.Fatalf("destination not pinned: %v", body)
			}
			localizations++
			service.Spec.Postgres.RuntimeID = "destination"
			service.Spec.Postgres.PrimaryNodeName = "node-b"
			service.Spec.Postgres.Instances = 1
			service.RuntimeStatus.DesiredInstances, service.RuntimeStatus.ReadyInstances = 1, 1
			result = map[string]any{"backing_service": service, "operation": model.Operation{ID: "op_move", Status: "completed"}}
		case "/v1/operations/op_move":
			result = map[string]any{"operation": model.Operation{ID: "op_move", Status: "completed"}}
		case "/v1/backing-services/service_db":
			verifications++
			result = map[string]any{"backing_service": service}
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(result)
	}))
	defer srv.Close()
	for i := 0; i < 2; i++ {
		var out, errOut bytes.Buffer
		if err := runWithStreams([]string{"--base-url", srv.URL, "--token", "fixture", "project", "move", "sample", "--to", "node-b"}, &out, &errOut); err != nil {
			t.Fatalf("run %d: %v (%s)", i, err, errOut.String())
		}
	}
	if localizations != 1 || verifications < 2 {
		t.Fatalf("localizations=%d verifications=%d", localizations, verifications)
	}
}

func TestProjectMoveServiceConvergenceRejectsIncompleteCutover(t *testing.T) {
	service := model.BackingService{Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "destination"}}, RuntimeStatus: &model.BackingServiceRuntimeStatus{Phase: model.ManagedPostgresRuntimePhaseActive, DesiredInstances: 1, ReadyInstances: 1}}
	if !projectMoveServiceConverged(service, "destination") {
		t.Fatal("healthy target rejected")
	}
	for _, scenario := range []string{"extra-replica", "no-primary", "wrong-runtime", "resuming", "missing-evidence"} {
		t.Run(scenario, func(t *testing.T) {
			copy := service
			pg, status := *service.Spec.Postgres, *service.RuntimeStatus
			copy.Spec.Postgres, copy.RuntimeStatus = &pg, &status
			switch scenario {
			case "extra-replica":
				status.ReadyInstances = 2
			case "no-primary":
				status.ReadyInstances = 0
			case "wrong-runtime":
				pg.RuntimeID = "source"
			case "resuming":
				status.Phase = "resuming"
			case "missing-evidence":
				copy.RuntimeStatus = nil
			}
			if projectMoveServiceConverged(copy, "destination") {
				t.Fatal("incomplete migration reported successful")
			}
		})
	}
}

func TestProjectMoveDoesNotSkipUncommittedAppPlacement(t *testing.T) {
	app := model.App{ID: "app", Spec: model.AppSpec{RuntimeID: "source"}}
	app.Status.CurrentRuntimeID = "destination"
	candidates, skipped := planProjectMove([]model.App{app}, "destination")
	if len(candidates) != 1 || len(skipped) != 0 {
		t.Fatal("observed placement skipped an uncommitted migration")
	}
	app.Spec.RuntimeID = "destination"
	candidates, skipped = planProjectMove([]model.App{app}, "destination")
	if len(candidates) != 0 || len(skipped) != 1 {
		t.Fatal("committed placement was not idempotent")
	}
}
