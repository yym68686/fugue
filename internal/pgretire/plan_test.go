package pgretire_test

import (
	"context"
	"encoding/json"
	"testing"

	"fugue/internal/model"
	"fugue/internal/pgretire"
	"fugue/internal/pgretire/retiretest"
)

func TestWitnessHashSurvivesJSONBObjectOrdering(t *testing.T) {
	p := model.PostgresSourceRetirementPlan{ServiceID: "database", SourceUID: "source", TargetUID: "target", PreserveStorage: true, Pods: []model.PostgresSourceRetirementPod{}, Volumes: []model.PostgresSourceRetirementVolume{{PVC: "data", PVCUID: "claim"}}}
	raw, _ := json.Marshal(p)
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	if pgretire.Hash(p) != pgretire.Hash(object) {
		t.Fatal("JSONB ordering changed durable witness hash")
	}
}

func TestRetirementWitnessRejectsChangedLiveState(t *testing.T) {
	app := model.App{ID: "client", TenantID: "tenant", ProjectID: "project"}
	svc := model.BackingService{ID: "database", TenantID: app.TenantID, ProjectID: app.ProjectID, Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{ServiceName: "current", Instances: 1}}}
	for _, scenario := range []string{"valid", "source-uid", "target-uid", "target-unready", "target-node", "source-node", "endpoint", "endpoints", "fencing", "claim-uid", "pv-binding", "extra-claim", "extra-pod", "job", "migration"} {
		t.Run(scenario, func(t *testing.T) {
			objects := retiretest.New(app, svc)
			get, ctx := retiretest.Get(objects), context.Background()
			p, err := pgretire.Inspect(ctx, get, app, svc)
			if err != nil {
				t.Fatal(err)
			}
			path := func(kind, name string) string { return pgretire.Path(p.Namespace, kind, name) }
			switch scenario {
			case "source-uid":
				pgretire.Meta(objects[path("clusters", p.SourceCluster)])["uid"] = "replacement"
			case "target-uid":
				pgretire.Meta(objects[path("clusters", p.TargetCluster)])["uid"] = "replacement"
			case "target-unready":
				objects[path("clusters", p.TargetCluster)]["status"] = map[string]any{}
			case "target-node":
				objects["/api/v1/nodes/node-b"]["status"] = map[string]any{}
			case "source-node":
				objects["/api/v1/nodes/node-a"]["status"] = map[string]any{}
			case "endpoint":
				pgretire.Object(objects[path("services", p.Endpoint)]["spec"])["selector"] = map[string]any{pgretire.ClusterLabel: p.SourceCluster}
			case "endpoints":
				objects[path("endpoints", p.Endpoint)]["subsets"] = []any{}
			case "fencing":
				pgretire.Meta(objects[path("clusters", p.SourceCluster)])["annotations"] = map[string]any{}
			case "claim-uid":
				pgretire.Meta(objects[path("persistentvolumeclaims", "previous-1")])["uid"] = "replacement"
			case "pv-binding":
				pgretire.Object(objects["/api/v1/persistentvolumes/source-pv"]["spec"])["claimRef"] = map[string]any{}
			case "extra-claim":
				objects[path("persistentvolumeclaims", "new")] = map[string]any{"metadata": map[string]any{"labels": map[string]any{pgretire.ClusterLabel: p.SourceCluster}}}
			case "extra-pod":
				objects[path("pods", "new")] = map[string]any{"metadata": map[string]any{}, "spec": map[string]any{"volumes": []any{map[string]any{"persistentVolumeClaim": map[string]any{"claimName": "previous-1"}}}}}
			case "job":
				objects["/apis/batch/v1/namespaces/"+p.Namespace+"/jobs/new"] = map[string]any{"metadata": map[string]any{"labels": map[string]any{pgretire.ClusterLabel: p.SourceCluster}}}
			case "migration":
				objects[path("configmaps", pgretire.MigrationName(svc.ID))]["data"] = map[string]any{}
			}
			err = pgretire.Validate(ctx, get, p, true)
			if scenario == "valid" && err != nil {
				t.Fatal(err)
			}
			if scenario != "valid" && err == nil {
				t.Fatal("changed live state accepted")
			}
		})
	}
}

func TestRetirementProgressOnlyAllowsExplicitRetainedStorage(t *testing.T) {
	app := model.App{ID: "client", TenantID: "tenant"}
	svc := model.BackingService{ID: "database", TenantID: app.TenantID, Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{ServiceName: "current", Instances: 1}}}
	objects := retiretest.New(app, svc)
	get, ctx := retiretest.Get(objects), context.Background()
	p, err := pgretire.Inspect(ctx, get, app, svc)
	if err != nil {
		t.Fatal(err)
	}
	delete(objects, pgretire.Path(p.Namespace, "clusters", p.SourceCluster))
	delete(objects, pgretire.Path(p.Namespace, "pods", "previous-1"))
	pvc := objects[pgretire.Path(p.Namespace, "persistentvolumeclaims", "previous-1")]
	delete(pgretire.Meta(pvc), "ownerReferences")
	delete(pgretire.Meta(pvc), "labels")
	if pgretire.Validate(ctx, get, p, true) == nil {
		t.Fatal("unwitnessed ownership removal accepted")
	}
	pgretire.Meta(pvc)["annotations"] = map[string]any{pgretire.RetainedUIDAnnotation: p.SourceUID, pgretire.RetainedPlanAnnotation: p.PlanHash}
	if err = pgretire.Validate(ctx, get, p, true); err != nil {
		t.Fatal(err)
	}
	if pgretire.Validate(ctx, get, p, false) == nil {
		t.Fatal("partial state accepted for fresh plan")
	}
}
