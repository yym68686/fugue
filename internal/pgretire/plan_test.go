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
	for _, scenario := range []string{"valid", "source-uid", "target-uid", "target-unready", "target-node", "source-node", "endpoint", "endpoints", "fencing", "claim-uid", "pv-binding", "extra-claim", "extra-pod", "pod-adopted", "pod-sidecar", "pod-ephemeral", "pod-native-sidecar", "job", "migration"} {
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
			case "pod-adopted":
				pgretire.Meta(objects[path("pods", "previous-1")])["ownerReferences"] = []any{map[string]any{"uid": "foreign", "kind": "Cluster", "apiVersion": "postgresql.cnpg.io/v1"}}
			case "pod-sidecar":
				pgretire.Object(objects[path("pods", "previous-1")]["spec"])["containers"] = []any{map[string]any{"name": "postgres"}, map[string]any{"name": "other"}}
			case "pod-ephemeral":
				pgretire.Object(objects[path("pods", "previous-1")]["spec"])["ephemeralContainers"] = []any{map[string]any{"name": "debugger"}}
			case "pod-native-sidecar":
				pgretire.Object(objects[path("pods", "previous-1")]["spec"])["initContainers"] = []any{map[string]any{"name": "sidecar", "restartPolicy": "Always"}}
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

func TestLabelCollidingStorageIsProtectedNotAdopted(t *testing.T) {
	app := model.App{ID: "client", TenantID: "tenant"}
	svc := model.BackingService{ID: "database", TenantID: app.TenantID, Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{ServiceName: "current", Instances: 1}}}
	for _, scenario := range []string{"unchanged", "claim-replaced", "pv-replaced", "claim-adopted", "claim-labels", "claim-annotations", "pv-policy", "pv-labels", "claim-missing", "pod-uses", "source-pod-uses", "job-uses"} {
		t.Run(scenario, func(t *testing.T) {
			objects := retiretest.New(app, svc)
			get, ctx := retiretest.Get(objects), context.Background()
			p, err := pgretire.Inspect(ctx, get, app, svc)
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Volumes) != 1 || len(p.ProtectedVolumes) != 1 || p.ProtectedVolumes[0].PVC != "unowned-archive" {
				t.Fatalf("source and protected inventories overlap: %+v", p)
			}
			claimPath := pgretire.Path(p.Namespace, "persistentvolumeclaims", "unowned-archive")
			claim, pv := objects[claimPath], objects["/api/v1/persistentvolumes/protected-pv"]
			volume := []any{map[string]any{"persistentVolumeClaim": map[string]any{"claimName": "unowned-archive"}}}
			switch scenario {
			case "claim-replaced":
				pgretire.Meta(claim)["uid"] = "replacement"
			case "pv-replaced":
				pgretire.Meta(pv)["uid"] = "replacement"
			case "claim-adopted":
				pgretire.Meta(claim)["ownerReferences"] = []any{map[string]any{"uid": "other-owner"}}
			case "claim-labels":
				pgretire.Meta(claim)["labels"] = map[string]any{}
			case "claim-annotations":
				pgretire.Meta(claim)["annotations"] = map[string]any{"new": "configuration"}
			case "pv-policy":
				pgretire.Object(pv["spec"])["persistentVolumeReclaimPolicy"] = "Retain"
			case "pv-labels":
				pgretire.Meta(pv)["labels"] = map[string]any{"new": "configuration"}
			case "claim-missing":
				delete(objects, claimPath)
			case "pod-uses":
				objects[pgretire.Path(p.Namespace, "pods", "reader")] = map[string]any{"metadata": map[string]any{}, "spec": map[string]any{"volumes": volume}}
			case "source-pod-uses":
				pgretire.Object(objects[pgretire.Path(p.Namespace, "pods", "previous-1")]["spec"])["volumes"] = volume
			case "job-uses":
				objects["/apis/batch/v1/namespaces/"+p.Namespace+"/jobs/reader"] = map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"volumes": volume}}}}
			}
			for _, progress := range []bool{false, true} {
				err = pgretire.Validate(ctx, get, p, progress)
				if (err == nil) != (scenario == "unchanged") {
					t.Fatalf("progress=%v: %v", progress, err)
				}
			}
		})
	}
	for _, scenario := range []string{"primary-unowned", "collision-foreign-owner", "collision-pv-owned", "collision-used"} {
		t.Run(scenario, func(t *testing.T) {
			objects := retiretest.New(app, svc)
			ns := "fg-tenant"
			switch scenario {
			case "primary-unowned":
				delete(pgretire.Meta(objects[pgretire.Path(ns, "persistentvolumeclaims", "previous-1")]), "ownerReferences")
			case "collision-foreign-owner":
				pgretire.Meta(objects[pgretire.Path(ns, "persistentvolumeclaims", "unowned-archive")])["ownerReferences"] = []any{map[string]any{"uid": "other-owner"}}
			case "collision-pv-owned":
				pgretire.Meta(objects["/api/v1/persistentvolumes/protected-pv"])["ownerReferences"] = []any{map[string]any{"uid": "other-owner"}}
			case "collision-used":
				objects[pgretire.Path(ns, "pods", "reader")] = map[string]any{"spec": map[string]any{"volumes": []any{map[string]any{"persistentVolumeClaim": map[string]any{"claimName": "unowned-archive"}}}}}
			}
			if _, err := pgretire.Inspect(context.Background(), retiretest.Get(objects), app, svc); err == nil {
				t.Fatal("unsafe storage accepted by preview")
			}
		})
	}
}
