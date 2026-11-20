package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestPostgresRuntimeRestoreSourceEvidence(t *testing.T) {
	f := newManagedPostgresResizeExecutorFixture(t)
	f.snapshot.Cluster.Status.SystemID = "current-system"
	o := f.snapshot.Observation
	target, err := managedPostgresLiveResourceSpec(o.DesiredResources)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"explicit", "legacy cpu", "legacy memory", "wrong cluster", "wrong system", "missing identity", "same pod", "wrong target"} {
		t.Run(name, func(t *testing.T) {
			const previousPodUID = "old-primary"
			prior := model.Operation{ID: "resize", RequestedByType: model.ActorTypeSystem}
			evidence := model.OperationEvidence{OperationID: prior.ID, Type: model.OperationEvidenceTypePostgresResizeVerified,
				SubjectNamespace: o.Namespace, SubjectUID: previousPodUID, PayloadVersion: managedPostgresResizeEvidencePayloadVersion,
				Payload: map[string]any{"cluster_name": f.snapshot.Cluster.Metadata.Name,
					"cluster_uid": f.snapshot.Cluster.Metadata.UID, "system_id": f.snapshot.Cluster.Status.SystemID,
					"pod_uid": previousPodUID, "resources": o.DesiredResources}}
			want := false
			switch name {
			case "explicit":
				want = true
			case "legacy cpu":
				delete(evidence.Payload, "system_id")
				prior.RequestedByID = managedPostgresAutoCPUActor + "/" + f.snapshot.Cluster.Metadata.UID + "/" + f.snapshot.Cluster.Status.SystemID
				want = true
			case "legacy memory":
				delete(evidence.Payload, "system_id")
				previous := f.snapshot
				previous.Observation.PodUID = previousPodUID
				evidence.Payload["container_started_at"] = previous.Observation.ContainerStartedAt
				evidence.Payload["restart_count"] = previous.Observation.RestartCount
				prior.RequestedByID = managedPostgresMemoryActor(previous, 2048)
				want = true
			case "wrong cluster":
				evidence.Payload["cluster_uid"] = "other-cluster"
			case "wrong system":
				evidence.Payload["system_id"] = "other-system"
			case "missing identity":
				delete(evidence.Payload, "system_id")
			case "same pod":
				evidence.SubjectUID, evidence.Payload["pod_uid"] = o.PodUID, o.PodUID
			case "wrong target":
				evidence.Payload["resources"] = kubeResourceRequirements{}
			}
			if got := verifiedPostgresRuntimeRestoreSource(prior, evidence, *target, f.snapshot); got != want {
				t.Fatalf("accepted=%v, want %v", got, want)
			}
		})
	}
}

func TestReconcilePostgresRuntimeRestoreQueuesOnlySafeVerifiedTarget(t *testing.T) {
	for _, name := range []string{"cpu request", "limit change", "foreign drift", "unproven", "downscale disabled", "memory without fresh evidence", "already current"} {
		t.Run(name, func(t *testing.T) {
			state := store.New(t.TempDir() + "/state.json")
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := state.CreateTenant("runtime restore")
			if err != nil {
				t.Fatal(err)
			}
			project, err := state.CreateProject(tenant.ID, "test", "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := state.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{CPUMilliCores: 2000, MemoryMebibytes: 4096, StorageGibibytes: 20}); err != nil {
				t.Fatal(err)
			}
			bootstrap := model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 1024, CPULimitMilliCores: 200, MemoryLimitMebibytes: 1536}
			app, err := state.CreateApp(tenant.ID, project.ID, "database-app", "", model.AppSpec{
				Image: "example/app:1", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID,
				Postgres: &model.AppPostgresSpec{Database: "app", User: "app", Password: "test", StorageSize: "1Gi", Resources: &bootstrap},
			})
			if err != nil {
				t.Fatal(err)
			}
			service := app.BackingServices[0]
			f := newManagedPostgresResizeExecutorFixture(t)
			ns, cluster := runtime.NamespaceForTenant(app.TenantID), service.Spec.Postgres.ServiceName
			f.snapshot.Cluster.Metadata.Name = cluster
			f.snapshot.Cluster.Metadata.Labels = map[string]string{runtime.FugueLabelBackingServiceID: service.ID}
			f.snapshot.Cluster.Spec.Resources = f.snapshot.Observation.DesiredResources
			f.snapshot.Cluster.Status.SystemID = "system-id"
			o := &f.snapshot.Observation
			o.Namespace = ns
			o.Labels[cloudNativePGClusterLabel] = cluster
			o.OwnerReferences[0].Name = cluster
			target := bootstrap
			target.CPUMilliCores = 25
			if name == "limit change" {
				target.CPULimitMilliCores = 150
			}
			if name == "memory without fresh evidence" {
				target.CPUMilliCores, target.MemoryMebibytes = bootstrap.CPUMilliCores, 768
			}
			resize, err := state.CreateOperation(model.Operation{
				TenantID: app.TenantID, AppID: app.ID, ServiceID: service.ID, Type: model.OperationTypeDatabaseResize,
				RequestedByType: model.ActorTypeSystem, RequestedByID: managedPostgresAutoCPUActor + "/cluster-uid/system-id",
				DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{RuntimeResources: &target}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := state.CompleteManagedOperation(resize.ID, "", "verified"); err != nil {
				t.Fatal(err)
			}
			app, err = state.GetApp(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			targetKube, err := managedPostgresKubeResources(target)
			if err != nil {
				t.Fatal(err)
			}
			if name != "unproven" {
				if _, err := state.RecordOperationEvidence(model.OperationEvidence{
					TenantID: tenant.ID, AppID: app.ID, OperationID: resize.ID, Type: model.OperationEvidenceTypePostgresResizeVerified,
					SubjectKind: "Pod", SubjectNamespace: ns, SubjectName: o.PodName, SubjectUID: "previous-pod",
					PayloadVersion: managedPostgresResizeEvidencePayloadVersion,
					Payload: map[string]any{"cluster_name": cluster, "cluster_uid": "cluster-uid", "system_id": "system-id",
						"pod_uid": "previous-pod", "resources": targetKube},
				}); err != nil {
					t.Fatal(err)
				}
			}
			switch name {
			case "foreign drift":
				o.DesiredResources = cloneKubeResourceRequirements(o.DesiredResources)
				o.DesiredResources.Requests["cpu"] = "90m"
				o.ActualResources = &o.DesiredResources
			case "already current":
				o.DesiredResources, o.ActualResources = targetKube, &targetKube
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("restore scheduler must not mutate Kubernetes: %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				switch r.URL.Path {
				case cloudNativePGClusterAPIPath(ns, cluster):
					_ = json.NewEncoder(w).Encode(f.snapshot.Cluster)
				case "/api/v1/namespaces/" + ns + "/pods/" + o.PodName:
					_ = json.NewEncoder(w).Encode(map[string]any{
						"metadata": map[string]any{"name": o.PodName, "namespace": ns, "uid": o.PodUID, "resourceVersion": o.ResourceVersion, "generation": o.Generation, "labels": o.Labels, "ownerReferences": o.OwnerReferences},
						"spec":     map[string]any{"nodeName": o.NodeName, "containers": []kubeResizeContainerSpec{{Name: "postgres", Resources: o.DesiredResources}}},
						"status": map[string]any{"phase": "Running", "observedGeneration": o.ObservedGeneration, "conditions": []map[string]string{{"type": "Ready", "status": "True"}},
							"containerStatuses": []map[string]any{{"name": "postgres", "ready": true, "restartCount": o.RestartCount, "resources": o.ActualResources, "state": map[string]any{"running": map[string]string{"startedAt": o.ContainerStartedAt}}}}},
					})
				case "/api/v1/nodes/" + o.NodeName:
					_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"conditions": []map[string]string{{"type": "Ready", "status": "True"}, {"type": "MemoryPressure", "status": "False"}}}})
				default:
					t.Errorf("unexpected read %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			c := &kubeClient{client: server.Client(), baseURL: server.URL}
			s := &Service{Store: state, Config: config.ControllerConfig{KubectlApply: true, ManagedPostgresInPlaceResize: config.ManagedPostgresInPlaceResizeConfig{
				Enabled: true, RecoveryEnabled: true, CPURequestDownscaleEnabled: name != "downscale disabled",
				CPULimitDownscaleEnabled: true, MemoryRequestDownscaleEnabled: true,
			}}}
			autoApp, queued := s.reconcilePostgresRuntimeResources(context.Background(), c, app)
			if queued != (name == "cpu request") || (len(autoApp.BackingServices) != 0) != (name == "already current") {
				t.Fatalf("queued=%v, eligible for a new recommendation=%d", queued, len(autoApp.BackingServices))
			}
			if queued {
				if _, duplicate := s.reconcilePostgresRuntimeResources(context.Background(), c, app); duplicate {
					t.Fatal("duplicate restoration queued")
				}
				ops, err := state.ListActiveOperations()
				if err != nil || len(ops) != 1 || ops[0].RequestedByID != postgresRuntimeRestoreActor(f.snapshot, resize.ID) ||
					*ops[0].DesiredSpec.Postgres.RuntimeResources != target {
					t.Fatalf("unexpected active restore: %+v, err=%v", ops, err)
				}
			}
		})
	}
}

func TestPostgresRuntimeRestoreLostResponseRequiresExactIntent(t *testing.T) {
	for _, change := range []string{"none", "different system", "different pod", "wrong generation", "different target"} {
		t.Run(change, func(t *testing.T) {
			f := newManagedPostgresResizeExecutorFixture(t)
			f.snapshot.Cluster.Status.SystemID = "system-id"
			o := &f.snapshot.Observation
			base, err := captureManagedPostgresResizeInvariantBaseline(o.Namespace, f.snapshot.Cluster.Metadata.Name, f.snapshot.Cluster, *o)
			if err != nil {
				t.Fatal(err)
			}
			target := cloneKubeResourceRequirements(o.DesiredResources)
			target.Requests["cpu"] = "25m"
			o.DesiredResources = cloneKubeResourceRequirements(target)
			o.Generation++
			ev := model.OperationEvidence{Payload: map[string]any{"system_id": "system-id", "resources": target}}
			switch change {
			case "different system":
				f.snapshot.Cluster.Status.SystemID = "other"
			case "different pod":
				o.PodUID = "replaced"
			case "wrong generation":
				o.Generation++
			case "different target":
				o.DesiredResources.Requests["cpu"] = "30m"
			}
			err = validatePostgresRestoreAcceptedIntent(&base, ev, f.snapshot, target)
			if (err == nil) != (change == "none") {
				t.Fatalf("unexpected lost-response recovery: %v", err)
			}
		})
	}
}
