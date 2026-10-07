package controller

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestDisabledMigrationCutoverRequiresQuiescentTarget(t *testing.T) {
	for _, test := range []struct {
		name      string
		violation string
	}{
		{name: "disabled"},
		{name: "completed_pod"},
		{name: "running_pod", violation: "disabled_app_has_active_pods"},
		{name: "pending_pod", violation: "disabled_app_has_active_pods"},
		{name: "terminating_pod", violation: "disabled_app_has_active_pods"},
		{name: "pod_read_failure", violation: "verify disabled app has no active pods"},
		{name: "ready_endpoint", violation: "disabled_app_has_ready_endpoint"},
		{name: "managed_not_disabled", violation: "managed_app_unready"},
		{name: "managed_not_zero", violation: "disabled_managed_app_not_zero"},
		{name: "deployment_not_zero", violation: "disabled_deployment_not_zero"},
		{name: "replicas_not_observed", violation: "disabled_deployment_not_zero"},
		{name: "wrong_runtime", violation: "managed_app_runtime_mismatch"},
		{name: "stale_generation", violation: "generation_not_observed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := store.New(filepath.Join(t.TempDir(), "store.json"))
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := state.CreateTenant("disabled-migration")
			if err != nil {
				t.Fatal(err)
			}
			project, err := state.CreateProject(tenant.ID, "project", "")
			if err != nil {
				t.Fatal(err)
			}
			app, err := state.CreateApp(tenant.ID, project.ID, "disabled", "", model.AppSpec{Image: "registry.example/app:v1", Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID})
			if err != nil {
				t.Fatal(err)
			}
			target, _, err := state.CreateRuntime(tenant.ID, "target", model.RuntimeTypeExternalOwned, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			desired := app.Spec
			desired.Replicas = 0
			desired.RuntimeID = target.ID
			op, err := state.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeMigrate, AppID: app.ID, TargetRuntimeID: target.ID, DesiredSpec: &desired})
			if err != nil {
				t.Fatal(err)
			}
			app.Spec = desired
			managed := runtime.ManagedAppObject{}
			managed.Metadata.Generation = 2
			managed.Spec.AppID = app.ID
			managed.Spec.AppSpec = desired
			managed.Status.ObservedGeneration = 2
			managed.Status.Phase = runtime.ManagedAppPhaseDisabled
			deployment := kubeDeployment{}
			zero := 0
			deployment.Spec.Replicas = &zero
			deployment.Metadata.Generation = 3
			deployment.Status.ObservedGeneration = 3
			pod := kubePod{}
			pod.Status.Phase = "Succeeded"
			switch test.name {
			case "running_pod":
				pod.Status.Phase = "Running"
			case "pending_pod":
				pod.Status.Phase = "Pending"
			case "terminating_pod":
				pod.Metadata.DeletionTimestamp = "2026-01-01T00:00:00Z"
			case "managed_not_disabled":
				managed.Status.Phase = runtime.ManagedAppPhaseReady
			case "managed_not_zero":
				managed.Spec.AppSpec.Replicas = 1
			case "deployment_not_zero":
				one := 1
				deployment.Spec.Replicas = &one
			case "replicas_not_observed":
				deployment.Status.Replicas = 1
			case "wrong_runtime":
				managed.Spec.AppSpec.RuntimeID = "other"
			case "stale_generation":
				managed.Status.ObservedGeneration = 1
			}
			namespace := runtime.NamespaceForTenant(app.TenantID)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("cutover verification mutated Kubernetes: %s", r.Method)
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v1/namespaces/kube-system":
					_, _ = w.Write([]byte(`{"metadata":{"uid":"cluster"}}`))
				case "/api/v1/namespaces/" + namespace, "/api/v1/namespaces/" + namespace + "/services/" + runtime.RuntimeAppServiceName(app):
					_, _ = w.Write([]byte(`{}`))
				case managedAppAPIPath(namespace, runtime.ManagedAppResourceName(app)):
					_ = json.NewEncoder(w).Encode(managed)
				case "/apis/apps/v1/namespaces/" + namespace + "/deployments/" + runtime.RuntimeAppResourceName(app):
					_ = json.NewEncoder(w).Encode(deployment)
				case "/api/v1/namespaces/" + namespace + "/pods":
					if test.name == "pod_read_failure" {
						http.Error(w, "unavailable", http.StatusServiceUnavailable)
						return
					}
					if r.URL.Query().Get("labelSelector") != managedAppPodLabelSelector(app) {
						t.Error("unscoped pod lookup")
					}
					pods := []kubePod{pod}
					if test.name == "disabled" {
						pods = nil
					}
					_ = json.NewEncoder(w).Encode(kubePodList{Items: pods})
				case "/apis/discovery.k8s.io/v1/namespaces/" + namespace + "/endpointslices":
					if test.name == "ready_endpoint" {
						_, _ = w.Write([]byte(`{"items":[{"endpoints":[{"addresses":["10.0.0.1"],"conditions":{"ready":true}}]}]}`))
					} else {
						_, _ = w.Write([]byte(`{"items":[]}`))
					}
				default:
					t.Errorf("unexpected Kubernetes request: %s", r.URL)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			svc := &Service{Store: state, newKubeClient: func(ns string) (*kubeClient, error) {
				return &kubeClient{client: server.Client(), baseURL: server.URL, namespace: ns}, nil
			}}
			ledger, err := svc.verifyManagedAppMigrationCutover(context.Background(), op, app, false)
			if test.violation != "" {
				if err == nil || !strings.Contains(err.Error(), test.violation) {
					t.Fatalf("want %s, got %v", test.violation, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if ledger.EndpointRequired || ledger.EndpointStatus != model.AppMigrationEvidenceNotApplicable || ledger.PhysicalReplicas == nil || *ledger.PhysicalReplicas != 0 || !ledger.OldArtifactsProtected {
				t.Fatalf("unsafe disabled cutover: %+v", ledger)
			}
			persisted, found, err := state.LatestAppMigrationLedger(op.ID)
			if err != nil || !found || persisted.CutoverStatus != model.AppMigrationCutoverVerified {
				t.Fatalf("cutover proof not persisted: %+v %v", persisted, err)
			}
		})
	}
}

func migrationImageEvidenceFixture(t *testing.T, strict bool) (*Service, model.App, model.Operation, string) {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Now().UTC()
	app := model.App{
		ID: "app_migration_image", TenantID: "tenant_migration_image",
		Spec: model.AppSpec{Image: "registry.fugue.internal:5000/fugue-apps/demo:v1", Replicas: 1, RuntimeID: "runtime-old"},
	}
	op := model.Operation{
		ID: "op_migration_image", AppID: app.ID, TenantID: app.TenantID,
		Type: model.OperationTypeMigrate, SourceRuntimeID: "runtime-old", TargetRuntimeID: "runtime-new",
	}
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID: app.TenantID, AppID: app.ID, ImageRef: app.Spec.Image,
		RuntimeID: "runtime-source", Status: model.ImageLocationStatusPresent, LastSeenAt: &now,
	}); err != nil {
		t.Fatalf("record source image location: %v", err)
	}
	svc := &Service{
		Store: stateStore, Logger: log.New(io.Discard, "", 0),
		Config: config.ControllerConfig{ImageStoreMode: map[bool]string{true: "distributed", false: "distributed-with-registry-fallback"}[strict]},
		now:    func() time.Time { return now },
	}
	return svc, app, op, app.Spec.Image
}

func TestMigrationTargetImageReplicationRequiresTargetScopedEvidence(t *testing.T) {
	t.Parallel()
	svc, app, op, imageRef := migrationImageEvidenceFixture(t, true)
	verified, reason, err := svc.verifyMigrationTargetImageReplication(context.Background(), app, op, imageRef, "", true)
	if err != nil || verified || reason == "" {
		t.Fatalf("source image evidence must not verify target replication: verified=%v reason=%q err=%v", verified, reason, err)
	}

	now := time.Now().UTC()
	if _, err := svc.Store.UpsertImageLocation(model.ImageLocation{
		TenantID: app.TenantID, AppID: app.ID, ImageRef: imageRef,
		RuntimeID: op.TargetRuntimeID, Status: model.ImageLocationStatusPresent, LastSeenAt: &now,
	}); err != nil {
		t.Fatalf("record target image location: %v", err)
	}
	verified, reason, err = svc.verifyMigrationTargetImageReplication(context.Background(), app, op, imageRef, "", true)
	if err != nil || !verified || reason == "" {
		t.Fatalf("fresh target image evidence must verify replication: verified=%v reason=%q err=%v", verified, reason, err)
	}
}

func TestMigrationTargetImageReplicationRequiresPreflightOutsideStrictStore(t *testing.T) {
	t.Parallel()
	svc, app, op, imageRef := migrationImageEvidenceFixture(t, false)
	verified, reason, err := svc.verifyMigrationTargetImageReplication(context.Background(), app, op, imageRef, "", false)
	if err != nil || verified || reason == "" {
		t.Fatalf("missing target preflight must block fallback cutover: verified=%v reason=%q err=%v", verified, reason, err)
	}
	verified, reason, err = svc.verifyMigrationTargetImageReplication(context.Background(), app, op, imageRef, "", true)
	if err != nil || !verified || reason == "" {
		t.Fatalf("successful target preflight must permit fallback cutover: verified=%v reason=%q err=%v", verified, reason, err)
	}
}

func TestMigrationTargetImageReplicationUsesAppliedSchedulingNode(t *testing.T) {
	t.Parallel()
	svc, app, op, imageRef := migrationImageEvidenceFixture(t, true)
	now := time.Now().UTC()
	if _, err := svc.Store.UpsertImageLocation(model.ImageLocation{
		TenantID: app.TenantID, AppID: app.ID, ImageRef: imageRef,
		RuntimeID: "runtime-physical-node", ClusterNodeName: "node-de",
		Status: model.ImageLocationStatusPresent, LastSeenAt: &now,
	}); err != nil {
		t.Fatalf("record physical target image location: %v", err)
	}

	verified, reason, err := svc.verifyMigrationTargetImageReplication(
		context.Background(), app, op, imageRef, "node-de", true,
	)
	if err != nil || !verified || reason == "" {
		t.Fatalf("applied target scheduling node must verify replication: verified=%v reason=%q err=%v", verified, reason, err)
	}
}

func TestMigrationReplicaCountExcludesPreviousRevision(t *testing.T) {
	t.Parallel()
	if got := minMigrationReplicaCount(0, 1, 1); got != 0 {
		t.Fatalf("ready replicas from a previous revision counted as current: %d", got)
	}
	if got := minMigrationReplicaCount(2, 2, 2); got != 2 {
		t.Fatalf("current updated/ready/available replicas = %d, want 2", got)
	}
}

func TestSourceClusterIDForMigrationRequiresAuthoritativeRuntimeIdentity(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := migrationImageEvidenceFixture(t, false)
	op := model.Operation{
		ID: "op-cluster-identity", SourceRuntimeID: "runtime-without-cluster-label", TargetRuntimeID: "runtime-target",
	}
	if _, err := svc.sourceClusterIDForMigration(op, "target-cluster"); err == nil {
		t.Fatal("cross-runtime migration accepted a missing source cluster identity")
	}

	op.SourceRuntimeID = model.DefaultManagedRuntimeID
	clusterID, err := svc.sourceClusterIDForMigration(op, "managed-cluster")
	if err != nil || clusterID != "managed-cluster" {
		t.Fatalf("managed source should use the controller's live Kubernetes identity: cluster=%q err=%v", clusterID, err)
	}

	op.SourceRuntimeID = op.TargetRuntimeID
	clusterID, err = svc.sourceClusterIDForMigration(op, "target-cluster")
	if err != nil || clusterID != "target-cluster" {
		t.Fatalf("same-runtime migration should use the observed target identity: cluster=%q err=%v", clusterID, err)
	}
}
