package controller

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func recoveryFixture(t *testing.T) (*Service, model.App, model.Operation) {
	t.Helper()
	state := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := state.CreateTenant("Recovery Tenant")
	if err != nil {
		t.Fatal(err)
	}
	project, err := state.CreateProject(tenant.ID, "recovery", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := state.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "service", "", model.AppSpec{Image: "registry.example/service:old", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID, Ports: []int{8080}}, model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/service:old"})
	if err != nil {
		t.Fatal(err)
	}
	desired := app.Spec
	desired.Image = "registry.example/service:next"
	source := model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: desired.Image}
	op, err := state.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &desired, DesiredSource: &source, DesiredOriginSource: &source, RequestedByID: model.OperationRequestedByGitHubSyncController})
	if err != nil {
		t.Fatal(err)
	}
	op, ok, err := state.TryClaimPendingOperation(op.ID)
	if err != nil || !ok {
		t.Fatal(err)
	}
	app, err = state.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	svc := &Service{Store: state, Config: config.ControllerConfig{KubectlApply: true}, Logger: log.New(io.Discard, "", 0)}
	return svc, app, op
}
func TestRecoverReadyDeployWithoutWorkloadWritesOrReplay(t *testing.T) {
	for _, scenario := range []string{"ready", "wrong-image", "unready", "changed-spec", "live-worker", "changed-object"} {
		t.Run(scenario, func(t *testing.T) {
			s, app, op := recoveryFixture(t)
			expected := app
			expected.Spec = *op.DesiredSpec
			model.SetAppSourceState(&expected, op.DesiredOriginSource, op.DesiredSource)
			expected.Spec.RolloutIntent = rolloutIntentForManagedOperation(op, app, expected)
			expected = s.Renderer.PrepareApp(expected)
			scheduling, err := s.managedSchedulingConstraints(expected.Spec.RuntimeID)
			if err != nil {
				t.Fatal(err)
			}
			managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(expected, scheduling))
			if err != nil {
				t.Fatal(err)
			}
			managed.Metadata.UID = "original-object"
			managed.Metadata.Generation = 1
			managed.Status.Phase = runtime.ManagedAppPhaseError
			managed.Status.Message = "prior control store outage"
			deployment, found := s.expectedManagedAppDeployment(expected, scheduling)
			if !found {
				t.Fatal("missing deployment")
			}
			deployment.Metadata.Generation = 1
			deployment.Status.ObservedGeneration = 1
			deployment.Status.Replicas = 1
			deployment.Status.UpdatedReplicas = 1
			deployment.Status.ReadyReplicas = 1
			deployment.Status.AvailableReplicas = 1
			switch scenario {
			case "wrong-image":
				deployment.Spec.Template.Spec.Containers[0].Image = "registry.example/service:other"
			case "unready":
				deployment.Status.ReadyReplicas = 0
			case "changed-spec":
				managed.Spec.AppSpec.Replicas = 2
			case "live-worker":
				s.runningManagedOperations.Store(op.ID, operationExecutionKey(op))
			}
			pod := readyTemplatePod("ready", deployment, kubeResourceRequirements{})
			reads, managedReads := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("recovery mutated runtime: %s %s", r.Method, r.URL.Path)
					http.Error(w, "read only", 405)
					return
				}
				reads++
				w.Header().Set("Content-Type", "application/json")
				namespace := runtime.NamespaceForTenant(app.TenantID)
				switch r.URL.Path {
				case managedAppAPIPath(namespace, runtime.ManagedAppResourceName(app)):
					managedReads++
					snapshot := managed
					if scenario == "changed-object" && managedReads >= 3 {
						snapshot.Metadata.UID = "replacement-object"
					}
					json.NewEncoder(w).Encode(snapshot)
				case deploymentAPIPath(namespace, runtime.RuntimeAppResourceName(app)):
					json.NewEncoder(w).Encode(deployment)
				case "/api/v1/pods", "/api/v1/namespaces/" + namespace + "/pods":
					json.NewEncoder(w).Encode(map[string]any{"items": []kubePod{pod}})
				default:
					t.Errorf("unexpected GET %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			s.newKubeClient = func(namespace string) (*kubeClient, error) {
				return &kubeClient{client: server.Client(), baseURL: server.URL, namespace: namespace}, nil
			}
			if err = s.recoverCompletedManagedDeploys(context.Background()); err != nil {
				t.Fatal(err)
			}
			current, err := s.Store.GetOperation(op.ID)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := s.Store.GetApp(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "ready" {
				if current.Status != model.OperationStatusCompleted || actual.Spec.Image != expected.Spec.Image || model.AppOriginSource(actual).ImageRef != expected.Spec.Image {
					t.Fatalf("not recovered: status=%s image=%s", current.Status, actual.Spec.Image)
				}
				if actual.Spec.Env["FUGUE_APP_ID"] != "" {
					t.Fatal("runtime-only injected env persisted as configuration")
				}
				if err = s.recoverCompletedManagedDeploys(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if current.Status != model.OperationStatusRunning || actual.Spec.Image != app.Spec.Image {
				t.Fatalf("unproven recovery accepted: %s %s", current.Status, actual.Spec.Image)
			}
			if scenario == "live-worker" && reads != 0 {
				t.Fatal("active worker also reconciled", reads)
			}
		})
	}
}
