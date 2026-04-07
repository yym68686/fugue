package controller

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestWaitForManagedAppRolloutFailsWhenManagedAppReportsError(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Replicas: 1,
		},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	deploymentName := runtime.RuntimeAppResourceName(app)
	managedAppName := runtime.ManagedAppResourceName(app)

	deployment := kubeDeployment{}
	deployment.Metadata.Name = deploymentName
	deployment.Metadata.Generation = 1
	deployment.Status.ObservedGeneration = 1
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	managedApp := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managedMetadata, _ := managedApp["metadata"].(map[string]any)
	if managedMetadata == nil {
		managedMetadata = map[string]any{}
		managedApp["metadata"] = managedMetadata
	}
	managedMetadata["generation"] = 1
	managedApp["status"] = map[string]any{
		"phase":              runtime.ManagedAppPhaseError,
		"message":            "pod demo-abc123 container demo failed: Error: exit_code=3",
		"observedGeneration": 1,
	}

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/" + namespace + "/deployments/" + deploymentName:
			if err := json.NewEncoder(w).Encode(deployment); err != nil {
				t.Fatalf("encode deployment: %v", err)
			}
		case managedAppAPIPath(namespace, managedAppName):
			if err := json.NewEncoder(w).Encode(managedApp); err != nil {
				t.Fatalf("encode managed app: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	svc := &Service{
		Config: config.ControllerConfig{
			ManagedAppRolloutTimeout: 2 * time.Second,
			PollInterval:             10 * time.Millisecond,
		},
		Logger: log.New(io.Discard, "", 0),
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
	}

	err := svc.waitForManagedAppRollout(context.Background(), app, "")
	if err == nil {
		t.Fatal("expected rollout wait to fail")
	}
	if !strings.Contains(err.Error(), "exit_code=3") {
		t.Fatalf("expected managed app failure message in error, got %v", err)
	}
}

func TestWaitForManagedAppRolloutWaitsForManagedPostgresClusterHealth(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Replicas: 1,
		},
		BackingServices: []model.BackingService{
			{
				ID:          "service_demo_postgres",
				TenantID:    "tenant_demo",
				OwnerAppID:  "app_demo",
				Name:        "demo-postgres",
				Type:        model.BackingServiceTypePostgres,
				Provisioner: model.BackingServiceProvisionerManaged,
				Status:      model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						ServiceName: "demo-postgres",
						Instances:   2,
					},
				},
			},
		},
		Bindings: []model.ServiceBinding{
			{
				ID:        "binding_demo_postgres",
				TenantID:  "tenant_demo",
				AppID:     "app_demo",
				ServiceID: "service_demo_postgres",
			},
		},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	deploymentName := runtime.RuntimeAppResourceName(app)
	managedAppName := runtime.ManagedAppResourceName(app)

	deployment := kubeDeployment{}
	deployment.Metadata.Name = deploymentName
	deployment.Metadata.Generation = 1
	deployment.Status.ObservedGeneration = 1
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1

	managedApp := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managedMetadata, _ := managedApp["metadata"].(map[string]any)
	if managedMetadata == nil {
		managedMetadata = map[string]any{}
		managedApp["metadata"] = managedMetadata
	}
	managedMetadata["generation"] = 1
	managedApp["status"] = map[string]any{
		"phase":              runtime.ManagedAppPhaseReady,
		"message":            "deployment ready",
		"observedGeneration": 1,
	}

	var managedAppGets int32
	var clusterGets int32
	var kubeServer *httptest.Server

	svc := &Service{
		Config: config.ControllerConfig{
			ManagedAppRolloutTimeout: 3 * time.Second,
			PollInterval:             10 * time.Millisecond,
		},
		Logger: log.New(io.Discard, "", 0),
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
	}
	targets, err := svc.managedBackingServiceRolloutTargets(context.Background(), app)
	if err != nil {
		t.Fatalf("resolve rollout targets: %v", err)
	}
	if len(targets) != 1 {
		t.Fatalf("expected exactly one backing service rollout target, got %d", len(targets))
	}
	clusterName := targets[0].ResourceName

	kubeServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/" + namespace + "/deployments/" + deploymentName:
			if err := json.NewEncoder(w).Encode(deployment); err != nil {
				t.Fatalf("encode deployment: %v", err)
			}
		case managedAppAPIPath(namespace, managedAppName):
			if atomic.AddInt32(&managedAppGets, 1) > 2 {
				http.NotFound(w, r)
				return
			}
			if err := json.NewEncoder(w).Encode(managedApp); err != nil {
				t.Fatalf("encode managed app: %v", err)
			}
		case cloudNativePGClusterAPIPath(namespace, clusterName):
			cluster := kubeCloudNativePGCluster{}
			cluster.Metadata.Name = clusterName
			cluster.Spec.Instances = 2
			cluster.Status.CurrentPrimary = clusterName + "-1"
			if atomic.AddInt32(&clusterGets, 1) >= 2 {
				cluster.Status.ReadyInstances = 2
			} else {
				cluster.Status.ReadyInstances = 1
			}
			if err := json.NewEncoder(w).Encode(cluster); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	if err := svc.waitForManagedAppRollout(context.Background(), app, ""); err != nil {
		t.Fatalf("expected rollout wait to succeed after cluster becomes healthy, got %v", err)
	}
	if got := atomic.LoadInt32(&clusterGets); got < 2 {
		t.Fatalf("expected rollout wait to poll cluster health until ready, got %d cluster reads", got)
	}
}

func TestManagedBackingServiceClusterRolloutReady(t *testing.T) {
	t.Parallel()

	cluster := kubeCloudNativePGCluster{}
	cluster.Spec.Instances = 1
	cluster.Status.ReadyInstances = 1
	cluster.Status.CurrentPrimary = "demo-postgres-1"

	ready, _ := managedBackingServiceClusterRolloutReady("demo-postgres", cluster, true)
	if !ready {
		t.Fatal("expected exact ready instance count to be treated as ready")
	}

	cluster.Status.ReadyInstances = 2
	ready, message := managedBackingServiceClusterRolloutReady("demo-postgres", cluster, true)
	if ready {
		t.Fatal("expected extra ready instances to keep rollout pending until scale down settles")
	}
	if !strings.Contains(message, "to settle") {
		t.Fatalf("expected settle message when cluster still has extra ready instances, got %q", message)
	}
}
