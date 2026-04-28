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

func TestManagedAppRolloutFailureIgnoresSIGTERMDuringRollingUpdate(t *testing.T) {
	t.Parallel()

	managed := runtime.ManagedAppObject{}
	managed.Metadata.Generation = 3
	managed.Status.Phase = runtime.ManagedAppPhaseError
	managed.Status.ObservedGeneration = 3
	managed.Status.Message = "pod demo-abc123 container demo failed: Error: exit_code=143"

	if got := managedAppRolloutFailure(managed, true); got != "" {
		t.Fatalf("expected SIGTERM rollout message to be ignored, got %q", got)
	}
}

func TestDeploymentRolloutReadyRequiresExpectedRelease(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:    "registry.pull.example/fugue-apps/demo:git-new",
			Replicas: 1,
		},
	}
	oldApp := app
	oldApp.Spec.Image = "registry.pull.example/fugue-apps/demo:git-old"
	expectedReleaseKey := runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{})
	oldReleaseKey := runtime.ManagedAppReleaseKey(oldApp, runtime.SchedulingConstraints{})

	deployment := readyKubeDeployment(runtime.RuntimeAppResourceName(app), 1)
	setKubeDeploymentPrimaryImage(&deployment, oldApp.Name, oldApp.Spec.Image)
	deployment.Metadata.Annotations = map[string]string{
		runtime.FugueAnnotationReleaseKey: oldReleaseKey,
	}

	ready, message, err := deploymentRolloutReady(deployment, true, 1, runtime.RuntimeAppResourceName(app), expectedReleaseKey, app.Spec.Image)
	if err != nil {
		t.Fatalf("unexpected rollout error: %v", err)
	}
	if ready {
		t.Fatal("expected old deployment image to be rejected")
	}
	if !strings.Contains(message, "image "+app.Spec.Image) {
		t.Fatalf("expected image mismatch message, got %q", message)
	}

	setKubeDeploymentPrimaryImage(&deployment, app.Name, app.Spec.Image)
	ready, message, err = deploymentRolloutReady(deployment, true, 1, runtime.RuntimeAppResourceName(app), expectedReleaseKey, app.Spec.Image)
	if err != nil {
		t.Fatalf("unexpected rollout error: %v", err)
	}
	if ready {
		t.Fatal("expected old deployment release key to be rejected")
	}
	if !strings.Contains(message, "release "+expectedReleaseKey) {
		t.Fatalf("expected release mismatch message, got %q", message)
	}

	deployment.Metadata.Annotations[runtime.FugueAnnotationReleaseKey] = expectedReleaseKey
	ready, _, err = deploymentRolloutReady(deployment, true, 1, runtime.RuntimeAppResourceName(app), expectedReleaseKey, app.Spec.Image)
	if err != nil {
		t.Fatalf("unexpected rollout error: %v", err)
	}
	if !ready {
		t.Fatal("expected deployment with matching image and release key to be ready")
	}
}

func TestWaitForManagedAppRolloutSucceedsWhenDeploymentIsReadyDespiteManagedAppError(t *testing.T) {
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
		"phase":              runtime.ManagedAppPhaseError,
		"message":            "pod demo-abc123 on node gcp3 failed: Evicted: The node had condition: [DiskPressure].",
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

	if err := svc.waitForManagedAppRollout(context.Background(), app, ""); err != nil {
		t.Fatalf("expected rollout wait to succeed once deployment is ready, got %v", err)
	}
}

func TestWaitForManagedAppRolloutUsesWatchEvents(t *testing.T) {
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

	deploymentForState := func(ready bool) kubeDeployment {
		deployment := kubeDeployment{}
		deployment.Metadata.Name = deploymentName
		deployment.Metadata.Generation = 1
		if ready {
			deployment.Metadata.ResourceVersion = "2"
			deployment.Status.ObservedGeneration = 1
			deployment.Status.Replicas = 1
			deployment.Status.UpdatedReplicas = 1
			deployment.Status.ReadyReplicas = 1
			deployment.Status.AvailableReplicas = 1
		} else {
			deployment.Metadata.ResourceVersion = "1"
			deployment.Status.ObservedGeneration = 1
			deployment.Status.Replicas = 1
			deployment.Status.UpdatedReplicas = 1
		}
		return deployment
	}

	managedApp := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managedMetadata, _ := managedApp["metadata"].(map[string]any)
	if managedMetadata == nil {
		managedMetadata = map[string]any{}
		managedApp["metadata"] = managedMetadata
	}
	managedMetadata["generation"] = 1
	managedMetadata["resourceVersion"] = "1"
	managedApp["status"] = map[string]any{
		"phase":              runtime.ManagedAppPhaseProgressing,
		"message":            "deployment progressing",
		"observedGeneration": 1,
	}

	var ready atomic.Int32
	var watchSeen atomic.Int32
	var kubeServer *httptest.Server
	kubeServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodGet &&
			r.URL.Path == deploymentCollectionAPIPath(namespace) &&
			r.URL.Query().Get("watch") == "true" &&
			r.URL.Query().Get("fieldSelector") == "metadata.name="+deploymentName:
			watchSeen.Store(1)
			time.Sleep(25 * time.Millisecond)
			ready.Store(1)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"type":   "MODIFIED",
				"object": deploymentForState(true),
			}); err != nil {
				t.Fatalf("encode deployment watch event: %v", err)
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		case r.Method == http.MethodGet && r.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deploymentName:
			if err := json.NewEncoder(w).Encode(deploymentForState(ready.Load() == 1)); err != nil {
				t.Fatalf("encode deployment: %v", err)
			}
		case r.Method == http.MethodGet &&
			r.URL.Path == managedAppCollectionAPIPath(namespace) &&
			r.URL.Query().Get("watch") == "true" &&
			r.URL.Query().Get("fieldSelector") == "metadata.name="+managedAppName:
			<-r.Context().Done()
		case r.Method == http.MethodGet && r.URL.Path == managedAppAPIPath(namespace, managedAppName):
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
			PollInterval:             time.Hour,
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

	startedAt := time.Now()
	if err := svc.waitForManagedAppRollout(context.Background(), app, ""); err != nil {
		t.Fatalf("expected rollout wait to succeed from watch event, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("expected watch event to avoid waiting for poll interval, elapsed=%s", elapsed)
	}
	if watchSeen.Load() == 0 {
		t.Fatal("expected deployment watch to be opened")
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
			if err := json.NewEncoder(w).Encode(managedApp); err != nil {
				t.Fatalf("encode managed app: %v", err)
			}
		case cloudNativePGClusterAPIPath(namespace, clusterName):
			cluster := kubeCloudNativePGCluster{}
			cluster.Metadata.Name = clusterName
			cluster.Spec.Instances = 2
			if atomic.AddInt32(&clusterGets, 1) >= 2 {
				cluster.Status.CurrentPrimary = clusterName + "-1"
				cluster.Status.ReadyInstances = 2
			} else {
				cluster.Status.ReadyInstances = 0
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

	cluster.Spec.Instances = 2
	cluster.Status.ReadyInstances = 1
	ready, message := managedBackingServiceClusterRolloutReady("demo-postgres", cluster, true)
	if !ready {
		t.Fatal("expected primary-ready replica recovery to keep rollout available")
	}
	if !strings.Contains(message, "remaining replicas recovering") {
		t.Fatalf("expected recovery message while replicas catch up, got %q", message)
	}

	cluster.Spec.Instances = 1
	cluster.Status.ReadyInstances = 2
	ready, message = managedBackingServiceClusterRolloutReady("demo-postgres", cluster, true)
	if ready {
		t.Fatal("expected extra ready instances to keep rollout pending until scale down settles")
	}
	if !strings.Contains(message, "to settle") {
		t.Fatalf("expected settle message when cluster still has extra ready instances, got %q", message)
	}
}

func TestWaitForManagedAppRolloutAllowsManagedPostgresPrimaryRecoveryAndCleansUpStrandedPods(t *testing.T) {
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
	clusterName := "demo-postgres"

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

	var deletedPod atomic.Int32
	var kubeServer *httptest.Server

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

	kubeServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deploymentName:
			if err := json.NewEncoder(w).Encode(deployment); err != nil {
				t.Fatalf("encode deployment: %v", err)
			}
		case r.Method == http.MethodGet && r.URL.Path == managedAppAPIPath(namespace, managedAppName):
			if err := json.NewEncoder(w).Encode(managedApp); err != nil {
				t.Fatalf("encode managed app: %v", err)
			}
		case r.Method == http.MethodGet && r.URL.Path == cloudNativePGClusterAPIPath(namespace, clusterName):
			cluster := kubeCloudNativePGCluster{}
			cluster.Metadata.Name = clusterName
			cluster.Spec.Instances = 2
			cluster.Status.ReadyInstances = 1
			cluster.Status.CurrentPrimary = "demo-postgres-4"
			if err := json.NewEncoder(w).Encode(cluster); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case r.Method == http.MethodGet &&
			r.URL.Path == "/api/v1/namespaces/"+namespace+"/pods" &&
			r.URL.Query().Get("labelSelector") == "cnpg.io/cluster=demo-postgres,app.kubernetes.io/managed-by=cloudnative-pg":
			pods := kubePodList{
				Items: []kubePod{
					{
						Metadata: struct {
							Name              string    `json:"name"`
							CreationTimestamp time.Time `json:"creationTimestamp"`
							DeletionTimestamp string    `json:"deletionTimestamp,omitempty"`
						}{
							Name:              "demo-postgres-1",
							CreationTimestamp: time.Date(2026, time.April, 12, 10, 0, 0, 0, time.UTC),
							DeletionTimestamp: time.Date(2026, time.April, 12, 11, 0, 0, 0, time.UTC).Format(time.RFC3339),
						},
						Spec: kubePodSpec{
							NodeName: "node-old",
						},
					},
					{
						Metadata: struct {
							Name              string    `json:"name"`
							CreationTimestamp time.Time `json:"creationTimestamp"`
							DeletionTimestamp string    `json:"deletionTimestamp,omitempty"`
						}{
							Name:              "demo-postgres-4",
							CreationTimestamp: time.Date(2026, time.April, 12, 11, 10, 0, 0, time.UTC),
						},
						Spec: kubePodSpec{
							NodeName: "node-new",
						},
					},
				},
			}
			if err := json.NewEncoder(w).Encode(pods); err != nil {
				t.Fatalf("encode pod list: %v", err)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/node-old":
			node := kubeNode{}
			node.Metadata.Name = "node-old"
			node.Status.Conditions = []kubeNodeCondition{{Type: "Ready", Status: "False"}}
			if err := json.NewEncoder(w).Encode(node); err != nil {
				t.Fatalf("encode old node: %v", err)
			}
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/"+namespace+"/pods/demo-postgres-1"):
			deletedPod.Store(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	if err := svc.waitForManagedAppRollout(context.Background(), app, ""); err != nil {
		t.Fatalf("expected rollout wait to succeed once the primary is ready, got %v", err)
	}
	if deletedPod.Load() == 0 {
		t.Fatal("expected stranded managed postgres pod to be force deleted")
	}
}

func readyKubeDeployment(name string, replicas int) kubeDeployment {
	deployment := kubeDeployment{}
	deployment.Metadata.Name = name
	deployment.Metadata.Generation = 1
	deployment.Status.ObservedGeneration = 1
	deployment.Status.Replicas = replicas
	deployment.Status.UpdatedReplicas = replicas
	deployment.Status.ReadyReplicas = replicas
	deployment.Status.AvailableReplicas = replicas
	return deployment
}

func setKubeDeploymentPrimaryImage(deployment *kubeDeployment, name, image string) {
	deployment.Spec.Template.Spec.Containers = []struct {
		Name  string `json:"name,omitempty"`
		Image string `json:"image,omitempty"`
	}{
		{Name: name, Image: image},
	}
}
