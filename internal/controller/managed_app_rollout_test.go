package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestRefreshManagedAppStatusPublishesReadyWhileOperationIsActive(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Project", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.example/fugue-apps/demo:release",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/demo:release"})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := stateStore.CreateOperation(model.Operation{
		TenantID:    app.TenantID,
		AppID:       app.ID,
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &app.Spec,
	}); err != nil {
		t.Fatalf("create active operation: %v", err)
	}

	app = runtime.Renderer{}.PrepareApp(app)
	namespace := runtime.NamespaceForTenant(app.TenantID)
	managedName := runtime.ManagedAppResourceName(app)
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}
	managed.Metadata.Generation = 2
	managed.Status = runtime.ManagedAppStatus{
		Phase:              runtime.ManagedAppPhaseProgressing,
		Message:            "waiting for deployment ready replicas 0/1",
		DesiredReplicas:    1,
		ObservedGeneration: 2,
	}
	deployment := kubeDeployment{}
	deployment.Metadata.Name = runtime.RuntimeAppResourceName(app)
	deployment.Metadata.Generation = 3
	deployment.Status.ObservedGeneration = 3
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1
	deployment.Status.Conditions = []runtime.ManagedAppCondition{{Type: "Available", Status: "True"}}

	var patched runtime.ManagedAppStatus
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == managedAppAPIPath(namespace, managedName):
			payload, marshalErr := json.Marshal(managed)
			if marshalErr != nil {
				t.Fatalf("marshal managed app: %v", marshalErr)
			}
			return okJSONResponse(string(payload)), nil
		case req.Method == http.MethodGet && req.URL.Path == deploymentAPIPath(namespace, runtime.RuntimeAppResourceName(app)):
			payload, marshalErr := json.Marshal(deployment)
			if marshalErr != nil {
				t.Fatalf("marshal deployment: %v", marshalErr)
			}
			return okJSONResponse(string(payload)), nil
		case req.Method == http.MethodGet && req.URL.Path == "/api/v1/namespaces/"+namespace+"/pods":
			return okJSONResponse(`{"items":[]}`), nil
		case req.Method == http.MethodPatch && req.URL.Path == managedAppAPIPath(namespace, managedName)+"/status":
			var body struct {
				Status runtime.ManagedAppStatus `json:"status"`
			}
			if decodeErr := json.NewDecoder(req.Body).Decode(&body); decodeErr != nil {
				t.Fatalf("decode status patch: %v", decodeErr)
			}
			patched = body.Status
			return okJSONResponse(`{}`), nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})
	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
	}
	svc := &Service{Store: stateStore}
	if err := svc.refreshManagedAppStatus(context.Background(), client, app); err != nil {
		t.Fatalf("refresh managed app status: %v", err)
	}
	if patched.Phase != runtime.ManagedAppPhaseReady || patched.ReadyReplicas != 1 {
		t.Fatalf("expected active operation status refresh to publish Ready 1/1, got %+v", patched)
	}
}

func TestWaitForManagedAppRolloutPublishesReadyBeforeGatingOnObservedStatus(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Rollout status tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Project", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.example/fugue-apps/demo:release",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/demo:release"})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:    app.TenantID,
		AppID:       app.ID,
		Type:        model.OperationTypeDeploy,
		DesiredSpec: &app.Spec,
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	op, claimed, err := stateStore.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatalf("claim operation: claimed=%v err=%v", claimed, err)
	}

	app = runtime.Renderer{}.PrepareApp(app)
	namespace := runtime.NamespaceForTenant(app.TenantID)
	managedName := runtime.ManagedAppResourceName(app)
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}
	managed.Metadata.Generation = 2
	managed.Status = runtime.ManagedAppStatus{
		Phase:               runtime.ManagedAppPhaseProgressing,
		Message:             "waiting for old replicas to terminate",
		DesiredReplicas:     1,
		ObservedGeneration:  2,
		LastAppliedSpecHash: runtime.ManagedAppSpecHash(managed.Spec),
	}
	deployment, found := expectedManagedAppDeployment(app, runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected deployment")
	}
	deployment.Metadata.Name = runtime.RuntimeAppResourceName(app)
	managedAppLiveGuardMarkReady(&deployment, 1)
	pod := readyTemplatePod("demo-ready", deployment, kubeResourceRequirements{})

	statusPatches := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == managedAppAPIPath(namespace, managedName):
			payload, marshalErr := json.Marshal(managed)
			if marshalErr != nil {
				t.Fatalf("marshal managed app: %v", marshalErr)
			}
			return okJSONResponse(string(payload)), nil
		case req.Method == http.MethodGet && req.URL.Path == deploymentAPIPath(namespace, deployment.Metadata.Name):
			payload, marshalErr := json.Marshal(deployment)
			if marshalErr != nil {
				t.Fatalf("marshal deployment: %v", marshalErr)
			}
			return okJSONResponse(string(payload)), nil
		case req.Method == http.MethodGet && req.URL.Path == "/api/v1/namespaces/"+namespace+"/pods":
			payload, marshalErr := json.Marshal(kubePodList{Items: []kubePod{pod}})
			if marshalErr != nil {
				t.Fatalf("marshal pods: %v", marshalErr)
			}
			return okJSONResponse(string(payload)), nil
		case req.Method == http.MethodGet && req.URL.Path == "/api/v1/pods":
			return okJSONResponse(`{"items":[]}`), nil
		case req.Method == http.MethodGet && req.URL.Path == "/api/v1/nodes":
			return okJSONResponse(`{"items":[]}`), nil
		case req.Method == http.MethodPatch && req.URL.Path == managedAppAPIPath(namespace, managedName)+"/status":
			var body struct {
				Status runtime.ManagedAppStatus `json:"status"`
			}
			if decodeErr := json.NewDecoder(req.Body).Decode(&body); decodeErr != nil {
				t.Fatalf("decode status patch: %v", decodeErr)
			}
			managed.Status = body.Status
			statusPatches++
			return okJSONResponse(`{}`), nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.String())
			return nil, nil
		}
	})
	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
		namespace:   namespace,
	}
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ManagedAppRolloutTimeout: 2 * time.Second,
			PollInterval:             10 * time.Millisecond,
		},
		Renderer: runtime.Renderer{},
		Logger:   log.New(io.Discard, "", 0),
		newKubeClient: func(string) (*kubeClient, error) {
			return client, nil
		},
	}
	if err := svc.waitForManagedAppRolloutWithScheduling(context.Background(), app, op.ID, runtime.SchedulingConstraints{}); err != nil {
		t.Fatalf("ready deployment must publish observed status before the final status gate: %v", err)
	}
	if statusPatches != 1 {
		t.Fatalf("expected exactly one ready status patch, got %d", statusPatches)
	}
	if managed.Status.Phase != runtime.ManagedAppPhaseReady || managed.Status.ReadyReplicas != 1 {
		t.Fatalf("expected refreshed managed app Ready 1/1, got %+v", managed.Status)
	}
}

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
	app = runtime.Renderer{}.PrepareApp(app)
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
		"message":            "pod demo-abc123 container demo failed: CrashLoopBackOff: back-off restarting failed container",
		"observedGeneration": 1,
	}
	markManagedAppFixtureApplied(t, managedApp)

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
		case "/api/v1/namespaces/" + namespace + "/pods":
			if err := json.NewEncoder(w).Encode(kubePodList{}); err != nil {
				t.Fatalf("encode pod list: %v", err)
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
	if !strings.Contains(err.Error(), "CrashLoopBackOff") {
		t.Fatalf("expected managed app failure message in error, got %v", err)
	}
}

func TestManagedAppRolloutFailureReportsNonSIGTERMExit(t *testing.T) {
	t.Parallel()

	managed := runtime.ManagedAppObject{}
	managed.Metadata.Generation = 3
	managed.Status.Phase = runtime.ManagedAppPhaseError
	managed.Status.ObservedGeneration = 3
	managed.Status.Message = "pod demo-abc123 container demo failed: Error: exit_code=3"

	if got := managedAppRolloutFailure(managed, true, ""); !strings.Contains(got, "exit_code=3") {
		t.Fatalf("expected non-SIGTERM exit to fail rollout, got %q", got)
	}
}

func TestManagedAppRolloutFailureIgnoresSIGTERMDuringRollingUpdate(t *testing.T) {
	t.Parallel()

	managed := runtime.ManagedAppObject{}
	managed.Metadata.Generation = 3
	managed.Status.Phase = runtime.ManagedAppPhaseError
	managed.Status.ObservedGeneration = 3
	managed.Status.Message = "pod demo-abc123 container demo failed: Error: exit_code=143"

	if got := managedAppRolloutFailure(managed, true, ""); got != "" {
		t.Fatalf("expected SIGTERM rollout message to be ignored, got %q", got)
	}
}

func TestWaitForManagedAppRolloutFailsWhenAppliedTemplatePodCrashes(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:    "registry.example/fugue-apps/demo:broken",
			Replicas: 1,
		},
	}
	app = runtime.Renderer{}.PrepareApp(app)
	namespace := runtime.NamespaceForTenant(app.TenantID)
	deploymentName := runtime.RuntimeAppResourceName(app)
	managedAppName := runtime.ManagedAppResourceName(app)

	deployment, found := expectedManagedAppDeployment(app, runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected managed app deployment fixture")
	}
	deployment.Metadata.Name = deploymentName
	deployment.Metadata.Generation = 3
	deployment.Metadata.ResourceVersion = "deployment-rv-1"
	releaseKey := runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{})
	deployment.Metadata.Annotations = map[string]string{
		runtime.FugueAnnotationReleaseKey: releaseKey,
	}
	deployment.Spec.Template.Metadata.Annotations = map[string]string{
		runtime.FugueAnnotationReleaseKey: releaseKey,
	}
	deployment.Status.ObservedGeneration = 3
	deployment.Status.Replicas = 2
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1

	oldPod := readyTemplatePod("demo-old", deployment, kubeResourceRequirements{})
	oldPod.Metadata.Annotations = map[string]string{
		runtime.FugueAnnotationReleaseKey: "old-release",
	}
	oldPod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name: "demo",
		State: kubeRuntimeState{
			Waiting: &kubeStateDetail{
				Reason:  "CrashLoopBackOff",
				Message: "old pod crash should be ignored",
			},
		},
	}}

	newPod := readyTemplatePod("demo-new", deployment, kubeResourceRequirements{})
	newPod.Status.Phase = "Running"
	newPod.Status.Conditions = nil
	newPod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name: "demo",
		State: kubeRuntimeState{
			Waiting: &kubeStateDetail{
				Reason:  "CrashLoopBackOff",
				Message: "back-off restarting failed container",
			},
		},
	}}

	managedApp := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managedMetadata := managedApp["metadata"].(map[string]any)
	managedMetadata["generation"] = 3
	managedMetadata["resourceVersion"] = "managed-rv-1"
	markManagedAppFixtureApplied(t, managedApp)

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Query().Get("watch") == "true":
			if err := json.NewEncoder(w).Encode(map[string]any{"type": "MODIFIED", "object": map[string]any{}}); err != nil {
				t.Errorf("encode watch event: %v", err)
			}
		case r.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+deploymentName:
			if err := json.NewEncoder(w).Encode(deployment); err != nil {
				t.Errorf("encode deployment: %v", err)
			}
		case r.URL.Path == managedAppAPIPath(namespace, managedAppName):
			if err := json.NewEncoder(w).Encode(managedApp); err != nil {
				t.Errorf("encode managed app: %v", err)
			}
		case r.URL.Path == "/api/v1/namespaces/"+namespace+"/pods":
			if err := json.NewEncoder(w).Encode(kubePodList{Items: []kubePod{oldPod, newPod}}); err != nil {
				t.Errorf("encode pods: %v", err)
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

	err := svc.waitForManagedAppRolloutWithScheduling(context.Background(), app, "", runtime.SchedulingConstraints{})
	if err == nil {
		t.Fatal("expected rollout wait to fail")
	}
	if !strings.Contains(err.Error(), "CrashLoopBackOff") {
		t.Fatalf("expected crashing pod failure message, got %v", err)
	}
	if strings.Contains(err.Error(), "old pod crash should be ignored") {
		t.Fatalf("expected old template pod failure to be ignored, got %v", err)
	}
}

func TestDeploymentTemplatePodFailureMessageIgnoresUnschedulableTemplatePod(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:    "registry.example/fugue-apps/demo:v2",
			Replicas: 1,
		},
	}
	app = runtime.Renderer{}.PrepareApp(app)
	deployment, found := expectedManagedAppDeployment(app, runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected managed app deployment fixture")
	}

	pod := readyTemplatePod("demo-pending", deployment, kubeResourceRequirements{})
	pod.Status.Phase = "Pending"
	pod.Status.Conditions = []kubePodCondition{
		{
			Type:    "PodScheduled",
			Status:  "False",
			Reason:  "Unschedulable",
			Message: "0/6 nodes are available: 1 Insufficient cpu, 5 Preemption is not helpful for scheduling.",
		},
	}

	if got := deploymentTemplatePodFailureMessage([]kubePod{pod}, deployment); got != "" {
		t.Fatalf("expected unschedulable pod to wait instead of failing rollout, got %q", got)
	}
	if got := deploymentTemplatePodSchedulingBlockMessage([]kubePod{pod}, deployment); !strings.Contains(got, "Insufficient cpu") {
		t.Fatalf("expected unschedulable pod scheduling message, got %q", got)
	}
}

func TestRolloutSchedulingBlockTrackerFailsAfterGracePeriodAndResets(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 9, 1, 17, 0, 0, 0, time.UTC)
	tracker := rolloutSchedulingBlockTracker{}
	message := "pod demo-pending is unschedulable: 0/7 nodes are available: 1 Insufficient cpu"

	if err := tracker.observe(startedAt, message); err != nil {
		t.Fatalf("first scheduling block observation: %v", err)
	}
	if err := tracker.observe(startedAt.Add(managedAppSchedulingBlockGracePeriod-time.Millisecond), message); err != nil {
		t.Fatalf("scheduling block before grace period: %v", err)
	}
	err := tracker.observe(startedAt.Add(managedAppSchedulingBlockGracePeriod), message)
	if err == nil || !strings.Contains(err.Error(), "rollout scheduling blocked for 30s") || !strings.Contains(err.Error(), "Insufficient cpu") {
		t.Fatalf("expected actionable scheduling timeout, got %v", err)
	}

	if err := tracker.observe(startedAt.Add(time.Minute), ""); err != nil {
		t.Fatalf("reset scheduling block: %v", err)
	}
	if err := tracker.observe(startedAt.Add(time.Minute), message); err != nil {
		t.Fatalf("first scheduling block after reset: %v", err)
	}
}

func TestRestoreFailedManagedAppRolloutUsesDurablePreviousSpec(t *testing.T) {
	t.Parallel()

	previous := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "registry.example/fugue-apps/demo:v1",
			Replicas:  1,
			Resources: &model.ResourceSpec{CPUMilliCores: 190, MemoryMebibytes: 512},
		},
	}
	op := model.Operation{
		ID:    "op_deploy",
		Type:  model.OperationTypeDeploy,
		AppID: previous.ID,
	}
	scheduling := runtime.SchedulingConstraints{NodeSelector: map[string]string{"kubernetes.io/hostname": "node-a"}}
	cause := errors.New("rollout scheduling blocked for 30s: Insufficient cpu")
	called := false
	svc := &Service{
		restoreFailedManagedAppSpec: func(_ context.Context, gotOp model.Operation, gotPrevious model.App, gotScheduling runtime.SchedulingConstraints, gotCause error) error {
			called = true
			if gotOp.ID != op.ID || gotPrevious.Spec.Resources == nil || gotPrevious.Spec.Resources.CPUMilliCores != 190 {
				t.Fatalf("restore did not receive the durable previous snapshot: op=%+v app=%+v", gotOp, gotPrevious)
			}
			if gotScheduling.NodeSelector["kubernetes.io/hostname"] != "node-a" || !errors.Is(gotCause, cause) {
				t.Fatalf("restore lost scheduling or failure evidence: scheduling=%+v cause=%v", gotScheduling, gotCause)
			}
			return nil
		},
	}
	if err := svc.restoreFailedManagedAppRollout(context.Background(), op, previous, scheduling, cause); err != nil {
		t.Fatalf("restore failed managed app rollout: %v", err)
	}
	if !called {
		t.Fatal("expected failed right-sizing rollout restore")
	}
}

func TestWaitForManagedAppRolloutFailsPersistentUnschedulableSurge(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:    "registry.example/fugue-apps/demo:v2",
			Replicas: 1,
		},
	}
	app = runtime.Renderer{}.PrepareApp(app)
	namespace := runtime.NamespaceForTenant(app.TenantID)
	deploymentName := runtime.RuntimeAppResourceName(app)
	managedAppName := runtime.ManagedAppResourceName(app)

	deployment, found := expectedManagedAppDeployment(app, runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected managed app deployment fixture")
	}
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 2
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.AvailableReplicas = 1
	deployment.Status.UnavailableReplicas = 1
	pendingPod := readyTemplatePod("demo-pending", deployment, kubeResourceRequirements{})
	pendingPod.Status.Phase = "Pending"
	pendingPod.Status.Conditions = []kubePodCondition{{
		Type:    "PodScheduled",
		Status:  "False",
		Reason:  "Unschedulable",
		Message: "0/7 nodes are available: 1 Insufficient cpu, 6 Preemption is not helpful for scheduling.",
	}}

	managedApp := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managedMetadata := managedApp["metadata"].(map[string]any)
	managedMetadata["generation"] = 2
	markManagedAppFixtureApplied(t, managedApp)

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/" + namespace + "/deployments/" + deploymentName:
			if err := json.NewEncoder(w).Encode(deployment); err != nil {
				t.Errorf("encode deployment: %v", err)
			}
		case managedAppAPIPath(namespace, managedAppName):
			if err := json.NewEncoder(w).Encode(managedApp); err != nil {
				t.Errorf("encode managed app: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/pods":
			if err := json.NewEncoder(w).Encode(kubePodList{Items: []kubePod{pendingPod}}); err != nil {
				t.Errorf("encode pods: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	startedAt := time.Date(2026, 9, 1, 17, 0, 0, 0, time.UTC)
	var observations atomic.Int64
	svc := &Service{
		Config: config.ControllerConfig{
			ManagedAppRolloutTimeout: 5 * time.Second,
			PollInterval:             time.Millisecond,
		},
		Renderer: runtime.Renderer{},
		Logger:   log.New(io.Discard, "", 0),
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
		now: func() time.Time {
			return startedAt.Add(time.Duration(observations.Add(1)-1) * managedAppSchedulingBlockGracePeriod)
		},
	}

	err := svc.waitForManagedAppRolloutWithScheduling(context.Background(), app, "", runtime.SchedulingConstraints{})
	if err == nil || !strings.Contains(err.Error(), "rollout scheduling blocked for 30s") || !strings.Contains(err.Error(), "Insufficient cpu") {
		t.Fatalf("expected persistent surge scheduling failure, got %v", err)
	}
}

func TestZeroDowntimeRolloutCapacityBlockMessageReportsInsufficientCPU(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:    "registry.example/fugue-apps/demo:v2",
			Replicas: 1,
			Resources: &model.ResourceSpec{
				CPUMilliCores:   700,
				MemoryMebibytes: 512,
			},
		},
	}
	app = runtime.Renderer{}.PrepareApp(app)
	deployment, found := expectedManagedAppDeployment(app, runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected managed app deployment fixture")
	}
	existingPod := kubePod{}
	existingPod.Metadata.Name = "demo-old"
	existingPod.Spec.NodeName = "node-a"
	existingPod.Spec.Containers = []kubeContainerSpec{{
		Name: "demo",
		Resources: kubeResourceRequirements{
			Requests: map[string]string{
				"cpu":    "900m",
				"memory": "512Mi",
			},
		},
	}}
	existingPod.Status.Phase = "Running"

	node := kubeNode{}
	node.Metadata.Name = "node-a"
	node.Status.Conditions = []kubeNodeCondition{{Type: "Ready", Status: "True"}}
	node.Status.Allocatable = map[string]string{
		"cpu":    "1500m",
		"memory": "4Gi",
	}

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/pods":
			if err := json.NewEncoder(w).Encode(kubePodList{Items: []kubePod{existingPod}}); err != nil {
				t.Fatalf("encode pods: %v", err)
			}
		case "/api/v1/nodes":
			if err := json.NewEncoder(w).Encode(kubeNodeList{Items: []kubeNode{node}}); err != nil {
				t.Fatalf("encode nodes: %v", err)
			}
		case "/api/v1/nodes/node-a":
			if err := json.NewEncoder(w).Encode(node); err != nil {
				t.Fatalf("encode node: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	client := &kubeClient{
		client:      kubeServer.Client(),
		baseURL:     kubeServer.URL,
		bearerToken: "test",
	}
	message, err := zeroDowntimeRolloutCapacityBlockMessage(context.Background(), client, deployment, 1)
	if err != nil {
		t.Fatalf("capacity preflight: %v", err)
	}
	if !strings.Contains(message, "zero-downtime rollout blocked") ||
		!strings.Contains(message, "node-a") ||
		!strings.Contains(message, "600m CPU") ||
		!strings.Contains(message, "700m CPU") {
		t.Fatalf("expected actionable insufficient CPU message, got %q", message)
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

func TestDeploymentSchedulingReadyRequiresRuntimeScheduling(t *testing.T) {
	t.Parallel()

	deployment := readyKubeDeployment("app-demo", 1)
	deployment.Spec.Template.Spec.NodeSelector = map[string]string{
		runtime.SharedPoolLabelKey: runtime.SharedPoolLabelValue,
	}

	expected := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			runtime.RuntimeIDLabelKey: "runtime_agent",
			runtime.TenantIDLabelKey:  "tenant_owner",
		},
		Tolerations: []runtime.Toleration{
			{
				Key:      runtime.TenantTaintKey,
				Operator: "Equal",
				Value:    "tenant_owner",
				Effect:   "NoSchedule",
			},
		},
	}

	ready, message := deploymentSchedulingReady(deployment, expected)
	if ready {
		t.Fatal("expected stale shared-pool nodeSelector to be rejected")
	}
	if !strings.Contains(message, "nodeSelector") {
		t.Fatalf("expected nodeSelector mismatch message, got %q", message)
	}

	deployment.Spec.Template.Spec.NodeSelector = map[string]string{
		runtime.RuntimeIDLabelKey: "runtime_agent",
		runtime.TenantIDLabelKey:  "tenant_owner",
	}
	ready, message = deploymentSchedulingReady(deployment, expected)
	if ready {
		t.Fatal("expected missing tenant toleration to be rejected")
	}
	if !strings.Contains(message, "tolerations") {
		t.Fatalf("expected toleration mismatch message, got %q", message)
	}

	deployment.Spec.Template.Spec.Tolerations = expected.Tolerations
	ready, message = deploymentSchedulingReady(deployment, expected)
	if !ready {
		t.Fatalf("expected matching runtime scheduling to be ready, got %q", message)
	}
}

func TestWaitForManagedAppRolloutWithSchedulingAcceptsAppliedHostnamePin(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Replicas: 1,
		},
	}
	appliedScheduling := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			kubeHostnameLabelKey: "node-a",
		},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	deploymentName := runtime.RuntimeAppResourceName(app)
	managedAppName := runtime.ManagedAppResourceName(app)

	deployment := readyKubeDeployment(deploymentName, 1)
	deployment.Spec.Template.Spec.NodeSelector = appliedScheduling.NodeSelector
	managedApp := runtime.BuildManagedAppObject(app, appliedScheduling)
	managedMetadata := managedApp["metadata"].(map[string]any)
	managedMetadata["generation"] = 1
	markManagedAppFixtureApplied(t, managedApp)

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/" + namespace + "/deployments/" + deploymentName:
			if err := json.NewEncoder(w).Encode(deployment); err != nil {
				t.Errorf("encode deployment: %v", err)
			}
		case managedAppAPIPath(namespace, managedAppName):
			if err := json.NewEncoder(w).Encode(managedApp); err != nil {
				t.Errorf("encode managed app: %v", err)
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

	if err := svc.waitForManagedAppRolloutWithScheduling(context.Background(), app, "", appliedScheduling); err != nil {
		t.Fatalf("expected rollout wait to accept applied hostname pin, got %v", err)
	}
}

func TestWaitForManagedAppRolloutRequiresReadyPodFromAppliedTemplate(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:    "registry.pull.example/fugue-apps/demo@sha256:new",
			Replicas: 1,
			Resources: &model.ResourceSpec{
				CPUMilliCores: 200,
			},
		},
	}
	app = runtime.Renderer{}.PrepareApp(app)
	namespace := runtime.NamespaceForTenant(app.TenantID)
	deploymentName := runtime.RuntimeAppResourceName(app)
	managedAppName := runtime.ManagedAppResourceName(app)
	expectedReleaseKey := runtime.ManagedAppReleaseKey(app, runtime.SchedulingConstraints{})

	deployment := readyKubeDeployment(deploymentName, 1)
	deployment.Metadata.ResourceVersion = "deployment-rv-1"
	deployment.Metadata.Annotations = map[string]string{
		runtime.FugueAnnotationReleaseKey: expectedReleaseKey,
	}
	deployment.Spec.Template.Metadata.Labels = map[string]string{"app": "demo"}
	deployment.Spec.Template.Spec.Containers = []kubeContainerSpec{{
		Name:  "demo",
		Image: app.Spec.Image,
		Resources: kubeResourceRequirements{
			Requests: map[string]string{"cpu": "200m"},
		},
	}}

	oldPod := readyTemplatePod("demo-old", deployment, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "100m"},
	})
	newPod := readyTemplatePod("demo-new", deployment, kubeResourceRequirements{
		Requests: map[string]string{"cpu": "200m"},
	})
	if podMatchesDeploymentTemplate(oldPod, deployment) {
		t.Fatal("expected old resource template pod not to match deployment template")
	}
	if !podMatchesDeploymentTemplate(newPod, deployment) {
		t.Fatal("expected new resource template pod to match deployment template")
	}

	managedApp := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managedMetadata := managedApp["metadata"].(map[string]any)
	managedMetadata["generation"] = 1
	managedMetadata["resourceVersion"] = "managed-rv-1"
	markManagedAppFixtureApplied(t, managedApp)

	var deploymentCalls atomic.Int32
	var managedAppCalls atomic.Int32
	var podListCalls atomic.Int32
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			if err := json.NewEncoder(w).Encode(map[string]any{"type": "MODIFIED", "object": map[string]any{}}); err != nil {
				t.Errorf("encode watch event: %v", err)
			}
			return
		}
		switch r.URL.Path {
		case "/apis/apps/v1/namespaces/" + namespace + "/deployments/" + deploymentName:
			deploymentCalls.Add(1)
			if err := json.NewEncoder(w).Encode(deployment); err != nil {
				t.Errorf("encode deployment: %v", err)
			}
		case managedAppAPIPath(namespace, managedAppName):
			managedAppCalls.Add(1)
			if err := json.NewEncoder(w).Encode(managedApp); err != nil {
				t.Errorf("encode managed app: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/pods":
			call := podListCalls.Add(1)
			pods := []kubePod{oldPod}
			if call >= 2 {
				pods = []kubePod{newPod}
			}
			if err := json.NewEncoder(w).Encode(kubePodList{Items: pods}); err != nil {
				t.Errorf("encode pods: %v", err)
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

	if err := svc.waitForManagedAppRolloutWithScheduling(context.Background(), app, "", runtime.SchedulingConstraints{}); err != nil {
		t.Fatalf("expected rollout wait to succeed once applied-template pod is ready, got %v (deployment calls=%d managed calls=%d pod list calls=%d)", err, deploymentCalls.Load(), managedAppCalls.Load(), podListCalls.Load())
	}
	if got := podListCalls.Load(); got < 2 {
		t.Fatalf("expected wait to reject old-template pod before succeeding, pod list calls=%d", got)
	}
}

func TestManagedAppRuntimeSchedulingReadyRequiresTargetRuntimeScheduling(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_owner",
		Name:     "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_agent",
		},
	}
	expected := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			runtime.RuntimeIDLabelKey: "runtime_agent",
			runtime.TenantIDLabelKey:  "tenant_owner",
		},
		Tolerations: []runtime.Toleration{
			{
				Key:      runtime.TenantTaintKey,
				Operator: "Equal",
				Value:    "tenant_owner",
				Effect:   "NoSchedule",
			},
		},
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			runtime.SharedPoolLabelKey: runtime.SharedPoolLabelValue,
		},
	}))
	if err != nil {
		t.Fatalf("decode managed app: %v", err)
	}

	ready, message := managedAppRuntimeSchedulingReady(managed, true, app, expected, "")
	if ready {
		t.Fatal("expected stale managed app scheduling to be rejected")
	}
	if !strings.Contains(message, "scheduling nodeSelector") {
		t.Fatalf("expected scheduling mismatch message, got %q", message)
	}

	managed.Spec.Scheduling = expected
	managed.Spec.AppSpec.RuntimeID = "runtime_other"
	ready, message = managedAppRuntimeSchedulingReady(managed, true, app, expected, "")
	if ready {
		t.Fatal("expected stale managed app runtime to be rejected")
	}
	if !strings.Contains(message, "runtime runtime_agent") {
		t.Fatalf("expected runtime mismatch message, got %q", message)
	}

	managed.Spec.AppSpec.RuntimeID = app.Spec.RuntimeID
	ready, message = managedAppRuntimeSchedulingReady(managed, true, app, expected, "")
	if !ready {
		t.Fatalf("expected matching managed app runtime scheduling to be ready, got %q", message)
	}
}

func TestManagedAppRuntimeSchedulingReadyRequiresObservedExpectedSpec(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:        "registry.pull.example/fugue-apps/demo@sha256:new",
			Replicas:     1,
			RuntimeID:    "runtime_agent",
			RestartToken: "restart_new",
		},
	}
	expected := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			runtime.RuntimeIDLabelKey: "runtime_agent",
		},
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, expected))
	if err != nil {
		t.Fatalf("decode managed app: %v", err)
	}
	managed.Metadata.Generation = 7
	expectedSpecHash := runtime.ManagedAppSpecHash(managed.Spec)

	managed.Status.ObservedGeneration = 6
	managed.Status.LastAppliedSpecHash = expectedSpecHash
	ready, message := managedAppRuntimeSchedulingReady(managed, true, app, expected, expectedSpecHash)
	if ready {
		t.Fatal("expected stale observed generation to be rejected")
	}
	if !strings.Contains(message, "observed generation") {
		t.Fatalf("expected observed generation message, got %q", message)
	}

	managed.Status.ObservedGeneration = 7
	managed.Status.LastAppliedSpecHash = "old-hash"
	ready, message = managedAppRuntimeSchedulingReady(managed, true, app, expected, expectedSpecHash)
	if ready {
		t.Fatal("expected stale applied spec hash to be rejected")
	}
	if !strings.Contains(message, "applied spec hash") {
		t.Fatalf("expected applied spec hash message, got %q", message)
	}

	managed.Status.LastAppliedSpecHash = expectedSpecHash
	ready, message = managedAppRuntimeSchedulingReady(managed, true, app, expected, expectedSpecHash)
	if !ready {
		t.Fatalf("expected observed matching managed app to be ready, got %q", message)
	}
}

func TestManagedAppRuntimeSchedulingReadyIgnoresObservedBackingServiceUsage(t *testing.T) {
	t.Parallel()

	memoryBytes := int64(128 * 1024 * 1024)
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "registry.pull.example/fugue-apps/demo@sha256:new",
			Replicas:  1,
			RuntimeID: "runtime_agent",
		},
		BackingServices: []model.BackingService{{
			ID:        "service_demo",
			TenantID:  "tenant_demo",
			ProjectID: "project_demo",
			Name:      "demo-postgres",
			Type:      model.BackingServiceTypePostgres,
			Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
				ServiceName: "demo-postgres",
			}},
			CurrentResourceUsage: &model.ResourceUsage{MemoryBytes: &memoryBytes},
		}},
	}
	expected := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{runtime.RuntimeIDLabelKey: "runtime_agent"},
	}
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, expected))
	if err != nil {
		t.Fatalf("decode managed app: %v", err)
	}
	// Kubernetes prunes observational fields that are not in the ManagedApp
	// desired-state schema. The expected hash must be computed from the same
	// durable subset or the rollout waiter can never converge.
	managed.Spec.BackingServices[0].CurrentResourceUsage = nil
	managed.Metadata.Generation = 7
	managed.Status.ObservedGeneration = 7
	expectedSpecHash := expectedManagedAppSpecHash(app, expected)
	managed.Status.LastAppliedSpecHash = runtime.ManagedAppSpecHash(managed.Spec)

	ready, message := managedAppRuntimeSchedulingReady(managed, true, app, expected, expectedSpecHash)
	if !ready {
		t.Fatalf("expected observed backing service usage to be excluded from rollout identity, got %q", message)
	}
}

func TestDeploymentRolloutPolicyReadyRejectsRecreateForOnlineRestart(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:         "registry.pull.example/fugue-apps/demo@sha256:new",
			Ports:         []int{8080},
			Replicas:      1,
			RuntimeID:     "runtime_agent",
			RestartToken:  "restart_new",
			RolloutIntent: model.AppRolloutIntentOnlineRestart,
		},
	}
	expected, found := expectedManagedAppDeployment(app, runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected deployment object")
	}

	recreate := expected
	recreate.Spec.Strategy.Type = "Recreate"
	recreate.Spec.Strategy.RollingUpdate.MaxUnavailable = nil
	recreate.Spec.Strategy.RollingUpdate.MaxSurge = nil

	ready, message := deploymentRolloutPolicyReady(recreate, true, expected)
	if ready {
		t.Fatal("expected recreate rollout policy to be rejected for online restart")
	}
	if !strings.Contains(message, "rollout annotations") && !strings.Contains(message, "strategy RollingUpdate") {
		t.Fatalf("expected rollout policy mismatch message, got %q", message)
	}
}

func TestDeploymentSchedulingReadyForRolloutSkipsDisabledApp(t *testing.T) {
	t.Parallel()

	deployment := kubeDeployment{}
	expected := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{
			runtime.RuntimeIDLabelKey: "runtime_agent",
			runtime.TenantIDLabelKey:  "tenant_owner",
		},
		Tolerations: []runtime.Toleration{
			{
				Key:      runtime.TenantTaintKey,
				Operator: "Equal",
				Value:    "tenant_owner",
				Effect:   "NoSchedule",
			},
		},
	}

	ready, message := deploymentSchedulingReadyForRollout(deployment, 0, expected)
	if !ready {
		t.Fatalf("expected disabled app rollout to skip deployment scheduling checks, got %q", message)
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
	markManagedAppFixtureApplied(t, managedApp)

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

func TestManagedAppRolloutFailureIgnoresStaleExpectedSpec(t *testing.T) {
	t.Parallel()

	oldApp := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:        "ghcr.io/example/demo:old",
			Replicas:     1,
			RuntimeID:    "runtime_demo",
			RestartToken: "restart_old",
		},
	}
	newApp := oldApp
	newApp.Spec.Image = "ghcr.io/example/demo:new"
	newApp.Spec.RestartToken = "restart_new"

	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(oldApp, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("decode managed app: %v", err)
	}
	managed.Metadata.Generation = 3
	managed.Status.Phase = runtime.ManagedAppPhaseError
	managed.Status.Message = "pod demo-old failed: Evicted: old pod"
	managed.Status.ObservedGeneration = 3
	managed.Status.LastAppliedSpecHash = runtime.ManagedAppSpecHash(managed.Spec)

	expected, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(newApp, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("decode expected managed app: %v", err)
	}
	expectedSpecHash := runtime.ManagedAppSpecHash(expected.Spec)

	if got := managedAppRolloutFailure(managed, true, expectedSpecHash); got != "" {
		t.Fatalf("expected stale managed app error to be ignored, got %q", got)
	}

	managed.Spec = expected.Spec
	managed.Status.LastAppliedSpecHash = expectedSpecHash
	if got := managedAppRolloutFailure(managed, true, expectedSpecHash); got == "" {
		t.Fatal("expected current managed app error to fail rollout")
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
	markManagedAppFixtureApplied(t, managedApp)

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
	markManagedAppFixtureApplied(t, managedApp)

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
			cluster.Metadata.Annotations = map[string]string{runtime.CloudNativePGHibernationAnno: runtime.CloudNativePGHibernationOff}
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

func TestWaitForManagedAppRolloutFailsWhenManagedPostgresPodCrashes(t *testing.T) {
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
						Instances:   1,
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

	deployment := readyKubeDeployment(deploymentName, 1)

	managedApp := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managedMetadata := managedApp["metadata"].(map[string]any)
	managedMetadata["generation"] = 1
	markManagedAppFixtureApplied(t, managedApp)

	clusterName := "demo-postgres"
	postgresPod := kubePod{}
	postgresPod.Metadata.Name = clusterName + "-1"
	postgresPod.Metadata.CreationTimestamp = time.Now().UTC()
	postgresPod.Status.Phase = "Running"
	postgresPod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name: "postgres",
		State: kubeRuntimeState{
			Waiting: &kubeStateDetail{
				Reason:  "CrashLoopBackOff",
				Message: "back-off restarting failed container",
			},
		},
		LastState: kubeRuntimeState{
			Terminated: &kubeStateDetail{
				Reason:   "Error",
				ExitCode: 4,
			},
		},
	}}

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
		case cloudNativePGClusterAPIPath(namespace, clusterName):
			cluster := kubeCloudNativePGCluster{}
			cluster.Metadata.Name = clusterName
			cluster.Spec.Instances = 1
			cluster.Status.ReadyInstances = 0
			if err := json.NewEncoder(w).Encode(cluster); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/pods":
			switch r.URL.Query().Get("labelSelector") {
			case managedAppPodLabelSelector(app):
				if err := json.NewEncoder(w).Encode(kubePodList{}); err != nil {
					t.Fatalf("encode app pods: %v", err)
				}
			case fmt.Sprintf(managedPostgresPodSelectorTemplate, clusterName):
				if err := json.NewEncoder(w).Encode(kubePodList{Items: []kubePod{postgresPod}}); err != nil {
					t.Fatalf("encode postgres pods: %v", err)
				}
			default:
				http.NotFound(w, r)
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
	if !strings.Contains(err.Error(), "CrashLoopBackOff") && !strings.Contains(err.Error(), "exit_code=4") {
		t.Fatalf("expected postgres pod failure in error, got %v", err)
	}
}

func TestManagedBackingServiceClusterRolloutReady(t *testing.T) {
	t.Parallel()

	cluster := kubeCloudNativePGCluster{}
	cluster.Metadata.Annotations = map[string]string{runtime.CloudNativePGHibernationAnno: runtime.CloudNativePGHibernationOff}
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

func TestManagedBackingServiceClusterRolloutReadyHandlesSuspendAndResume(t *testing.T) {
	t.Parallel()

	deployment := runtime.ManagedBackingServiceDeployment{
		ResourceName:     "demo-postgres",
		DesiredInstances: 1,
		Suspended:        true,
	}
	cluster := kubeCloudNativePGCluster{}
	cluster.Metadata.Annotations = map[string]string{runtime.CloudNativePGHibernationAnno: runtime.CloudNativePGHibernationOn}
	cluster.Spec.Instances = 1
	cluster.Status.Conditions = []runtime.ManagedAppCondition{{
		Type:    runtime.CloudNativePGHibernationAnno,
		Status:  "True",
		Reason:  "Hibernated",
		Message: "Cluster has been hibernated",
	}}

	ready, message := managedBackingServiceClusterRolloutReadyForDeployment(deployment, cluster, true)
	if !ready || !strings.Contains(message, "suspended") {
		t.Fatalf("expected fully hibernated cluster to complete suspension, ready=%v message=%q", ready, message)
	}

	cluster.Status.ReadyInstances = 1
	ready, _ = managedBackingServiceClusterRolloutReadyForDeployment(deployment, cluster, true)
	if ready {
		t.Fatal("expected remaining ready instance to keep suspend rollout pending")
	}

	deployment.Suspended = false
	cluster.Metadata.Annotations[runtime.CloudNativePGHibernationAnno] = runtime.CloudNativePGHibernationOn
	cluster.Status.ReadyInstances = 0
	ready, message = managedBackingServiceClusterRolloutReadyForDeployment(deployment, cluster, true)
	if ready || !strings.Contains(message, "disabled") {
		t.Fatalf("expected resume to wait for annotation off, ready=%v message=%q", ready, message)
	}

	cluster.Metadata.Annotations[runtime.CloudNativePGHibernationAnno] = runtime.CloudNativePGHibernationOff
	cluster.Status.Conditions = nil
	cluster.Status.ReadyInstances = 1
	cluster.Status.CurrentPrimary = "demo-postgres-1"
	ready, message = managedBackingServiceClusterRolloutReadyForDeployment(deployment, cluster, true)
	if !ready || !strings.Contains(message, "ready") {
		t.Fatalf("expected non-hibernated primary to complete resume, ready=%v message=%q", ready, message)
	}
}

func TestManagedBackingServicesRolloutRequiresZeroObservedPodsForSuspension(t *testing.T) {
	t.Parallel()

	const (
		namespace   = "tenant-demo"
		clusterName = "demo-postgres"
	)
	var remainingPods atomic.Int32
	var denyPodObservation atomic.Bool
	remainingPods.Store(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, clusterName):
			cluster := kubeCloudNativePGCluster{}
			cluster.Metadata.Name = clusterName
			cluster.Metadata.Annotations = map[string]string{runtime.CloudNativePGHibernationAnno: runtime.CloudNativePGHibernationOn}
			cluster.Spec.Instances = 1
			cluster.Status.Conditions = []runtime.ManagedAppCondition{{
				Type:   runtime.CloudNativePGHibernationAnno,
				Status: "True",
				Reason: "Hibernated",
			}}
			if err := json.NewEncoder(w).Encode(cluster); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/pods":
			if denyPodObservation.Load() {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"message":"forbidden"}`))
				return
			}
			pods := kubePodList{}
			if remainingPods.Load() > 0 {
				pod := kubePod{}
				pod.Metadata.Name = "demo-postgres-1"
				pods.Items = []kubePod{pod}
			}
			if err := json.NewEncoder(w).Encode(pods); err != nil {
				t.Fatalf("encode pods: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &kubeClient{
		client:      server.Client(),
		baseURL:     server.URL,
		bearerToken: "test",
		namespace:   namespace,
	}
	service := &Service{Logger: log.New(io.Discard, "", 0)}
	deployments := []runtime.ManagedBackingServiceDeployment{{
		ResourceName:     clusterName,
		ResourceKind:     runtime.CloudNativePGClusterKind,
		DesiredInstances: 1,
		Suspended:        true,
	}}

	ready, message, _, err := service.managedBackingServicesRolloutReady(context.Background(), client, namespace, deployments)
	if err != nil {
		t.Fatalf("check suspension rollout: %v", err)
	}
	if ready || !strings.Contains(message, "1 remaining") {
		t.Fatalf("expected observed pod to keep suspension pending, ready=%v message=%q", ready, message)
	}

	remainingPods.Store(0)
	ready, message, _, err = service.managedBackingServicesRolloutReady(context.Background(), client, namespace, deployments)
	if err != nil {
		t.Fatalf("check completed suspension rollout: %v", err)
	}
	if !ready || message != "" {
		t.Fatalf("expected zero observed pods to complete suspension, ready=%v message=%q", ready, message)
	}

	denyPodObservation.Store(true)
	ready, _, _, err = service.managedBackingServicesRolloutReady(context.Background(), client, namespace, deployments, true)
	if err == nil || !strings.Contains(err.Error(), "cannot verify zero database pods") {
		t.Fatalf("expected strict suspension verification to reject unavailable pod evidence, ready=%v err=%v", ready, err)
	}
}

func TestWaitForManagedBackingServiceLifecycleChecksSuspendedDatabaseWhenAppIsDisabled(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec:     model.AppSpec{Replicas: 0},
		BackingServices: []model.BackingService{{
			ID:          "service_postgres",
			Name:        "demo-postgres",
			Type:        model.BackingServiceTypePostgres,
			Provisioner: model.BackingServiceProvisionerManaged,
			Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
				ServiceName: "demo-postgres",
				Instances:   1,
				Suspended:   true,
			}},
		}},
		Bindings: []model.ServiceBinding{{
			AppID:     "app_demo",
			ServiceID: "service_postgres",
		}},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case cloudNativePGClusterAPIPath(namespace, "demo-postgres"):
			cluster := kubeCloudNativePGCluster{}
			cluster.Metadata.Name = "demo-postgres"
			cluster.Metadata.Annotations = map[string]string{runtime.CloudNativePGHibernationAnno: runtime.CloudNativePGHibernationOn}
			cluster.Spec.Instances = 1
			cluster.Status.Conditions = []runtime.ManagedAppCondition{{
				Type:   runtime.CloudNativePGHibernationAnno,
				Status: "True",
				Reason: "Hibernated",
			}}
			if err := json.NewEncoder(w).Encode(cluster); err != nil {
				t.Fatalf("encode cluster: %v", err)
			}
		case "/api/v1/namespaces/" + namespace + "/pods":
			if err := json.NewEncoder(w).Encode(kubePodList{}); err != nil {
				t.Fatalf("encode pods: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	service := &Service{
		Config: config.ControllerConfig{
			ManagedAppRolloutTimeout: time.Second,
			PollInterval:             10 * time.Millisecond,
		},
		Logger: log.New(io.Discard, "", 0),
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      server.Client(),
				baseURL:     server.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
	}
	if err := service.waitForManagedBackingServiceLifecycle(context.Background(), app, "", "service_postgres"); err != nil {
		t.Fatalf("expected disabled owner app database suspension to be verified, got %v", err)
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
	markManagedAppFixtureApplied(t, managedApp)

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
			cluster.Metadata.Annotations = map[string]string{runtime.CloudNativePGHibernationAnno: runtime.CloudNativePGHibernationOff}
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
							Name              string            `json:"name"`
							CreationTimestamp time.Time         `json:"creationTimestamp"`
							DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
							Labels            map[string]string `json:"labels,omitempty"`
							Annotations       map[string]string `json:"annotations,omitempty"`
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
							Name              string            `json:"name"`
							CreationTimestamp time.Time         `json:"creationTimestamp"`
							DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
							Labels            map[string]string `json:"labels,omitempty"`
							Annotations       map[string]string `json:"annotations,omitempty"`
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

func TestCleanupRetainedManagedAppEvictedPodsDeletesOnlyProvenOldReplicaSetPods(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC)
	app := model.App{ID: "app_demo", TenantID: "tenant_demo", Name: "Demo App"}
	namespace := runtime.NamespaceForTenant(app.TenantID)

	old := retainedEvictedPodFixture(app, "old", "uid-old", now.Add(-48*time.Hour))
	rejected := retainedEvictedPodFixture(app, "rejected", "uid-rejected", time.Time{})
	rejected.Metadata.CreationTimestamp = now.Add(-72 * time.Hour)
	rejected.ObservedStartTime = rejected.Metadata.CreationTimestamp.Add(time.Second)
	rejected.Status.Message = "Pod was rejected: The node had condition: [DiskPressure]."

	recent := retainedEvictedPodFixture(app, "recent", "uid-recent", now.Add(-2*time.Hour))
	nonEvicted := retainedEvictedPodFixture(app, "non-evicted", "uid-non-evicted", now.Add(-48*time.Hour))
	nonEvicted.Status.Reason = "Error"
	wrongOwner := retainedEvictedPodFixture(app, "wrong-owner", "uid-wrong-owner", now.Add(-48*time.Hour))
	wrongOwner.ObservedOwnerReferences[0].Kind = "Job"
	wrongApp := retainedEvictedPodFixture(app, "wrong-app", "uid-wrong-app", now.Add(-48*time.Hour))
	wrongApp.Metadata.Labels[runtime.FugueLabelAppID] = "app_other"
	missingUID := retainedEvictedPodFixture(app, "missing-uid", "", now.Add(-48*time.Hour))
	terminating := retainedEvictedPodFixture(app, "terminating", "uid-terminating", now.Add(-48*time.Hour))
	terminating.Metadata.DeletionTimestamp = now.Add(-time.Hour).Format(time.RFC3339)
	missingTerminalProof := retainedEvictedPodFixture(app, "missing-terminal-proof", "uid-missing-terminal", time.Time{})
	missingTerminalProof.Metadata.CreationTimestamp = now.Add(-72 * time.Hour)
	missingTerminalProof.ObservedStartTime = missingTerminalProof.Metadata.CreationTimestamp.Add(-time.Second)
	missingTerminalProof.Status.Message = "Pod was rejected: The node had condition: [DiskPressure]."

	wantUID := map[string]string{
		"old":      "uid-old",
		"rejected": "uid-rejected",
	}
	deleted := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || !strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/"+namespace+"/pods/") {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/"+namespace+"/pods/")
		uid, ok := wantUID[name]
		if !ok {
			t.Fatalf("unexpected pod delete %q", name)
		}
		var options struct {
			APIVersion         string `json:"apiVersion"`
			Kind               string `json:"kind"`
			GracePeriodSeconds int64  `json:"gracePeriodSeconds"`
			PropagationPolicy  string `json:"propagationPolicy"`
			Preconditions      struct {
				UID string `json:"uid"`
			} `json:"preconditions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&options); err != nil {
			t.Fatalf("decode delete options: %v", err)
		}
		if options.APIVersion != "v1" || options.Kind != "DeleteOptions" || options.GracePeriodSeconds != 0 || options.PropagationPolicy != "Background" {
			t.Fatalf("unexpected delete options: %+v", options)
		}
		if options.Preconditions.UID != uid {
			t.Fatalf("expected UID precondition %q for %s, got %q", uid, name, options.Preconditions.UID)
		}
		deleted[name] = true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &kubeClient{client: server.Client(), baseURL: server.URL, bearerToken: "test", namespace: namespace}
	service := &Service{now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0)}
	pods := []kubePod{old, rejected, recent, nonEvicted, wrongOwner, wrongApp, missingUID, terminating, missingTerminalProof}
	if err := service.cleanupRetainedManagedAppEvictedPods(context.Background(), client, namespace, app, pods); err != nil {
		t.Fatalf("cleanup retained evicted pods: %v", err)
	}
	if len(deleted) != len(wantUID) || !deleted["old"] || !deleted["rejected"] {
		t.Fatalf("expected only proven old evicted pods to be deleted, got %+v", deleted)
	}
}

func TestCleanupRetainedManagedAppEvictedPodsBoundsBatchAndSkipsUIDConflict(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC)
	app := model.App{ID: "app_demo", TenantID: "tenant_demo", Name: "demo"}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	pods := make([]kubePod, 0, managedAppEvictedPodBatchLimit+2)
	for i := 0; i < managedAppEvictedPodBatchLimit+2; i++ {
		name := fmt.Sprintf("pod-%02d", i)
		pods = append(pods, retainedEvictedPodFixture(app, name, "uid-"+name, now.Add(-time.Duration(72-i)*time.Hour)))
	}

	deleted := make([]string, 0, managedAppEvictedPodBatchLimit)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/"+namespace+"/pods/")
		deleted = append(deleted, name)
		if name == "pod-03" {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"UID precondition failed"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &kubeClient{client: server.Client(), baseURL: server.URL, bearerToken: "test", namespace: namespace}
	service := &Service{now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0)}
	if err := service.cleanupRetainedManagedAppEvictedPods(context.Background(), client, namespace, app, pods); err != nil {
		t.Fatalf("expected UID conflict to fail closed without blocking the batch, got %v", err)
	}
	if len(deleted) != managedAppEvictedPodBatchLimit {
		t.Fatalf("expected bounded batch of %d deletes, got %d: %+v", managedAppEvictedPodBatchLimit, len(deleted), deleted)
	}
	for i, name := range deleted {
		want := fmt.Sprintf("pod-%02d", i)
		if name != want {
			t.Fatalf("expected oldest pods first; delete %d=%q, want %q", i, name, want)
		}
	}
}

func retainedEvictedPodFixture(app model.App, name, uid string, terminalAt time.Time) kubePod {
	pod := kubePod{}
	pod.Metadata.Name = name
	pod.ObservedUID = uid
	pod.Metadata.CreationTimestamp = terminalAt.Add(-time.Hour)
	pod.Metadata.Labels = map[string]string{
		runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue,
		runtime.FugueLabelName:      runtime.RuntimeResourceName(app.Name),
		runtime.FugueLabelAppID:     app.ID,
		runtime.FugueLabelTenantID:  app.TenantID,
	}
	pod.ObservedOwnerReferences = []kubePodOwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "ReplicaSet",
		Name:       "demo-rs",
		UID:        "rs-uid",
		Controller: true,
	}}
	pod.Status.Phase = "Failed"
	pod.Status.Reason = "Evicted"
	if !terminalAt.IsZero() {
		pod.Status.Conditions = []kubePodCondition{{
			Type:               "DisruptionTarget",
			Status:             "True",
			Reason:             "TerminationByKubelet",
			LastTransitionTime: terminalAt,
		}}
	}
	return pod
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

func markManagedAppFixtureApplied(t *testing.T, managedApp map[string]any) {
	t.Helper()

	managed, err := runtime.ManagedAppObjectFromMap(managedApp)
	if err != nil {
		t.Fatalf("decode managed app fixture: %v", err)
	}
	metadata, _ := managedApp["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		managedApp["metadata"] = metadata
	}
	generation := managed.Metadata.Generation
	if generation <= 0 {
		generation = 1
		metadata["generation"] = generation
	}

	status, _ := managedApp["status"].(map[string]any)
	if status == nil {
		status = map[string]any{}
		managedApp["status"] = status
	}
	status["observedGeneration"] = generation
	status["lastAppliedSpecHash"] = runtime.ManagedAppSpecHash(managed.Spec)
}

func readyTemplatePod(name string, deployment kubeDeployment, resources kubeResourceRequirements) kubePod {
	pod := kubePod{}
	pod.Metadata.Name = name
	pod.Metadata.Labels = deployment.Spec.Template.Metadata.Labels
	pod.Metadata.Annotations = deployment.Spec.Template.Metadata.Annotations
	pod.Spec.TerminationGracePeriodSeconds = deployment.Spec.Template.Spec.TerminationGracePeriodSeconds
	for _, container := range deployment.Spec.Template.Spec.Containers {
		container.Resources = resources
		pod.Spec.Containers = append(pod.Spec.Containers, container)
	}
	for _, container := range deployment.Spec.Template.Spec.InitContainers {
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, container)
	}
	pod.Status.Phase = "Running"
	pod.Status.Conditions = []kubePodCondition{{Type: "Ready", Status: "True"}}
	return pod
}

func setKubeDeploymentPrimaryImage(deployment *kubeDeployment, name, image string) {
	deployment.Spec.Template.Spec.Containers = []kubeContainerSpec{
		{Name: name, Image: image},
	}
}

func TestCaptureDeploymentRolloutFailureEvidenceCapturesLogsEventsSnapshotsAndReleaseLink(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Collector Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "ops", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{Image: "registry.example/fugue-apps/demo:broken", Replicas: 1})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	app = runtime.Renderer{}.PrepareApp(app)
	spec := app.Spec
	op, err := stateStore.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDeploy, AppID: app.ID, DesiredSpec: &spec})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	attempt, err := stateStore.CreateReleaseAttempt(model.ReleaseAttempt{
		TenantID:          tenant.ID,
		ProjectID:         project.ID,
		AppID:             app.ID,
		TriggerType:       model.ReleaseAttemptTriggerImageTrackingManualSync,
		TriggerActorType:  model.ReleaseAttemptActorUser,
		SourceOperationID: op.ID,
		RootOperationID:   op.ID,
		Status:            model.ReleaseAttemptStatusRollingOut,
		Confidence:        model.OperationEvidenceConfidenceEvidenceBacked,
	})
	if err != nil {
		t.Fatalf("create release attempt: %v", err)
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	deployment, found := expectedManagedAppDeployment(app, runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected deployment fixture")
	}
	deployment.Metadata.Name = runtime.RuntimeAppResourceName(app)
	deployment.Metadata.Namespace = namespace
	deployment.Metadata.Generation = 7
	deployment.Status.ObservedGeneration = 7
	deployment.Status.Replicas = 1
	deployment.Status.UpdatedReplicas = 1

	pod := readyTemplatePod("demo-crash", deployment, kubeResourceRequirements{})
	pod.Status.Phase = "Running"
	pod.Spec.NodeName = "node-a"
	pod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name: "demo",
		State: kubeRuntimeState{Terminated: &kubeStateDetail{
			Reason:     "Error",
			Message:    "process exited",
			ExitCode:   1,
			StartedAt:  time.Now().Add(-5 * time.Second).UTC().Format(time.RFC3339),
			FinishedAt: time.Now().UTC().Format(time.RFC3339),
		}},
		RestartCount: 1,
	}}

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/log"):
			if r.URL.Query().Get("previous") == "true" {
				_, _ = w.Write([]byte("startup failed token=super-secret user@example.com\n"))
				return
			}
			_, _ = w.Write([]byte("current log line\n"))
		case r.URL.Path == "/api/v1/namespaces/"+namespace+"/events":
			_ = json.NewEncoder(w).Encode(kubeEventList{Items: []kubeEvent{{
				Type:    "Warning",
				Reason:  "BackOff",
				Message: "Back-off restarting failed container demo",
			}}})
		case r.URL.Path == "/apis/apps/v1/namespaces/"+namespace+"/replicasets":
			_ = json.NewEncoder(w).Encode(kubeReplicaSetList{Items: []kubeReplicaSet{{}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()
	client := &kubeClient{client: kubeServer.Client(), baseURL: kubeServer.URL, bearerToken: "test", namespace: namespace}
	svc := &Service{Store: stateStore, Logger: log.New(io.Discard, "", 0)}

	primaryID := svc.captureDeploymentRolloutFailureEvidence(context.Background(), client, app, op.ID, namespace, deployment, []kubePod{pod}, "pod demo-crash failed")
	if primaryID == "" {
		t.Fatal("expected primary evidence id")
	}
	evidence, err := stateStore.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: tenant.ID, OperationID: op.ID, Limit: 100})
	if err != nil {
		t.Fatalf("list evidence: %v", err)
	}
	for _, typ := range []string{
		model.OperationEvidenceTypeRolloutContainerTerminated,
		model.OperationEvidenceTypeRolloutPodSnapshot,
		model.OperationEvidenceTypeRolloutPreviousLogs,
		model.OperationEvidenceTypeRolloutCurrentLogs,
		model.OperationEvidenceTypeRolloutKubernetesEvent,
		model.OperationEvidenceTypeRolloutDeploymentSnapshot,
		model.OperationEvidenceTypeRolloutReplicaSetSnapshot,
	} {
		if !operationEvidenceHasType(evidence, typ) {
			t.Fatalf("expected evidence type %s in %+v", typ, evidence)
		}
	}
	for _, item := range evidence {
		if item.ReleaseAttemptID != attempt.ID {
			t.Fatalf("expected evidence %s linked to release attempt %s, got %q", item.ID, attempt.ID, item.ReleaseAttemptID)
		}
		if item.Type == model.OperationEvidenceTypeRolloutPreviousLogs {
			logTail, _ := item.Payload["log_tail"].(string)
			if strings.Contains(logTail, "super-secret") || strings.Contains(logTail, "user@example.com") {
				t.Fatalf("expected previous log payload redacted, got %q", logTail)
			}
			if item.RedactionStatus != model.OperationEvidenceRedactionRedacted {
				t.Fatalf("expected redaction status redacted for previous logs, got %+v", item)
			}
		}
	}
}

func TestCaptureDeploymentRolloutFailureEvidenceCollectorErrorDoesNotHidePrimaryFailure(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, _ := stateStore.CreateTenant("Collector Error Tenant")
	project, _ := stateStore.CreateProject(tenant.ID, "ops", "")
	app, _ := stateStore.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{Image: "registry.example/fugue-apps/demo:broken", Replicas: 1})
	app = runtime.Renderer{}.PrepareApp(app)
	spec := app.Spec
	op, _ := stateStore.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDeploy, AppID: app.ID, DesiredSpec: &spec})
	namespace := runtime.NamespaceForTenant(app.TenantID)
	deployment, _ := expectedManagedAppDeployment(app, runtime.SchedulingConstraints{})
	deployment.Metadata.Name = runtime.RuntimeAppResourceName(app)
	pod := readyTemplatePod("demo-crash", deployment, kubeResourceRequirements{})
	pod.Status.ContainerStatuses = []kubeContainerStatus{{Name: "demo", State: kubeRuntimeState{Waiting: &kubeStateDetail{Reason: "CrashLoopBackOff", Message: "back-off restarting failed container"}}}}

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "collector backend unavailable", http.StatusInternalServerError)
	}))
	defer kubeServer.Close()
	client := &kubeClient{client: kubeServer.Client(), baseURL: kubeServer.URL, bearerToken: "test", namespace: namespace}
	svc := &Service{Store: stateStore, Logger: log.New(io.Discard, "", 0)}

	primaryID := svc.captureDeploymentRolloutFailureEvidence(context.Background(), client, app, op.ID, namespace, deployment, []kubePod{pod}, "pod demo-crash failed")
	if primaryID == "" {
		t.Fatal("expected primary evidence id despite collector errors")
	}
	evidence, err := stateStore.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: tenant.ID, OperationID: op.ID, Limit: 100})
	if err != nil {
		t.Fatalf("list evidence: %v", err)
	}
	if !operationEvidenceHasType(evidence, model.OperationEvidenceTypeRolloutContainerTerminated) {
		t.Fatalf("expected primary pod failure evidence, got %+v", evidence)
	}
	if !operationEvidenceHasType(evidence, model.OperationEvidenceTypeCollectorError) {
		t.Fatalf("expected collector_error evidence, got %+v", evidence)
	}
}

func TestClassifyKubernetesEventEvidenceTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		event kubeEvent
		want  string
	}{
		{name: "scheduling", event: kubeEvent{Reason: "FailedScheduling", Message: "0/3 nodes are available"}, want: model.OperationEvidenceTypeSchedulerFailure},
		{name: "image pull", event: kubeEvent{Reason: "ErrImagePull", Message: "pull access denied"}, want: model.OperationEvidenceTypeImagePullFailure},
		{name: "mount", event: kubeEvent{Reason: "FailedMount", Message: "Unable to attach or mount volumes"}, want: model.OperationEvidenceTypeVolumeMountFailure},
		{name: "readiness", event: kubeEvent{Reason: "Unhealthy", Message: "Readiness probe failed: HTTP 500"}, want: model.OperationEvidenceTypeReadinessProbeFailure},
		{name: "liveness", event: kubeEvent{Reason: "Unhealthy", Message: "Liveness probe failed: timeout"}, want: model.OperationEvidenceTypeLivenessProbeFailure},
		{name: "startup", event: kubeEvent{Reason: "Unhealthy", Message: "Startup probe failed: timeout"}, want: model.OperationEvidenceTypeStartupProbeFailure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyKubernetesEventEvidenceType(tc.event); got != tc.want {
				t.Fatalf("expected %s, got %s", tc.want, got)
			}
		})
	}
}

func TestKubernetesEventEvidenceSummarizesSameNodeOnlineMountUnsupported(t *testing.T) {
	t.Parallel()

	app := model.App{
		Spec: model.AppSpec{
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode:             model.AppPersistentStorageModeMovableRWO,
				StorageClassName: model.AppStorageClassFugueWorkspaceRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/workspace"},
				},
			},
		},
	}
	event := kubeEvent{
		Reason:  "FailedMount",
		Message: "MountVolume.SetUp failed for volume \"pvc\" : rpc error: code = Internal desc = verifyMount: device already mounted",
	}
	if got := classifyKubernetesEventEvidenceType(event); got != model.OperationEvidenceTypeVolumeMountFailure {
		t.Fatalf("expected volume mount evidence type, got %s", got)
	}
	if got := kubernetesEventEvidenceSummary(app, event); got != "storage class fugue-workspace-rwo does not support same-node online dual mount" {
		t.Fatalf("expected storage class capability summary, got %q", got)
	}
	payload := map[string]any{}
	augmentKubernetesEventEvidencePayload(app, event, payload)
	if got := payload["storage_rollout_failure"]; got != "same_node_online_dual_mount_unsupported" {
		t.Fatalf("expected storage rollout failure payload, got %#v", payload)
	}
	if got := payload["same_node_online_mount_supported"]; got != false {
		t.Fatalf("expected same-node mount support=false payload, got %#v", payload)
	}
}

func operationEvidenceHasType(items []model.OperationEvidence, typ string) bool {
	for _, item := range items {
		if item.Type == typ {
			return true
		}
	}
	return false
}

func TestPrimaryFailingContainerStatusPrefersInitContainerFailure(t *testing.T) {
	t.Parallel()

	pod := kubePod{}
	pod.Status.InitContainerStatuses = []kubeContainerStatus{{
		Name:  "init-db",
		State: kubeRuntimeState{Terminated: &kubeStateDetail{Reason: "Error", Message: "migration failed", ExitCode: 2}},
	}}
	pod.Status.ContainerStatuses = []kubeContainerStatus{{
		Name:  "api",
		State: kubeRuntimeState{Waiting: &kubeStateDetail{Reason: "CrashLoopBackOff", Message: "backoff"}},
	}}
	status, detail, stateKind := primaryFailingContainerStatus(pod)
	if status.Name != "init-db" || detail.ExitCode != 2 || stateKind != "terminated" {
		t.Fatalf("expected init container failure to be primary, status=%+v detail=%+v state=%s", status, detail, stateKind)
	}
	if got := classifyPodFailureEvidenceType(pod, status, detail); got != model.OperationEvidenceTypeRolloutContainerTerminated {
		t.Fatalf("expected terminated evidence type, got %s", got)
	}
}
