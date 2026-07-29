package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestManagedAppLiveGuardAllowsInitialDeploymentOnlyWhenDeploymentIsAbsent(t *testing.T) {
	app := managedAppLiveGuardTestApp(nil)
	managed := managedAppLiveGuardObject(t, app, runtime.SchedulingConstraints{})
	client := managedAppLiveGuardClient(t, managed, kubeDeployment{}, false, false, nil)

	prepared, err := (&Service{Renderer: runtime.Renderer{}}).prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, app, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
	)
	if err != nil {
		t.Fatalf("initial deployment must be allowed when no workload has ever served: %v", err)
	}
	if prepared.ID != app.ID {
		t.Fatalf("unexpected prepared app: %+v", prepared)
	}

	managed.Status.CurrentReleaseReadyAt = "2026-07-28T00:00:00Z"
	if _, err := (&Service{Renderer: runtime.Renderer{}}).prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, app, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
	); err == nil || !strings.Contains(err.Error(), "live deployment is missing") {
		t.Fatalf("historical serving evidence must make a missing deployment fail closed, got %v", err)
	}
}

func TestManagedAppLiveGuardAllowsReadyStrategyOnlyUpgradeFromRecreate(t *testing.T) {
	app := managedAppLiveGuardTestApp(nil)
	managed := managedAppLiveGuardObject(t, app, runtime.SchedulingConstraints{})
	managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1}
	svc := &Service{Renderer: runtime.Renderer{}}
	live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(app), runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected rendered deployment")
	}
	managedAppLiveGuardMarkReady(&live, 1)
	live.Spec.Strategy.Type = "Recreate"
	live.Spec.Strategy.RollingUpdate.MaxUnavailable = nil
	live.Spec.Strategy.RollingUpdate.MaxSurge = nil
	client := managedAppLiveGuardClient(t, managed, live, true, true, nil)

	prepared, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, app, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
	)
	if err != nil {
		t.Fatalf("a ready same-template Recreate to RollingUpdate metadata transition must be safe: %v", err)
	}
	deployment := svc.zeroDowntimeManagedAppDeployment(prepared, runtime.SchedulingConstraints{})
	if !zeroDowntimeDeploymentStrategyIsSafe(deployment) {
		t.Fatalf("expected safe RollingUpdate after preparation: %#v", deployment)
	}
}

func TestManagedAppLiveGuardRefusesUnknownReleaseIdentity(t *testing.T) {
	app := managedAppLiveGuardTestApp(nil)
	managed := managedAppLiveGuardObject(t, app, runtime.SchedulingConstraints{})
	managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1}
	svc := &Service{Renderer: runtime.Renderer{}}
	live, _ := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(app), runtime.SchedulingConstraints{})
	managedAppLiveGuardMarkReady(&live, 1)
	live.Metadata.Annotations[runtime.FugueAnnotationReleaseKey] = "unknown-live-release"
	client := managedAppLiveGuardClient(t, managed, live, true, true, nil)

	_, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, app, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
	)
	if err == nil || !strings.Contains(err.Error(), "matches neither current snapshot") {
		t.Fatalf("an unproven live identity must fail closed, got %v", err)
	}
}

func TestManagedAppLiveGuardRefusesLocalRWOReplacementWithoutExactNodeProof(t *testing.T) {
	storage := &model.AppPersistentStorageSpec{
		Mode:             model.AppPersistentStorageModeMovableRWO,
		StorageClassName: model.AppStorageClassFugueLocalRWO,
		Mounts: []model.AppPersistentStorageMount{
			{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
		},
	}
	current := managedAppLiveGuardTestApp(storage)
	desired := current
	desired.Spec.Env = map[string]string{"MODE": "new"}
	managed := managedAppLiveGuardObject(t, current, runtime.SchedulingConstraints{})
	managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1}
	svc := &Service{Renderer: runtime.Renderer{}}
	live, _ := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(current), runtime.SchedulingConstraints{})
	managedAppLiveGuardMarkReady(&live, 1)
	client := managedAppLiveGuardClient(t, managed, live, true, true, nil)

	_, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, desired, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
	)
	if err == nil || !strings.Contains(err.Error(), "no validated current-node placement") {
		t.Fatalf("local RWO replacement without an exact live-node pin must fail closed, got %v", err)
	}
}

func TestManagedAppLiveGuardPreservesIntentAndRefusesIgnoredDrainTemplateDriftWithoutNodeProof(t *testing.T) {
	storage := &model.AppPersistentStorageSpec{
		Mode:             model.AppPersistentStorageModeMovableRWO,
		StorageClassName: model.AppStorageClassFugueLocalRWO,
		Mounts: []model.AppPersistentStorageMount{
			{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
		},
	}
	app := managedAppLiveGuardTestApp(storage)
	app.Spec.RolloutIntent = model.AppRolloutIntentOnlineRestart
	managed := managedAppLiveGuardObject(t, app, runtime.SchedulingConstraints{})
	managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1}
	svc := &Service{Renderer: runtime.Renderer{}}
	live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(app), runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected rendered deployment")
	}
	managedAppLiveGuardMarkReady(&live, 1)
	for _, key := range []string{
		"fugue.io/drain-mode",
		"fugue.io/drain-timeout-seconds",
		"fugue.io/drain-quiet-period-seconds",
		"fugue.io/drain-agent-port",
		"fugue.io/termination-grace-min-seconds",
	} {
		delete(live.Spec.Template.Metadata.Annotations, key)
	}
	initContainers := make([]kubeContainerSpec, 0, len(live.Spec.Template.Spec.InitContainers))
	for _, container := range live.Spec.Template.Spec.InitContainers {
		if container.Name != "fugue-drain-agent" {
			initContainers = append(initContainers, container)
		}
	}
	live.Spec.Template.Spec.InitContainers = initContainers
	for index := range live.Spec.Template.Spec.Containers {
		live.Spec.Template.Spec.Containers[index].Lifecycle = nil
	}
	client := managedAppLiveGuardClient(t, managed, live, true, true, nil)

	_, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, app, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
	)
	if err == nil || !strings.Contains(err.Error(), "no validated current-node placement") {
		t.Fatalf("release-key-ignored drain drift must be treated as a pod-template change, got %v", err)
	}
}

func TestManagedRolloutIntentPreservationRequiresUnchangedDesiredStateAndScheduling(t *testing.T) {
	current := managedAppLiveGuardTestApp(nil)
	current.Spec.RolloutIntent = model.AppRolloutIntentOnlineRestart
	desired := current
	desired.Spec.RolloutIntent = ""

	if got := managedRolloutIntentForDesiredState(current, desired, runtime.SchedulingConstraints{}, runtime.SchedulingConstraints{}); got != current.Spec.RolloutIntent {
		t.Fatalf("expected unchanged desired state to preserve %q, got %q", current.Spec.RolloutIntent, got)
	}

	changedSpec := desired
	changedSpec.Spec.Ports = []int{9090}
	if got := managedRolloutIntentForDesiredState(current, changedSpec, runtime.SchedulingConstraints{}, runtime.SchedulingConstraints{}); got != "" {
		t.Fatalf("an unclassified desired-state change must not reuse stale rollout intent, got %q", got)
	}

	changedScheduling := runtime.SchedulingConstraints{NodeSelector: map[string]string{kubeHostnameLabelKey: "node-b"}}
	if got := managedRolloutIntentForDesiredState(current, desired, runtime.SchedulingConstraints{}, changedScheduling); got != "" {
		t.Fatalf("a scheduling change must not reuse stale rollout intent, got %q", got)
	}
}

func TestManagedRolloutIntentAllowsCanonicalPostgresServiceNameRepair(t *testing.T) {
	current, desired := managedPostgresCanonicalizationRolloutApps()
	svc := &Service{Renderer: runtime.Renderer{}}

	intent := svc.managedRolloutIntentForDesiredState(
		current,
		desired,
		runtime.SchedulingConstraints{},
		runtime.SchedulingConstraints{},
	)
	if intent != model.AppRolloutIntentOnlineEnvironmentUpdate {
		t.Fatalf("expected MecGod -> mecgod to use an online environment rollout, got %q", intent)
	}

	desired.Spec.RolloutIntent = intent
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired.Spec}
	decision := svc.zeroDowntimeRolloutGuardDecision(op, current, desired, runtime.SchedulingConstraints{})
	if decision.Refused {
		t.Fatalf("canonical managed postgres hostname repair must pass the zero-downtime guard: %+v", decision)
	}
}

func TestManagedAppLiveGuardAllowsDatabaseLocalizeToRepairCanonicalPostgresName(t *testing.T) {
	current, desired := managedPostgresCanonicalizationRolloutApps()
	managed := managedAppLiveGuardObject(t, current, runtime.SchedulingConstraints{})
	managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1}
	svc := &Service{Renderer: runtime.Renderer{}}
	live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(current), runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected a live deployment fixture")
	}
	managedAppLiveGuardMarkReady(&live, 1)
	client := managedAppLiveGuardClient(t, managed, live, true, true, nil)

	prepared, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(),
		client,
		managed.Metadata.Namespace,
		managed,
		desired,
		model.OperationTypeDatabaseLocalize,
		runtime.SchedulingConstraints{},
	)
	if err != nil {
		t.Fatalf("database localize must repair the generated hostname through a validated online rollout: %v", err)
	}
	if prepared.Spec.RolloutIntent != model.AppRolloutIntentOnlineEnvironmentUpdate {
		t.Fatalf("expected online environment rollout intent, got %q", prepared.Spec.RolloutIntent)
	}
}

func TestManagedRolloutIntentPreservesValidatedIntentAcrossPostgresPlacementOnlyState(t *testing.T) {
	_, current := managedPostgresCanonicalizationRolloutApps()
	current.Spec.RolloutIntent = model.AppRolloutIntentOnlineEnvironmentUpdate
	desired := current
	desired.Spec = *cloneControllerAppSpec(&current.Spec)
	desired.Spec.RolloutIntent = ""
	desired.BackingServices = cloneControllerBackingServices(current.BackingServices)
	desired.Bindings = cloneControllerServiceBindings(current.Bindings)
	desiredPostgres := desired.BackingServices[0].Spec.Postgres
	desiredPostgres.Instances = 1
	desiredPostgres.PrimaryPlacementPendingRebalance = false
	desiredPostgres.SynchronousReplicas = 0

	intent := (&Service{Renderer: runtime.Renderer{}}).managedRolloutIntentForDesiredState(
		current,
		desired,
		runtime.SchedulingConstraints{},
		runtime.SchedulingConstraints{},
	)
	if intent != current.Spec.RolloutIntent {
		t.Fatalf("expected placement-only finalization to preserve %q, got %q", current.Spec.RolloutIntent, intent)
	}
}

func TestManagedRolloutIntentDoesNotHidePostgresCredentialChangeBehindCanonicalization(t *testing.T) {
	current, desired := managedPostgresCanonicalizationRolloutApps()
	desired.BackingServices[0].Spec.Postgres.Password = "rotated-secret"
	desired.Bindings[0].Env["DB_PASSWORD"] = "rotated-secret"

	intent := (&Service{Renderer: runtime.Renderer{}}).managedRolloutIntentForDesiredState(
		current,
		desired,
		runtime.SchedulingConstraints{},
		runtime.SchedulingConstraints{},
	)
	if intent != "" {
		t.Fatalf("a credential change must remain fail-closed, got rollout intent %q", intent)
	}
}

func managedPostgresCanonicalizationRolloutApps() (model.App, model.App) {
	current := managedAppLiveGuardTestApp(nil)
	current.Spec.RolloutIntent = model.AppRolloutIntentOnlineImageUpdate
	current.BackingServices = []model.BackingService{{
		ID:          "service-postgres",
		TenantID:    current.TenantID,
		ProjectID:   current.ProjectID,
		OwnerAppID:  current.ID,
		Name:        "mecgod",
		Type:        model.BackingServiceTypePostgres,
		Provisioner: model.BackingServiceProvisionerManaged,
		Status:      model.BackingServiceStatusActive,
		Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
			Database:    "mecgod",
			User:        "mecgod",
			Password:    "stable-secret",
			ServiceName: "MecGod",
			RuntimeID:   "runtime_managed_shared",
			StorageSize: "1Gi",
			Instances:   1,
		}},
	}}
	current.Bindings = []model.ServiceBinding{{
		ID:        "binding-postgres",
		TenantID:  current.TenantID,
		AppID:     current.ID,
		ServiceID: "service-postgres",
		Env: map[string]string{
			"DB_HOST":     "MecGod-rw",
			"DB_NAME":     "mecgod",
			"DB_USER":     "mecgod",
			"DB_PASSWORD": "stable-secret",
			"DB_PORT":     "5432",
			"DB_TYPE":     "postgres",
		},
	}}

	desired := current
	desired.Spec = *cloneControllerAppSpec(&current.Spec)
	desired.Spec.RolloutIntent = ""
	desired.BackingServices = cloneControllerBackingServices(current.BackingServices)
	desired.Bindings = cloneControllerServiceBindings(current.Bindings)
	desiredPostgres := desired.BackingServices[0].Spec.Postgres
	desiredPostgres.ServiceName = "mecgod"
	desiredPostgres.PrimaryNodeName = "v2202605354515455529"
	desiredPostgres.PrimaryPlacementPendingRebalance = true
	desiredPostgres.Instances = 2
	desired.Bindings[0].Env["DB_HOST"] = "mecgod-rw"
	return current, desired
}

func TestApplyManagedAppDesiredStateDoesNotWriteBeforeLiveIdentityProof(t *testing.T) {
	app := managedAppLiveGuardTestApp(nil)
	managed := managedAppLiveGuardObject(t, app, runtime.SchedulingConstraints{})
	managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1}
	svc := &Service{Renderer: runtime.Renderer{}}
	live, _ := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(app), runtime.SchedulingConstraints{})
	managedAppLiveGuardMarkReady(&live, 1)
	live.Metadata.Annotations[runtime.FugueAnnotationReleaseKey] = "unproven-live-release"
	writes := 0
	client := managedAppLiveGuardClient(t, managed, live, true, true, &writes)
	svc.newKubeClient = func(string) (*kubeClient, error) { return client, nil }

	err := svc.applyManagedAppDesiredState(context.Background(), app, runtime.SchedulingConstraints{})
	if err == nil || !strings.Contains(err.Error(), "matches neither current snapshot") {
		t.Fatalf("expected live identity refusal, got %v", err)
	}
	if writes != 0 {
		t.Fatalf("zero-downtime refusal must happen before every Kubernetes write, got %d writes", writes)
	}
}

func TestManagedAppZeroDowntimeBlockedConditionIsDurableServingEvidence(t *testing.T) {
	status := runtime.ManagedAppStatus{
		Phase: runtime.ManagedAppPhaseError,
		Conditions: []runtime.ManagedAppCondition{{
			Type:   "ZeroDowntimeBlocked",
			Status: "True",
		}},
	}
	if !managedAppStatusShowsServing(status) {
		t.Fatal("a prior zero-downtime block must remain durable protection evidence on the next reconcile")
	}
}

func TestPatchManagedAppZeroDowntimeBlockedStatusPreservesServingStateAndIsDetectablyCurrent(t *testing.T) {
	app := managedAppLiveGuardTestApp(nil)
	managed := managedAppLiveGuardObject(t, app, runtime.SchedulingConstraints{})
	managed.Status = runtime.ManagedAppStatus{
		Phase:                   runtime.ManagedAppPhaseReady,
		ReadyReplicas:           1,
		CurrentReleaseKey:       "release-current",
		CurrentReleaseStartedAt: "2026-07-28T00:00:00Z",
		CurrentReleaseReadyAt:   "2026-07-28T00:00:05Z",
		BackingServices: []runtime.ManagedBackingServiceStatus{{
			ServiceID:      "service-db",
			ReadyInstances: 1,
		}},
	}
	cause := errors.New("zero-downtime deploy refused: unproven live identity")
	var patched runtime.ManagedAppStatus
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPatch || !strings.HasSuffix(req.URL.Path, "/status") {
			t.Fatalf("unexpected request %s %s", req.Method, req.URL.Path)
		}
		var body struct {
			Status runtime.ManagedAppStatus `json:"status"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatalf("decode blocked status: %v", err)
		}
		patched = body.Status
		return okJSONResponse(`{}`), nil
	})
	client := &kubeClient{client: &http.Client{Transport: transport}, baseURL: "http://kube.test", namespace: managed.Metadata.Namespace}

	if err := patchManagedAppZeroDowntimeBlockedStatus(context.Background(), client, managed.Metadata.Namespace, managed, app, cause); !errors.Is(err, cause) {
		t.Fatalf("expected original refusal error, got %v", err)
	}
	if patched.ReadyReplicas != 1 || patched.CurrentReleaseKey != managed.Status.CurrentReleaseKey || len(patched.BackingServices) != 1 {
		t.Fatalf("blocked status must preserve the last serving state, got %+v", patched)
	}
	if !managedAppZeroDowntimeBlockedStatusCurrent(patched, cause) {
		t.Fatalf("the next reconcile must recognize the same block without another status write: %+v", patched)
	}
}

func managedAppLiveGuardTestApp(storage *model.AppPersistentStorageSpec) model.App {
	return model.App{
		ID:        "app_live_guard",
		TenantID:  "tenant_live_guard",
		ProjectID: "project_live_guard",
		Name:      "live-guard",
		Spec: model.AppSpec{
			Image:             "registry.example/live-guard:v1",
			Ports:             []int{8080},
			Replicas:          1,
			RuntimeID:         "runtime_managed_shared",
			Env:               map[string]string{"MODE": "old"},
			PersistentStorage: storage,
		},
	}
}

func managedAppLiveGuardObject(t *testing.T, app model.App, scheduling runtime.SchedulingConstraints) runtime.ManagedAppObject {
	t.Helper()
	managed, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(app, scheduling))
	if err != nil {
		t.Fatalf("build managed app: %v", err)
	}
	return managed
}

func managedAppLiveGuardMarkReady(deployment *kubeDeployment, replicas int) {
	deployment.Metadata.Generation = 1
	deployment.Status.ObservedGeneration = 1
	deployment.Status.Replicas = replicas
	deployment.Status.UpdatedReplicas = replicas
	deployment.Status.ReadyReplicas = replicas
	deployment.Status.AvailableReplicas = replicas
}

func managedAppLiveGuardClient(
	t *testing.T,
	managed runtime.ManagedAppObject,
	deployment kubeDeployment,
	deploymentFound bool,
	readyEndpoint bool,
	writes *int,
) *kubeClient {
	t.Helper()
	namespace := managed.Metadata.Namespace
	managedName := managed.Metadata.Name
	deploymentName := deployment.Metadata.Name
	if deploymentName == "" {
		deploymentName = runtime.RuntimeAppResourceName(runtime.AppFromManagedApp(managed))
	}
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet {
			if writes != nil {
				*writes++
			}
			return okJSONResponse(`{}`), nil
		}
		switch {
		case req.URL.Path == managedAppAPIPath(namespace, managedName):
			data, err := json.Marshal(managed)
			if err != nil {
				t.Fatalf("marshal managed app: %v", err)
			}
			return okJSONResponse(string(data)), nil
		case req.URL.Path == deploymentAPIPath(namespace, deploymentName):
			if !deploymentFound {
				return notFoundJSONResponse(), nil
			}
			data, err := json.Marshal(deployment)
			if err != nil {
				t.Fatalf("marshal deployment: %v", err)
			}
			return okJSONResponse(string(data)), nil
		case strings.Contains(req.URL.Path, "/endpointslices"):
			return okJSONResponse(`{"items":[]}`), nil
		case strings.Contains(req.URL.Path, "/endpoints/"):
			if !readyEndpoint {
				return okJSONResponse(`{"subsets":[]}`), nil
			}
			return okJSONResponse(`{"subsets":[{"addresses":[{"ip":"10.0.0.1"}]}]}`), nil
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"kind":"Status","reason":"NotFound","code":404}`)),
				Header:     make(http.Header),
			}, nil
		}
	})
	return &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
		namespace:   namespace,
	}
}
