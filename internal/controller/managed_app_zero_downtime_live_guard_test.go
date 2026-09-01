package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
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

func TestManagedAppLiveGuardAllowsTerminalUnavailableStatelessRecovery(t *testing.T) {
	current := managedAppLiveGuardTestApp(nil)
	desired := current
	desired.Spec.Image = "registry.example/live-guard:v2"
	managed := managedAppLiveGuardObject(t, current, runtime.SchedulingConstraints{})
	managed.Status = runtime.ManagedAppStatus{
		Phase:         runtime.ManagedAppPhaseError,
		ReadyReplicas: 0,
		Conditions: []runtime.ManagedAppCondition{{
			Type:   "ZeroDowntimeBlocked",
			Status: "True",
		}},
	}
	svc := &Service{Renderer: runtime.Renderer{}}
	live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(current), runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected rendered deployment")
	}
	managedAppLiveGuardMarkTerminalUnavailable(&live)
	client := managedAppLiveGuardClient(t, managed, live, true, false, nil)

	prepared, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, desired, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
	)
	if err != nil {
		t.Fatalf("a terminally unavailable stateless workload must accept its repair: %v", err)
	}
	if prepared.Spec.Image != desired.Spec.Image {
		t.Fatalf("expected repaired image %q, got %q", desired.Spec.Image, prepared.Spec.Image)
	}
	if prepared.Spec.RolloutIntent != model.AppRolloutIntentOnlineImageUpdate {
		t.Fatalf("expected a validated image rollout, got intent %q", prepared.Spec.RolloutIntent)
	}
}

func TestManagedAppLiveGuardAllowsTerminalUnavailableAuxiliaryTemplateRecoveryWithoutOnlineIntent(t *testing.T) {
	app := managedAppLiveGuardTestApp(nil)
	managed := managedAppLiveGuardObject(t, app, runtime.SchedulingConstraints{})
	managed.Status = runtime.ManagedAppStatus{
		Phase:         runtime.ManagedAppPhaseError,
		ReadyReplicas: 0,
		Conditions: []runtime.ManagedAppCondition{{
			Type:   "ZeroDowntimeBlocked",
			Status: "True",
		}},
	}
	svc := &Service{Renderer: runtime.Renderer{}}
	live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(app), runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected rendered deployment")
	}
	managedAppLiveGuardMarkTerminalUnavailable(&live)
	for index := range live.Spec.Template.Spec.InitContainers {
		if live.Spec.Template.Spec.InitContainers[index].Name == "fugue-drain-agent" {
			live.Spec.Template.Spec.InitContainers[index].Image = "ghcr.io/acme/fugue-drain-agent:previous"
		}
	}
	client := managedAppLiveGuardClient(t, managed, live, true, false, nil)

	prepared, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, app, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
	)
	if err != nil {
		t.Fatalf("a terminally unavailable stateless workload must accept auxiliary template repair: %v", err)
	}
	if prepared.Spec.RolloutIntent != "" {
		t.Fatalf("unavailable recovery must not invent an online rollout intent, got %q", prepared.Spec.RolloutIntent)
	}
}

func TestManagedAppLiveGuardUnavailableRecoveryRemainsFailClosedWithoutProof(t *testing.T) {
	localStorage := &model.AppPersistentStorageSpec{
		Mode:             model.AppPersistentStorageModeMovableRWO,
		StorageClassName: model.AppStorageClassFugueLocalRWO,
		Mounts: []model.AppPersistentStorageMount{
			{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"},
		},
	}
	tests := []struct {
		name          string
		storage       *model.AppPersistentStorageSpec
		readyEndpoint bool
		mutate        func(*kubeDeployment)
	}{
		{
			name: "rollout has not reached a terminal failure",
			mutate: func(deployment *kubeDeployment) {
				deployment.Status.Conditions = nil
			},
		},
		{
			name: "deployment generation has not been observed",
			mutate: func(deployment *kubeDeployment) {
				deployment.Metadata.Generation++
			},
		},
		{
			name: "deployment still has a ready replica",
			mutate: func(deployment *kubeDeployment) {
				deployment.Status.ReadyReplicas = 1
			},
		},
		{
			name:          "service still has a ready endpoint",
			readyEndpoint: true,
		},
		{
			name:    "workload has local single-writer storage",
			storage: localStorage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := managedAppLiveGuardTestApp(tt.storage)
			desired := current
			desired.Spec.Image = "registry.example/live-guard:v2"
			managed := managedAppLiveGuardObject(t, current, runtime.SchedulingConstraints{})
			managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseError}
			svc := &Service{Renderer: runtime.Renderer{}}
			live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(current), runtime.SchedulingConstraints{})
			if !found {
				t.Fatal("expected rendered deployment")
			}
			managedAppLiveGuardMarkTerminalUnavailable(&live)
			if tt.mutate != nil {
				tt.mutate(&live)
			}
			client := managedAppLiveGuardClient(t, managed, live, true, tt.readyEndpoint, nil)

			_, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
				context.Background(), client, managed.Metadata.Namespace, managed, desired, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
			)
			if err == nil || !strings.Contains(err.Error(), "live deployment is not fully ready") {
				t.Fatalf("unproven or unsafe unavailable replacement must fail closed, got %v", err)
			}
		})
	}
}

func TestManagedAppLiveGuardRefusesUnknownReleaseIdentity(t *testing.T) {
	app := managedAppLiveGuardTestApp(nil)
	managed := managedAppLiveGuardObject(t, app, runtime.SchedulingConstraints{})
	managed.Metadata.Generation = 2
	managed.Status = runtime.ManagedAppStatus{
		Phase:                   runtime.ManagedAppPhaseReady,
		ReadyReplicas:           1,
		ObservedGeneration:      2,
		PendingReleaseKey:       "unknown-live-release",
		PendingReleaseStartedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	svc := &Service{Renderer: runtime.Renderer{}}
	live, _ := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(app), runtime.SchedulingConstraints{})
	managedAppLiveGuardMarkReady(&live, 1)
	live.Metadata.Annotations[runtime.FugueAnnotationReleaseKey] = "unknown-live-release"
	client := managedAppLiveGuardClient(t, managed, live, true, true, nil)

	_, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, app, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
	)
	if err == nil || !strings.Contains(err.Error(), "matches neither current snapshot") {
		t.Fatalf("a pending identity without operation-history proof must fail closed, got %v", err)
	}
	// The same unproven identity must remain fail-closed when an older status
	// writer promoted it to CurrentReleaseKey and cleared the pending fields.
	managed.Status.PendingReleaseKey = ""
	managed.Status.PendingReleaseStartedAt = ""
	managed.Status.CurrentReleaseKey = "unknown-live-release"
	managed.Status.CurrentReleaseStartedAt = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	managed.Status.CurrentReleaseReadyAt = time.Now().UTC().Format(time.RFC3339Nano)
	_, err = svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, app, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
	)
	if err == nil || !strings.Contains(err.Error(), "matches neither current snapshot") {
		t.Fatalf("a current identity without operation-history proof must fail closed, got %v", err)
	}
}

func TestManagedAppLiveGuardRecoversControllerAuthoredPendingDeploySnapshot(t *testing.T) {
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Pending release recovery")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	source := model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "registry.example/live-guard:latest",
		ResolvedImageRef: "registry.example/live-guard:v1",
	}
	app, err := stateStore.CreateImportedAppWithoutRoute(
		tenant.ID,
		project.ID,
		"live-guard",
		"",
		model.AppSpec{
			Image:     "registry.example/live-guard:v1",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_managed_shared",
			Env:       map[string]string{"MODE": "old"},
		},
		source,
	)
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	failedSpec := *cloneControllerAppSpec(&app.Spec)
	failedSpec.Image = "registry.example/live-guard:v2"
	failedSpec.RestartToken = "restart_failed_attempt"
	failedSource := source
	failedSource.ResolvedImageRef = "registry.example/live-guard:v2"
	failedDeploy, err := stateStore.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		AppID:               app.ID,
		DesiredSpec:         &failedSpec,
		DesiredSource:       &failedSource,
		DesiredOriginSource: &failedSource,
	})
	if err != nil {
		t.Fatalf("create failed deploy: %v", err)
	}
	failedDeploy, claimed, err := stateStore.TryClaimPendingOperation(failedDeploy.ID)
	if err != nil || !claimed {
		t.Fatalf("claim failed deploy: claimed=%v err=%v", claimed, err)
	}
	failedDeploy, err = stateStore.FailOperation(failedDeploy.ID, "status publish failed after rollout became ready")
	if err != nil {
		t.Fatalf("fail prior deploy: %v", err)
	}
	if failedDeploy.CompletedAt == nil {
		t.Fatal("failed deploy must have a completion timestamp")
	}

	svc := &Service{Store: stateStore, Renderer: runtime.Renderer{}}
	pendingApp := app
	pendingApp.Spec = failedSpec
	model.SetAppSourceState(&pendingApp, &failedSource, &failedSource)
	pendingApp.Spec.RolloutIntent = rolloutIntentForManagedOperation(failedDeploy, app, pendingApp)
	pendingApp = svc.Renderer.PrepareApp(pendingApp)
	pendingKey := svc.Renderer.ManagedAppReleaseKey(pendingApp, runtime.SchedulingConstraints{})
	if strings.TrimSpace(pendingKey) == "" {
		t.Fatal("expected pending release key")
	}

	managed := managedAppLiveGuardObject(t, app, runtime.SchedulingConstraints{})
	managed.Metadata.Generation = 2
	managed.Status = runtime.ManagedAppStatus{
		Phase:                   runtime.ManagedAppPhaseError,
		ReadyReplicas:           1,
		ObservedGeneration:      2,
		PendingReleaseKey:       pendingKey,
		PendingReleaseStartedAt: failedDeploy.CompletedAt.UTC().Format(time.RFC3339Nano),
	}
	live, found := svc.expectedManagedAppDeployment(pendingApp, runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected pending deployment")
	}
	managedAppLiveGuardMarkReady(&live, 1)
	client := managedAppLiveGuardClient(t, managed, live, true, true, nil)

	nextSpec := *cloneControllerAppSpec(&failedSpec)
	nextSpec.RestartToken = "restart_next_attempt"
	activeDeploy, err := stateStore.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		AppID:               app.ID,
		DesiredSpec:         &nextSpec,
		DesiredSource:       &failedSource,
		DesiredOriginSource: &failedSource,
	})
	if err != nil {
		t.Fatalf("create active deploy: %v", err)
	}
	activeDeploy, claimed, err = stateStore.TryClaimPendingOperation(activeDeploy.ID)
	if err != nil || !claimed {
		t.Fatalf("claim active deploy: claimed=%v err=%v", claimed, err)
	}
	desired := app
	desired.Spec = nextSpec
	model.SetAppSourceState(&desired, &failedSource, &failedSource)
	ctx := withManagedAppApplySource(context.Background(), managedAppApplySourceOperation, activeDeploy.ID)
	prepared, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		ctx,
		client,
		managed.Metadata.Namespace,
		managed,
		desired,
		model.OperationTypeDeploy,
		runtime.SchedulingConstraints{},
	)
	if err != nil {
		t.Fatalf("controller-authored ready pending release must be a recoverable baseline: %v", err)
	}
	if prepared.Spec.Image != nextSpec.Image || prepared.Spec.RestartToken != nextSpec.RestartToken {
		t.Fatalf("unexpected prepared desired spec: %+v", prepared.Spec)
	}
	if prepared.Spec.RolloutIntent != model.AppRolloutIntentOnlineRestart {
		t.Fatalf("expected pending image to become the current baseline for a restart-only retry, got %q", prepared.Spec.RolloutIntent)
	}

	// Older failed status publication could promote the serving key directly to
	// CurrentReleaseKey and clear PendingReleaseKey. The exact failed operation
	// proof must make that representation recoverable as well.
	releaseStartedAt := failedDeploy.CreatedAt
	if failedDeploy.StartedAt != nil {
		releaseStartedAt = *failedDeploy.StartedAt
	}
	managed.Status.PendingReleaseKey = ""
	managed.Status.PendingReleaseStartedAt = ""
	managed.Status.CurrentReleaseKey = pendingKey
	managed.Status.CurrentReleaseStartedAt = releaseStartedAt.UTC().Format(time.RFC3339Nano)
	managed.Status.CurrentReleaseReadyAt = failedDeploy.CompletedAt.UTC().Format(time.RFC3339Nano)
	prepared, err = svc.prepareManagedAppReconcileRolloutWithEvidence(
		ctx,
		client,
		managed.Metadata.Namespace,
		managed,
		desired,
		model.OperationTypeDeploy,
		runtime.SchedulingConstraints{},
	)
	if err != nil {
		t.Fatalf("controller-authored current release key must be a recoverable baseline: %v", err)
	}
	if prepared.Spec.RolloutIntent != model.AppRolloutIntentOnlineRestart {
		t.Fatalf("expected current release fallback to preserve restart intent, got %q", prepared.Spec.RolloutIntent)
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

func TestManagedAppLiveGuardReturnsRefinedRolloutIdentity(t *testing.T) {
	current := managedAppLiveGuardTestApp(nil)
	current.Spec.Image = "registry.example/live-guard:v2"
	current.Spec.RestartToken = "restart_previous_attempt"
	current.Spec.RolloutIntent = model.AppRolloutIntentOnlineImageUpdate

	managed := managedAppLiveGuardObject(t, current, runtime.SchedulingConstraints{})
	managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1}
	svc := &Service{Renderer: runtime.Renderer{}}
	live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(current), runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected rendered deployment")
	}
	managedAppLiveGuardMarkReady(&live, 1)
	client := managedAppLiveGuardClient(t, managed, live, true, true, nil)

	desired := current
	desired.Spec.RestartToken = "restart_current_attempt"
	// The operation classified this from its older durable-store baseline. The
	// live guard sees that the image is already serving and correctly refines
	// the actual transition to a restart.
	desired.Spec.RolloutIntent = model.AppRolloutIntentOnlineImageUpdate
	applied, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(), client, managed.Metadata.Namespace, managed, desired, model.OperationTypeDeploy, runtime.SchedulingConstraints{},
	)
	if err != nil {
		t.Fatalf("apply managed app desired state: %v", err)
	}
	if applied.Spec.RolloutIntent != model.AppRolloutIntentOnlineRestart {
		t.Fatalf("expected live guard rollout intent %q, got %q", model.AppRolloutIntentOnlineRestart, applied.Spec.RolloutIntent)
	}

	staleWaiterHash := expectedManagedAppSpecHash(desired, runtime.SchedulingConstraints{})
	appliedWaiterHash := expectedManagedAppSpecHash(applied, runtime.SchedulingConstraints{})
	if staleWaiterHash == appliedWaiterHash {
		t.Fatalf("expected live guard refinement to change rollout identity, both were %q", staleWaiterHash)
	}
	managedApplied, err := runtime.ManagedAppObjectFromMap(runtime.BuildManagedAppObject(applied, runtime.SchedulingConstraints{}))
	if err != nil {
		t.Fatalf("decode applied managed app: %v", err)
	}
	if got := runtime.ManagedAppSpecHash(managedApplied.Spec); got != appliedWaiterHash {
		t.Fatalf("returned applied snapshot must reproduce Kubernetes identity: got %q want %q", got, appliedWaiterHash)
	}
}

func TestManagedAppLiveGuardHydratesOriginSourceForResourceRestore(t *testing.T) {
	current := managedAppLiveGuardTestApp(nil)
	current.Spec.Resources = &model.ResourceSpec{CPUMilliCores: 145, MemoryMebibytes: 512}
	current.Spec.RightSizing = &model.AppRightSizingSpec{
		Mode:        model.AppRightSizingModeAuto,
		WindowHours: 168,
		MinSamples:  12,
	}
	current.Spec.RolloutIntent = model.AppRolloutIntentOnlineResourceUpdate
	origin := model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "registry.example/live-guard:origin",
		ResolvedImageRef: "registry.example/live-guard:v1",
	}
	build := model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "registry.example/live-guard:build",
		ResolvedImageRef: current.Spec.Image,
	}
	model.SetAppSourceState(&current, &origin, &build)

	managed := managedAppLiveGuardObject(t, current, runtime.SchedulingConstraints{})
	managed.Status = runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady, ReadyReplicas: 1}
	svc := &Service{Renderer: runtime.Renderer{}}
	live, found := svc.expectedManagedAppDeployment(svc.Renderer.PrepareApp(current), runtime.SchedulingConstraints{})
	if !found {
		t.Fatal("expected rendered deployment")
	}
	managedAppLiveGuardMarkReady(&live, 1)
	client := managedAppLiveGuardClient(t, managed, live, true, true, nil)

	desired := current
	desired.Spec = *cloneControllerAppSpec(&current.Spec)
	desired.Spec.Resources = &model.ResourceSpec{CPUMilliCores: 190, MemoryMebibytes: 512}
	desired.Spec.RightSizing = &model.AppRightSizingSpec{
		Mode:        model.AppRightSizingModeRecommend,
		WindowHours: 168,
		MinSamples:  12,
	}
	desired.Spec.RolloutIntent = ""

	prepared, err := svc.prepareManagedAppReconcileRolloutWithEvidence(
		context.Background(),
		client,
		managed.Metadata.Namespace,
		managed,
		desired,
		model.OperationTypeDeploy,
		runtime.SchedulingConstraints{},
	)
	if err != nil {
		t.Fatalf("resource restore with split origin/build source must remain an online rollout: %v", err)
	}
	if prepared.Spec.RolloutIntent != model.AppRolloutIntentOnlineResourceUpdate {
		t.Fatalf("expected online resource restore, got rollout intent %q", prepared.Spec.RolloutIntent)
	}
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

func managedAppLiveGuardMarkTerminalUnavailable(deployment *kubeDeployment) {
	deployment.Metadata.Generation = 2
	deployment.Status.ObservedGeneration = 2
	deployment.Status.Replicas = 2
	deployment.Status.UpdatedReplicas = 1
	deployment.Status.ReadyReplicas = 0
	deployment.Status.AvailableReplicas = 0
	deployment.Status.UnavailableReplicas = 2
	deployment.Status.Conditions = []runtime.ManagedAppCondition{{
		Type:    "Progressing",
		Status:  "False",
		Reason:  "ProgressDeadlineExceeded",
		Message: "ReplicaSet has timed out progressing",
	}}
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
