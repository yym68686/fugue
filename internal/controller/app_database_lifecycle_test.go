package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestManagedPostgresLifecycleDesiredAppDoesNotMutatePersistedSnapshot(t *testing.T) {
	t.Parallel()

	resources := &model.ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 256}
	postgres := model.AppPostgresSpec{
		Database:    "demo",
		User:        "demo",
		Password:    "secret",
		ServiceName: "demo-postgres",
		RuntimeID:   "runtime_demo",
		StorageSize: "10Gi",
		Instances:   1,
		Resources:   resources,
		Suspended:   false,
	}
	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			RuntimeID: "runtime_demo",
			Replicas:  0,
		},
		Bindings: []model.ServiceBinding{{
			ID:        "binding_pg",
			TenantID:  "tenant_demo",
			AppID:     "app_demo",
			ServiceID: "service_pg",
			Env:       map[string]string{"DATABASE_URL": "postgres://redacted"},
		}},
		BackingServices: []model.BackingService{{
			ID:          "service_pg",
			TenantID:    "tenant_demo",
			OwnerAppID:  "app_demo",
			Type:        model.BackingServiceTypePostgres,
			Provisioner: model.BackingServiceProvisionerManaged,
			Spec:        model.BackingServiceSpec{Postgres: &postgres},
		}},
	}
	target := store.ManagedPostgresOperationTarget{
		ServiceID: "service_pg",
		Service:   &app.BackingServices[0],
		Postgres:  postgres,
		AppOwned:  true,
	}
	desired := postgres
	desired.Suspended = true

	next, err := managedPostgresLifecycleDesiredApp(app, target, desired)
	if err != nil {
		t.Fatalf("prepare lifecycle desired app: %v", err)
	}
	if next.BackingServices[0].Spec.Postgres == nil || !next.BackingServices[0].Spec.Postgres.Suspended {
		t.Fatalf("expected cloned service to be suspended, got %+v", next.BackingServices[0].Spec.Postgres)
	}
	if app.BackingServices[0].Spec.Postgres.Suspended {
		t.Fatal("expected original persisted snapshot to remain active")
	}
	if next.Spec.Postgres != nil {
		t.Fatalf("expected modern service lifecycle to stay on backing service, got app postgres %+v", next.Spec.Postgres)
	}

	next.BackingServices[0].Spec.Postgres.Resources.CPUMilliCores = 1000
	if got := app.BackingServices[0].Spec.Postgres.Resources.CPUMilliCores; got != 250 {
		t.Fatalf("expected postgres resources to be deeply cloned, got original CPU request %d", got)
	}
	next.Bindings[0].Env["DATABASE_URL"] = "changed"
	if got := app.Bindings[0].Env["DATABASE_URL"]; got != "postgres://redacted" {
		t.Fatalf("expected binding env to be deeply cloned, got original value %q", got)
	}
}

func TestManagedPostgresLifecycleRejectsUnrelatedSpecChanges(t *testing.T) {
	t.Parallel()

	current := model.AppPostgresSpec{ServiceName: "demo-postgres", Instances: 1, Suspended: false}
	desired := current
	desired.Suspended = true
	if !managedPostgresLifecycleOnlyChangesSuspension(current, desired) {
		t.Fatal("expected suspension-only desired state to be accepted")
	}
	desired.StorageSize = "20Gi"
	if managedPostgresLifecycleOnlyChangesSuspension(current, desired) {
		t.Fatal("expected storage change to be rejected by lifecycle operation")
	}
}

func TestManagedPostgresLifecycleDesiredAppSupportsStandaloneBoundService(t *testing.T) {
	t.Parallel()

	app := lifecycleControllerTestApp(false)
	app.BackingServices[0].OwnerAppID = ""
	target := store.ManagedPostgresOperationTarget{
		ServiceID: "service_pg",
		Service:   &app.BackingServices[0],
		Postgres:  *app.BackingServices[0].Spec.Postgres,
		AppOwned:  false,
	}
	desired := target.Postgres
	desired.Suspended = true

	next, err := managedPostgresLifecycleDesiredApp(app, target, desired)
	if err != nil {
		t.Fatalf("prepare standalone lifecycle app: %v", err)
	}
	if !next.BackingServices[0].Spec.Postgres.Suspended {
		t.Fatal("expected exclusively bound standalone service to receive lifecycle intent")
	}
	if !controllerAppHasServiceBinding(next, target.ServiceID) {
		t.Fatal("expected standalone service binding to be preserved")
	}

	app.Bindings = nil
	if _, err := managedPostgresLifecycleDesiredApp(app, target, desired); err == nil {
		t.Fatal("expected unowned and unbound service to be rejected")
	}
	app = lifecycleControllerTestApp(false)
	app.BackingServices[0].OwnerAppID = "app_other"
	target.Service = &app.BackingServices[0]
	if _, err := managedPostgresLifecycleDesiredApp(app, target, desired); err == nil {
		t.Fatal("expected service owned by another app to be rejected")
	}
}

func TestManagedPostgresLifecycleFailedSuspendRestoresActiveState(t *testing.T) {
	t.Parallel()

	desired := lifecycleControllerTestApp(true)
	previous := lifecycleControllerTestApp(false)
	var applied []bool
	var waited []bool
	var progress []string
	hooks := managedPostgresLifecycleConvergenceHooks{
		Apply: func(_ context.Context, app model.App, _ bool) (runtime.Bundle, error) {
			applied = append(applied, lifecycleControllerTestSuspended(app))
			if len(applied) == 1 {
				return runtime.Bundle{}, errors.New("partial apply failed")
			}
			return runtime.Bundle{}, nil
		},
		Wait: func(_ context.Context, app model.App, _ bool) error {
			waited = append(waited, lifecycleControllerTestSuspended(app))
			return nil
		},
		Progress: func(message string) error {
			progress = append(progress, message)
			return nil
		},
	}

	_, err := convergeManagedPostgresLifecycle(context.Background(), desired, previous, "service_pg", true, false, hooks)
	if err == nil || !strings.Contains(err.Error(), "previous active state restored") {
		t.Fatalf("expected failed suspend to report successful active-state restore, got %v", err)
	}
	if got := lifecycleBoolSequence(applied); got != "true,false" {
		t.Fatalf("expected desired suspend then active compensation apply, got %s", got)
	}
	if got := lifecycleBoolSequence(waited); got != "false" {
		t.Fatalf("expected compensation to verify active state, got %s", got)
	}
	if !lifecycleProgressContains(progress, "restoring previous active state") {
		t.Fatalf("expected active restore progress, got %#v", progress)
	}
}

func TestManagedPostgresLifecycleRefreshesTerminalStatusBeforeCompletion(t *testing.T) {
	t.Parallel()

	desired := lifecycleControllerTestApp(true)
	previous := lifecycleControllerTestApp(false)
	observedPhase := model.ManagedPostgresRuntimePhaseActive
	cnpgConverged := false
	var events []string
	hooks := managedPostgresLifecycleConvergenceHooks{
		Apply: func(_ context.Context, _ model.App, requestedTransition bool) (runtime.Bundle, error) {
			if requestedTransition {
				events = append(events, "apply_requested")
				observedPhase = model.ManagedPostgresRuntimePhaseSuspending
			}
			return runtime.Bundle{}, nil
		},
		Wait: func(_ context.Context, _ model.App, requestedTransition bool) error {
			if requestedTransition {
				events = append(events, "cnpg_terminal")
				if observedPhase != model.ManagedPostgresRuntimePhaseSuspending {
					return fmt.Errorf("expected transitional ManagedApp status before CNPG convergence, got %s", observedPhase)
				}
				cnpgConverged = true
			}
			return nil
		},
		Refresh: func(_ context.Context, _ model.App, requestedTransition bool) error {
			if !requestedTransition {
				return nil
			}
			events = append(events, "managed_status_terminal")
			if !cnpgConverged {
				return errors.New("managed status refreshed before CNPG convergence")
			}
			observedPhase = model.ManagedPostgresRuntimePhaseSuspended
			return nil
		},
	}

	if _, err := convergeManagedPostgresLifecycle(context.Background(), desired, previous, "service_pg", true, false, hooks); err != nil {
		t.Fatalf("converge lifecycle with terminal status refresh: %v", err)
	}
	if observedPhase != model.ManagedPostgresRuntimePhaseSuspended {
		t.Fatalf("expected terminal status before convergence returned, got %s", observedPhase)
	}
	if got := strings.Join(events, ","); got != "apply_requested,cnpg_terminal,managed_status_terminal" {
		t.Fatalf("expected apply -> CNPG convergence -> status refresh ordering, got %s", got)
	}
}

func TestManagedPostgresSuspendWaitsForTerminatingConsumerPodBeforeApply(t *testing.T) {
	t.Parallel()

	desired := lifecycleControllerTestApp(true)
	desired.Status.CurrentReplicas = 0
	previous := lifecycleControllerTestApp(false)
	terminatingPod := kubePod{}
	terminatingPod.Metadata.Name = "app-demo-terminating"
	terminatingPod.Metadata.DeletionTimestamp = "2026-07-15T12:00:00Z"
	var events []string
	listCalls := 0
	applied := false
	drainHooks := managedPostgresSuspendPodDrainHooks{
		ListPods: func(_ context.Context, _ model.App) ([]kubePod, error) {
			listCalls++
			if listCalls == 1 {
				events = append(events, "terminating_pod_observed")
				return []kubePod{terminatingPod}, nil
			}
			events = append(events, "pod_list_empty")
			return nil, nil
		},
		Wait: func(_ context.Context, _ []kubeWatchTarget, _ time.Duration) error {
			events = append(events, "wait_for_pod_delete")
			if applied {
				return errors.New("CNPG intent applied before terminating app pod disappeared")
			}
			return nil
		},
		EnsureOwned: func() error { return nil },
	}
	hooks := managedPostgresLifecycleConvergenceHooks{
		Preflight: func(ctx context.Context) error {
			return waitForManagedPostgresSuspendConsumerPodDrain(ctx, time.Second, time.Millisecond, []model.App{desired}, drainHooks)
		},
		Apply: func(_ context.Context, _ model.App, requestedTransition bool) (runtime.Bundle, error) {
			if requestedTransition {
				applied = true
				events = append(events, "apply_hibernation")
			}
			return runtime.Bundle{}, nil
		},
		Wait: func(_ context.Context, _ model.App, _ bool) error { return nil },
	}

	if _, err := convergeManagedPostgresLifecycle(context.Background(), desired, previous, "service_pg", true, false, hooks); err != nil {
		t.Fatalf("converge suspend after app pod drain: %v", err)
	}
	if !applied {
		t.Fatal("expected hibernation intent to apply after actual pod list became empty")
	}
	if got := strings.Join(events, ","); got != "terminating_pod_observed,wait_for_pod_delete,pod_list_empty,apply_hibernation" {
		t.Fatalf("expected terminating pod drain before hibernation apply, got %s", got)
	}
}

func TestManagedPostgresSuspendPodDrainFailureDoesNotApplyHibernation(t *testing.T) {
	t.Parallel()

	desired := lifecycleControllerTestApp(true)
	desired.Status.CurrentReplicas = 0
	previous := lifecycleControllerTestApp(false)
	terminatingPod := kubePod{}
	terminatingPod.Metadata.Name = "app-demo-terminating"
	terminatingPod.Metadata.DeletionTimestamp = "2026-07-15T12:00:00Z"
	applied := false
	drainHooks := managedPostgresSuspendPodDrainHooks{
		ListPods: func(_ context.Context, _ model.App) ([]kubePod, error) {
			return []kubePod{terminatingPod}, nil
		},
		Wait: func(_ context.Context, _ []kubeWatchTarget, _ time.Duration) error {
			return context.DeadlineExceeded
		},
	}
	hooks := managedPostgresLifecycleConvergenceHooks{
		Preflight: func(ctx context.Context) error {
			return waitForManagedPostgresSuspendConsumerPodDrain(ctx, time.Second, time.Millisecond, []model.App{desired}, drainHooks)
		},
		Apply: func(_ context.Context, _ model.App, _ bool) (runtime.Bundle, error) {
			applied = true
			return runtime.Bundle{}, nil
		},
		Wait: func(_ context.Context, _ model.App, _ bool) error { return nil },
	}

	if _, err := convergeManagedPostgresLifecycle(context.Background(), desired, previous, "service_pg", true, false, hooks); err == nil {
		t.Fatal("expected terminating consumer pod to block suspend")
	}
	if applied {
		t.Fatal("expected CNPG hibernation intent not to apply while terminating app pod exists")
	}
}

func TestManagedPostgresLifecycleFailedResumeRestoresSuspendedState(t *testing.T) {
	t.Parallel()

	desired := lifecycleControllerTestApp(false)
	previous := lifecycleControllerTestApp(true)
	var applied []bool
	var waited []bool
	var progress []string
	hooks := managedPostgresLifecycleConvergenceHooks{
		Apply: func(_ context.Context, app model.App, _ bool) (runtime.Bundle, error) {
			applied = append(applied, lifecycleControllerTestSuspended(app))
			return runtime.Bundle{}, nil
		},
		Wait: func(_ context.Context, app model.App, requestedTransition bool) error {
			waited = append(waited, lifecycleControllerTestSuspended(app))
			if requestedTransition {
				return errors.New("resume readiness timed out")
			}
			return nil
		},
		Progress: func(message string) error {
			progress = append(progress, message)
			return nil
		},
	}

	_, err := convergeManagedPostgresLifecycle(context.Background(), desired, previous, "service_pg", false, true, hooks)
	if err == nil || !strings.Contains(err.Error(), "previous suspended state restored") {
		t.Fatalf("expected failed resume to report successful suspended-state restore, got %v", err)
	}
	if got := lifecycleBoolSequence(applied); got != "false,true" {
		t.Fatalf("expected desired resume then suspended compensation apply, got %s", got)
	}
	if got := lifecycleBoolSequence(waited); got != "false,true" {
		t.Fatalf("expected resume wait then suspended compensation verification, got %s", got)
	}
	if !lifecycleProgressContains(progress, "restoring previous suspended state") {
		t.Fatalf("expected suspended restore progress, got %#v", progress)
	}
}

func TestManagedPostgresLifecycleCancellationLeavesIntentForRequeue(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	desired := lifecycleControllerTestApp(true)
	previous := lifecycleControllerTestApp(false)
	var applied []bool
	var waited []bool
	hooks := managedPostgresLifecycleConvergenceHooks{
		Apply: func(_ context.Context, app model.App, _ bool) (runtime.Bundle, error) {
			applied = append(applied, lifecycleControllerTestSuspended(app))
			return runtime.Bundle{}, nil
		},
		Wait: func(_ context.Context, app model.App, _ bool) error {
			waited = append(waited, lifecycleControllerTestSuspended(app))
			cancel()
			return context.Canceled
		},
	}

	_, err := convergeManagedPostgresLifecycle(ctx, desired, previous, "service_pg", true, false, hooks)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected controller cancellation to propagate for requeue, got %v", err)
	}
	if got := lifecycleBoolSequence(applied); got != "true" {
		t.Fatalf("expected no compensation apply during cancellation, got %s", got)
	}
	if got := lifecycleBoolSequence(waited); got != "true" {
		t.Fatalf("expected only requested-state wait during cancellation, got %s", got)
	}
}

func TestManagedPostgresLifecycleLostOwnershipDoesNotOverwriteNewerIntent(t *testing.T) {
	t.Parallel()

	desired := lifecycleControllerTestApp(true)
	previous := lifecycleControllerTestApp(false)
	var applied []bool
	hooks := managedPostgresLifecycleConvergenceHooks{
		Apply: func(_ context.Context, app model.App, _ bool) (runtime.Bundle, error) {
			applied = append(applied, lifecycleControllerTestSuspended(app))
			return runtime.Bundle{}, nil
		},
		Wait: func(_ context.Context, _ model.App, requestedTransition bool) error {
			if requestedTransition {
				return errOperationNoLongerActive
			}
			return nil
		},
		CanCompensate: func() error {
			return errOperationNoLongerActive
		},
	}

	_, err := convergeManagedPostgresLifecycle(context.Background(), desired, previous, "service_pg", true, false, hooks)
	if !errors.Is(err, errOperationNoLongerActive) {
		t.Fatalf("expected lost operation ownership to propagate, got %v", err)
	}
	if got := lifecycleBoolSequence(applied); got != "true" {
		t.Fatalf("expected stale operation not to apply previous state after losing ownership, got %s", got)
	}
	if !strings.Contains(err.Error(), "compensation skipped because operation ownership could not be confirmed") {
		t.Fatalf("expected explicit ownership skip evidence, got %v", err)
	}
}

func TestManagedPostgresLifecycleOwnershipUnknownRemainsRetryable(t *testing.T) {
	t.Parallel()

	desired := lifecycleControllerTestApp(true)
	previous := lifecycleControllerTestApp(false)
	var applied []bool
	hooks := managedPostgresLifecycleConvergenceHooks{
		Apply: func(_ context.Context, app model.App, _ bool) (runtime.Bundle, error) {
			applied = append(applied, lifecycleControllerTestSuspended(app))
			return runtime.Bundle{}, nil
		},
		Wait: func(_ context.Context, _ model.App, requestedTransition bool) error {
			if requestedTransition {
				return errors.New("runtime convergence timed out")
			}
			return nil
		},
		CanCompensate: func() error {
			return fmt.Errorf("%w: store temporarily unavailable", errManagedPostgresLifecycleOwnershipUnknown)
		},
	}

	_, err := convergeManagedPostgresLifecycle(context.Background(), desired, previous, "service_pg", true, false, hooks)
	if !errors.Is(err, errManagedPostgresLifecycleOwnershipUnknown) {
		t.Fatalf("expected ownership-unknown sentinel to survive for requeue, got %v", err)
	}
	if got := lifecycleBoolSequence(applied); got != "true" {
		t.Fatalf("expected no unsafe compensation while ownership is unknown, got %s", got)
	}
}

func TestManagedPostgresLifecycleClaimOwnershipRejectsRequeue(t *testing.T) {
	t.Parallel()

	started := time.Date(2026, 7, 15, 12, 0, 0, 123456789, time.UTC)
	storedStarted := started.Truncate(time.Microsecond)
	expected := model.Operation{
		ID:            "op_lifecycle",
		Status:        model.OperationStatusRunning,
		ExecutionMode: model.ExecutionModeManaged,
		StartedAt:     &started,
	}
	current := expected
	current.StartedAt = &storedStarted
	if !managedPostgresLifecycleClaimMatches(expected, current) {
		t.Fatal("expected the same PostgreSQL-backed microsecond claim timestamp to match")
	}

	current.Status = model.OperationStatusPending
	if managedPostgresLifecycleClaimMatches(expected, current) {
		t.Fatal("expected a requeued pending operation not to remain owned by the old worker")
	}
	current.Status = model.OperationStatusRunning
	reclaimedStarted := storedStarted.Add(time.Microsecond)
	current.StartedAt = &reclaimedStarted
	if managedPostgresLifecycleClaimMatches(expected, current) {
		t.Fatal("expected a newly claimed generation of the same operation ID not to match")
	}
}

func TestManagedPostgresLifecycleApplyUpdatesExistingClusterHibernationAnnotation(t *testing.T) {
	t.Parallel()

	const path = "/apis/postgresql.cnpg.io/v1/namespaces/tenant-demo/clusters/demo-postgres"
	requests := make([]string, 0, 2)
	var applied map[string]any
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests = append(requests, req.Method+" "+req.URL.Path)
		switch req.Method {
		case http.MethodGet:
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(`{
					"apiVersion":"postgresql.cnpg.io/v1",
					"kind":"Cluster",
					"metadata":{
						"name":"demo-postgres",
						"namespace":"tenant-demo",
						"annotations":{"cnpg.io/hibernation":"off"}
					},
					"spec":{"instances":1}
				}`)),
				Header: make(http.Header),
			}, nil
		case http.MethodPatch:
			if got := req.Header.Get("Content-Type"); got != "application/apply-patch+yaml" {
				t.Fatalf("expected server-side apply content type, got %q", got)
			}
			if err := json.NewDecoder(req.Body).Decode(&applied); err != nil {
				t.Fatalf("decode applied cluster: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{}`)),
				Header:     make(http.Header),
			}, nil
		default:
			t.Fatalf("unexpected request method %s", req.Method)
			return nil, nil
		}
	})
	client := &kubeClient{
		client:      &http.Client{Transport: transport},
		baseURL:     "http://kube.test",
		bearerToken: "token",
		namespace:   "tenant-demo",
	}
	desired := map[string]any{
		"apiVersion": runtime.CloudNativePGAPIVersion,
		"kind":       runtime.CloudNativePGClusterKind,
		"metadata": map[string]any{
			"name": "demo-postgres",
			"annotations": map[string]string{
				runtime.CloudNativePGHibernationAnno: runtime.CloudNativePGHibernationOn,
			},
		},
		"spec": map[string]any{"instances": 1},
	}

	backgroundCtx := withSkipExistingCloudNativePGWrites(context.Background())
	if err := client.applyObject(managedPostgresLifecycleApplyContext(backgroundCtx), desired, nil); err != nil {
		t.Fatalf("apply lifecycle cluster intent: %v", err)
	}
	if got, want := strings.Join(requests, "\n"), "GET "+path+"\nPATCH "+path; got != want {
		t.Fatalf("expected existing cluster read and update, got %q", got)
	}
	metadata, _ := applied["metadata"].(map[string]any)
	annotations, _ := metadata["annotations"].(map[string]any)
	if got := annotations[runtime.CloudNativePGHibernationAnno]; got != runtime.CloudNativePGHibernationOn {
		t.Fatalf("expected hibernation annotation %q, got %#v", runtime.CloudNativePGHibernationOn, got)
	}
}

func TestManagedPostgresLifecycleObservedStatusTerminal(t *testing.T) {
	t.Parallel()

	if !managedPostgresLifecycleObservedStatusTerminal(runtime.ManagedBackingServiceStatus{
		Phase:            model.ManagedPostgresRuntimePhaseSuspended,
		ReadyInstances:   0,
		DesiredInstances: 1,
	}, true) {
		t.Fatal("expected suspended zero-ready status to be terminal")
	}
	if managedPostgresLifecycleObservedStatusTerminal(runtime.ManagedBackingServiceStatus{
		Phase:            model.ManagedPostgresRuntimePhaseSuspending,
		ReadyInstances:   0,
		DesiredInstances: 1,
	}, true) {
		t.Fatal("expected transitional suspend status not to be terminal")
	}
	if !managedPostgresLifecycleObservedStatusTerminal(runtime.ManagedBackingServiceStatus{
		Phase:            model.ManagedPostgresRuntimePhaseActive,
		ReadyInstances:   1,
		DesiredInstances: 1,
	}, false) {
		t.Fatal("expected active ready status to be terminal")
	}
	if managedPostgresLifecycleObservedStatusTerminal(runtime.ManagedBackingServiceStatus{
		Phase:            model.ManagedPostgresRuntimePhaseResuming,
		ReadyInstances:   1,
		DesiredInstances: 1,
	}, false) {
		t.Fatal("expected transitional resume status not to be terminal")
	}
}

func TestVerifyManagedPostgresLifecycleObservedStatusReadsTerminalManagedAppState(t *testing.T) {
	t.Parallel()

	app := lifecycleControllerTestApp(true)
	namespace := runtime.NamespaceForTenant(app.TenantID)
	managedName := runtime.ManagedAppResourceName(app)
	managed := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed["status"] = map[string]any{
		"phase":              runtime.ManagedAppPhaseDisabled,
		"observedGeneration": 1,
		"backingServices": []map[string]any{{
			"serviceID":        "service_pg",
			"phase":            model.ManagedPostgresRuntimePhaseSuspending,
			"readyInstances":   0,
			"desiredInstances": 1,
		}},
	}

	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != managedAppAPIPath(namespace, managedName) {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(managed)
	}))
	defer kubeServer.Close()

	svc := &Service{
		Renderer: runtime.Renderer{},
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{
				client:      kubeServer.Client(),
				baseURL:     kubeServer.URL,
				bearerToken: "test",
				namespace:   namespace,
			}, nil
		},
	}
	if err := svc.verifyManagedPostgresLifecycleObservedStatus(context.Background(), app, "service_pg", true); err == nil {
		t.Fatal("expected transitional ManagedApp status to block lifecycle completion")
	}
	managed["status"].(map[string]any)["backingServices"] = []map[string]any{{
		"serviceID":        "service_pg",
		"phase":            model.ManagedPostgresRuntimePhaseSuspended,
		"readyInstances":   0,
		"desiredInstances": 1,
	}}
	if err := svc.verifyManagedPostgresLifecycleObservedStatus(context.Background(), app, "service_pg", true); err != nil {
		t.Fatalf("expected terminal ManagedApp status to pass immediate verification: %v", err)
	}
}

func lifecycleControllerTestApp(suspended bool) model.App {
	postgres := &model.AppPostgresSpec{
		Database:    "demo",
		User:        "demo",
		Password:    "secret",
		ServiceName: "demo-postgres",
		RuntimeID:   "runtime_demo",
		Instances:   1,
		Suspended:   suspended,
	}
	return model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			RuntimeID: "runtime_demo",
			Replicas:  0,
		},
		Bindings: []model.ServiceBinding{{
			ID:        "binding_pg",
			TenantID:  "tenant_demo",
			AppID:     "app_demo",
			ServiceID: "service_pg",
		}},
		BackingServices: []model.BackingService{{
			ID:          "service_pg",
			TenantID:    "tenant_demo",
			OwnerAppID:  "app_demo",
			Type:        model.BackingServiceTypePostgres,
			Provisioner: model.BackingServiceProvisionerManaged,
			Spec:        model.BackingServiceSpec{Postgres: postgres},
		}},
	}
}

func lifecycleControllerTestSuspended(app model.App) bool {
	if len(app.BackingServices) == 0 || app.BackingServices[0].Spec.Postgres == nil {
		return app.Spec.Postgres != nil && app.Spec.Postgres.Suspended
	}
	return app.BackingServices[0].Spec.Postgres.Suspended
}

func lifecycleBoolSequence(values []bool) string {
	parts := make([]string, len(values))
	for index, value := range values {
		if value {
			parts[index] = "true"
		} else {
			parts[index] = "false"
		}
	}
	return strings.Join(parts, ",")
}

func lifecycleProgressContains(messages []string, fragment string) bool {
	for _, message := range messages {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}
