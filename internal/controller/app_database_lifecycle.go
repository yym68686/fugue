package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

var errManagedPostgresLifecycleOwnershipUnknown = errors.New("managed postgres lifecycle operation ownership is unknown")

// executeManagedDatabaseLifecycleOperation changes only the runtime lifecycle
// intent of an existing managed PostgreSQL service. The desired service is
// cloned in memory and applied to Kubernetes first; the store is updated only
// after the ManagedApp rollout observer confirms that CNPG reached the desired
// hibernation state.
func (s *Service) executeManagedDatabaseLifecycleOperation(
	ctx context.Context,
	op model.Operation,
	app model.App,
) error {
	if !s.Config.KubectlApply {
		return fmt.Errorf("managed postgres lifecycle operation requires kubernetes apply mode")
	}

	suspended, action, err := managedPostgresLifecycleOperationIntent(op)
	if err != nil {
		return err
	}
	target, err := store.ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil {
		return fmt.Errorf("resolve managed postgres lifecycle target for app %s: %w", app.ID, err)
	}
	if target == nil {
		return fmt.Errorf("managed postgres is not configured for app %s", app.ID)
	}
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		return fmt.Errorf("%s operation %s missing desired managed postgres spec", action, op.ID)
	}
	desiredPostgres := cloneControllerPostgresSpec(op.DesiredSpec.Postgres)
	if desiredPostgres == nil || desiredPostgres.Suspended != suspended {
		return fmt.Errorf("%s operation %s has inconsistent managed postgres lifecycle intent", action, op.ID)
	}
	if !managedPostgresLifecycleOnlyChangesSuspension(target.Postgres, *desiredPostgres) {
		return fmt.Errorf("%s operation %s unexpectedly changes managed postgres configuration", action, op.ID)
	}

	desiredApp, err := managedPostgresLifecycleDesiredApp(app, *target, *desiredPostgres)
	if err != nil {
		return fmt.Errorf("prepare %s desired state for app %s: %w", action, app.ID, err)
	}
	previousApp, err := managedPostgresLifecycleDesiredApp(app, *target, target.Postgres)
	if err != nil {
		return fmt.Errorf("prepare previous managed postgres state for app %s: %w", app.ID, err)
	}
	targetLabel := managedPostgresLifecycleTargetLabel(app, *target)

	if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
		return err
	}
	if _, err := s.Store.UpdateOperationProgress(op.ID, fmt.Sprintf("%s managed postgres service %s", action, targetLabel)); err != nil {
		return fmt.Errorf("update %s operation %s progress: %w", action, op.ID, err)
	}

	hooks := managedPostgresLifecycleConvergenceHooks{
		Apply: func(callCtx context.Context, candidate model.App, _ bool) (runtime.Bundle, error) {
			return s.applyManagedPostgresLifecycleState(callCtx, op, candidate)
		},
		Wait: func(callCtx context.Context, candidate model.App, _ bool) error {
			if err := s.waitForManagedBackingServiceLifecycle(callCtx, candidate, op.ID, target.ServiceID); err != nil {
				return err
			}
			return s.ensureManagedPostgresLifecycleOperationOwned(op)
		},
		Refresh: func(callCtx context.Context, candidate model.App, requestedTransition bool) error {
			expectedSuspended := target.Postgres.Suspended
			if requestedTransition {
				expectedSuspended = suspended
			}
			return s.refreshManagedPostgresLifecycleObservedStatus(callCtx, op, candidate, target.ServiceID, expectedSuspended)
		},
		Progress: func(message string) error {
			_, err := s.Store.UpdateOperationProgress(op.ID, message)
			return err
		},
		CanCompensate: func() error {
			return s.ensureManagedPostgresLifecycleOperationOwned(op)
		},
	}
	if suspended {
		hooks.Preflight = func(callCtx context.Context) error {
			return s.waitForManagedPostgresSuspendConsumersStopped(callCtx, op, app, target.ServiceID)
		}
	}
	bundle, err := convergeManagedPostgresLifecycle(ctx, desiredApp, previousApp, targetLabel, suspended, target.Postgres.Suspended, hooks)
	if err != nil {
		return err
	}

	message := managedPostgresLifecycleCompletionMessage(targetLabel, suspended)
	completed, err := s.Store.CompleteManagedOperationWithResult(
		op.ID,
		bundle.ManifestPath,
		message,
		nil,
		nil,
	)
	if err != nil {
		completionErr := fmt.Errorf("complete %s operation %s: %w", action, op.ID, err)
		return compensateManagedPostgresLifecycle(ctx, previousApp, targetLabel, suspended, target.Postgres.Suspended, completionErr, hooks)
	}
	s.logOperationAppEvent("completed", "info", completed, desiredApp, message, map[string]any{
		"elapsed_ms": operationElapsedMilliseconds(completed, time.Now().UTC()),
		"service_id": strings.TrimSpace(target.ServiceID),
	})
	if s.Logger != nil {
		s.Logger.Printf("operation %s completed managed postgres service %s %s; manifest=%s", op.ID, targetLabel, action, bundle.ManifestPath)
	}
	return nil
}

type managedPostgresLifecycleConvergenceHooks struct {
	// The bool passed to Apply, Wait, and Refresh is true for the requested
	// transition and false for compensation. Production callbacks keep the same
	// operation ownership check for both paths; the marker keeps tests observable.
	Preflight     func(context.Context) error
	Apply         func(context.Context, model.App, bool) (runtime.Bundle, error)
	Wait          func(context.Context, model.App, bool) error
	Refresh       func(context.Context, model.App, bool) error
	Progress      func(string) error
	CanCompensate func() error
}

func convergeManagedPostgresLifecycle(
	ctx context.Context,
	desiredApp model.App,
	previousApp model.App,
	targetLabel string,
	desiredSuspended bool,
	previousSuspended bool,
	hooks managedPostgresLifecycleConvergenceHooks,
) (runtime.Bundle, error) {
	action := managedPostgresLifecycleAction(desiredSuspended)
	if hooks.Apply == nil || hooks.Wait == nil {
		return runtime.Bundle{}, fmt.Errorf("managed postgres %s convergence hooks are incomplete", action)
	}
	if hooks.Preflight != nil {
		if err := hooks.Preflight(ctx); err != nil {
			return runtime.Bundle{}, fmt.Errorf("verify managed postgres service %s suspend consumers stopped: %w", targetLabel, err)
		}
	}

	bundle, err := hooks.Apply(ctx, desiredApp, true)
	if err != nil {
		transitionErr := fmt.Errorf("apply managed postgres service %s %s intent: %w", targetLabel, action, err)
		return bundle, compensateManagedPostgresLifecycle(ctx, previousApp, targetLabel, desiredSuspended, previousSuspended, transitionErr, hooks)
	}
	waitMessage := fmt.Sprintf("waiting for managed postgres service %s to %s", targetLabel, managedPostgresLifecycleConvergenceVerb(desiredSuspended))
	if hooks.Progress != nil {
		if err := hooks.Progress(waitMessage); err != nil {
			transitionErr := fmt.Errorf("update %s convergence progress for managed postgres service %s: %w", action, targetLabel, err)
			return bundle, compensateManagedPostgresLifecycle(ctx, previousApp, targetLabel, desiredSuspended, previousSuspended, transitionErr, hooks)
		}
	}
	if err := hooks.Wait(ctx, desiredApp, true); err != nil {
		transitionErr := fmt.Errorf("wait for managed postgres service %s to %s: %w", targetLabel, managedPostgresLifecycleConvergenceVerb(desiredSuspended), err)
		return bundle, compensateManagedPostgresLifecycle(ctx, previousApp, targetLabel, desiredSuspended, previousSuspended, transitionErr, hooks)
	}
	if hooks.Refresh != nil {
		managedPostgresLifecycleProgressBestEffort(
			hooks.Progress,
			fmt.Sprintf("refreshing terminal observed status for managed postgres service %s", targetLabel),
		)
		if err := hooks.Refresh(ctx, desiredApp, true); err != nil {
			transitionErr := fmt.Errorf("refresh terminal observed status for managed postgres service %s: %w", targetLabel, err)
			return bundle, compensateManagedPostgresLifecycle(ctx, previousApp, targetLabel, desiredSuspended, previousSuspended, transitionErr, hooks)
		}
	}
	return bundle, nil
}

func compensateManagedPostgresLifecycle(
	ctx context.Context,
	previousApp model.App,
	targetLabel string,
	desiredSuspended bool,
	previousSuspended bool,
	originalErr error,
	hooks managedPostgresLifecycleConvergenceHooks,
) error {
	if originalErr == nil {
		return nil
	}
	// Controller shutdown is intentionally different from a failed transition:
	// leave the explicitly requested runtime intent in place and let the normal
	// operation requeue resume convergence after leadership returns.
	if ctx.Err() != nil {
		return originalErr
	}
	// Store operation serialization makes an active original operation the
	// ownership lease for this desired state. Never force the old state after
	// that lease is lost: a cancellation or newer lifecycle operation may now
	// own the service and must not be overwritten by stale compensation.
	if hooks.CanCompensate != nil {
		if err := hooks.CanCompensate(); err != nil {
			return errors.Join(
				originalErr,
				fmt.Errorf("managed postgres compensation skipped because operation ownership could not be confirmed: %w", err),
			)
		}
	}

	action := managedPostgresLifecycleAction(desiredSuspended)
	previousState := managedPostgresLifecycleStateLabel(previousSuspended)
	restoreStarted := fmt.Sprintf(
		"managed postgres service %s failed to %s; restoring previous %s state",
		targetLabel,
		managedPostgresLifecycleConvergenceVerb(desiredSuspended),
		previousState,
	)
	managedPostgresLifecycleProgressBestEffort(hooks.Progress, restoreStarted)

	if hooks.Apply == nil || hooks.Wait == nil {
		restoreErr := fmt.Errorf("managed postgres compensation hooks are incomplete")
		return errors.Join(originalErr, restoreErr)
	}
	if _, err := hooks.Apply(ctx, previousApp, false); err != nil {
		restoreErr := fmt.Errorf("restore previous %s state for managed postgres service %s: %w", previousState, targetLabel, err)
		managedPostgresLifecycleProgressBestEffort(hooks.Progress, restoreErr.Error())
		return errors.Join(originalErr, restoreErr)
	}
	if err := hooks.Wait(ctx, previousApp, false); err != nil {
		restoreErr := fmt.Errorf("verify restored %s state for managed postgres service %s: %w", previousState, targetLabel, err)
		managedPostgresLifecycleProgressBestEffort(hooks.Progress, restoreErr.Error())
		return errors.Join(originalErr, restoreErr)
	}
	if hooks.Refresh != nil {
		if err := hooks.Refresh(ctx, previousApp, false); err != nil {
			restoreErr := fmt.Errorf("refresh restored %s status for managed postgres service %s: %w", previousState, targetLabel, err)
			managedPostgresLifecycleProgressBestEffort(hooks.Progress, restoreErr.Error())
			return errors.Join(originalErr, restoreErr)
		}
	}

	restoredMessage := fmt.Sprintf(
		"managed postgres service %s previous %s state restored after failed %s",
		targetLabel,
		previousState,
		action,
	)
	managedPostgresLifecycleProgressBestEffort(hooks.Progress, restoredMessage)
	return fmt.Errorf("%w (%s)", originalErr, restoredMessage)
}

func managedPostgresLifecycleProgressBestEffort(progress func(string) error, message string) {
	if progress != nil {
		_ = progress(message)
	}
}

type managedPostgresSuspendPodDrainHooks struct {
	ListPods    func(context.Context, model.App) ([]kubePod, error)
	Wait        func(context.Context, []kubeWatchTarget, time.Duration) error
	EnsureOwned func() error
	Progress    func(string) error
}

func (s *Service) waitForManagedPostgresSuspendConsumersStopped(
	ctx context.Context,
	op model.Operation,
	anchorApp model.App,
	serviceID string,
) error {
	consumerApps, err := s.managedPostgresSuspendConsumerApps(anchorApp, serviceID)
	if err != nil {
		return err
	}
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes suspend consumer client: %w", err)
	}
	timeout := s.Config.ManagedAppRolloutTimeout
	if timeout <= 0 {
		timeout = config.DefaultManagedAppRolloutTimeout
	}
	interval := s.Config.PollInterval
	if interval <= 0 || interval > 2*time.Second {
		interval = 2 * time.Second
	}
	hooks := managedPostgresSuspendPodDrainHooks{
		ListPods: func(callCtx context.Context, app model.App) ([]kubePod, error) {
			selector := strings.TrimSpace(managedAppPodLabelSelector(app))
			if selector == "" {
				return nil, fmt.Errorf("managed app %s has no pod selector", app.ID)
			}
			return client.listPodsBySelector(callCtx, runtime.NamespaceForTenant(app.TenantID), selector)
		},
		Wait: func(callCtx context.Context, targets []kubeWatchTarget, waitInterval time.Duration) error {
			return client.waitForAnyObjectEvent(callCtx, targets, waitInterval)
		},
		EnsureOwned: func() error {
			return s.ensureManagedPostgresLifecycleOperationOwned(op)
		},
		Progress: func(message string) error {
			_, err := s.Store.UpdateOperationProgress(op.ID, message)
			return err
		},
	}
	return waitForManagedPostgresSuspendConsumerPodDrain(ctx, timeout, interval, consumerApps, hooks)
}

func (s *Service) managedPostgresSuspendConsumerApps(anchorApp model.App, serviceID string) ([]model.App, error) {
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return nil, fmt.Errorf("managed postgres service id is required")
	}
	apps, err := s.Store.ListApps(anchorApp.TenantID, false)
	if err != nil {
		return nil, fmt.Errorf("list managed postgres consumer apps: %w", err)
	}
	consumers := make([]model.App, 0, 1)
	for _, app := range apps {
		if !controllerAppHasServiceBinding(app, serviceID) {
			continue
		}
		if app.Spec.Replicas > 0 {
			return nil, fmt.Errorf("app %s still desires %d replicas", app.ID, app.Spec.Replicas)
		}
		consumers = append(consumers, app)
	}
	if len(consumers) == 0 {
		return nil, fmt.Errorf("managed postgres service %s has no bound consumer app", serviceID)
	}
	return consumers, nil
}

func waitForManagedPostgresSuspendConsumerPodDrain(
	ctx context.Context,
	timeout time.Duration,
	interval time.Duration,
	consumerApps []model.App,
	hooks managedPostgresSuspendPodDrainHooks,
) error {
	if len(consumerApps) == 0 || hooks.ListPods == nil || hooks.Wait == nil {
		return fmt.Errorf("managed postgres suspend pod-drain inputs are incomplete")
	}
	if timeout <= 0 {
		timeout = config.DefaultManagedAppRolloutTimeout
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		if hooks.EnsureOwned != nil {
			if err := hooks.EnsureOwned(); err != nil {
				return err
			}
		}
		remaining := 0
		watchTargets := make([]kubeWatchTarget, 0, len(consumerApps))
		for _, app := range consumerApps {
			pods, err := hooks.ListPods(waitCtx, app)
			if err != nil {
				return fmt.Errorf("list actual pods for managed postgres consumer app %s: %w", app.ID, err)
			}
			// Every returned pod blocks hibernation, including terminating,
			// succeeded, and failed pods that still exist in the API server.
			remaining += len(pods)
			if len(pods) > 0 {
				watchTargets = append(watchTargets, managedAppPodRolloutWatchTargets(runtime.NamespaceForTenant(app.TenantID), app)...)
			}
		}
		if remaining == 0 {
			if hooks.EnsureOwned != nil {
				return hooks.EnsureOwned()
			}
			return nil
		}
		managedPostgresLifecycleProgressBestEffort(
			hooks.Progress,
			fmt.Sprintf("waiting for %d managed postgres consumer app pod(s) to terminate", remaining),
		)
		if len(watchTargets) == 0 {
			return fmt.Errorf("cannot watch %d remaining managed postgres consumer app pod(s)", remaining)
		}
		if err := hooks.Wait(waitCtx, watchTargets, interval); err != nil {
			return fmt.Errorf("wait for managed postgres consumer app pods to terminate: %w (%d remaining)", err, remaining)
		}
	}
}

func (s *Service) applyManagedPostgresLifecycleState(
	ctx context.Context,
	op model.Operation,
	app model.App,
) (runtime.Bundle, error) {
	if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
		return runtime.Bundle{}, err
	}

	app, err := store.OverlayDesiredManagedPostgres(app)
	if err != nil {
		return runtime.Bundle{}, fmt.Errorf("overlay desired managed postgres state for app %s: %w", app.ID, err)
	}
	postgresPlacements, err := s.managedPostgresPlacements(ctx, app)
	if err != nil {
		return runtime.Bundle{}, fmt.Errorf("resolve managed postgres placements for app %s: %w", app.ID, err)
	}
	scheduling, err := s.managedSchedulingConstraintsForApp(ctx, app)
	if err != nil {
		return runtime.Bundle{}, err
	}
	app = s.appWithResolvedLaunchOverride(ctx, app)

	bundle, err := s.Renderer.RenderAppBundleWithPlacements(app, scheduling, postgresPlacements)
	if err != nil {
		return runtime.Bundle{}, fmt.Errorf("render manifest for app %s: %w", app.ID, err)
	}
	if !s.Config.KubectlApply {
		return bundle, nil
	}
	bundle, err = s.Renderer.RenderManagedAppBundle(app, scheduling)
	if err != nil {
		return runtime.Bundle{}, fmt.Errorf("render managed app manifest for app %s: %w", app.ID, err)
	}

	// Lifecycle transitions intentionally update an existing CNPG Cluster even
	// when the app snapshot still carries an online rollout intent. Background
	// reconcile keeps its existing no-write safety behavior.
	if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
		return runtime.Bundle{}, err
	}
	applyCtx := managedPostgresLifecycleApplyContext(ctx)
	applyCtx = withManagedAppApplySource(applyCtx, managedAppApplySourceOperation, op.ID)
	if err := s.applyManagedAppDesiredState(applyCtx, app, scheduling); err != nil {
		return runtime.Bundle{}, fmt.Errorf("apply managed app desired state %s: %w", app.ID, err)
	}
	if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
		return runtime.Bundle{}, err
	}
	return bundle, nil
}

func (s *Service) refreshManagedPostgresLifecycleObservedStatus(
	ctx context.Context,
	op model.Operation,
	app model.App,
	serviceID string,
	expectedSuspended bool,
) error {
	// Reconcile the same desired snapshot once more after the dedicated CNPG
	// waiter reaches terminal state. The normal reconciler intentionally skips
	// stored-state replacement while this operation is active; this explicit
	// operation-owned pass refreshes ManagedApp status and Store runtime times.
	if _, err := s.applyManagedPostgresLifecycleState(ctx, op, app); err != nil {
		return err
	}
	if err := s.verifyManagedPostgresLifecycleObservedStatus(ctx, app, serviceID, expectedSuspended); err != nil {
		return err
	}
	return s.ensureManagedPostgresLifecycleOperationOwned(op)
}

func (s *Service) verifyManagedPostgresLifecycleObservedStatus(
	ctx context.Context,
	app model.App,
	serviceID string,
	expectedSuspended bool,
) error {
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes lifecycle status client: %w", err)
	}
	app = s.Renderer.PrepareApp(app)
	namespace := runtime.NamespaceForTenant(app.TenantID)
	managedName := runtime.ManagedAppResourceName(app)
	managed, found, err := client.getManagedApp(ctx, namespace, managedName)
	if err != nil {
		return fmt.Errorf("read managed app %s/%s after lifecycle status refresh: %w", namespace, managedName, err)
	}
	if !found {
		return fmt.Errorf("managed app %s/%s was not found after lifecycle status refresh", namespace, managedName)
	}
	status, found := managedPostgresLifecycleBackingServiceStatus(managed.Status.BackingServices, serviceID)
	if !found {
		return fmt.Errorf("managed app %s/%s is missing backing service status %s", namespace, managedName, strings.TrimSpace(serviceID))
	}
	if !managedPostgresLifecycleObservedStatusTerminal(status, expectedSuspended) {
		return fmt.Errorf(
			"managed postgres service %s status is not terminal after refresh: phase=%s ready=%d desired=%d",
			strings.TrimSpace(serviceID),
			strings.TrimSpace(status.Phase),
			status.ReadyInstances,
			status.DesiredInstances,
		)
	}
	return nil
}

func managedPostgresLifecycleBackingServiceStatus(
	statuses []runtime.ManagedBackingServiceStatus,
	serviceID string,
) (runtime.ManagedBackingServiceStatus, bool) {
	serviceID = strings.TrimSpace(serviceID)
	for _, status := range statuses {
		if strings.TrimSpace(status.ServiceID) == serviceID {
			return status, true
		}
	}
	return runtime.ManagedBackingServiceStatus{}, false
}

func managedPostgresLifecycleObservedStatusTerminal(status runtime.ManagedBackingServiceStatus, suspended bool) bool {
	if suspended {
		return strings.EqualFold(strings.TrimSpace(status.Phase), model.ManagedPostgresRuntimePhaseSuspended) &&
			status.ReadyInstances == 0
	}
	return strings.EqualFold(strings.TrimSpace(status.Phase), model.ManagedPostgresRuntimePhaseActive) &&
		status.DesiredInstances > 0 &&
		status.ReadyInstances > 0
}

func (s *Service) ensureManagedPostgresLifecycleOperationOwned(expected model.Operation) error {
	operation, err := s.Store.GetOperation(expected.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return errOperationNoLongerActive
		}
		return fmt.Errorf("%w: load operation %s: %v", errManagedPostgresLifecycleOwnershipUnknown, expected.ID, err)
	}
	if operation.Status != model.OperationStatusRunning || operation.ExecutionMode != model.ExecutionModeManaged {
		return fmt.Errorf(
			"%w: managed postgres lifecycle operation %s is %s/%s",
			errOperationNoLongerActive,
			expected.ID,
			operation.Status,
			operation.ExecutionMode,
		)
	}
	if !managedPostgresLifecycleClaimMatches(expected, operation) {
		return fmt.Errorf("%w: managed postgres lifecycle operation %s claim changed", errOperationNoLongerActive, expected.ID)
	}
	return nil
}

func managedPostgresLifecycleClaimMatches(expected, current model.Operation) bool {
	return expected.ID != "" &&
		expected.ID == current.ID &&
		current.Status == model.OperationStatusRunning &&
		current.ExecutionMode == model.ExecutionModeManaged &&
		expected.StartedAt != nil &&
		current.StartedAt != nil &&
		current.StartedAt.UnixMicro() == expected.StartedAt.UnixMicro()
}

func managedPostgresLifecycleApplyContext(ctx context.Context) context.Context {
	return withForceExistingCloudNativePGWrites(ctx)
}

func managedPostgresLifecycleOperationIntent(op model.Operation) (bool, string, error) {
	switch op.Type {
	case model.OperationTypeDatabaseSuspend:
		return true, "suspend", nil
	case model.OperationTypeDatabaseResume:
		return false, "resume", nil
	default:
		return false, "", fmt.Errorf("unsupported managed postgres lifecycle operation type %s", op.Type)
	}
}

func managedPostgresLifecycleOnlyChangesSuspension(current, desired model.AppPostgresSpec) bool {
	current.Suspended = false
	desired.Suspended = false
	return reflect.DeepEqual(current, desired)
}

func managedPostgresLifecycleDesiredApp(
	app model.App,
	target store.ManagedPostgresOperationTarget,
	desired model.AppPostgresSpec,
) (model.App, error) {
	next := app
	if spec := cloneControllerAppSpec(&app.Spec); spec != nil {
		next.Spec = *spec
	}
	next.BackingServices = cloneControllerBackingServices(app.BackingServices)
	next.Bindings = cloneControllerServiceBindings(app.Bindings)

	desiredCopy := cloneControllerPostgresSpec(&desired)
	if desiredCopy == nil {
		return model.App{}, fmt.Errorf("desired managed postgres spec is required")
	}
	serviceID := strings.TrimSpace(target.ServiceID)
	if serviceID == "" {
		next.Spec.Postgres = desiredCopy
		return next, nil
	}

	for index := range next.BackingServices {
		service := &next.BackingServices[index]
		if strings.TrimSpace(service.ID) != serviceID {
			continue
		}
		ownerAppID := strings.TrimSpace(service.OwnerAppID)
		if ownerAppID != "" && ownerAppID != strings.TrimSpace(app.ID) {
			return model.App{}, fmt.Errorf("managed postgres service %s is not owned by app %s", serviceID, app.ID)
		}
		if ownerAppID == "" && !controllerAppHasServiceBinding(next, serviceID) {
			return model.App{}, fmt.Errorf("standalone managed postgres service %s is not bound to app %s", serviceID, app.ID)
		}
		if service.Type != model.BackingServiceTypePostgres || service.Spec.Postgres == nil {
			return model.App{}, fmt.Errorf("backing service %s is not managed postgres", serviceID)
		}
		service.Spec.Postgres = desiredCopy
		// Modern apps carry the database desired state on the backing service.
		// Keeping this nil prevents the overlay helper from selecting another
		// service implicitly.
		next.Spec.Postgres = nil
		return next, nil
	}
	return model.App{}, fmt.Errorf("managed postgres service %s is missing from app %s", serviceID, app.ID)
}

func controllerAppHasServiceBinding(app model.App, serviceID string) bool {
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return false
	}
	for _, binding := range app.Bindings {
		if strings.TrimSpace(binding.ServiceID) == serviceID {
			return true
		}
	}
	return false
}

func managedPostgresLifecycleTargetLabel(app model.App, target store.ManagedPostgresOperationTarget) string {
	if serviceID := strings.TrimSpace(target.ServiceID); serviceID != "" {
		return serviceID
	}
	if name := strings.TrimSpace(target.Postgres.ServiceName); name != "" {
		return name
	}
	return strings.TrimSpace(app.ID)
}

func managedPostgresLifecycleConvergenceVerb(suspended bool) string {
	if suspended {
		return "suspend"
	}
	return "resume and become ready"
}

func managedPostgresLifecycleAction(suspended bool) string {
	if suspended {
		return "suspend"
	}
	return "resume"
}

func managedPostgresLifecycleStateLabel(suspended bool) string {
	if suspended {
		return "suspended"
	}
	return "active"
}

func managedPostgresLifecycleCompletionMessage(targetLabel string, suspended bool) string {
	if suspended {
		return fmt.Sprintf("managed postgres service %s suspended; persistent storage retained", targetLabel)
	}
	return fmt.Sprintf("managed postgres service %s resumed and primary ready", targetLabel)
}
