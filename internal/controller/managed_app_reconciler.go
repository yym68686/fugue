package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"

	"k8s.io/apimachinery/pkg/api/resource"
)

const managedPostgresExistingServiceInitGracePeriod = 15 * time.Minute

func (s *Service) applyManagedAppDesiredState(ctx context.Context, app model.App, scheduling runtime.SchedulingConstraints) error {
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes managed app client: %w", err)
	}

	if normalizedApp, changed := s.normalizeManagedAppRuntimeImageRefs(app); changed {
		app = normalizedApp
	}
	if err := validateManagedAppDeployableImage(app); err != nil {
		return err
	}
	app = s.appWithResolvedLaunchOverride(ctx, app)
	app = s.Renderer.PrepareApp(app)
	objects := runtime.BuildManagedAppStateObjects(app, scheduling)
	if err := client.applyObjects(ctx, objects); err != nil {
		return fmt.Errorf("apply managed app state objects: %w", err)
	}
	if err := client.replaceObjectSpecsByKind(ctx, objects, runtime.ManagedAppAPIVersion, runtime.ManagedAppKind); err != nil {
		return fmt.Errorf("replace managed app desired spec: %w", err)
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	name := runtime.ManagedAppResourceName(app)
	managed, found, err := client.getManagedApp(ctx, namespace, name)
	if err != nil {
		return fmt.Errorf("read managed app %s/%s after apply: %w", namespace, name, err)
	}
	if !found {
		return fmt.Errorf("managed app %s/%s was not found after apply", namespace, name)
	}
	managed.Spec.Scheduling = cloneControllerSchedulingConstraints(scheduling)
	return s.reconcileManagedAppResolvedObject(ctx, client, namespace, managed, app, false, false)
}

func (s *Service) deleteManagedAppDesiredState(ctx context.Context, app model.App) error {
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes managed app client: %w", err)
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	if err := client.deleteManagedApp(ctx, namespace, runtime.ManagedAppResourceName(app)); err != nil {
		return fmt.Errorf("delete managed app custom resource: %w", err)
	}
	if err := s.deleteManagedAppResources(ctx, client, namespace, app); err != nil {
		return fmt.Errorf("delete managed app child resources: %w", err)
	}
	return nil
}

func (s *Service) reconcileManagedApps(ctx context.Context) error {
	startedAt := time.Now()
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes managed app client: %w", err)
	}
	client.writeStats.reset()
	ctx = withSkipExistingCloudNativePGWrites(ctx)

	managedApps, err := client.listManagedApps(ctx)
	if err != nil {
		return fmt.Errorf("list managed apps: %w", err)
	}

	var firstErr error
	for _, managed := range managedApps {
		if err := s.reconcileManagedAppObject(ctx, client, managed); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			s.Logger.Printf("managed app %s/%s reconcile error: %v", managed.Metadata.Namespace, managed.Metadata.Name, err)
		}
	}
	if s.Logger != nil {
		if summary := client.writeStats.summary(); summary != "" {
			s.Logger.Printf(
				"managed app reconcile kubernetes write summary managed_apps=%d duration_ms=%d %s",
				len(managedApps),
				time.Since(startedAt).Milliseconds(),
				summary,
			)
		}
	}
	return firstErr
}

func (s *Service) appHasActiveOperation(app model.App) (bool, error) {
	if strings.TrimSpace(app.ID) == "" {
		return false, nil
	}
	return s.Store.HasActiveOperationByApp(app.TenantID, false, app.ID)
}

func (s *Service) observedManagedPostgresDesiredApp(ctx context.Context, app model.App) (model.App, bool, error) {
	alignedSpec, changed, err := s.alignManagedPostgresRuntimeToObservedPrimary(ctx, app)
	if err != nil || !changed {
		return app, changed, err
	}

	alignedApp := app
	alignedApp.Spec = alignedSpec
	alignedApp, err = store.OverlayDesiredManagedPostgres(alignedApp)
	if err != nil {
		return app, false, fmt.Errorf("overlay observed managed postgres desired state for app %s: %w", app.ID, err)
	}

	updatedApp, err := s.Store.SyncObservedManagedPostgresSpec(app.ID, alignedSpec)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("persist observed managed postgres desired state for app %s failed: %v", app.ID, err)
		}
		return alignedApp, true, nil
	}
	return updatedApp, true, nil
}

func (s *Service) reconcileManagedAppObject(ctx context.Context, client *kubeClient, managed runtime.ManagedAppObject) error {
	app := runtime.AppFromManagedApp(managed)
	namespace := strings.TrimSpace(managed.Metadata.Namespace)
	if namespace == "" {
		namespace = runtime.NamespaceForTenant(app.TenantID)
	}
	recoverStoredBaseline := false
	syncStoredManagedAppSnapshot := false
	if appID := strings.TrimSpace(app.ID); appID == "" {
		return s.cleanupOrphanManagedApp(ctx, client, namespace, managed, app, "orphaned managed app: spec.appID is empty")
	} else if storedApp, err := s.Store.GetApp(appID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return s.disableMissingStoreManagedApp(ctx, client, namespace, managed, app, "orphaned managed app: app not found in store; disabled workload and retained storage for audit")
		}
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("read app from store: %w", err))
	} else {
		hasActiveOp, opErr := s.appHasActiveOperation(storedApp)
		switch {
		case opErr != nil:
			if s.Logger != nil {
				s.Logger.Printf("skip managed app %s reconcile after active operation check failed: %v", app.ID, opErr)
			}
			return fmt.Errorf("check active app operations: %w", opErr)
		case hasActiveOp:
			return nil
		default:
			recoverStoredBaseline = managedAppBaselineNeedsRecovery(storedApp)
			if recoverStoredBaseline {
				recoverable, recoverErr := s.managedAppSnapshotRecoverable(ctx, client, namespace, managed, app)
				if recoverErr != nil {
					return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("check managed app baseline recovery state: %w", recoverErr))
				}
				if !recoverable && managedAppSnapshotTerminalFailure(managed.Status) {
					return s.disableUnrecoverableManagedAppSnapshot(ctx, client, namespace, managed, app, "managed app has no deployable stored baseline and the live snapshot is not ready")
				}
			}
			app, useStoredBaseline := selectManagedAppDesiredApp(app, storedApp, false)
			syncStoredManagedAppSnapshot = useStoredBaseline
			if !useStoredBaseline {
				break
			}
			if observedApp, changed, syncErr := s.observedManagedPostgresDesiredApp(ctx, storedApp); syncErr != nil {
				if s.Logger != nil {
					s.Logger.Printf("skip observed managed postgres sync for app %s: %v", app.ID, syncErr)
				}
			} else if changed {
				app = observedApp
			}
		}
	}
	if normalizedApp, changed := s.normalizeManagedAppRuntimeImageRefs(app); changed {
		app = normalizedApp
		if syncStoredManagedAppSnapshot {
			if updatedApp, syncErr := s.Store.SyncObservedManagedAppBaseline(app.ID, app.Spec, app.Source); syncErr != nil {
				if s.Logger != nil {
					s.Logger.Printf("persist normalized managed app runtime image for app %s failed: %v", app.ID, syncErr)
				}
			} else {
				app = updatedApp
			}
		}
	}
	return s.reconcileManagedAppResolvedObject(ctx, client, namespace, managed, app, recoverStoredBaseline, syncStoredManagedAppSnapshot)
}

func (s *Service) reconcileManagedAppResolvedObject(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, app model.App, recoverStoredBaseline bool, syncStoredManagedAppSnapshot bool) error {
	if normalizedApp, changed := s.normalizeManagedAppRuntimeImageRefs(app); changed {
		app = normalizedApp
	}
	if syncStoredManagedAppSnapshot {
		refreshedApp, refreshedManaged, skipApply, err := s.refreshStoredManagedAppDesiredBeforeApply(ctx, client, namespace, managed, app)
		if err != nil {
			return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, err)
		}
		if skipApply {
			return nil
		}
		app = refreshedApp
		managed = refreshedManaged
	}
	if strings.TrimSpace(managed.Metadata.DeletionTimestamp) != "" {
		status := managedAppBaseStatus(managed, app)
		status.Phase = runtime.ManagedAppPhaseDeleting
		status.Message = "deletion requested"
		status.ReadyReplicas = 0
		if err := client.patchManagedAppStatus(ctx, namespace, managed.Metadata.Name, status); err != nil {
			return fmt.Errorf("patch deleting status for managed app %s/%s: %w", namespace, managed.Metadata.Name, err)
		}
		return nil
	}
	if syncStoredManagedAppSnapshot {
		app = s.appWithResolvedLaunchOverride(ctx, app)
		app = storedManagedAppDesiredWithRolloutIntent(runtime.AppFromManagedApp(managed), app)
		app = s.Renderer.PrepareApp(app)
		desiredScheduling, err := s.managedSchedulingConstraintsForApp(ctx, app)
		if err != nil {
			return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("resolve stored managed app scheduling: %w", err))
		}
		desiredScheduling = s.onlineDurableRolloutScheduling(ctx, runtime.AppFromManagedApp(managed), app, desiredScheduling)
		desiredObjects := runtime.BuildManagedAppStateObjects(app, desiredScheduling)
		if err := client.applyObjects(ctx, desiredObjects); err != nil {
			return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("sync managed app desired snapshot from store: %w", err))
		}
		if err := client.replaceObjectSpecsByKind(ctx, desiredObjects, runtime.ManagedAppAPIVersion, runtime.ManagedAppKind); err != nil {
			return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("replace managed app desired snapshot spec from store: %w", err))
		}
		updatedManaged, found, err := client.getManagedApp(ctx, namespace, managed.Metadata.Name)
		if err != nil {
			return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("read managed app after desired snapshot sync: %w", err))
		}
		if found {
			managed = updatedManaged
		}
		managed.Spec.Scheduling = cloneControllerSchedulingConstraints(desiredScheduling)
	}

	app = s.appWithResolvedLaunchOverride(ctx, app)
	app = s.Renderer.PrepareApp(app)
	if err := validateManagedAppDeployableImage(app); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, err)
	}
	if err := s.ensureManagedDeployImageReady(ctx, app, managed.Spec.Scheduling); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, err)
	}
	ownerRef := runtime.ManagedAppOwnerReference(managed)
	postgresPlacements, err := s.managedPostgresPlacements(ctx, app)
	if err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("resolve postgres placements: %w", err))
	}
	childObjects := s.Renderer.BuildManagedAppChildObjectsWithPlacements(app, managed.Spec.Scheduling, postgresPlacements, ownerRef)
	fenceEpoch, err := s.currentAppFenceEpoch(ctx, client, app)
	if err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("read app fence epoch: %w", err))
	}
	decorateManagedAppObjectsWithFenceEpoch(childObjects, app, fenceEpoch)
	if err := s.ensureManagedPostgresDataSafety(ctx, client, namespace, managed, app, childObjects); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, err)
	}
	releaseKey := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(app, managed.Spec.Scheduling))
	rolloutDecision := managedAppRolloutDecisionFromObjects(ctx, namespace, managed, app, childObjects, releaseKey)
	if s.controllerObservabilityEndpointConfigured() {
		rolloutDecision.OldReplicaSet = s.latestManagedAppReplicaSetName(ctx, client, namespace, app)
	}
	stabilizedPostgresStorage, err := s.stabilizeManagedPostgresStorageSpecs(ctx, client, namespace, childObjects)
	if err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, err)
	}
	if err := s.prepareManagedPostgresInPlaceStorageExpansionForDesiredObjects(ctx, client, namespace, childObjects); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, err)
	}
	applyCtx := managedAppCloudNativePGApplyContext(ctx, app, stabilizedPostgresStorage)
	if err := client.applyObjects(applyCtx, childObjects); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("apply managed app child objects: %w", err))
	}
	if err := reconcileCloudNativePGManagedRoles(ctx, client, namespace, childObjects); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("reconcile managed postgres roles: %w", err))
	}
	if err := client.replaceObjectSpecsByKind(ctx, childObjects, "apps/v1", "Deployment"); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("replace managed app deployment desired spec: %w", err))
	}
	if s.controllerObservabilityEndpointConfigured() {
		rolloutDecision.NewReplicaSet = s.latestManagedAppReplicaSetName(ctx, client, namespace, app)
	}
	s.recordManagedAppRolloutDecision(ctx, app, rolloutDecision)
	if err := client.replaceObjectSpecsByKind(applyCtx, childObjects, runtime.CloudNativePGAPIVersion, runtime.CloudNativePGClusterKind); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("replace managed postgres desired spec: %w", err))
	}
	if err := s.reconcileManagedAppPlatformEnvDrift(ctx, client, namespace, childObjects); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, err)
	}
	if err := s.pruneManagedAppStaleObjects(ctx, client, namespace, app, childObjects); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("prune stale managed app child objects: %w", err))
	}
	if err := s.reconcileWorkspaceReplicationSource(ctx, client, app, ownerRef); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("reconcile workspace replication source: %w", err))
	}

	deployment, found, err := client.getDeployment(ctx, namespace, runtime.RuntimeAppResourceName(app))
	if err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("read deployment status: %w", err))
	}
	appPods := make([]kubePod, 0)
	if found {
		appPods, err = client.listPodsBySelector(ctx, namespace, managedAppPodLabelSelector(app))
		if err != nil {
			return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("list app pods: %w", err))
		}
	}

	backingServiceStatuses := make([]runtime.ManagedBackingServiceStatus, 0)
	for _, serviceDeployment := range runtime.ManagedBackingServiceDeploymentsWithPlacements(app, managed.Spec.Scheduling, postgresPlacements) {
		switch serviceDeployment.ResourceKind {
		case runtime.CloudNativePGClusterKind:
			clusterStatus, clusterFound, err := client.getCloudNativePGCluster(ctx, namespace, serviceDeployment.ResourceName)
			if err != nil {
				return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("read backing service cluster %s status: %w", serviceDeployment.ResourceName, err))
			}
			backingServiceStatuses = append(backingServiceStatuses, buildManagedBackingServiceClusterStatus(managed.Status, serviceDeployment, clusterStatus, clusterFound))
		default:
			serviceDeploymentStatus, deploymentFound, err := client.getDeployment(ctx, namespace, serviceDeployment.ResourceName)
			if err != nil {
				return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("read backing service deployment %s status: %w", serviceDeployment.ResourceName, err))
			}
			backingServiceStatuses = append(backingServiceStatuses, buildManagedBackingServiceStatus(managed.Status, serviceDeployment, serviceDeploymentStatus, deploymentFound))
		}
	}

	status := buildManagedAppStatus(managed, app, deployment, found, appPods, backingServiceStatuses, releaseKey)
	if err := client.patchManagedAppStatus(ctx, namespace, managed.Metadata.Name, status); err != nil {
		return fmt.Errorf("patch managed app status for %s/%s: %w", namespace, managed.Metadata.Name, err)
	}
	s.sampleManagedAppReadyEndpoints(ctx, client, namespace, app, status)
	if err := s.Store.SyncManagedAppRuntimeStatus(app.ID, managedStatusTimePointer(status.CurrentReleaseStartedAt), managedStatusTimePointer(status.CurrentReleaseReadyAt), backingServiceRuntimeStatuses(status.BackingServices)); err != nil {
		return fmt.Errorf("sync managed app runtime status for %s: %w", app.ID, err)
	}
	if recoverStoredBaseline && managedAppStatusReady(status, app) {
		if _, err := s.Store.SyncObservedManagedAppBaseline(app.ID, app.Spec, app.Source); err != nil {
			if s.Logger != nil {
				s.Logger.Printf("persist observed managed app baseline for app %s failed: %v", app.ID, err)
			}
		}
	}
	return nil
}

func managedAppCloudNativePGApplyContext(ctx context.Context, app model.App, forceWrite bool) context.Context {
	if forceWrite {
		return withoutSkipExistingCloudNativePGWrites(ctx)
	}
	if appHasOnlineRolloutIntent(app) {
		return withSkipExistingCloudNativePGWrites(ctx)
	}
	return ctx
}

func (s *Service) refreshStoredManagedAppDesiredBeforeApply(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	managed runtime.ManagedAppObject,
	app model.App,
) (model.App, runtime.ManagedAppObject, bool, error) {
	if s.Store == nil || strings.TrimSpace(app.ID) == "" {
		return app, managed, false, nil
	}
	storedApp, err := s.Store.GetApp(app.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return app, managed, false, nil
		}
		return app, managed, false, fmt.Errorf("refresh stored managed app desired state: %w", err)
	}
	hasActiveOp, err := s.appHasActiveOperation(storedApp)
	if err != nil {
		return app, managed, false, fmt.Errorf("refresh active app operation state: %w", err)
	}
	if hasActiveOp {
		return app, managed, true, nil
	}
	if client != nil {
		managedName := strings.TrimSpace(managed.Metadata.Name)
		if managedName == "" {
			managedName = runtime.ManagedAppResourceName(app)
		}
		latestManaged, found, err := client.getManagedApp(ctx, namespace, managedName)
		if err != nil {
			return app, managed, false, fmt.Errorf("refresh managed app desired snapshot before apply: %w", err)
		}
		if found {
			managed = latestManaged
		}
	}
	refreshed, useStoredBaseline := selectManagedAppDesiredApp(runtime.AppFromManagedApp(managed), storedApp, false)
	if !useStoredBaseline {
		return refreshed, managed, false, nil
	}
	observedApp, changed, err := s.observedManagedPostgresDesiredApp(ctx, storedApp)
	if err != nil {
		return app, managed, false, fmt.Errorf("refresh observed managed postgres desired state: %w", err)
	}
	if changed {
		refreshed = observedApp
	}
	if normalizedApp, changed := s.normalizeManagedAppRuntimeImageRefs(refreshed); changed {
		refreshed = normalizedApp
	}
	return refreshed, managed, false, nil
}

func (s *Service) managedAppSnapshotRecoverable(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, app model.App) (bool, error) {
	if app.Spec.Replicas <= 0 {
		return true, nil
	}
	if strings.TrimSpace(managed.Status.CurrentReleaseReadyAt) != "" && managed.Status.ReadyReplicas > 0 {
		return true, nil
	}

	deployment, _, err := client.getDeployment(ctx, namespace, runtime.RuntimeAppResourceName(app))
	if err != nil {
		return false, err
	}
	return managedDeploymentStatusReady(deployment, app.Spec.Replicas), nil
}

func managedAppSnapshotTerminalFailure(status runtime.ManagedAppStatus) bool {
	return strings.EqualFold(strings.TrimSpace(status.Phase), runtime.ManagedAppPhaseError)
}

func (s *Service) disableUnrecoverableManagedAppSnapshot(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, app model.App, reason string) error {
	disabledApp := app
	disabledApp.Spec.Replicas = 0
	disabledApp = s.Renderer.PrepareApp(disabledApp)

	objects := runtime.BuildManagedAppStateObjects(disabledApp, managed.Spec.Scheduling)
	if err := client.applyObjects(ctx, objects); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("disable unrecoverable managed app snapshot: %w", err))
	}
	if err := client.scaleDeployment(ctx, namespace, runtime.RuntimeAppResourceName(disabledApp), 0); err != nil && !isKubernetesResourceNotFound(err) {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("scale unrecoverable managed app deployment to zero: %w", err))
	}

	status := managedAppBaseStatus(managed, disabledApp)
	status.Phase = runtime.ManagedAppPhaseDisabled
	status.Message = strings.TrimSpace(reason)
	status.ReadyReplicas = 0
	if err := client.patchManagedAppStatus(ctx, namespace, managed.Metadata.Name, status); err != nil {
		return fmt.Errorf("patch disabled status for unrecoverable managed app %s/%s: %w", namespace, managed.Metadata.Name, err)
	}
	if s.Logger != nil {
		s.Logger.Printf("disabled unrecoverable managed app snapshot %s/%s: %s", namespace, managed.Metadata.Name, strings.TrimSpace(reason))
	}
	return nil
}

func selectManagedAppDesiredApp(managedSnapshot, stored model.App, hasActiveOperation bool) (model.App, bool) {
	if hasActiveOperation {
		app := managedSnapshot
		backfillManagedAppSource(&app, stored)
		return app, false
	}
	if managedAppBaselineNeedsRecovery(stored) {
		app := managedSnapshot
		backfillManagedAppSource(&app, stored)
		backfillManagedAppBackingServices(&app, stored)
		return app, false
	}
	if managedAppSnapshotCarriesCurrentOnlineRollout(managedSnapshot, stored) {
		app := managedSnapshot
		backfillManagedAppSource(&app, stored)
		backfillManagedAppBackingServices(&app, stored)
		return app, false
	}
	return stored, true
}

func managedAppSnapshotCarriesCurrentOnlineRollout(managedSnapshot, stored model.App) bool {
	if !appHasOnlineRolloutIntent(managedSnapshot) {
		return false
	}
	if !managedAppRolloutSnapshotIdentityEqual(managedSnapshot, stored) {
		return false
	}
	switch strings.TrimSpace(managedSnapshot.Spec.RolloutIntent) {
	case model.AppRolloutIntentOnlineImageUpdate:
		return managedAppImageRolloutSnapshotMatchesStored(managedSnapshot, stored)
	case model.AppRolloutIntentOnlineResourceUpdate:
		return managedAppResourceRolloutSnapshotMatchesStored(managedSnapshot, stored)
	case model.AppRolloutIntentOnlineLifecycleUpdate:
		return managedAppLifecycleRolloutSnapshotMatchesStored(managedSnapshot, stored)
	case model.AppRolloutIntentOnlineRestart:
		return managedAppRestartRolloutSnapshotMatchesStored(managedSnapshot, stored)
	case model.AppRolloutIntentOnlineConfigUpdate:
		return managedAppConfigRolloutSnapshotMatchesStored(managedSnapshot, stored)
	default:
		return reflect.DeepEqual(
			comparableManagedAppRolloutSnapshot(managedSnapshot),
			comparableManagedAppRolloutSnapshot(stored),
		)
	}
}

type managedAppRolloutSnapshot struct {
	ID              string
	TenantID        string
	ProjectID       string
	Name            string
	Route           *model.AppRoute
	Spec            model.AppSpec
	Bindings        []model.ServiceBinding
	BackingServices []model.BackingService
}

func comparableManagedAppRolloutSnapshot(app model.App) managedAppRolloutSnapshot {
	spec := cloneControllerAppSpec(&app.Spec)
	if spec != nil {
		normalized, _ := model.StripFugueInjectedAppEnvFromSpec(*spec)
		spec = &normalized
		spec.RolloutIntent = ""
		model.ApplyAppSpecDefaults(spec)
	}
	bindings := app.Bindings
	if len(bindings) == 0 {
		bindings = nil
	}
	backingServices := app.BackingServices
	if len(backingServices) == 0 {
		backingServices = nil
	}
	return managedAppRolloutSnapshot{
		ID:              strings.TrimSpace(app.ID),
		TenantID:        strings.TrimSpace(app.TenantID),
		ProjectID:       strings.TrimSpace(app.ProjectID),
		Name:            strings.TrimSpace(app.Name),
		Route:           app.Route,
		Spec:            derefControllerAppSpec(spec),
		Bindings:        bindings,
		BackingServices: backingServices,
	}
}

func managedAppRolloutSnapshotIdentityEqual(left, right model.App) bool {
	leftSnapshot := comparableManagedAppRolloutSnapshot(left)
	rightSnapshot := comparableManagedAppRolloutSnapshot(right)
	leftSnapshot.Spec = model.AppSpec{}
	rightSnapshot.Spec = model.AppSpec{}
	return reflect.DeepEqual(leftSnapshot, rightSnapshot)
}

func storedManagedAppDesiredWithRolloutIntent(managedSnapshot, storedDesired model.App) model.App {
	if strings.TrimSpace(storedDesired.Spec.RolloutIntent) != "" {
		return storedDesired
	}
	if intent := rolloutIntentForManagedDesiredState(managedSnapshot, storedDesired); intent != "" {
		storedDesired.Spec.RolloutIntent = intent
	}
	return storedDesired
}

func managedAppImageRolloutSnapshotMatchesStored(managedSnapshot, stored model.App) bool {
	if strings.TrimSpace(managedSnapshot.Spec.Image) == "" ||
		strings.TrimSpace(managedSnapshot.Spec.Image) != strings.TrimSpace(stored.Spec.Image) {
		return false
	}
	left := comparableImageOnlySpec(managedSnapshot.Spec)
	right := comparableImageOnlySpec(stored.Spec)
	return reflect.DeepEqual(left, right)
}

func managedAppResourceRolloutSnapshotMatchesStored(managedSnapshot, stored model.App) bool {
	if managedDeployOperationResourcesDiffer(managedSnapshot.Spec, stored.Spec) {
		return false
	}
	left := comparableResourceOnlySpec(managedSnapshot.Spec)
	right := comparableResourceOnlySpec(stored.Spec)
	return reflect.DeepEqual(left, right)
}

func managedAppLifecycleRolloutSnapshotMatchesStored(managedSnapshot, stored model.App) bool {
	if managedSnapshot.Spec.TerminationGracePeriodSeconds != stored.Spec.TerminationGracePeriodSeconds {
		return false
	}
	left := comparableLifecycleOnlySpec(managedSnapshot.Spec)
	right := comparableLifecycleOnlySpec(stored.Spec)
	return reflect.DeepEqual(left, right)
}

func managedAppRestartRolloutSnapshotMatchesStored(managedSnapshot, stored model.App) bool {
	if strings.TrimSpace(managedSnapshot.Spec.RestartToken) == "" ||
		strings.TrimSpace(managedSnapshot.Spec.RestartToken) != strings.TrimSpace(stored.Spec.RestartToken) {
		return false
	}
	left := comparableRestartSpec(managedSnapshot.Spec)
	right := comparableRestartSpec(stored.Spec)
	return reflect.DeepEqual(left, right)
}

func managedAppConfigRolloutSnapshotMatchesStored(managedSnapshot, stored model.App) bool {
	return reflect.DeepEqual(
		comparableManagedAppRolloutSnapshot(managedSnapshot),
		comparableManagedAppRolloutSnapshot(stored),
	)
}

func derefControllerAppSpec(spec *model.AppSpec) model.AppSpec {
	if spec == nil {
		return model.AppSpec{}
	}
	return *spec
}

func managedAppBaselineNeedsRecovery(app model.App) bool {
	return strings.TrimSpace(app.Spec.Image) == "" || managedAppSourceNeedsRecovery(model.AppBuildSource(app))
}

func validateManagedAppDeployableImage(app model.App) error {
	if app.Spec.Replicas <= 0 || strings.TrimSpace(app.Spec.Image) != "" {
		return nil
	}
	return fmt.Errorf("managed app %s has no deployable image for %d desired replicas", strings.TrimSpace(app.ID), app.Spec.Replicas)
}

func managedAppSourceNeedsRecovery(source *model.AppSource) bool {
	if source == nil {
		return true
	}
	switch strings.TrimSpace(source.Type) {
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate:
		return strings.TrimSpace(source.RepoURL) == "" ||
			strings.TrimSpace(source.CommitSHA) == "" ||
			strings.TrimSpace(source.ResolvedImageRef) == ""
	case model.AppSourceTypeUpload:
		return strings.TrimSpace(source.UploadID) == "" ||
			strings.TrimSpace(source.ArchiveSHA256) == "" ||
			strings.TrimSpace(source.ResolvedImageRef) == ""
	case model.AppSourceTypeDockerImage:
		return strings.TrimSpace(source.ImageRef) == ""
	default:
		return false
	}
}

func cloneControllerSchedulingConstraints(in runtime.SchedulingConstraints) runtime.SchedulingConstraints {
	out := runtime.SchedulingConstraints{}
	if len(in.NodeSelector) > 0 {
		out.NodeSelector = make(map[string]string, len(in.NodeSelector))
		for key, value := range in.NodeSelector {
			out.NodeSelector[key] = value
		}
	}
	if len(in.Tolerations) > 0 {
		out.Tolerations = append([]runtime.Toleration(nil), in.Tolerations...)
	}
	return out
}

func (s *Service) cleanupOrphanManagedApp(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, app model.App, reason string) error {
	managedName := strings.TrimSpace(managed.Metadata.Name)
	if managedName == "" {
		managedName = runtime.ManagedAppResourceName(app)
	}

	status := managedAppBaseStatus(managed, app)
	status.Phase = runtime.ManagedAppPhaseDeleting
	status.Message = strings.TrimSpace(reason)
	status.ReadyReplicas = 0
	if managedName != "" {
		if err := client.patchManagedAppStatus(ctx, namespace, managedName, status); err != nil && !isKubernetesResourceNotFound(err) {
			if s.Logger != nil {
				s.Logger.Printf("patch orphan managed app status failed for %s/%s: %v", namespace, managedName, err)
			}
		}
	}

	var cleanupErrs []error
	if managedName != "" {
		if err := client.deleteManagedApp(ctx, namespace, managedName); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("delete managed app custom resource: %w", err))
		}
	}
	if err := s.deleteManagedAppResources(ctx, client, namespace, app); err != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("delete managed app child resources: %w", err))
	}
	if err := errors.Join(cleanupErrs...); err != nil {
		return fmt.Errorf("cleanup orphan managed app %s/%s: %w", namespace, managedName, err)
	}
	if s.Logger != nil {
		s.Logger.Printf("deleted orphan managed app %s/%s: %s", namespace, managedName, strings.TrimSpace(reason))
	}
	return nil
}

func (s *Service) markOrphanManagedAppObservedOnly(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, app model.App, reason string) error {
	managedName := strings.TrimSpace(managed.Metadata.Name)
	if managedName == "" {
		managedName = runtime.ManagedAppResourceName(app)
	}
	if managedName == "" {
		return nil
	}
	reason = strings.TrimSpace(reason)
	if managedAppObservedOnlyStatusCurrent(managed.Status, reason) {
		return nil
	}
	status := managedAppBaseStatus(managed, app)
	status.Phase = runtime.ManagedAppPhaseError
	status.Message = reason
	if err := client.patchManagedAppStatus(ctx, namespace, managedName, status); err != nil && !isKubernetesResourceNotFound(err) {
		return fmt.Errorf("patch observed-only managed app status %s/%s: %w", namespace, managedName, err)
	}
	if s.Logger != nil {
		s.Logger.Printf("observed-only managed app %s/%s: %s", namespace, managedName, reason)
	}
	return nil
}

func (s *Service) disableMissingStoreManagedApp(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, app model.App, reason string) error {
	managedName := strings.TrimSpace(managed.Metadata.Name)
	if managedName == "" {
		managedName = runtime.ManagedAppResourceName(app)
	}
	if managedName == "" {
		return nil
	}
	reason = strings.TrimSpace(reason)
	statusChanged := false
	if !managedAppDisabledOrphanStatusCurrent(managed.Status, reason) {
		status := managedAppBaseStatus(managed, app)
		status.Phase = runtime.ManagedAppPhaseDisabled
		status.Message = reason
		status.ReadyReplicas = 0
		if err := client.patchManagedAppStatus(ctx, namespace, managedName, status); err != nil && !isKubernetesResourceNotFound(err) {
			return fmt.Errorf("patch disabled orphan managed app status %s/%s: %w", namespace, managedName, err)
		}
		statusChanged = true
	}

	resourceName := runtime.RuntimeAppResourceName(app)
	if resourceName != "" {
		if err := client.scaleDeployment(ctx, namespace, resourceName, 0); err != nil && !isKubernetesResourceNotFound(err) {
			return fmt.Errorf("scale orphan managed app deployment %s/%s to zero: %w", namespace, resourceName, err)
		}
	}
	serviceName := runtime.RuntimeAppServiceName(app)
	if serviceName != "" {
		if err := client.deleteService(ctx, namespace, serviceName); err != nil && !isKubernetesResourceNotFound(err) {
			return fmt.Errorf("delete orphan managed app service %s/%s: %w", namespace, serviceName, err)
		}
	}
	if statusChanged && s.Logger != nil {
		s.Logger.Printf("disabled orphan managed app %s/%s: %s", namespace, managedName, reason)
	}
	return nil
}

func managedAppObservedOnlyStatusCurrent(status runtime.ManagedAppStatus, reason string) bool {
	return strings.EqualFold(strings.TrimSpace(status.Phase), runtime.ManagedAppPhaseError) &&
		strings.TrimSpace(status.Message) == strings.TrimSpace(reason)
}

func managedAppDisabledOrphanStatusCurrent(status runtime.ManagedAppStatus, reason string) bool {
	return strings.EqualFold(strings.TrimSpace(status.Phase), runtime.ManagedAppPhaseDisabled) &&
		strings.TrimSpace(status.Message) == strings.TrimSpace(reason) &&
		status.ReadyReplicas == 0
}

func patchManagedAppErrorStatus(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, app model.App, cause error) error {
	status := managedAppBaseStatus(managed, app)
	status.Phase = runtime.ManagedAppPhaseError
	status.Message = strings.TrimSpace(cause.Error())
	status.CurrentReleaseKey = strings.TrimSpace(managed.Status.CurrentReleaseKey)
	status.CurrentReleaseStartedAt = strings.TrimSpace(managed.Status.CurrentReleaseStartedAt)
	status.CurrentReleaseReadyAt = strings.TrimSpace(managed.Status.CurrentReleaseReadyAt)
	status.PendingReleaseKey = strings.TrimSpace(managed.Status.PendingReleaseKey)
	status.PendingReleaseStartedAt = strings.TrimSpace(managed.Status.PendingReleaseStartedAt)
	status.BackingServices = append([]runtime.ManagedBackingServiceStatus(nil), managed.Status.BackingServices...)
	if err := client.patchManagedAppStatus(ctx, namespace, managed.Metadata.Name, status); err != nil {
		return fmt.Errorf("%w (also failed to patch managed app status: %v)", cause, err)
	}
	return cause
}

func buildManagedAppStatus(managed runtime.ManagedAppObject, app model.App, deployment kubeDeployment, found bool, pods []kubePod, backingServiceStatuses []runtime.ManagedBackingServiceStatus, releaseKeys ...string) runtime.ManagedAppStatus {
	status := managedAppBaseStatus(managed, app)
	if found {
		status.ReadyReplicas = maxInt(deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas)
		status.Conditions = append([]runtime.ManagedAppCondition(nil), deployment.Status.Conditions...)
	}
	status.BackingServices = append([]runtime.ManagedBackingServiceStatus(nil), backingServiceStatuses...)
	releaseKey := ""
	if len(releaseKeys) > 0 {
		releaseKey = strings.TrimSpace(releaseKeys[0])
	}
	if releaseKey == "" {
		releaseKey = strings.TrimSpace(runtime.ManagedAppReleaseKey(app, managed.Spec.Scheduling))
	}
	podFailureCutoff, allowPodFailure := managedAppPodFailureCutoff(managed.Status, releaseKey)
	if allowPodFailure && managedAppReleaseAttemptAdvanced(managed.Status, managed.Metadata.Generation, releaseKey) {
		allowPodFailure = false
	}
	podFailureMessage := ""
	podSchedulingBlockMessage := ""
	if allowPodFailure {
		podFailureMessage = managedAppPodFailureMessage(pods, podFailureCutoff)
		podSchedulingBlockMessage = managedAppPodSchedulingBlockMessage(pods, podFailureCutoff)
	}

	switch {
	case app.Spec.Replicas <= 0:
		status.Phase = runtime.ManagedAppPhaseDisabled
		status.Message = "desired replicas set to 0"
	case !found:
		status.Phase = runtime.ManagedAppPhasePending
		status.Message = fmt.Sprintf("waiting for deployment %s", runtime.RuntimeAppResourceName(app))
	case managedDeploymentStatusReady(deployment, app.Spec.Replicas):
		status.Phase = runtime.ManagedAppPhaseReady
		status.Message = fmt.Sprintf("deployment ready (%d/%d replicas)", status.ReadyReplicas, app.Spec.Replicas)
	case deployment.Status.Replicas == 0:
		status.Phase = runtime.ManagedAppPhasePending
		status.Message = fmt.Sprintf("deployment created; waiting for replicas (desired=%d)", app.Spec.Replicas)
	case podFailureMessage != "":
		status.Phase = runtime.ManagedAppPhaseError
		status.Message = podFailureMessage
	case hasDeploymentFailureCondition(deployment.Status.Conditions):
		status.Phase = runtime.ManagedAppPhaseError
		status.Message = deploymentFailureMessage(deployment.Status.Conditions)
	case podSchedulingBlockMessage != "":
		status.Phase = runtime.ManagedAppPhaseProgressing
		status.Message = podSchedulingBlockMessage
	default:
		status.Phase = runtime.ManagedAppPhaseProgressing
		status.Message = managedDeploymentProgressMessage(deployment, app.Spec.Replicas, runtime.RuntimeAppResourceName(app))
	}
	applyManagedAppReleaseStatus(&status, managed.Status, app, releaseKey)
	return status
}

func managedAppStatusReady(status runtime.ManagedAppStatus, app model.App) bool {
	return strings.EqualFold(strings.TrimSpace(status.Phase), runtime.ManagedAppPhaseReady) &&
		status.ReadyReplicas >= app.Spec.Replicas &&
		app.Spec.Replicas > 0
}

func managedDeploymentStatusReady(deployment kubeDeployment, desiredReplicas int) bool {
	if desiredReplicas <= 0 {
		return false
	}
	if deployment.Status.ObservedGeneration < deployment.Metadata.Generation {
		return false
	}
	if deployment.Status.UpdatedReplicas < desiredReplicas {
		return false
	}
	if deployment.Status.ReadyReplicas < desiredReplicas {
		return false
	}
	if deployment.Status.AvailableReplicas < desiredReplicas {
		return false
	}
	if deployment.Status.Replicas > desiredReplicas {
		return false
	}
	if deployment.Status.UnavailableReplicas > 0 {
		return false
	}
	return true
}

func managedDeploymentProgressMessage(deployment kubeDeployment, desiredReplicas int, deploymentName string) string {
	deploymentName = strings.TrimSpace(deploymentName)
	if deploymentName == "" {
		deploymentName = "deployment"
	}
	if deployment.Status.ObservedGeneration < deployment.Metadata.Generation {
		return fmt.Sprintf("waiting for deployment %s observed generation %d/%d", deploymentName, deployment.Status.ObservedGeneration, deployment.Metadata.Generation)
	}
	if deployment.Status.UpdatedReplicas < desiredReplicas {
		return fmt.Sprintf("waiting for deployment %s updated replicas %d/%d", deploymentName, deployment.Status.UpdatedReplicas, desiredReplicas)
	}
	if deployment.Status.ReadyReplicas < desiredReplicas {
		return fmt.Sprintf("waiting for deployment %s ready replicas %d/%d", deploymentName, deployment.Status.ReadyReplicas, desiredReplicas)
	}
	if deployment.Status.AvailableReplicas < desiredReplicas {
		return fmt.Sprintf("waiting for deployment %s available replicas %d/%d", deploymentName, deployment.Status.AvailableReplicas, desiredReplicas)
	}
	if deployment.Status.Replicas > desiredReplicas {
		return fmt.Sprintf("waiting for deployment %s old replicas to terminate (%d total, desired=%d)", deploymentName, deployment.Status.Replicas, desiredReplicas)
	}
	if deployment.Status.UnavailableReplicas > 0 {
		return fmt.Sprintf("waiting for deployment %s unavailable replicas to drain (%d)", deploymentName, deployment.Status.UnavailableReplicas)
	}
	return fmt.Sprintf("deployment progressing (%d/%d ready replicas)", maxInt(deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas), desiredReplicas)
}

func managedAppBaseStatus(managed runtime.ManagedAppObject, app model.App) runtime.ManagedAppStatus {
	return runtime.ManagedAppStatus{
		DesiredReplicas:     app.Spec.Replicas,
		ObservedGeneration:  managed.Metadata.Generation,
		LastAppliedSpecHash: runtime.ManagedAppSpecHash(managed.Spec),
		LastAppliedTime:     time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func hasDeploymentFailureCondition(conditions []runtime.ManagedAppCondition) bool {
	for _, condition := range conditions {
		switch strings.TrimSpace(condition.Type) {
		case "ReplicaFailure":
			if strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
				return true
			}
		case "Progressing":
			if strings.EqualFold(strings.TrimSpace(condition.Status), "False") {
				return true
			}
		}
	}
	return false
}

func deploymentFailureMessage(conditions []runtime.ManagedAppCondition) string {
	for _, condition := range conditions {
		switch strings.TrimSpace(condition.Type) {
		case "ReplicaFailure":
			if strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
				return managedAppConditionMessage(condition, "deployment replica failure")
			}
		case "Progressing":
			if strings.EqualFold(strings.TrimSpace(condition.Status), "False") {
				return managedAppConditionMessage(condition, "deployment rollout failed")
			}
		}
	}
	return "deployment reported a failed condition"
}

func managedAppConditionMessage(condition runtime.ManagedAppCondition, fallback string) string {
	message := strings.TrimSpace(condition.Message)
	reason := strings.TrimSpace(condition.Reason)
	switch {
	case reason != "" && message != "":
		return reason + ": " + message
	case message != "":
		return message
	case reason != "":
		return reason
	default:
		return fallback
	}
}

func managedAppPodLabelSelector(app model.App) string {
	selectors := []string{
		runtime.FugueLabelManagedBy + "=" + runtime.FugueLabelManagedByValue,
	}
	if name := strings.TrimSpace(runtime.RuntimeResourceName(app.Name)); name != "" {
		selectors = append(selectors, runtime.FugueLabelName+"="+name)
	}
	if appID := strings.TrimSpace(app.ID); appID != "" {
		selectors = append(selectors, runtime.FugueLabelAppID+"="+appID)
	}
	if tenantID := strings.TrimSpace(app.TenantID); tenantID != "" {
		selectors = append(selectors, runtime.FugueLabelTenantID+"="+tenantID)
	}
	return strings.Join(selectors, ",")
}

func managedAppPodFailureCutoff(previous runtime.ManagedAppStatus, releaseKey string) (*time.Time, bool) {
	releaseKey = strings.TrimSpace(releaseKey)
	if releaseKey == "" {
		return nil, true
	}

	if strings.TrimSpace(previous.PendingReleaseKey) == releaseKey {
		return parseManagedAppStatusTimestamp(previous.PendingReleaseStartedAt), true
	}

	currentKey := strings.TrimSpace(previous.CurrentReleaseKey)
	if currentKey == "" && strings.TrimSpace(previous.PendingReleaseKey) == "" {
		if cutoff := parseManagedAppStatusTimestamp(previous.LastAppliedTime); cutoff != nil {
			return cutoff, true
		}
		return nil, true
	}
	if currentKey != "" && currentKey != releaseKey {
		return nil, false
	}
	if currentKey == releaseKey {
		return parseManagedAppStatusTimestamp(previous.CurrentReleaseStartedAt), true
	}

	return nil, true
}

func managedAppReleaseAttemptAdvanced(previous runtime.ManagedAppStatus, generation int64, releaseKey string) bool {
	releaseKey = strings.TrimSpace(releaseKey)
	if releaseKey == "" {
		return false
	}
	if strings.TrimSpace(previous.PendingReleaseKey) != releaseKey {
		return false
	}
	if previous.ObservedGeneration <= 0 || generation <= 0 {
		return false
	}
	return previous.ObservedGeneration < generation
}

func parseManagedAppStatusTimestamp(raw string) *time.Time {
	timestamp := strings.TrimSpace(raw)
	if timestamp == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return nil
	}
	return &parsed
}

func managedAppPodFailureMessage(pods []kubePod, notBefore *time.Time) string {
	for _, pod := range pods {
		if notBefore != nil && pod.Metadata.CreationTimestamp.Before(notBefore.UTC()) {
			continue
		}
		if strings.TrimSpace(pod.Metadata.DeletionTimestamp) != "" {
			continue
		}
		if summary := summarizeManagedAppPodFailure(pod); summary != "" {
			return summary
		}
	}
	return ""
}

func managedAppPodSchedulingBlockMessage(pods []kubePod, notBefore *time.Time) string {
	for _, pod := range pods {
		if notBefore != nil && pod.Metadata.CreationTimestamp.Before(notBefore.UTC()) {
			continue
		}
		if strings.TrimSpace(pod.Metadata.DeletionTimestamp) != "" {
			continue
		}
		if summary := summarizeManagedAppPodSchedulingBlock(pod); summary != "" {
			return summary
		}
	}
	return ""
}

func summarizeManagedAppPodSchedulingBlock(pod kubePod) string {
	prefix := "pod " + strings.TrimSpace(pod.Metadata.Name)
	if node := strings.TrimSpace(pod.Spec.NodeName); node != "" {
		prefix += " on node " + node
	}
	for _, condition := range pod.Status.Conditions {
		if !strings.EqualFold(strings.TrimSpace(condition.Type), "PodScheduled") ||
			!strings.EqualFold(strings.TrimSpace(condition.Status), "False") ||
			!strings.EqualFold(strings.TrimSpace(condition.Reason), "Unschedulable") {
			continue
		}
		return summarizeManagedAppProgressLine(prefix, strings.TrimSpace(condition.Reason), strings.TrimSpace(condition.Message))
	}
	return ""
}

func summarizeManagedAppPodFailure(pod kubePod) string {
	prefix := "pod " + strings.TrimSpace(pod.Metadata.Name)
	if node := strings.TrimSpace(pod.Spec.NodeName); node != "" {
		prefix += " on node " + node
	}
	if reason := strings.TrimSpace(pod.Status.Reason); isFailingManagedAppPodReason(reason) {
		return summarizeManagedAppFailureLine(prefix, reason, strings.TrimSpace(pod.Status.Message))
	}
	if summary := summarizeManagedAppPodConditionFailure(prefix, pod.Status.Conditions); summary != "" {
		return summary
	}

	for _, status := range pod.Status.InitContainerStatuses {
		if status.State.Terminated != nil && isFailingManagedAppTermination(*status.State.Terminated) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "terminated", *status.State.Terminated)
		}
		if !managedAppContainerRecovered(status) && status.LastState.Terminated != nil && isFailingManagedAppTermination(*status.LastState.Terminated) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "terminated", *status.LastState.Terminated)
		}
		if status.State.Waiting != nil && isFailingManagedAppWaitingReason(status.State.Waiting.Reason) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "waiting", *status.State.Waiting)
		}
		if !managedAppContainerRecovered(status) && status.LastState.Waiting != nil && isFailingManagedAppWaitingReason(status.LastState.Waiting.Reason) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "waiting", *status.LastState.Waiting)
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Terminated != nil && managedAppServiceProcessExitedSuccessfully(*status.State.Terminated) && !status.Ready {
			return summarizeManagedAppServiceProcessExit(prefix, status.Name, "")
		}
		if status.State.Terminated != nil && isFailingManagedAppTermination(*status.State.Terminated) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "terminated", *status.State.Terminated)
		}
		if !managedAppContainerRecovered(status) && status.LastState.Terminated != nil && managedAppServiceProcessExitedSuccessfully(*status.LastState.Terminated) && status.State.Waiting != nil {
			return summarizeManagedAppServiceProcessExit(prefix, status.Name, strings.TrimSpace(status.State.Waiting.Reason))
		}
		if !managedAppContainerRecovered(status) && status.LastState.Terminated != nil && isFailingManagedAppTermination(*status.LastState.Terminated) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "terminated", *status.LastState.Terminated)
		}
		if status.State.Waiting != nil && isFailingManagedAppWaitingReason(status.State.Waiting.Reason) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "waiting", *status.State.Waiting)
		}
		if !managedAppContainerRecovered(status) && status.LastState.Waiting != nil && isFailingManagedAppWaitingReason(status.LastState.Waiting.Reason) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "waiting", *status.LastState.Waiting)
		}
	}

	phase := strings.TrimSpace(pod.Status.Phase)
	if strings.EqualFold(phase, "Failed") {
		return fmt.Sprintf("%s failed with phase %s", prefix, phase)
	}
	return ""
}

func summarizeManagedAppProgressLine(subject, reason, message string) string {
	subject = strings.TrimSpace(subject)
	reason = strings.TrimSpace(reason)
	message = strings.TrimSpace(message)
	switch {
	case reason != "" && message != "":
		return fmt.Sprintf("%s blocked: %s: %s", subject, reason, message)
	case reason != "":
		return fmt.Sprintf("%s blocked: %s", subject, reason)
	case message != "":
		return fmt.Sprintf("%s blocked: %s", subject, message)
	default:
		return fmt.Sprintf("%s blocked", subject)
	}
}

func summarizeManagedAppServiceProcessExit(prefix, containerName, waitingReason string) string {
	subject := strings.TrimSpace(prefix)
	if strings.TrimSpace(containerName) != "" {
		subject += " container " + strings.TrimSpace(containerName)
	}
	waitingReason = strings.TrimSpace(waitingReason)
	if waitingReason != "" {
		return fmt.Sprintf("%s failed: process exited successfully and is now waiting: %s", subject, waitingReason)
	}
	return fmt.Sprintf("%s failed: process exited successfully instead of staying online", subject)
}

func managedAppContainerRecovered(status kubeContainerStatus) bool {
	if status.Ready {
		return true
	}
	if status.State.Terminated != nil && !isFailingManagedAppTermination(*status.State.Terminated) {
		return true
	}
	return false
}

func managedAppServiceProcessExitedSuccessfully(detail kubeStateDetail) bool {
	return detail.ExitCode == 0 && strings.EqualFold(strings.TrimSpace(detail.Reason), "Completed")
}

func summarizeManagedAppContainerFailure(prefix, containerName, state string, detail kubeStateDetail) string {
	subject := prefix
	if strings.TrimSpace(containerName) != "" {
		subject += " container " + strings.TrimSpace(containerName)
	}
	reason := strings.TrimSpace(detail.Reason)
	message := strings.TrimSpace(detail.Message)
	if detail.ExitCode != 0 {
		if message == "" {
			message = fmt.Sprintf("exit_code=%d", detail.ExitCode)
		} else {
			message = fmt.Sprintf("%s (exit_code=%d)", message, detail.ExitCode)
		}
	}
	if reason == "" {
		reason = state
	}
	return summarizeManagedAppFailureLine(subject, reason, message)
}

func summarizeManagedAppFailureLine(subject, reason, message string) string {
	subject = strings.TrimSpace(subject)
	reason = strings.TrimSpace(reason)
	message = strings.TrimSpace(message)
	switch {
	case reason != "" && message != "":
		return fmt.Sprintf("%s failed: %s: %s", subject, reason, message)
	case reason != "":
		return fmt.Sprintf("%s failed: %s", subject, reason)
	case message != "":
		return fmt.Sprintf("%s failed: %s", subject, message)
	default:
		return fmt.Sprintf("%s failed", subject)
	}
}

func isFailingManagedAppPodReason(reason string) bool {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "evicted", "unexpectedadmissionerror":
		return true
	default:
		return false
	}
}

func summarizeManagedAppPodConditionFailure(prefix string, conditions []kubePodCondition) string {
	for _, condition := range conditions {
		if !isFailingManagedAppPodCondition(condition) {
			continue
		}
		reason := strings.TrimSpace(condition.Reason)
		if reason == "" {
			reason = strings.TrimSpace(condition.Type)
		}
		return summarizeManagedAppFailureLine(prefix, reason, strings.TrimSpace(condition.Message))
	}
	return ""
}

func isFailingManagedAppPodCondition(condition kubePodCondition) bool {
	if !strings.EqualFold(strings.TrimSpace(condition.Status), "False") {
		return false
	}
	reason := strings.ToLower(strings.TrimSpace(condition.Reason))
	if !strings.EqualFold(strings.TrimSpace(condition.Type), "PodScheduled") {
		return false
	}
	// FailedScheduling is often temporary while the scheduler waits for capacity.
	if reason == "unschedulable" {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(condition.Message))
	if message == "" {
		return false
	}
	for _, marker := range []string{
		"volume node affinity conflict",
		"unbound immediate persistentvolumeclaims",
		"persistentvolumeclaim",
		"disk-pressure",
		"didn't tolerate",
		"did not tolerate",
		"untolerated taint",
		"had taint",
		"node affinity",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func isFailingManagedAppWaitingReason(reason string) bool {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "crashloopbackoff", "createcontainerconfigerror", "createcontainererror", "runcontainererror", "imagepullbackoff", "errimagepull", "invalidimagename", "errimageneverpull":
		return true
	default:
		return false
	}
}

func isFailingManagedAppTermination(detail kubeStateDetail) bool {
	if detail.ExitCode == 143 {
		return false
	}
	reason := strings.TrimSpace(detail.Reason)
	if reason == "" {
		return detail.ExitCode != 0
	}
	return !strings.EqualFold(reason, "Completed")
}

func applyManagedAppReleaseStatus(status *runtime.ManagedAppStatus, previous runtime.ManagedAppStatus, app model.App, releaseKey string) {
	if status == nil {
		return
	}
	if app.Spec.Replicas <= 0 || strings.EqualFold(strings.TrimSpace(status.Phase), runtime.ManagedAppPhaseDisabled) || strings.EqualFold(strings.TrimSpace(status.Phase), runtime.ManagedAppPhaseDeleting) {
		status.CurrentReleaseKey = ""
		status.CurrentReleaseStartedAt = ""
		status.CurrentReleaseReadyAt = ""
		status.PendingReleaseKey = ""
		status.PendingReleaseStartedAt = ""
		return
	}

	releaseKey = strings.TrimSpace(releaseKey)
	currentKey := strings.TrimSpace(previous.CurrentReleaseKey)
	currentStartedAt := strings.TrimSpace(previous.CurrentReleaseStartedAt)
	currentReadyAt := strings.TrimSpace(previous.CurrentReleaseReadyAt)
	pendingKey := strings.TrimSpace(previous.PendingReleaseKey)
	pendingStartedAt := strings.TrimSpace(previous.PendingReleaseStartedAt)
	now := formatKubeTimestamp(time.Now().UTC())
	ready := strings.EqualFold(strings.TrimSpace(status.Phase), runtime.ManagedAppPhaseReady) && status.ReadyReplicas >= app.Spec.Replicas && app.Spec.Replicas > 0

	if releaseKey == "" {
		status.CurrentReleaseKey = currentKey
		status.CurrentReleaseStartedAt = currentStartedAt
		status.CurrentReleaseReadyAt = currentReadyAt
		status.PendingReleaseKey = pendingKey
		status.PendingReleaseStartedAt = pendingStartedAt
		return
	}

	if ready {
		if currentKey == releaseKey {
			status.CurrentReleaseKey = currentKey
			status.CurrentReleaseStartedAt = currentStartedAt
			if status.CurrentReleaseStartedAt == "" && pendingKey == releaseKey {
				status.CurrentReleaseStartedAt = pendingStartedAt
			}
			if status.CurrentReleaseStartedAt == "" {
				status.CurrentReleaseStartedAt = now
			}
			status.CurrentReleaseReadyAt = currentReadyAt
			if status.CurrentReleaseReadyAt == "" {
				status.CurrentReleaseReadyAt = now
			}
		} else {
			status.CurrentReleaseKey = releaseKey
			status.CurrentReleaseStartedAt = pendingStartedAt
			if pendingKey != releaseKey || status.CurrentReleaseStartedAt == "" {
				status.CurrentReleaseStartedAt = now
			}
			status.CurrentReleaseReadyAt = now
		}
		status.PendingReleaseKey = ""
		status.PendingReleaseStartedAt = ""
		return
	}

	status.CurrentReleaseKey = currentKey
	status.CurrentReleaseStartedAt = currentStartedAt
	status.CurrentReleaseReadyAt = currentReadyAt
	if currentKey == releaseKey {
		status.PendingReleaseKey = ""
		status.PendingReleaseStartedAt = ""
		if currentKey == "" {
			status.PendingReleaseKey = releaseKey
			status.PendingReleaseStartedAt = firstNonEmptyString(pendingStartedAt, now)
		}
		return
	}

	status.PendingReleaseKey = releaseKey
	if pendingKey == releaseKey && pendingStartedAt != "" && previous.ObservedGeneration >= status.ObservedGeneration {
		status.PendingReleaseStartedAt = pendingStartedAt
	} else {
		status.PendingReleaseStartedAt = now
	}
}

func buildManagedBackingServiceStatus(previous runtime.ManagedAppStatus, deployment runtime.ManagedBackingServiceDeployment, status kubeDeployment, found bool) runtime.ManagedBackingServiceStatus {
	out := runtime.ManagedBackingServiceStatus{
		ServiceID:  deployment.ServiceID,
		RuntimeKey: deployment.RuntimeKey,
	}
	if !found {
		return out
	}

	prev := managedBackingServiceStatusByID(previous.BackingServices, deployment.ServiceID)
	if strings.TrimSpace(prev.RuntimeKey) == strings.TrimSpace(deployment.RuntimeKey) {
		out.CurrentRuntimeStartedAt = strings.TrimSpace(prev.CurrentRuntimeStartedAt)
		out.CurrentRuntimeReadyAt = strings.TrimSpace(prev.CurrentRuntimeReadyAt)
	}
	if out.CurrentRuntimeStartedAt == "" {
		out.CurrentRuntimeStartedAt = formatKubeTimestamp(time.Now().UTC())
	}
	if managedBackingServiceReady(status, found) {
		if out.CurrentRuntimeReadyAt == "" {
			out.CurrentRuntimeReadyAt = formatKubeTimestamp(time.Now().UTC())
		}
	} else {
		out.CurrentRuntimeReadyAt = ""
	}
	return out
}

func buildManagedBackingServiceClusterStatus(previous runtime.ManagedAppStatus, deployment runtime.ManagedBackingServiceDeployment, status kubeCloudNativePGCluster, found bool) runtime.ManagedBackingServiceStatus {
	out := runtime.ManagedBackingServiceStatus{
		ServiceID:  deployment.ServiceID,
		RuntimeKey: deployment.RuntimeKey,
	}
	if !found {
		return out
	}

	prev := managedBackingServiceStatusByID(previous.BackingServices, deployment.ServiceID)
	if strings.TrimSpace(prev.RuntimeKey) == strings.TrimSpace(deployment.RuntimeKey) {
		out.CurrentRuntimeStartedAt = strings.TrimSpace(prev.CurrentRuntimeStartedAt)
		out.CurrentRuntimeReadyAt = strings.TrimSpace(prev.CurrentRuntimeReadyAt)
	}
	if out.CurrentRuntimeStartedAt == "" {
		out.CurrentRuntimeStartedAt = formatKubeTimestamp(time.Now().UTC())
	}
	if managedBackingServiceClusterReady(status, found) {
		if out.CurrentRuntimeReadyAt == "" {
			out.CurrentRuntimeReadyAt = formatKubeTimestamp(time.Now().UTC())
		}
	} else {
		out.CurrentRuntimeReadyAt = ""
	}
	return out
}

func managedBackingServiceStatusByID(statuses []runtime.ManagedBackingServiceStatus, serviceID string) runtime.ManagedBackingServiceStatus {
	serviceID = strings.TrimSpace(serviceID)
	for _, status := range statuses {
		if strings.TrimSpace(status.ServiceID) == serviceID {
			return status
		}
	}
	return runtime.ManagedBackingServiceStatus{}
}

func managedBackingServiceReady(deployment kubeDeployment, found bool) bool {
	if !found {
		return false
	}
	if hasDeploymentFailureCondition(deployment.Status.Conditions) {
		return false
	}
	if deployment.Status.ObservedGeneration < deployment.Metadata.Generation {
		return false
	}
	if deployment.Status.UpdatedReplicas < 1 {
		return false
	}
	if maxInt(deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas) < 1 {
		return false
	}
	if deployment.Status.UnavailableReplicas > 0 {
		return false
	}
	return true
}

func managedBackingServiceClusterReady(cluster kubeCloudNativePGCluster, found bool) bool {
	if !found {
		return false
	}
	desiredInstances := cluster.Spec.Instances
	if desiredInstances <= 0 {
		desiredInstances = 1
	}
	if cluster.Status.ReadyInstances < desiredInstances {
		return false
	}
	if strings.TrimSpace(cluster.Status.CurrentPrimary) == "" {
		return false
	}
	targetPrimary := strings.TrimSpace(cluster.Status.TargetPrimary)
	if targetPrimary != "" && targetPrimary != strings.TrimSpace(cluster.Status.CurrentPrimary) {
		return false
	}
	return true
}

func managedStatusTimePointer(value string) *time.Time {
	parsed := parseKubeTimestamp(value)
	if parsed.IsZero() {
		return nil
	}
	return &parsed
}

func backingServiceRuntimeStatuses(statuses []runtime.ManagedBackingServiceStatus) []store.ManagedBackingServiceRuntimeStatus {
	if len(statuses) == 0 {
		return nil
	}
	out := make([]store.ManagedBackingServiceRuntimeStatus, 0, len(statuses))
	for _, status := range statuses {
		out = append(out, store.ManagedBackingServiceRuntimeStatus{
			ServiceID:               strings.TrimSpace(status.ServiceID),
			CurrentRuntimeStartedAt: managedStatusTimePointer(status.CurrentRuntimeStartedAt),
			CurrentRuntimeReadyAt:   managedStatusTimePointer(status.CurrentRuntimeReadyAt),
		})
	}
	return out
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (s *Service) ensureManagedPostgresDataSafety(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	managed runtime.ManagedAppObject,
	app model.App,
	desiredObjects []map[string]any,
) error {
	desiredClustersByServiceID := desiredManagedPostgresClustersByServiceID(desiredObjects)
	if len(desiredClustersByServiceID) == 0 {
		return nil
	}

	now := time.Now().UTC()
	if s != nil && s.now != nil {
		now = s.now().UTC()
	}
	for _, service := range app.BackingServices {
		serviceID := strings.TrimSpace(service.ID)
		clusterName := desiredClustersByServiceID[serviceID]
		if serviceID == "" || clusterName == "" {
			continue
		}
		if !managedPostgresMissingClusterShouldBlock(app, service, managedBackingServiceStatusByID(managed.Status.BackingServices, serviceID), now) {
			continue
		}
		_, found, err := client.getCloudNativePGCluster(ctx, namespace, clusterName)
		if err != nil {
			return fmt.Errorf("read managed postgres cluster %s/%s before apply: %w", namespace, clusterName, err)
		}
		if found {
			continue
		}
		return fmt.Errorf(
			"refusing to initialize managed postgres cluster %s/%s for existing backing service %s; "+
				"the control plane has evidence this database existed before, so recreate requires an explicit database restore or reset operation",
			namespace,
			clusterName,
			serviceID,
		)
	}
	return nil
}

func desiredManagedPostgresClustersByServiceID(objects []map[string]any) map[string]string {
	out := make(map[string]string)
	for _, obj := range objects {
		if strings.TrimSpace(objectStringField(obj, "apiVersion")) != runtime.CloudNativePGAPIVersion ||
			strings.TrimSpace(objectStringField(obj, "kind")) != runtime.CloudNativePGClusterKind {
			continue
		}
		metadata, _ := obj["metadata"].(map[string]any)
		if metadata == nil {
			continue
		}
		name, _ := metadata["name"].(string)
		labels, _ := metadata["labels"].(map[string]string)
		if labels == nil {
			if rawLabels, _ := metadata["labels"].(map[string]any); len(rawLabels) > 0 {
				labels = make(map[string]string, len(rawLabels))
				for key, value := range rawLabels {
					labels[key] = fmt.Sprint(value)
				}
			}
		}
		if labels[runtime.FugueLabelBackingServiceType] != model.BackingServiceTypePostgres {
			continue
		}
		serviceID := strings.TrimSpace(labels[runtime.FugueLabelBackingServiceID])
		if serviceID == "" || strings.TrimSpace(name) == "" {
			continue
		}
		out[serviceID] = strings.TrimSpace(name)
	}
	return out
}

func (s *Service) stabilizeManagedPostgresStorageSpecs(ctx context.Context, client *kubeClient, defaultNamespace string, desiredObjects []map[string]any) (bool, error) {
	stabilized := false
	for _, obj := range desiredObjects {
		if strings.TrimSpace(objectStringField(obj, "apiVersion")) != runtime.CloudNativePGAPIVersion ||
			strings.TrimSpace(objectStringField(obj, "kind")) != runtime.CloudNativePGClusterKind {
			continue
		}
		name, namespace := objectNameAndNamespace(defaultNamespace, obj)
		if strings.TrimSpace(name) == "" {
			continue
		}
		spec, _ := obj["spec"].(map[string]any)
		storage, _ := spec["storage"].(map[string]any)
		desiredSize, _ := storage["size"].(string)
		desiredSize = strings.TrimSpace(desiredSize)
		if spec == nil || storage == nil || desiredSize == "" {
			continue
		}
		desiredQuantity, err := resource.ParseQuantity(desiredSize)
		if err != nil {
			continue
		}
		currentCluster, currentClusterFound, err := client.getCloudNativePGCluster(ctx, namespace, name)
		if err != nil {
			return false, fmt.Errorf("read managed postgres cluster %s/%s before storage stabilization: %w", namespace, name, err)
		}

		pvcNames, err := client.listPersistentVolumeClaimNamesByLabel(ctx, namespace, "cnpg.io/cluster="+strings.TrimSpace(name)+",cnpg.io/pvcRole=PG_DATA")
		if err != nil {
			return false, fmt.Errorf("list managed postgres PVCs for %s/%s: %w", namespace, name, err)
		}
		for _, pvcName := range pvcNames {
			preserveSize, preserve, err := s.managedPostgresPVCStorageSizeToPreserve(ctx, client, namespace, pvcName, desiredQuantity)
			if err != nil {
				return false, fmt.Errorf("check managed postgres PVC resize for %s/%s: %w", namespace, pvcName, err)
			}
			if !preserve {
				continue
			}
			if currentClusterFound && managedPostgresClusterStorageSpecExceedsSize(currentCluster, preserveSize) {
				s.logManagedPostgresStorageMigrationRequired(namespace, name, pvcName, currentCluster.Spec.Storage.Size, preserveSize)
				break
			}
			storage["size"] = preserveSize
			stabilized = true
			if s != nil && s.Logger != nil {
				s.Logger.Printf(
					"preserving managed postgres storage size for %s/%s at %s because existing PVC %s cannot be resized in-place to %s",
					namespace,
					name,
					preserveSize,
					pvcName,
					desiredSize,
				)
			}
			break
		}
	}
	return stabilized, nil
}

func (s *Service) prepareManagedPostgresInPlaceStorageExpansionForDesiredObjects(
	ctx context.Context,
	client *kubeClient,
	defaultNamespace string,
	desiredObjects []map[string]any,
) error {
	for _, obj := range desiredObjects {
		if strings.TrimSpace(objectStringField(obj, "apiVersion")) != runtime.CloudNativePGAPIVersion ||
			strings.TrimSpace(objectStringField(obj, "kind")) != runtime.CloudNativePGClusterKind {
			continue
		}
		name, namespace := objectNameAndNamespace(defaultNamespace, obj)
		if strings.TrimSpace(name) == "" {
			continue
		}
		spec, _ := obj["spec"].(map[string]any)
		storage, _ := spec["storage"].(map[string]any)
		if spec == nil || storage == nil {
			continue
		}
		storageSize, _ := storage["size"].(string)
		storageClassName, _ := storage["storageClass"].(string)
		target := managedPostgresStorageTarget{
			StorageClassName: strings.TrimSpace(storageClassName),
			StorageSize:      strings.TrimSpace(storageSize),
		}
		if target.isZero() {
			continue
		}
		if err := s.prepareManagedPostgresInPlaceStorageExpansion(ctx, client, namespace, name, target); err != nil {
			return fmt.Errorf("prepare managed postgres in-place storage expansion for %s/%s: %w", namespace, name, err)
		}
	}
	return nil
}

func (s *Service) logManagedPostgresStorageMigrationRequired(namespace, clusterName, pvcName, recordedSize, pvcSize string) {
	if s == nil || s.Logger == nil {
		return
	}
	namespace = strings.TrimSpace(namespace)
	clusterName = strings.TrimSpace(clusterName)
	pvcName = strings.TrimSpace(pvcName)
	recordedSize = strings.TrimSpace(recordedSize)
	pvcSize = strings.TrimSpace(pvcSize)
	key := strings.Join([]string{namespace, clusterName, pvcName, recordedSize, pvcSize}, "\x00")
	s.managedPostgresStorageNoticeMu.Lock()
	if s.managedPostgresStorageNotices == nil {
		s.managedPostgresStorageNotices = make(map[string]struct{})
	}
	if _, ok := s.managedPostgresStorageNotices[key]; ok {
		s.managedPostgresStorageNoticeMu.Unlock()
		return
	}
	s.managedPostgresStorageNotices[key] = struct{}{}
	s.managedPostgresStorageNoticeMu.Unlock()

	s.Logger.Printf(
		"managed postgres storage expansion for %s/%s is already recorded at %s but existing PVC %s remains %s on non-expandable storage; explicit data migration is required",
		namespace,
		clusterName,
		recordedSize,
		pvcName,
		pvcSize,
	)
}

func managedPostgresClusterStorageSpecExceedsSize(cluster kubeCloudNativePGCluster, size string) bool {
	currentSpecSize := strings.TrimSpace(cluster.Spec.Storage.Size)
	size = strings.TrimSpace(size)
	if currentSpecSize == "" || size == "" {
		return false
	}
	currentQuantity, err := resource.ParseQuantity(currentSpecSize)
	if err != nil {
		return false
	}
	sizeQuantity, err := resource.ParseQuantity(size)
	if err != nil {
		return false
	}
	return currentQuantity.Cmp(sizeQuantity) > 0
}

func (s *Service) managedPostgresPVCStorageSizeToPreserve(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	pvcName string,
	desiredQuantity resource.Quantity,
) (string, bool, error) {
	pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, pvcName)
	if err != nil || !found {
		return "", false, err
	}
	currentSize := strings.TrimSpace(pvc.Status.Capacity["storage"])
	if currentSize == "" {
		currentSize = strings.TrimSpace(pvc.Spec.Resources.Requests["storage"])
	}
	if currentSize == "" {
		return "", false, nil
	}
	currentQuantity, err := resource.ParseQuantity(currentSize)
	if err != nil {
		return "", false, nil
	}
	comparison := desiredQuantity.Cmp(currentQuantity)
	if comparison == 0 {
		return "", false, nil
	}
	if comparison < 0 {
		return currentSize, true, nil
	}
	expandable, err := s.persistentVolumeClaimAllowsExpansion(ctx, client, pvc)
	if err != nil {
		return "", false, err
	}
	if expandable {
		return "", false, nil
	}
	return currentSize, true, nil
}

func (s *Service) persistentVolumeClaimAllowsExpansion(ctx context.Context, client *kubeClient, pvc kubePersistentVolumeClaim) (bool, error) {
	storageClassName := strings.TrimSpace(pvc.Spec.StorageClassName)
	if storageClassName == "" {
		return false, nil
	}
	storageClass, found, err := client.getStorageClass(ctx, storageClassName)
	if err != nil || !found || storageClass.AllowVolumeExpansion == nil {
		return false, err
	}
	return *storageClass.AllowVolumeExpansion, nil
}

func managedPostgresMissingClusterShouldBlock(app model.App, service model.BackingService, status runtime.ManagedBackingServiceStatus, now time.Time) bool {
	if strings.TrimSpace(service.ID) == "" ||
		!strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) ||
		service.Spec.Postgres == nil ||
		strings.EqualFold(strings.TrimSpace(service.Status), model.BackingServiceStatusDeleted) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(service.OwnerAppID), strings.TrimSpace(app.ID)) {
		return false
	}
	if service.CurrentRuntimeStartedAt != nil || service.CurrentRuntimeReadyAt != nil ||
		strings.TrimSpace(status.CurrentRuntimeStartedAt) != "" ||
		strings.TrimSpace(status.CurrentRuntimeReadyAt) != "" {
		return true
	}
	if !managedAppHasRuntimeHistory(app) || service.CreatedAt.IsZero() {
		return false
	}
	return now.Sub(service.CreatedAt) >= managedPostgresExistingServiceInitGracePeriod
}

func managedAppHasRuntimeHistory(app model.App) bool {
	if strings.TrimSpace(app.Status.CurrentRuntimeID) != "" ||
		app.Status.CurrentReplicas > 0 ||
		app.Status.CurrentReleaseStartedAt != nil ||
		app.Status.CurrentReleaseReadyAt != nil {
		return true
	}
	switch strings.TrimSpace(strings.ToLower(app.Status.Phase)) {
	case "deployed", "scaled", "disabled", "failed", "failed-over":
		return true
	default:
		return false
	}
}

func managedPostgresClusterLooksStateful(cluster kubeCloudNativePGCluster) bool {
	labels := cluster.Metadata.Labels
	return strings.EqualFold(strings.TrimSpace(labels[runtime.FugueLabelBackingServiceType]), model.BackingServiceTypePostgres) ||
		strings.TrimSpace(labels[runtime.FugueLabelBackingServiceID]) != ""
}

func persistentVolumeClaimLooksLikeManagedPostgresData(pvc kubePersistentVolumeClaim) bool {
	labels := pvc.Metadata.Labels
	if strings.TrimSpace(labels["cnpg.io/cluster"]) != "" {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(labels[runtime.FugueLabelBackingServiceType]), model.BackingServiceTypePostgres) ||
		strings.TrimSpace(labels[runtime.FugueLabelBackingServiceID]) != ""
}

func (s *Service) pruneManagedAppStaleObjects(ctx context.Context, client *kubeClient, namespace string, app model.App, desiredObjects []map[string]any) error {
	if strings.TrimSpace(app.ID) == "" {
		return nil
	}

	desiredByKind := desiredObjectNamesByKind(desiredObjects)

	deployments, err := s.listOwnedDeploymentNames(ctx, client, namespace, app.ID)
	if err != nil {
		return err
	}
	for _, name := range deployments {
		if _, ok := desiredByKind["Deployment"][name]; ok {
			continue
		}
		if err := client.deleteDeployment(ctx, namespace, name); err != nil {
			return err
		}
	}

	clusters, err := s.listOwnedCloudNativePGClusterNames(ctx, client, namespace, app.ID)
	if err != nil {
		return err
	}
	for _, name := range clusters {
		if _, ok := desiredByKind[runtime.CloudNativePGClusterKind][name]; ok {
			continue
		}
		cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, name)
		if err != nil {
			return err
		}
		if found && managedPostgresClusterLooksStateful(cluster) {
			return fmt.Errorf("refusing to prune managed postgres cluster %s/%s outside an explicit database restore or reset operation", namespace, name)
		}
		if err := client.deleteCloudNativePGCluster(ctx, namespace, name); err != nil {
			return err
		}
	}

	services, err := s.listOwnedServiceNames(ctx, client, namespace, app.ID)
	if err != nil {
		return err
	}
	for _, name := range services {
		if _, ok := desiredByKind["Service"][name]; ok {
			continue
		}
		if err := client.deleteService(ctx, namespace, name); err != nil {
			return err
		}
	}

	networkPolicies, err := s.listOwnedNetworkPolicyNames(ctx, client, namespace, app.ID)
	if err != nil {
		return err
	}
	for _, name := range networkPolicies {
		if _, ok := desiredByKind["NetworkPolicy"][name]; ok {
			continue
		}
		if err := client.deleteNetworkPolicy(ctx, namespace, name); err != nil {
			return err
		}
	}

	replicationDestinations, err := s.listOwnedVolSyncReplicationDestinationNames(ctx, client, namespace, app.ID)
	if err != nil {
		return err
	}
	for _, name := range replicationDestinations {
		if _, ok := desiredByKind[runtime.VolSyncReplicationDestinationKind][name]; ok {
			continue
		}
		if err := client.deleteVolSyncReplicationDestination(ctx, namespace, name); err != nil {
			return err
		}
	}

	replicationSources, err := s.listOwnedVolSyncReplicationSourceNames(ctx, client, namespace, app.ID)
	if err != nil {
		return err
	}
	for _, name := range replicationSources {
		if _, ok := desiredByKind[runtime.VolSyncReplicationSourceKind][name]; ok {
			continue
		}
		if err := client.deleteVolSyncReplicationSource(ctx, namespace, name); err != nil {
			return err
		}
	}

	pvcs, err := s.listOwnedPersistentVolumeClaimNames(ctx, client, namespace, app.ID)
	if err != nil {
		return err
	}
	for _, name := range pvcs {
		if _, ok := desiredByKind["PersistentVolumeClaim"][name]; ok {
			continue
		}
		pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, name)
		if err != nil {
			return err
		}
		if found && persistentVolumeClaimLooksLikeManagedPostgresData(pvc) {
			return fmt.Errorf("refusing to prune managed postgres pvc %s/%s outside an explicit database restore or reset operation", namespace, name)
		}
		if err := client.deletePersistentVolumeClaim(ctx, namespace, name); err != nil {
			return err
		}
	}

	secrets, err := s.listOwnedSecretNames(ctx, client, namespace, app.ID)
	if err != nil {
		return err
	}
	for _, name := range secrets {
		if _, ok := desiredByKind["Secret"][name]; ok {
			continue
		}
		if err := client.deleteSecret(ctx, namespace, name); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) deleteManagedAppResources(ctx context.Context, client *kubeClient, namespace string, app model.App) error {
	resourceNames, err := s.managedAppOwnedResourceNames(ctx, client, namespace, app)
	if err != nil {
		return err
	}

	for _, name := range resourceNames["Deployment"] {
		if err := client.deleteDeployment(ctx, namespace, name); err != nil {
			return err
		}
	}

	for _, name := range resourceNames[runtime.CloudNativePGClusterKind] {
		if err := client.deleteCloudNativePGCluster(ctx, namespace, name); err != nil {
			return err
		}
	}

	for _, name := range resourceNames["Service"] {
		if err := client.deleteService(ctx, namespace, name); err != nil {
			return err
		}
	}

	for _, name := range resourceNames["NetworkPolicy"] {
		if err := client.deleteNetworkPolicy(ctx, namespace, name); err != nil {
			return err
		}
	}

	for _, name := range resourceNames[runtime.VolSyncReplicationDestinationKind] {
		if err := client.deleteVolSyncReplicationDestination(ctx, namespace, name); err != nil {
			return err
		}
	}

	for _, name := range resourceNames[runtime.VolSyncReplicationSourceKind] {
		if err := client.deleteVolSyncReplicationSource(ctx, namespace, name); err != nil {
			return err
		}
	}

	for _, name := range resourceNames["PersistentVolumeClaim"] {
		if err := client.deletePersistentVolumeClaim(ctx, namespace, name); err != nil {
			return err
		}
	}

	for _, name := range resourceNames["Secret"] {
		if err := client.deleteSecret(ctx, namespace, name); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) managedAppOwnedResourceNames(ctx context.Context, client *kubeClient, namespace string, app model.App) (map[string][]string, error) {
	out := make(map[string]map[string]struct{})
	addNames := func(kind string, names []string) {
		for _, name := range names {
			trimmed := strings.TrimSpace(name)
			if trimmed == "" {
				continue
			}
			if out[kind] == nil {
				out[kind] = make(map[string]struct{})
			}
			out[kind][trimmed] = struct{}{}
		}
	}

	for kind, names := range managedAppExpectedObjectNamesByKind(app) {
		for name := range names {
			addNames(kind, []string{name})
		}
	}

	if strings.TrimSpace(app.ID) != "" {
		deployments, err := s.listOwnedDeploymentNames(ctx, client, namespace, app.ID)
		if err != nil {
			return nil, err
		}
		addNames("Deployment", deployments)

		clusters, err := s.listOwnedCloudNativePGClusterNames(ctx, client, namespace, app.ID)
		if err != nil {
			return nil, err
		}
		addNames(runtime.CloudNativePGClusterKind, clusters)

		services, err := s.listOwnedServiceNames(ctx, client, namespace, app.ID)
		if err != nil {
			return nil, err
		}
		addNames("Service", services)

		networkPolicies, err := s.listOwnedNetworkPolicyNames(ctx, client, namespace, app.ID)
		if err != nil {
			return nil, err
		}
		addNames("NetworkPolicy", networkPolicies)

		pvcs, err := s.listOwnedPersistentVolumeClaimNames(ctx, client, namespace, app.ID)
		if err != nil {
			return nil, err
		}
		addNames("PersistentVolumeClaim", pvcs)

		replicationDestinations, err := s.listOwnedVolSyncReplicationDestinationNames(ctx, client, namespace, app.ID)
		if err != nil {
			return nil, err
		}
		addNames(runtime.VolSyncReplicationDestinationKind, replicationDestinations)

		replicationSources, err := s.listOwnedVolSyncReplicationSourceNames(ctx, client, namespace, app.ID)
		if err != nil {
			return nil, err
		}
		addNames(runtime.VolSyncReplicationSourceKind, replicationSources)

		secrets, err := s.listOwnedSecretNames(ctx, client, namespace, app.ID)
		if err != nil {
			return nil, err
		}
		addNames("Secret", secrets)
	}

	sorted := make(map[string][]string, len(out))
	for kind, names := range out {
		sorted[kind] = setToSortedNames(names)
	}
	return sorted, nil
}

func (s *Service) listOwnedDeploymentNames(ctx context.Context, client *kubeClient, namespace, appID string) ([]string, error) {
	return listOwnedNames(ctx, appID, func(selector string) ([]string, error) {
		return client.listDeploymentNamesByLabel(ctx, namespace, selector)
	})
}

func (s *Service) listOwnedCloudNativePGClusterNames(ctx context.Context, client *kubeClient, namespace, appID string) ([]string, error) {
	return listOwnedNames(ctx, appID, func(selector string) ([]string, error) {
		return client.listCloudNativePGClusterNamesByLabel(ctx, namespace, selector)
	})
}

func (s *Service) listOwnedServiceNames(ctx context.Context, client *kubeClient, namespace, appID string) ([]string, error) {
	return listOwnedNames(ctx, appID, func(selector string) ([]string, error) {
		return client.listServiceNamesByLabel(ctx, namespace, selector)
	})
}

func (s *Service) listOwnedNetworkPolicyNames(ctx context.Context, client *kubeClient, namespace, appID string) ([]string, error) {
	return listOwnedNames(ctx, appID, func(selector string) ([]string, error) {
		return client.listNetworkPolicyNamesByLabel(ctx, namespace, selector)
	})
}

func (s *Service) listOwnedPersistentVolumeClaimNames(ctx context.Context, client *kubeClient, namespace, appID string) ([]string, error) {
	return listOwnedNames(ctx, appID, func(selector string) ([]string, error) {
		return client.listPersistentVolumeClaimNamesByLabel(ctx, namespace, selector)
	})
}

func (s *Service) listOwnedVolSyncReplicationDestinationNames(ctx context.Context, client *kubeClient, namespace, appID string) ([]string, error) {
	return listOwnedNames(ctx, appID, func(selector string) ([]string, error) {
		return client.listVolSyncReplicationDestinationNamesByLabel(ctx, namespace, selector)
	})
}

func (s *Service) listOwnedVolSyncReplicationSourceNames(ctx context.Context, client *kubeClient, namespace, appID string) ([]string, error) {
	return listOwnedNames(ctx, appID, func(selector string) ([]string, error) {
		return client.listVolSyncReplicationSourceNamesByLabel(ctx, namespace, selector)
	})
}

func (s *Service) listOwnedSecretNames(ctx context.Context, client *kubeClient, namespace, appID string) ([]string, error) {
	return listOwnedNames(ctx, appID, func(selector string) ([]string, error) {
		return client.listSecretNamesByLabel(ctx, namespace, selector)
	})
}

func listOwnedNames(ctx context.Context, appID string, fn func(selector string) ([]string, error)) ([]string, error) {
	if strings.TrimSpace(appID) == "" {
		return nil, nil
	}

	unique := make(map[string]struct{})
	for _, selector := range ownedAppSelectors(appID) {
		names, err := fn(selector)
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			trimmed := strings.TrimSpace(name)
			if trimmed == "" {
				continue
			}
			unique[trimmed] = struct{}{}
		}
	}

	out := make([]string, 0, len(unique))
	for name := range unique {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func backfillManagedAppSource(app *model.App, stored model.App) {
	if app == nil || app.Source != nil || stored.Source == nil {
		return
	}
	model.SetAppSourceState(app, model.AppOriginSource(stored), model.AppBuildSource(stored))
}

func backfillManagedAppBackingServices(app *model.App, stored model.App) {
	if app == nil {
		return
	}
	app.BackingServices = cloneControllerBackingServices(stored.BackingServices)
	app.Bindings = cloneControllerServiceBindings(stored.Bindings)
}

func cloneControllerBackingServices(services []model.BackingService) []model.BackingService {
	if len(services) == 0 {
		return nil
	}
	out := make([]model.BackingService, len(services))
	for index, service := range services {
		out[index] = service
		out[index].Spec = cloneControllerBackingServiceSpec(service.Spec)
	}
	return out
}

func cloneControllerBackingServiceSpec(spec model.BackingServiceSpec) model.BackingServiceSpec {
	out := spec
	if spec.Postgres != nil {
		postgres := *spec.Postgres
		if spec.Postgres.Resources != nil {
			resources := *spec.Postgres.Resources
			postgres.Resources = &resources
		}
		out.Postgres = &postgres
	}
	return out
}

func cloneControllerServiceBindings(bindings []model.ServiceBinding) []model.ServiceBinding {
	if len(bindings) == 0 {
		return nil
	}
	out := make([]model.ServiceBinding, len(bindings))
	for index, binding := range bindings {
		out[index] = binding
		if len(binding.Env) == 0 {
			continue
		}
		out[index].Env = make(map[string]string, len(binding.Env))
		for key, value := range binding.Env {
			out[index].Env[key] = value
		}
	}
	return out
}

func managedAppExpectedObjectNamesByKind(app model.App) map[string]map[string]struct{} {
	out := desiredObjectNamesByKind(runtime.BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil))
	if runtime.AppVolumeReplicationEnabled(app) {
		if out[runtime.VolSyncReplicationSourceKind] == nil {
			out[runtime.VolSyncReplicationSourceKind] = make(map[string]struct{})
		}
		out[runtime.VolSyncReplicationSourceKind][runtime.WorkspaceReplicationSourceName(app)] = struct{}{}
	}
	return out
}

func setToSortedNames(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func desiredObjectNamesByKind(objects []map[string]any) map[string]map[string]struct{} {
	out := make(map[string]map[string]struct{})
	for _, obj := range objects {
		kind, _ := obj["kind"].(string)
		metadata, _ := obj["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		kind = strings.TrimSpace(kind)
		name = strings.TrimSpace(name)
		if kind == "" || name == "" {
			continue
		}
		if out[kind] == nil {
			out[kind] = make(map[string]struct{})
		}
		out[kind][name] = struct{}{}
	}
	return out
}

func ownedAppSelectors(appID string) []string {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil
	}
	return []string{
		runtime.FugueLabelOwnerAppID + "=" + appID,
		runtime.FugueLabelAppID + "=" + appID,
	}
}

func maxInt(values ...int) int {
	max := 0
	for index, value := range values {
		if index == 0 || value > max {
			max = value
		}
	}
	return max
}
