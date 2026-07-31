package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"fugue/internal/localpvsafety"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"

	"github.com/jackc/pgx/v5"
	"k8s.io/apimachinery/pkg/api/resource"
)

const managedPostgresPodSelectorTemplate = "cnpg.io/cluster=%s,app.kubernetes.io/managed-by=cloudnative-pg"

const openEBSLocalLVMProvisioner = "local.csi.openebs.io"

const managedPostgresStageRollbackTimeout = 2 * time.Minute

const managedPostgresReplicationStableSamples = 3

const managedPostgresPrimaryReadinessQuery = `
SELECT pg_is_in_recovery(), current_setting('transaction_read_only'), COALESCE(host(inet_server_addr()), '')
`

func (s *Service) executeManagedDatabaseSwitchoverOperation(
	ctx context.Context,
	op model.Operation,
	app model.App,
) error {
	if !s.Config.KubectlApply {
		return fmt.Errorf("database switchover requires kubernetes apply mode")
	}
	targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
	if targetRuntimeID == "" {
		return fmt.Errorf("database switchover operation %s missing target runtime", op.ID)
	}

	target, err := store.ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil {
		return fmt.Errorf("resolve managed postgres target for app %s: %w", app.ID, err)
	}
	if target == nil {
		return fmt.Errorf("managed postgres is not configured for app %s", app.ID)
	}
	currentDatabase := &target.Postgres
	if strings.TrimSpace(target.ServiceID) != "" && !target.AppOwned {
		return s.executeBoundManagedDatabaseSwitchoverOperation(ctx, op, app, *target)
	}
	sourceRuntimeID := strings.TrimSpace(op.SourceRuntimeID)
	if sourceRuntimeID == "" {
		sourceRuntimeID = strings.TrimSpace(currentDatabase.RuntimeID)
	}
	if sourceRuntimeID == "" {
		sourceRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	if sourceRuntimeID == "" {
		return fmt.Errorf("managed postgres for app %s is missing a source runtime", app.ID)
	}

	clusterName := strings.TrimSpace(currentDatabase.ServiceName)
	if clusterName == "" {
		return fmt.Errorf("managed postgres for app %s is missing a cluster service name", app.ID)
	}
	clusterName = model.NormalizePostgresServiceName(clusterName, "")

	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes client for database switchover: %w", err)
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	targetPrimary, alreadySwitched, err := s.managedPostgresPrimaryMatchesTarget(ctx, client, namespace, clusterName, targetRuntimeID, "")
	if err != nil {
		return err
	}
	if !alreadySwitched {
		if sourceRuntimeID == targetRuntimeID {
			return fmt.Errorf("managed postgres for app %s is configured on runtime %s but the observed primary is elsewhere", app.ID, targetRuntimeID)
		}
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("preparing managed postgres standby on runtime %s", targetRuntimeID))
		stageSpec := databaseSwitchoverStageSpec(app.Spec, currentDatabase, sourceRuntimeID, targetRuntimeID)
		if _, err := s.applyManagedDesiredAppState(ctx, op.ID, app, stageSpec); err != nil {
			cause := fmt.Errorf("prepare managed postgres standby on %s: %w", targetRuntimeID, err)
			return s.rollbackAppOwnedManagedPostgresStage(ctx, op, app, currentDatabase, cause)
		}

		targetPrimary, err = s.waitForManagedPostgresReplicaOnRuntime(
			ctx,
			client,
			namespace,
			clusterName,
			targetRuntimeID,
			op.ID,
		)
		if err != nil {
			cause := fmt.Errorf("wait for managed postgres standby on %s: %w", targetRuntimeID, err)
			return s.rollbackAppOwnedManagedPostgresStage(ctx, op, app, currentDatabase, cause)
		}
		if err := s.waitForManagedPostgresReplicationCatchup(ctx, client, namespace, clusterName, targetPrimary, op.ID, *currentDatabase); err != nil {
			cause := fmt.Errorf("wait for managed postgres standby %s replication catch-up: %w", targetPrimary, err)
			return s.rollbackAppOwnedManagedPostgresStage(ctx, op, app, currentDatabase, cause)
		}

		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return s.rollbackAppOwnedManagedPostgresStage(ctx, op, app, currentDatabase, err)
		}
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("standby %s is ready; requesting managed postgres switchover", targetPrimary))
		if err := client.patchCloudNativePGClusterStatus(
			ctx,
			namespace,
			clusterName,
			targetPrimary,
			"Switchover",
			fmt.Sprintf("Switching over to %s", targetPrimary),
		); err != nil {
			return recoverableManagedPostgresTransitionError("switchover request", fmt.Errorf("request managed postgres switchover to %s: %w", targetPrimary, err))
		}
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("managed postgres switchover to %s requested; waiting for observed primary", targetPrimary))
	} else {
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("observed managed postgres primary %s on target runtime; resuming finalization", targetPrimary))
	}

	placementBefore, err := s.waitForManagedPostgresPrimary(
		ctx, client, namespace, clusterName, targetPrimary, op.ID, *currentDatabase,
	)
	if err != nil {
		return recoverableManagedPostgresTransitionError("primary convergence", fmt.Errorf("wait for managed postgres switchover to %s: %w", targetPrimary, err))
	}
	if strings.TrimSpace(placementBefore.RuntimeID) != targetRuntimeID {
		return recoverableManagedPostgresTransitionError("placement witness", fmt.Errorf(
			"managed postgres SQL-ready primary %s is on runtime %s, want %s",
			placementBefore.PrimaryPod, placementBefore.RuntimeID, targetRuntimeID,
		))
	}
	finalPostgres := databaseSwitchoverFinalPostgresSpec(currentDatabase, targetRuntimeID, sourceRuntimeID, placementBefore.NodeName)
	finalSpec := app.Spec
	finalSpec.Postgres = &finalPostgres
	finalBundle, err := s.applyManagedDesiredAppState(ctx, op.ID, app, finalSpec)
	if err != nil {
		return recoverableManagedPostgresTransitionError("finalization", fmt.Errorf("finalize managed postgres runtime assignments: %w", err))
	}
	placementAfter, err := s.waitForManagedPostgresPrimary(
		ctx,
		client,
		namespace,
		clusterName,
		targetPrimary,
		op.ID,
		*currentDatabase,
	)
	if err != nil {
		return recoverableManagedPostgresTransitionError("final convergence", fmt.Errorf("wait for managed postgres to settle after switchover: %w", err))
	}
	if strings.TrimSpace(placementAfter.RuntimeID) != targetRuntimeID ||
		!managedPostgresPrimaryPlacementsEqual(placementBefore, placementAfter) {
		return recoverableManagedPostgresTransitionError("placement witness", fmt.Errorf(
			"managed postgres SQL-ready placement changed during finalization: before=%+v after=%+v",
			placementBefore, placementAfter,
		))
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return recoverableManagedPostgresTransitionError("completion", err)
	}

	message := fmt.Sprintf("managed postgres switched over from %s to %s", sourceRuntimeID, targetRuntimeID)
	_, err = s.Store.CompleteManagedPostgresSwitchoverWithPlacement(
		op.ID,
		finalBundle.ManifestPath,
		message,
		managedPostgresPlacementMutation(app, *target, *currentDatabase, finalPostgres, placementAfter),
	)
	if err != nil {
		return recoverableManagedPostgresTransitionError("completion", fmt.Errorf("complete database switchover operation %s: %w", op.ID, err))
	}
	s.Logger.Printf(
		"operation %s completed managed postgres switchover from %s to %s; manifest=%s",
		op.ID,
		sourceRuntimeID,
		targetRuntimeID,
		finalBundle.ManifestPath,
	)
	return nil
}

func (s *Service) executeBoundManagedDatabaseSwitchoverOperation(
	ctx context.Context,
	op model.Operation,
	app model.App,
	target store.ManagedPostgresOperationTarget,
) error {
	currentDatabase := &target.Postgres
	targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
	sourceRuntimeID := strings.TrimSpace(op.SourceRuntimeID)
	if sourceRuntimeID == "" {
		sourceRuntimeID = strings.TrimSpace(currentDatabase.RuntimeID)
	}
	if sourceRuntimeID == "" {
		sourceRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	if sourceRuntimeID == "" {
		return fmt.Errorf("managed postgres service %s for app %s is missing a source runtime", target.ServiceID, app.ID)
	}
	clusterName := strings.TrimSpace(currentDatabase.ServiceName)
	if clusterName == "" {
		return fmt.Errorf("managed postgres service %s for app %s is missing a cluster service name", target.ServiceID, app.ID)
	}
	clusterName = model.NormalizePostgresServiceName(clusterName, "")

	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes client for database switchover: %w", err)
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	targetPrimary, alreadySwitched, err := s.managedPostgresPrimaryMatchesTarget(ctx, client, namespace, clusterName, targetRuntimeID, "")
	if err != nil {
		return err
	}
	if !alreadySwitched {
		if sourceRuntimeID == targetRuntimeID {
			return fmt.Errorf("managed postgres service %s is configured on runtime %s but the observed primary is elsewhere", target.ServiceID, targetRuntimeID)
		}
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("preparing managed postgres service %s standby on runtime %s", target.ServiceID, targetRuntimeID))
		stagePostgres := databaseSwitchoverStagePostgresSpec(currentDatabase, sourceRuntimeID, targetRuntimeID)
		stageApp, err := appWithBackingServicePostgres(target.ServiceID, app, stagePostgres)
		if err != nil {
			return fmt.Errorf("stage managed postgres service %s standby on %s: %w", target.ServiceID, targetRuntimeID, err)
		}
		if _, err := s.applyManagedDesiredAppState(ctx, op.ID, stageApp, stageApp.Spec); err != nil {
			cause := fmt.Errorf("prepare managed postgres service %s standby on %s: %w", target.ServiceID, targetRuntimeID, err)
			return s.rollbackBoundManagedPostgresEphemeralStage(ctx, op, app, target.ServiceID, currentDatabase, cause)
		}

		targetPrimary, err = s.waitForManagedPostgresReplicaOnRuntime(ctx, client, namespace, clusterName, targetRuntimeID, op.ID)
		if err != nil {
			cause := fmt.Errorf("wait for managed postgres service %s standby on %s: %w", target.ServiceID, targetRuntimeID, err)
			return s.rollbackBoundManagedPostgresEphemeralStage(ctx, op, app, target.ServiceID, currentDatabase, cause)
		}
		if err := s.waitForManagedPostgresReplicationCatchup(ctx, client, namespace, clusterName, targetPrimary, op.ID, *currentDatabase); err != nil {
			cause := fmt.Errorf("wait for managed postgres service %s standby %s replication catch-up: %w", target.ServiceID, targetPrimary, err)
			return s.rollbackBoundManagedPostgresEphemeralStage(ctx, op, app, target.ServiceID, currentDatabase, cause)
		}

		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return s.rollbackBoundManagedPostgresEphemeralStage(ctx, op, app, target.ServiceID, currentDatabase, err)
		}
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("standby %s is ready; requesting managed postgres service %s switchover", targetPrimary, target.ServiceID))
		if err := client.patchCloudNativePGClusterStatus(
			ctx,
			namespace,
			clusterName,
			targetPrimary,
			"Switchover",
			fmt.Sprintf("Switching over service %s to %s", target.ServiceID, targetPrimary),
		); err != nil {
			return recoverableManagedPostgresTransitionError("switchover request", fmt.Errorf("request managed postgres service %s switchover to %s: %w", target.ServiceID, targetPrimary, err))
		}
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("managed postgres service %s switchover to %s requested; waiting for observed primary", target.ServiceID, targetPrimary))
	} else {
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("observed managed postgres service %s primary %s on target runtime; resuming finalization", target.ServiceID, targetPrimary))
	}

	placementBefore, err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID, *currentDatabase)
	if err != nil {
		return recoverableManagedPostgresTransitionError("primary convergence", fmt.Errorf("wait for managed postgres service %s switchover to %s: %w", target.ServiceID, targetPrimary, err))
	}
	if strings.TrimSpace(placementBefore.RuntimeID) != targetRuntimeID {
		return recoverableManagedPostgresTransitionError("placement witness", fmt.Errorf(
			"managed postgres service %s SQL-ready primary %s is on runtime %s, want %s",
			target.ServiceID, placementBefore.PrimaryPod, placementBefore.RuntimeID, targetRuntimeID,
		))
	}
	finalPostgres := databaseSwitchoverFinalPostgresSpec(currentDatabase, targetRuntimeID, sourceRuntimeID, placementBefore.NodeName)
	finalApp, err := appWithBackingServicePostgres(target.ServiceID, app, finalPostgres)
	if err != nil {
		return recoverableManagedPostgresTransitionError("finalization", fmt.Errorf("finalize managed postgres service %s runtime assignments: %w", target.ServiceID, err))
	}
	finalBundle, err := s.applyManagedDesiredAppState(ctx, op.ID, finalApp, finalApp.Spec)
	if err != nil {
		return recoverableManagedPostgresTransitionError("finalization", fmt.Errorf("apply finalized managed postgres service %s state: %w", target.ServiceID, err))
	}
	placementAfter, err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID, *currentDatabase)
	if err != nil {
		return recoverableManagedPostgresTransitionError("final convergence", fmt.Errorf("wait for managed postgres service %s to settle after switchover: %w", target.ServiceID, err))
	}
	if strings.TrimSpace(placementAfter.RuntimeID) != targetRuntimeID ||
		!managedPostgresPrimaryPlacementsEqual(placementBefore, placementAfter) {
		return recoverableManagedPostgresTransitionError("placement witness", fmt.Errorf(
			"managed postgres service %s SQL-ready placement changed during finalization: before=%+v after=%+v",
			target.ServiceID, placementBefore, placementAfter,
		))
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return recoverableManagedPostgresTransitionError("completion", err)
	}

	message := fmt.Sprintf("managed postgres service %s switched over from %s to %s", target.ServiceID, sourceRuntimeID, targetRuntimeID)
	_, err = s.Store.CompleteManagedPostgresSwitchoverWithPlacement(
		op.ID,
		finalBundle.ManifestPath,
		message,
		managedPostgresPlacementMutation(app, target, *currentDatabase, finalPostgres, placementAfter),
	)
	if err != nil {
		return recoverableManagedPostgresTransitionError("completion", fmt.Errorf("complete database switchover operation %s: %w", op.ID, err))
	}
	s.Logger.Printf(
		"operation %s completed managed postgres service %s switchover from %s to %s; manifest=%s",
		op.ID,
		target.ServiceID,
		sourceRuntimeID,
		targetRuntimeID,
		finalBundle.ManifestPath,
	)
	return nil
}

func (s *Service) updateAppBackingServicePostgres(serviceID string, app model.App, postgres model.AppPostgresSpec) (model.App, error) {
	spec := model.BackingServiceSpec{Postgres: &postgres}
	updated, err := s.Store.UpdateBackingServiceSpec(serviceID, spec)
	if err != nil {
		return model.App{}, err
	}
	next := app
	replaced := false
	for index, service := range next.BackingServices {
		if strings.TrimSpace(service.ID) != strings.TrimSpace(serviceID) {
			continue
		}
		next.BackingServices[index] = updated
		replaced = true
		break
	}
	if !replaced {
		next.BackingServices = append(next.BackingServices, updated)
	}
	return next, nil
}

func appWithBackingServicePostgres(serviceID string, app model.App, postgres model.AppPostgresSpec) (model.App, error) {
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return model.App{}, fmt.Errorf("managed postgres backing service id is required")
	}
	next := app
	next.BackingServices = append([]model.BackingService(nil), app.BackingServices...)
	for index := range next.BackingServices {
		if strings.TrimSpace(next.BackingServices[index].ID) != serviceID {
			continue
		}
		service := next.BackingServices[index]
		service.Spec.Postgres = model.CloneAppPostgresSpec(&postgres)
		next.BackingServices[index] = service
		return next, nil
	}
	return model.App{}, fmt.Errorf("managed postgres backing service %s is not attached to app %s", serviceID, app.ID)
}

func (s *Service) rollbackAppOwnedManagedPostgresStage(
	ctx context.Context,
	op model.Operation,
	app model.App,
	stablePostgres *model.AppPostgresSpec,
	cause error,
) error {
	return s.rollbackManagedPostgresStage(ctx, op, cause, func(rollbackCtx context.Context) error {
		stableSpec := app.Spec
		stableSpec.Postgres = model.CloneAppPostgresSpec(stablePostgres)
		_, err := s.applyManagedDesiredAppState(rollbackCtx, op.ID, app, stableSpec)
		return err
	})
}

func (s *Service) rollbackBoundManagedPostgresStage(
	ctx context.Context,
	op model.Operation,
	app model.App,
	serviceID string,
	stablePostgres *model.AppPostgresSpec,
	cause error,
) error {
	return s.rollbackManagedPostgresStage(ctx, op, cause, func(rollbackCtx context.Context) error {
		stableApp, err := s.updateAppBackingServicePostgres(serviceID, app, *model.CloneAppPostgresSpec(stablePostgres))
		if err != nil {
			return fmt.Errorf("restore managed postgres service %s persisted state: %w", serviceID, err)
		}
		if _, err := s.applyManagedDesiredAppState(rollbackCtx, op.ID, stableApp, stableApp.Spec); err != nil {
			return fmt.Errorf("apply restored managed postgres service %s state: %w", serviceID, err)
		}
		return nil
	})
}

func (s *Service) rollbackBoundManagedPostgresEphemeralStage(
	ctx context.Context,
	op model.Operation,
	app model.App,
	serviceID string,
	stablePostgres *model.AppPostgresSpec,
	cause error,
) error {
	return s.rollbackManagedPostgresStage(ctx, op, cause, func(rollbackCtx context.Context) error {
		stableApp, err := appWithBackingServicePostgres(serviceID, app, *model.CloneAppPostgresSpec(stablePostgres))
		if err != nil {
			return fmt.Errorf("restore managed postgres service %s rendered state: %w", serviceID, err)
		}
		if _, err := s.applyManagedDesiredAppState(rollbackCtx, op.ID, stableApp, stableApp.Spec); err != nil {
			return fmt.Errorf("apply restored managed postgres service %s state: %w", serviceID, err)
		}
		return nil
	})
}

func (s *Service) rollbackManagedPostgresStage(
	ctx context.Context,
	op model.Operation,
	cause error,
	rollback func(context.Context) error,
) error {
	if cause == nil {
		return nil
	}
	// A canceled controller run is requeued by the worker. Leaving the staged
	// replica in place is safe and lets the resumed operation adopt it. Likewise,
	// never overwrite state after ownership of this operation has changed.
	if ctx.Err() != nil || errors.Is(cause, errOperationNoLongerActive) {
		return cause
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return fmt.Errorf("%w; automatic staged-state rollback skipped because operation ownership could not be confirmed: %v", cause, err)
	}

	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), managedPostgresStageRollbackTimeout)
	defer cancel()
	rollbackCtx = withForceExistingCloudNativePGWrites(rollbackCtx)
	s.updateManagedPostgresTransitionProgress(op.ID, "managed postgres standby preparation failed; restoring the previous stable state")
	if err := rollback(rollbackCtx); err != nil {
		return fmt.Errorf("%w; automatic staged-state rollback failed: %v", cause, err)
	}
	s.updateManagedPostgresTransitionProgress(op.ID, "managed postgres standby preparation failed; previous stable state restored")
	return fmt.Errorf("%w; staged managed postgres state was rolled back", cause)
}

func (s *Service) updateManagedPostgresTransitionProgress(operationID, message string) {
	if s == nil || s.Store == nil || strings.TrimSpace(operationID) == "" || strings.TrimSpace(message) == "" {
		return
	}
	if _, err := s.Store.UpdateOperationProgress(operationID, message); err != nil && s.Logger != nil {
		s.Logger.Printf("update managed postgres transition %s progress failed: %v", operationID, err)
	}
}

func recoverableManagedPostgresTransitionError(phase string, cause error) error {
	phase = strings.TrimSpace(phase)
	if phase == "" {
		phase = "an observed-state transition"
	}
	return fmt.Errorf(
		"managed postgres transition is recoverable from observed cluster state after %s; retrying the operation will resume without issuing a blind rollback: %w",
		phase,
		cause,
	)
}

func (s *Service) executeManagedDatabaseLocalizeOperation(
	ctx context.Context,
	op model.Operation,
	app model.App,
) error {
	if !s.Config.KubectlApply {
		return fmt.Errorf("database localize requires kubernetes apply mode")
	}

	target, err := store.ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil {
		return fmt.Errorf("resolve managed postgres target for app %s: %w", app.ID, err)
	}
	if target == nil {
		return fmt.Errorf("managed postgres is not configured for app %s", app.ID)
	}
	if strings.TrimSpace(target.ServiceID) != "" && !target.AppOwned {
		return s.executeBoundManagedDatabaseLocalizeOperation(ctx, op, app, *target)
	}
	currentDatabase := &target.Postgres
	sourceRuntimeID := strings.TrimSpace(op.SourceRuntimeID)
	if sourceRuntimeID == "" {
		sourceRuntimeID = strings.TrimSpace(currentDatabase.RuntimeID)
	}
	if sourceRuntimeID == "" {
		sourceRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
	if targetRuntimeID == "" && op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
		targetRuntimeID = strings.TrimSpace(op.DesiredSpec.Postgres.RuntimeID)
	}
	if targetRuntimeID == "" {
		targetRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	if sourceRuntimeID == "" || targetRuntimeID == "" {
		return fmt.Errorf("database localize operation %s missing source or target runtime", op.ID)
	}
	desiredDatabase := databaseLocalizeDesiredPostgresSpec(currentDatabase, desiredPostgresSpec(op))
	storageMigrationRequired := managedPostgresStorageMigrationRequired(currentDatabase, desiredDatabase)
	storageTarget := databaseLocalizeStorageTarget(storageMigrationRequired, desiredDatabase)

	clusterName := strings.TrimSpace(currentDatabase.ServiceName)
	if clusterName == "" {
		return fmt.Errorf("managed postgres for app %s is missing a cluster service name", app.ID)
	}
	clusterName = model.NormalizePostgresServiceName(clusterName, "")

	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes client for database localize: %w", err)
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	targetNodeName := ""
	if op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
		targetNodeName = strings.TrimSpace(op.DesiredSpec.Postgres.PrimaryNodeName)
	}
	inPlaceStorageExpansionRequired := managedPostgresInPlaceStorageExpansionRequired(currentDatabase, desiredDatabase, sourceRuntimeID, targetRuntimeID, targetNodeName)
	if !inPlaceStorageExpansionRequired {
		targetNodeName, err = s.resolveDatabaseLocalizeTargetNode(ctx, client, app, targetRuntimeID, targetNodeName)
		if err != nil {
			return err
		}
	}
	restoreStorageExpansion, err := s.prepareManagedPostgresStorageMigrationExpansion(ctx, client, namespace, clusterName, storageTarget)
	if err != nil {
		return err
	}
	if restoreStorageExpansion != nil {
		defer func() {
			if err := restoreStorageExpansion(context.Background()); err != nil && s != nil && s.Logger != nil {
				s.Logger.Printf("restore storage expansion state after database localize %s: %v", op.ID, err)
			}
		}()
	}
	if inPlaceStorageExpansionRequired {
		if err := s.prepareManagedPostgresInPlaceStorageExpansionForExistingCluster(ctx, client, namespace, clusterName, storageTarget); err != nil {
			return err
		}
	}

	targetPrimary := ""
	currentPrimary, alreadyLocalized, err := s.managedPostgresPrimaryMatchesTarget(ctx, client, namespace, clusterName, targetRuntimeID, targetNodeName)
	if err != nil {
		return err
	}
	if inPlaceStorageExpansionRequired {
		targetPrimary = currentPrimary
	} else if alreadyLocalized {
		targetPrimary = currentPrimary
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("observed managed postgres primary %s on localization target; resuming finalization", targetPrimary))
	} else {
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("preparing localized managed postgres standby on runtime %s", targetRuntimeID))
		stageSpec := databaseLocalizeStageSpec(app.Spec, desiredDatabase, sourceRuntimeID, targetRuntimeID, targetNodeName)
		if storageMigrationRequired && !inPlaceStorageExpansionRequired && stageSpec.Postgres != nil {
			ensureDatabaseLocalizeStorageMigrationCapacity(stageSpec.Postgres, currentDatabase)
		}
		if _, err := s.applyManagedDesiredAppState(ctx, op.ID, app, stageSpec); err != nil {
			cause := fmt.Errorf("prepare localized managed postgres standby on runtime %s: %w", targetRuntimeID, err)
			return s.rollbackAppOwnedManagedPostgresStage(ctx, op, app, currentDatabase, cause)
		}
		if targetNodeName != "" {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnNode(ctx, client, namespace, clusterName, targetNodeName, op.ID, storageTarget)
		} else {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnRuntime(ctx, client, namespace, clusterName, targetRuntimeID, op.ID, storageTarget)
		}
		if err != nil {
			cause := fmt.Errorf("wait for localized managed postgres standby on runtime %s: %w", targetRuntimeID, err)
			return s.rollbackAppOwnedManagedPostgresStage(ctx, op, app, currentDatabase, cause)
		}
		if err := s.waitForManagedPostgresReplicationCatchup(ctx, client, namespace, clusterName, targetPrimary, op.ID, *currentDatabase); err != nil {
			cause := fmt.Errorf("wait for localized managed postgres standby %s replication catch-up: %w", targetPrimary, err)
			return s.rollbackAppOwnedManagedPostgresStage(ctx, op, app, currentDatabase, cause)
		}
		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return s.rollbackAppOwnedManagedPostgresStage(ctx, op, app, currentDatabase, err)
		}
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("standby %s is ready; requesting managed postgres localization switchover", targetPrimary))
		if err := client.patchCloudNativePGClusterStatus(
			ctx,
			namespace,
			clusterName,
			targetPrimary,
			"Switchover",
			fmt.Sprintf("Localizing managed postgres primary to %s", targetPrimary),
		); err != nil {
			return recoverableManagedPostgresTransitionError("localize switchover request", fmt.Errorf("request managed postgres localize switchover to %s: %w", targetPrimary, err))
		}
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("managed postgres localization switchover to %s requested; waiting for observed primary", targetPrimary))
		if _, err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID, *currentDatabase); err != nil {
			return recoverableManagedPostgresTransitionError("primary convergence", fmt.Errorf("wait for managed postgres localize switchover to %s: %w", targetPrimary, err))
		}
	}

	finalSpec := databaseLocalizeSpec(app.Spec, desiredDatabase, targetRuntimeID, targetNodeName, true, false)
	finalBundle, err := s.applyManagedDesiredAppState(ctx, op.ID, app, finalSpec)
	if err != nil {
		return recoverableManagedPostgresTransitionError("finalization", fmt.Errorf("finalize localized managed postgres state: %w", err))
	}
	// In-place expansion must take precedence over the normal primary check:
	// a healthy primary says nothing about PVC or filesystem resize completion.
	if inPlaceStorageExpansionRequired {
		if err := s.waitForManagedPostgresStorageExpansion(ctx, client, namespace, clusterName, op.ID, storageTarget); err != nil {
			return recoverableManagedPostgresTransitionError("storage convergence", fmt.Errorf("wait for expanded managed postgres to settle: %w", err))
		}
	} else if targetPrimary != "" {
		if _, err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID, *currentDatabase); err != nil {
			return recoverableManagedPostgresTransitionError("final convergence", fmt.Errorf("wait for localized managed postgres to settle: %w", err))
		}
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return recoverableManagedPostgresTransitionError("completion", err)
	}

	message := fmt.Sprintf("managed postgres localized to runtime %s", targetRuntimeID)
	if targetNodeName != "" {
		message = fmt.Sprintf("managed postgres localized to runtime %s node %s", targetRuntimeID, targetNodeName)
	}
	_, err = s.Store.CompleteManagedOperationWithResult(
		op.ID,
		finalBundle.ManifestPath,
		message,
		&finalSpec,
		nil,
	)
	if err != nil {
		return recoverableManagedPostgresTransitionError("completion", fmt.Errorf("complete database localize operation %s: %w", op.ID, err))
	}
	s.Logger.Printf(
		"operation %s completed managed postgres localize from runtime %s to %s node=%s; manifest=%s",
		op.ID,
		sourceRuntimeID,
		targetRuntimeID,
		targetNodeName,
		finalBundle.ManifestPath,
	)
	return nil
}

func (s *Service) executeBoundManagedDatabaseLocalizeOperation(
	ctx context.Context,
	op model.Operation,
	app model.App,
	target store.ManagedPostgresOperationTarget,
) error {
	currentDatabase := &target.Postgres
	sourceRuntimeID := strings.TrimSpace(op.SourceRuntimeID)
	if sourceRuntimeID == "" {
		sourceRuntimeID = strings.TrimSpace(currentDatabase.RuntimeID)
	}
	if sourceRuntimeID == "" {
		sourceRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
	if targetRuntimeID == "" && op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
		targetRuntimeID = strings.TrimSpace(op.DesiredSpec.Postgres.RuntimeID)
	}
	if targetRuntimeID == "" {
		targetRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	if sourceRuntimeID == "" || targetRuntimeID == "" {
		return fmt.Errorf("database localize operation %s missing source or target runtime", op.ID)
	}
	desiredDatabase := databaseLocalizeDesiredPostgresSpec(currentDatabase, desiredPostgresSpec(op))
	storageMigrationRequired := managedPostgresStorageMigrationRequired(currentDatabase, desiredDatabase)
	storageTarget := databaseLocalizeStorageTarget(storageMigrationRequired, desiredDatabase)

	clusterName := strings.TrimSpace(currentDatabase.ServiceName)
	if clusterName == "" {
		return fmt.Errorf("managed postgres service %s for app %s is missing a cluster service name", target.ServiceID, app.ID)
	}
	clusterName = model.NormalizePostgresServiceName(clusterName, "")

	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes client for database localize: %w", err)
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	targetNodeName := ""
	if op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
		targetNodeName = strings.TrimSpace(op.DesiredSpec.Postgres.PrimaryNodeName)
	}
	inPlaceStorageExpansionRequired := managedPostgresInPlaceStorageExpansionRequired(currentDatabase, desiredDatabase, sourceRuntimeID, targetRuntimeID, targetNodeName)
	if !inPlaceStorageExpansionRequired {
		targetNodeName, err = s.resolveDatabaseLocalizeTargetNode(ctx, client, app, targetRuntimeID, targetNodeName)
		if err != nil {
			return err
		}
	}
	restoreStorageExpansion, err := s.prepareManagedPostgresStorageMigrationExpansion(ctx, client, namespace, clusterName, storageTarget)
	if err != nil {
		return err
	}
	if restoreStorageExpansion != nil {
		defer func() {
			if err := restoreStorageExpansion(context.Background()); err != nil && s != nil && s.Logger != nil {
				s.Logger.Printf("restore storage expansion state after database localize %s: %v", op.ID, err)
			}
		}()
	}
	if inPlaceStorageExpansionRequired {
		if err := s.prepareManagedPostgresInPlaceStorageExpansionForExistingCluster(ctx, client, namespace, clusterName, storageTarget); err != nil {
			return err
		}
	}

	targetPrimary := ""
	currentPrimary, alreadyLocalized, err := s.managedPostgresPrimaryMatchesTarget(ctx, client, namespace, clusterName, targetRuntimeID, targetNodeName)
	if err != nil {
		return err
	}
	if inPlaceStorageExpansionRequired {
		targetPrimary = currentPrimary
	} else if alreadyLocalized {
		targetPrimary = currentPrimary
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("observed managed postgres service %s primary %s on localization target; resuming finalization", target.ServiceID, targetPrimary))
	} else {
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("preparing localized managed postgres service %s standby on runtime %s", target.ServiceID, targetRuntimeID))
		stagePostgres := databaseLocalizeStagePostgresSpec(desiredDatabase, sourceRuntimeID, targetRuntimeID, targetNodeName)
		if storageMigrationRequired && !inPlaceStorageExpansionRequired {
			ensureDatabaseLocalizeStorageMigrationCapacity(&stagePostgres, currentDatabase)
		}
		stageApp, err := s.updateAppBackingServicePostgres(target.ServiceID, app, stagePostgres)
		if err != nil {
			return fmt.Errorf("prepare localized managed postgres service %s state: %w", target.ServiceID, err)
		}
		if _, err := s.applyManagedDesiredAppState(ctx, op.ID, stageApp, stageApp.Spec); err != nil {
			cause := fmt.Errorf("prepare localized managed postgres service %s standby on runtime %s: %w", target.ServiceID, targetRuntimeID, err)
			return s.rollbackBoundManagedPostgresStage(ctx, op, app, target.ServiceID, currentDatabase, cause)
		}
		if targetNodeName != "" {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnNode(ctx, client, namespace, clusterName, targetNodeName, op.ID, storageTarget)
		} else {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnRuntime(ctx, client, namespace, clusterName, targetRuntimeID, op.ID, storageTarget)
		}
		if err != nil {
			cause := fmt.Errorf("wait for localized managed postgres service %s standby on runtime %s: %w", target.ServiceID, targetRuntimeID, err)
			return s.rollbackBoundManagedPostgresStage(ctx, op, app, target.ServiceID, currentDatabase, cause)
		}
		if err := s.waitForManagedPostgresReplicationCatchup(ctx, client, namespace, clusterName, targetPrimary, op.ID, *currentDatabase); err != nil {
			cause := fmt.Errorf("wait for localized managed postgres service %s standby %s replication catch-up: %w", target.ServiceID, targetPrimary, err)
			return s.rollbackBoundManagedPostgresStage(ctx, op, app, target.ServiceID, currentDatabase, cause)
		}
		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return s.rollbackBoundManagedPostgresStage(ctx, op, app, target.ServiceID, currentDatabase, err)
		}
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("standby %s is ready; requesting managed postgres service %s localization switchover", targetPrimary, target.ServiceID))
		if err := client.patchCloudNativePGClusterStatus(
			ctx,
			namespace,
			clusterName,
			targetPrimary,
			"Switchover",
			fmt.Sprintf("Localizing managed postgres service %s primary to %s", target.ServiceID, targetPrimary),
		); err != nil {
			return recoverableManagedPostgresTransitionError("localize switchover request", fmt.Errorf("request managed postgres service %s localize switchover to %s: %w", target.ServiceID, targetPrimary, err))
		}
		s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("managed postgres service %s localization switchover to %s requested; waiting for observed primary", target.ServiceID, targetPrimary))
		if _, err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID, *currentDatabase); err != nil {
			return recoverableManagedPostgresTransitionError("primary convergence", fmt.Errorf("wait for managed postgres service %s localize switchover to %s: %w", target.ServiceID, targetPrimary, err))
		}
	}

	finalPostgres := databaseLocalizePostgresSpec(desiredDatabase, targetRuntimeID, targetNodeName, true, false)
	finalApp, err := s.updateAppBackingServicePostgres(target.ServiceID, app, finalPostgres)
	if err != nil {
		return recoverableManagedPostgresTransitionError("finalization", fmt.Errorf("finalize localized managed postgres service %s state: %w", target.ServiceID, err))
	}
	finalBundle, err := s.applyManagedDesiredAppState(ctx, op.ID, finalApp, finalApp.Spec)
	if err != nil {
		return recoverableManagedPostgresTransitionError("finalization", fmt.Errorf("apply finalized managed postgres service %s state: %w", target.ServiceID, err))
	}
	// In-place expansion must take precedence over the normal primary check:
	// a healthy primary says nothing about PVC or filesystem resize completion.
	if inPlaceStorageExpansionRequired {
		if err := s.waitForManagedPostgresStorageExpansion(ctx, client, namespace, clusterName, op.ID, storageTarget); err != nil {
			return recoverableManagedPostgresTransitionError("storage convergence", fmt.Errorf("wait for expanded managed postgres service %s to settle: %w", target.ServiceID, err))
		}
	} else if targetPrimary != "" {
		if _, err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID, *currentDatabase); err != nil {
			return recoverableManagedPostgresTransitionError("final convergence", fmt.Errorf("wait for localized managed postgres service %s to settle: %w", target.ServiceID, err))
		}
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return recoverableManagedPostgresTransitionError("completion", err)
	}

	message := fmt.Sprintf("managed postgres service %s localized to runtime %s", target.ServiceID, targetRuntimeID)
	if targetNodeName != "" {
		message = fmt.Sprintf("managed postgres service %s localized to runtime %s node %s", target.ServiceID, targetRuntimeID, targetNodeName)
	}
	_, err = s.Store.CompleteManagedOperationWithResult(
		op.ID,
		finalBundle.ManifestPath,
		message,
		&finalApp.Spec,
		nil,
	)
	if err != nil {
		return recoverableManagedPostgresTransitionError("completion", fmt.Errorf("complete database localize operation %s: %w", op.ID, err))
	}
	s.Logger.Printf(
		"operation %s completed managed postgres service %s localize from runtime %s to %s node=%s; manifest=%s",
		op.ID,
		target.ServiceID,
		sourceRuntimeID,
		targetRuntimeID,
		targetNodeName,
		finalBundle.ManifestPath,
	)
	return nil
}

type managedPostgresStorageTarget struct {
	StorageClassName string
	StorageSize      string
}

func (target managedPostgresStorageTarget) isZero() bool {
	return strings.TrimSpace(target.StorageClassName) == "" && strings.TrimSpace(target.StorageSize) == ""
}

func desiredPostgresSpec(op model.Operation) *model.AppPostgresSpec {
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		return nil
	}
	return op.DesiredSpec.Postgres
}

func databaseLocalizeDesiredPostgresSpec(current, desired *model.AppPostgresSpec) *model.AppPostgresSpec {
	if current == nil {
		return clonePostgresForDatabaseOperation(desired)
	}
	if desired == nil {
		return clonePostgresForDatabaseOperation(current)
	}
	out := clonePostgresForDatabaseOperation(desired)
	if out == nil {
		return clonePostgresForDatabaseOperation(current)
	}
	if strings.TrimSpace(out.StorageSize) == "" {
		out.StorageSize = strings.TrimSpace(current.StorageSize)
	}
	if strings.TrimSpace(out.StorageClassName) == "" {
		out.StorageClassName = strings.TrimSpace(current.StorageClassName)
	}
	return out
}

func clonePostgresForDatabaseOperation(spec *model.AppPostgresSpec) *model.AppPostgresSpec {
	return model.CloneAppPostgresSpec(spec)
}

func managedPostgresStorageMigrationRequired(current, desired *model.AppPostgresSpec) bool {
	if current == nil || desired == nil {
		return false
	}
	return strings.TrimSpace(current.StorageClassName) != strings.TrimSpace(desired.StorageClassName) ||
		strings.TrimSpace(current.StorageSize) != strings.TrimSpace(desired.StorageSize)
}

func managedPostgresInPlaceStorageExpansionRequired(current, desired *model.AppPostgresSpec, sourceRuntimeID, targetRuntimeID, requestedTargetNodeName string) bool {
	if current == nil || desired == nil {
		return false
	}
	if strings.TrimSpace(sourceRuntimeID) == "" ||
		strings.TrimSpace(targetRuntimeID) == "" ||
		strings.TrimSpace(sourceRuntimeID) != strings.TrimSpace(targetRuntimeID) ||
		strings.TrimSpace(requestedTargetNodeName) != "" {
		return false
	}
	if strings.TrimSpace(current.StorageClassName) != strings.TrimSpace(desired.StorageClassName) {
		return false
	}
	currentSize := strings.TrimSpace(current.StorageSize)
	desiredSize := strings.TrimSpace(desired.StorageSize)
	if currentSize == "" || desiredSize == "" || currentSize == desiredSize {
		return false
	}
	currentQuantity, err := resource.ParseQuantity(currentSize)
	if err != nil {
		return false
	}
	desiredQuantity, err := resource.ParseQuantity(desiredSize)
	if err != nil {
		return false
	}
	return desiredQuantity.Cmp(currentQuantity) > 0
}

func databaseLocalizeStorageTarget(required bool, postgres *model.AppPostgresSpec) managedPostgresStorageTarget {
	if !required || postgres == nil {
		return managedPostgresStorageTarget{}
	}
	return managedPostgresStorageTarget{
		StorageClassName: strings.TrimSpace(postgres.StorageClassName),
		StorageSize:      strings.TrimSpace(postgres.StorageSize),
	}
}

func ensureDatabaseLocalizeStorageMigrationCapacity(postgres, current *model.AppPostgresSpec) {
	if postgres == nil {
		return
	}
	minInstances := 2
	if current != nil && current.Instances > 0 {
		minInstances = current.Instances + 1
	}
	if postgres.Instances < minInstances {
		postgres.Instances = minInstances
	}
	if postgres.SynchronousReplicas >= postgres.Instances {
		postgres.SynchronousReplicas = postgres.Instances - 1
	}
}

func databaseSwitchoverStageSpec(
	base model.AppSpec,
	postgres *model.AppPostgresSpec,
	primaryRuntimeID, failoverTargetRuntimeID string,
) model.AppSpec {
	next := base
	if postgres != nil {
		postgresCopy := databaseSwitchoverStagePostgresSpec(postgres, primaryRuntimeID, failoverTargetRuntimeID)
		next.Postgres = &postgresCopy
	}
	return next
}

// databaseSwitchoverStagePostgresSpec keeps the last durable primary-node pin
// while the replica is prepared. Clearing or changing it before SQL-ready
// cutover would let a partially staged operation alter the active primary.
func databaseSwitchoverStagePostgresSpec(
	postgres *model.AppPostgresSpec,
	primaryRuntimeID, failoverTargetRuntimeID string,
) model.AppPostgresSpec {
	postgresCopy := *model.CloneAppPostgresSpec(postgres)
	postgresCopy.RuntimeID = strings.TrimSpace(primaryRuntimeID)
	postgresCopy.FailoverTargetRuntimeID = strings.TrimSpace(failoverTargetRuntimeID)
	postgresCopy.PrimaryPlacementPendingRebalance = false
	if postgresCopy.Instances < 2 {
		postgresCopy.Instances = 2
	}
	if postgresCopy.SynchronousReplicas < 1 {
		postgresCopy.SynchronousReplicas = 1
	}
	return postgresCopy
}

func databaseSwitchoverFinalPostgresSpec(
	postgres *model.AppPostgresSpec,
	primaryRuntimeID, failoverTargetRuntimeID, primaryNodeName string,
) model.AppPostgresSpec {
	postgresCopy := databaseSwitchoverStagePostgresSpec(postgres, primaryRuntimeID, failoverTargetRuntimeID)
	postgresCopy.PrimaryNodeName = strings.TrimSpace(primaryNodeName)
	return postgresCopy
}

func databaseLocalizeSpec(
	base model.AppSpec,
	postgres *model.AppPostgresSpec,
	targetRuntimeID, targetNodeName string,
	singleInstance, holdPrimaryPlacement bool,
) model.AppSpec {
	next := base
	if postgres == nil {
		return next
	}
	postgresCopy := databaseLocalizePostgresSpec(postgres, targetRuntimeID, targetNodeName, singleInstance, holdPrimaryPlacement)
	next.Postgres = &postgresCopy
	return next
}

func databaseLocalizeStageSpec(
	base model.AppSpec,
	postgres *model.AppPostgresSpec,
	sourceRuntimeID, targetRuntimeID, targetNodeName string,
) model.AppSpec {
	next := base
	if postgres == nil {
		return next
	}
	postgresCopy := databaseLocalizeStagePostgresSpec(postgres, sourceRuntimeID, targetRuntimeID, targetNodeName)
	next.Postgres = &postgresCopy
	return next
}

func databaseLocalizeStagePostgresSpec(
	postgres *model.AppPostgresSpec,
	sourceRuntimeID, targetRuntimeID, targetNodeName string,
) model.AppPostgresSpec {
	sourceRuntimeID = strings.TrimSpace(sourceRuntimeID)
	targetRuntimeID = strings.TrimSpace(targetRuntimeID)
	targetNodeName = strings.TrimSpace(targetNodeName)
	if targetNodeName != "" {
		return databaseLocalizePostgresSpec(postgres, targetRuntimeID, targetNodeName, false, true)
	}
	if sourceRuntimeID != "" && targetRuntimeID != "" && sourceRuntimeID != targetRuntimeID {
		postgresCopy := databaseSwitchoverStagePostgresSpec(postgres, sourceRuntimeID, targetRuntimeID)
		postgresCopy.PrimaryNodeName = ""
		postgresCopy.PrimaryPlacementPendingRebalance = true
		return postgresCopy
	}
	return databaseLocalizePostgresSpec(postgres, targetRuntimeID, targetNodeName, false, true)
}

func databaseLocalizePostgresSpec(
	postgres *model.AppPostgresSpec,
	targetRuntimeID, targetNodeName string,
	singleInstance, holdPrimaryPlacement bool,
) model.AppPostgresSpec {
	postgresCopy := *model.CloneAppPostgresSpec(postgres)
	postgresCopy.RuntimeID = strings.TrimSpace(targetRuntimeID)
	postgresCopy.FailoverTargetRuntimeID = ""
	postgresCopy.PrimaryNodeName = strings.TrimSpace(targetNodeName)
	postgresCopy.SynchronousReplicas = 0
	postgresCopy.PrimaryPlacementPendingRebalance = holdPrimaryPlacement
	if singleInstance {
		postgresCopy.Instances = 1
	} else if postgresCopy.Instances < 2 {
		postgresCopy.Instances = 2
	}
	return postgresCopy
}

func (s *Service) applyManagedDesiredAppState(
	ctx context.Context,
	operationID string,
	baseApp model.App,
	desiredSpec model.AppSpec,
) (runtime.Bundle, error) {
	app := baseApp
	app.Spec = desiredSpec

	if err := s.ensureOperationStillActive(operationID); err != nil {
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
	applyCtx := withManagedAppApplySource(ctx, managedAppApplySourceOperation, operationID)
	if err := s.applyManagedAppDesiredState(applyCtx, app, scheduling); err != nil {
		return runtime.Bundle{}, fmt.Errorf("apply managed app desired state %s: %w", app.ID, err)
	}
	// Database-only operations mutate CloudNativePG placement and service state.
	// The app Deployment may be observed during the apply, but app rollout is not
	// the readiness gate for a database switchover/localize operation. Waiting on
	// it here can deadlock when a transient app ReplicaSet is held behind the same
	// database state this operation is preparing.
	return bundle, nil
}

type storageClassExpansionRestore struct {
	Name     string
	HadValue bool
	Value    bool
}

func (s *Service) prepareManagedPostgresStorageMigrationExpansion(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName string,
	target managedPostgresStorageTarget,
) (func(context.Context) error, error) {
	if target.isZero() || strings.TrimSpace(target.StorageSize) == "" {
		return nil, nil
	}
	targetQuantity, err := resource.ParseQuantity(strings.TrimSpace(target.StorageSize))
	if err != nil {
		return nil, nil
	}
	pvcNames, err := client.listPersistentVolumeClaimNamesByLabel(ctx, namespace, "cnpg.io/cluster="+strings.TrimSpace(clusterName)+",cnpg.io/pvcRole=PG_DATA")
	if err != nil {
		return nil, fmt.Errorf("list postgres PVCs for storage migration %s/%s: %w", namespace, clusterName, err)
	}

	restoresByClass := make(map[string]storageClassExpansionRestore)
	for _, pvcName := range pvcNames {
		pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, pvcName)
		if err != nil {
			return nil, fmt.Errorf("read postgres PVC %s/%s for storage migration: %w", namespace, pvcName, err)
		}
		if !found {
			continue
		}
		storageClassName := strings.TrimSpace(pvc.Spec.StorageClassName)
		if storageClassName == "" || storageClassName == strings.TrimSpace(target.StorageClassName) {
			continue
		}
		currentSize := managedPostgresPVCStorageSize(pvc)
		if currentSize == "" {
			continue
		}
		currentQuantity, err := resource.ParseQuantity(currentSize)
		if err != nil || currentQuantity.Cmp(targetQuantity) >= 0 {
			continue
		}
		storageClass, found, err := client.getStorageClass(ctx, storageClassName)
		if err != nil {
			return nil, fmt.Errorf("read storage class %s for postgres storage migration: %w", storageClassName, err)
		}
		if !found {
			continue
		}
		if storageClass.AllowVolumeExpansion != nil && *storageClass.AllowVolumeExpansion {
			continue
		}
		if _, ok := restoresByClass[storageClassName]; ok {
			continue
		}
		restoresByClass[storageClassName] = storageClassExpansionRestore{
			Name:     storageClassName,
			HadValue: storageClass.AllowVolumeExpansion != nil,
			Value:    storageClass.AllowVolumeExpansion != nil && *storageClass.AllowVolumeExpansion,
		}
		if err := client.patchStorageClassAllowVolumeExpansion(ctx, storageClassName, true); err != nil {
			return nil, fmt.Errorf("temporarily allow expansion for storage class %s during postgres storage migration: %w", storageClassName, err)
		}
		if s != nil && s.Logger != nil {
			s.Logger.Printf(
				"temporarily enabled volume expansion for storage class %s so postgres storage migration %s/%s can create target PVC %s/%s without old PVC resize admission blocking CNPG",
				storageClassName,
				namespace,
				clusterName,
				strings.TrimSpace(target.StorageClassName),
				strings.TrimSpace(target.StorageSize),
			)
		}
	}
	if len(restoresByClass) == 0 {
		return nil, nil
	}
	return func(ctx context.Context) error {
		var restoreErrs []error
		for _, restore := range restoresByClass {
			if restore.HadValue {
				if err := client.patchStorageClassAllowVolumeExpansion(ctx, restore.Name, restore.Value); err != nil {
					restoreErrs = append(restoreErrs, fmt.Errorf("restore storage class %s allowVolumeExpansion=%v: %w", restore.Name, restore.Value, err))
				}
				continue
			}
			if err := client.removeStorageClassAllowVolumeExpansion(ctx, restore.Name); err != nil {
				restoreErrs = append(restoreErrs, fmt.Errorf("remove storage class %s allowVolumeExpansion: %w", restore.Name, err))
			}
		}
		return errors.Join(restoreErrs...)
	}, nil
}

func (s *Service) prepareManagedPostgresInPlaceStorageExpansion(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName string,
	target managedPostgresStorageTarget,
) error {
	return s.prepareManagedPostgresInPlaceStorageExpansionWithPVCRequirement(
		ctx,
		client,
		namespace,
		clusterName,
		target,
		false,
	)
}

func (s *Service) prepareManagedPostgresInPlaceStorageExpansionForExistingCluster(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName string,
	target managedPostgresStorageTarget,
) error {
	return s.prepareManagedPostgresInPlaceStorageExpansionWithPVCRequirement(
		ctx,
		client,
		namespace,
		clusterName,
		target,
		true,
	)
}

func (s *Service) prepareManagedPostgresInPlaceStorageExpansionWithPVCRequirement(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName string,
	target managedPostgresStorageTarget,
	requireExistingDataPVC bool,
) error {
	if target.isZero() || strings.TrimSpace(target.StorageSize) == "" {
		return nil
	}
	targetQuantity, err := resource.ParseQuantity(strings.TrimSpace(target.StorageSize))
	if err != nil {
		return fmt.Errorf("parse postgres in-place expansion target %q: %w", target.StorageSize, err)
	}
	if targetQuantity.Value() <= 0 {
		return fmt.Errorf("postgres in-place expansion target %q must be positive", target.StorageSize)
	}
	pvcNames, err := client.listPersistentVolumeClaimNamesByLabel(ctx, namespace, "cnpg.io/cluster="+strings.TrimSpace(clusterName)+",cnpg.io/pvcRole=PG_DATA")
	if err != nil {
		return fmt.Errorf("list postgres PVCs for in-place expansion %s/%s: %w", namespace, clusterName, err)
	}
	if requireExistingDataPVC && len(pvcNames) == 0 {
		return fmt.Errorf("no postgres data PVCs found for in-place expansion %s/%s", namespace, clusterName)
	}
	pvcsByName := make(map[string]kubePersistentVolumeClaim, len(pvcNames))
	for _, pvcName := range pvcNames {
		pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, pvcName)
		if err != nil {
			return fmt.Errorf("read postgres PVC %s/%s for in-place expansion: %w", namespace, pvcName, err)
		}
		if !found {
			return fmt.Errorf("postgres PVC %s/%s disappeared during in-place expansion preflight", namespace, pvcName)
		}
		pvcsByName[pvcName] = pvc
	}

	plans := make([]managedPostgresPVCExpansionPlan, 0, len(pvcNames))
	for _, pvcName := range pvcNames {
		pvc := pvcsByName[pvcName]
		if resizeErr := managedPostgresPVCResizeError(pvc); resizeErr != "" {
			return fmt.Errorf("postgres PVC %s/%s reports resize error: %s", namespace, pvcName, resizeErr)
		}
		requestedSize := strings.TrimSpace(pvc.Spec.Resources.Requests["storage"])
		if requestedSize == "" {
			requestedSize = managedPostgresPVCStorageSize(pvc)
		}
		requestedQuantity, requestedErr := resource.ParseQuantity(requestedSize)
		capacitySize := strings.TrimSpace(pvc.Status.Capacity["storage"])
		capacityQuantity, capacityErr := resource.ParseQuantity(capacitySize)
		requestConverged := requestedErr == nil && requestedQuantity.Cmp(targetQuantity) >= 0
		capacityConverged := capacityErr == nil && capacityQuantity.Cmp(targetQuantity) >= 0
		if requestConverged && capacityConverged {
			continue
		}
		plans = append(plans, managedPostgresPVCExpansionPlan{
			Name:               strings.TrimSpace(pvcName),
			PVC:                pvc,
			RequestConverged:   requestConverged,
			CapacitySize:       capacitySize,
			CapacityQuantity:   capacityQuantity,
			CapacityParseError: capacityErr,
		})
	}
	// A previously adopted PVC may use a non-expandable or legacy storage
	// class. Once its requested and actual capacities already satisfy the
	// target, there is no storage operation to prepare or storage class to
	// validate.
	if len(pvcNames) > 0 && len(plans) == 0 {
		return nil
	}

	storageClassName := strings.TrimSpace(target.StorageClassName)
	if storageClassName == "" && len(pvcNames) > 0 {
		storageClassName = strings.TrimSpace(pvcsByName[pvcNames[0]].Spec.StorageClassName)
	}
	if storageClassName == "" {
		// A newly created cluster may intentionally rely on Kubernetes' default
		// StorageClass. There is no existing volume to expand or preflight yet.
		if len(pvcNames) == 0 {
			return nil
		}
		return fmt.Errorf(
			"postgres PVC %s/%s has no storage class for in-place expansion",
			namespace,
			pvcNames[0],
		)
	}
	for _, pvcName := range pvcNames {
		if actualStorageClass := strings.TrimSpace(pvcsByName[pvcName].Spec.StorageClassName); actualStorageClass != storageClassName {
			return fmt.Errorf(
				"postgres PVC %s/%s uses storage class %q, expected %q for in-place expansion",
				namespace,
				pvcName,
				actualStorageClass,
				storageClassName,
			)
		}
	}

	storageClass, found, err := client.getStorageClass(ctx, storageClassName)
	if err != nil {
		return fmt.Errorf("read storage class %s for postgres in-place expansion: %w", storageClassName, err)
	}
	if !found {
		return fmt.Errorf("storage class %s was not found for postgres in-place expansion", storageClassName)
	}
	if storageClass.AllowVolumeExpansion == nil || !*storageClass.AllowVolumeExpansion {
		return fmt.Errorf("storage class %s does not allow postgres in-place volume expansion", storageClassName)
	}

	if strings.EqualFold(strings.TrimSpace(storageClass.Provisioner), openEBSLocalLVMProvisioner) {
		if err := s.validateManagedPostgresLocalPVExpansionCapacity(ctx, client, namespace, storageClass, targetQuantity, plans); err != nil {
			return err
		}
	}
	for _, plan := range plans {
		if plan.RequestConverged {
			continue
		}
		if err := client.patchPersistentVolumeClaimStorageRequest(ctx, namespace, plan.Name, strings.TrimSpace(target.StorageSize)); err != nil {
			return fmt.Errorf("expand postgres PVC %s/%s request to %s: %w", namespace, plan.Name, strings.TrimSpace(target.StorageSize), err)
		}
		if s != nil && s.Logger != nil {
			s.Logger.Printf("expanded postgres PVC %s/%s request to %s for in-place storage growth", namespace, plan.Name, strings.TrimSpace(target.StorageSize))
		}
	}
	return nil
}

type managedPostgresPVCExpansionPlan struct {
	Name               string
	PVC                kubePersistentVolumeClaim
	RequestConverged   bool
	CapacitySize       string
	CapacityQuantity   resource.Quantity
	CapacityParseError error
}

type managedPostgresLocalPVCapacityRequirement struct {
	NodeName       string
	VGName         string
	ExpansionBytes int64
	PVCNames       []string
}

func (s *Service) validateManagedPostgresLocalPVExpansionCapacity(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	storageClass kubeStorageClass,
	targetQuantity resource.Quantity,
	plans []managedPostgresPVCExpansionPlan,
) error {
	if len(plans) == 0 {
		return nil
	}
	requirements := map[string]*managedPostgresLocalPVCapacityRequirement{}
	for _, plan := range plans {
		if plan.RequestConverged {
			// This invocation will not submit another expansion request. The
			// convergence wait below still verifies PVC and filesystem state.
			continue
		}
		if plan.CapacityParseError != nil || strings.TrimSpace(plan.CapacitySize) == "" {
			return fmt.Errorf(
				"postgres PVC %s/%s has no parseable actual capacity before LocalPV expansion",
				namespace,
				plan.Name,
			)
		}
		if targetQuantity.Cmp(plan.CapacityQuantity) <= 0 {
			continue
		}
		volumeName := strings.TrimSpace(plan.PVC.Spec.VolumeName)
		if volumeName == "" {
			return fmt.Errorf("postgres PVC %s/%s is not bound; cannot verify LocalPV expansion capacity", namespace, plan.Name)
		}
		pv, found, err := client.getPersistentVolume(ctx, volumeName)
		if err != nil {
			return fmt.Errorf("read postgres PV %s for LocalPV expansion capacity: %w", volumeName, err)
		}
		if !found {
			return fmt.Errorf("postgres PV %s was not found for LocalPV expansion capacity", volumeName)
		}
		nodeName := persistentVolumeNodeName(pv)
		if nodeName == "" {
			return fmt.Errorf("postgres PV %s has no unambiguous LocalPV node affinity", volumeName)
		}
		vgName := managedPostgresLocalPVVolumeGroup(storageClass, pv)
		if vgName == "" {
			return fmt.Errorf("postgres PV %s has no LocalPV volume group identity", volumeName)
		}
		expansionBytes := targetQuantity.Value() - plan.CapacityQuantity.Value()
		if expansionBytes <= 0 {
			continue
		}
		key := nodeName + "\x00" + vgName
		requirement := requirements[key]
		if requirement == nil {
			requirement = &managedPostgresLocalPVCapacityRequirement{
				NodeName: nodeName,
				VGName:   vgName,
			}
			requirements[key] = requirement
		}
		if requirement.ExpansionBytes > math.MaxInt64-expansionBytes {
			return fmt.Errorf("LocalPV expansion byte requirement overflow for node %s volume group %s", nodeName, vgName)
		}
		requirement.ExpansionBytes += expansionBytes
		requirement.PVCNames = append(requirement.PVCNames, plan.Name)
	}
	if len(requirements) == 0 {
		return nil
	}
	if s == nil || s.Store == nil {
		return fmt.Errorf("cannot verify LocalPV capacity without the Fugue state store")
	}
	inventories, err := s.Store.ListLocalPVInventories(model.LocalPVInventoryFilter{})
	if err != nil {
		return fmt.Errorf("list LocalPV inventory before postgres expansion: %w", err)
	}
	now := time.Now().UTC()
	for _, requirement := range requirements {
		inventory, found := newestLocalPVInventoryForNodeAndVG(inventories, requirement.NodeName, requirement.VGName)
		if !found {
			return fmt.Errorf(
				"no LocalPV inventory exists for node %s volume group %s; refusing postgres expansion for PVCs %s",
				requirement.NodeName,
				requirement.VGName,
				strings.Join(requirement.PVCNames, ","),
			)
		}
		if !localpvsafety.IsFresh(inventory.ObservedAt, now, localpvsafety.DefaultInventoryTTL) {
			return fmt.Errorf(
				"LocalPV inventory for node %s volume group %s is stale (observed_at=%s ttl=%s); refusing postgres expansion",
				requirement.NodeName,
				requirement.VGName,
				inventory.ObservedAt.UTC().Format(time.RFC3339),
				localpvsafety.DefaultInventoryTTL,
			)
		}
		requiredReserve := localpvsafety.RequiredFreeBytes(inventory.PVSizeBytes)
		if requirement.ExpansionBytes > math.MaxInt64-requiredReserve {
			return fmt.Errorf(
				"LocalPV expansion capacity requirement overflow on node %s volume group %s",
				requirement.NodeName,
				requirement.VGName,
			)
		}
		requiredTotal := requirement.ExpansionBytes + requiredReserve
		if inventory.PVSizeBytes <= 0 ||
			inventory.PVFreeBytes < 0 ||
			inventory.PVFreeBytes > inventory.PVSizeBytes ||
			inventory.PVFreeBytes < requiredTotal {
			return fmt.Errorf(
				"insufficient LocalPV capacity on node %s volume group %s for postgres expansion: free_bytes=%d expansion_bytes=%d required_reserve_bytes=%d pv_size_bytes=%d PVCs=%s",
				requirement.NodeName,
				requirement.VGName,
				inventory.PVFreeBytes,
				requirement.ExpansionBytes,
				requiredReserve,
				inventory.PVSizeBytes,
				strings.Join(requirement.PVCNames, ","),
			)
		}
	}
	return nil
}

func managedPostgresLocalPVVolumeGroup(storageClass kubeStorageClass, pv kubePersistentVolume) string {
	for _, key := range []string{"volgroup", "vgname", "openebs.io/volgroup"} {
		if value := strings.TrimSpace(storageClass.Parameters[key]); value != "" {
			return value
		}
	}
	if pv.Spec.CSI != nil {
		for _, key := range []string{"openebs.io/volgroup", "volgroup", "vgname"} {
			if value := strings.TrimSpace(pv.Spec.CSI.VolumeAttributes[key]); value != "" {
				return value
			}
		}
	}
	return ""
}

func newestLocalPVInventoryForNodeAndVG(
	inventories []model.LocalPVInventory,
	nodeName, vgName string,
) (model.LocalPVInventory, bool) {
	nodeName = strings.TrimSpace(nodeName)
	vgName = strings.TrimSpace(vgName)
	var newest model.LocalPVInventory
	found := false
	for _, inventory := range inventories {
		nodeMatches := strings.TrimSpace(inventory.ClusterNodeName) == nodeName ||
			strings.TrimSpace(inventory.NodeID) == nodeName
		if !nodeMatches || strings.TrimSpace(inventory.VGName) != vgName {
			continue
		}
		if !found || inventory.ObservedAt.After(newest.ObservedAt) {
			newest = inventory
			found = true
		}
	}
	return newest, found
}

func (s *Service) resolveDatabaseLocalizeTargetNode(
	ctx context.Context,
	client *kubeClient,
	app model.App,
	targetRuntimeID, requestedNodeName string,
) (string, error) {
	targetRuntime, err := s.Store.GetRuntime(targetRuntimeID)
	if err != nil {
		return "", fmt.Errorf("load database localize target runtime %s: %w", targetRuntimeID, err)
	}
	if targetRuntime.Type != model.RuntimeTypeManagedShared {
		return strings.TrimSpace(requestedNodeName), nil
	}

	sourceSelector := runtime.ManagedSharedNodeSelector(targetRuntime)
	if nodeName := strings.TrimSpace(requestedNodeName); nodeName != "" {
		matchedNode, found, err := managedSharedNodeMatchingSelector(ctx, client, nodeName, sourceSelector)
		if err != nil {
			return "", err
		}
		if !found {
			return "", fmt.Errorf("database localize target node %s does not match runtime %s", nodeName, targetRuntimeID)
		}
		return matchedNode, nil
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	pods, err := client.listPodsBySelector(ctx, namespace, managedAppPodLabelSelector(app))
	if err != nil {
		return "", fmt.Errorf("list app pods for database localize: %w", err)
	}
	nodes := make(map[string]struct{})
	for _, pod := range pods {
		nodeName := strings.TrimSpace(pod.Spec.NodeName)
		if nodeName == "" || strings.TrimSpace(pod.Metadata.DeletionTimestamp) != "" {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Running") || !kubePodReady(pod) {
			continue
		}
		matchedNode, found, err := managedSharedNodeMatchingSelector(ctx, client, nodeName, sourceSelector)
		if err != nil {
			return "", err
		}
		if found {
			nodes[matchedNode] = struct{}{}
		}
	}
	if len(nodes) == 0 {
		return "", fmt.Errorf("database localize could not find a ready app pod on target runtime %s; pass target_node_name explicitly after confirming placement", targetRuntimeID)
	}
	if len(nodes) > 1 {
		return "", fmt.Errorf("database localize found ready app pods on multiple nodes for runtime %s; pass target_node_name explicitly", targetRuntimeID)
	}
	for nodeName := range nodes {
		return nodeName, nil
	}
	return "", fmt.Errorf("database localize could not resolve target node")
}

func kubePodReady(pod kubePod) bool {
	for _, condition := range pod.Status.Conditions {
		if !strings.EqualFold(strings.TrimSpace(condition.Type), "Ready") {
			continue
		}
		return strings.EqualFold(strings.TrimSpace(condition.Status), "True")
	}
	return true
}

func (s *Service) waitForManagedPostgresReplicaOnRuntime(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetRuntimeID, operationID string,
	storageTargets ...managedPostgresStorageTarget,
) (string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, s.Config.ManagedAppRolloutTimeout)
	defer cancel()

	interval := 2 * time.Second
	if s.Config.PollInterval > interval {
		interval = s.Config.PollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastMessage := ""
	for {
		if strings.TrimSpace(operationID) != "" {
			if err := s.ensureOperationStillActive(operationID); err != nil {
				return "", err
			}
		}

		cluster, found, err := client.getCloudNativePGCluster(waitCtx, namespace, clusterName)
		if err != nil {
			return "", fmt.Errorf("read cloudnativepg cluster %s/%s: %w", namespace, clusterName, err)
		}
		if !found {
			lastMessage = fmt.Sprintf("waiting for cluster %s to be created", clusterName)
		} else if !managedBackingServiceClusterReady(cluster, found) {
			lastMessage = fmt.Sprintf(
				"waiting for cluster %s to become ready (%d/%d instances)",
				clusterName,
				cluster.Status.ReadyInstances,
				max(cluster.Spec.Instances, 1),
			)
		} else {
			targetPrimary, err := s.selectManagedPostgresSwitchoverTarget(waitCtx, client, namespace, clusterName, targetRuntimeID, cluster.Status.CurrentPrimary, firstStorageTarget(storageTargets))
			if err != nil {
				return "", err
			}
			if targetPrimary != "" {
				return targetPrimary, nil
			}
			lastMessage = fmt.Sprintf("waiting for a standby on runtime %s for cluster %s", targetRuntimeID, clusterName)
		}

		select {
		case <-waitCtx.Done():
			if lastMessage != "" {
				return "", fmt.Errorf("%w (%s)", waitCtx.Err(), lastMessage)
			}
			return "", waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) waitForManagedPostgresReplicaOnNode(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetNodeName, operationID string,
	storageTargets ...managedPostgresStorageTarget,
) (string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, s.Config.ManagedAppRolloutTimeout)
	defer cancel()

	interval := 2 * time.Second
	if s.Config.PollInterval > interval {
		interval = s.Config.PollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastMessage := ""
	for {
		if strings.TrimSpace(operationID) != "" {
			if err := s.ensureOperationStillActive(operationID); err != nil {
				return "", err
			}
		}

		cluster, found, err := client.getCloudNativePGCluster(waitCtx, namespace, clusterName)
		if err != nil {
			return "", fmt.Errorf("read cloudnativepg cluster %s/%s: %w", namespace, clusterName, err)
		}
		if !found {
			lastMessage = fmt.Sprintf("waiting for cluster %s to be created", clusterName)
		} else {
			boundPod, err := s.bindManagedPostgresPendingReplicaOnNode(waitCtx, client, namespace, clusterName, targetNodeName, cluster.Status.CurrentPrimary, firstStorageTarget(storageTargets))
			if err != nil {
				return "", err
			}
			if boundPod != "" {
				lastMessage = fmt.Sprintf("bound pending standby %s to node %s for cluster %s", boundPod, targetNodeName, clusterName)
			}
			if !managedBackingServiceClusterReady(cluster, found) {
				lastMessage = fmt.Sprintf(
					"waiting for cluster %s to become ready (%d/%d instances)",
					clusterName,
					cluster.Status.ReadyInstances,
					max(cluster.Spec.Instances, 1),
				)
			} else {
				targetPrimary, err := s.selectManagedPostgresSwitchoverTargetOnNode(waitCtx, client, namespace, clusterName, targetNodeName, cluster.Status.CurrentPrimary, firstStorageTarget(storageTargets))
				if err != nil {
					return "", err
				}
				if targetPrimary != "" {
					return targetPrimary, nil
				}
				lastMessage = fmt.Sprintf("waiting for a standby on node %s for cluster %s", targetNodeName, clusterName)
			}
		}

		select {
		case <-waitCtx.Done():
			if lastMessage != "" {
				return "", fmt.Errorf("%w (%s)", waitCtx.Err(), lastMessage)
			}
			return "", waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) waitForManagedPostgresReplicationCatchup(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetPodName, operationID string,
	postgres model.AppPostgresSpec,
) error {
	if strings.TrimSpace(postgres.Database) == "" || strings.TrimSpace(postgres.User) == "" || postgres.Password == "" {
		return fmt.Errorf("managed postgres credentials are incomplete")
	}
	waitCtx, cancel := context.WithTimeout(ctx, s.Config.ManagedAppRolloutTimeout)
	defer cancel()

	cluster, found, err := client.getCloudNativePGCluster(waitCtx, namespace, clusterName)
	if err != nil {
		return fmt.Errorf("read cloudnativepg cluster %s/%s before replication gate: %w", namespace, clusterName, err)
	}
	if !found {
		return fmt.Errorf("cloudnativepg cluster %s/%s disappeared before replication gate", namespace, clusterName)
	}
	primaryPodName := strings.TrimSpace(cluster.Status.CurrentPrimary)
	targetPodName = strings.TrimSpace(targetPodName)
	if primaryPodName == "" || targetPodName == "" || primaryPodName == targetPodName {
		return fmt.Errorf("replication gate requires distinct current primary and standby pods")
	}
	primaryIP, found, err := client.getPodIP(waitCtx, namespace, primaryPodName)
	if err != nil {
		return fmt.Errorf("read managed postgres primary pod %s/%s IP: %w", namespace, primaryPodName, err)
	}
	if !found || primaryIP == "" {
		return fmt.Errorf("managed postgres primary pod %s/%s has no reachable IP", namespace, primaryPodName)
	}
	targetIP, found, err := client.getPodIP(waitCtx, namespace, targetPodName)
	if err != nil {
		return fmt.Errorf("read managed postgres standby pod %s/%s IP: %w", namespace, targetPodName, err)
	}
	if !found || targetIP == "" {
		return fmt.Errorf("managed postgres standby pod %s/%s has no reachable IP", namespace, targetPodName)
	}

	primaryConn, err := pgx.Connect(waitCtx, managedPostgresPodDatabaseURL(primaryIP, postgres))
	if err != nil {
		return fmt.Errorf("connect to managed postgres primary pod %s/%s for replication gate: %w", namespace, primaryPodName, err)
	}
	defer closeManagedPostgresReplicationConnection(primaryConn)
	standbyConn, err := pgx.Connect(waitCtx, managedPostgresPodDatabaseURL(targetIP, postgres))
	if err != nil {
		return fmt.Errorf("connect to managed postgres standby pod %s/%s for replication gate: %w", namespace, targetPodName, err)
	}
	defer closeManagedPostgresReplicationConnection(standbyConn)

	interval := time.Second
	if s.Config.PollInterval > interval {
		interval = s.Config.PollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	s.updateManagedPostgresTransitionProgress(operationID, fmt.Sprintf("standby %s is ready; verifying streaming replication catch-up", targetPodName))

	stableSamples := 0
	lastDetail := "waiting for the standby replay LSN"
	for {
		if strings.TrimSpace(operationID) != "" {
			if err := s.ensureOperationStillActive(operationID); err != nil {
				return err
			}
		}

		var primaryLSN string
		if err := primaryConn.QueryRow(waitCtx, `SELECT pg_current_wal_flush_lsn()::text`).Scan(&primaryLSN); err != nil {
			return fmt.Errorf("read managed postgres primary flush LSN from %s: %w", primaryPodName, err)
		}
		var standbyInRecovery bool
		var standbyReplayLSN string
		if err := standbyConn.QueryRow(waitCtx, `
SELECT pg_is_in_recovery(), COALESCE(pg_last_wal_replay_lsn()::text, '')
`).Scan(&standbyInRecovery, &standbyReplayLSN); err != nil {
			return fmt.Errorf("read managed postgres standby replay LSN from %s: %w", targetPodName, err)
		}
		converged, err := managedPostgresReplicationLSNConverged(primaryLSN, standbyReplayLSN, standbyInRecovery)
		if err != nil {
			return err
		}
		if converged {
			stableSamples++
			lastDetail = fmt.Sprintf(
				"standby %s replayed primary LSN %s (%d/%d stable samples)",
				targetPodName,
				primaryLSN,
				stableSamples,
				managedPostgresReplicationStableSamples,
			)
			if stableSamples >= managedPostgresReplicationStableSamples {
				s.updateManagedPostgresTransitionProgress(operationID, fmt.Sprintf("standby %s streaming replication is caught up; switchover may proceed", targetPodName))
				return nil
			}
		} else {
			stableSamples = 0
			lastDetail = fmt.Sprintf("waiting for standby %s replay LSN %s to reach primary flush LSN %s", targetPodName, standbyReplayLSN, primaryLSN)
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("%w (%s)", waitCtx.Err(), lastDetail)
		case <-ticker.C:
		}
	}
}

func managedPostgresPodDatabaseURL(podIP string, postgres model.AppPostgresSpec) string {
	return managedPostgresDatabaseURL(podIP, postgres, "fugue-controller-replication-gate")
}

func managedPostgresServiceDatabaseURL(serviceHost string, postgres model.AppPostgresSpec) string {
	return managedPostgresDatabaseURL(serviceHost, postgres, "fugue-controller-primary-readiness")
}

func managedPostgresDatabaseURL(host string, postgres model.AppPostgresSpec, applicationName string) string {
	databaseURL := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(strings.TrimSpace(postgres.User), postgres.Password),
		Host:   net.JoinHostPort(strings.TrimSpace(host), "5432"),
		Path:   "/" + strings.TrimSpace(postgres.Database),
	}
	query := databaseURL.Query()
	query.Set("application_name", strings.TrimSpace(applicationName))
	query.Set("connect_timeout", "5")
	query.Set("sslmode", "disable")
	databaseURL.RawQuery = query.Encode()
	return databaseURL.String()
}

func closeManagedPostgresReplicationConnection(conn *pgx.Conn) {
	if conn == nil {
		return
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.Close(closeCtx)
}

func managedPostgresReplicationLSNConverged(primaryLSN, standbyReplayLSN string, standbyInRecovery bool) (bool, error) {
	if !standbyInRecovery {
		return false, fmt.Errorf("managed postgres switchover target is not a streaming standby")
	}
	if strings.TrimSpace(standbyReplayLSN) == "" {
		return false, nil
	}
	primary, err := parsePostgresLSN(primaryLSN)
	if err != nil {
		return false, fmt.Errorf("parse managed postgres primary LSN %q: %w", primaryLSN, err)
	}
	standby, err := parsePostgresLSN(standbyReplayLSN)
	if err != nil {
		return false, fmt.Errorf("parse managed postgres standby replay LSN %q: %w", standbyReplayLSN, err)
	}
	return standby >= primary, nil
}

func parsePostgresLSN(value string) (uint64, error) {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, fmt.Errorf("invalid PostgreSQL LSN")
	}
	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("parse high word: %w", err)
	}
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("parse low word: %w", err)
	}
	return high<<32 | low, nil
}

type managedPostgresPrimarySQLProbeFunc func(context.Context, string, string, model.AppPostgresSpec) error

type managedPostgresPrimarySQLConnection interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Close(context.Context) error
}

type managedPostgresPrimarySQLConnectFunc func(context.Context, string) (managedPostgresPrimarySQLConnection, error)

func (s *Service) waitForManagedPostgresPrimary(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetPrimary, operationID string,
	postgres model.AppPostgresSpec,
) (managedPostgresPrimaryPlacement, error) {
	waitCtx, cancel := context.WithTimeout(ctx, s.Config.ManagedAppRolloutTimeout)
	defer cancel()

	interval := 2 * time.Second
	if s.Config.PollInterval > interval {
		interval = s.Config.PollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastMessage := ""
	for {
		if strings.TrimSpace(operationID) != "" {
			if err := s.ensureOperationStillActive(operationID); err != nil {
				return managedPostgresPrimaryPlacement{}, err
			}
		}

		placement, ready, detail, err := s.observeManagedPostgresPrimaryReadiness(
			waitCtx,
			client,
			namespace,
			clusterName,
			targetPrimary,
			postgres,
		)
		if err != nil {
			return managedPostgresPrimaryPlacement{}, err
		}
		if ready {
			s.updateManagedPostgresTransitionProgress(operationID, fmt.Sprintf(
				"managed postgres primary %s is serving read-write SQL through %s-rw",
				strings.TrimSpace(targetPrimary),
				strings.TrimSpace(clusterName),
			))
			return placement, nil
		}
		lastMessage = detail

		select {
		case <-waitCtx.Done():
			if lastMessage != "" {
				return managedPostgresPrimaryPlacement{}, fmt.Errorf("%w (%s)", waitCtx.Err(), lastMessage)
			}
			return managedPostgresPrimaryPlacement{}, waitCtx.Err()
		case <-ticker.C:
		}
	}
}

// observeManagedPostgresPrimaryReadiness is deliberately fail-closed. CNPG's
// readyInstances/CurrentPrimary fields describe controller intent, not proof
// that the new primary Pod is serving traffic. A migration may complete only
// after the Pod, the -rw service endpoint, and a read-write SQL connection all
// agree on the same primary.
func (s *Service) observeManagedPostgresPrimaryReadiness(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetPrimary string,
	postgres model.AppPostgresSpec,
) (managedPostgresPrimaryPlacement, bool, string, error) {
	namespace = strings.TrimSpace(namespace)
	clusterName = strings.TrimSpace(clusterName)
	targetPrimary = strings.TrimSpace(targetPrimary)
	if clusterName == "" || targetPrimary == "" {
		return managedPostgresPrimaryPlacement{}, false, "managed postgres readiness requires cluster and target primary", nil
	}

	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, clusterName)
	if err != nil {
		return managedPostgresPrimaryPlacement{}, false, "", fmt.Errorf("read cloudnativepg cluster %s/%s: %w", namespace, clusterName, err)
	}
	if !found {
		return managedPostgresPrimaryPlacement{}, false, fmt.Sprintf("waiting for cluster %s to exist", clusterName), nil
	}
	if !managedBackingServiceClusterReady(cluster, true) {
		return managedPostgresPrimaryPlacement{}, false, fmt.Sprintf(
			"waiting for cluster %s readiness (%d/%d instances, current=%s target=%s)",
			clusterName,
			cluster.Status.ReadyInstances,
			max(cluster.Spec.Instances, 1),
			strings.TrimSpace(cluster.Status.CurrentPrimary),
			strings.TrimSpace(cluster.Status.TargetPrimary),
		), nil
	}
	if strings.TrimSpace(cluster.Status.CurrentPrimary) != targetPrimary {
		return managedPostgresPrimaryPlacement{}, false, fmt.Sprintf(
			"waiting for cluster %s primary to switch to %s (current=%s target=%s)",
			clusterName,
			targetPrimary,
			strings.TrimSpace(cluster.Status.CurrentPrimary),
			strings.TrimSpace(cluster.Status.TargetPrimary),
		), nil
	}

	pod, found, err := client.getPod(ctx, namespace, targetPrimary)
	if err != nil {
		return managedPostgresPrimaryPlacement{}, false, "", fmt.Errorf("read managed postgres primary pod %s/%s: %w", namespace, targetPrimary, err)
	}
	if !found {
		return managedPostgresPrimaryPlacement{}, false, fmt.Sprintf("waiting for managed postgres primary pod %s/%s to exist", namespace, targetPrimary), nil
	}
	if pod.Metadata.DeletionTimestamp != "" || strings.TrimSpace(pod.Status.Phase) != "Running" {
		return managedPostgresPrimaryPlacement{}, false, fmt.Sprintf(
			"waiting for managed postgres primary pod %s/%s to be Running (phase=%s deleting=%t)",
			namespace,
			targetPrimary,
			strings.TrimSpace(pod.Status.Phase),
			pod.Metadata.DeletionTimestamp != "",
		), nil
	}
	if !managedPostgresPrimaryPodReady(pod) {
		return managedPostgresPrimaryPlacement{}, false, fmt.Sprintf("waiting for managed postgres primary pod %s/%s Ready condition", namespace, targetPrimary), nil
	}
	nodeName := strings.TrimSpace(pod.Spec.NodeName)
	if nodeName == "" {
		return managedPostgresPrimaryPlacement{}, false, fmt.Sprintf("waiting for managed postgres primary pod %s/%s node assignment", namespace, targetPrimary), nil
	}
	runtimeID, err := s.runtimeIDForNode(ctx, client, nodeName)
	if err != nil {
		return managedPostgresPrimaryPlacement{}, false, "", fmt.Errorf("resolve managed postgres primary pod %s/%s runtime from node %s: %w", namespace, targetPrimary, nodeName, err)
	}
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return managedPostgresPrimaryPlacement{}, false, fmt.Sprintf("waiting to resolve managed postgres primary node %s runtime", nodeName), nil
	}
	podIP, found, err := client.getPodIP(ctx, namespace, targetPrimary)
	if err != nil {
		return managedPostgresPrimaryPlacement{}, false, "", fmt.Errorf("read managed postgres primary pod %s/%s IP: %w", namespace, targetPrimary, err)
	}
	podIP = strings.TrimSpace(podIP)
	if !found {
		return managedPostgresPrimaryPlacement{}, false, fmt.Sprintf("waiting for managed postgres primary pod %s/%s IP", namespace, targetPrimary), nil
	}
	if podIP == "" {
		return managedPostgresPrimaryPlacement{}, false, fmt.Sprintf("waiting for managed postgres primary pod %s/%s to receive an IP", namespace, targetPrimary), nil
	}

	serviceName := model.PostgresRWServiceName(clusterName)
	endpointReady, endpointDetail, err := managedPostgresRWEndpointContainsPod(ctx, client, namespace, serviceName, podIP)
	if err != nil {
		return managedPostgresPrimaryPlacement{}, false, "", err
	}
	if !endpointReady {
		return managedPostgresPrimaryPlacement{}, false, endpointDetail, nil
	}

	serviceHost := managedPostgresRWServiceHost(namespace, serviceName)
	if err := s.probeManagedPostgresPrimarySQL(ctx, serviceHost, podIP, postgres); err != nil {
		return managedPostgresPrimaryPlacement{}, false, fmt.Sprintf("waiting for managed postgres read-write SQL through %s: %v", serviceHost, err), nil
	}
	return managedPostgresPrimaryPlacement{
		RuntimeID: runtimeID, NodeName: nodeName, PrimaryPod: targetPrimary, PodIP: podIP,
	}, true, "", nil
}

func managedPostgresPrimaryPodReady(pod kubePod) bool {
	for _, condition := range pod.Status.Conditions {
		if strings.TrimSpace(condition.Type) == "Ready" {
			return strings.EqualFold(strings.TrimSpace(condition.Status), "True")
		}
	}
	return false
}

func managedPostgresRWEndpointContainsPod(
	ctx context.Context,
	client *kubeClient,
	namespace, serviceName, podIP string,
) (bool, string, error) {
	serviceName = strings.TrimSpace(serviceName)
	podIP = strings.TrimSpace(podIP)
	if serviceName == "" || podIP == "" {
		return false, "managed postgres -rw endpoint evidence is incomplete", nil
	}

	endpoints, found, err := client.getEndpointsForService(ctx, namespace, serviceName)
	if err != nil {
		return false, "", fmt.Errorf("read managed postgres -rw endpoints %s/%s: %w", namespace, serviceName, err)
	}
	if found && managedPostgresEndpointsContainIP(endpoints, podIP) {
		return true, "", nil
	}

	// EndpointSlice is the newer API, and is useful on clusters where the
	// legacy Endpoints object is intentionally not populated. The client
	// treats RBAC/404 as unavailable; in that case the fail-closed result below
	// prevents a migration from completing without endpoint evidence.
	slices, err := client.listEndpointSlicesForService(ctx, namespace, serviceName)
	if err != nil {
		return false, "", fmt.Errorf("read managed postgres -rw endpoint slices %s/%s: %w", namespace, serviceName, err)
	}
	if managedPostgresEndpointSlicesContainIP(slices, podIP) {
		return true, "", nil
	}
	return false, fmt.Sprintf(
		"waiting for managed postgres -rw service %s to publish ready endpoint for primary IP %s",
		serviceName,
		podIP,
	), nil
}

func managedPostgresEndpointsContainIP(endpoints kubeEndpoints, podIP string) bool {
	for _, subset := range endpoints.Subsets {
		for _, address := range subset.Addresses {
			if sameIP(address.IP, podIP) {
				return true
			}
		}
	}
	return false
}

func managedPostgresEndpointSlicesContainIP(slices []kubeEndpointSlice, podIP string) bool {
	for _, slice := range slices {
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
				continue
			}
			if endpoint.Conditions.Serving != nil && !*endpoint.Conditions.Serving {
				continue
			}
			if endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating {
				continue
			}
			for _, address := range endpoint.Addresses {
				if sameIP(address, podIP) {
					return true
				}
			}
		}
	}
	return false
}

func sameIP(left, right string) bool {
	leftIP := net.ParseIP(strings.TrimSpace(left))
	rightIP := net.ParseIP(strings.TrimSpace(right))
	if leftIP != nil && rightIP != nil {
		return leftIP.Equal(rightIP)
	}
	return strings.TrimSpace(left) == strings.TrimSpace(right)
}

func managedPostgresRWServiceHost(namespace, serviceName string) string {
	return fmt.Sprintf("%s.%s.svc", strings.TrimSpace(serviceName), strings.TrimSpace(namespace))
}

func (s *Service) probeManagedPostgresPrimarySQL(ctx context.Context, serviceHost, expectedPodIP string, postgres model.AppPostgresSpec) error {
	if s.managedPostgresPrimarySQLProbe != nil {
		return s.managedPostgresPrimarySQLProbe(ctx, serviceHost, expectedPodIP, postgres)
	}
	if strings.TrimSpace(postgres.Database) == "" || strings.TrimSpace(postgres.User) == "" || postgres.Password == "" {
		return fmt.Errorf("managed postgres credentials are incomplete")
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	connect := s.postgresPrimarySQLConnect
	if connect == nil {
		connect = func(ctx context.Context, databaseURL string) (managedPostgresPrimarySQLConnection, error) {
			return pgx.Connect(ctx, databaseURL)
		}
	}
	conn, err := connect(probeCtx, managedPostgresServiceDatabaseURL(serviceHost, postgres))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer closeManagedPostgresPrimarySQLConnection(conn)

	var inRecovery bool
	var transactionReadOnly string
	var serverAddress string
	if err := conn.QueryRow(probeCtx, managedPostgresPrimaryReadinessQuery).Scan(&inRecovery, &transactionReadOnly, &serverAddress); err != nil {
		return fmt.Errorf("query readiness: %w", err)
	}
	return validateManagedPostgresPrimarySQL(serverAddress, expectedPodIP, transactionReadOnly, inRecovery)
}

func closeManagedPostgresPrimarySQLConnection(conn managedPostgresPrimarySQLConnection) {
	if conn == nil {
		return
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.Close(closeCtx)
}

func validateManagedPostgresPrimarySQL(serverAddress, expectedPodIP, transactionReadOnly string, inRecovery bool) error {
	if !managedPostgresServerAddressMatchesPodIP(serverAddress, expectedPodIP) {
		return fmt.Errorf("service connected to %s instead of target primary %s", strings.TrimSpace(serverAddress), strings.TrimSpace(expectedPodIP))
	}
	if inRecovery {
		return fmt.Errorf("database system is still in recovery")
	}
	if !strings.EqualFold(strings.TrimSpace(transactionReadOnly), "off") {
		return fmt.Errorf("transaction_read_only=%s", strings.TrimSpace(transactionReadOnly))
	}
	return nil
}

func managedPostgresServerAddressMatchesPodIP(serverAddress, expectedPodIP string) bool {
	serverIP := net.ParseIP(strings.TrimSpace(serverAddress))
	if serverIP == nil {
		parsedIP, _, err := net.ParseCIDR(strings.TrimSpace(serverAddress))
		if err != nil {
			return false
		}
		serverIP = parsedIP
	}
	expectedIP := net.ParseIP(strings.TrimSpace(expectedPodIP))
	return expectedIP != nil && serverIP.Equal(expectedIP)
}

func (s *Service) waitForManagedPostgresStorageExpansion(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, operationID string,
	target managedPostgresStorageTarget,
) error {
	waitCtx, cancel := context.WithTimeout(ctx, s.Config.ManagedAppRolloutTimeout)
	defer cancel()

	interval := 2 * time.Second
	if s.Config.PollInterval > interval {
		interval = s.Config.PollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastMessage := ""
	for {
		if strings.TrimSpace(operationID) != "" {
			if err := s.ensureOperationStillActive(operationID); err != nil {
				return err
			}
		}

		cluster, found, err := client.getCloudNativePGCluster(waitCtx, namespace, clusterName)
		if err != nil {
			return fmt.Errorf("read cloudnativepg cluster %s/%s: %w", namespace, clusterName, err)
		}
		if managedBackingServiceClusterReady(cluster, found) {
			converged, detail, err := inspectManagedPostgresStorageExpansion(
				waitCtx,
				client,
				namespace,
				clusterName,
				target,
			)
			if err != nil {
				return err
			}
			if converged {
				return nil
			}
			lastMessage = detail
		} else if !found {
			lastMessage = fmt.Sprintf("waiting for cluster %s to exist", clusterName)
		} else {
			lastMessage = fmt.Sprintf(
				"waiting for cluster %s to become ready (%d/%d instances)",
				clusterName,
				cluster.Status.ReadyInstances,
				max(cluster.Spec.Instances, 1),
			)
		}

		select {
		case <-waitCtx.Done():
			if lastMessage != "" {
				return fmt.Errorf("%w (%s)", waitCtx.Err(), lastMessage)
			}
			return waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) waitForManagedPostgresStorageConvergenceForDeployments(
	ctx context.Context,
	operationID, namespace string,
	deployments []runtime.ManagedBackingServiceDeployment,
) error {
	if len(deployments) == 0 {
		return nil
	}
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes postgres storage convergence client: %w", err)
	}
	for _, deployment := range deployments {
		if deployment.ResourceKind != runtime.CloudNativePGClusterKind || deployment.Suspended {
			continue
		}
		target := managedPostgresStorageTarget{
			StorageClassName: strings.TrimSpace(deployment.StorageClassName),
			StorageSize:      strings.TrimSpace(deployment.StorageSize),
		}
		if target.isZero() {
			continue
		}
		if err := s.waitForManagedPostgresStorageExpansion(
			ctx,
			client,
			namespace,
			strings.TrimSpace(deployment.ResourceName),
			operationID,
			target,
		); err != nil {
			return fmt.Errorf(
				"verify managed postgres storage convergence for %s/%s: %w",
				namespace,
				strings.TrimSpace(deployment.ResourceName),
				err,
			)
		}
	}
	return nil
}

func inspectManagedPostgresStorageExpansion(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName string,
	target managedPostgresStorageTarget,
) (bool, string, error) {
	targetSize := strings.TrimSpace(target.StorageSize)
	targetQuantity, err := resource.ParseQuantity(targetSize)
	if err != nil {
		return false, "", fmt.Errorf("parse managed postgres storage expansion target %q: %w", targetSize, err)
	}
	if targetQuantity.Value() <= 0 {
		return false, "", fmt.Errorf("managed postgres storage expansion target %q must be positive", targetSize)
	}
	pvcNames, err := client.listPersistentVolumeClaimNamesByLabel(
		ctx,
		namespace,
		"cnpg.io/cluster="+strings.TrimSpace(clusterName)+",cnpg.io/pvcRole=PG_DATA",
	)
	if err != nil {
		return false, "", fmt.Errorf("list postgres PVCs while verifying expansion %s/%s: %w", namespace, clusterName, err)
	}
	if len(pvcNames) == 0 {
		return false, fmt.Sprintf("waiting for postgres data PVCs for cluster %s", clusterName), nil
	}
	pods, err := client.listPodsBySelector(
		ctx,
		namespace,
		fmt.Sprintf(managedPostgresPodSelectorTemplate, strings.TrimSpace(clusterName)),
	)
	if err != nil {
		return false, "", fmt.Errorf("list postgres pods while verifying expansion %s/%s: %w", namespace, clusterName, err)
	}
	summaries := map[string]kubeNodeSummary{}
	summaryLoaded := map[string]bool{}
	for _, pvcName := range pvcNames {
		pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, pvcName)
		if err != nil {
			return false, "", fmt.Errorf("read postgres PVC %s/%s while verifying expansion: %w", namespace, pvcName, err)
		}
		if !found {
			return false, fmt.Sprintf("waiting for postgres PVC %s/%s to exist", namespace, pvcName), nil
		}
		if resizeErr := managedPostgresPVCResizeError(pvc); resizeErr != "" {
			return false, "", fmt.Errorf("postgres PVC %s/%s reports resize error: %s", namespace, pvcName, resizeErr)
		}
		requestedSize := strings.TrimSpace(pvc.Spec.Resources.Requests["storage"])
		requestedQuantity, err := resource.ParseQuantity(requestedSize)
		if err != nil || requestedQuantity.Cmp(targetQuantity) < 0 {
			return false, fmt.Sprintf(
				"waiting for postgres PVC %s/%s request to reach %s (current=%s)",
				namespace,
				pvcName,
				targetSize,
				firstNonEmptyString(requestedSize, "unknown"),
			), nil
		}
		if allocatedSize := strings.TrimSpace(pvc.Status.AllocatedResources["storage"]); allocatedSize != "" {
			allocatedQuantity, err := resource.ParseQuantity(allocatedSize)
			if err != nil || allocatedQuantity.Cmp(targetQuantity) < 0 {
				return false, fmt.Sprintf(
					"waiting for postgres PVC %s/%s allocated storage to reach %s (current=%s)",
					namespace,
					pvcName,
					targetSize,
					allocatedSize,
				), nil
			}
		}
		capacitySize := strings.TrimSpace(pvc.Status.Capacity["storage"])
		capacityQuantity, err := resource.ParseQuantity(capacitySize)
		if err != nil || capacityQuantity.Cmp(targetQuantity) < 0 {
			return false, fmt.Sprintf(
				"waiting for postgres PVC %s/%s status capacity to reach %s (current=%s)",
				namespace,
				pvcName,
				targetSize,
				firstNonEmptyString(capacitySize, "unknown"),
			), nil
		}
		filesystemCapacity, found, err := managedPostgresPVCFilesystemCapacity(
			ctx,
			client,
			namespace,
			pvcName,
			pods,
			summaries,
			summaryLoaded,
		)
		if err != nil {
			return false, "", err
		}
		minimumFilesystemCapacity := localpvsafety.MinimumFilesystemCapacityBytes(targetQuantity.Value())
		if !found || !localpvsafety.FilesystemCapacityConverged(filesystemCapacity, targetQuantity.Value()) {
			current := "unknown"
			if found {
				current = fmt.Sprintf("%d", filesystemCapacity)
			}
			return false, fmt.Sprintf(
				"waiting for postgres PVC %s/%s filesystem capacity to reach at least %d bytes for a %d-byte provisioned volume (current=%s)",
				namespace,
				pvcName,
				minimumFilesystemCapacity,
				targetQuantity.Value(),
				current,
			), nil
		}
	}
	return true, "", nil
}

func managedPostgresPVCResizeError(pvc kubePersistentVolumeClaim) string {
	for resourceName, status := range pvc.Status.AllocatedResourceStatuses {
		normalized := strings.ToLower(strings.TrimSpace(status))
		if strings.Contains(normalized, "error") || strings.Contains(normalized, "fail") {
			return fmt.Sprintf("allocatedResourceStatuses[%s]=%s", resourceName, status)
		}
	}
	for _, condition := range pvc.Status.Conditions {
		if !strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
			continue
		}
		kind := strings.ToLower(strings.Join([]string{condition.Type, condition.Reason}, " "))
		if !strings.Contains(kind, "error") && !strings.Contains(kind, "fail") {
			continue
		}
		detail := strings.TrimSpace(strings.Join([]string{condition.Type, condition.Reason, condition.Message}, ": "))
		return detail
	}
	return ""
}

func managedPostgresPVCFilesystemCapacity(
	ctx context.Context,
	client *kubeClient,
	namespace, pvcName string,
	pods []kubePod,
	summaries map[string]kubeNodeSummary,
	summaryLoaded map[string]bool,
) (int64, bool, error) {
	minimumCapacity := int64(0)
	capacityObserved := false
	mountedPodCount := 0
	for _, pod := range pods {
		if !kubePodMountsPersistentVolumeClaim(pod, pvcName) {
			continue
		}
		mountedPodCount++
		nodeName := strings.TrimSpace(pod.Spec.NodeName)
		if nodeName == "" {
			return 0, false, nil
		}
		if !summaryLoaded[nodeName] {
			summary, err := client.getNodeSummary(ctx, nodeName)
			if err != nil {
				return 0, false, fmt.Errorf("read kubelet stats summary for postgres node %s: %w", nodeName, err)
			}
			summaries[nodeName] = summary
			summaryLoaded[nodeName] = true
		}
		summary := summaries[nodeName]
		podObservationFound := false
		for _, summaryPod := range summary.Pods {
			if strings.TrimSpace(summaryPod.PodRef.Name) != strings.TrimSpace(pod.Metadata.Name) ||
				strings.TrimSpace(summaryPod.PodRef.Namespace) != strings.TrimSpace(namespace) {
				continue
			}
			for _, volume := range summaryPod.Volumes {
				if volume.PVCRef == nil ||
					strings.TrimSpace(volume.PVCRef.Name) != strings.TrimSpace(pvcName) {
					continue
				}
				refNamespace := strings.TrimSpace(volume.PVCRef.Namespace)
				if refNamespace != "" && refNamespace != strings.TrimSpace(namespace) {
					continue
				}
				if volume.CapacityBytes == nil || *volume.CapacityBytes > math.MaxInt64 {
					return 0, false, nil
				}
				capacity := int64(*volume.CapacityBytes)
				if capacity <= 0 {
					return 0, false, nil
				}
				if !capacityObserved || capacity < minimumCapacity {
					minimumCapacity = capacity
				}
				capacityObserved = true
				podObservationFound = true
			}
		}
		if !podObservationFound {
			return 0, false, nil
		}
	}
	return minimumCapacity, mountedPodCount > 0 && capacityObserved, nil
}

func kubePodMountsPersistentVolumeClaim(pod kubePod, pvcName string) bool {
	pvcName = strings.TrimSpace(pvcName)
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil &&
			strings.TrimSpace(volume.PersistentVolumeClaim.ClaimName) == pvcName {
			return true
		}
	}
	return false
}

func (s *Service) bindManagedPostgresPendingReplicaOnNode(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetNodeName, currentPrimary string,
	storageTarget managedPostgresStorageTarget,
) (string, error) {
	targetNodeName = strings.TrimSpace(targetNodeName)
	if targetNodeName == "" || storageTarget.isZero() {
		return "", nil
	}
	pods, err := client.listPodsBySelector(
		ctx,
		namespace,
		fmt.Sprintf(managedPostgresPodSelectorTemplate, clusterName),
	)
	if err != nil {
		return "", fmt.Errorf("list postgres pods for cluster %s: %w", clusterName, err)
	}

	currentPrimary = strings.TrimSpace(currentPrimary)
	for _, pod := range pods {
		podName := strings.TrimSpace(pod.Metadata.Name)
		if podName == "" || podName == currentPrimary || strings.TrimSpace(pod.Spec.NodeName) != "" {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Pending") {
			continue
		}
		matches, err := s.managedPostgresPodMatchesStorageTarget(ctx, client, namespace, pod, storageTarget)
		if err != nil {
			return "", err
		}
		if !matches {
			continue
		}
		pvcMatchesNode, err := s.managedPostgresPodPVCBoundToNode(ctx, client, namespace, pod, targetNodeName)
		if err != nil {
			return "", err
		}
		if !pvcMatchesNode {
			continue
		}
		if err := client.bindPodToNode(ctx, namespace, podName, targetNodeName); err != nil {
			return "", fmt.Errorf("bind pending postgres replica %s/%s to node %s: %w", namespace, podName, targetNodeName, err)
		}
		if s != nil && s.Logger != nil {
			s.Logger.Printf("bound pending postgres replica %s/%s to node %s for same-node storage migration", namespace, podName, targetNodeName)
		}
		return podName, nil
	}
	return "", nil
}

func (s *Service) selectManagedPostgresSwitchoverTarget(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetRuntimeID, currentPrimary string,
	storageTarget managedPostgresStorageTarget,
) (string, error) {
	pods, err := client.listPodsBySelector(
		ctx,
		namespace,
		fmt.Sprintf(managedPostgresPodSelectorTemplate, clusterName),
	)
	if err != nil {
		return "", fmt.Errorf("list postgres pods for cluster %s: %w", clusterName, err)
	}

	currentPrimary = strings.TrimSpace(currentPrimary)
	for _, pod := range pods {
		podName := strings.TrimSpace(pod.Metadata.Name)
		if podName == "" || podName == currentPrimary {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Running") {
			continue
		}
		runtimeID, err := s.runtimeIDForNode(ctx, client, strings.TrimSpace(pod.Spec.NodeName))
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(runtimeID) != targetRuntimeID {
			continue
		}
		matches, err := s.managedPostgresPodMatchesStorageTarget(ctx, client, namespace, pod, storageTarget)
		if err != nil {
			return "", err
		}
		if !matches {
			continue
		}
		return podName, nil
	}
	return "", nil
}

func (s *Service) selectManagedPostgresSwitchoverTargetOnNode(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetNodeName, currentPrimary string,
	storageTarget managedPostgresStorageTarget,
) (string, error) {
	pods, err := client.listPodsBySelector(
		ctx,
		namespace,
		fmt.Sprintf(managedPostgresPodSelectorTemplate, clusterName),
	)
	if err != nil {
		return "", fmt.Errorf("list postgres pods for cluster %s: %w", clusterName, err)
	}

	currentPrimary = strings.TrimSpace(currentPrimary)
	targetNodeName = strings.TrimSpace(targetNodeName)
	for _, pod := range pods {
		podName := strings.TrimSpace(pod.Metadata.Name)
		if podName == "" || podName == currentPrimary {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Running") {
			continue
		}
		if strings.TrimSpace(pod.Spec.NodeName) != targetNodeName {
			continue
		}
		matches, err := s.managedPostgresPodMatchesStorageTarget(ctx, client, namespace, pod, storageTarget)
		if err != nil {
			return "", err
		}
		if !matches {
			continue
		}
		return podName, nil
	}
	return "", nil
}

func firstStorageTarget(targets []managedPostgresStorageTarget) managedPostgresStorageTarget {
	if len(targets) == 0 {
		return managedPostgresStorageTarget{}
	}
	return targets[0]
}

func (s *Service) managedPostgresPodPVCBoundToNode(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	pod kubePod,
	targetNodeName string,
) (bool, error) {
	pvcName := managedPostgresPVCNameForPod(pod)
	if pvcName == "" {
		pvcName = strings.TrimSpace(pod.Metadata.Name)
	}
	if pvcName == "" {
		return false, nil
	}
	pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, pvcName)
	if err != nil {
		return false, fmt.Errorf("read postgres pvc %s/%s for pending replica binding: %w", namespace, pvcName, err)
	}
	if !found || strings.TrimSpace(pvc.Spec.VolumeName) == "" {
		return false, nil
	}
	pv, found, err := client.getPersistentVolume(ctx, pvc.Spec.VolumeName)
	if err != nil {
		return false, fmt.Errorf("read postgres pv %s for pending replica binding: %w", pvc.Spec.VolumeName, err)
	}
	if !found {
		return false, nil
	}
	return persistentVolumeNodeAffinityIncludesNode(pv, targetNodeName), nil
}

func persistentVolumeNodeAffinityIncludesNode(pv kubePersistentVolume, targetNodeName string) bool {
	targetNodeName = strings.TrimSpace(targetNodeName)
	if targetNodeName == "" || pv.Spec.NodeAffinity.Required == nil {
		return false
	}
	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, expression := range term.MatchExpressions {
			if !strings.EqualFold(strings.TrimSpace(expression.Operator), "In") {
				continue
			}
			for _, value := range expression.Values {
				if strings.TrimSpace(value) == targetNodeName {
					return true
				}
			}
		}
	}
	return false
}

func (s *Service) managedPostgresPodMatchesStorageTarget(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	pod kubePod,
	target managedPostgresStorageTarget,
) (bool, error) {
	if target.isZero() {
		return true, nil
	}
	pvcName := managedPostgresPVCNameForPod(pod)
	if pvcName == "" {
		pvcName = strings.TrimSpace(pod.Metadata.Name)
	}
	if pvcName == "" {
		return false, nil
	}
	pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, pvcName)
	if err != nil {
		return false, fmt.Errorf("read postgres pvc %s/%s for storage migration target: %w", namespace, pvcName, err)
	}
	if !found {
		return false, nil
	}
	if storageClassName := strings.TrimSpace(target.StorageClassName); storageClassName != "" &&
		strings.TrimSpace(pvc.Spec.StorageClassName) != storageClassName {
		return false, nil
	}
	if storageSize := strings.TrimSpace(target.StorageSize); storageSize != "" &&
		managedPostgresPVCStorageSize(pvc) != storageSize {
		return false, nil
	}
	return true, nil
}

func managedPostgresPVCStorageSize(pvc kubePersistentVolumeClaim) string {
	if size := strings.TrimSpace(pvc.Status.Capacity["storage"]); size != "" {
		return size
	}
	return strings.TrimSpace(pvc.Spec.Resources.Requests["storage"])
}

func (s *Service) managedPostgresPrimaryMatchesTarget(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetRuntimeID, targetNodeName string,
) (string, bool, error) {
	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, clusterName)
	if err != nil {
		return "", false, fmt.Errorf("read cloudnativepg cluster %s/%s: %w", namespace, clusterName, err)
	}
	if !found {
		return "", false, nil
	}
	currentPrimary := strings.TrimSpace(cluster.Status.CurrentPrimary)
	if currentPrimary == "" {
		return "", false, nil
	}
	pod, found, err := client.getPod(ctx, namespace, currentPrimary)
	if err != nil {
		return "", false, fmt.Errorf("read current postgres primary pod %s/%s: %w", namespace, currentPrimary, err)
	}
	if !found {
		return currentPrimary, false, nil
	}
	if targetNodeName != "" {
		return currentPrimary, strings.TrimSpace(pod.Spec.NodeName) == strings.TrimSpace(targetNodeName), nil
	}
	runtimeID, err := s.runtimeIDForNode(ctx, client, strings.TrimSpace(pod.Spec.NodeName))
	if err != nil {
		return currentPrimary, false, err
	}
	return currentPrimary, strings.TrimSpace(runtimeID) == strings.TrimSpace(targetRuntimeID), nil
}

func max(left, right int) int {
	if left >= right {
		return left
	}
	return right
}
