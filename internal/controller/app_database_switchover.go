package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
	"k8s.io/apimachinery/pkg/api/resource"
)

const managedPostgresPodSelectorTemplate = "cnpg.io/cluster=%s,app.kubernetes.io/managed-by=cloudnative-pg"

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
	if sourceRuntimeID == targetRuntimeID {
		return fmt.Errorf("managed postgres for app %s is already on runtime %s", app.ID, targetRuntimeID)
	}

	clusterName := strings.TrimSpace(currentDatabase.ServiceName)
	if clusterName == "" {
		return fmt.Errorf("managed postgres for app %s is missing a cluster service name", app.ID)
	}

	stageSpec := databaseSwitchoverSpec(app.Spec, currentDatabase, sourceRuntimeID, targetRuntimeID)
	if _, err := s.applyManagedDesiredAppState(ctx, op.ID, app, stageSpec); err != nil {
		return fmt.Errorf("prepare managed postgres standby on %s: %w", targetRuntimeID, err)
	}

	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes client for database switchover: %w", err)
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	targetPrimary, err := s.waitForManagedPostgresReplicaOnRuntime(
		ctx,
		client,
		namespace,
		clusterName,
		targetRuntimeID,
		op.ID,
	)
	if err != nil {
		return fmt.Errorf("wait for managed postgres standby on %s: %w", targetRuntimeID, err)
	}

	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	if err := client.patchCloudNativePGClusterStatus(
		ctx,
		namespace,
		clusterName,
		targetPrimary,
		"Switchover",
		fmt.Sprintf("Switching over to %s", targetPrimary),
	); err != nil {
		return fmt.Errorf("request managed postgres switchover to %s: %w", targetPrimary, err)
	}
	if err := s.waitForManagedPostgresPrimary(
		ctx,
		client,
		namespace,
		clusterName,
		targetPrimary,
		op.ID,
	); err != nil {
		return fmt.Errorf("wait for managed postgres switchover to %s: %w", targetPrimary, err)
	}

	finalSpec := databaseSwitchoverSpec(app.Spec, currentDatabase, targetRuntimeID, sourceRuntimeID)
	finalBundle, err := s.applyManagedDesiredAppState(ctx, op.ID, app, finalSpec)
	if err != nil {
		return fmt.Errorf("finalize managed postgres runtime assignments: %w", err)
	}
	if err := s.waitForManagedPostgresPrimary(
		ctx,
		client,
		namespace,
		clusterName,
		targetPrimary,
		op.ID,
	); err != nil {
		return fmt.Errorf("wait for managed postgres to settle after switchover: %w", err)
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}

	message := fmt.Sprintf("managed postgres switched over from %s to %s", sourceRuntimeID, targetRuntimeID)
	_, err = s.Store.CompleteManagedOperationWithResult(
		op.ID,
		finalBundle.ManifestPath,
		message,
		&finalSpec,
		nil,
	)
	if err != nil {
		return fmt.Errorf("complete database switchover operation %s: %w", op.ID, err)
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
	sourceRuntimeID := strings.TrimSpace(currentDatabase.RuntimeID)
	if sourceRuntimeID == "" {
		sourceRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	if sourceRuntimeID == "" {
		return fmt.Errorf("managed postgres service %s for app %s is missing a source runtime", target.ServiceID, app.ID)
	}
	if sourceRuntimeID == targetRuntimeID {
		return fmt.Errorf("managed postgres service %s is already on runtime %s", target.ServiceID, targetRuntimeID)
	}

	clusterName := strings.TrimSpace(currentDatabase.ServiceName)
	if clusterName == "" {
		return fmt.Errorf("managed postgres service %s for app %s is missing a cluster service name", target.ServiceID, app.ID)
	}

	stagePostgres := databaseSwitchoverPostgresSpec(currentDatabase, sourceRuntimeID, targetRuntimeID)
	stageApp, err := s.updateAppBackingServicePostgres(target.ServiceID, app, stagePostgres)
	if err != nil {
		return fmt.Errorf("stage managed postgres service %s standby on %s: %w", target.ServiceID, targetRuntimeID, err)
	}
	if _, err := s.applyManagedDesiredAppState(ctx, op.ID, stageApp, stageApp.Spec); err != nil {
		return fmt.Errorf("prepare managed postgres service %s standby on %s: %w", target.ServiceID, targetRuntimeID, err)
	}

	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes client for database switchover: %w", err)
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	targetPrimary, err := s.waitForManagedPostgresReplicaOnRuntime(ctx, client, namespace, clusterName, targetRuntimeID, op.ID)
	if err != nil {
		return fmt.Errorf("wait for managed postgres service %s standby on %s: %w", target.ServiceID, targetRuntimeID, err)
	}

	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	if err := client.patchCloudNativePGClusterStatus(
		ctx,
		namespace,
		clusterName,
		targetPrimary,
		"Switchover",
		fmt.Sprintf("Switching over service %s to %s", target.ServiceID, targetPrimary),
	); err != nil {
		return fmt.Errorf("request managed postgres service %s switchover to %s: %w", target.ServiceID, targetPrimary, err)
	}
	if err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID); err != nil {
		return fmt.Errorf("wait for managed postgres service %s switchover to %s: %w", target.ServiceID, targetPrimary, err)
	}

	finalPostgres := databaseSwitchoverPostgresSpec(currentDatabase, targetRuntimeID, sourceRuntimeID)
	finalApp, err := s.updateAppBackingServicePostgres(target.ServiceID, app, finalPostgres)
	if err != nil {
		return fmt.Errorf("finalize managed postgres service %s runtime assignments: %w", target.ServiceID, err)
	}
	finalBundle, err := s.applyManagedDesiredAppState(ctx, op.ID, finalApp, finalApp.Spec)
	if err != nil {
		return fmt.Errorf("apply finalized managed postgres service %s state: %w", target.ServiceID, err)
	}
	if err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID); err != nil {
		return fmt.Errorf("wait for managed postgres service %s to settle after switchover: %w", target.ServiceID, err)
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}

	message := fmt.Sprintf("managed postgres service %s switched over from %s to %s", target.ServiceID, sourceRuntimeID, targetRuntimeID)
	_, err = s.Store.CompleteManagedOperationWithResult(
		op.ID,
		finalBundle.ManifestPath,
		message,
		&finalApp.Spec,
		nil,
	)
	if err != nil {
		return fmt.Errorf("complete database switchover operation %s: %w", op.ID, err)
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

	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes client for database localize: %w", err)
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	targetNodeName := ""
	if op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
		targetNodeName = strings.TrimSpace(op.DesiredSpec.Postgres.PrimaryNodeName)
	}
	targetNodeName, err = s.resolveDatabaseLocalizeTargetNode(ctx, client, app, targetRuntimeID, targetNodeName)
	if err != nil {
		return err
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

	targetPrimary := ""
	currentPrimary, alreadyLocalized, err := s.managedPostgresPrimaryMatchesTarget(ctx, client, namespace, clusterName, targetRuntimeID, targetNodeName)
	if err != nil {
		return err
	}
	if alreadyLocalized && !storageMigrationRequired {
		targetPrimary = currentPrimary
	} else {
		stageSpec := databaseLocalizeStageSpec(app.Spec, desiredDatabase, sourceRuntimeID, targetRuntimeID, targetNodeName)
		if storageMigrationRequired && stageSpec.Postgres != nil {
			ensureDatabaseLocalizeStorageMigrationCapacity(stageSpec.Postgres, currentDatabase)
		}
		if _, err := s.applyManagedDesiredAppState(ctx, op.ID, app, stageSpec); err != nil {
			return fmt.Errorf("prepare localized managed postgres standby on runtime %s: %w", targetRuntimeID, err)
		}
		if targetNodeName != "" {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnNode(ctx, client, namespace, clusterName, targetNodeName, op.ID, storageTarget)
		} else {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnRuntime(ctx, client, namespace, clusterName, targetRuntimeID, op.ID, storageTarget)
		}
		if err != nil {
			return fmt.Errorf("wait for localized managed postgres standby on runtime %s: %w", targetRuntimeID, err)
		}
		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return err
		}
		if err := client.patchCloudNativePGClusterStatus(
			ctx,
			namespace,
			clusterName,
			targetPrimary,
			"Switchover",
			fmt.Sprintf("Localizing managed postgres primary to %s", targetPrimary),
		); err != nil {
			return fmt.Errorf("request managed postgres localize switchover to %s: %w", targetPrimary, err)
		}
		if err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID); err != nil {
			return fmt.Errorf("wait for managed postgres localize switchover to %s: %w", targetPrimary, err)
		}
	}

	finalSpec := databaseLocalizeSpec(app.Spec, desiredDatabase, targetRuntimeID, targetNodeName, true, false)
	finalBundle, err := s.applyManagedDesiredAppState(ctx, op.ID, app, finalSpec)
	if err != nil {
		return fmt.Errorf("finalize localized managed postgres state: %w", err)
	}
	if targetPrimary != "" {
		if err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID); err != nil {
			return fmt.Errorf("wait for localized managed postgres to settle: %w", err)
		}
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
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
		return fmt.Errorf("complete database localize operation %s: %w", op.ID, err)
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

	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes client for database localize: %w", err)
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	targetNodeName := ""
	if op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
		targetNodeName = strings.TrimSpace(op.DesiredSpec.Postgres.PrimaryNodeName)
	}
	targetNodeName, err = s.resolveDatabaseLocalizeTargetNode(ctx, client, app, targetRuntimeID, targetNodeName)
	if err != nil {
		return err
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

	targetPrimary := ""
	currentPrimary, alreadyLocalized, err := s.managedPostgresPrimaryMatchesTarget(ctx, client, namespace, clusterName, targetRuntimeID, targetNodeName)
	if err != nil {
		return err
	}
	if alreadyLocalized && !storageMigrationRequired {
		targetPrimary = currentPrimary
	} else {
		stagePostgres := databaseLocalizeStagePostgresSpec(desiredDatabase, sourceRuntimeID, targetRuntimeID, targetNodeName)
		if storageMigrationRequired {
			ensureDatabaseLocalizeStorageMigrationCapacity(&stagePostgres, currentDatabase)
		}
		stageApp, err := s.updateAppBackingServicePostgres(target.ServiceID, app, stagePostgres)
		if err != nil {
			return fmt.Errorf("prepare localized managed postgres service %s state: %w", target.ServiceID, err)
		}
		if _, err := s.applyManagedDesiredAppState(ctx, op.ID, stageApp, stageApp.Spec); err != nil {
			return fmt.Errorf("prepare localized managed postgres service %s standby on runtime %s: %w", target.ServiceID, targetRuntimeID, err)
		}
		if targetNodeName != "" {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnNode(ctx, client, namespace, clusterName, targetNodeName, op.ID, storageTarget)
		} else {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnRuntime(ctx, client, namespace, clusterName, targetRuntimeID, op.ID, storageTarget)
		}
		if err != nil {
			return fmt.Errorf("wait for localized managed postgres service %s standby on runtime %s: %w", target.ServiceID, targetRuntimeID, err)
		}
		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return err
		}
		if err := client.patchCloudNativePGClusterStatus(
			ctx,
			namespace,
			clusterName,
			targetPrimary,
			"Switchover",
			fmt.Sprintf("Localizing managed postgres service %s primary to %s", target.ServiceID, targetPrimary),
		); err != nil {
			return fmt.Errorf("request managed postgres service %s localize switchover to %s: %w", target.ServiceID, targetPrimary, err)
		}
		if err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID); err != nil {
			return fmt.Errorf("wait for managed postgres service %s localize switchover to %s: %w", target.ServiceID, targetPrimary, err)
		}
	}

	finalPostgres := databaseLocalizePostgresSpec(desiredDatabase, targetRuntimeID, targetNodeName, true, false)
	finalApp, err := s.updateAppBackingServicePostgres(target.ServiceID, app, finalPostgres)
	if err != nil {
		return fmt.Errorf("finalize localized managed postgres service %s state: %w", target.ServiceID, err)
	}
	finalBundle, err := s.applyManagedDesiredAppState(ctx, op.ID, finalApp, finalApp.Spec)
	if err != nil {
		return fmt.Errorf("apply finalized managed postgres service %s state: %w", target.ServiceID, err)
	}
	if targetPrimary != "" {
		if err := s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, targetPrimary, op.ID); err != nil {
			return fmt.Errorf("wait for localized managed postgres service %s to settle: %w", target.ServiceID, err)
		}
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
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
		return fmt.Errorf("complete database localize operation %s: %w", op.ID, err)
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
	if spec == nil {
		return nil
	}
	out := *spec
	if spec.Resources != nil {
		resources := *spec.Resources
		out.Resources = &resources
	}
	return &out
}

func managedPostgresStorageMigrationRequired(current, desired *model.AppPostgresSpec) bool {
	if current == nil || desired == nil {
		return false
	}
	return strings.TrimSpace(current.StorageClassName) != strings.TrimSpace(desired.StorageClassName) ||
		strings.TrimSpace(current.StorageSize) != strings.TrimSpace(desired.StorageSize)
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

func databaseSwitchoverSpec(
	base model.AppSpec,
	postgres *model.AppPostgresSpec,
	primaryRuntimeID, failoverTargetRuntimeID string,
) model.AppSpec {
	next := base
	if postgres != nil {
		postgresCopy := databaseSwitchoverPostgresSpec(postgres, primaryRuntimeID, failoverTargetRuntimeID)
		next.Postgres = &postgresCopy
	}
	return next
}

func databaseSwitchoverPostgresSpec(
	postgres *model.AppPostgresSpec,
	primaryRuntimeID, failoverTargetRuntimeID string,
) model.AppPostgresSpec {
	postgresCopy := *postgres
	if postgres.Resources != nil {
		resources := *postgres.Resources
		postgresCopy.Resources = &resources
	}
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
		postgresCopy := databaseSwitchoverPostgresSpec(postgres, sourceRuntimeID, targetRuntimeID)
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
	postgresCopy := *postgres
	if postgres.Resources != nil {
		resources := *postgres.Resources
		postgresCopy.Resources = &resources
	}
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
	if err := s.applyManagedAppDesiredState(ctx, app, scheduling); err != nil {
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

func (s *Service) waitForManagedPostgresPrimary(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetPrimary, operationID string,
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
		if found &&
			managedBackingServiceClusterReady(cluster, found) &&
			strings.TrimSpace(cluster.Status.CurrentPrimary) == strings.TrimSpace(targetPrimary) {
			return nil
		}

		if !found {
			lastMessage = fmt.Sprintf("waiting for cluster %s to exist", clusterName)
		} else {
			lastMessage = fmt.Sprintf(
				"waiting for cluster %s primary to switch to %s (current=%s target=%s)",
				clusterName,
				targetPrimary,
				strings.TrimSpace(cluster.Status.CurrentPrimary),
				strings.TrimSpace(cluster.Status.TargetPrimary),
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
