package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
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
	if strings.TrimSpace(op.ServiceID) != "" && !target.AppOwned {
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
	if strings.TrimSpace(op.ServiceID) != "" && !target.AppOwned {
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

	targetPrimary := ""
	currentPrimary, alreadyLocalized, err := s.managedPostgresPrimaryMatchesTarget(ctx, client, namespace, clusterName, targetRuntimeID, targetNodeName)
	if err != nil {
		return err
	}
	if alreadyLocalized {
		targetPrimary = currentPrimary
	} else {
		stageSpec := databaseLocalizeStageSpec(app.Spec, currentDatabase, sourceRuntimeID, targetRuntimeID, targetNodeName)
		if _, err := s.applyManagedDesiredAppState(ctx, op.ID, app, stageSpec); err != nil {
			return fmt.Errorf("prepare localized managed postgres standby on runtime %s: %w", targetRuntimeID, err)
		}
		if targetNodeName != "" {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnNode(ctx, client, namespace, clusterName, targetNodeName, op.ID)
		} else {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnRuntime(ctx, client, namespace, clusterName, targetRuntimeID, op.ID)
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

	finalSpec := databaseLocalizeSpec(app.Spec, currentDatabase, targetRuntimeID, targetNodeName, true, false)
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

	targetPrimary := ""
	currentPrimary, alreadyLocalized, err := s.managedPostgresPrimaryMatchesTarget(ctx, client, namespace, clusterName, targetRuntimeID, targetNodeName)
	if err != nil {
		return err
	}
	if alreadyLocalized {
		targetPrimary = currentPrimary
	} else {
		stagePostgres := databaseLocalizeStagePostgresSpec(currentDatabase, sourceRuntimeID, targetRuntimeID, targetNodeName)
		stageApp, err := s.updateAppBackingServicePostgres(target.ServiceID, app, stagePostgres)
		if err != nil {
			return fmt.Errorf("prepare localized managed postgres service %s state: %w", target.ServiceID, err)
		}
		if _, err := s.applyManagedDesiredAppState(ctx, op.ID, stageApp, stageApp.Spec); err != nil {
			return fmt.Errorf("prepare localized managed postgres service %s standby on runtime %s: %w", target.ServiceID, targetRuntimeID, err)
		}
		if targetNodeName != "" {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnNode(ctx, client, namespace, clusterName, targetNodeName, op.ID)
		} else {
			targetPrimary, err = s.waitForManagedPostgresReplicaOnRuntime(ctx, client, namespace, clusterName, targetRuntimeID, op.ID)
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

	finalPostgres := databaseLocalizePostgresSpec(currentDatabase, targetRuntimeID, targetNodeName, true, false)
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
	if sourceRuntimeID != "" && targetRuntimeID != "" && sourceRuntimeID != targetRuntimeID {
		postgresCopy := databaseSwitchoverPostgresSpec(postgres, sourceRuntimeID, targetRuntimeID)
		postgresCopy.PrimaryNodeName = ""
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
			targetPrimary, err := s.selectManagedPostgresSwitchoverTarget(waitCtx, client, namespace, clusterName, targetRuntimeID, cluster.Status.CurrentPrimary)
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
			targetPrimary, err := s.selectManagedPostgresSwitchoverTargetOnNode(waitCtx, client, namespace, clusterName, targetNodeName, cluster.Status.CurrentPrimary)
			if err != nil {
				return "", err
			}
			if targetPrimary != "" {
				return targetPrimary, nil
			}
			lastMessage = fmt.Sprintf("waiting for a standby on node %s for cluster %s", targetNodeName, clusterName)
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

func (s *Service) selectManagedPostgresSwitchoverTarget(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetRuntimeID, currentPrimary string,
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
		return podName, nil
	}
	return "", nil
}

func (s *Service) selectManagedPostgresSwitchoverTargetOnNode(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName, targetNodeName, currentPrimary string,
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
		return podName, nil
	}
	return "", nil
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
