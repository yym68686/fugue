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

	currentDatabase := store.OwnedManagedPostgresSpec(app)
	if currentDatabase == nil {
		return fmt.Errorf("managed postgres is not configured for app %s", app.ID)
	}
	sourceRuntimeID := strings.TrimSpace(currentDatabase.RuntimeID)
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

func databaseSwitchoverSpec(
	base model.AppSpec,
	postgres *model.AppPostgresSpec,
	primaryRuntimeID, failoverTargetRuntimeID string,
) model.AppSpec {
	next := base
	if postgres != nil {
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
		next.Postgres = &postgresCopy
	}
	return next
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
	scheduling, err := s.managedSchedulingConstraints(app.Spec.RuntimeID)
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
	if err := s.waitForManagedAppRollout(ctx, app, operationID); err != nil {
		return runtime.Bundle{}, fmt.Errorf("wait for managed app rollout %s: %w", app.ID, err)
	}
	return bundle, nil
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

func max(left, right int) int {
	if left >= right {
		return left
	}
	return right
}
