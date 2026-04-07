package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func (s *Service) waitForManagedAppRollout(ctx context.Context, app model.App, operationID string) error {
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes rollout client: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, s.Config.ManagedAppRolloutTimeout)
	defer cancel()

	interval := 2 * time.Second
	if s.Config.PollInterval > interval {
		interval = s.Config.PollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	namespace := runtime.NamespaceForTenant(app.TenantID)
	name := runtime.RuntimeAppResourceName(app)
	managedAppName := runtime.ManagedAppResourceName(app)
	backingServices, err := s.managedBackingServiceRolloutTargets(waitCtx, app)
	if err != nil {
		return fmt.Errorf("resolve backing service rollout targets for %s/%s: %w", namespace, name, err)
	}
	lastMessage := ""

	for {
		if strings.TrimSpace(operationID) != "" {
			if err := s.ensureOperationStillActive(operationID); err != nil {
				return err
			}
		}

		deployment, found, err := client.getDeployment(waitCtx, namespace, name)
		if err != nil {
			return fmt.Errorf("read deployment rollout for %s/%s: %w", namespace, name, err)
		}

		ready, message, err := deploymentRolloutReady(deployment, found, app.Spec.Replicas, name)
		if err != nil {
			return err
		}
		managed, foundManagedApp, err := client.getManagedApp(waitCtx, namespace, managedAppName)
		if err != nil {
			return fmt.Errorf("read managed app rollout for %s/%s: %w", namespace, managedAppName, err)
		}
		if failureMessage := managedAppRolloutFailure(managed, foundManagedApp); failureMessage != "" {
			return fmt.Errorf("managed app %s/%s rollout failed: %s", namespace, managedAppName, failureMessage)
		}
		if ready {
			backingServicesReady, backingServiceMessage, err := managedBackingServicesRolloutReady(waitCtx, client, namespace, backingServices)
			if err != nil {
				return err
			}
			if !backingServicesReady {
				if strings.TrimSpace(backingServiceMessage) != "" {
					lastMessage = strings.TrimSpace(backingServiceMessage)
				}
				select {
				case <-waitCtx.Done():
					if lastMessage != "" {
						return fmt.Errorf("wait for deployment rollout %s/%s: %w (%s)", namespace, name, waitCtx.Err(), lastMessage)
					}
					return fmt.Errorf("wait for deployment rollout %s/%s: %w", namespace, name, waitCtx.Err())
				case <-ticker.C:
					continue
				}
			}
			if s.Store != nil {
				if err := s.refreshManagedAppStatus(waitCtx, client, app); err != nil {
					s.Logger.Printf("refresh managed app status after rollout failed for %s/%s: %v", namespace, name, err)
				}
			}
			return nil
		}
		if strings.TrimSpace(message) != "" {
			lastMessage = strings.TrimSpace(message)
		}

		select {
		case <-waitCtx.Done():
			if lastMessage != "" {
				return fmt.Errorf("wait for deployment rollout %s/%s: %w (%s)", namespace, name, waitCtx.Err(), lastMessage)
			}
			return fmt.Errorf("wait for deployment rollout %s/%s: %w", namespace, name, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (s *Service) managedBackingServiceRolloutTargets(ctx context.Context, app model.App) ([]runtime.ManagedBackingServiceDeployment, error) {
	postgresPlacements, err := s.managedPostgresPlacements(ctx, app)
	if err != nil {
		return nil, fmt.Errorf("resolve postgres placements: %w", err)
	}
	scheduling, err := s.managedSchedulingConstraints(app.Spec.RuntimeID)
	if err != nil {
		return nil, err
	}
	return runtime.ManagedBackingServiceDeploymentsWithPlacements(app, scheduling, postgresPlacements), nil
}

func managedBackingServicesRolloutReady(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	deployments []runtime.ManagedBackingServiceDeployment,
) (bool, string, error) {
	for _, deployment := range deployments {
		switch deployment.ResourceKind {
		case runtime.CloudNativePGClusterKind:
			cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, deployment.ResourceName)
			if err != nil {
				return false, "", fmt.Errorf("read backing service cluster %s/%s: %w", namespace, deployment.ResourceName, err)
			}
			ready, message := managedBackingServiceClusterRolloutReady(deployment.ResourceName, cluster, found)
			if !ready {
				return false, message, nil
			}
		default:
			status, found, err := client.getDeployment(ctx, namespace, deployment.ResourceName)
			if err != nil {
				return false, "", fmt.Errorf("read backing service deployment %s/%s: %w", namespace, deployment.ResourceName, err)
			}
			ready, message, err := deploymentRolloutReady(status, found, 1, deployment.ResourceName)
			if err != nil {
				return false, "", fmt.Errorf("wait for backing service deployment %s/%s: %w", namespace, deployment.ResourceName, err)
			}
			if !ready {
				return false, message, nil
			}
		}
	}
	return true, "", nil
}

func managedBackingServiceClusterRolloutReady(clusterName string, cluster kubeCloudNativePGCluster, found bool) (bool, string) {
	clusterName = strings.TrimSpace(clusterName)
	if clusterName == "" {
		clusterName = "cluster"
	}
	if !found {
		return false, fmt.Sprintf("waiting for backing service cluster %s to be created", clusterName)
	}

	desiredInstances := cluster.Spec.Instances
	if desiredInstances <= 0 {
		desiredInstances = 1
	}
	if cluster.Status.ReadyInstances != desiredInstances {
		return false, fmt.Sprintf(
			"waiting for backing service cluster %s to settle (%d/%d ready instances)",
			clusterName,
			cluster.Status.ReadyInstances,
			desiredInstances,
		)
	}

	currentPrimary := strings.TrimSpace(cluster.Status.CurrentPrimary)
	if currentPrimary == "" {
		return false, fmt.Sprintf("waiting for backing service cluster %s current primary", clusterName)
	}
	targetPrimary := strings.TrimSpace(cluster.Status.TargetPrimary)
	if targetPrimary != "" && targetPrimary != currentPrimary {
		return false, fmt.Sprintf(
			"waiting for backing service cluster %s switchover to settle (%s -> %s)",
			clusterName,
			currentPrimary,
			targetPrimary,
		)
	}
	return true, fmt.Sprintf("backing service cluster %s ready", clusterName)
}

func managedAppRolloutFailure(managed runtime.ManagedAppObject, found bool) string {
	if !found {
		return ""
	}
	if !strings.EqualFold(strings.TrimSpace(managed.Status.Phase), runtime.ManagedAppPhaseError) {
		return ""
	}
	if managed.Status.ObservedGeneration < managed.Metadata.Generation {
		return ""
	}

	message := strings.TrimSpace(managed.Status.Message)
	if message != "" {
		return message
	}

	return "managed app reported an error"
}

func (s *Service) refreshManagedAppStatus(ctx context.Context, client *kubeClient, app model.App) error {
	namespace := runtime.NamespaceForTenant(app.TenantID)
	name := runtime.ManagedAppResourceName(app)
	managed, found, err := client.getManagedApp(ctx, namespace, name)
	if err != nil {
		return fmt.Errorf("read managed app %s/%s: %w", namespace, name, err)
	}
	if !found {
		return nil
	}
	return s.reconcileManagedAppObject(ctx, client, managed)
}

func deploymentRolloutReady(deployment kubeDeployment, found bool, desiredReplicas int, deploymentName string) (bool, string, error) {
	deploymentName = strings.TrimSpace(deploymentName)
	if deploymentName == "" {
		deploymentName = "deployment"
	}

	if desiredReplicas <= 0 {
		if !found {
			return true, "deployment removed", nil
		}
		if hasDeploymentFailureCondition(deployment.Status.Conditions) {
			return false, "", fmt.Errorf("deployment %s scale down failed: %s", deploymentName, deploymentFailureMessage(deployment.Status.Conditions))
		}
		if deployment.Status.ObservedGeneration < deployment.Metadata.Generation {
			return false, fmt.Sprintf("waiting for deployment %s observed generation %d/%d", deploymentName, deployment.Status.ObservedGeneration, deployment.Metadata.Generation), nil
		}
		if deployment.Status.Replicas == 0 && deployment.Status.ReadyReplicas == 0 && deployment.Status.AvailableReplicas == 0 && deployment.Status.UpdatedReplicas == 0 && deployment.Status.UnavailableReplicas == 0 {
			return true, "deployment scaled to zero", nil
		}
		return false, fmt.Sprintf("waiting for deployment %s scale down (replicas=%d ready=%d available=%d)", deploymentName, deployment.Status.Replicas, deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas), nil
	}

	if !found {
		return false, fmt.Sprintf("waiting for deployment %s to be created", deploymentName), nil
	}
	if hasDeploymentFailureCondition(deployment.Status.Conditions) {
		return false, "", fmt.Errorf("deployment %s rollout failed: %s", deploymentName, deploymentFailureMessage(deployment.Status.Conditions))
	}
	if deployment.Status.ObservedGeneration < deployment.Metadata.Generation {
		return false, fmt.Sprintf("waiting for deployment %s observed generation %d/%d", deploymentName, deployment.Status.ObservedGeneration, deployment.Metadata.Generation), nil
	}
	if deployment.Status.UpdatedReplicas < desiredReplicas {
		return false, fmt.Sprintf("waiting for deployment %s updated replicas %d/%d", deploymentName, deployment.Status.UpdatedReplicas, desiredReplicas), nil
	}
	if deployment.Status.ReadyReplicas < desiredReplicas {
		return false, fmt.Sprintf("waiting for deployment %s ready replicas %d/%d", deploymentName, deployment.Status.ReadyReplicas, desiredReplicas), nil
	}
	if deployment.Status.AvailableReplicas < desiredReplicas {
		return false, fmt.Sprintf("waiting for deployment %s available replicas %d/%d", deploymentName, deployment.Status.AvailableReplicas, desiredReplicas), nil
	}
	if deployment.Status.Replicas > desiredReplicas {
		return false, fmt.Sprintf("waiting for deployment %s old replicas to terminate (%d total, desired=%d)", deploymentName, deployment.Status.Replicas, desiredReplicas), nil
	}
	if deployment.Status.UnavailableReplicas > 0 {
		return false, fmt.Sprintf("waiting for deployment %s unavailable replicas to drain (%d)", deploymentName, deployment.Status.UnavailableReplicas), nil
	}
	return true, fmt.Sprintf("deployment %s ready", deploymentName), nil
}
