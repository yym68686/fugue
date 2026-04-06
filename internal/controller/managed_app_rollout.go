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
		if ready {
			if err := s.refreshManagedAppStatus(waitCtx, client, app); err != nil {
				s.Logger.Printf("refresh managed app status after rollout failed for %s/%s: %v", namespace, name, err)
			}
			return nil
		}

		managed, foundManagedApp, err := client.getManagedApp(waitCtx, namespace, managedAppName)
		if err != nil {
			return fmt.Errorf("read managed app rollout for %s/%s: %w", namespace, managedAppName, err)
		}
		if failureMessage := managedAppRolloutFailure(managed, foundManagedApp); failureMessage != "" {
			return fmt.Errorf("managed app %s/%s rollout failed: %s", namespace, managedAppName, failureMessage)
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
