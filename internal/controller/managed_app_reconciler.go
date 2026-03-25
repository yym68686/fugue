package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func (s *Service) applyManagedAppDesiredState(ctx context.Context, app model.App, scheduling runtime.SchedulingConstraints) error {
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes managed app client: %w", err)
	}

	objects := runtime.BuildManagedAppStateObjects(app, scheduling)
	if err := client.applyObjects(ctx, objects); err != nil {
		return fmt.Errorf("apply managed app state objects: %w", err)
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
	return s.reconcileManagedAppObject(ctx, client, managed)
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
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes managed app client: %w", err)
	}

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
	return firstErr
}

func (s *Service) reconcileManagedAppObject(ctx context.Context, client *kubeClient, managed runtime.ManagedAppObject) error {
	app := runtime.AppFromManagedApp(managed)
	namespace := strings.TrimSpace(managed.Metadata.Namespace)
	if namespace == "" {
		namespace = runtime.NamespaceForTenant(app.TenantID)
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

	ownerRef := runtime.ManagedAppOwnerReference(managed)
	childObjects := runtime.BuildManagedAppChildObjects(app, managed.Spec.Scheduling, ownerRef)
	if err := client.applyObjects(ctx, childObjects); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("apply managed app child objects: %w", err))
	}
	if err := s.pruneManagedAppStaleObjects(ctx, client, namespace, app, childObjects); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("prune stale managed app child objects: %w", err))
	}

	deployment, found, err := client.getDeployment(ctx, namespace, runtime.RuntimeResourceName(app.Name))
	if err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("read deployment status: %w", err))
	}

	status := buildManagedAppStatus(managed, app, deployment, found)
	if err := client.patchManagedAppStatus(ctx, namespace, managed.Metadata.Name, status); err != nil {
		return fmt.Errorf("patch managed app status for %s/%s: %w", namespace, managed.Metadata.Name, err)
	}
	return nil
}

func patchManagedAppErrorStatus(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, app model.App, cause error) error {
	status := managedAppBaseStatus(managed, app)
	status.Phase = runtime.ManagedAppPhaseError
	status.Message = strings.TrimSpace(cause.Error())
	if err := client.patchManagedAppStatus(ctx, namespace, managed.Metadata.Name, status); err != nil {
		return fmt.Errorf("%w (also failed to patch managed app status: %v)", cause, err)
	}
	return cause
}

func buildManagedAppStatus(managed runtime.ManagedAppObject, app model.App, deployment kubeDeployment, found bool) runtime.ManagedAppStatus {
	status := managedAppBaseStatus(managed, app)
	if found {
		status.ReadyReplicas = maxInt(deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas)
		status.Conditions = append([]runtime.ManagedAppCondition(nil), deployment.Status.Conditions...)
	}

	switch {
	case app.Spec.Replicas <= 0:
		status.Phase = runtime.ManagedAppPhaseDisabled
		status.Message = "desired replicas set to 0"
	case !found:
		status.Phase = runtime.ManagedAppPhasePending
		status.Message = fmt.Sprintf("waiting for deployment %s", runtime.RuntimeResourceName(app.Name))
	case hasDeploymentFailureCondition(deployment.Status.Conditions):
		status.Phase = runtime.ManagedAppPhaseError
		status.Message = deploymentFailureMessage(deployment.Status.Conditions)
	case status.ReadyReplicas >= app.Spec.Replicas:
		status.Phase = runtime.ManagedAppPhaseReady
		status.Message = fmt.Sprintf("deployment ready (%d/%d replicas)", status.ReadyReplicas, app.Spec.Replicas)
	case deployment.Status.Replicas == 0:
		status.Phase = runtime.ManagedAppPhasePending
		status.Message = fmt.Sprintf("deployment created; waiting for replicas (desired=%d)", app.Spec.Replicas)
	default:
		status.Phase = runtime.ManagedAppPhaseProgressing
		status.Message = fmt.Sprintf("deployment progressing (%d/%d ready replicas)", status.ReadyReplicas, app.Spec.Replicas)
	}
	return status
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
	if strings.TrimSpace(app.ID) == "" {
		return nil
	}

	deployments, err := s.listOwnedDeploymentNames(ctx, client, namespace, app.ID)
	if err != nil {
		return err
	}
	for _, name := range deployments {
		if err := client.deleteDeployment(ctx, namespace, name); err != nil {
			return err
		}
	}

	services, err := s.listOwnedServiceNames(ctx, client, namespace, app.ID)
	if err != nil {
		return err
	}
	for _, name := range services {
		if err := client.deleteService(ctx, namespace, name); err != nil {
			return err
		}
	}

	secrets, err := s.listOwnedSecretNames(ctx, client, namespace, app.ID)
	if err != nil {
		return err
	}
	for _, name := range secrets {
		if err := client.deleteSecret(ctx, namespace, name); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) listOwnedDeploymentNames(ctx context.Context, client *kubeClient, namespace, appID string) ([]string, error) {
	return listOwnedNames(ctx, appID, func(selector string) ([]string, error) {
		return client.listDeploymentNamesByLabel(ctx, namespace, selector)
	})
}

func (s *Service) listOwnedServiceNames(ctx context.Context, client *kubeClient, namespace, appID string) ([]string, error) {
	return listOwnedNames(ctx, appID, func(selector string) ([]string, error) {
		return client.listServiceNamesByLabel(ctx, namespace, selector)
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
