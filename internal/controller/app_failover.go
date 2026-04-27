package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const minimumAppFenceLeaseDuration = 5 * time.Minute

func appFenceLeaseName(app model.App) string {
	name := runtime.ManagedAppResourceName(app)
	if name == "" {
		name = strings.TrimSpace(app.ID)
	}
	if name == "" {
		name = "app"
	}
	return name + "-fence"
}

func (s *Service) currentAppFenceEpoch(ctx context.Context, client *kubeClient, app model.App) (string, error) {
	if client == nil {
		return "", nil
	}
	lease, found, err := client.getLease(ctx, runtime.NamespaceForTenant(app.TenantID), appFenceLeaseName(app))
	if err != nil || !found || lease.Spec.LeaseTransitions <= 0 {
		return "", err
	}
	return strconv.Itoa(lease.Spec.LeaseTransitions), nil
}

func (s *Service) acquireAppFenceLease(ctx context.Context, client *kubeClient, app model.App, holder string) (string, error) {
	if client == nil {
		return "", fmt.Errorf("kubernetes client is required for failover fencing")
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	leaseName := appFenceLeaseName(app)
	now := s.now().UTC()
	duration := s.Config.ManagedAppRolloutTimeout
	if duration < minimumAppFenceLeaseDuration {
		duration = minimumAppFenceLeaseDuration
	}

	current, found, err := client.getLease(ctx, namespace, leaseName)
	if err != nil {
		return "", err
	}
	if !found {
		record := kubeLease{
			APIVersion: "coordination.k8s.io/v1",
			Kind:       "Lease",
		}
		record.Metadata.Name = leaseName
		record.Metadata.Namespace = namespace
		record.Spec.HolderIdentity = strings.TrimSpace(holder)
		record.Spec.LeaseDurationSeconds = int(duration.Seconds())
		record.Spec.AcquireTime = formatKubeTimestamp(now)
		record.Spec.RenewTime = formatKubeTimestamp(now)
		record.Spec.LeaseTransitions = 1
		if err := client.createLease(ctx, namespace, record); err != nil {
			return "", err
		}
		return "1", nil
	}

	currentHolder := strings.TrimSpace(current.Spec.HolderIdentity)
	if currentHolder != strings.TrimSpace(holder) {
		current.Spec.LeaseTransitions++
		current.Spec.AcquireTime = formatKubeTimestamp(now)
	}
	if current.Spec.LeaseTransitions <= 0 {
		current.Spec.LeaseTransitions = 1
	}
	current.Spec.HolderIdentity = strings.TrimSpace(holder)
	current.Spec.LeaseDurationSeconds = int(duration.Seconds())
	current.Spec.RenewTime = formatKubeTimestamp(now)
	if strings.TrimSpace(current.Spec.AcquireTime) == "" {
		current.Spec.AcquireTime = formatKubeTimestamp(now)
	}
	if err := client.updateLease(ctx, namespace, current); err != nil {
		return "", err
	}
	return strconv.Itoa(current.Spec.LeaseTransitions), nil
}

func decorateManagedAppObjectsWithFenceEpoch(objects []map[string]any, app model.App, epoch string) {
	epoch = strings.TrimSpace(epoch)
	if len(objects) == 0 || epoch == "" {
		return
	}
	resourceName := runtime.RuntimeAppResourceName(app)
	for _, obj := range objects {
		kind, _ := obj["kind"].(string)
		name, _ := objectNameAndNamespace("", obj)
		if strings.TrimSpace(name) != resourceName {
			continue
		}
		switch strings.TrimSpace(kind) {
		case "Deployment":
			decorateDeploymentFenceEpoch(obj, epoch)
		case "Service":
			decorateServiceFenceEpoch(obj, epoch)
		}
	}
}

func decorateDeploymentFenceEpoch(obj map[string]any, epoch string) {
	metadata, _ := obj["metadata"].(map[string]any)
	labels := normalizeStringMap(metadata["labels"])
	labels[runtime.FugueLabelFenceEpoch] = epoch
	metadata["labels"] = labels

	spec, _ := obj["spec"].(map[string]any)
	selector, _ := spec["selector"].(map[string]any)
	matchLabels := normalizeStringMap(selector["matchLabels"])
	matchLabels[runtime.FugueLabelFenceEpoch] = epoch
	selector["matchLabels"] = matchLabels
	spec["selector"] = selector

	template, _ := spec["template"].(map[string]any)
	templateMetadata, _ := template["metadata"].(map[string]any)
	templateLabels := normalizeStringMap(templateMetadata["labels"])
	templateLabels[runtime.FugueLabelFenceEpoch] = epoch
	templateMetadata["labels"] = templateLabels
	template["metadata"] = templateMetadata
	spec["template"] = template
	obj["spec"] = spec
}

func decorateServiceFenceEpoch(obj map[string]any, epoch string) {
	metadata, _ := obj["metadata"].(map[string]any)
	labels := normalizeStringMap(metadata["labels"])
	labels[runtime.FugueLabelFenceEpoch] = epoch
	metadata["labels"] = labels

	spec, _ := obj["spec"].(map[string]any)
	selector := normalizeStringMap(spec["selector"])
	selector[runtime.FugueLabelFenceEpoch] = epoch
	spec["selector"] = selector
	obj["spec"] = spec
}

func normalizeStringMap(raw any) map[string]string {
	switch typed := raw.(type) {
	case map[string]string:
		out := make(map[string]string, len(typed))
		for key, value := range typed {
			out[key] = value
		}
		return out
	case map[string]any:
		out := make(map[string]string, len(typed))
		for key, value := range typed {
			if text, ok := value.(string); ok {
				out[key] = text
			}
		}
		return out
	default:
		return map[string]string{}
	}
}

func (s *Service) reconcileWorkspaceReplicationSource(ctx context.Context, client *kubeClient, app model.App, ownerRef *runtime.OwnerReference) error {
	if client == nil || !runtime.AppVolumeReplicationEnabled(app) {
		return nil
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	destination, found, err := client.getVolSyncReplicationDestination(ctx, namespace, runtime.WorkspaceReplicationDestinationName(app))
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	address := nestedObjectString(destination, "status", "rsyncTLS", "address")
	keySecret := nestedObjectString(destination, "status", "rsyncTLS", "keySecret")
	if address == "" || keySecret == "" {
		return nil
	}
	return client.applyObject(ctx, runtime.BuildWorkspaceReplicationSourceObject(app, ownerRef, address, keySecret), nil)
}

func (s *Service) ensureWorkspaceFinalSync(ctx context.Context, client *kubeClient, app model.App, syncToken string) error {
	if client == nil || !runtime.AppHasReplicableVolume(app) {
		return nil
	}
	if !runtime.AppVolumeReplicationEnabled(app) {
		return fmt.Errorf("volume replication is disabled for app %s", app.ID)
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	sourceName := runtime.WorkspaceReplicationSourceName(app)
	source, found, err := client.getVolSyncReplicationSource(ctx, namespace, sourceName)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("workspace replication source %s/%s is not ready", namespace, sourceName)
	}
	if lastManualSync := nestedObjectString(source, "status", "lastManualSync"); lastManualSync == strings.TrimSpace(syncToken) {
		return nil
	}
	if err := client.patchVolSyncReplicationSourceTrigger(ctx, namespace, sourceName, syncToken); err != nil {
		return err
	}

	waitCtx, cancel := context.WithTimeout(ctx, s.Config.ManagedAppRolloutTimeout)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		source, found, err = client.getVolSyncReplicationSource(waitCtx, namespace, sourceName)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("workspace replication source %s/%s disappeared during failover", namespace, sourceName)
		}
		if nestedObjectString(source, "status", "lastManualSync") == strings.TrimSpace(syncToken) {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for workspace final sync %s/%s: %w", namespace, sourceName, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func nestedObjectString(raw map[string]any, keys ...string) string {
	var current any = raw
	for _, key := range keys {
		switch typed := current.(type) {
		case map[string]any:
			current = typed[key]
		default:
			return ""
		}
	}
	text, _ := current.(string)
	return strings.TrimSpace(text)
}

func appCurrentRuntimeID(app model.App) string {
	if runtimeID := strings.TrimSpace(app.Status.CurrentRuntimeID); runtimeID != "" {
		return runtimeID
	}
	return strings.TrimSpace(app.Spec.RuntimeID)
}

func operationInFlight(op model.Operation) bool {
	switch op.Status {
	case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
		return true
	default:
		return false
	}
}

func (s *Service) queueAutomaticFailovers() error {
	apps, err := s.Store.ListApps("", true)
	if err != nil {
		return err
	}
	runtimes, err := s.Store.ListRuntimes("", true)
	if err != nil {
		return err
	}
	operations, err := s.Store.ListOperations("", true)
	if err != nil {
		return err
	}

	runtimeByID := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		runtimeByID[runtimeObj.ID] = runtimeObj
	}
	inFlightByAppID := make(map[string]struct{})
	for _, op := range operations {
		if operationInFlight(op) {
			inFlightByAppID[op.AppID] = struct{}{}
		}
	}

	for _, app := range apps {
		if app.Spec.Failover == nil || !app.Spec.Failover.Auto {
			continue
		}
		if _, exists := inFlightByAppID[app.ID]; exists {
			continue
		}
		currentRuntime, ok := runtimeByID[appCurrentRuntimeID(app)]
		if !ok || currentRuntime.Status != model.RuntimeStatusOffline {
			continue
		}
		targetRuntimeID := strings.TrimSpace(app.Spec.Failover.TargetRuntimeID)
		targetRuntime, ok := runtimeByID[targetRuntimeID]
		if !ok || targetRuntime.Status != model.RuntimeStatusActive {
			continue
		}
		if targetRuntimeID == currentRuntime.ID {
			continue
		}
		if _, err := s.Store.CreateOperation(model.Operation{
			TenantID:        app.TenantID,
			Type:            model.OperationTypeFailover,
			RequestedByType: model.ActorTypeBootstrap,
			RequestedByID:   model.OperationRequestedByAutoFailover,
			AppID:           app.ID,
			TargetRuntimeID: targetRuntimeID,
		}); err != nil {
			s.Logger.Printf("auto failover queue for app %s failed: %v", app.ID, err)
		}
	}
	return nil
}

func (s *Service) executeManagedFailoverOperation(ctx context.Context, op model.Operation, app model.App) error {
	sourceRuntimeID := strings.TrimSpace(op.SourceRuntimeID)
	if sourceRuntimeID == "" {
		sourceRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
	if sourceRuntimeID == "" || targetRuntimeID == "" || sourceRuntimeID == targetRuntimeID {
		return fmt.Errorf("invalid failover operation source=%q target=%q", sourceRuntimeID, targetRuntimeID)
	}

	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes failover client: %w", err)
	}
	if _, err := s.acquireAppFenceLease(ctx, client, app, op.ID); err != nil {
		return fmt.Errorf("acquire app fence lease: %w", err)
	}
	if err := s.ensureWorkspaceFinalSync(ctx, client, app, op.ID); err != nil {
		return fmt.Errorf("ensure workspace final sync: %w", err)
	}

	originalReplicas := app.Spec.Replicas
	if originalReplicas > 0 {
		fencedApp := app
		fencedApp.Spec.RuntimeID = sourceRuntimeID
		fencedApp.Spec.Replicas = 0
		scheduling, err := s.managedSchedulingConstraints(fencedApp.Spec.RuntimeID)
		if err != nil {
			return err
		}
		if s.Config.KubectlApply {
			if err := s.applyManagedAppDesiredState(ctx, fencedApp, scheduling); err != nil {
				return fmt.Errorf("apply fenced managed app state %s: %w", app.ID, err)
			}
			if err := s.waitForManagedAppRollout(ctx, fencedApp, ""); err != nil {
				return fmt.Errorf("wait for fenced managed app rollout %s: %w", app.ID, err)
			}
		}
	}

	if _, err := s.acquireAppFenceLease(ctx, client, app, op.ID); err != nil {
		return fmt.Errorf("renew app fence lease: %w", err)
	}

	failedOverApp := app
	failedOverSpec := store.FailoverDesiredSpec(app, targetRuntimeID)
	if failedOverSpec == nil {
		return fmt.Errorf("build failover desired spec for app %s", app.ID)
	}
	failedOverSpec.Replicas = originalReplicas
	failedOverApp.Spec = *failedOverSpec
	failedOverApp, err = store.OverlayDesiredManagedPostgres(failedOverApp)
	if err != nil {
		return fmt.Errorf("overlay managed postgres failover state for app %s: %w", app.ID, err)
	}
	scheduling, err := s.managedSchedulingConstraints(failedOverApp.Spec.RuntimeID)
	if err != nil {
		return err
	}
	bundle, err := s.Renderer.RenderManagedAppBundle(failedOverApp, scheduling)
	if err != nil {
		return fmt.Errorf("render failover manifest for app %s: %w", app.ID, err)
	}
	if s.Config.KubectlApply {
		if err := s.applyManagedAppDesiredState(ctx, failedOverApp, scheduling); err != nil {
			return fmt.Errorf("apply failed-over managed app state %s: %w", app.ID, err)
		}
		if err := s.waitForManagedAppRollout(ctx, failedOverApp, ""); err != nil {
			return fmt.Errorf("wait for failed-over managed app rollout %s: %w", app.ID, err)
		}
	}

	message := fmt.Sprintf("managed app failed over from runtime %s to %s", sourceRuntimeID, targetRuntimeID)
	if _, err := s.Store.CompleteManagedOperationWithResult(op.ID, bundle.ManifestPath, message, failedOverSpec, nil); err != nil {
		return fmt.Errorf("complete failover operation %s: %w", op.ID, err)
	}
	s.Logger.Printf("operation %s completed failover from %s to %s", op.ID, sourceRuntimeID, targetRuntimeID)
	return nil
}
