package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
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

func (s *Service) appHasActiveOperation(app model.App) (bool, error) {
	if strings.TrimSpace(app.ID) == "" {
		return false, nil
	}

	ops, err := s.Store.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil {
		return false, err
	}
	for _, op := range ops {
		switch op.Status {
		case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
			return true, nil
		}
	}
	return false, nil
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
	if appID := strings.TrimSpace(app.ID); appID == "" {
		return s.cleanupOrphanManagedApp(ctx, client, namespace, managed, app, "orphaned managed app: spec.appID is empty")
	} else if storedApp, err := s.Store.GetApp(appID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return s.cleanupOrphanManagedApp(ctx, client, namespace, managed, app, "orphaned managed app: app not found in store")
		}
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("read app from store: %w", err))
	} else {
		hasActiveOp, opErr := s.appHasActiveOperation(storedApp)
		switch {
		case opErr != nil:
			if s.Logger != nil {
				s.Logger.Printf("skip active operation check for managed app %s: %v", app.ID, opErr)
			}
			backfillManagedAppSource(&app, storedApp)
		case hasActiveOp:
			backfillManagedAppSource(&app, storedApp)
		default:
			app = storedApp
			if observedApp, changed, syncErr := s.observedManagedPostgresDesiredApp(ctx, storedApp); syncErr != nil {
				if s.Logger != nil {
					s.Logger.Printf("skip observed managed postgres sync for app %s: %v", app.ID, syncErr)
				}
			} else if changed {
				app = observedApp
			}
		}
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
	postgresPlacements, err := s.managedPostgresPlacements(ctx, app)
	if err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("resolve postgres placements: %w", err))
	}
	childObjects := runtime.BuildManagedAppChildObjectsWithPlacements(app, managed.Spec.Scheduling, postgresPlacements, ownerRef)
	fenceEpoch, err := s.currentAppFenceEpoch(ctx, client, app)
	if err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("read app fence epoch: %w", err))
	}
	decorateManagedAppObjectsWithFenceEpoch(childObjects, app, fenceEpoch)
	if err := client.applyObjects(ctx, childObjects); err != nil {
		return patchManagedAppErrorStatus(ctx, client, namespace, managed, app, fmt.Errorf("apply managed app child objects: %w", err))
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

	status := buildManagedAppStatus(managed, app, deployment, found, appPods, backingServiceStatuses)
	if err := client.patchManagedAppStatus(ctx, namespace, managed.Metadata.Name, status); err != nil {
		return fmt.Errorf("patch managed app status for %s/%s: %w", namespace, managed.Metadata.Name, err)
	}
	if err := s.Store.SyncManagedAppRuntimeStatus(app.ID, managedStatusTimePointer(status.CurrentReleaseStartedAt), managedStatusTimePointer(status.CurrentReleaseReadyAt), backingServiceRuntimeStatuses(status.BackingServices)); err != nil {
		return fmt.Errorf("sync managed app runtime status for %s: %w", app.ID, err)
	}
	return nil
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

func buildManagedAppStatus(managed runtime.ManagedAppObject, app model.App, deployment kubeDeployment, found bool, pods []kubePod, backingServiceStatuses []runtime.ManagedBackingServiceStatus) runtime.ManagedAppStatus {
	status := managedAppBaseStatus(managed, app)
	if found {
		status.ReadyReplicas = maxInt(deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas)
		status.Conditions = append([]runtime.ManagedAppCondition(nil), deployment.Status.Conditions...)
	}
	status.BackingServices = append([]runtime.ManagedBackingServiceStatus(nil), backingServiceStatuses...)
	podFailureCutoff, allowPodFailure := managedAppPodFailureCutoff(managed.Status, app, managed.Spec.Scheduling)
	podFailureMessage := ""
	if allowPodFailure {
		podFailureMessage = managedAppPodFailureMessage(pods, podFailureCutoff)
	}

	switch {
	case app.Spec.Replicas <= 0:
		status.Phase = runtime.ManagedAppPhaseDisabled
		status.Message = "desired replicas set to 0"
	case !found:
		status.Phase = runtime.ManagedAppPhasePending
		status.Message = fmt.Sprintf("waiting for deployment %s", runtime.RuntimeAppResourceName(app))
	case status.ReadyReplicas >= app.Spec.Replicas:
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
	default:
		status.Phase = runtime.ManagedAppPhaseProgressing
		status.Message = fmt.Sprintf("deployment progressing (%d/%d ready replicas)", status.ReadyReplicas, app.Spec.Replicas)
	}
	applyManagedAppReleaseStatus(&status, managed.Status, app, managed.Spec.Scheduling)
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

func managedAppPodFailureCutoff(previous runtime.ManagedAppStatus, app model.App, scheduling runtime.SchedulingConstraints) (*time.Time, bool) {
	releaseKey := strings.TrimSpace(runtime.ManagedAppReleaseKey(app, scheduling))
	if releaseKey == "" {
		return nil, true
	}

	if strings.TrimSpace(previous.PendingReleaseKey) == releaseKey {
		return parseManagedAppStatusTimestamp(previous.PendingReleaseStartedAt), true
	}

	currentKey := strings.TrimSpace(previous.CurrentReleaseKey)
	if currentKey != "" && currentKey != releaseKey {
		return nil, false
	}
	if currentKey == releaseKey {
		return parseManagedAppStatusTimestamp(previous.CurrentReleaseStartedAt), true
	}

	return nil, true
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
		if summary := summarizeManagedAppPodFailure(pod); summary != "" {
			return summary
		}
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

	statuses := append([]kubeContainerStatus(nil), pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	for _, status := range statuses {
		if status.State.Terminated != nil && isFailingManagedAppTermination(*status.State.Terminated) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "terminated", *status.State.Terminated)
		}
		if status.LastState.Terminated != nil && isFailingManagedAppTermination(*status.LastState.Terminated) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "terminated", *status.LastState.Terminated)
		}
		if status.State.Waiting != nil && isFailingManagedAppWaitingReason(status.State.Waiting.Reason) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "waiting", *status.State.Waiting)
		}
		if status.LastState.Waiting != nil && isFailingManagedAppWaitingReason(status.LastState.Waiting.Reason) {
			return summarizeManagedAppContainerFailure(prefix, status.Name, "waiting", *status.LastState.Waiting)
		}
	}

	phase := strings.TrimSpace(pod.Status.Phase)
	if strings.EqualFold(phase, "Failed") {
		return fmt.Sprintf("%s failed with phase %s", prefix, phase)
	}
	return ""
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

func isFailingManagedAppWaitingReason(reason string) bool {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "crashloopbackoff", "createcontainerconfigerror", "createcontainererror", "runcontainererror", "imagepullbackoff", "errimagepull", "invalidimagename", "errimageneverpull":
		return true
	default:
		return false
	}
}

func isFailingManagedAppTermination(detail kubeStateDetail) bool {
	reason := strings.TrimSpace(detail.Reason)
	if reason == "" {
		return detail.ExitCode != 0
	}
	return !strings.EqualFold(reason, "Completed")
}

func applyManagedAppReleaseStatus(status *runtime.ManagedAppStatus, previous runtime.ManagedAppStatus, app model.App, scheduling runtime.SchedulingConstraints) {
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

	releaseKey := strings.TrimSpace(runtime.ManagedAppReleaseKey(app, scheduling))
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
	if pendingKey == releaseKey && pendingStartedAt != "" {
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
	sourceCopy := *stored.Source
	if len(stored.Source.ComposeDependsOn) > 0 {
		sourceCopy.ComposeDependsOn = append([]string(nil), stored.Source.ComposeDependsOn...)
	}
	app.Source = &sourceCopy
}

func managedAppExpectedObjectNamesByKind(app model.App) map[string]map[string]struct{} {
	out := desiredObjectNamesByKind(runtime.BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil))
	if app.Spec.Workspace != nil || app.Spec.PersistentStorage != nil {
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
