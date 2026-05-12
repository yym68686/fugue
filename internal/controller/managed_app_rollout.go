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
	app = s.Renderer.PrepareApp(app)
	scheduling, err := s.managedSchedulingConstraintsForApp(ctx, app)
	if err != nil {
		return err
	}
	expectedReleaseKey := expectedManagedAppReleaseKey(app, scheduling)
	expectedImage := strings.TrimSpace(app.Spec.Image)

	waitCtx, cancel := context.WithTimeout(ctx, s.Config.ManagedAppRolloutTimeout)
	defer cancel()

	interval := 2 * time.Second
	if s.Config.PollInterval > interval {
		interval = s.Config.PollInterval
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	name := runtime.RuntimeAppResourceName(app)
	managedAppName := runtime.ManagedAppResourceName(app)
	backingServices, err := s.managedBackingServiceRolloutTargets(waitCtx, app)
	if err != nil {
		return fmt.Errorf("resolve backing service rollout targets for %s/%s: %w", namespace, name, err)
	}
	lastMessage := ""
	waitForNextSignal := func(targets []kubeWatchTarget) error {
		if err := client.waitForAnyObjectEvent(waitCtx, targets, interval); err != nil {
			if lastMessage != "" {
				return fmt.Errorf("wait for deployment rollout %s/%s: %w (%s)", namespace, name, err, lastMessage)
			}
			return fmt.Errorf("wait for deployment rollout %s/%s: %w", namespace, name, err)
		}
		return nil
	}

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

		ready, message, err := deploymentRolloutReady(deployment, found, app.Spec.Replicas, name, expectedReleaseKey, expectedImage)
		if err != nil {
			return err
		}
		if ready {
			if schedulingReady, schedulingMessage := deploymentSchedulingReady(deployment, scheduling); !schedulingReady {
				ready = false
				message = schedulingMessage
			}
		}
		watchTargets := rolloutWatchTargets(namespace, name, deployment, found)
		managed, foundManagedApp, err := client.getManagedApp(waitCtx, namespace, managedAppName)
		if err != nil {
			return fmt.Errorf("read managed app rollout for %s/%s: %w", namespace, managedAppName, err)
		}
		watchTargets = append(watchTargets, managedAppRolloutWatchTargets(namespace, managedAppName, managed, foundManagedApp)...)
		if ready {
			backingServicesReady, backingServiceMessage, backingServiceWatchTargets, err := s.managedBackingServicesRolloutReady(waitCtx, client, namespace, backingServices)
			if err != nil {
				return err
			}
			if !backingServicesReady {
				if strings.TrimSpace(backingServiceMessage) != "" {
					lastMessage = strings.TrimSpace(backingServiceMessage)
				}
				watchTargets = append(watchTargets, backingServiceWatchTargets...)
				if err := waitForNextSignal(watchTargets); err != nil {
					return err
				}
				continue
			}
			if err := s.cleanupStrandedManagedAppPods(waitCtx, client, namespace, app); err != nil && s.Logger != nil {
				s.Logger.Printf("cleanup stranded managed app pods after rollout failed for %s/%s: %v", namespace, name, err)
			}
			if s.Store != nil {
				if err := s.refreshManagedAppStatus(waitCtx, client, app); err != nil {
					s.Logger.Printf("refresh managed app status after rollout failed for %s/%s: %v", namespace, name, err)
				}
			}
			return nil
		}
		if failureMessage := managedAppRolloutFailure(managed, foundManagedApp); failureMessage != "" {
			return fmt.Errorf("managed app %s/%s rollout failed: %s", namespace, managedAppName, failureMessage)
		}
		if strings.TrimSpace(message) != "" {
			lastMessage = strings.TrimSpace(message)
		}

		if err := waitForNextSignal(watchTargets); err != nil {
			return err
		}
	}
}

func (s *Service) managedBackingServiceRolloutTargets(ctx context.Context, app model.App) ([]runtime.ManagedBackingServiceDeployment, error) {
	postgresPlacements, err := s.managedPostgresPlacements(ctx, app)
	if err != nil {
		return nil, fmt.Errorf("resolve postgres placements: %w", err)
	}
	scheduling, err := s.managedSchedulingConstraintsForApp(ctx, app)
	if err != nil {
		return nil, err
	}
	return runtime.ManagedBackingServiceDeploymentsWithPlacements(app, scheduling, postgresPlacements), nil
}

func rolloutWatchTargets(namespace, name string, deployment kubeDeployment, found bool) []kubeWatchTarget {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	target := kubeWatchTarget{
		apiPath:       deploymentCollectionAPIPath(namespace),
		fieldSelector: "metadata.name=" + strings.TrimSpace(name),
	}
	if found {
		target.resourceVersion = strings.TrimSpace(deployment.Metadata.ResourceVersion)
	}
	return []kubeWatchTarget{target}
}

func managedAppRolloutWatchTargets(namespace, name string, managed runtime.ManagedAppObject, found bool) []kubeWatchTarget {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	target := kubeWatchTarget{
		apiPath:       managedAppCollectionAPIPath(namespace),
		fieldSelector: "metadata.name=" + strings.TrimSpace(name),
	}
	if found {
		target.resourceVersion = strings.TrimSpace(managed.Metadata.ResourceVersion)
	}
	return []kubeWatchTarget{target}
}

func cloudNativePGClusterRolloutWatchTargets(namespace, name string, cluster kubeCloudNativePGCluster, found bool) []kubeWatchTarget {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	target := kubeWatchTarget{
		apiPath:       cloudNativePGClusterCollectionAPIPath(namespace),
		fieldSelector: "metadata.name=" + strings.TrimSpace(name),
	}
	if found {
		target.resourceVersion = strings.TrimSpace(cluster.Metadata.ResourceVersion)
	}
	return []kubeWatchTarget{target}
}

func (s *Service) managedBackingServicesRolloutReady(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	deployments []runtime.ManagedBackingServiceDeployment,
) (bool, string, []kubeWatchTarget, error) {
	watchTargets := make([]kubeWatchTarget, 0, len(deployments))
	for _, deployment := range deployments {
		switch deployment.ResourceKind {
		case runtime.CloudNativePGClusterKind:
			cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, deployment.ResourceName)
			if err != nil {
				return false, "", watchTargets, fmt.Errorf("read backing service cluster %s/%s: %w", namespace, deployment.ResourceName, err)
			}
			watchTargets = append(watchTargets, cloudNativePGClusterRolloutWatchTargets(namespace, deployment.ResourceName, cluster, found)...)
			if err := s.cleanupStrandedManagedPostgresPods(ctx, client, namespace, deployment.ResourceName); err != nil && s.Logger != nil {
				s.Logger.Printf("cleanup stranded managed postgres pods for %s/%s failed: %v", namespace, deployment.ResourceName, err)
			}
			ready, message := managedBackingServiceClusterRolloutReady(deployment.ResourceName, cluster, found)
			if !ready {
				return false, message, watchTargets, nil
			}
		default:
			status, found, err := client.getDeployment(ctx, namespace, deployment.ResourceName)
			if err != nil {
				return false, "", watchTargets, fmt.Errorf("read backing service deployment %s/%s: %w", namespace, deployment.ResourceName, err)
			}
			watchTargets = append(watchTargets, rolloutWatchTargets(namespace, deployment.ResourceName, status, found)...)
			ready, message, err := deploymentRolloutReady(status, found, 1, deployment.ResourceName, "", "")
			if err != nil {
				return false, "", watchTargets, fmt.Errorf("wait for backing service deployment %s/%s: %w", namespace, deployment.ResourceName, err)
			}
			if !ready {
				return false, message, watchTargets, nil
			}
		}
	}
	return true, "", watchTargets, nil
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
	if cluster.Status.ReadyInstances < 1 {
		return false, fmt.Sprintf(
			"waiting for backing service cluster %s primary readiness (%d/%d ready instances)",
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
	if cluster.Status.ReadyInstances > desiredInstances {
		return false, fmt.Sprintf(
			"waiting for backing service cluster %s to settle (%d/%d ready instances)",
			clusterName,
			cluster.Status.ReadyInstances,
			desiredInstances,
		)
	}
	if cluster.Status.ReadyInstances < desiredInstances {
		return true, fmt.Sprintf(
			"backing service cluster %s primary ready (%d/%d instances); remaining replicas recovering",
			clusterName,
			cluster.Status.ReadyInstances,
			desiredInstances,
		)
	}
	return true, fmt.Sprintf("backing service cluster %s ready", clusterName)
}

func (s *Service) cleanupStrandedManagedAppPods(ctx context.Context, client *kubeClient, namespace string, app model.App) error {
	return s.cleanupStrandedPodsBySelector(ctx, client, namespace, managedAppPodLabelSelector(app), "managed app")
}

func (s *Service) cleanupStrandedManagedPostgresPods(ctx context.Context, client *kubeClient, namespace, clusterName string) error {
	clusterName = strings.TrimSpace(clusterName)
	if clusterName == "" {
		return nil
	}
	return s.cleanupStrandedPodsBySelector(ctx, client, namespace, fmt.Sprintf(managedPostgresPodSelectorTemplate, clusterName), "managed postgres")
}

func (s *Service) cleanupStrandedPodsBySelector(ctx context.Context, client *kubeClient, namespace, selector, resourceLabel string) error {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return nil
	}

	pods, err := client.listPodsBySelector(ctx, namespace, selector)
	if err != nil {
		return err
	}

	nodeReadyCache := make(map[string]bool)
	nodeKnownCache := make(map[string]bool)
	for _, pod := range pods {
		if strings.TrimSpace(pod.Metadata.DeletionTimestamp) == "" {
			continue
		}
		nodeName := strings.TrimSpace(pod.Spec.NodeName)
		if nodeName == "" {
			continue
		}
		ready, known, err := podNodeReadyState(ctx, client, nodeName, nodeReadyCache, nodeKnownCache)
		if err != nil {
			return err
		}
		if known && ready {
			continue
		}
		if err := client.forceDeletePod(ctx, namespace, pod.Metadata.Name); err != nil {
			return err
		}
		if s.Logger != nil {
			s.Logger.Printf(
				"force deleted stranded %s pod %s/%s on unavailable node %s",
				strings.TrimSpace(resourceLabel),
				namespace,
				strings.TrimSpace(pod.Metadata.Name),
				nodeName,
			)
		}
	}
	return nil
}

func podNodeReadyState(
	ctx context.Context,
	client *kubeClient,
	nodeName string,
	readyCache map[string]bool,
	knownCache map[string]bool,
) (ready bool, known bool, err error) {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return false, false, nil
	}
	if cached, ok := readyCache[nodeName]; ok {
		return cached, knownCache[nodeName], nil
	}
	node, found, err := client.getNode(ctx, nodeName)
	if err != nil {
		return false, false, err
	}
	knownCache[nodeName] = found
	if !found {
		readyCache[nodeName] = false
		return false, false, nil
	}
	ready = kubeNodeReady(node)
	readyCache[nodeName] = ready
	return ready, true, nil
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
	if isBenignManagedAppRolloutFailureMessage(message) {
		return ""
	}
	if message != "" {
		return message
	}

	return "managed app reported an error"
}

func isBenignManagedAppRolloutFailureMessage(message string) bool {
	message = strings.ToLower(strings.TrimSpace(message))
	return strings.Contains(message, "exit_code=143") || strings.Contains(message, "exit code 143")
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

func deploymentRolloutReady(deployment kubeDeployment, found bool, desiredReplicas int, deploymentName, expectedReleaseKey, expectedImage string) (bool, string, error) {
	deploymentName = strings.TrimSpace(deploymentName)
	if deploymentName == "" {
		deploymentName = "deployment"
	}
	expectedReleaseKey = strings.TrimSpace(expectedReleaseKey)
	expectedImage = strings.TrimSpace(expectedImage)

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
	if expectedImage != "" {
		currentImage := deploymentPrimaryContainerImage(deployment)
		if currentImage != expectedImage {
			if currentImage == "" {
				return false, fmt.Sprintf("waiting for deployment %s image %s to be applied", deploymentName, expectedImage), nil
			}
			return false, fmt.Sprintf("waiting for deployment %s image %s to be applied (current=%s)", deploymentName, expectedImage, currentImage), nil
		}
	}
	if expectedReleaseKey != "" {
		currentReleaseKey := deploymentReleaseKey(deployment)
		if currentReleaseKey != expectedReleaseKey {
			if currentReleaseKey == "" {
				return false, fmt.Sprintf("waiting for deployment %s release %s to be applied", deploymentName, expectedReleaseKey), nil
			}
			return false, fmt.Sprintf("waiting for deployment %s release %s to be applied (current=%s)", deploymentName, expectedReleaseKey, currentReleaseKey), nil
		}
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

func expectedManagedAppReleaseKey(app model.App, scheduling runtime.SchedulingConstraints) string {
	if strings.TrimSpace(app.Spec.Image) == "" {
		return ""
	}
	return strings.TrimSpace(runtime.ManagedAppReleaseKey(app, scheduling))
}

func deploymentReleaseKey(deployment kubeDeployment) string {
	return strings.TrimSpace(deployment.Metadata.Annotations[runtime.FugueAnnotationReleaseKey])
}

func deploymentSchedulingReady(deployment kubeDeployment, expected runtime.SchedulingConstraints) (bool, string) {
	actualSelector := deployment.Spec.Template.Spec.NodeSelector
	if !stringMapsEqual(actualSelector, expected.NodeSelector) {
		return false, fmt.Sprintf("waiting for deployment %s nodeSelector to match runtime scheduling", strings.TrimSpace(deployment.Metadata.Name))
	}
	if !tolerationSetsEqual(deployment.Spec.Template.Spec.Tolerations, expected.Tolerations) {
		return false, fmt.Sprintf("waiting for deployment %s tolerations to match runtime scheduling", strings.TrimSpace(deployment.Metadata.Name))
	}
	return true, ""
}

func stringMapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		if strings.TrimSpace(right[key]) != strings.TrimSpace(leftValue) {
			return false
		}
	}
	return true
}

func tolerationSetsEqual(left, right []runtime.Toleration) bool {
	leftSet := tolerationSet(left)
	rightSet := tolerationSet(right)
	if len(leftSet) != len(rightSet) {
		return false
	}
	for key := range leftSet {
		if _, ok := rightSet[key]; !ok {
			return false
		}
	}
	return true
}

func tolerationSet(in []runtime.Toleration) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for _, item := range in {
		key := strings.Join([]string{
			strings.TrimSpace(item.Key),
			strings.TrimSpace(item.Operator),
			strings.TrimSpace(item.Value),
			strings.TrimSpace(item.Effect),
		}, "\x00")
		if key == "\x00\x00\x00" {
			continue
		}
		out[key] = struct{}{}
	}
	return out
}

func deploymentPrimaryContainerImage(deployment kubeDeployment) string {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if image := strings.TrimSpace(container.Image); image != "" {
			return image
		}
	}
	return ""
}
