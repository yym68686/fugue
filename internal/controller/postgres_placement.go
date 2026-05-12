package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"k8s.io/apimachinery/pkg/api/resource"
)

func (s *Service) managedPostgresPlacements(ctx context.Context, app model.App) (map[string][]runtimepkg.SchedulingConstraints, error) {
	placements := make(map[string][]runtimepkg.SchedulingConstraints)

	buildPlacements := func(serviceName, appRuntimeID string, spec model.AppPostgresSpec) error {
		serviceName = strings.TrimSpace(serviceName)
		if serviceName == "" {
			return nil
		}

		scheduling, err := s.managedPostgresServicePlacements(ctx, app, serviceName, appRuntimeID, spec)
		if err != nil {
			return err
		}
		if len(scheduling) > 0 {
			placements[serviceName] = scheduling
		}
		return nil
	}

	for _, service := range app.BackingServices {
		if service.Type != model.BackingServiceTypePostgres || service.Spec.Postgres == nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(service.Provisioner), model.BackingServiceProvisionerManaged) {
			continue
		}
		if err := buildPlacements(service.Spec.Postgres.ServiceName, app.Spec.RuntimeID, *service.Spec.Postgres); err != nil {
			return nil, err
		}
	}

	if app.Spec.Postgres != nil {
		if err := buildPlacements(app.Spec.Postgres.ServiceName, app.Spec.RuntimeID, *app.Spec.Postgres); err != nil {
			return nil, err
		}
	}

	if len(placements) == 0 {
		return nil, nil
	}
	return placements, nil
}

func (s *Service) managedPostgresServicePlacements(
	ctx context.Context,
	app model.App,
	serviceName, appRuntimeID string,
	spec model.AppPostgresSpec,
) ([]runtimepkg.SchedulingConstraints, error) {
	primaryRuntimeID := strings.TrimSpace(spec.RuntimeID)
	if primaryRuntimeID == "" {
		primaryRuntimeID = strings.TrimSpace(appRuntimeID)
	}
	targetRuntimeID := strings.TrimSpace(spec.FailoverTargetRuntimeID)

	var targetPlacement runtimepkg.SchedulingConstraints
	if targetRuntimeID != "" && targetRuntimeID != primaryRuntimeID {
		var err error
		targetPlacement, err = s.managedSchedulingConstraints(targetRuntimeID)
		if err != nil {
			return nil, fmt.Errorf("resolve postgres failover target runtime %s: %w", targetRuntimeID, err)
		}
	}

	primaryPlacement, err := s.managedPostgresPrimaryPlacement(ctx, app, serviceName, primaryRuntimeID, spec, targetPlacement)
	if err != nil {
		return nil, err
	}

	placements := make([]runtimepkg.SchedulingConstraints, 0, 2)
	appendPlacement := func(constraints runtimepkg.SchedulingConstraints) {
		if len(constraints.NodeSelector) == 0 && len(constraints.Tolerations) == 0 {
			return
		}
		for _, existing := range placements {
			if schedulingConstraintsEqual(existing, constraints) {
				return
			}
		}
		placements = append(placements, constraints)
	}

	appendPlacement(primaryPlacement)
	appendPlacement(targetPlacement)
	return placements, nil
}

func (s *Service) managedPostgresPrimaryPlacement(
	ctx context.Context,
	app model.App,
	serviceName, primaryRuntimeID string,
	spec model.AppPostgresSpec,
	targetPlacement runtimepkg.SchedulingConstraints,
) (runtimepkg.SchedulingConstraints, error) {
	if strings.TrimSpace(primaryRuntimeID) == "" {
		return runtimepkg.SchedulingConstraints{}, nil
	}

	runtimeObj, err := s.Store.GetRuntime(primaryRuntimeID)
	if err != nil {
		return runtimepkg.SchedulingConstraints{}, fmt.Errorf("load postgres primary runtime %s: %w", primaryRuntimeID, err)
	}

	primaryPlacement := runtimepkg.SchedulingForRuntime(runtimeObj)
	if runtimeObj.Type != model.RuntimeTypeManagedShared {
		return primaryPlacement, nil
	}

	exactPlacement, found, err := s.managedSharedPostgresPrimaryHostPlacement(ctx, app, serviceName, runtimeObj, spec, targetPlacement.NodeSelector)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf(
				"resolve shared postgres primary placement for app=%s service=%s runtime=%s failed, falling back to shared selector: %v",
				app.ID,
				serviceName,
				primaryRuntimeID,
				err,
			)
		}
		return primaryPlacement, nil
	}
	if found {
		return exactPlacement, nil
	}
	return primaryPlacement, nil
}

func (s *Service) managedSharedPostgresPrimaryHostPlacement(
	ctx context.Context,
	app model.App,
	serviceName string,
	sourceRuntime model.Runtime,
	spec model.AppPostgresSpec,
	targetSelector map[string]string,
) (runtimepkg.SchedulingConstraints, bool, error) {
	client, err := s.kubeClient()
	if err != nil {
		return runtimepkg.SchedulingConstraints{}, false, fmt.Errorf("initialize kubernetes client: %w", err)
	}

	sourceSelector := runtimepkg.ManagedSharedNodeSelector(sourceRuntime)
	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	if nodeName := strings.TrimSpace(spec.PrimaryNodeName); nodeName != "" {
		matchedNode, found, err := managedSharedNodeMatchingSelector(ctx, client, nodeName, sourceSelector)
		if err != nil {
			return runtimepkg.SchedulingConstraints{}, false, err
		}
		if !found {
			return runtimepkg.SchedulingConstraints{}, false, fmt.Errorf("postgres primary node %s does not match runtime %s", nodeName, sourceRuntime.ID)
		}
		node, found, err := client.getNode(ctx, matchedNode)
		if err != nil {
			return runtimepkg.SchedulingConstraints{}, false, fmt.Errorf("read kubernetes node %s: %w", matchedNode, err)
		}
		if !found || !managedSharedNodeSchedulable(node) {
			return runtimepkg.SchedulingConstraints{}, false, fmt.Errorf("postgres primary node %s is not schedulable", matchedNode)
		}
		return runtimepkg.SchedulingConstraints{
			NodeSelector: map[string]string{
				kubeHostnameLabelKey: matchedNode,
			},
		}, true, nil
	}
	if namespace != "" && serviceName != "" {
		cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, serviceName)
		if err != nil {
			return runtimepkg.SchedulingConstraints{}, false, fmt.Errorf("read cloudnativepg cluster %s/%s: %w", namespace, serviceName, err)
		}
		if found {
			if nodeName, found, err := matchingManagedSharedPostgresPrimaryNode(ctx, client, namespace, cluster.Status.CurrentPrimary, sourceSelector); err != nil {
				return runtimepkg.SchedulingConstraints{}, false, err
			} else if found {
				return runtimepkg.SchedulingConstraints{
					NodeSelector: map[string]string{
						kubeHostnameLabelKey: nodeName,
					},
				}, true, nil
			}
			if nodeName, found, err := matchingManagedSharedPostgresExistingInstanceNode(ctx, client, namespace, serviceName, sourceSelector, targetSelector); err != nil {
				return runtimepkg.SchedulingConstraints{}, false, err
			} else if found {
				return runtimepkg.SchedulingConstraints{
					NodeSelector: map[string]string{
						kubeHostnameLabelKey: nodeName,
					},
				}, true, nil
			}
		}
	}

	nodeRequestsByName, hasNodeRequests, err := managedSharedNodeRequestsByName(ctx, client)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf(
				"resolve shared postgres node request inventory for app=%s service=%s runtime=%s failed, continuing with allocatable-only placement: %v",
				app.ID,
				serviceName,
				sourceRuntime.ID,
				err,
			)
		}
		hasNodeRequests = false
	}
	postgresRequest := managedPostgresPlacementRequest(spec)

	nodeNames, err := client.listNodeNames(ctx)
	if err != nil {
		return runtimepkg.SchedulingConstraints{}, false, fmt.Errorf("list kubernetes nodes: %w", err)
	}
	sort.Strings(nodeNames)

	candidates := make([]managedSharedNodeCandidate, 0, len(nodeNames))
	for _, nodeName := range nodeNames {
		node, found, err := client.getNode(ctx, nodeName)
		if err != nil {
			return runtimepkg.SchedulingConstraints{}, false, fmt.Errorf("read kubernetes node %s: %w", nodeName, err)
		}
		if !found || !nodeLabelsMatchSelector(node.Metadata.Labels, sourceSelector) {
			continue
		}
		if !managedSharedNodeSchedulable(node) {
			continue
		}
		requested := nodeRequestsByName[nodeName]
		candidates = append(candidates, managedSharedNodeCandidate{
			nodeName:                 nodeName,
			overlapsTargetSelector:   len(targetSelector) > 0 && nodeLabelsMatchSelector(node.Metadata.Labels, targetSelector),
			allocatableEphemeralByte: parseKubeResourceBytes(node.Status.Allocatable["ephemeral-storage"]),
			allocatableMemoryBytes:   parseKubeResourceBytes(node.Status.Allocatable["memory"]),
			allocatableCPUMilli:      parseKubeResourceMilli(node.Status.Allocatable["cpu"]),
			requestedCPUMilli:        requested.cpuMilli,
			requestedMemoryBytes:     requested.memoryBytes,
			requestedEphemeralBytes:  requested.ephemeralBytes,
		})
	}

	if len(candidates) == 0 {
		return runtimepkg.SchedulingConstraints{}, false, nil
	}
	if hasNodeRequests {
		filtered := candidates[:0]
		for _, candidate := range candidates {
			if candidate.fits(postgresRequest) {
				filtered = append(filtered, candidate.withPostgresRequest(postgresRequest))
			}
		}
		if len(filtered) == 0 {
			return runtimepkg.SchedulingConstraints{}, false, nil
		}
		candidates = filtered
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if left.overlapsTargetSelector != right.overlapsTargetSelector {
			return !left.overlapsTargetSelector && right.overlapsTargetSelector
		}
		if hasNodeRequests {
			if left.remainingMemoryBytes != right.remainingMemoryBytes {
				return left.remainingMemoryBytes > right.remainingMemoryBytes
			}
			if left.remainingCPUMilli != right.remainingCPUMilli {
				return left.remainingCPUMilli > right.remainingCPUMilli
			}
		}
		if left.allocatableEphemeralByte != right.allocatableEphemeralByte {
			return left.allocatableEphemeralByte > right.allocatableEphemeralByte
		}
		if left.allocatableMemoryBytes != right.allocatableMemoryBytes {
			return left.allocatableMemoryBytes > right.allocatableMemoryBytes
		}
		if left.allocatableCPUMilli != right.allocatableCPUMilli {
			return left.allocatableCPUMilli > right.allocatableCPUMilli
		}
		return left.nodeName < right.nodeName
	})

	return runtimepkg.SchedulingConstraints{
		NodeSelector: map[string]string{
			kubeHostnameLabelKey: candidates[0].nodeName,
		},
	}, true, nil
}

type managedSharedNodeCandidate struct {
	nodeName                 string
	overlapsTargetSelector   bool
	allocatableEphemeralByte int64
	allocatableMemoryBytes   int64
	allocatableCPUMilli      int64
	requestedEphemeralBytes  int64
	requestedMemoryBytes     int64
	requestedCPUMilli        int64
	remainingMemoryBytes     int64
	remainingCPUMilli        int64
}

type managedSharedNodeRequests struct {
	cpuMilli       int64
	memoryBytes    int64
	ephemeralBytes int64
}

func (candidate managedSharedNodeCandidate) fits(request managedSharedNodeRequests) bool {
	if candidate.allocatableCPUMilli > 0 && candidate.requestedCPUMilli+request.cpuMilli > resourceCapacityWithRatio(candidate.allocatableCPUMilli, 1.5) {
		return false
	}
	if candidate.allocatableMemoryBytes > 0 && candidate.requestedMemoryBytes+request.memoryBytes > resourceCapacityWithRatio(candidate.allocatableMemoryBytes, 0.9) {
		return false
	}
	if candidate.allocatableEphemeralByte > 0 && candidate.requestedEphemeralBytes+request.ephemeralBytes > candidate.allocatableEphemeralByte {
		return false
	}
	return true
}

func (candidate managedSharedNodeCandidate) withPostgresRequest(request managedSharedNodeRequests) managedSharedNodeCandidate {
	candidate.remainingCPUMilli = resourceCapacityWithRatio(candidate.allocatableCPUMilli, 1.5) - candidate.requestedCPUMilli - request.cpuMilli
	candidate.remainingMemoryBytes = resourceCapacityWithRatio(candidate.allocatableMemoryBytes, 0.9) - candidate.requestedMemoryBytes - request.memoryBytes
	return candidate
}

func resourceCapacityWithRatio(value int64, ratio float64) int64 {
	if value <= 0 || ratio <= 0 {
		return value
	}
	return int64(float64(value) * ratio)
}

func managedSharedNodeSchedulable(node kubeNode) bool {
	if !kubeNodeReady(node) {
		return false
	}
	if node.Spec.Unschedulable {
		return false
	}
	if kubeNodeConditionTrue(node.Status.Conditions, "DiskPressure") {
		return false
	}
	for _, taint := range node.Spec.Taints {
		switch strings.TrimSpace(taint.Effect) {
		case "NoSchedule", "NoExecute":
			return false
		}
	}
	return true
}

func managedPostgresPlacementRequest(spec model.AppPostgresSpec) managedSharedNodeRequests {
	resources := model.DefaultManagedPostgresResources()
	if spec.Resources != nil {
		resources = *spec.Resources
	}
	return managedSharedNodeRequests{
		cpuMilli:    maxInt64(0, resources.CPUMilliCores),
		memoryBytes: maxInt64(0, resources.MemoryMebibytes) * 1024 * 1024,
	}
}

func managedSharedNodeRequestsByName(ctx context.Context, client *kubeClient) (map[string]managedSharedNodeRequests, bool, error) {
	pods, found, err := client.listAllPods(ctx)
	if err != nil || !found {
		return nil, false, err
	}
	out := make(map[string]managedSharedNodeRequests)
	for _, pod := range pods {
		nodeName := strings.TrimSpace(pod.Spec.NodeName)
		if nodeName == "" || managedPostgresPodFinished(pod) {
			continue
		}
		request := kubePodRequests(pod)
		current := out[nodeName]
		current.cpuMilli += request.cpuMilli
		current.memoryBytes += request.memoryBytes
		current.ephemeralBytes += request.ephemeralBytes
		out[nodeName] = current
	}
	return out, true, nil
}

func kubePodRequests(pod kubePod) managedSharedNodeRequests {
	regular := kubeContainerRequests(pod.Spec.Containers)
	init := kubeMaxContainerRequests(pod.Spec.InitContainers)
	return managedSharedNodeRequests{
		cpuMilli:       maxInt64(regular.cpuMilli, init.cpuMilli),
		memoryBytes:    maxInt64(regular.memoryBytes, init.memoryBytes),
		ephemeralBytes: maxInt64(regular.ephemeralBytes, init.ephemeralBytes),
	}
}

func kubeContainerRequests(containers []kubeContainerSpec) managedSharedNodeRequests {
	var total managedSharedNodeRequests
	for _, container := range containers {
		requests := container.Resources.Requests
		total.cpuMilli += parseKubeResourceMilli(requests["cpu"])
		total.memoryBytes += parseKubeResourceBytes(requests["memory"])
		total.ephemeralBytes += parseKubeResourceBytes(requests["ephemeral-storage"])
	}
	return total
}

func kubeMaxContainerRequests(containers []kubeContainerSpec) managedSharedNodeRequests {
	var total managedSharedNodeRequests
	for _, container := range containers {
		requests := container.Resources.Requests
		total.cpuMilli = maxInt64(total.cpuMilli, parseKubeResourceMilli(requests["cpu"]))
		total.memoryBytes = maxInt64(total.memoryBytes, parseKubeResourceBytes(requests["memory"]))
		total.ephemeralBytes = maxInt64(total.ephemeralBytes, parseKubeResourceBytes(requests["ephemeral-storage"]))
	}
	return total
}

func kubeNodeConditionTrue(conditions []kubeNodeCondition, conditionType string) bool {
	for _, condition := range conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), strings.TrimSpace(conditionType)) {
			return strings.EqualFold(strings.TrimSpace(condition.Status), "True")
		}
	}
	return false
}

func parseKubeResourceMilli(value string) int64 {
	quantity, err := resource.ParseQuantity(strings.TrimSpace(value))
	if err != nil {
		return 0
	}
	return quantity.MilliValue()
}

func parseKubeResourceBytes(value string) int64 {
	quantity, err := resource.ParseQuantity(strings.TrimSpace(value))
	if err != nil {
		return 0
	}
	return quantity.Value()
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func matchingManagedSharedPostgresPrimaryNode(
	ctx context.Context,
	client *kubeClient,
	namespace, podName string,
	sourceSelector map[string]string,
) (string, bool, error) {
	podName = strings.TrimSpace(podName)
	if podName == "" {
		return "", false, nil
	}

	pod, found, err := client.getPod(ctx, namespace, podName)
	if err != nil {
		return "", false, fmt.Errorf("read postgres pod %s/%s: %w", namespace, podName, err)
	}
	if !found {
		return "", false, nil
	}

	nodeName := strings.TrimSpace(pod.Spec.NodeName)
	if nodeName == "" {
		return "", false, nil
	}

	node, found, err := client.getNode(ctx, nodeName)
	if err != nil {
		return "", false, fmt.Errorf("read kubernetes node %s: %w", nodeName, err)
	}
	if !found || !nodeLabelsMatchSelector(node.Metadata.Labels, sourceSelector) {
		return "", false, nil
	}
	return nodeName, true, nil
}

func matchingManagedSharedPostgresExistingInstanceNode(
	ctx context.Context,
	client *kubeClient,
	namespace, clusterName string,
	sourceSelector, targetSelector map[string]string,
) (string, bool, error) {
	clusterName = strings.TrimSpace(clusterName)
	if clusterName == "" {
		return "", false, nil
	}

	pods, err := client.listPodsBySelector(
		ctx,
		namespace,
		fmt.Sprintf(managedPostgresPodSelectorTemplate, clusterName),
	)
	if err != nil {
		return "", false, fmt.Errorf("list postgres pods for cluster %s: %w", clusterName, err)
	}
	for _, pod := range pods {
		if managedPostgresPodFinished(pod) {
			continue
		}
		if nodeName, found, err := managedSharedNodeMatchingSelector(ctx, client, pod.Spec.NodeName, sourceSelector); err != nil {
			return "", false, err
		} else if found {
			if overlaps, err := nodeMatchesSelector(ctx, client, nodeName, targetSelector); err != nil {
				return "", false, err
			} else if overlaps {
				continue
			}
			return nodeName, true, nil
		}

		pvcName := managedPostgresPVCNameForPod(pod)
		if pvcName == "" {
			pvcName = strings.TrimSpace(pod.Metadata.Name)
		}
		if nodeName, found, err := managedSharedPostgresPVCNode(ctx, client, namespace, pvcName, sourceSelector); err != nil {
			return "", false, err
		} else if found {
			if overlaps, err := nodeMatchesSelector(ctx, client, nodeName, targetSelector); err != nil {
				return "", false, err
			} else if overlaps {
				continue
			}
			return nodeName, true, nil
		}
	}

	pvcNames, err := client.listPersistentVolumeClaimNamesByLabel(ctx, namespace, "cnpg.io/cluster="+clusterName)
	if err != nil {
		return "", false, fmt.Errorf("list postgres pvcs for cluster %s: %w", clusterName, err)
	}
	sort.Strings(pvcNames)
	for _, pvcName := range pvcNames {
		if nodeName, found, err := managedSharedPostgresPVCNode(ctx, client, namespace, pvcName, sourceSelector); err != nil {
			return "", false, err
		} else if found {
			if overlaps, err := nodeMatchesSelector(ctx, client, nodeName, targetSelector); err != nil {
				return "", false, err
			} else if overlaps {
				continue
			}
			return nodeName, true, nil
		}
	}
	for _, pvcName := range pvcNames {
		if nodeName, found, err := managedPostgresPVCNode(ctx, client, namespace, pvcName); err != nil {
			return "", false, err
		} else if found {
			node, found, err := client.getNode(ctx, nodeName)
			if err != nil {
				return "", false, fmt.Errorf("read kubernetes node %s: %w", nodeName, err)
			}
			if found && managedSharedNodeSchedulable(node) && (len(targetSelector) == 0 || !nodeLabelsMatchSelector(node.Metadata.Labels, targetSelector)) {
				return nodeName, true, nil
			}
		}
	}
	return "", false, nil
}

func managedPostgresPodFinished(pod kubePod) bool {
	phase := strings.TrimSpace(pod.Status.Phase)
	return strings.EqualFold(phase, "Succeeded") || strings.EqualFold(phase, "Failed")
}

func managedSharedPostgresPVCNode(
	ctx context.Context,
	client *kubeClient,
	namespace, pvcName string,
	sourceSelector map[string]string,
) (string, bool, error) {
	pvcName = strings.TrimSpace(pvcName)
	if pvcName == "" {
		return "", false, nil
	}

	nodeName, found, err := managedPostgresPVCNode(ctx, client, namespace, pvcName)
	if err != nil {
		return "", false, err
	}
	if !found {
		return "", false, nil
	}
	return managedSharedNodeMatchingSelector(ctx, client, nodeName, sourceSelector)
}

func managedPostgresPVCNode(
	ctx context.Context,
	client *kubeClient,
	namespace, pvcName string,
) (string, bool, error) {
	pvcName = strings.TrimSpace(pvcName)
	if pvcName == "" {
		return "", false, nil
	}

	pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, pvcName)
	if err != nil {
		return "", false, fmt.Errorf("read postgres pvc %s/%s: %w", namespace, pvcName, err)
	}
	if !found {
		return "", false, nil
	}

	if nodeName := strings.TrimSpace(pvc.Metadata.Annotations[pvcSelectedNodeAnnotation]); nodeName != "" {
		return nodeName, true, nil
	}

	volumeName := strings.TrimSpace(pvc.Spec.VolumeName)
	if volumeName == "" {
		return "", false, nil
	}
	pv, found, err := client.getPersistentVolume(ctx, volumeName)
	if err != nil {
		if strings.Contains(err.Error(), "status=403") {
			return "", false, nil
		}
		return "", false, fmt.Errorf("read persistent volume %s for pvc %s/%s: %w", volumeName, namespace, pvcName, err)
	}
	if !found {
		return "", false, nil
	}
	if nodeName := persistentVolumeNodeName(pv); nodeName != "" {
		return nodeName, true, nil
	}
	return "", false, nil
}

func managedSharedNodeMatchingSelector(
	ctx context.Context,
	client *kubeClient,
	nodeName string,
	sourceSelector map[string]string,
) (string, bool, error) {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return "", false, nil
	}

	node, found, err := client.getNode(ctx, nodeName)
	if err != nil {
		return "", false, fmt.Errorf("read kubernetes node %s: %w", nodeName, err)
	}
	if !found || !nodeLabelsMatchSelector(node.Metadata.Labels, sourceSelector) {
		return "", false, nil
	}
	return nodeName, true, nil
}

func nodeMatchesSelector(ctx context.Context, client *kubeClient, nodeName string, selector map[string]string) (bool, error) {
	if len(selector) == 0 {
		return false, nil
	}
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return false, nil
	}
	node, found, err := client.getNode(ctx, nodeName)
	if err != nil {
		return false, fmt.Errorf("read kubernetes node %s: %w", nodeName, err)
	}
	if !found {
		return false, nil
	}
	return nodeLabelsMatchSelector(node.Metadata.Labels, selector), nil
}

func schedulingConstraintsEqual(left, right runtimepkg.SchedulingConstraints) bool {
	if len(left.NodeSelector) != len(right.NodeSelector) || len(left.Tolerations) != len(right.Tolerations) {
		return false
	}
	for key, value := range left.NodeSelector {
		if right.NodeSelector[key] != value {
			return false
		}
	}
	for index := range left.Tolerations {
		if left.Tolerations[index] != right.Tolerations[index] {
			return false
		}
	}
	return true
}
