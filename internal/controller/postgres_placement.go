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

	primaryPlacement, err := s.managedPostgresPrimaryPlacement(ctx, app, serviceName, primaryRuntimeID, targetPlacement)
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

	exactPlacement, found, err := s.managedSharedPostgresPrimaryHostPlacement(ctx, app, serviceName, runtimeObj, targetPlacement.NodeSelector)
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
	targetSelector map[string]string,
) (runtimepkg.SchedulingConstraints, bool, error) {
	client, err := s.kubeClient()
	if err != nil {
		return runtimepkg.SchedulingConstraints{}, false, fmt.Errorf("initialize kubernetes client: %w", err)
	}

	sourceSelector := runtimepkg.ManagedSharedNodeSelector(sourceRuntime)
	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
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
		}
	}

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
		candidates = append(candidates, managedSharedNodeCandidate{
			nodeName:                 nodeName,
			overlapsTargetSelector:   len(targetSelector) > 0 && nodeLabelsMatchSelector(node.Metadata.Labels, targetSelector),
			allocatableEphemeralByte: parseKubeResourceBytes(node.Status.Allocatable["ephemeral-storage"]),
			allocatableMemoryBytes:   parseKubeResourceBytes(node.Status.Allocatable["memory"]),
			allocatableCPUMilli:      parseKubeResourceMilli(node.Status.Allocatable["cpu"]),
		})
	}

	if len(candidates) == 0 {
		return runtimepkg.SchedulingConstraints{}, false, nil
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if left.overlapsTargetSelector != right.overlapsTargetSelector {
			return !left.overlapsTargetSelector && right.overlapsTargetSelector
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
