package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

type rolloutCapacityCandidate struct {
	nodeName               string
	remainingCPUMilli      int64
	remainingMemoryBytes   int64
	remainingEphemeralByte int64
}

func (s *Service) preflightManagedAppZeroDowntimeRolloutCapacity(ctx context.Context, app model.App, scheduling runtimepkg.SchedulingConstraints) error {
	if s == nil || app.Spec.Replicas <= 0 {
		return nil
	}
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes capacity preflight client: %w", err)
	}
	app = s.Renderer.PrepareApp(app)
	deployment, found := expectedManagedAppDeployment(app, scheduling)
	if !found {
		return nil
	}
	message, err := zeroDowntimeRolloutCapacityBlockMessage(ctx, client, deployment, app.Spec.Replicas)
	if err != nil {
		return err
	}
	if strings.TrimSpace(message) != "" {
		return fmt.Errorf("%s", message)
	}
	return nil
}

func zeroDowntimeRolloutCapacityBlockMessage(ctx context.Context, client *kubeClient, deployment kubeDeployment, desiredReplicas int) (string, error) {
	if client == nil || desiredReplicas <= 0 || !deploymentNeedsZeroDowntimeSurgeCapacity(deployment) {
		return "", nil
	}
	request := deploymentTemplateRequests(deployment)
	if request == (managedSharedNodeRequests{}) {
		return "", nil
	}
	pods, found, err := client.listAllPods(ctx)
	if err != nil || !found {
		return "", err
	}
	requestsByNode := managedSharedNodeRequestsByPods(pods)
	nodeNames, err := client.listNodeNames(ctx)
	if err != nil {
		return "", fmt.Errorf("list kubernetes nodes for zero-downtime capacity preflight: %w", err)
	}
	sort.Strings(nodeNames)

	candidates := make([]rolloutCapacityCandidate, 0, len(nodeNames))
	for _, nodeName := range nodeNames {
		node, found, err := client.getNode(ctx, nodeName)
		if err != nil {
			return "", fmt.Errorf("read kubernetes node %s for zero-downtime capacity preflight: %w", nodeName, err)
		}
		if !found || !nodeEligibleForDeploymentTemplate(node, deployment) {
			continue
		}
		requested := requestsByNode[nodeName]
		candidate := rolloutCapacityCandidate{
			nodeName:               nodeName,
			remainingCPUMilli:      parseKubeResourceMilli(node.Status.Allocatable["cpu"]) - requested.cpuMilli,
			remainingMemoryBytes:   parseKubeResourceBytes(node.Status.Allocatable["memory"]) - requested.memoryBytes,
			remainingEphemeralByte: parseKubeResourceBytes(node.Status.Allocatable["ephemeral-storage"]) - requested.ephemeralBytes,
		}
		if rolloutCapacityCandidateFits(candidate, request) {
			return "", nil
		}
		candidates = append(candidates, candidate)
	}

	required := formatRolloutCapacityRequest(request)
	if len(candidates) == 0 {
		return fmt.Sprintf("zero-downtime rollout blocked: no schedulable node matches the deployment template; surge pod requires %s", required), nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if left.remainingCPUMilli != right.remainingCPUMilli {
			return left.remainingCPUMilli > right.remainingCPUMilli
		}
		if left.remainingMemoryBytes != right.remainingMemoryBytes {
			return left.remainingMemoryBytes > right.remainingMemoryBytes
		}
		return left.nodeName < right.nodeName
	})
	best := candidates[0]
	return fmt.Sprintf(
		"zero-downtime rollout blocked: no eligible node has capacity for the surge pod; best node %s has %s free, surge pod requires %s",
		best.nodeName,
		formatRolloutCapacityCandidate(best),
		required,
	), nil
}

func deploymentNeedsZeroDowntimeSurgeCapacity(deployment kubeDeployment) bool {
	strategyType := strings.TrimSpace(deployment.Spec.Strategy.Type)
	if strategyType == "" {
		return false
	}
	if !strings.EqualFold(strategyType, "RollingUpdate") {
		return false
	}
	if normalizedKubeIntOrString(deployment.Spec.Strategy.RollingUpdate.MaxUnavailable) != "0" {
		return false
	}
	return kubeIntOrStringPositive(deployment.Spec.Strategy.RollingUpdate.MaxSurge)
}

func kubeIntOrStringPositive(value any) bool {
	normalized := normalizedKubeIntOrString(value)
	if normalized == "" || normalized == "0" || normalized == "0%" {
		return false
	}
	return true
}

func deploymentTemplateRequests(deployment kubeDeployment) managedSharedNodeRequests {
	regular := kubeContainerRequests(deployment.Spec.Template.Spec.Containers)
	init := kubeMaxContainerRequests(deployment.Spec.Template.Spec.InitContainers)
	return managedSharedNodeRequests{
		cpuMilli:       maxInt64(regular.cpuMilli, init.cpuMilli),
		memoryBytes:    maxInt64(regular.memoryBytes, init.memoryBytes),
		ephemeralBytes: maxInt64(regular.ephemeralBytes, init.ephemeralBytes),
	}
}

func nodeEligibleForDeploymentTemplate(node kubeNode, deployment kubeDeployment) bool {
	if !kubeNodeReady(node) || node.Spec.Unschedulable || kubeNodeConditionTrue(node.Status.Conditions, "DiskPressure") {
		return false
	}
	if !nodeLabelsMatchSelector(node.Metadata.Labels, deployment.Spec.Template.Spec.NodeSelector) {
		return false
	}
	return kubeTaintsTolerated(node.Spec.Taints, deployment.Spec.Template.Spec.Tolerations)
}

func kubeTaintsTolerated(taints []kubeTaint, tolerations []runtimepkg.Toleration) bool {
	for _, taint := range taints {
		effect := strings.TrimSpace(taint.Effect)
		if effect != "NoSchedule" && effect != "NoExecute" {
			continue
		}
		if !kubeTaintTolerated(taint, tolerations) {
			return false
		}
	}
	return true
}

func kubeTaintTolerated(taint kubeTaint, tolerations []runtimepkg.Toleration) bool {
	for _, toleration := range tolerations {
		if effect := strings.TrimSpace(toleration.Effect); effect != "" && effect != strings.TrimSpace(taint.Effect) {
			continue
		}
		key := strings.TrimSpace(toleration.Key)
		operator := strings.TrimSpace(toleration.Operator)
		if operator == "" {
			operator = "Equal"
		}
		switch operator {
		case "Exists":
			if key == "" || key == strings.TrimSpace(taint.Key) {
				return true
			}
		case "Equal":
			if key == strings.TrimSpace(taint.Key) && strings.TrimSpace(toleration.Value) == strings.TrimSpace(taint.Value) {
				return true
			}
		}
	}
	return false
}

func rolloutCapacityCandidateFits(candidate rolloutCapacityCandidate, request managedSharedNodeRequests) bool {
	if candidate.remainingCPUMilli < request.cpuMilli {
		return false
	}
	if candidate.remainingMemoryBytes < request.memoryBytes {
		return false
	}
	if request.ephemeralBytes > 0 && candidate.remainingEphemeralByte < request.ephemeralBytes {
		return false
	}
	return true
}

func formatRolloutCapacityRequest(request managedSharedNodeRequests) string {
	parts := []string{
		fmt.Sprintf("%dm CPU", request.cpuMilli),
		fmt.Sprintf("%dMi memory", bytesToMiB(request.memoryBytes)),
	}
	if request.ephemeralBytes > 0 {
		parts = append(parts, fmt.Sprintf("%dMi ephemeral-storage", bytesToMiB(request.ephemeralBytes)))
	}
	return strings.Join(parts, ", ")
}

func formatRolloutCapacityCandidate(candidate rolloutCapacityCandidate) string {
	parts := []string{
		fmt.Sprintf("%dm CPU", candidate.remainingCPUMilli),
		fmt.Sprintf("%dMi memory", bytesToMiB(candidate.remainingMemoryBytes)),
	}
	if candidate.remainingEphemeralByte != 0 {
		parts = append(parts, fmt.Sprintf("%dMi ephemeral-storage", bytesToMiB(candidate.remainingEphemeralByte)))
	}
	return strings.Join(parts, ", ")
}

func bytesToMiB(value int64) int64 {
	if value == 0 {
		return 0
	}
	return value / 1024 / 1024
}
