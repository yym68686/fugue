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
	remainingMemoryBytes   int64
	remainingEphemeralByte int64
	remainingPodAddresses  int64
	podAddressesKnown      bool
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
	message, err := s.observeZeroDowntimeCapacity(ctx, app, client, deployment)
	if err != nil {
		return err
	}
	if strings.TrimSpace(message) != "" {
		return fmt.Errorf("%s", message)
	}
	return nil
}

func (s *Service) observeZeroDowntimeCapacity(ctx context.Context, app model.App, client *kubeClient, deployment kubeDeployment) (message string, err error) {
	observation := newSchedulingObservation(app, "surge_capacity")
	observation.decision.MemoryRequestRatio, observation.decision.CPUOvercommitRatio = 1, 1
	observation.decision.RequiredPodAddresses = 1
	ctx = context.WithValue(ctx, schedulingObservationKey{}, observation)
	defer func() {
		s.recordSchedulingObservation(ctx, app, observation, err)
		if err != nil && observation.decision.Outcome == "observation_failed" {
			err = schedulingObservationError{cause: err}
		}
	}()
	return zeroDowntimeRolloutCapacityBlockMessage(ctx, client, deployment, app.Spec.Replicas)
}

func zeroDowntimeRolloutCapacityBlockMessage(ctx context.Context, client *kubeClient, deployment kubeDeployment, desiredReplicas int) (string, error) {
	if client == nil || desiredReplicas <= 0 || !deploymentNeedsZeroDowntimeSurgeCapacity(deployment) {
		return "", nil
	}
	request := deploymentTemplateRequests(deployment)
	observation, _ := ctx.Value(schedulingObservationKey{}).(*schedulingObservation)
	if observation == nil {
		observation = newSchedulingObservation(model.App{}, "surge_capacity")
		observation.decision.MemoryRequestRatio, observation.decision.CPUOvercommitRatio = 1, 1
		observation.decision.RequiredPodAddresses = 1
	}
	observation.decision.Demand = schedulingResources(request)
	observation.diagnostics["required_selector"] = deployment.Spec.Template.Spec.NodeSelector
	// CPU is compressible and its request is a contention guarantee, not a
	// forecast of the surge Pod's peak demand. Kubernetes still enforces that
	// request; Fugue's preflight only hard-gates non-compressible resources.
	if request.memoryBytes <= 0 && request.ephemeralBytes <= 0 {
		return "", nil
	}
	pods, found, err := client.listAllPods(ctx)
	if err != nil || !found {
		observation.decision.Outcome = "observation_failed"
		observation.decision.MissingEvidence = []string{"pod_resource_inventory"}
		return "", err
	}
	observation.resourceInventoryObserved = true
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
		nodeDecision := observation.observeNode(nodeName, node, found, requestsByNode[nodeName], managedSharedNodeRequests{}, deployment.Spec.Template.Spec.NodeSelector, deployment.Spec.Template.Spec.Tolerations, pods, nil)
		if !found || !nodeEligibleForDeploymentTemplate(node, deployment) {
			continue
		}
		observation.capacity(nodeDecision)
		requested := requestsByNode[nodeName]
		candidate := rolloutCapacityCandidate{
			nodeName:               nodeName,
			remainingMemoryBytes:   parseKubeResourceBytes(node.Status.Allocatable["memory"]) - requested.memoryBytes,
			remainingEphemeralByte: parseKubeResourceBytes(node.Status.Allocatable["ephemeral-storage"]) - requested.ephemeralBytes,
		}
		candidate.remainingPodAddresses, candidate.podAddressesKnown = availablePodAddresses(node, pods)
		if rolloutCapacityCandidateFits(candidate, request) {
			nodeDecision.Eligible = true
			observation.decision.Outcome = "selected"
			observation.decision.SelectedCandidate = nodeDecision.CandidateID
			return "", nil
		}
		candidates = append(candidates, candidate)
	}

	required := formatRolloutCapacityRequest(request)
	observation.decision.Outcome = "rejected"
	if len(candidates) == 0 {
		return fmt.Sprintf("zero-downtime rollout blocked: no schedulable node matches the deployment template; surge pod requires %s; %s", required, observation.decision.Summary()), nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if left.remainingMemoryBytes != right.remainingMemoryBytes {
			return left.remainingMemoryBytes > right.remainingMemoryBytes
		}
		if left.remainingEphemeralByte != right.remainingEphemeralByte {
			return left.remainingEphemeralByte > right.remainingEphemeralByte
		}
		return left.nodeName < right.nodeName
	})
	best := candidates[0]
	alias := "candidate"
	for i, detail := range observation.nodes {
		if detail["node_name"] == best.nodeName {
			alias = observation.decision.Nodes[i].CandidateID
			break
		}
	}
	return fmt.Sprintf(
		"zero-downtime rollout blocked: no eligible node has capacity for the surge pod; best node %s has %s free, surge pod requires %s; %s",
		alias,
		formatRolloutCapacityCandidate(best),
		required,
		observation.decision.Summary(),
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
	return kubeEffectiveContainerRequests(
		deployment.Spec.Template.Spec.Containers,
		deployment.Spec.Template.Spec.InitContainers,
	)
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
	if candidate.podAddressesKnown && candidate.remainingPodAddresses < 1 {
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
	parts := []string{fmt.Sprintf("%dMi memory", bytesToMiB(request.memoryBytes))}
	if request.ephemeralBytes > 0 {
		parts = append(parts, fmt.Sprintf("%dMi ephemeral-storage", bytesToMiB(request.ephemeralBytes)))
	}
	return strings.Join(parts, ", ")
}

func formatRolloutCapacityCandidate(candidate rolloutCapacityCandidate) string {
	parts := []string{fmt.Sprintf("%dMi memory", bytesToMiB(candidate.remainingMemoryBytes))}
	if candidate.podAddressesKnown {
		parts = append(parts, fmt.Sprintf("%d Pod IP addresses", candidate.remainingPodAddresses))
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
