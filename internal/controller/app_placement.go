package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

type appPlacementPolicy struct {
	cpuOvercommitRatio float64
	memoryRequestRatio float64
}

func (s *Service) managedSchedulingConstraintsForApp(ctx context.Context, app model.App) (runtimepkg.SchedulingConstraints, error) {
	base, err := s.managedSchedulingConstraints(app.Spec.RuntimeID)
	if err != nil {
		return runtimepkg.SchedulingConstraints{}, err
	}
	if s == nil || s.Store == nil || strings.TrimSpace(app.Spec.RuntimeID) == "" {
		return base, nil
	}
	runtimeObj, err := s.Store.GetRuntime(app.Spec.RuntimeID)
	if err != nil {
		return runtimepkg.SchedulingConstraints{}, fmt.Errorf("load app runtime %s: %w", app.Spec.RuntimeID, err)
	}
	// Stopping a workload must not depend on a new attachment being possible.
	if app.Spec.Replicas <= 0 {
		return base, nil
	}
	if app.Spec.PersistentStorage != nil || app.Spec.Workspace != nil {
		client, err := s.kubeClient()
		if err != nil {
			return runtimepkg.SchedulingConstraints{}, fmt.Errorf("observe app storage placement: %w", err)
		}
		if live, found, err := client.getDeployment(ctx, runtimepkg.NamespaceForTenant(app.TenantID), runtimepkg.RuntimeAppResourceName(app)); err != nil {
			return runtimepkg.SchedulingConstraints{}, fmt.Errorf("observe serving storage placement: %w", err)
		} else if found && managedDeploymentStatusReady(live, app.Spec.Replicas) {
			observed := runtimepkg.SchedulingConstraints{NodeSelector: live.Spec.Template.Spec.NodeSelector, Tolerations: live.Spec.Template.Spec.Tolerations}
			if desiredStringMapSubset(observed.NodeSelector, base.NodeSelector) && deploymentTargetsExpectedRollout(live, s.expectedManagedAppReleaseKey(s.Renderer.PrepareApp(app), observed), strings.TrimSpace(app.Spec.Image)) {
				return observed, nil
			}
		}
		objects := s.Renderer.BuildManagedAppChildObjects(app, base, nil)
		storage, err := s.appStoragePlacement(ctx, client, app, objects)
		if err != nil {
			return runtimepkg.SchedulingConstraints{}, err
		}
		if storage != nil {
			node, found, err := s.selectManagedAppNode(ctx, app, base, storage)
			if err != nil {
				return runtimepkg.SchedulingConstraints{}, err
			}
			if !found {
				return runtimepkg.SchedulingConstraints{}, fmt.Errorf("no eligible storage attachment node for runtime %s", app.Spec.RuntimeID)
			}
			return schedulingPinnedToNode(base, node), nil
		}
	}
	if runtimeObj.Type != model.RuntimeTypeManagedShared {
		return base, nil
	}
	if !shouldPinManagedSharedApp(app) {
		return base, nil
	}
	nodeName, found, err := s.selectManagedSharedAppNode(ctx, app, base.NodeSelector)
	if err != nil {
		return runtimepkg.SchedulingConstraints{}, err
	}
	if !found {
		return base, nil
	}
	return schedulingPinnedToNode(base, nodeName), nil
}

func shouldPinManagedSharedApp(app model.App) bool {
	if app.Spec.Workspace != nil {
		return false
	}
	return strings.TrimSpace(app.Spec.RuntimeID) != ""
}

func schedulingPinnedToNode(base runtimepkg.SchedulingConstraints, nodeName string) runtimepkg.SchedulingConstraints {
	out := base
	out.NodeSelector = clonePlacementStringMap(base.NodeSelector)
	if out.NodeSelector == nil {
		out.NodeSelector = map[string]string{}
	}
	out.NodeSelector[kubeHostnameLabelKey] = strings.TrimSpace(nodeName)
	return out
}

func (s *Service) selectManagedSharedAppNode(ctx context.Context, app model.App, selector map[string]string) (string, bool, error) {
	return s.selectManagedAppNode(ctx, app, runtimepkg.SchedulingConstraints{NodeSelector: selector}, nil)
}

func (s *Service) selectManagedAppNode(ctx context.Context, app model.App, constraints runtimepkg.SchedulingConstraints, storage *storagePlacement) (string, bool, error) {
	selector := constraints.NodeSelector
	client, err := s.kubeClient()
	if err != nil {
		if storage != nil {
			return "", false, err
		}
		if s.Logger != nil {
			s.Logger.Printf("initialize app placement client for app=%s failed, continuing with runtime-only scheduling: %v", app.ID, err)
		}
		return "", false, nil
	}
	pods, hasNodeRequests, err := client.listAllPods(ctx)
	if err != nil || !hasNodeRequests {
		if storage != nil {
			return "", false, fmt.Errorf("observe pod resource and volume usage before storage placement: available=%t error=%v", hasNodeRequests, err)
		}
		if s.Logger != nil {
			s.Logger.Printf("resolve app node request inventory for app=%s failed, continuing with runtime-only scheduling: %v", app.ID, err)
		}
		return "", false, nil
	}
	nodeRequestsByName := managedSharedNodeRequestsByPods(pods)
	appRequestsByNode := managedAppRequestsByNode(app, pods)
	readyAppNodesByName := managedReadyAppNodesByNode(app, pods)
	nodeNames, err := client.listNodeNames(ctx)
	if err != nil {
		return "", false, fmt.Errorf("list kubernetes nodes: %w", err)
	}
	sort.Strings(nodeNames)

	request := managedAppPlacementRequest(app)
	policy := managedAppPlacementPolicy(app)
	candidates := make([]managedSharedNodeCandidate, 0, len(nodeNames))
	var rejected []string
	for _, nodeName := range nodeNames {
		node, found, err := client.getNode(ctx, nodeName)
		if err != nil {
			return "", false, fmt.Errorf("read kubernetes node %s: %w", nodeName, err)
		}
		if !found || !nodeLabelsMatchSelector(node.Metadata.Labels, selector) {
			continue
		}
		if !kubeNodeReady(node) || node.Spec.Unschedulable || kubeNodeConditionTrue(node.Status.Conditions, "DiskPressure") || !kubeTaintsTolerated(node.Spec.Taints, constraints.Tolerations) {
			continue
		}
		if reason := storage.rejection(node); reason != "" {
			if _, serving := readyAppNodesByName[nodeName]; serving {
				return "", false, fmt.Errorf("preserving serving app on node %s; new storage placement blocked: %s", nodeName, reason)
			}
			rejected = append(rejected, nodeName+": "+reason)
			continue
		}
		// Preserve a healthy app's existing node pin even when the node is
		// currently over the placement policy budget. Capacity policy governs
		// new placement; it must not make reconciliation evict a serving app.
		if len(readyAppNodesByName) == 1 {
			if _, serving := readyAppNodesByName[nodeName]; serving {
				return nodeName, true, nil
			}
		}
		requested := nodeRequestsByName[nodeName]
		if free, known := availablePodAddresses(node, pods); known && free < int64(app.Spec.Replicas) {
			continue
		}
		if existing := appRequestsByNode[nodeName]; existing != (managedSharedNodeRequests{}) {
			requested.cpuMilli = maxInt64(0, requested.cpuMilli-existing.cpuMilli)
			requested.memoryBytes = maxInt64(0, requested.memoryBytes-existing.memoryBytes)
			requested.ephemeralBytes = maxInt64(0, requested.ephemeralBytes-existing.ephemeralBytes)
		}
		candidate := managedSharedNodeCandidate{
			nodeName:                 nodeName,
			allocatableEphemeralByte: parseKubeResourceBytes(node.Status.Allocatable["ephemeral-storage"]),
			allocatableMemoryBytes:   parseKubeResourceBytes(node.Status.Allocatable["memory"]),
			allocatableCPUMilli:      parseKubeResourceMilli(node.Status.Allocatable["cpu"]),
			requestedCPUMilli:        requested.cpuMilli,
			requestedMemoryBytes:     requested.memoryBytes,
			requestedEphemeralBytes:  requested.ephemeralBytes,
		}
		if !managedSharedNodeCandidateFitsPolicy(candidate, request, policy) {
			continue
		}
		candidate.remainingCPUMilli = resourceCapacityWithRatio(candidate.allocatableCPUMilli, policy.cpuOvercommitRatio) - candidate.requestedCPUMilli - request.cpuMilli
		candidate.remainingMemoryBytes = resourceCapacityWithRatio(candidate.allocatableMemoryBytes, policy.memoryRequestRatio) - candidate.requestedMemoryBytes - request.memoryBytes
		candidates = append(candidates, candidate)
	}
	if len(candidates) == 0 {
		if storage != nil {
			return "", false, fmt.Errorf("no eligible storage attachment node matches runtime/capacity constraints: %s", strings.Join(rejected, "; "))
		}
		return "", false, nil
	}
	for _, candidate := range candidates {
		if _, ok := appRequestsByNode[candidate.nodeName]; ok {
			return candidate.nodeName, true, nil
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if left.remainingMemoryBytes != right.remainingMemoryBytes {
			return left.remainingMemoryBytes > right.remainingMemoryBytes
		}
		if left.remainingCPUMilli != right.remainingCPUMilli {
			return left.remainingCPUMilli > right.remainingCPUMilli
		}
		return left.nodeName < right.nodeName
	})
	return candidates[0].nodeName, true, nil
}

func managedSharedNodeRequestsByPods(pods []kubePod) map[string]managedSharedNodeRequests {
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
	return out
}

func managedAppRequestsByNode(app model.App, pods []kubePod) map[string]managedSharedNodeRequests {
	out := make(map[string]managedSharedNodeRequests)
	appID := strings.TrimSpace(app.ID)
	if appID == "" {
		return out
	}
	for _, pod := range pods {
		if managedPostgresPodFinished(pod) || strings.TrimSpace(pod.Spec.NodeName) == "" {
			continue
		}
		if !managedAppPodNameMatchesApp(app, pod.Metadata.Name) {
			continue
		}
		request := kubePodRequests(pod)
		current := out[strings.TrimSpace(pod.Spec.NodeName)]
		current.cpuMilli += request.cpuMilli
		current.memoryBytes += request.memoryBytes
		current.ephemeralBytes += request.ephemeralBytes
		out[strings.TrimSpace(pod.Spec.NodeName)] = current
	}
	return out
}

func managedReadyAppNodesByNode(app model.App, pods []kubePod) map[string]struct{} {
	out := make(map[string]struct{})
	for _, pod := range pods {
		nodeName := strings.TrimSpace(pod.Spec.NodeName)
		if nodeName == "" || managedPostgresPodFinished(pod) || !managedAppPodNameMatchesApp(app, pod.Metadata.Name) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Running") || !kubePodReady(pod) {
			continue
		}
		out[nodeName] = struct{}{}
	}
	return out
}

func managedSharedNodeCandidateFitsPolicy(candidate managedSharedNodeCandidate, request managedSharedNodeRequests, policy appPlacementPolicy) bool {
	if candidate.allocatableMemoryBytes > 0 && candidate.requestedMemoryBytes+request.memoryBytes > resourceCapacityWithRatio(candidate.allocatableMemoryBytes, policy.memoryRequestRatio) {
		return false
	}
	if candidate.allocatableEphemeralByte > 0 && candidate.requestedEphemeralBytes+request.ephemeralBytes > candidate.allocatableEphemeralByte {
		return false
	}
	return true
}

func managedAppPlacementRequest(app model.App) managedSharedNodeRequests {
	if app.Spec.Resources == nil {
		return managedSharedNodeRequests{}
	}
	replicas := app.Spec.Replicas
	if replicas <= 0 {
		replicas = 1
	}
	return managedSharedNodeRequests{
		cpuMilli:    maxInt64(0, app.Spec.Resources.CPUMilliCores) * int64(replicas),
		memoryBytes: maxInt64(0, app.Spec.Resources.MemoryMebibytes) * 1024 * 1024 * int64(replicas),
	}
}

func managedAppPlacementPolicy(app model.App) appPlacementPolicy {
	switch model.EffectiveWorkloadClass(app.Spec) {
	case model.WorkloadClassCritical:
		return appPlacementPolicy{cpuOvercommitRatio: 1.0, memoryRequestRatio: 0.8}
	case model.WorkloadClassDemo:
		return appPlacementPolicy{cpuOvercommitRatio: 3.0, memoryRequestRatio: 0.9}
	case model.WorkloadClassBatch:
		return appPlacementPolicy{cpuOvercommitRatio: 3.0, memoryRequestRatio: 0.85}
	default:
		return appPlacementPolicy{cpuOvercommitRatio: 2.0, memoryRequestRatio: 0.9}
	}
}

func managedAppPodNameMatchesApp(app model.App, podName string) bool {
	prefix := strings.TrimSpace(runtimepkg.RuntimeAppResourceName(app))
	podName = strings.TrimSpace(podName)
	if prefix == "" || podName == "" {
		return false
	}
	return podName == prefix || strings.HasPrefix(podName, prefix+"-")
}

func clonePlacementStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
