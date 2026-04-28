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
	if !shouldPinManagedSharedApp(app) {
		return base, nil
	}
	runtimeObj, err := s.Store.GetRuntime(app.Spec.RuntimeID)
	if err != nil {
		return runtimepkg.SchedulingConstraints{}, fmt.Errorf("load app runtime %s: %w", app.Spec.RuntimeID, err)
	}
	if runtimeObj.Type != model.RuntimeTypeManagedShared {
		return base, nil
	}
	nodeName, found, err := s.selectManagedSharedAppNode(ctx, app, base.NodeSelector)
	if err != nil {
		return runtimepkg.SchedulingConstraints{}, err
	}
	if !found {
		return base, nil
	}
	out := base
	out.NodeSelector = clonePlacementStringMap(base.NodeSelector)
	if out.NodeSelector == nil {
		out.NodeSelector = map[string]string{}
	}
	out.NodeSelector[kubeHostnameLabelKey] = nodeName
	return out, nil
}

func shouldPinManagedSharedApp(app model.App) bool {
	if app.Spec.Workspace != nil || app.Spec.PersistentStorage != nil {
		return false
	}
	return strings.TrimSpace(app.Spec.RuntimeID) != ""
}

func (s *Service) selectManagedSharedAppNode(ctx context.Context, app model.App, selector map[string]string) (string, bool, error) {
	client, err := s.kubeClient()
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("initialize app placement client for app=%s failed, continuing with runtime-only scheduling: %v", app.ID, err)
		}
		return "", false, nil
	}
	pods, hasNodeRequests, err := client.listAllPods(ctx)
	if err != nil || !hasNodeRequests {
		if s.Logger != nil {
			s.Logger.Printf("resolve app node request inventory for app=%s failed, continuing with runtime-only scheduling: %v", app.ID, err)
		}
		return "", false, nil
	}
	nodeRequestsByName := managedSharedNodeRequestsByPods(pods)
	appRequestsByNode := managedAppRequestsByNode(app, pods)
	nodeNames, err := client.listNodeNames(ctx)
	if err != nil {
		return "", false, fmt.Errorf("list kubernetes nodes: %w", err)
	}
	sort.Strings(nodeNames)

	request := managedAppPlacementRequest(app)
	policy := managedAppPlacementPolicy(app)
	candidates := make([]managedSharedNodeCandidate, 0, len(nodeNames))
	for _, nodeName := range nodeNames {
		node, found, err := client.getNode(ctx, nodeName)
		if err != nil {
			return "", false, fmt.Errorf("read kubernetes node %s: %w", nodeName, err)
		}
		if !found || !nodeLabelsMatchSelector(node.Metadata.Labels, selector) {
			continue
		}
		if !managedSharedNodeSchedulable(node) {
			continue
		}
		requested := nodeRequestsByName[nodeName]
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

func managedSharedNodeCandidateFitsPolicy(candidate managedSharedNodeCandidate, request managedSharedNodeRequests, policy appPlacementPolicy) bool {
	if candidate.allocatableCPUMilli > 0 && candidate.requestedCPUMilli+request.cpuMilli > resourceCapacityWithRatio(candidate.allocatableCPUMilli, policy.cpuOvercommitRatio) {
		return false
	}
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
