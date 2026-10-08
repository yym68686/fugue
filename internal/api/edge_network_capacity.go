package api

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"fugue/internal/model"
	corev1 "k8s.io/api/core/v1"
)

func networkCapacityNode(node corev1.Node, nodeID, address string) ([]string, error) {
	fail := errors.New("node capacity identity, readiness or limits unavailable")
	if node.Name != nodeID || node.UID == "" || node.CreationTimestamp.IsZero() || node.DeletionTimestamp != nil || node.Spec.Unschedulable {
		return nil, fail
	}
	owned := false
	for _, entry := range node.Status.Addresses {
		owned = owned || entry.Address == address
	}
	if !owned || node.Status.Allocatable.Cpu().MilliValue() <= 0 || node.Status.Allocatable.Memory().Value() <= 0 {
		return nil, fail
	}
	conditions := map[corev1.NodeConditionType]corev1.ConditionStatus{}
	for _, condition := range node.Status.Conditions {
		if _, duplicate := conditions[condition.Type]; duplicate {
			return nil, fail
		}
		conditions[condition.Type] = condition.Status
	}
	if conditions[corev1.NodeReady] != corev1.ConditionTrue {
		return nil, fail
	}
	if network, exists := conditions[corev1.NodeNetworkUnavailable]; exists && network != corev1.ConditionFalse {
		return nil, fail
	}
	pressure := []string{}
	for _, name := range []corev1.NodeConditionType{corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure} {
		switch conditions[name] {
		case corev1.ConditionTrue:
			pressure = append(pressure, string(name))
		case corev1.ConditionFalse:
		default:
			return nil, fail
		}
	}
	return pressure, nil
}

func readNetworkNodeCapacity(ctx context.Context, client *clusterNodeClient, nodeID, address string, clock func() time.Time) (*model.EdgeNetworkNodeCapacity, error) {
	started := clock().UTC()
	path := "/api/v1/nodes/" + url.PathEscape(nodeID)
	var node corev1.Node
	if err := client.doJSON(ctx, http.MethodGet, path, &node); err != nil {
		return nil, err
	}
	pressure, err := networkCapacityNode(node, nodeID, address)
	if err != nil {
		return nil, err
	}
	summary, err := client.getNodeSummary(ctx, nodeID)
	if err != nil || summary == nil || summary.Node.NodeName != nodeID || summary.Node.CPU.UsageNanoCores == nil || summary.Node.Memory.WorkingSetBytes == nil {
		return nil, errors.New("node CPU or working-set memory sample unavailable")
	}
	cpuAt, cpuErr := time.Parse(time.RFC3339Nano, summary.Node.CPU.Time)
	memoryAt, memoryErr := time.Parse(time.RFC3339Nano, summary.Node.Memory.Time)
	now := clock().UTC()
	if cpuErr != nil || memoryErr != nil || now.Before(started) || cpuAt.After(now) || memoryAt.After(now) ||
		cpuAt.Before(node.CreationTimestamp.Time) || memoryAt.Before(node.CreationTimestamp.Time) {
		return nil, errors.New("node capacity timestamp does not belong to live node")
	}
	observed := cpuAt
	if memoryAt.Before(observed) {
		observed = memoryAt
	}
	capacity := &model.EdgeNetworkNodeCapacity{Source: "kubelet_node_allocatable_v1", NodeUID: string(node.UID), ObservedAt: observed, ValidUntil: observed.Add(2 * time.Minute),
		CPUObservedAt: cpuAt, MemoryObservedAt: memoryAt, CPUUsageNanoCores: *summary.Node.CPU.UsageNanoCores,
		CPUAllocatableMilliCores: node.Status.Allocatable.Cpu().MilliValue(), MemoryWorkingSetBytes: *summary.Node.Memory.WorkingSetBytes,
		MemoryAllocatableBytes: node.Status.Allocatable.Memory().Value(), Pressure: pressure}
	if model.ValidateEdgeNetworkNodeCapacity(capacity) != nil || !capacity.ValidUntil.After(now) {
		return nil, errors.New("node capacity stale or invalid")
	}
	var current corev1.Node
	if err := client.doJSON(ctx, http.MethodGet, path, &current); err != nil {
		return nil, err
	}
	currentPressure, err := networkCapacityNode(current, nodeID, address)
	finished := clock().UTC()
	if err != nil || current.UID != node.UID || !reflect.DeepEqual(pressure, currentPressure) ||
		current.Status.Allocatable.Cpu().MilliValue() != capacity.CPUAllocatableMilliCores || current.Status.Allocatable.Memory().Value() != capacity.MemoryAllocatableBytes ||
		finished.Before(now) || !capacity.ValidUntil.After(finished) {
		return nil, errors.New("node identity, capacity or pressure changed during observation")
	}
	return capacity, nil
}
