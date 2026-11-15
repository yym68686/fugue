package api

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"time"

	"fugue/internal/agentedge"
	corev1 "k8s.io/api/core/v1"
)

type agentCapacityEvidence struct {
	NodeID                   string    `json:"node_id"`
	NodeUID                  string    `json:"node_uid"`
	Address                  string    `json:"address"`
	ObservedAt               time.Time `json:"observed_at"`
	ValidUntil               time.Time `json:"valid_until"`
	CPUUsageNanoCores        uint64    `json:"cpu_usage_nanocores"`
	CPUAllocatableMilliCores int64     `json:"cpu_allocatable_millicores"`
	MemoryWorkingSetBytes    uint64    `json:"memory_working_set_bytes"`
	MemoryAllocatableBytes   int64     `json:"memory_allocatable_bytes"`
}

func agentCapacityNodeReady(node corev1.Node, address string) bool {
	if node.UID == "" || node.DeletionTimestamp != nil || node.Spec.Unschedulable {
		return false
	}
	addressOwned := false
	for _, a := range node.Status.Addresses {
		addressOwned = addressOwned || a.Address == address
	}
	conditions := map[corev1.NodeConditionType]corev1.ConditionStatus{}
	for _, c := range node.Status.Conditions {
		if _, duplicate := conditions[c.Type]; duplicate {
			return false
		}
		conditions[c.Type] = c.Status
	}
	network, hasNetwork := conditions[corev1.NodeNetworkUnavailable]
	return addressOwned && conditions[corev1.NodeReady] == corev1.ConditionTrue &&
		conditions[corev1.NodeMemoryPressure] == corev1.ConditionFalse && conditions[corev1.NodeDiskPressure] == corev1.ConditionFalse &&
		conditions[corev1.NodePIDPressure] == corev1.ConditionFalse && (!hasNetwork || network == corev1.ConditionFalse)
}

func readAgentCapacity(ctx context.Context, c *clusterNodeClient, nodeID, address string, p agentedge.CapacityPolicy, clockNow func() time.Time) (agentCapacityEvidence, error) {
	return readAgentCapacityWithObserver(ctx, c, nodeID, address, p, clockNow, nil)
}

func readAgentCapacityWithObserver(ctx context.Context, c *clusterNodeClient, nodeID, address string, p agentedge.CapacityPolicy, clockNow func() time.Time, observe func(string)) (agentCapacityEvidence, error) {
	event := captureEvent{Stage: "agent-capacity-ok", Node: nodeID, Capacity: &captureCapacity{MaxCPUPercent: p.MaxNodeCPUPercent, MaxMemoryPercent: p.MaxNodeMemoryPercent}}
	traceStarted := time.Now()
	defer func() {
		event.DurationMS = time.Since(traceStarted).Milliseconds()
		observeCapture(ctx, event)
	}()
	fail := func(stage string) (agentCapacityEvidence, error) {
		event.Stage = stage
		if observe != nil {
			observe(stage)
		}
		return agentCapacityEvidence{}, errors.New("fresh authenticated node capacity unavailable")
	}
	started := clockNow().UTC()
	var node corev1.Node
	path := "/api/v1/nodes/" + url.PathEscape(nodeID)
	nodeErr := c.doJSON(ctx, http.MethodGet, path, &node)
	event.ErrorClass = captureErrorClass(nodeErr)
	event.HTTPStatus = captureHTTPStatus(nodeErr)
	event.Capacity.NodeUID = string(node.UID)
	event.Capacity.Unschedulable = node.Spec.Unschedulable
	event.Capacity.AddressOwned = slices.ContainsFunc(node.Status.Addresses, func(a corev1.NodeAddress) bool { return a.Address == address })
	for _, condition := range node.Status.Conditions {
		status := string(condition.Status)
		switch condition.Type {
		case corev1.NodeReady:
			event.Capacity.NodeReady = status
		case corev1.NodeMemoryPressure:
			event.Capacity.MemoryPressure = status
		case corev1.NodeDiskPressure:
			event.Capacity.DiskPressure = status
		case corev1.NodePIDPressure:
			event.Capacity.PIDPressure = status
		case corev1.NodeNetworkUnavailable:
			event.Capacity.NetworkUnavailable = status
		}
	}
	if nodeErr != nil || node.Name != nodeID || !agentCapacityNodeReady(node, address) {
		return fail("agent-capacity-node-unavailable")
	}
	event.Capacity.CPUMilliCores = node.Status.Allocatable.Cpu().MilliValue()
	event.Capacity.MemoryAvailable = node.Status.Allocatable.Memory().Value()
	summary, err := c.getNodeSummary(ctx, nodeID)
	event.ErrorClass = captureErrorClass(err)
	event.HTTPStatus = captureHTTPStatus(err)
	if err != nil || summary == nil || summary.Node.NodeName != nodeID || summary.Node.CPU.UsageNanoCores == nil {
		return fail("agent-capacity-summary-unavailable")
	}
	used := summary.Node.Memory.WorkingSetBytes
	if used == nil {
		used = summary.Node.Memory.UsageBytes
	}
	if used == nil {
		return fail("agent-capacity-summary-unavailable")
	}
	event.Capacity.CPUNanoCores = *summary.Node.CPU.UsageNanoCores
	event.Capacity.MemoryBytes = *used
	// Kubelet may collect a sample while this authenticated request is in
	// flight. Compare it with read completion, never with request start. The
	// observation and expiry below remain the original metric timestamps.
	now := clockNow().UTC()
	if now.Before(started) {
		return fail("agent-capacity-time-invalid")
	}
	cpuAt, e1 := time.Parse(time.RFC3339Nano, summary.Node.CPU.Time)
	memoryAt, e2 := time.Parse(time.RFC3339Nano, summary.Node.Memory.Time)
	event.Capacity.CPUTime, event.Capacity.MemoryTime = cpuAt, memoryAt
	if e1 != nil || e2 != nil || cpuAt.After(now) || memoryAt.After(now) || cpuAt.Before(node.CreationTimestamp.Time) || memoryAt.Before(node.CreationTimestamp.Time) {
		return fail("agent-capacity-time-invalid")
	}
	observed := cpuAt
	if memoryAt.Before(observed) {
		observed = memoryAt
	}
	until := observed.Add(time.Duration(p.FactMaxAgeSeconds) * time.Second)
	event.Capacity.ValidUntil = until
	if !until.After(now) {
		return fail("agent-capacity-expired")
	}
	cpu := node.Status.Allocatable.Cpu().MilliValue()
	memory := node.Status.Allocatable.Memory().Value()
	if cpu <= 0 || memory <= 0 || float64(*summary.Node.CPU.UsageNanoCores)/1e6/float64(cpu)*100 > float64(p.MaxNodeCPUPercent) ||
		float64(*used)/float64(memory)*100 > float64(p.MaxNodeMemoryPercent) {
		return fail("agent-capacity-pressure")
	}
	var current corev1.Node
	recheckErr := c.doJSON(ctx, http.MethodGet, path, &current)
	event.ErrorClass = captureErrorClass(recheckErr)
	event.HTTPStatus = captureHTTPStatus(recheckErr)
	if recheckErr != nil || current.UID != node.UID || !agentCapacityNodeReady(current, address) ||
		current.Status.Allocatable.Cpu().MilliValue() != cpu || current.Status.Allocatable.Memory().Value() != memory {
		return fail("agent-capacity-node-changed")
	}
	finished := clockNow().UTC()
	if finished.Before(now) || !until.After(finished) {
		return fail("agent-capacity-expired")
	}
	return agentCapacityEvidence{NodeID: nodeID, NodeUID: string(node.UID), Address: address, ObservedAt: observed, ValidUntil: until,
		CPUUsageNanoCores: *summary.Node.CPU.UsageNanoCores, CPUAllocatableMilliCores: cpu, MemoryWorkingSetBytes: *used, MemoryAllocatableBytes: memory}, nil
}
