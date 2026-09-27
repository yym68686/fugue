package api

import (
	"context"
	"errors"
	"net/http"
	"net/url"
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
	return addressOwned && conditions[corev1.NodeReady] == corev1.ConditionTrue &&
		conditions[corev1.NodeMemoryPressure] == corev1.ConditionFalse && conditions[corev1.NodeDiskPressure] == corev1.ConditionFalse &&
		conditions[corev1.NodePIDPressure] == corev1.ConditionFalse && conditions[corev1.NodeNetworkUnavailable] != corev1.ConditionTrue
}

func readAgentCapacity(ctx context.Context, c *clusterNodeClient, nodeID, address string, p agentedge.CapacityPolicy, now time.Time) (agentCapacityEvidence, error) {
	fail := func() (agentCapacityEvidence, error) {
		return agentCapacityEvidence{}, errors.New("fresh authenticated node capacity unavailable")
	}
	var node corev1.Node
	path := "/api/v1/nodes/" + url.PathEscape(nodeID)
	if c.doJSON(ctx, http.MethodGet, path, &node) != nil || node.Name != nodeID || !agentCapacityNodeReady(node, address) {
		return fail()
	}
	summary, err := c.getNodeSummary(ctx, nodeID)
	if err != nil || summary == nil || summary.Node.NodeName != nodeID || summary.Node.CPU.UsageNanoCores == nil {
		return fail()
	}
	used := summary.Node.Memory.WorkingSetBytes
	if used == nil {
		used = summary.Node.Memory.UsageBytes
	}
	if used == nil {
		return fail()
	}
	cpuAt, e1 := time.Parse(time.RFC3339Nano, summary.Node.CPU.Time)
	memoryAt, e2 := time.Parse(time.RFC3339Nano, summary.Node.Memory.Time)
	if e1 != nil || e2 != nil || cpuAt.After(now) || memoryAt.After(now) || cpuAt.Before(node.CreationTimestamp.Time) || memoryAt.Before(node.CreationTimestamp.Time) {
		return fail()
	}
	observed := cpuAt
	if memoryAt.Before(observed) {
		observed = memoryAt
	}
	until := observed.Add(time.Duration(p.FactMaxAgeSeconds) * time.Second)
	if !until.After(now) {
		return fail()
	}
	cpu := node.Status.Allocatable.Cpu().MilliValue()
	memory := node.Status.Allocatable.Memory().Value()
	if cpu <= 0 || memory <= 0 || float64(*summary.Node.CPU.UsageNanoCores)/1e6/float64(cpu)*100 > float64(p.MaxNodeCPUPercent) ||
		float64(*used)/float64(memory)*100 > float64(p.MaxNodeMemoryPercent) {
		return fail()
	}
	var current corev1.Node
	if c.doJSON(ctx, http.MethodGet, path, &current) != nil || current.UID != node.UID || !agentCapacityNodeReady(current, address) ||
		current.Status.Allocatable.Cpu().MilliValue() != cpu || current.Status.Allocatable.Memory().Value() != memory {
		return fail()
	}
	return agentCapacityEvidence{NodeID: nodeID, NodeUID: string(node.UID), Address: address, ObservedAt: observed, ValidUntil: until,
		CPUUsageNanoCores: *summary.Node.CPU.UsageNanoCores, CPUAllocatableMilliCores: cpu, MemoryWorkingSetBytes: *used, MemoryAllocatableBytes: memory}, nil
}
