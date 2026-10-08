package model

import (
	"testing"
	"time"
)

func TestNetworkNodeCapacityRequiresOriginalTimesAndDenominators(t *testing.T) {
	now := time.Now().UTC()
	capacity := EdgeNetworkNodeCapacity{Source: "kubelet_node_allocatable_v1", NodeUID: "node-a", ObservedAt: now, ValidUntil: now.Add(2 * time.Minute),
		CPUObservedAt: now.Add(time.Second), MemoryObservedAt: now, CPUAllocatableMilliCores: 1000, MemoryAllocatableBytes: 1 << 30, Pressure: []string{}}
	if err := ValidateEdgeNetworkNodeCapacity(&capacity); err != nil {
		t.Fatal("observed zero rejected", err)
	}
	for _, edit := range []func(*EdgeNetworkNodeCapacity){
		func(value *EdgeNetworkNodeCapacity) { value.Source = "legacy_active_requests" },
		func(value *EdgeNetworkNodeCapacity) { value.NodeUID = "" },
		func(value *EdgeNetworkNodeCapacity) { value.CPUAllocatableMilliCores = 0 },
		func(value *EdgeNetworkNodeCapacity) { value.MemoryAllocatableBytes = -1 },
		func(value *EdgeNetworkNodeCapacity) { value.CPUObservedAt = time.Time{} },
		func(value *EdgeNetworkNodeCapacity) { value.ObservedAt = now.Add(time.Second) },
		func(value *EdgeNetworkNodeCapacity) { value.ValidUntil = now.Add(time.Hour) },
		func(value *EdgeNetworkNodeCapacity) { value.Pressure = []string{"unknown"} },
		func(value *EdgeNetworkNodeCapacity) { value.Pressure = []string{"MemoryPressure", "MemoryPressure"} },
	} {
		modified := capacity
		edit(&modified)
		if err := ValidateEdgeNetworkNodeCapacity(&modified); err == nil {
			t.Fatal("invalid capacity accepted", modified)
		}
	}
	capacity.CPUUsageNanoCores = 3_000_000_000
	capacity.MemoryWorkingSetBytes = 2 << 30
	capacity.Pressure = []string{"MemoryPressure"}
	if err := ValidateEdgeNetworkNodeCapacity(&capacity); err != nil {
		t.Fatal("real saturation hidden as unknown", err)
	}
}
