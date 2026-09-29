package api

import (
	"encoding/json"
	"testing"
)

func TestKubeletFilesystemPressureUsesPhysicalAvailability(t *testing.T) {
	// Decode the kubelet wire shape. A hand-built summary previously hid the
	// misplaced runtime field and silently discarded all image filesystem data.
	var summary kubeNodeSummary
	if err := json.Unmarshal([]byte(`{"node":{"fs":{"capacityBytes":1000,"usedBytes":650,"availableBytes":280},"runtime":{"imageFs":{"capacityBytes":1000,"usedBytes":250,"availableBytes":280}}}}`), &summary); err != nil {
		t.Fatal(err)
	}
	node := kubeNode{}
	node.Status.Capacity = map[string]string{"ephemeral-storage": "1000"}
	node.Status.Allocatable = map[string]string{"ephemeral-storage": "700"}
	storage := buildClusterNodeStorageStats(node, &summary, int64Ptr(140))
	image := buildClusterNodeImageFilesystemStats(&summary)
	if storage.UsagePercent == nil || *storage.UsagePercent != 72 || image == nil || image.UsagePercent == nil || *image.UsagePercent != 72 {
		t.Fatalf("physical availability must determine fullness: node=%+v image=%+v", storage, image)
	}
	if *storage.UsedBytes != 650 || *image.UsedBytes != 250 || *storage.AllocatableBytes != 700 || *storage.RequestPercent != 20 || *storage.SchedulableFreeBytes != 560 {
		t.Fatalf("physical fullness must preserve raw counters and scheduling accounting: node=%+v image=%+v", storage, image)
	}
	built := buildClusterNode(node, &summary, nil, false)
	pressure, _, reason := clusterNodeFilesystemPressure(clusterNodeSnapshot{node: built})
	if pressure {
		t.Fatalf("scheduler reservations must not create filesystem pressure: %s", reason)
	}
	// The same small image-owned byte count on a physically full device must
	// trigger pressure, even when nodefs is on a different, healthy filesystem.
	available := uint64(80)
	summary.Node.Runtime.ImageFS.AvailableBytes = &available
	built = buildClusterNode(node, &summary, nil, false)
	pressure, usage, _ := clusterNodeFilesystemPressure(clusterNodeSnapshot{node: built})
	if !pressure || usage == nil || *usage != 92 {
		t.Fatalf("image filesystem fullness was masked by image-owned bytes: pressure=%v usage=%v", pressure, usage)
	}
}

func TestFilesystemUsageMissingAndInvalidAvailability(t *testing.T) {
	percent := func(value float64) *float64 { return &value }
	for _, tc := range []struct {
		name string
		wire string
		want *float64
	}{
		{"used fallback", `{"node":{"fs":{"capacityBytes":1000,"usedBytes":650}}}`, percent(65)},
		{"invalid available fallback", `{"node":{"fs":{"capacityBytes":1000,"usedBytes":650,"availableBytes":2000}}}`, percent(65)},
		{"available without used", `{"node":{"fs":{"capacityBytes":1000,"availableBytes":100}}}`, percent(90)},
		{"missing usage", `{"node":{"fs":{"capacityBytes":1000}}}`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var summary kubeNodeSummary
			if err := json.Unmarshal([]byte(tc.wire), &summary); err != nil {
				t.Fatal(err)
			}
			node := kubeNode{}
			node.Status.Capacity = map[string]string{"ephemeral-storage": "1000"}
			node.Status.Allocatable = map[string]string{"ephemeral-storage": "700"}
			got := buildClusterNodeStorageStats(node, &summary, nil).UsagePercent
			if (got == nil) != (tc.want == nil) || got != nil && *got != *tc.want {
				t.Fatalf("usage=%v want=%v", got, tc.want)
			}
		})
	}
}
