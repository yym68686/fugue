package inventory

import (
	"slices"
	"testing"
	"time"

	"fugue/internal/localpvsafety"
	"fugue/internal/model"
)

func TestObservedSnapshotRecomputesSafetyAtTheFreshnessBoundary(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	in := model.LocalPVInventory{
		ImagePath: "/storage/volume.img", LoopDevice: "/dev/loop-test",
		ObservedAt: now.Add(-localpvsafety.DefaultInventoryTTL),
	}
	if got := AssessObservedSnapshot(in, now); !got.SafeToDecommission || len(got.UnsafeReasons) != 0 {
		t.Fatalf("fresh evidence at TTL boundary: %+v", got)
	}
	if got := AssessObservedSnapshot(in, now.Add(time.Nanosecond)); got.SafeToDecommission ||
		!slices.Equal(got.UnsafeReasons, []string{"inventory_stale"}) || in.UnsafeReasons != nil {
		t.Fatalf("stale evidence or unsafe input alias: %+v; source=%+v", got, in)
	}
}

func TestEvaluateLocalPVDecommissionSafetyGates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   model.LocalPVInventory
		safe bool
	}{
		{
			name: "empty vg is eligible",
			in: model.LocalPVInventory{
				ImagePath:       "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				ImageSizeBytes:  1024,
				LoopDevice:      "/dev/loop9",
				LoopBackingFile: "/var/lib/fugue/lvm-localpv/fugue-vg.img",
			},
			safe: true,
		},
		{
			name: "active lv refused",
			in: model.LocalPVInventory{
				ImagePath:       "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				ImageSizeBytes:  1024,
				LoopDevice:      "/dev/loop9",
				LoopBackingFile: "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				LVCount:         1,
				ActiveLVCount:   1,
			},
		},
		{
			name: "missing loop refused",
			in: model.LocalPVInventory{
				ImagePath:      "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				ImageSizeBytes: 1024,
			},
		},
		{
			name: "path mismatch refused",
			in: model.LocalPVInventory{
				ImagePath:       "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				ImageSizeBytes:  1024,
				LoopDevice:      "/dev/loop9",
				LoopBackingFile: "/tmp/other.img",
			},
		},
		{
			name: "bound pv refused",
			in: model.LocalPVInventory{
				ImagePath:       "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				ImageSizeBytes:  1024,
				LoopDevice:      "/dev/loop9",
				LoopBackingFile: "/var/lib/fugue/lvm-localpv/fugue-vg.img",
				BoundPVCount:    1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			safe, reasons := DecommissionSafety(tt.in)
			if safe != tt.safe {
				t.Fatalf("safe=%t want %t reasons=%v", safe, tt.safe, reasons)
			}
			if !tt.safe && len(reasons) == 0 {
				t.Fatal("expected unsafe reasons")
			}
		})
	}
}
