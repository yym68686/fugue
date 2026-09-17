package controller

import (
	"fugue/internal/localpvsafety"
	"fugue/internal/model"
	"testing"
)

func TestRecoveryPreservesFailedClusterExpansionAndPVCRequests(t *testing.T) {
	for _, tc := range []struct {
		values []string
		want   string
	}{
		{[]string{"20Gi", "40Gi", "22Gi"}, "40Gi"},
		{[]string{"40Gi", "40Gi", "48Gi"}, "48Gi"},
		{[]string{"20Gi", "", ""}, "20Gi"},
	} {
		got, err := maximumRecoveryStorageSize(tc.values...)
		if err != nil || got != tc.want {
			t.Fatalf("%v: %q %v", tc.values, got, err)
		}
	}
	for _, bad := range []string{"0", "-1Gi", "garbage"} {
		if _, err := maximumRecoveryStorageSize("20Gi", bad); err == nil {
			t.Fatalf("accepted invalid live observation %q", bad)
		}
	}
}

func TestRecoveryPoolIncludesReserveAfterGrowth(t *testing.T) {
	inventory := model.LocalPVInventory{ImageSizeBytes: 224 << 30, PVSizeBytes: (224 << 30) - (4 << 20), PVFreeBytes: (6 << 30) - (4 << 20)}
	target, err := recoveryPoolTarget(inventory, 2<<30)
	if err != nil {
		t.Fatal(err)
	}
	growth := target - inventory.ImageSizeBytes
	if growth <= 0 || growth > 64<<30 {
		t.Fatalf("invalid growth %d", growth)
	}
	if inventory.PVFreeBytes+growth-(2<<30) < localpvsafety.RequiredFreeBytes(inventory.PVSizeBytes+growth) {
		t.Fatal("expanded pool would violate reserve")
	}
	inventory.PVFreeBytes += growth
	inventory.PVSizeBytes += growth
	inventory.ImageSizeBytes = target
	repeat, err := recoveryPoolTarget(inventory, 2<<30)
	if err != nil || repeat != target {
		t.Fatalf("retry grew the pool again: %d %v", repeat, err)
	}
	inventory.PVFreeBytes = 0
	if _, err := recoveryPoolTarget(inventory, 100<<30); err == nil {
		t.Fatal("unbounded recovery growth accepted")
	}
}
