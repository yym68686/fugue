package store

import (
	"fugue/internal/model"
	"testing"
)

func TestManagedPostgresStorageGrowthKeepsExistingPlacement(t *testing.T) {
	current := &model.AppPostgresSpec{StorageSize: "5Gi", StorageClassName: "local", PrimaryNodeName: "worker"}
	for _, tc := range []struct {
		name, node, source, target, size, class string
		want                                    bool
	}{
		{"same pin", "worker", "r1", "r1", "10Gi", "local", true},
		{"unspecified pin", "", "r1", "r1", "10Gi", "local", true},
		{"new pin", "other", "r1", "r1", "10Gi", "local", false},
		{"new runtime", "worker", "r1", "r2", "10Gi", "local", false},
		{"new class", "worker", "r1", "r1", "10Gi", "remote", false},
		{"same capacity", "worker", "r1", "r1", "5120Mi", "local", false},
		{"shrink", "worker", "r1", "r1", "4Gi", "local", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			desired := *current
			desired.StorageSize = tc.size
			desired.StorageClassName = tc.class
			if got := ManagedPostgresStorageGrowthInPlace(current, &desired, tc.source, tc.target, tc.node); got != tc.want {
				t.Fatalf("got %t want %t", got, tc.want)
			}
		})
	}
}
