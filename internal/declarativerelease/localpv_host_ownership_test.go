package declarativerelease

import (
	"os"
	"slices"
	"testing"
)

func TestLocalPVHostReleaseSourceOwnership(t *testing.T) {
	f, err := os.Open("../../deploy/releases/components.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	registry, err := DecodeRegistry(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		want []string
	}{
		{"internal/localpvsafety/host/localpv_unit.go", []string{"api"}},
		{"internal/localpvsafety/host/localpv_unit.py", []string{"api"}},
		{"internal/localpvsafety/inventory/scheduler.go", []string{"api", "controller"}},
		{"internal/store/localpv_inventory_tasks.go", []string{"api", "controller", "observation-worker", "ssh-control"}},
		{"internal/localpvsafety/host/test_localpv_unit.py", nil},
		{"internal/localpvsafety/hostcontract/prepare_test.go", nil},
		{"scripts/prepare_fugue_lvm_localpv_node.sh", nil},
	} {
		var got []string
		for _, component := range registry.Components {
			for _, root := range component.SourceRoots {
				if pathMatchesRoot(tc.path, root) {
					got = append(got, component.ID)
					break
				}
			}
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("source owners for %s = %v, want %v", tc.path, got, tc.want)
		}
	}
}
