package inventory

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/localpvsafety"
	"fugue/internal/model"
)

// AssessObservedSnapshot refreshes the safety verdict for a reader without
// updating the persisted snapshot.
func AssessObservedSnapshot(in model.LocalPVInventory, now time.Time) model.LocalPVInventory {
	in.SafeToDecommission, in.UnsafeReasons = DecommissionSafety(in)
	if !localpvsafety.IsFresh(in.ObservedAt, now, localpvsafety.DefaultInventoryTTL) {
		in.SafeToDecommission = false
		in.UnsafeReasons = uniqueNonEmptyStrings(append(in.UnsafeReasons, "inventory_stale"))
	}
	return in
}

// DecommissionSafety classifies reported evidence without writing inventory.
func DecommissionSafety(in model.LocalPVInventory) (bool, []string) {
	reasons := append([]string(nil), in.UnsafeReasons...)
	if in.LVCount != 0 {
		reasons = append(reasons, "active_lvs_present")
	}
	if in.ActiveLVCount != 0 {
		reasons = append(reasons, "active_lvs_present")
	}
	if in.BoundPVCount < 0 {
		reasons = append(reasons, "bound_pv_count_unknown")
	} else if in.BoundPVCount != 0 {
		reasons = append(reasons, "bound_pvs_present")
	}
	if strings.TrimSpace(in.ImagePath) == "" {
		reasons = append(reasons, "image_path_missing")
	}
	if strings.TrimSpace(in.LoopDevice) == "" {
		reasons = append(reasons, "loop_device_missing")
	}
	if strings.TrimSpace(in.LoopBackingFile) != "" && strings.TrimSpace(in.ImagePath) != "" && strings.TrimSpace(in.LoopBackingFile) != strings.TrimSpace(in.ImagePath) {
		reasons = append(reasons, "loop_backing_file_mismatch")
	}
	reasons = uniqueNonEmptyStrings(reasons)
	return len(reasons) == 0, reasons
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
