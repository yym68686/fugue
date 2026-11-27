package localpvsafety

import (
	"slices"
	"strings"

	"fugue/internal/model"
)

// EvaluateDecommission checks whether the host and control-plane evidence permits
// removing the backing image. Unknown claim bindings fail closed.
func EvaluateDecommission(in model.LocalPVInventory) (bool, []string) {
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
	normalized := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		if reason = strings.TrimSpace(reason); reason != "" {
			normalized = append(normalized, reason)
		}
	}
	slices.Sort(normalized)
	normalized = slices.Compact(normalized)
	return len(normalized) == 0, normalized
}
