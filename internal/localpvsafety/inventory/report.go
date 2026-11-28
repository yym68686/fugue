package inventory

import (
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
)

// PrepareReport binds the reporter identity, resolves ambiguous legacy binding
// evidence through the supplied read adapter, and recomputes decommission safety.
// Authentication and persistence stay with the caller.
func PrepareReport(in model.LocalPVInventory, updater model.NodeUpdater, known *bool, now func() time.Time, observeBindings func(*model.LocalPVInventory)) model.LocalPVInventory {
	for _, field := range []struct {
		value    *string
		fallback string
	}{
		{&in.NodeID, updater.MachineID},
		{&in.ClusterNodeName, updater.ClusterNodeName},
		{&in.RuntimeID, updater.RuntimeID},
	} {
		*field.value = strings.TrimSpace(*field.value)
		if *field.value == "" {
			*field.value = strings.TrimSpace(field.fallback)
		}
	}
	in.ReportedByNodeUpdaterID = updater.ID
	// Older reports may contain zero alongside a failed kubectl observation.
	if (known != nil && !*known) || in.BoundPVCount < 0 ||
		slices.Contains(in.UnsafeReasons, "kubectl_pv_unavailable") ||
		slices.Contains(in.UnsafeReasons, "bound_pv_count_unknown") {
		in.UnsafeReasons = slices.Clone(in.UnsafeReasons)
		observeBindings(&in)
	} else {
		in.BoundPVCountKnown = true
	}
	if in.ObservedAt.IsZero() {
		in.ObservedAt = now().UTC()
	}
	in.SafeToDecommission, in.UnsafeReasons = DecommissionSafety(in)
	return in
}
