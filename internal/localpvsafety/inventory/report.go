package inventory

import (
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
)

// Reporter owns the report command after the transport authenticates the node
// updater. Its write capability is limited to replacing one inventory snapshot.
type Reporter struct {
	save func(model.LocalPVInventory) (model.LocalPVInventory, error)
}

func NewReporter(save func(model.LocalPVInventory) (model.LocalPVInventory, error)) Reporter {
	return Reporter{save: save}
}

func (r Reporter) Report(in model.LocalPVInventory, updater model.NodeUpdater, known *bool, observeBindings func(*model.LocalPVInventory)) (model.LocalPVInventory, error) {
	return r.save(prepareReport(in, updater, known, time.Now, observeBindings))
}

// prepareReport binds the reporter identity, resolves ambiguous legacy binding
// evidence through the read adapter, and recomputes decommission safety.
func prepareReport(in model.LocalPVInventory, updater model.NodeUpdater, known *bool, now func() time.Time, observeBindings func(*model.LocalPVInventory)) model.LocalPVInventory {
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
		if observeBindings != nil {
			observeBindings(&in)
		} else {
			in.BoundPVCount, in.BoundPVCountKnown = -1, false
		}
	} else {
		in.BoundPVCountKnown = true
	}
	if in.ObservedAt.IsZero() {
		in.ObservedAt = now().UTC()
	}
	in.SafeToDecommission, in.UnsafeReasons = DecommissionSafety(in)
	return in
}
