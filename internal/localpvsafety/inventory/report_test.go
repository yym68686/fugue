package inventory

import (
	"errors"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestReporterPersistsOnlyPreparedSnapshotAndPropagatesFailure(t *testing.T) {
	storeError := errors.New("snapshot unavailable")
	known := false
	calls := 0
	reporter := NewReporter(func(in model.LocalPVInventory) (model.LocalPVInventory, error) {
		calls++
		if in.ReportedByNodeUpdaterID != "authenticated" || in.ClusterNodeName != "node" ||
			in.BoundPVCountKnown || in.BoundPVCount != -1 || in.SafeToDecommission {
			t.Fatalf("writer received unprepared or unsafe input: %+v", in)
		}
		return model.LocalPVInventory{}, storeError
	})
	_, err := reporter.Report(model.LocalPVInventory{ReportedByNodeUpdaterID: "untrusted", SafeToDecommission: true},
		model.NodeUpdater{ID: "authenticated", ClusterNodeName: "node"}, &known, nil)
	if calls != 1 || !errors.Is(err, storeError) {
		t.Fatalf("writes=%d, error=%v", calls, err)
	}
}

func TestPrepareReportSuppliesIdentityAndRecomputesEligibility(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("test", 3600))
	source := model.LocalPVInventory{
		NodeID: " reported-node ", ImagePath: "/storage/volume.img",
		LoopDevice: "/dev/loop-test", LVCount: 1, SafeToDecommission: true,
		ReportedByNodeUpdaterID: "untrusted",
	}
	updater := model.NodeUpdater{ID: "authenticated", MachineID: "machine", ClusterNodeName: " node ", RuntimeID: " runtime "}
	result := prepareReport(source, updater, nil, func() time.Time { return now }, func(*model.LocalPVInventory) {
		t.Fatal("legacy report with a known zero must not require a Kubernetes observation")
	})
	if result.NodeID != "reported-node" || result.ClusterNodeName != "node" || result.RuntimeID != "runtime" || result.ReportedByNodeUpdaterID != "authenticated" {
		t.Fatalf("unexpected report identity: %+v", result)
	}
	if !result.BoundPVCountKnown || result.SafeToDecommission || !slices.Equal(result.UnsafeReasons, []string{"active_lvs_present"}) {
		t.Fatalf("report safety was not recomputed: %+v", result)
	}
	if !result.ObservedAt.Equal(now) || result.ObservedAt.Location() != time.UTC {
		t.Fatalf("unexpected observation time: %v", result.ObservedAt)
	}
}

func TestPrepareReportChecksUnknownBindingsThroughReadAdapter(t *testing.T) {
	unknown := false
	for _, tc := range []struct {
		name   string
		known  *bool
		count  int
		reason string
	}{
		{name: "explicitly unknown", known: &unknown},
		{name: "negative sentinel", count: -1},
		{name: "legacy kubectl failure", reason: "kubectl_pv_unavailable"},
		{name: "legacy count unknown", reason: "bound_pv_count_unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			in := model.LocalPVInventory{BoundPVCount: tc.count, UnsafeReasons: []string{tc.reason}, ObservedAt: at}
			calls := 0
			got := prepareReport(in, model.NodeUpdater{ClusterNodeName: "node"}, tc.known, func() time.Time {
				t.Fatal("must preserve the supplied timestamp")
				return time.Time{}
			}, func(report *model.LocalPVInventory) {
				calls++
				if report.ClusterNodeName != "node" {
					t.Fatal("observer did not receive the resolved node")
				}
				report.UnsafeReasons[0] = ""
				report.BoundPVCount = 2
				report.BoundPVCountKnown = true
			})
			if calls != 1 || got.BoundPVCount != 2 || !got.BoundPVCountKnown || !slices.Contains(got.UnsafeReasons, "bound_pvs_present") ||
				in.UnsafeReasons[0] != tc.reason || !got.ObservedAt.Equal(at) {
				t.Fatalf("observation or input isolation failed: calls=%d result=%+v source=%+v", calls, got, in)
			}
		})
	}
}
