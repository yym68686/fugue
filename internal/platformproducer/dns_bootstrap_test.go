package platformproducer

import (
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSBootstrapOnlyDelta(t *testing.T) {
	fixture := func() ProjectionPolicyInput {
		seed := model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}
		return ProjectionPolicyInput{Generation: "before", DNSQueryPolicy: &platformconfig.DNSQueryPolicy{
			DynamicQuality: &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic"},
			OrderedProjection: &platformconfig.DNSOrderedProjection{DefaultOrder: seed, Overrides: []platformconfig.DNSOrderOverride{
				{NodeID: "dns-a", Hostname: "a.example.test", Type: "A", Order: *model.CloneDNSPhysicalOrder(&seed)},
				{NodeID: "dns-a", Hostname: "b.example.test", Type: "A", Order: *model.CloneDNSPhysicalOrder(&seed)},
			}},
		}}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*ProjectionPolicyInput, *ProjectionPolicyInput)
		want   bool
	}{
		{"only mark one seed", func(_, _ *ProjectionPolicyInput) {}, true},
		{"changed edge order", func(_, n *ProjectionPolicyInput) {
			n.DNSQueryPolicy.OrderedProjection.Overrides[0].Order.OrderedEdgeIDs[0] = "other"
		}, false},
		{"changed default", func(_, n *ProjectionPolicyInput) {
			n.DNSQueryPolicy.OrderedProjection.DefaultOrder.OrderedEdgeIDs[0] = "other"
		}, false},
		{"changed quality", func(_, n *ProjectionPolicyInput) { n.DNSQueryPolicy.DynamicQuality.RefreshConcurrency++ }, false},
		{"removed fixed record", func(_, n *ProjectionPolicyInput) {
			o := n.DNSQueryPolicy.OrderedProjection
			o.Overrides = o.Overrides[:1]
		}, false},
		{"unmarked previous seed", func(o, _ *ProjectionPolicyInput) {
			o.DNSQueryPolicy.OrderedProjection.Overrides[1].BootstrapOnly = true
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, after := fixture(), fixture()
			after.Generation = "after"
			after.DNSQueryPolicy.OrderedProjection.Overrides[0].BootstrapOnly = true
			tc.mutate(&before, &after)
			if got := dnsBootstrapOnlyDelta(before, after); got != tc.want {
				t.Fatalf("authorized = %v, want %v", got, tc.want)
			}
			if !after.DNSQueryPolicy.OrderedProjection.Overrides[0].BootstrapOnly {
				t.Fatal("mutated decoded successor")
			}
		})
	}
}
