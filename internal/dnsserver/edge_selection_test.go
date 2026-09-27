package dnsserver

import (
	"testing"

	"fugue/internal/platformconfig"
)

func TestDNSEdgeSelectionRequiresSignedReadinessTargets(t *testing.T) {
	constraint := platformconfig.EdgeSelectionConstraint{Hostname: "app.example.test", MinCandidates: 2}
	plan := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{
		{ID: "probe-a", Hostname: "app.example.test", EdgeID: "edge-a", EdgeGroupID: "edge-group-a", Address: "8.8.8.8"},
		{ID: "probe-b", Hostname: "app.example.test", EdgeID: "edge-b", EdgeGroupID: "edge-group-b", Address: "8.8.4.4"},
		{ID: "probe-b-api", Hostname: "app.example.test", EdgeID: "edge-b", EdgeGroupID: "edge-group-b", Address: "8.8.4.4", Path: "/api"},
	}, Records: []platformconfig.DNSReadinessRecord{{Hostname: "app.example.test", Targets: []platformconfig.DNSReadinessTarget{
		{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", Address: "8.8.8.8", ProbeIDs: []string{"probe-a"}},
		{EdgeID: "edge-b", EdgeGroupID: "edge-group-b", Address: "8.8.4.4", ProbeIDs: []string{"probe-b", "probe-b-api"}},
	}}}}
	if !dnsEdgeSelectionRequirementsComplete(plan, []platformconfig.EdgeSelectionConstraint{constraint}) {
		t.Fatal("complete readiness requirements were rejected")
	}
	plan.Probes[1].Hostname = "other.example.test"
	plan.Probes[2].Hostname = "other.example.test"
	if dnsEdgeSelectionRequirementsComplete(plan, []platformconfig.EdgeSelectionConstraint{constraint}) {
		t.Fatal("unrelated hostname or repeated path counted as an Edge")
	}
	plan.Probes[1].Hostname = constraint.Hostname
	plan.Records[0].Targets = plan.Records[0].Targets[:1]
	if dnsEdgeSelectionRequirementsComplete(plan, []platformconfig.EdgeSelectionConstraint{constraint}) {
		t.Fatal("orphaned readiness probe counted as a target")
	}
	if dnsEdgeSelectionRequirementsComplete(nil, []platformconfig.EdgeSelectionConstraint{constraint}) {
		t.Fatal("missing readiness plan was accepted")
	}
	if !dnsEdgeSelectionRequirementsComplete(nil, nil) {
		t.Fatal("legacy policy requires no Edge selection plan")
	}
}
