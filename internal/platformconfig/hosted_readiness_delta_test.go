package platformconfig

import (
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestHostedReadinessDeltaIsolatesSharedRequirements(t *testing.T) {
	const h, alias, other = "owner.example.test", "alias.example.test", "other.example.test"
	policy := &DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 4, FactFreshnessSeconds: 120, MaxConcurrency: 2, MaxProbes: 128}
	before, after := []string{"/"}, []string{"/", "/blue"}
	fixture := func(paths []string, scenario string) *DNSReadinessPlan {
		plan := new(DNSReadinessPlan)
		probe := func(name, path string) string {
			p := DNSReadinessProbe{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", Address: "93.184.216.34", Hostname: name, Path: path, RouteDigest: "sha256:" + strings.Repeat("a", 64)}
			if scenario == "freshness" && name == h {
				p.FactMaxAgeSeconds = 30
			}
			if scenario == "status" && name == h {
				p.State = "unavailable"
			}
			if scenario == "different-foreign-probe" && name == other {
				p.RouteDigest = "sha256:" + strings.Repeat("b", 64)
			}
			p.ID, _ = DNSReadinessProbeID(p)
			plan.Probes = append(plan.Probes, p)
			return p.ID
		}
		var ids []string
		for _, path := range paths {
			ids = append(ids, probe(h, path))
		}
		outside := probe(other, "/")
		row := func(name string, ids []string) DNSReadinessRecord {
			return DNSReadinessRecord{Hostname: name, MinimumHealthyEdges: 1, Targets: []DNSReadinessTarget{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", Address: "93.184.216.34", Family: "A", ProbeIDs: slices.Clone(ids)}}}
		}
		plan.Records = []DNSReadinessRecord{row(h, ids), row(alias, append(slices.Clone(ids), outside)), row(other, []string{outside})}
		switch scenario {
		case "foreign-pending":
			plan.Records[2].MinimumHealthyEdges = 2
		case "removed-alias":
			plan.Records = slices.Delete(plan.Records, 1, 2)
		case "alias-detached":
			plan.Records[1] = row(alias, []string{outside})
		case "incomplete-alias":
			plan.Records[1] = row(alias, []string{ids[0], outside})
		case "alias-quorum":
			plan.Records[1].MinimumHealthyEdges = 2
		case "target-identity":
			plan.Records[0].Targets[0].FailureDomains = map[string]string{"provider": "new"}
		}
		return plan
	}
	base, want := fixture(before, ""), fixture(after, "foreign-pending")
	overlay, _, e := ComposeHostedReadinessRequirements(h, base, want, policy, before, after)
	if e != nil || overlay.Records[2].MinimumHealthyEdges != base.Records[2].MinimumHealthyEdges || overlay.Records[2].MinimumHealthyEdges == want.Records[2].MinimumHealthyEdges {
		t.Fatal("overlay published an unrelated pending change", e)
	}
	backView, _, e := ComposeHostedReadinessRequirements(h, overlay, base, policy, after, before)
	if e != nil || len(backView.Probes) != len(base.Probes) {
		t.Fatal("cannot restore original requirements", e)
	}
	for i, rec := range base.Records {
		if !reflect.DeepEqual(rec.Targets[0].ProbeIDs, backView.Records[i].Targets[0].ProbeIDs) &&
			!slices.Equal(slices.Sorted(slices.Values(rec.Targets[0].ProbeIDs)), slices.Sorted(slices.Values(backView.Records[i].Targets[0].ProbeIDs))) {
			t.Fatal("restored a different dependency set")
		}
	}
	overlay.Records[2].Targets[0].ProbeIDs[0] = "changed in caller"
	if base.Records[2].Targets[0].ProbeIDs[0] == "changed in caller" || want.Records[2].Targets[0].ProbeIDs[0] == "changed in caller" {
		t.Fatal("overlay aliases immutable source storage")
	}
	original, _ := Digest(base)
	desired, _ := Digest(want)
	d, err := PlanHostedReadinessDelta(h, base, want, policy, before, after)
	if err != nil || len(d.Edits) != 2 || d.Edits[0].Record != alias || d.Edits[1].Record != h || d.BasePlanDigest != original || d.DesiredPlanDigest != desired {
		t.Fatal(d, err)
	}
	undo, err := PlanHostedReadinessDelta(h, want, base, policy, after, before)
	if err != nil || undo.BasePlanDigest != desired || undo.DesiredPlanDigest != original {
		t.Fatal(undo, err)
	}
	for i, e := range d.Edits {
		r := undo.Edits[i]
		if len(e.Before) != 1 || len(e.After) != 2 || r.Record != e.Record || !reflect.DeepEqual(r.Before, e.After) || !reflect.DeepEqual(r.After, e.Before) {
			t.Fatalf("incomplete scoped inverse: %+v", r)
		}
	}
	b2, _ := Digest(base)
	w2, _ := Digest(want)
	if b2 != original || w2 != desired {
		t.Fatal("modified input")
	}
	for _, bad := range []string{"removed-alias", "alias-detached", "incomplete-alias", "alias-quorum", "target-identity", "different-foreign-probe", "freshness", "status"} {
		t.Run(bad, func(t *testing.T) {
			d, e := PlanHostedReadinessDelta(h, base, fixture(after, bad), policy, before, after)
			if e == nil || len(d.Edits) != 0 {
				t.Fatal("accepted non-path change", d, e)
			}
		})
	}
	for _, bad := range [][]string{nil, {"", "/blue"}, {"/blue"}, {"/", "/blue", "/blue"}, {"/"}, {"/", "/wrong"}} {
		if _, e := PlanHostedReadinessDelta(h, base, want, policy, before, bad); e == nil {
			t.Fatal("accepted wrong desired paths", bad)
		}
	}
}
