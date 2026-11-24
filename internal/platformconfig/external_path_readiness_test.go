package platformconfig

import (
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestExternalPathReadinessConcurrentSharedTarget(t *testing.T) {
	const a, b, target = "one.example.test", "two.example.test", "target.example.test"
	const ip = "93.184.216.34"
	owner := ExternalPathTarget{a, target, "tenant", "owner"}
	policy := &DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 4, FactFreshnessSeconds: 120, MaxConcurrency: 2, MaxProbes: 128}
	root, four, two := []string{"/"}, []string{"/", "/a", "/b", "/c"}, []string{"/", "/live"}
	records := []DNSIntent{{Hostname: target, Type: "A", TTL: 60, Values: []string{ip}, RecordKind: "custom-domain-target", TenantID: "tenant", AppID: "owner"}}
	plan := func(left, right []string) *DNSReadinessPlan {
		row := DNSReadinessTarget{Family: "A", Address: ip, EdgeID: "edge-a", EdgeGroupID: "edge-group-a"}
		p := new(DNSReadinessPlan)
		for _, item := range []struct {
			name  string
			paths []string
		}{{a, left}, {b, right}} {
			for _, path := range item.paths {
				pr := DNSReadinessProbe{EdgeID: row.EdgeID, EdgeGroupID: row.EdgeGroupID, Address: ip, Hostname: item.name, Path: path, RouteDigest: "sha256:" + strings.Repeat("a", 64)}
				pr.ID, _ = DNSReadinessProbeID(pr)
				p.Probes = append(p.Probes, pr)
				row.ProbeIDs = append(row.ProbeIDs, pr.ID)
			}
		}
		slices.SortFunc(p.Probes, func(x, y DNSReadinessProbe) int { return strings.Compare(x.ID, y.ID) })
		slices.Sort(row.ProbeIDs)
		p.Records = []DNSReadinessRecord{{Hostname: target, Targets: []DNSReadinessTarget{row}, MinimumHealthyEdges: 1}}
		return p
	}
	apply := func(who ExternalPathTarget, current, want *DNSReadinessPlan, from, to []string) *DNSReadinessPlan {
		t.Helper()
		view, d, err := ComposeExternalPathRequirements(who, current, want, policy, from, to, records, records)
		if err != nil || len(d.Edits) != 1 || d.Edits[0].Record != target || len(d.Edits[0].Before) != len(from) || len(d.Edits[0].After) != len(to) {
			t.Fatalf("bad host patch: %+v %v", d, err)
		}
		return view
	}
	original := plan(root, root)
	forward := apply(owner, original, plan(four, root), root, four)
	peer := owner
	peer.Hostname = b
	both := apply(peer, forward, plan(root, two), root, two)
	if !reflect.DeepEqual(both, plan(four, two)) {
		t.Fatal("peer publication replaced first host")
	}
	back := apply(owner, both, original, four, root)
	if !reflect.DeepEqual(back, plan(root, two)) || !reflect.DeepEqual(original, plan(root, root)) {
		t.Fatal("withdrawal lost concurrent shared-target update")
	}
	for _, name := range []string{"ttl", "owner", "address", "managed", "wrong-target", "other-record", "single-publication", "new-host-reference", "changed-quorum", "missing-record", "orphan-address"} {
		t.Run(name, func(t *testing.T) {
			one, two := plan(root, root), plan(four, root)
			rr := slices.Clone(records)
			o := owner
			switch name {
			case "ttl":
				rr[0].TTL++
			case "owner":
				rr[0].AppID = "foreign"
			case "address":
				rr[0].Values = []string{"93.184.216.35"}
			case "managed":
				rr[0].RecordKind = "hosted"
			case "wrong-target":
				o.Target = "another.example.test"
			case "other-record":
				r := records[0]
				r.Hostname = a
				rr = append(rr, r)
			case "single-publication":
				one.Records[0].Targets[0].RequireSinglePublication = true
				two.Records[0].Targets[0].RequireSinglePublication = true
			case "new-host-reference":
				r := two.Records[0]
				r.Hostname = "third.example.test"
				two.Records = append(two.Records, r)
			case "changed-quorum":
				two.Records[0].MinimumHealthyEdges++
			case "missing-record":
				rr = nil
			case "orphan-address":
				rr[0].Values = []string{ip, "93.184.216.35"}
			}
			if v, _, e := ComposeExternalPathRequirements(o, one, two, policy, root, four, records, rr); e == nil || v != nil {
				t.Fatal("accepted an unrelated authority or address change")
			}
		})
	}
}
