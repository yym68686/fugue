package platformconfig

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestExternalPathReadinessConcurrentSharedTarget(t *testing.T) {
	const a, b, target = "one.example.test", "two.example.test", "target.example.test"
	const ip = "93.184.216.34"
	owner := ExternalPathTarget{a, target, "tenant", "owner", []string{a, b}}
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
	t.Run("recipient replays independent offers over its current plan", func(t *testing.T) {
		live := DNSArtifactPayload{Records: slices.Clone(records), ReadinessPlan: original}
		live.Policy.Scope = GlobalScopeKey
		live.Policy.DNSPlacementMode = DNSPlacementConsumerReadiness
		live.Policy.DNSReadiness = policy
		sum := "sha256:" + strings.Repeat("1", 64)
		ref := PreviousTrafficPublicationReference{ReleaseSetID: "parent", ReleaseSetDigest: sum, ReleaseID: "release", FencingToken: 1,
			RouteArtifactID: "routes", RouteArtifactDigest: sum, TLSArtifactID: "certs", TLSArtifactDigest: sum, DNSArtifactID: "records", DNSArtifactDigest: sum}
		offer := func(who ExternalPathTarget, paths []string, desired *DNSReadinessPlan) HostedScopeProposal {
			t.Helper()
			shape, members, e := externalPathShape(who, live)
			if e != nil {
				t.Fatal(e)
			}
			_, delta, e := ComposeExternalPathRequirements(who, original, desired, policy, root, paths, records, records)
			if e != nil {
				t.Fatal(e)
			}
			rows := func(paths []string) HostedDispatchRows {
				r := HostedDispatchRows{Owner: model.EdgeTLSAllowlistEntry{Hostname: who.Hostname, TenantID: who.TenantID, AppID: who.AppID, Status: "verified"}}
				for _, path := range paths {
					r.Routes = append(r.Routes, model.EdgeRouteIntent{Hostname: who.Hostname, PathPrefix: path, TenantID: who.TenantID, AppID: who.AppID})
				}
				return r
			}
			own := func(plan *DNSReadinessPlan) []DNSReadinessProbe {
				return slices.DeleteFunc(slices.Clone(plan.Probes), func(pr DNSReadinessProbe) bool { return pr.Hostname != who.Hostname })
			}
			p := HostedScopeProposal{Schema: ExternalPathProposalSchema, Host: who.Hostname, Base: ref, Before: rows(root), Prepared: rows(root), After: rows(paths),
				Records: records, Physical: members, Eligible: members, BaseProbes: own(original), HoldProbes: own(original), PathProbes: own(desired),
				ExternalPath: &ExternalPathAnchor{Target: who, Shape: shape}, Changes: delta.Edits}
			raw, e := json.Marshal(p)
			var saved HostedScopeProposal
			if e != nil || json.Unmarshal(raw, &saved) != nil {
				t.Fatal("cannot round-trip offer", e)
			}
			return saved
		}
		left, right := offer(owner, four, plan(four, root)), offer(peer, two, plan(root, two))
		run := func(p HostedScopeProposal, phase string, want *DNSReadinessPlan) {
			t.Helper()
			result, e := ComposeHostedDNSPhase(live, p, phase)
			if e != nil || !reflect.DeepEqual(result.Plan, *want) || !reflect.DeepEqual(live.QueryViews, result.Views) || !reflect.DeepEqual(live.Policy.DNSAnswerRules, result.Rules) {
				t.Fatalf("%s/%s: wrong receiver state: %v", p.Host, phase, e)
			}
			live.ReadinessPlan = &result.Plan
		}
		run(left, "prepare", plan(root, root))
		run(left, "forward", plan(four, root))
		run(right, "prepare", plan(four, root))
		run(right, "forward", plan(four, two))
		if _, e := ComposeHostedDNSPhase(live, left, "forward"); e == nil {
			t.Fatal("receiver replayed a stale forward edit")
		}
		run(left, "withdraw", plan(root, two))
		if _, e := ComposeHostedDNSPhase(live, left, "withdraw"); e == nil {
			t.Fatal("receiver replayed a stale withdrawal")
		}
		live.Records[0].TTL++
		if _, e := ComposeHostedDNSPhase(live, right, "withdraw"); e == nil {
			t.Fatal("old offer survived a wire TTL change")
		}
		if !reflect.DeepEqual(original, plan(root, root)) {
			t.Fatal("receiver mutated the immutable parent")
		}
	})
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
