package platformconfig_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
)

func TestExternalWithheldRouteTLSUsesExactWorkerAssignment(t *testing.T) {
	s, p := pc.ExternalWithheldRosterFixtureForTest(t)
	held := p.ExternalPath.Held[0]
	draft, err := pc.Digest(p)
	if err != nil {
		t.Fatal(err)
	}
	img, err := routeartifact.FreezeHostedProgram(s, p, draft)
	if err != nil {
		t.Fatal(err)
	}
	const enrollment = "0123456789abcdef0123456789abcdef"
	build := func(kind string, topology platformcontrol.ExpectedConsumerTopology) model.PlatformExpectedConsumerSet {
		t.Helper()
		set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: p.Base.ReleaseSetID, ArtifactReleaseID: p.Base.ReleaseID, ArtifactKind: kind, ScopeKey: "global", Generation: "v1", Revision: 1, PreparedAt: time.Now(), Topology: topology})
		if err != nil {
			t.Fatal(err)
		}
		return set
	}
	nodes := []model.EdgeNode{{ID: held.EdgeID, EdgeGroupID: held.EdgeGroupID, PublicIPv4: held.Address}}
	topology := platformcontrol.ExpectedConsumerTopology{HostedRouteTLSOnly: nodes, WorkerEnrollments: map[string]string{held.EdgeID: enrollment}}
	for _, spec := range []struct{ kind, child, hash string }{
		{model.PlatformArtifactKindEdgeRouteBundle, p.Base.RouteArtifactID, p.Base.RouteArtifactDigest},
		{model.PlatformArtifactKindCaddyRouteConfig, p.Base.TLSArtifactID, p.Base.TLSArtifactDigest},
	} {
		t.Run(spec.kind, func(t *testing.T) {
			set := build(spec.kind, topology)
			who := platformcontrol.PlatformComponentIdentityClaims{Component: model.PlatformConsumerComponentEdgeWorker, NodeID: held.EdgeID, ScopeKey: "global", ArtifactKinds: []string{spec.kind}, WorkerEnrollmentID: enrollment}
			a := model.PlatformConsumerAssignment{ScopeKey: "global", ReleaseChannel: "full", ReleaseSetID: p.Base.ReleaseSetID, ArtifactReleaseID: p.Base.ReleaseID, ArtifactID: spec.child, ArtifactKind: spec.kind, ContentHash: spec.hash, FencingToken: p.Base.FencingToken, ExpectedConsumerSetID: set.ID, Revision: set.Revision, ExpectedGeneration: set.ExpectedGeneration}
			for _, phase := range []string{"prepare", "forward", "withdraw"} {
				d, pin, err := routeartifact.AddressHostedPreparation(p, draft, img.Digest, img.Wire, phase, who, a, set)
				if err != nil || pin == "" || !d.SelectedWorkerRequired || len(d.Parts) != 1 || d.Parts[0].Recipient.Node != held.EdgeID || d.Parts[0].Content.DNS != nil {
					t.Fatalf("%s lost a required route/TLS part or gave the held Worker DNS: %+v, %v", phase, d, err)
				}
				wire, err := json.Marshal(d)
				var decoded routeartifact.HostedDelivery
				if err != nil || json.Unmarshal(wire, &decoded) != nil || decoded.SelectedWorkerRequired {
					t.Fatalf("%s let a client control the selected physical Worker check", phase)
				}
			}
			for _, bad := range []string{"unmarked", "ordinary optional", "group", "enrollment", "fence", "hold_address", "eligible", "missing"} {
				t.Run(bad, func(t *testing.T) {
					current, assignment, identity, proposal := set, a, who, p
					current.Consumers = slices.Clone(set.Consumers)
					switch bad {
					case "unmarked":
						current.Consumers[0].HostedRouteTLSOnly = false
					case "ordinary optional":
						top := topology
						top.EdgeNodes, top.HostedRouteTLSOnly = slices.Clone(nodes), nil
						top.EdgeNodes[0].Draining = true
						current = build(spec.kind, top)
						assignment.ExpectedConsumerSetID = current.ID
					case "group":
						current.Consumers[0].Cohort = "edge-group-other"
					case "enrollment":
						identity.WorkerEnrollmentID = ""
					case "fence":
						assignment.FencingToken++
					case "hold_address":
						proposal.Physical = slices.Clone(p.Physical)
						for i := range proposal.Physical {
							if proposal.Physical[i].EdgeID == held.EdgeID {
								proposal.Physical[i].Address = "1.1.1.1"
							}
						}
					case "eligible":
						proposal.Eligible = append(slices.Clone(p.Eligible), held)
					case "missing":
						current.Consumers = nil
					}
					if _, _, err := routeartifact.AddressHostedPreparation(proposal, draft, img.Digest, img.Wire, "prepare", identity, assignment, current); err == nil {
						t.Fatal("accepted a non-authorized physical recipient")
					}
				})
			}
		})
	}
}

// Baselines come from the complete original source, not the forward command
// and not an invented positive observation. Exercise regular/held/external
// target projections using real compiler output and every physical recipient.
func TestHostedMembershipBaselinesSurviveIndependentParentRenewal(t *testing.T) {
	run := func(t *testing.T, s pc.HostedAddressSource, p pc.HostedScopeProposal) {
		t.Helper()
		bind := func(s pc.HostedAddressSource, p pc.HostedScopeProposal) (*routeartifact.HostedPhaseResolver, routeartifact.CompiledHostedProgram) {
			t.Helper()
			p.Schema = pc.HostedMembershipProposalSchema
			d, e := pc.Digest(p)
			if e != nil {
				t.Fatal(e)
			}
			im, e := routeartifact.FreezeHostedMembershipProgram(s, p, d)
			if e != nil {
				t.Fatal(e)
			}
			same, e := routeartifact.FreezeHostedMembershipProgram(s, p, d)
			if e != nil || !bytes.Equal(im.Wire, same.Wire) {
				t.Fatal("nondeterministic member image", e)
			}
			u, e := routeartifact.OpenHostedPhase(p, d, im.Digest, im.Wire, "forward")
			if e != nil {
				t.Fatal(e)
			}
			if _, e := routeartifact.FreezeHostedProgram(s, p, d); e == nil {
				t.Fatal("legacy encoder accepted a v2 draft")
			}
			old := p
			old.Schema = pc.HostedProposalSchema
			legacy, e := routeartifact.FreezeHostedProgram(s, old, d)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = routeartifact.OpenHostedPhase(p, d, legacy.Digest, legacy.Wire, "forward"); e == nil {
				t.Fatal("missing promised baselines accepted")
			}
			return u, im
		}
		one, im := bind(s, p)
		s.Publication.ReleaseSetID += "-next"
		s.Publication.ReleaseID += "-next"
		s.Publication.FencingToken++
		s.Authority.LKGID += "-next"
		p.Base, p.Authority = s.Publication, s.Authority
		two, next := bind(s, p)
		if im.Digest == next.Digest {
			t.Fatal("replayed old image digest on different parent")
		}
		counts := map[string]int{}
		for _, r := range one.Required() {
			d := routeartifact.HostedDelivery{Host: p.Host, Parts: []routeartifact.HostedDeliveryPart{{Recipient: r}}}
			x, y := one.MemberBaseline(d), two.MemberBaseline(d)
			if x == "" || x != y {
				t.Fatal("parent changed host baseline", r)
			}
			counts[r.Kind]++
			if r.Kind == model.PlatformArtifactKindCaddyRouteConfig {
				want, e := pc.Digest(p.Before.Owner)
				if e != nil || x != want {
					t.Fatal("certificate owner pin differs")
				}
			}
		}
		if len(counts) != 3 {
			t.Fatal("incomplete physical coverage", counts)
		}
	}
	t.Run("managed-four-paths", func(t *testing.T) {
		pc.HostedFourPathFixtureForTest(t, func(s pc.HostedAddressSource, p pc.HostedScopeProposal) { run(t, s, p) })
	})
	t.Run("stable-four-paths", func(t *testing.T) { pc.HostedStableFourPathFixtureForTest(t, run) })
	t.Run("external-shared-held", func(t *testing.T) { s, p := pc.ExternalHeldFixtureForTest(t); run(t, s, p) })
	t.Run("physical-worker-already-withheld-from-dns", func(t *testing.T) {
		s, p := pc.ExternalWithheldRosterFixtureForTest(t)
		run(t, s, p)
	})
}

func TestExternalHeldCompilerProgramReachesEveryRecipient(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture func(*testing.T) (pc.HostedAddressSource, pc.HostedScopeProposal)
	}{
		{"newly withheld", pc.ExternalHeldFixtureForTest},
		{"already withheld", pc.ExternalWithheldRosterFixtureForTest},
	} {
		t.Run(tc.name, func(t *testing.T) { assertExternalHeldRecipients(t, tc.fixture) })
	}
}

func assertExternalHeldRecipients(t *testing.T, fixture func(*testing.T) (pc.HostedAddressSource, pc.HostedScopeProposal)) {
	t.Helper()
	s, p := fixture(t)
	digest, err := pc.Digest(p)
	if err != nil {
		t.Fatal(err)
	}
	img, err := routeartifact.FreezeHostedProgram(s, p, digest)
	if err != nil || len(img.Wire) == 0 || img.Prepared == img.Forward {
		t.Fatal("freeze", err)
	}
	prepared, forward, err := routeartifact.BuildHostedProgramUnits(s, p)
	if err != nil {
		t.Fatal(err)
	}
	for i, phase := range []routeartifact.HostedPhase{prepared, forward} {
		parts, err := routeartifact.HostedRecipientInputs(p, phase)
		if err != nil {
			t.Fatal(i, err)
		}
		dns, tls, routes := 0, 0, 0
		for who, part := range parts {
			switch {
			case part.DNS != nil:
				dns++
				base, err := routeartifact.HostedDNSMemberBase(p.Host, who.Node, who.Group, s.Prior)
				if err != nil || !reflect.DeepEqual(base[who].Records, part.Records) {
					t.Fatal("selected DNS parent and signed program disagree about declaration", err)
				}
				if !slices.Equal(part.DNS.Withheld, p.ExternalPath.Held) || !reflect.DeepEqual(part.Records, p.Records) {
					t.Fatal("DNS declaration or hold lost")
				}
				for _, r := range part.DNS.Plan.Records {
					for _, target := range r.Targets {
						if target.Address == p.ExternalPath.Held[0].Address {
							t.Fatal("DNS hold recovered without a release")
						}
					}
				}
			case len(part.TLS) > 0:
				tls++
				if !reflect.DeepEqual(part.TLS, []model.EdgeTLSAllowlistEntry{p.Before.Owner}) {
					t.Fatal("changed certificate ownership")
				}
			case len(part.Routes) > 0:
				routes++
				if len(part.Routes) != []int{1, 4}[i] {
					t.Fatalf("phase %d: %d routes", i, len(part.Routes))
				}
			}
		}
		if dns == 0 || tls != len(p.Physical) || routes != tls {
			t.Fatal("missing physical recipients", dns, tls, routes)
		}
	}
}

func TestStableFourPathProgramOnRefreshedRoster(t *testing.T) {
	pc.HostedStableFourPathFixtureForTest(t, func(t *testing.T, s pc.HostedAddressSource, p pc.HostedScopeProposal) {
		d, err := pc.Digest(p)
		if err != nil {
			t.Fatal(err)
		}
		img, err := routeartifact.FreezeHostedProgram(s, p, d)
		if err != nil {
			t.Fatal("freeze path-only program", err)
		}
		a, b, err := routeartifact.BuildHostedProgramUnits(s, p)
		if err != nil {
			t.Fatal(err)
		}
		for i, phase := range []routeartifact.HostedPhase{a, b} {
			parts, err := routeartifact.HostedRecipientInputs(p, phase)
			if err != nil {
				t.Fatal(err)
			}
			kinds := [3]int{}
			for _, v := range parts {
				switch {
				case v.DNS != nil:
					kinds[0]++
					if !reflect.DeepEqual(v.Records, p.Records) || len(v.DNS.Withheld) != 0 {
						t.Fatal("changed wire or eligible roster")
					}
				case len(v.TLS) > 0:
					kinds[1]++
					if !reflect.DeepEqual(v.TLS, []model.EdgeTLSAllowlistEntry{p.Before.Owner}) {
						t.Fatal("changed certificate owner")
					}
				case len(v.Routes) > 0:
					kinds[2]++
					if len(v.Routes) != []int{1, 4}[i] {
						t.Fatal("wrong route count", i)
					}
				}
			}
			if kinds[0] == 0 || kinds[1] != len(p.Physical) || kinds[2] != kinds[1] {
				t.Fatal("incomplete delivery", kinds)
			}
		}
		groups := map[string]model.EdgeRouteBundle{}
		for _, n := range p.Physical {
			g, e := routeartifact.MaterializeForGroup(*s.PriorRoute, n.EdgeGroupID)
			if e != nil {
				t.Fatal(e)
			}
			groups[n.EdgeGroupID] = g
		}
		dns := s.Prior
		for _, phase := range []string{"prepare", "forward", "withdraw"} {
			v, e := routeartifact.ComposeHostedExecutorChange(groups, dns, p, phase, d, img.Digest, img.Wire)
			if e != nil {
				t.Fatal("execute", phase, e)
			}
			if !reflect.DeepEqual(v.DNS.Views, s.Prior.QueryViews) || !reflect.DeepEqual(v.DNS.Rules, s.Prior.Policy.DNSAnswerRules) {
				t.Fatal("changed DNS", phase)
			}
			for k, g := range v.Groups {
				prior := groups[k]
				foreign := func(rows []model.EdgeRouteBinding) []model.EdgeRouteBinding {
					return slices.DeleteFunc(slices.Clone(rows), func(row model.EdgeRouteBinding) bool { return row.Hostname == p.Host })
				}
				if len(foreign(prior.Routes)) == 0 || !reflect.DeepEqual(foreign(prior.Routes), foreign(g.Routes)) || !reflect.DeepEqual(prior.TLSAllowlist, g.TLSAllowlist) {
					t.Fatal("lost independent route or certificate", phase)
				}
				groups[k] = model.EdgeRouteBundle{EdgeGroupID: k, Routes: g.Routes, TLSAllowlist: g.TLSAllowlist, CachePolicies: g.CachePolicies}
			}
			dns.ReadinessPlan, dns.QueryViews, dns.Policy.DNSAnswerRules = &v.DNS.Plan, v.DNS.Views, v.DNS.Rules
		}
		if !reflect.DeepEqual(dns.ReadinessPlan, s.Prior.ReadinessPlan) {
			t.Fatal("withdraw did not restore selected readiness")
		}
	})
}

func TestCompilerToHostProgramIncludesEveryPhysicalIngressWithoutDNSReadmission(t *testing.T) {
	pc.HostedTransitionFixtureForTest(t, true, func(s pc.HostedAddressSource, p pc.HostedScopeProposal) {
		a, b, err := routeartifact.HostedProgramDigests(s, p)
		if err != nil || a == "" || b == "" || a == b {
			t.Fatal("host phases indistinguishable", err)
		}
		wire, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		var stored pc.HostedScopeProposal
		if err = json.Unmarshal(wire, &stored); err != nil {
			t.Fatal(err)
		}
		x, y, err := routeartifact.HostedProgramDigests(s, stored)
		if err != nil || x != a || y != b {
			t.Fatal("stored draft no longer binds the same reversible phases", err)
		}
		prepared, forward, e := routeartifact.BuildHostedProgram(s, p)
		if e != nil {
			t.Fatal(e)
		}
		for _, n := range p.Physical {
			v, ok := forward.Groups[n.EdgeGroupID]
			if !ok || !reflect.DeepEqual(v.TLSAllowlist, prepared.Groups[n.EdgeGroupID].TLSAllowlist) {
				t.Fatal("held ingress or its original TLS identity was lost")
			}
			paths := map[string]string{}
			for _, r := range v.Routes {
				if r.Hostname == p.Host {
					paths[r.PathPrefix] = r.AppID
				}
			}
			if len(paths) != len(p.After.Routes) {
				t.Fatal("incomplete routing for a physical ingress")
			}
		}
		if !reflect.DeepEqual(prepared.DNS.Views, forward.DNS.Views) {
			t.Fatal("path edit changed eligible DNS views")
		}
		q := p
		q.Authority.LKGID += "different-source"
		if _, _, e := routeartifact.BuildHostedProgram(s, q); e == nil {
			t.Fatal("borrowed authority")
		}
		q = p
		q.Eligible = slices.Clone(p.Physical)
		if _, _, e := routeartifact.BuildHostedProgram(s, q); e == nil {
			t.Fatal("readmitted held ingress")
		}
		q = p
		q.After.Routes = slices.Clone(p.After.Routes)
		q.After.Routes[0].RuntimeID += "different-origin"
		if _, _, e := routeartifact.BuildHostedProgram(s, q); e == nil {
			t.Fatal("DNS proof was not bound to its actual route")
		}
	})
}

// A starts with the authenticated compiler fixture. B is applied to the live
// group while A's image remains fixed; B deliberately targets the SAME app
// using a different release. This is unsigned executor input, not an ACK.
func TestHostImageComposesOnNewestPeerSelectionAndRejectsDrift(t *testing.T) {
	pc.HostedFourPathFixtureForTest(t, func(s pc.HostedAddressSource, p pc.HostedScopeProposal) {
		draft, e := pc.Digest(p)
		if e != nil {
			t.Fatal(e)
		}
		im, e := routeartifact.FreezeHostedProgram(s, p, draft)
		if e != nil {
			t.Fatal(e)
		}
		groups := map[string]model.EdgeRouteBundle{}
		for _, member := range p.Physical {
			group := member.EdgeGroupID
			g, e := routeartifact.MaterializeForGroup(*s.PriorRoute, group)
			if e != nil {
				t.Fatal(e)
			}
			groups[group] = g
		}
		dns := s.Prior
		apply := func(phase string) (routeartifact.HostedPhase, error) {
			return routeartifact.ComposeHostedExecutorChange(groups, dns, p, phase, draft, im.Digest, im.Wire)
		}
		stage := func(name string) {
			t.Helper()
			next, e := apply(name)
			if e != nil {
				t.Fatal(name, e)
			}
			for group, c := range next.Groups {
				groups[group] = model.EdgeRouteBundle{EdgeGroupID: group, Routes: c.Routes, CachePolicies: c.CachePolicies, TLSAllowlist: c.TLSAllowlist}
			}
			dns.ReadinessPlan, dns.QueryViews, dns.Policy.DNSAnswerRules = &next.DNS.Plan, next.DNS.Views, next.DNS.Rules
		}
		stage("prepare")
		if len(p.After.Routes) != 4 {
			t.Fatal("fixture lost the four paths")
		}
		const host = "independent.example.test"
		expected := map[string]model.EdgeRouteBinding{}
		owner := model.EdgeTLSAllowlistEntry{Hostname: host, TenantID: p.Before.Owner.TenantID, AppID: p.Before.Owner.AppID, Status: "verified"}
		now := time.Now().UTC()
		fact := pc.ReleaseObservation{ID: "peer-stable-v2", TenantID: owner.TenantID, AppID: owner.AppID, RuntimeID: "peer-runtime-v2", UpstreamURL: "http://peer-v2.example.test", Status: "active", ObservedAt: now}
		route := pc.RouteIntent{Hostname: host, PathPrefix: "/", AppID: owner.AppID, TenantID: owner.TenantID, RuntimeID: fact.RuntimeID, UpstreamURL: fact.UpstreamURL, Enabled: true}
		cert := pc.TLSIntent{Hostname: host, AppID: owner.AppID, TenantID: owner.TenantID, Policy: "automatic", DomainRef: "domain-peer"}
		rule := pc.TrafficPolicyConstraint{ID: "peer-rule", AppID: owner.AppID, TenantID: owner.TenantID, Mode: model.AppTrafficModeSingle, StableWeight: 100, StableReleaseID: fact.ID}
		certFact := pc.TLSDomainObservation{Hostname: host, AppID: owner.AppID, TenantID: owner.TenantID, Ref: cert.DomainRef, Status: "verified", VerifiedAt: &now}
		peer, e := pc.Compile(pc.CompileRequest{
			Intent:          pc.PlatformIntent{Generation: "peer", Routes: []pc.RouteIntent{route}, TLS: []pc.TLSIntent{cert}},
			Policy:          pc.PolicySnapshot{Generation: "peer-policy", MaxStaleSeconds: 120, TrafficConstraints: []pc.TrafficPolicyConstraint{rule}},
			RuntimeSnapshot: pc.RuntimeSnapshot{CapturedAt: &now, Releases: []pc.ReleaseObservation{fact}, TLSDomains: []pc.TLSDomainObservation{certFact}},
		})
		if e != nil {
			t.Fatal("independent B compiler", e)
		}
		for group, g := range groups {
			b, e := routeartifact.MaterializeForGroup(peer.RouteArtifact, group)
			if e != nil || len(b.Routes) != 1 {
				t.Fatal(e)
			}
			row := b.Routes[0]
			if len(row.Upstreams) != 1 || row.Upstreams[0].ReleaseID != fact.ID {
				t.Fatal("B release missing")
			}
			i := slices.IndexFunc(g.Routes, func(r model.EdgeRouteBinding) bool { return r.Hostname == host })
			if i < 0 || len(g.Routes[i].Upstreams) != 1 || g.Routes[i].Upstreams[0].ReleaseID != "host-stable-v1" {
				t.Fatal("missing B predecessor")
			}
			g.Routes[i] = row
			if len(b.TLSAllowlist) != 1 || b.TLSAllowlist[0] != owner {
				t.Fatal("independent B certificate not verified")
			}
			g.TLSAllowlist = append(g.TLSAllowlist, b.TLSAllowlist[0])
			groups[group] = g
			expected[group] = row
		}
		// The later DNS publication is static on purpose: its wire identity
		// and TTL must survive without becoming A's health prerequisite.
		i := slices.IndexFunc(dns.Records, func(r pc.DNSIntent) bool { return r.Hostname == host && r.Type == "A" })
		if i < 0 || dns.Records[i].TTL != 90 {
			t.Fatal("missing B wire predecessor")
		}
		rr := dns.Records[i]
		rr.TTL = 300
		dns.Records = slices.Clone(dns.Records)
		dns.Records[i] = rr
		dns.QueryViews = slices.Clone(dns.QueryViews)
		for i, v := range dns.QueryViews {
			if host == v.Zone || strings.HasSuffix(host, "."+v.Zone) {
				j := slices.IndexFunc(v.Records, func(r model.EdgeDNSRecord) bool { return r.Name == host && r.Type == rr.Type })
				if j < 0 {
					t.Fatal("missing B selector")
				}
				dns.QueryViews[i].Records = slices.Clone(v.Records)
				dns.QueryViews[i].Records[j].TTL = rr.TTL
			}
		}
		stage("forward")
		for group, g := range groups {
			count, shared := 0, 0
			for _, r := range g.Routes {
				if r.Hostname == p.Host {
					count++
					if r.AppID == owner.AppID {
						if len(r.Upstreams) != 1 || r.Upstreams[0].ReleaseID != "host-stable-v1" {
							t.Fatal("A followed B release")
						}
						shared++
					}
				}
			}
			if count != 4 || shared == 0 || !slices.ContainsFunc(g.Routes, func(r model.EdgeRouteBinding) bool { return reflect.DeepEqual(r, expected[group]) }) {
				t.Fatal("four paths and independent release not simultaneous")
			}
		}
		stage("withdraw")
		for group, g := range groups {
			n := 0
			for _, r := range g.Routes {
				if r.Hostname == host {
					n++
					if !reflect.DeepEqual(r, expected[group]) {
						t.Fatal("changed B's release")
					}
				}
			}
			if n != 1 || !slices.Contains(g.TLSAllowlist, owner) {
				t.Fatal("lost peer")
			}
		}
		if !reflect.DeepEqual(rr, dns.Records[i]) {
			t.Fatal("lost B address/TTL")
		}
		for _, v := range dns.QueryViews {
			if host == v.Zone || strings.HasSuffix(host, "."+v.Zone) {
				if !slices.ContainsFunc(v.Records, func(r model.EdgeDNSRecord) bool {
					return r.Name == host && r.TTL == 300 && slices.Equal(r.Values, rr.Values)
				}) {
					t.Fatal("lost peer DNS selector")
				}
			}
		}
		if _, e := apply("withdraw"); e == nil {
			t.Fatal("accepted stale predecessor")
		}
		if _, e := routeartifact.ComposeHostedExecutorChange(groups, dns, p, "forward", draft, im.Digest, append(slices.Clone(im.Wire), ' ')); e == nil {
			t.Fatal("accepted changed body")
		}
		drift := dns
		plan := *dns.ReadinessPlan
		rrPlan := plan.Records[0]
		rrPlan.Hostname = "foreign.example.test"
		plan.Records = append(slices.Clone(plan.Records), rrPlan)
		drift.ReadinessPlan = &plan
		if _, e := pc.HostedDNSProgramSource(drift, p); e == nil {
			t.Fatal("foreign record borrowed A probes")
		}
	})
}
