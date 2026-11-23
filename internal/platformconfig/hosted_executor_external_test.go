package platformconfig_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/routeartifact"
)

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
