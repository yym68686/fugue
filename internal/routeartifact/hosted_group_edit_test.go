package routeartifact

import (
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
)

func TestHostedGroupCompareReplaceAndWithdraw(t *testing.T) {
	const group = "edge-group-example"
	const host = "service.example.test"
	d := "sha256:" + strings.Repeat("a", 64)
	baseRef := pc.PreviousTrafficPublicationReference{ReleaseSetID: "parent", ReleaseSetDigest: d, ReleaseID: "full", FencingToken: 5, RouteArtifactID: "route", RouteArtifactDigest: d, TLSArtifactID: "tls", TLSArtifactDigest: d, DNSArtifactID: "dns", DNSArtifactDigest: d}
	owner := model.EdgeTLSAllowlistEntry{Hostname: host, TenantID: "tenant", AppID: "backend", Status: model.AppDomainStatusVerified}
	original := model.EdgeRouteIntent{Generation: "initial", Hostname: host, PathPrefix: "/", AppID: owner.AppID, TenantID: owner.TenantID, RuntimeID: "backend-runtime", CachePolicyID: "shared", TargetGroupMode: model.EdgeRouteIntentGroupModeAllGroups, OriginStatus: model.EdgeRouteStatusActive, RoutePolicy: model.EdgeRoutePolicyEnabled}
	foreign := original
	foreign.Hostname = "other.example.test"
	foreign.AppID = "other"
	foreign.TenantID = "other-tenant"
	entry := owner
	entry.Hostname = foreign.Hostname
	entry.AppID = foreign.AppID
	entry.TenantID = foreign.TenantID
	policy := model.CachePolicy{ID: "shared", TTLSeconds: 60}
	s := model.EdgeRouteIntentSnapshot{Generation: "initial", Routes: []model.EdgeRouteIntent{foreign, original}, TLSAllowlist: []model.EdgeTLSAllowlistEntry{entry, owner}, CachePolicies: []model.CachePolicy{policy}}
	start, e := MaterializeSnapshotForGroup(s, group)
	if e != nil {
		t.Fatal(e)
	}
	old := pc.HostedDispatchRows{Owner: owner, Routes: []model.EdgeRouteIntent{original}, Policies: []model.CachePolicy{policy}}
	ready := original
	ready.RuntimeID = "updated-backend"
	prepared := pc.HostedDispatchRows{Owner: owner, Routes: []model.EdgeRouteIntent{ready}, Policies: []model.CachePolicy{policy}}
	web := ready
	web.AppID = "site"
	web.RuntimeID = "site-runtime"
	web.OriginStatus = model.EdgeRouteStatusUnavailable
	web.CachePolicyID = "site-cache"
	child := ready
	child.PathPrefix = "/api"
	p := pc.HostedScopeProposal{Schema: pc.HostedProposalSchema, Host: host, Base: baseRef, Before: old, Prepared: prepared,
		After:    pc.HostedDispatchRows{Owner: owner, Routes: []model.EdgeRouteIntent{web, child}, Policies: []model.CachePolicy{policy, {ID: web.CachePolicyID, TTLSeconds: 10}}},
		Physical: []pc.HostedAddressHold{{EdgeID: "held", EdgeGroupID: group, Address: "192.0.2.13"}},
	}
	wire := func(c HostedGroupContent) model.EdgeRouteBundle {
		return model.EdgeRouteBundle{EdgeGroupID: group, Routes: c.Routes, TLSAllowlist: c.TLSAllowlist, CachePolicies: c.CachePolicies}
	}
	if _, e := ComposeHostedGroupEdit(start, p, "forward"); e == nil {
		t.Fatal("skipped prerequisite")
	}
	mid, e := ComposeHostedGroupEdit(start, p, "prepare")
	if e != nil {
		t.Fatal(e)
	}
	out, e := ComposeHostedGroupEdit(wire(mid), p, "forward")
	if e != nil {
		t.Fatal(e)
	}
	if len(out.Routes) != 3 || !reflect.DeepEqual(out.Routes[0], start.Routes[0]) || !reflect.DeepEqual(start.TLSAllowlist, out.TLSAllowlist) ||
		out.Routes[1].AppID != "site" || out.Routes[1].Status != model.EdgeRouteStatusUnavailable || out.Routes[2].PathPrefix != "/api" || out.Routes[2].RuntimeID != "updated-backend" {
		t.Fatal("foreign dispatch, TLS, or tenant failure rewritten")
	}
	back, e := ComposeHostedGroupEdit(wire(out), p, "withdraw")
	if e != nil || !reflect.DeepEqual(mid, back) {
		t.Fatal("host-only reverse did not restore prepared content", e)
	}
	out.Routes[0].RuntimeID = "independent-update"
	back, e = ComposeHostedGroupEdit(wire(out), p, "withdraw")
	if e != nil || back.Routes[0].RuntimeID != "independent-update" || start.Routes[0].RuntimeID == "independent-update" {
		t.Fatal("withdrawal overwrote another application", e)
	}
	for _, change := range []func(*model.EdgeRouteBundle, *pc.HostedScopeProposal){
		func(b *model.EdgeRouteBundle, p *pc.HostedScopeProposal) { b.EdgeGroupID = "edge-group-foreign" },
		func(b *model.EdgeRouteBundle, p *pc.HostedScopeProposal) { b.TLSAllowlist[1].AppID = "foreign" },
		func(b *model.EdgeRouteBundle, p *pc.HostedScopeProposal) { b.Routes[1].AppID = "stale" },
		func(b *model.EdgeRouteBundle, p *pc.HostedScopeProposal) { p.After.Policies[0].TTLSeconds++ },
		func(b *model.EdgeRouteBundle, p *pc.HostedScopeProposal) {
			p.After.Routes[0].CachePolicyID = "foreign-missing"
			p.After.Policies[1].ID = "foreign-missing"
			b.Routes[0].CachePolicyID = "foreign-missing"
		},
	} {
		q := p
		q.After.Policies = append([]model.CachePolicy{}, p.After.Policies...)
		q.After.Routes = append([]model.EdgeRouteIntent{}, p.After.Routes...)
		b := wire(mid)
		b.TLSAllowlist = append([]model.EdgeTLSAllowlistEntry{}, b.TLSAllowlist...)
		b.Routes = append([]model.EdgeRouteBinding{}, b.Routes...)
		change(&b, &q)
		if _, e := ComposeHostedGroupEdit(b, q, "forward"); e == nil {
			t.Fatal("mismatched serving base, cache share or owner accepted")
		}
	}
}
