package api

import (
	"fugue/internal/model"
	"testing"
)

func TestHostedDispatchSelectsOnlyPlannedHostAndSameCertificateOwner(t *testing.T) {
	const host, other, tenant, owner = "app.example.test", "unrelated.example.test", "owner-tenant", "original-service"
	paths := map[string]string{"/": "site", "/api": owner}
	entry := model.EdgeTLSAllowlistEntry{Hostname: host, TenantID: tenant, AppID: owner, Status: model.AppDomainStatusVerified, TLSStatus: model.AppDomainTLSStatusReady}
	route := func(h, p, a string) model.EdgeRouteIntent {
		return model.EdgeRouteIntent{Hostname: h, PathPrefix: p, AppID: a, TenantID: tenant, RuntimeID: "runtime-" + a, Generation: "signed", OriginStatus: model.EdgeRouteStatusActive, RoutePolicy: model.EdgeRoutePolicyEnabled}
	}
	fixture := func() model.EdgeRouteIntentSnapshot {
		b := route(host, "/api", owner)
		b.CachePolicyID = "cache-original"
		foreign := route(other, "/", "other-app")
		foreign.TenantID = "other-tenant"
		foreign.CachePolicyID = "other-cache"
		return model.EdgeRouteIntentSnapshot{Routes: []model.EdgeRouteIntent{b, foreign, route(host, "/", "site")}, TLSAllowlist: []model.EdgeTLSAllowlistEntry{entry},
			CachePolicies: []model.CachePolicy{{ID: "other-cache"}, {ID: "cache-original"}}}
	}
	got, e := selectHostedDispatchRows(host, tenant, owner, paths, fixture(), []model.EdgeTLSAllowlistEntry{entry})
	if e != nil || len(got.Routes) != 2 || got.Routes[0].PathPrefix != "/" || got.Routes[0].AppID != "site" || got.Owner != entry || len(got.Policies) != 1 || got.Policies[0].ID != "cache-original" {
		t.Fatal("wrong source closure", e)
	}
	for name, edit := range map[string]func(*model.EdgeRouteIntentSnapshot){
		"omitted-prefix":       func(p *model.EdgeRouteIntentSnapshot) { p.Routes = p.Routes[1:] },
		"extra-prefix":         func(p *model.EdgeRouteIntentSnapshot) { p.Routes = append(p.Routes, route(host, "/api-shadow", owner)) },
		"duplicate-prefix":     func(p *model.EdgeRouteIntentSnapshot) { p.Routes = append(p.Routes, p.Routes[0]) },
		"foreign-tenant":       func(p *model.EdgeRouteIntentSnapshot) { p.Routes[0].TenantID = "other" },
		"wrong-app":            func(p *model.EdgeRouteIntentSnapshot) { p.Routes[0].AppID = "site" },
		"unknown-policy":       func(p *model.EdgeRouteIntentSnapshot) { p.Routes[0].RoutePolicy = "invalid" },
		"foreign-cert":         func(p *model.EdgeRouteIntentSnapshot) { p.TLSAllowlist[0].AppID = "site" },
		"different-tls-status": func(p *model.EdgeRouteIntentSnapshot) { p.TLSAllowlist[0].TLSStatus = "pending" },
		"missing-policy":       func(p *model.EdgeRouteIntentSnapshot) { p.CachePolicies = p.CachePolicies[:1] },
		"repeat-policy":        func(p *model.EdgeRouteIntentSnapshot) { p.CachePolicies = append(p.CachePolicies, p.CachePolicies[1]) },
	} {
		t.Run(name, func(t *testing.T) {
			p := fixture()
			edit(&p)
			if _, e := selectHostedDispatchRows(host, tenant, owner, paths, p, []model.EdgeTLSAllowlistEntry{entry}); e == nil {
				t.Fatal("invalid host projection accepted")
			}
		})
	}
}
