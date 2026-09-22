package api

import (
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"testing"
)

func TestProjectDefaultAppDNSAddsOnlyRootBusinessRoutes(t *testing.T) {
	r := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{
		{Hostname: "one.example.test", AppID: "app-a", TenantID: "tenant-a", PathPrefix: "/", UpstreamURL: "http://one", Enabled: true},
		{Hostname: "one.example.test", AppID: "app-a", TenantID: "tenant-a", PathPrefix: "/api", UpstreamURL: "http://one", Enabled: true},
		{Hostname: "two.other.test", AppID: "app-b", TenantID: "tenant-b", PathPrefix: "/", UpstreamURL: "http://two", Enabled: true},
		{Hostname: "platform.example.test", Kind: model.EdgeRouteKindPlatformRoute, PathPrefix: "/", UpstreamURL: "http://platform", Enabled: true},
	}, DNS: []platformconfig.DNSIntent{{Hostname: "one.example.test", Type: "FUGUE_APP", AppID: "app-a", TenantID: "tenant-a", Values: []string{"app-a"}, TTL: 60, Application: &platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}}}
	apps := map[string]model.App{"app-a": {ID: "app-a", TenantID: "tenant-a", Route: &model.AppRoute{Hostname: "one.example.test"}}}
	if err := projectDefaultAppDNS(&r, apps, "example.test", 90); err != nil {
		t.Fatal(err)
	}
	if len(r.Intent.DNS) != 1 {
		t.Fatalf("existing explicit app record was duplicated: %+v", r.Intent.DNS)
	}
	r.Intent.DNS = nil
	if err := projectDefaultAppDNS(&r, apps, "example.test", 90); err != nil {
		t.Fatal(err)
	}
	if len(r.Intent.DNS) != 1 || r.Intent.DNS[0].Hostname != "one.example.test" || r.Intent.DNS[0].AppID != "app-a" || r.Intent.DNS[0].Route == nil || r.Intent.DNS[0].TTL != 90 {
		t.Fatalf("default app DNS=%+v", r.Intent.DNS)
	}
	if err := platformconfig.ValidatePlatformIntent(r.Intent); err != nil {
		t.Fatal(err)
	}
}

func TestProjectDefaultAppDNSRejectsAmbiguousRootOwner(t *testing.T) {
	r := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{{Hostname: "one.example.test", AppID: "app-a", TenantID: "tenant-a", UpstreamURL: "http://one", Enabled: true}, {Hostname: "one.example.test", AppID: "app-b", TenantID: "tenant-b", UpstreamURL: "http://two", Enabled: true}}}}
	apps := map[string]model.App{"app-a": {ID: "app-a", TenantID: "tenant-a", Route: &model.AppRoute{Hostname: "one.example.test"}}, "app-b": {ID: "app-b", TenantID: "tenant-b", Route: &model.AppRoute{Hostname: "one.example.test"}}}
	if err := projectDefaultAppDNS(&r, apps, "example.test", 90); err == nil {
		t.Fatal("ambiguous route owner accepted")
	}
}

func TestProjectDefaultAppDNSRequiresFrozenDefaultHostnameAndOwner(t *testing.T) {
	for _, scenario := range []string{"matching", "project_alias", "missing_app", "missing_route", "wrong_tenant", "wrong_app", "explicit_alias"} {
		t.Run(scenario, func(t *testing.T) {
			app := model.App{ID: "app", TenantID: "tenant", Route: &model.AppRoute{Hostname: "default.example.test"}}
			r := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{
				{Hostname: "default.example.test", AppID: app.ID, TenantID: app.TenantID, PathPrefix: "/", UpstreamURL: "http://application", Kind: model.EdgeRouteKindPlatform, Enabled: true},
			}}}
			switch scenario {
			case "project_alias", "explicit_alias":
				r.Intent.Routes[0].Hostname = "alias.example.test"
				r.Intent.Routes[0].Kind = model.EdgeRouteKindPlatformDomain
				if scenario == "explicit_alias" {
					r.Intent.DNS = []platformconfig.DNSIntent{{Hostname: "alias.example.test", Type: "A", Values: []string{"192.0.2.8"}, TTL: 300}}
				}
			case "missing_route":
				app.Route = nil
			case "wrong_tenant":
				app.TenantID = "other-tenant"
			case "wrong_app":
				app.ID = "other-app"
			}
			apps := map[string]model.App{"app": app}
			if scenario == "missing_app" {
				apps = nil
			}
			if err := projectDefaultAppDNS(&r, apps, "example.test", 60); err != nil {
				t.Fatal(err)
			}
			want := 0
			if scenario == "matching" || scenario == "explicit_alias" {
				want = 1
			}
			if len(r.Intent.DNS) != want {
				t.Fatal("default DNS publication exceeded frozen owner intent", r.Intent.DNS)
			}
			if scenario == "explicit_alias" && (r.Intent.DNS[0].Type != "A" || r.Intent.DNS[0].Values[0] != "192.0.2.8") {
				t.Fatal("explicit alias DNS changed")
			}
		})
	}
}
