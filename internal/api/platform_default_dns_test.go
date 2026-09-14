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
	if err := projectDefaultAppDNS(&r, "example.test"); err != nil {
		t.Fatal(err)
	}
	if len(r.Intent.DNS) != 1 {
		t.Fatalf("existing explicit app record was duplicated: %+v", r.Intent.DNS)
	}
	r.Intent.DNS = nil
	if err := projectDefaultAppDNS(&r, "example.test"); err != nil {
		t.Fatal(err)
	}
	if len(r.Intent.DNS) != 1 || r.Intent.DNS[0].Hostname != "one.example.test" || r.Intent.DNS[0].AppID != "app-a" || r.Intent.DNS[0].Route == nil {
		t.Fatalf("default app DNS=%+v", r.Intent.DNS)
	}
	if err := platformconfig.ValidatePlatformIntent(r.Intent); err != nil {
		t.Fatal(err)
	}
}

func TestProjectDefaultAppDNSRejectsAmbiguousRootOwner(t *testing.T) {
	r := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{{Hostname: "one.example.test", AppID: "app-a", TenantID: "tenant-a", UpstreamURL: "http://one", Enabled: true}, {Hostname: "one.example.test", AppID: "app-b", TenantID: "tenant-b", UpstreamURL: "http://two", Enabled: true}}}}
	if err := projectDefaultAppDNS(&r, "example.test"); err == nil {
		t.Fatal("ambiguous route owner accepted")
	}
}
