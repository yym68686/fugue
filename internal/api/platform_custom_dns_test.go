package api

import (
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func customDNSTestServer() *Server {
	return &Server{appBaseDomain: "apps.example.test", customDomainBaseDomain: "dns.example.test", dnsBundleTTL: 90}
}
func customDNSApp(id, tenant, host string) model.App {
	return model.App{ID: id, TenantID: tenant, Route: &model.AppRoute{Hostname: host}}
}
func customDNSDomain(host, app, tenant, target, mode string) model.AppDomain {
	return model.AppDomain{Hostname: host, AppID: app, TenantID: tenant, Status: model.AppDomainStatusVerified, DNSMode: mode, RouteTarget: target}
}

func TestProjectCustomDomainDNSAggregatesAliasesAndPreservesRouteOwners(t *testing.T) {
	s := customDNSTestServer()
	r := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{{Hostname: "one.customer.test", AppID: "app-a", TenantID: "tenant-a", PathPrefix: "/", Enabled: true, UpstreamURL: "http://a"}, {Hostname: "two.customer.test", AppID: "app-a", TenantID: "tenant-a", PathPrefix: "/api", Enabled: true, UpstreamURL: "http://a"}}}}
	apps := map[string]model.App{"app-a": customDNSApp("app-a", "tenant-a", "one.customer.test")}
	domains := []model.AppDomain{customDNSDomain("two.customer.test", "app-a", "tenant-a", "target.dns.example.test", model.AppDomainDNSModeExternal), customDNSDomain("one.customer.test", "app-a", "tenant-a", "target.dns.example.test", model.AppDomainDNSModeManaged)}
	if err := s.projectCustomDomainDNS(&r, domains, apps); err != nil {
		t.Fatal(err)
	}
	if len(r.Intent.DNS) != 1 || !reflect.DeepEqual(r.Intent.DNS[0].Route.Hostnames, []string{"one.customer.test", "two.customer.test"}) {
		t.Fatalf("aliases=%+v", r.Intent.DNS)
	}
}

func TestProjectCustomDomainDNSEmitsExplicitBindingsForSharedTenantPaths(t *testing.T) {
	s := customDNSTestServer()
	r := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{
		{Hostname: "shared.customer.test", PathPrefix: "/", AppID: "app-a", TenantID: "tenant-a", Enabled: true, UpstreamURL: "http://a"},
		{Hostname: "shared.customer.test", PathPrefix: "/v1", AppID: "app-b", TenantID: "tenant-a", Enabled: true, UpstreamURL: "http://b"},
	}}}
	apps := map[string]model.App{"app-a": customDNSApp("app-a", "tenant-a", "shared.customer.test"), "app-b": {ID: "app-b", TenantID: "tenant-a"}}
	domains := []model.AppDomain{customDNSDomain("shared.customer.test", "app-a", "tenant-a", "target.dns.example.test", model.AppDomainDNSModeManaged)}
	if err := s.projectCustomDomainDNS(&r, domains, apps); err != nil {
		t.Fatal(err)
	}
	if len(r.Intent.DNS) != 1 || len(r.Intent.DNS[0].Route.Bindings) != 2 {
		t.Fatalf("expected two explicit path bindings: %+v", r.Intent.DNS)
	}
}

func TestProjectCustomDomainDNSRejectsOwnerAndTargetConflicts(t *testing.T) {
	s := customDNSTestServer()
	r := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{{Hostname: "new.customer.test", AppID: "app-new", TenantID: "tenant-new", Enabled: true, UpstreamURL: "http://new"}}, DNS: []platformconfig.DNSIntent{{Hostname: "target.dns.example.test", Type: "A", Values: []string{"192.0.2.10"}, TTL: 60}}}}
	apps := map[string]model.App{"app-new": customDNSApp("app-new", "tenant-new", "new.customer.test"), "app-other": customDNSApp("app-other", "tenant-other", "other.customer.test")}
	domains := []model.AppDomain{customDNSDomain("new.customer.test", "app-new", "tenant-new", "target.dns.example.test", model.AppDomainDNSModeManaged), customDNSDomain("foreign.customer.test", "app-other", "tenant-new", "other.dns.example.test", model.AppDomainDNSModeManaged)}
	if err := s.projectCustomDomainDNS(&r, domains, apps); err != nil {
		t.Fatal(err)
	}
	if len(r.Intent.DNS) != 1 || r.Intent.DNS[0].Type != "A" {
		t.Fatalf("address target overwritten: %+v", r.Intent.DNS)
	}
	codes := map[string]bool{}
	for _, i := range r.Issues {
		codes[i.Code] = true
	}
	if !codes["dns_custom_domain_target_conflict"] || !codes["dns_custom_domain_owner_mismatch"] {
		t.Fatalf("issues=%+v", r.Issues)
	}
}

func TestProjectCustomDomainDNSReportsMissingRoutesAndInvalidTargets(t *testing.T) {
	s := customDNSTestServer()
	r := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{{Hostname: "present.customer.test", AppID: "app-a", TenantID: "tenant-a", Enabled: true, UpstreamURL: "http://a"}}}}
	apps := map[string]model.App{"app-a": customDNSApp("app-a", "tenant-a", "present.customer.test")}
	domains := []model.AppDomain{customDNSDomain("missing.customer.test", "app-a", "tenant-a", "target.dns.example.test", model.AppDomainDNSModeManaged), customDNSDomain("bad.customer.test", "app-a", "tenant-a", "outside.example.net", model.AppDomainDNSModeManaged)}
	if err := s.projectCustomDomainDNS(&r, domains, apps); err != nil {
		t.Fatal(err)
	}
	codes := map[string]bool{}
	for _, i := range r.Issues {
		codes[i.Code] = true
	}
	if !codes["dns_custom_domain_route_missing"] || !codes["dns_custom_domain_target_invalid"] {
		t.Fatalf("issues=%+v", r.Issues)
	}
}
