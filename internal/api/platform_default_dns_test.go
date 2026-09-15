package api

import (
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"reflect"
	"testing"
	"time"
)

func TestProjectDefaultAppDNSAddsOnlyRootBusinessRoutes(t *testing.T) {
	r := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{
		{Hostname: "one.example.test", AppID: "app-a", TenantID: "tenant-a", PathPrefix: "/", UpstreamURL: "http://one", Enabled: true},
		{Hostname: "one.example.test", AppID: "app-a", TenantID: "tenant-a", PathPrefix: "/api", UpstreamURL: "http://one", Enabled: true},
		{Hostname: "two.other.test", AppID: "app-b", TenantID: "tenant-b", PathPrefix: "/", UpstreamURL: "http://two", Enabled: true},
		{Hostname: "platform.example.test", Kind: model.EdgeRouteKindPlatformRoute, PathPrefix: "/", UpstreamURL: "http://platform", Enabled: true},
	}, DNS: []platformconfig.DNSIntent{{Hostname: "one.example.test", Type: "FUGUE_APP", AppID: "app-a", TenantID: "tenant-a", Values: []string{"app-a"}, TTL: 60, Application: &platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}}}
	if err := projectDefaultAppDNS(&r, "example.test", 90); err != nil {
		t.Fatal(err)
	}
	if len(r.Intent.DNS) != 1 {
		t.Fatalf("existing explicit app record was duplicated: %+v", r.Intent.DNS)
	}
	r.Intent.DNS = nil
	if err := projectDefaultAppDNS(&r, "example.test", 90); err != nil {
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
	if err := projectDefaultAppDNS(&r, "example.test", 90); err == nil {
		t.Fatal("ambiguous route owner accepted")
	}
}

func TestProjectManagedCustomDomainDNSProjectsSharedTarget(t *testing.T) {
	now := time.Now().UTC()
	result := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{{Hostname: "custom.example.test", AppID: "app-a", TenantID: "tenant-a", Enabled: true, UpstreamURL: "http://app"}}}}
	domains := []model.AppDomain{{Hostname: "www.customer.test", AppID: "app-a", TenantID: "tenant-a", Status: model.AppDomainStatusVerified, DNSMode: model.AppDomainDNSModeManaged, DNSStatus: model.AppDomainDNSStatusReady, TLSStatus: model.AppDomainTLSStatusReady, RouteTarget: "target.example.test", CreatedAt: now, UpdatedAt: now}}
	apps := map[string]model.App{"app-a": {ID: "app-a", TenantID: "tenant-a", Route: &model.AppRoute{Hostname: "custom.example.test"}}}
	if err := projectManagedCustomDomainDNS(&result, domains, apps, func(model.App) string { return "fallback.example.test" }, 90); err != nil {
		t.Fatal(err)
	}
	if len(result.Intent.DNS) != 1 {
		t.Fatalf("expected one target record: %+v", result.Intent.DNS)
	}
	record := result.Intent.DNS[0]
	if record.Hostname != "target.example.test" || record.Type != "FUGUE_ROUTE" || record.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget || record.AppID != "app-a" || record.TenantID != "tenant-a" || record.Route == nil || len(record.Route.Hostnames) != 1 || record.Route.Hostnames[0] != "www.customer.test" {
		t.Fatalf("unexpected projected record: %+v", record)
	}
}

func TestProjectManagedCustomDomainDNSMergesAliasesForSharedTarget(t *testing.T) {
	result := platformIntentProjectionResponse{}
	domains := []model.AppDomain{
		{Hostname: "one.customer.test", AppID: "app-a", TenantID: "tenant-a", Status: model.AppDomainStatusVerified, DNSMode: model.AppDomainDNSModeManaged, RouteTarget: "target.example.test"},
		{Hostname: "two.customer.test", AppID: "app-a", TenantID: "tenant-a", Status: model.AppDomainStatusVerified, DNSMode: model.AppDomainDNSModeManaged, RouteTarget: "target.example.test"},
	}
	apps := map[string]model.App{"app-a": {ID: "app-a", TenantID: "tenant-a", Route: &model.AppRoute{Hostname: "app.example.test"}}}
	if err := projectManagedCustomDomainDNS(&result, domains, apps, nil, 60); err != nil {
		t.Fatal(err)
	}
	if len(result.Intent.DNS) != 1 || !reflect.DeepEqual(result.Intent.DNS[0].Route.Hostnames, []string{"one.customer.test", "two.customer.test"}) {
		t.Fatalf("shared target aliases were not merged: %+v", result.Intent.DNS)
	}
}

func TestValidateProjectedRouteDNSReferencesReportsMissingHost(t *testing.T) {
	result := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{{Hostname: "covered.example.test", PathPrefix: "/", Enabled: true}, {Hostname: "missing.example.test", PathPrefix: "/api", Enabled: true}}, DNS: []platformconfig.DNSIntent{{Hostname: "target.example.test", Type: "FUGUE_ROUTE", Values: []string{}, Route: &platformconfig.DNSRouteIntent{Hostnames: []string{"covered.example.test"}}}}}}
	validateProjectedRouteDNSReferences(&result)
	if len(result.Issues) != 1 || result.Issues[0].Code != "dns_route_placement_not_projected" || result.Issues[0].Hostname != "missing.example.test" || result.Issues[0].PathPrefix != "/api" {
		t.Fatalf("missing route reference was not diagnosed: %+v", result.Issues)
	}
}

func TestProjectManagedCustomDomainDNSRejectsConflictingSharedTarget(t *testing.T) {
	result := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{DNS: []platformconfig.DNSIntent{{Hostname: "target.example.test", Type: "FUGUE_ROUTE", Values: []string{}, TTL: 60, RecordKind: model.EdgeDNSRecordKindCustomDomainTarget, AppID: "app-old", TenantID: "tenant-old", Route: &platformconfig.DNSRouteIntent{Hostnames: []string{"old.customer.test"}}}}}}
	domains := []model.AppDomain{{Hostname: "new.customer.test", AppID: "app-new", TenantID: "tenant-new", Status: model.AppDomainStatusVerified, DNSMode: model.AppDomainDNSModeManaged, DNSStatus: model.AppDomainDNSStatusReady, TLSStatus: model.AppDomainTLSStatusReady, RouteTarget: "target.example.test"}}
	apps := map[string]model.App{"app-new": {ID: "app-new", TenantID: "tenant-new", Route: &model.AppRoute{Hostname: "new.example.test"}}}
	if err := projectManagedCustomDomainDNS(&result, domains, apps, nil, 60); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, issue := range result.Issues {
		if issue.Code == "dns_custom_domain_target_conflict" && issue.Hostname == "new.customer.test" {
			found = true
		}
	}
	if !found {
		t.Fatalf("target conflict was not diagnosed: %+v", result.Issues)
	}
}

func TestProjectManagedCustomDomainDNSSkipsUnreadyOrExternalDomains(t *testing.T) {
	result := platformIntentProjectionResponse{}
	domains := []model.AppDomain{{Hostname: "pending.customer.test", AppID: "app-a", Status: model.AppDomainStatusVerified, DNSMode: model.AppDomainDNSModeManaged, DNSStatus: model.AppDomainDNSStatusPending, TLSStatus: model.AppDomainTLSStatusReady, RouteTarget: "target.example.test"}, {Hostname: "external.customer.test", AppID: "app-a", Status: model.AppDomainStatusVerified, DNSMode: model.AppDomainDNSModeExternal, DNSStatus: model.AppDomainDNSStatusReady, TLSStatus: model.AppDomainTLSStatusReady, RouteTarget: "external.target.test"}}
	apps := map[string]model.App{"app-a": {ID: "app-a", TenantID: "tenant-a", Route: &model.AppRoute{Hostname: "app.example.test"}}}
	if err := projectManagedCustomDomainDNS(&result, domains, apps, nil, 60); err != nil {
		t.Fatal(err)
	}
	if len(result.Intent.DNS) != 1 || result.Intent.DNS[0].Hostname != "target.example.test" {
		t.Fatalf("managed desired intent was not projected: %+v", result.Intent.DNS)
	}
}
