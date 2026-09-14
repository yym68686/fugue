package api

import (
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestProjectPlatformEntryDNSReplacesOnlyAddressRecordsAndPreservesAudit(t *testing.T) {
	result := platformIntentProjectionResponse{
		Intent:        platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: platformconfig.GlobalScopeKey, Generation: "before", Routes: []platformconfig.RouteIntent{{Hostname: "api.example.test", Kind: model.EdgeRouteKindPlatformRoute, UpstreamURL: "http://api.internal:8080", Enabled: true}}},
		DNSExclusions: []platformDNSExclusion{},
	}
	static := []model.EdgeDNSRecord{
		{Name: "api.example.test", Type: "A", Values: []string{"192.0.2.10"}, TTL: 60, RecordKind: model.EdgeDNSRecordKindProtected},
		{Name: "api.example.test", Type: "AAAA", Values: []string{"2001:db8::10"}, TTL: 60, RecordKind: model.EdgeDNSRecordKindProtected},
		{Name: "mail.example.test", Type: "MX", Values: []string{"10 mail.example.test"}, TTL: 60, RecordKind: model.EdgeDNSRecordKindProtected},
	}
	entries := []model.PlatformRoute{{Hostname: "api.example.test", Kind: model.EdgeRouteKindPlatformRoute, UpstreamURL: "http://api.internal:8080", Status: model.EdgeRouteStatusActive, TTL: 30}}
	result.Intent.DNS, _ = projectBusinessDNSDraft(&result, nil, nil, nil, static)
	if err := projectPlatformEntryDNS(&result, entries, static, []string{"example.test"}); err != nil {
		t.Fatal(err)
	}
	if len(result.DNSExclusions) != 2 || result.DNSExclusions[0].Reason != "static_address_replaced_by_platform_entry" {
		t.Fatalf("audit=%+v", result.DNSExclusions)
	}
	if len(result.Intent.DNS) != 2 {
		t.Fatalf("dns=%+v", result.Intent.DNS)
	}
	var route platformconfig.DNSIntent
	for _, record := range result.Intent.DNS {
		if record.Hostname == "api.example.test" {
			route = record
		}
	}
	if route.Type != "FUGUE_ROUTE" || route.Route == nil || !reflect.DeepEqual(route.Route.Hostnames, []string{"api.example.test"}) || route.TTL != 60 || len(route.Values) != 0 {
		t.Fatalf("route=%+v", route)
	}
	var mail platformconfig.DNSIntent
	for _, record := range result.Intent.DNS {
		if record.Hostname == "mail.example.test" {
			mail = record
		}
	}
	if mail.Type != "MX" || len(mail.Values) != 1 {
		t.Fatalf("mail record lost=%+v", mail)
	}
	if err := platformconfig.ValidatePlatformIntent(result.Intent); err != nil {
		t.Fatal(err)
	}
}

func TestProjectPlatformEntryDNSRejectsMissingRouteAndBusinessOwner(t *testing.T) {
	base := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: platformconfig.GlobalScopeKey, Generation: "before", Routes: []platformconfig.RouteIntent{{Hostname: "api.example.test", AppID: "app-a", TenantID: "tenant-a", UpstreamURL: "http://api", Enabled: true}}}}
	entry := []model.PlatformRoute{{Hostname: "missing.example.test", UpstreamURL: "http://missing", Status: model.EdgeRouteStatusActive}}
	if err := projectPlatformEntryDNS(&base, entry, nil, []string{"example.test"}); err == nil {
		t.Fatal("missing route accepted")
	}
	base.Intent.Routes[0].AppID = ""
	base.Intent.Routes[0].TenantID = ""
	entry[0].Hostname = "api.example.test"
	base.Intent.Routes[0].AppID = "app-a"
	if err := projectPlatformEntryDNS(&base, entry, nil, []string{"example.test"}); err == nil {
		t.Fatal("business-owned route accepted as platform entry")
	}
}

func TestProjectPlatformEntryDNSPreservesConflictingTenantRecord(t *testing.T) {
	result := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "before", Routes: []platformconfig.RouteIntent{{Hostname: "api.example.test", UpstreamURL: "http://api", Enabled: true}}, DNS: []platformconfig.DNSIntent{{Hostname: "api.example.test", Type: "A", TTL: 60, Values: []string{"93.184.216.34"}, TenantID: "tenant-a", RecordKind: model.EdgeDNSRecordKindHosted}}}}
	static := []model.EdgeDNSRecord{{Name: "api.example.test", Type: "A", TTL: 60, Values: []string{"192.0.2.10"}, RecordKind: model.EdgeDNSRecordKindProtected}}
	if err := projectPlatformEntryDNS(&result, []model.PlatformRoute{{Hostname: "api.example.test", UpstreamURL: "http://api", Status: "active"}}, static, []string{"example.test"}); err != nil {
		t.Fatal(err)
	}
	if len(result.Intent.DNS) != 2 || len(result.DNSExclusions) != 0 {
		t.Fatal("tenant record disappeared", result.Intent.DNS, result.DNSExclusions)
	}
	found := false
	for _, issue := range result.Issues {
		found = found || issue.Code == "platform_dns_conflicting_business_record"
	}
	if !found {
		t.Fatal("conflicting record lost diagnostic")
	}
	if err := platformconfig.ValidatePlatformIntent(result.Intent); err == nil {
		t.Fatal("conflict did not prevent compilation")
	}
}
