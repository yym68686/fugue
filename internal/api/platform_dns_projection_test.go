package api

import (
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestBusinessDNSProjectionExcludesDeletedAndUnownedRecords(t *testing.T) {
	zones := []model.HostedZone{{ID: "active", ZoneName: "example", TenantID: "tenant", Status: model.HostedZoneStatusActive}, {ID: "deleted", ZoneName: "deleted.example", TenantID: "tenant", Status: model.HostedZoneStatusDeleted}}
	records := []model.DNSRecord{
		{ID: "a", ZoneID: "active", TenantID: "tenant", FQDN: "app.example", Type: "A", TTL: 60, Values: []string{"192.0.2.1"}},
		{ID: "deleted-row", ZoneID: "deleted", TenantID: "tenant", FQDN: "deleted.example", Type: "A", TTL: 60, Values: []string{"192.0.2.2"}},
		{ID: "orphan", ZoneID: "missing", TenantID: "tenant", FQDN: "orphan.example", Type: "A", TTL: 60, Values: []string{"192.0.2.3"}},
		{ID: "foreign", ZoneID: "active", TenantID: "other", FQDN: "foreign.example", Type: "A", TTL: 60, Values: []string{"192.0.2.4"}},
		{ID: "outside", ZoneID: "active", TenantID: "tenant", FQDN: "other.test", Type: "A", TTL: 60, Values: []string{"192.0.2.5"}},
	}
	result := platformIntentProjectionResponse{}
	dns, excluded := projectBusinessDNSDraft(&result, nil, zones, records, nil)
	if len(dns) != 1 || len(excluded) != 4 || dns[0].RecordKind != model.EdgeDNSRecordKindHosted || dns[0].AppID != "" {
		t.Fatalf("incorrect DNS projection: %+v %+v", dns, excluded)
	}
	if len(result.Issues) != 3 {
		t.Fatalf("deleted tombstone confused with orphan: %+v", result.Issues)
	}
	for _, issue := range result.Issues {
		if issue.Hostname == "deleted.example" {
			t.Fatal("deleted zone blocks migration")
		}
	}
	for _, row := range excluded {
		if row.RecordID == "deleted-row" && row.Reason != "dns_zone_deleted" {
			t.Fatal("deleted zone reason lost")
		}
	}
}

func TestBusinessDNSProjectionPreservesDesiredBindingsAndStaticInputs(t *testing.T) {
	zones := []model.HostedZone{{ID: "zone", ZoneName: "example", TenantID: "tenant", Status: model.HostedZoneStatusActive}}
	apps := map[string]model.App{"app-id": {ID: "app-id", TenantID: "tenant", Name: "app"}}
	records := []model.DNSRecord{
		{ID: "app", ZoneID: "zone", TenantID: "tenant", FQDN: "app.example", Type: model.DNSRecordTypeFUGUEAPP, Values: []string{"app-id"}, TTL: 60, SourceRefID: "app.example"},
		{ID: "flatten", ZoneID: "zone", TenantID: "tenant", FQDN: "alias.example", Type: model.DNSRecordTypeCNAME, Values: []string{"external.example"}, TTL: 60, FlattenMode: "always", FlattenTarget: "external.example", FlattenedA: []string{"192.0.2.1"}},
	}
	static := []model.EdgeDNSRecord{{Name: "example", Type: "NS", TTL: 300, Values: []string{"ns.example"}, RecordKind: "protected", Status: "active"}}
	result := platformIntentProjectionResponse{}
	first, excluded := projectBusinessDNSDraft(&result, apps, zones, records, static)
	if len(first) != 3 || len(excluded) != 0 {
		t.Fatalf("missing DNS sources: %+v", first)
	}
	if first[1].AppID != "app-id" {
		t.Fatal("source_ref hostname used as app identity")
	}
	if first[1].Application == nil || first[1].Application.IPv4Policy != "auto" || first[1].Application.TTLPolicy != "bounded" {
		t.Fatal("application DNS behavior was lost")
	}
	if first[0].Flatten == nil || first[0].Flatten.Target != "external.example" {
		t.Fatal("flatten intent lost")
	}
	if err := platformconfig.ValidateDNSIntents(first); err == nil {
		t.Fatal("unresolved draft is compilable")
	}
	records[1].FlattenedA = []string{"192.0.2.2"}
	records[1].FlattenStatus = "error"
	records[1].ResolveError = "transient failure"
	second, _ := projectBusinessDNSDraft(&result, apps, zones, records, static)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("runtime DNS resolution changed desired configuration")
	}
	static[0].Values[0] = "changed"
	if first[2].Values[0] != "ns.example" {
		t.Fatal("static values alias input")
	}
}

func TestBusinessDNSProjectionResolvesNamesToStableOwnedApplicationIDs(t *testing.T) {
	zones := []model.HostedZone{{ID: "zone", ZoneName: "example.test", TenantID: "tenant", Status: model.HostedZoneStatusActive}}
	apps := map[string]model.App{"id-a": {ID: "id-a", TenantID: "tenant", Name: "display-name"}}
	records := []model.DNSRecord{{ID: "dns", ZoneID: "zone", TenantID: "tenant", FQDN: "app.example.test", Type: "FUGUE_APP", Values: []string{"display-name"}, TTL: 60, FlattenIPv4Policy: "ipv4_only", FlattenTTLPolicy: "min", FlattenFallbackPolicy: "stale_if_error"}}
	result := platformIntentProjectionResponse{}
	projected, _ := projectBusinessDNSDraft(&result, apps, zones, records, nil)
	if len(projected) != 1 || projected[0].AppID != "id-a" || !reflect.DeepEqual(projected[0].Values, []string{"id-a"}) || projected[0].Application.IPv4Policy != "ipv4_only" || projected[0].Application.TTLPolicy != "min" || projected[0].Application.FallbackPolicy != "stale_if_error" {
		t.Fatalf("application desired configuration lost: %+v", projected)
	}
	apps["id-b"] = model.App{ID: "id-b", TenantID: "tenant", Name: "display-name"}
	ambiguous, _ := projectBusinessDNSDraft(&result, apps, zones, records, nil)
	if ambiguous[0].AppID != "" {
		t.Fatal("ambiguous name silently selected an app")
	}
}
