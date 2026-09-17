package api

import (
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func platformDomainDNSFixture(t *testing.T) (*Server, platformIntentProjectionResponse, []model.AppDomain) {
	t.Helper()
	s := &Server{appBaseDomain: "example.test", customDomainBaseDomain: "dns.example.test", dnsBundleTTL: 90, reservedAppHosts: map[string]struct{}{"api.example.test": {}}}
	s.dnsStaticRecords = []model.EdgeDNSRecord{
		{Name: "example.test", Type: "A", Values: []string{"192.0.2.1"}, TTL: 300, RecordKind: model.EdgeDNSRecordKindProtected},
		{Name: "example.test", Type: "AAAA", Values: []string{"2001:db8::1"}, TTL: 300},
		{Name: "www.example.test", Type: "CNAME", Values: []string{"old.example.test"}, TTL: 300},
		{Name: "example.test", Type: "TXT", Values: []string{"verification-record"}, TTL: 300, RecordKind: model.EdgeDNSRecordKindProtected},
		{Name: "api.example.test", Type: "A", Values: []string{"192.0.2.2"}, TTL: 300},
		{Name: "pending.example.test", Type: "A", Values: []string{"192.0.2.3"}, TTL: 300},
		{Name: "name.dns.example.test", Type: "A", Values: []string{"192.0.2.4"}, TTL: 300},
	}
	domains := []model.AppDomain{{Hostname: "example.test", AppID: "root", TenantID: "tenant", Status: "verified"}, {Hostname: "www.example.test", AppID: "root", TenantID: "tenant", Status: "verified"},
		{Hostname: "api.example.test", AppID: "root", TenantID: "tenant", Status: "verified"}, {Hostname: "pending.example.test", AppID: "root", TenantID: "tenant", Status: "pending"}, {Hostname: "name.dns.example.test", AppID: "root", TenantID: "tenant", Status: "verified"}}
	result := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "before", Routes: []platformconfig.RouteIntent{
		{Hostname: "example.test", AppID: "root", TenantID: "tenant", UpstreamURL: "http://root", Enabled: true},
		{Hostname: "example.test", PathPrefix: "/api", AppID: "child", TenantID: "tenant", UpstreamURL: "http://child", Enabled: false},
		{Hostname: "www.example.test", AppID: "root", TenantID: "tenant", UpstreamURL: "http://root", Enabled: true},
	}}}
	result.Intent.DNS, _ = projectBusinessDNSDraft(&result, nil, nil, nil, s.dnsStaticRecords)
	return s, result, domains
}

func TestPlatformDomainDNSReplacesExactStaticAddressesWithOwnedRouteReferences(t *testing.T) {
	s, result, domains := platformDomainDNSFixture(t)
	staticBefore, _ := json.Marshal(s.dnsStaticRecords)
	domainBefore, _ := json.Marshal(domains)
	if err := s.projectPlatformDomainDNS(&result, domains); err != nil {
		t.Fatal(err)
	}
	if len(result.DNSExclusions) != 3 || len(result.Issues) != 0 {
		t.Fatal("static override audit incomplete", result.DNSExclusions, result.Issues)
	}
	counts := map[string]int{}
	for _, record := range result.Intent.DNS {
		counts[record.Hostname+":"+record.Type]++
		if record.Type != "FUGUE_ROUTE" {
			continue
		}
		if len(record.Values) != 0 || record.TTL != 90 || record.AppID != "root" || record.TenantID != "tenant" || record.RecordKind != model.EdgeDNSRecordKindPlatformDomain || record.Route == nil {
			t.Fatal("selected addresses or old static TTL leaked into desired platform domain", record)
		}
		if record.Hostname == "example.test" && (len(record.Route.Bindings) != 2 || record.Route.Bindings[1].AppID != "child") {
			t.Fatal("shared path owner missing from DNS dependency graph", record.Route)
		}
	}
	for _, key := range []string{"example.test:FUGUE_ROUTE", "www.example.test:FUGUE_ROUTE", "example.test:TXT", "api.example.test:A", "pending.example.test:A", "name.dns.example.test:A"} {
		if counts[key] != 1 {
			t.Fatal("record priority or protected scope changed", key, counts)
		}
	}
	if counts["example.test:A"] != 0 || counts["example.test:AAAA"] != 0 || counts["www.example.test:CNAME"] != 0 || len(counts) != 6 {
		t.Fatal("old static address survived desired override", counts)
	}
	if result.Intent.Generation == "before" || result.Intent.Generation != result.RuntimeSnapshot.IntentGeneration || platformconfig.ValidatePlatformIntent(result.Intent) != nil {
		t.Fatal("intent generation or graph validation missing")
	}
	staticAfter, _ := json.Marshal(s.dnsStaticRecords)
	domainAfter, _ := json.Marshal(domains)
	if string(staticBefore) != string(staticAfter) || string(domainBefore) != string(domainAfter) {
		t.Fatal("projection mutated its frozen sources")
	}
	_, again, reordered := platformDomainDNSFixture(t)
	for i, j := 0, len(reordered)-1; i < j; i, j = i+1, j-1 {
		reordered[i], reordered[j] = reordered[j], reordered[i]
	}
	if err := s.projectPlatformDomainDNS(&again, reordered); err != nil || !reflect.DeepEqual(result.Intent, again.Intent) || !reflect.DeepEqual(result.DNSExclusions, again.DNSExclusions) {
		t.Fatal("domain ordering changed deterministic projection", err)
	}
	if _, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: result.Intent, Policy: platformconfig.PolicySnapshot{Generation: "policy"}}); err == nil {
		t.Fatal("symbolic route references bypassed placement evidence")
	}
}

func TestPlatformDomainDNSPreservesConflictingHostedAddress(t *testing.T) {
	s, result, domains := platformDomainDNSFixture(t)
	result.Intent.DNS = append(result.Intent.DNS, platformconfig.DNSIntent{Hostname: "example.test", Type: "A", Values: []string{"192.0.2.99"}, TTL: 600, RecordKind: model.EdgeDNSRecordKindHosted, TenantID: "other-tenant"})
	if err := s.projectPlatformDomainDNS(&result, domains); err != nil {
		t.Fatal(err)
	}
	found, issue := false, false
	for _, record := range result.Intent.DNS {
		found = found || record.Hostname == "example.test" && record.Type == "A" && record.TenantID == "other-tenant"
	}
	for _, i := range result.Issues {
		issue = issue || i.Code == "platform_domain_dns_conflicting_business_record"
	}
	if !found || !issue || platformconfig.ValidatePlatformIntent(result.Intent) == nil {
		t.Fatal("hosted address conflict was silently overwritten or compiled", result.Issues)
	}
}

func TestPlatformDomainDNSRejectsWrongOwnerAndMissingRoutes(t *testing.T) {
	for _, scenario := range []string{"missing route", "foreign path", "missing owner", "duplicate binding"} {
		t.Run(scenario, func(t *testing.T) {
			s, result, domains := platformDomainDNSFixture(t)
			switch scenario {
			case "missing route":
				result.Intent.Routes = nil
			case "foreign path":
				result.Intent.Routes[1].TenantID = "other-tenant"
			case "missing owner":
				domains[0].AppID = "other-app"
			case "duplicate binding":
				domains = append(domains, domains[0])
			}
			if err := s.projectPlatformDomainDNS(&result, domains); err == nil {
				t.Fatal("unbound platform domain accepted")
			}
		})
	}
}
