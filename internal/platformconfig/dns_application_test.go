package platformconfig

import (
	"strings"
	"testing"
)

func applicationDNSFixture() PlatformIntent {
	return PlatformIntent{Generation: "application-dns", Routes: []RouteIntent{{Hostname: "app.example.test", AppID: "app-a", TenantID: "tenant-a", UpstreamURL: "http://origin:8080", Enabled: true}}, DNS: []DNSIntent{{Hostname: "app.example.test", AppID: "app-a", TenantID: "tenant-a", Type: "FUGUE_APP", Values: []string{"app-a"}, TTL: 60, Application: &DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}}
}

func TestApplicationDNSIntentCanBeVersionedButNotServedUnresolved(t *testing.T) {
	intent := applicationDNSFixture()
	if err := ValidatePlatformIntent(intent); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDNSIntents(intent.DNS); err == nil {
		t.Fatal("symbolic application reference passed wire validation")
	}
	if result, err := Compile(CompileRequest{Intent: intent, Policy: PolicySnapshot{Generation: "policy"}}); err == nil || !strings.Contains(err.Error(), "fixed placement resolution") || result.DNSArtifact.Content != nil {
		t.Fatalf("unresolved binding produced serving content: %+v %v", result, err)
	}
	first, err := PlatformIntentGeneration(intent)
	if err != nil {
		t.Fatal(err)
	}
	normalized := NormalizePlatformIntent(intent)
	normalized.DNS[0].Application.IPv4Policy = "ipv4_only"
	second, err := PlatformIntentGeneration(normalized)
	if err != nil || first == second || intent.DNS[0].Application.IPv4Policy != "auto" {
		t.Fatal("policy mutation changed original or failed to version intent", err)
	}
}

func TestApplicationDNSIntentRejectsIncompleteConflictingAndForeignBindings(t *testing.T) {
	for name, mutate := range map[string]func(*PlatformIntent){
		"no policy":         func(i *PlatformIntent) { i.DNS[0].Application = nil },
		"no app":            func(i *PlatformIntent) { i.DNS[0].AppID = "" },
		"no tenant":         func(i *PlatformIntent) { i.DNS[0].TenantID = "" },
		"name as value":     func(i *PlatformIntent) { i.DNS[0].Values = []string{"display-name"} },
		"no matching route": func(i *PlatformIntent) { i.Routes = nil },
		"foreign tenant":    func(i *PlatformIntent) { i.Routes[0].TenantID = "foreign" },
		"foreign app":       func(i *PlatformIntent) { i.Routes[0].AppID = "app-b" },
		"bad TTL":           func(i *PlatformIntent) { i.DNS[0].TTL = 0 },
		"bad hostname":      func(i *PlatformIntent) { i.DNS[0].Hostname = "bad host.example" },
		"mixed resolver":    func(i *PlatformIntent) { i.DNS[0].Flatten = &DNSFlattenIntent{} },
		"wire record":       func(i *PlatformIntent) { i.DNS[0].Type = "A" },
		"IP family":         func(i *PlatformIntent) { i.DNS[0].Application.IPv4Policy = "unknown" },
		"family conflict": func(i *PlatformIntent) {
			i.DNS[0].Application.IPv4Policy = "ipv4_only"
			i.DNS[0].Application.IPv6Policy = "ipv6_only"
		},
		"TTL policy": func(i *PlatformIntent) { i.DNS[0].Application.TTLPolicy = "unknown" },
		"fallback":   func(i *PlatformIntent) { i.DNS[0].Application.FallbackPolicy = "unknown" },
		"competing A": func(i *PlatformIntent) {
			i.DNS = append(i.DNS, DNSIntent{Hostname: i.DNS[0].Hostname, Type: "A", Values: []string{"192.0.2.1"}, TTL: 60})
		},
	} {
		t.Run(name, func(t *testing.T) {
			intent := applicationDNSFixture()
			mutate(&intent)
			if err := ValidatePlatformIntent(intent); err == nil {
				t.Fatal("invalid application intent accepted")
			}
		})
	}
}
