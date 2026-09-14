package platformconfig

import (
	"reflect"
	"slices"
	"testing"
	"time"
)

func routeDNSFixture() CompileRequest {
	r := placementFixture()
	options := *r.Intent.DNS[0].Application
	r.Intent.DNS[0].Application = nil
	r.Intent.DNS[0].Route = &DNSRouteIntent{DNSApplicationIntent: options, Hostnames: []string{r.Intent.Routes[0].Hostname}}
	r.Intent.DNS[0].Hostname = "target.example.test"
	r.Intent.DNS[0].Type = "FUGUE_ROUTE"
	r.Intent.DNS[0].Values = []string{}
	rebindPlacement(&r)
	return r
}

func TestDNSRouteReferencesCompileLeasedAliasesAndPlatformOwners(t *testing.T) {
	for _, platform := range []bool{false, true} {
		r := routeDNSFixture()
		if platform {
			r.Intent.Routes[0].AppID = ""
			r.Intent.Routes[0].TenantID = ""
			r.Intent.DNS[0].AppID = ""
			r.Intent.DNS[0].TenantID = ""
		}
		route := r.Intent.Routes[0]
		route.Hostname = "second.example.test"
		r.Intent.Routes = append(r.Intent.Routes, route)
		route.PathPrefix = "/api"
		r.Intent.Routes = append(r.Intent.Routes, route)
		r.Intent.DNS[0].Route.Hostnames = append(r.Intent.DNS[0].Route.Hostnames, "second.example.test")
		rebindPlacement(&r)
		before := NormalizePlatformIntent(r.Intent)
		result, err := Compile(r)
		if err != nil {
			t.Fatal(err)
		}
		records := flattenedRecords(t, result)
		if len(records) != 2 || records[0].Hostname != "target.example.test" || records[0].Route != nil || records[0].TTL != 10 {
			t.Fatal(records)
		}
		if !reflect.DeepEqual(before, NormalizePlatformIntent(r.Intent)) {
			t.Fatal("compiler mutated configuration")
		}
		if wire, err := DNSRecordsAt(records, r.RuntimeSnapshot.CapturedAt.Add(10*time.Second)); err != nil || len(wire) != 0 {
			t.Fatal("alias renewed address lease", wire, err)
		}
		slices.Reverse(r.Intent.Routes)
		slices.Reverse(r.Intent.DNS[0].Route.Hostnames)
		replay, err := Compile(r)
		if err != nil || !reflect.DeepEqual(result.DNSArtifact.Content, replay.DNSArtifact.Content) {
			t.Fatal("reference order changed output", err)
		}
		r.Intent.Routes[0].UpstreamURL = "http://changed:8080"
		if _, err := Compile(r); err == nil {
			t.Fatal("changed referenced path reused old readiness")
		}
	}
}

func TestDNSRouteReferencesRejectMissingForeignAndConflictingInputs(t *testing.T) {
	for name, change := range map[string]func(*CompileRequest){
		"no references":           func(r *CompileRequest) { r.Intent.DNS[0].Route.Hostnames = nil },
		"missing referenced host": func(r *CompileRequest) { r.Intent.DNS[0].Route.Hostnames = []string{"absent.example.test"} },
		"duplicate reference": func(r *CompileRequest) {
			r.Intent.DNS[0].Route.Hostnames = append(r.Intent.DNS[0].Route.Hostnames, r.Intent.DNS[0].Route.Hostnames[0])
		},
		"wildcard route":                   func(r *CompileRequest) { r.Intent.DNS[0].Route.Hostnames = []string{"*.example.test"} },
		"bad reference":                    func(r *CompileRequest) { r.Intent.DNS[0].Route.Hostnames = []string{"bad host.example.test"} },
		"foreign owner":                    func(r *CompileRequest) { r.Intent.Routes[0].TenantID = "other" },
		"platform binding to tenant route": func(r *CompileRequest) { r.Intent.DNS[0].AppID = ""; r.Intent.DNS[0].TenantID = "" },
		"partial owner":                    func(r *CompileRequest) { r.Intent.DNS[0].TenantID = "" },
		"literal values":                   func(r *CompileRequest) { r.Intent.DNS[0].Values = []string{"93.184.216.34"} },
		"mixed application":                func(r *CompileRequest) { r.Intent.DNS[0].Application = &DNSApplicationIntent{} },
		"mixed flatten":                    func(r *CompileRequest) { r.Intent.DNS[0].Flatten = &DNSFlattenIntent{} },
		"wire type":                        func(r *CompileRequest) { r.Intent.DNS[0].Type = "A" },
		"missing route object":             func(r *CompileRequest) { r.Intent.DNS[0].Route = nil },
		"competing A": func(r *CompileRequest) {
			r.Intent.DNS = append(r.Intent.DNS, DNSIntent{Hostname: r.Intent.DNS[0].Hostname, Type: "A", Values: []string{"93.184.216.34"}, TTL: 60})
		},
		"missing observation": func(r *CompileRequest) { r.RuntimeSnapshot.DNSPlacements = nil },
	} {
		t.Run(name, func(t *testing.T) {
			r := routeDNSFixture()
			change(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("invalid route binding compiled")
			}
		})
	}
}

func TestDNSRouteNormalizationCopiesReferencesAndUnresolvedCannotServe(t *testing.T) {
	r := routeDNSFixture()
	normalized := NormalizePlatformIntent(r.Intent)
	normalized.DNS[0].Route.Hostnames[0] = "other.example.test"
	if r.Intent.DNS[0].Route.Hostnames[0] == normalized.DNS[0].Route.Hostnames[0] {
		t.Fatal("normalization shares references")
	}
	if err := ValidatePlatformIntent(r.Intent); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDNSIntents(r.Intent.DNS); err == nil {
		t.Fatal("unresolved route reference passed wire validation")
	}
	if _, err := ResolveDNSFlatten(r.Intent.DNS, r.RuntimeSnapshot, r.Policy); err == nil {
		t.Fatal("flatten accepted unresolved route reference")
	}
}
