package platformconfig

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestApplicationDomainsAreBoundedCanonicalAndDetached(t *testing.T) {
	for _, scenario := range []string{"valid", "disabled", "zero ttl", "large ttl", "mixed case", "trailing dot", "url", "wildcard", "empty label", "bad label", "duplicate", "empty reserved", "too many"} {
		t.Run(scenario, func(t *testing.T) {
			d := &ApplicationDomainsIntent{AppBaseDomain: "apps.example.test", CustomDomainBaseDomain: "dns.example.test", ReservedHostnames: []string{"registry.example.test", "api.example.test"}, DefaultDNSTTL: 180}
			switch scenario {
			case "disabled":
				d.AppBaseDomain, d.CustomDomainBaseDomain = "", ""
			case "zero ttl":
				d.DefaultDNSTTL = 0
			case "large ttl":
				d.DefaultDNSTTL = 86401
			case "mixed case":
				d.AppBaseDomain = "APPS.example.test"
			case "trailing dot":
				d.AppBaseDomain += "."
			case "url":
				d.AppBaseDomain = "https://example.test"
			case "wildcard":
				d.AppBaseDomain = "*.example.test"
			case "empty label":
				d.AppBaseDomain = "apps..test"
			case "bad label":
				d.AppBaseDomain = strings.Repeat("a", 64) + ".test"
			case "duplicate":
				d.ReservedHostnames = []string{"api.test", "api.test"}
			case "empty reserved":
				d.ReservedHostnames = []string{""}
			case "too many":
				d.ReservedHostnames = make([]string, 4097)
			}
			in := PlatformIntent{SchemaVersion: SchemaVersion, Generation: "input", ApplicationDomains: d}
			err := ValidatePlatformIntent(in)
			if scenario != "valid" && scenario != "disabled" {
				if err == nil {
					t.Fatal("invalid namespace accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			normalized := NormalizePlatformIntent(in)
			if d.ReservedHostnames[0] != "registry.example.test" {
				t.Fatal("normalization mutated source")
			}
			first, err := PlatformIntentGeneration(in)
			if err != nil {
				t.Fatal(err)
			}
			again, _ := PlatformIntentGeneration(normalized)
			if first != again {
				t.Fatal("ordering changed intent identity")
			}
			normalized.ApplicationDomains.DefaultDNSTTL++
			normalized.ApplicationDomains.ReservedHostnames[0] = "other.example.test"
			changed, _ := PlatformIntentGeneration(normalized)
			if changed == first || d.DefaultDNSTTL != 180 || d.ReservedHostnames[1] != "api.example.test" {
				t.Fatal("namespace missing from digest or source mutated")
			}
		})
	}
}

func TestApplicationDomainsRejectMissingNullAndUnknownFields(t *testing.T) {
	valid := map[string]any{"app_base_domain": "", "custom_domain_base_domain": "", "reserved_hostnames": []string{}, "default_dns_ttl": 60}
	for _, key := range []string{"app_base_domain", "custom_domain_base_domain", "reserved_hostnames", "default_dns_ttl"} {
		for _, null := range []bool{false, true} {
			fields := map[string]any{}
			for k, v := range valid {
				fields[k] = v
			}
			if null {
				fields[key] = nil
			} else {
				delete(fields, key)
			}
			raw, _ := json.Marshal(fields)
			var out ApplicationDomainsIntent
			if json.Unmarshal(raw, &out) == nil {
				t.Fatal("incomplete namespace accepted", key, null)
			}
		}
	}
	raw, _ := json.Marshal(valid)
	var out ApplicationDomainsIntent
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal("explicit disabled namespace rejected", err)
	}
	valid["shell"] = "echo"
	raw, _ = json.Marshal(valid)
	if json.Unmarshal(raw, &out) == nil {
		t.Fatal("unknown namespace field accepted")
	}
}
