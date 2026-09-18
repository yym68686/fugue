package platformconfig

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestDNSClientPolicyPreservesPrecedenceAndVersionsCompiler(t *testing.T) {
	req := dnsQueryFixture()
	req.Policy.DNSClientPolicies = []DNSClientPolicy{{NodeID: "dns-a", Rules: []DNSClientRule{{CIDR: "192.0.2.0/24", Country: "aa"}, {CIDR: "192.0.2.128/25", Country: "bb"}, {CIDR: "2001:db8::/32", ASN: "64500"}}}}
	rebindPlacement(&req)
	before, _ := json.Marshal(req)
	result, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(req)
	if string(before) != string(after) {
		t.Fatal("compiler mutated input")
	}
	again, err := Compile(req)
	if err != nil || !reflect.DeepEqual(result.DNSArtifact.Content, again.DNSArtifact.Content) {
		t.Fatal("replay differs", err)
	}
	matcher, err := NewDNSClientMatcher(req.Policy.DNSClientPolicies[0].Rules)
	if err != nil {
		t.Fatal(err)
	}
	rule, ok := matcher.Lookup("192.0.2.200")
	if !ok || rule.Country != "aa" {
		t.Fatal("ordered precedence changed")
	}
	rule, ok = matcher.Lookup("2001:db8::1")
	if !ok || rule.ASN != "64500" {
		t.Fatal("IPv6 mapping missing")
	}
	if _, ok := matcher.Lookup("198.51.100.1"); ok {
		t.Fatal("outside CIDR matched")
	}
	slices.Reverse(req.Policy.DNSClientPolicies[0].Rules)
	rebindPlacement(&req)
	changed, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Lineage.PolicyDigest == result.Lineage.PolicyDigest || changed.DNSArtifact.Generation == result.DNSArtifact.Generation || changed.Lineage.IntentDigest != result.Lineage.IntentDigest {
		t.Fatal("precedence not versioned solely in policy")
	}
}

func TestDNSClientPolicyRejectsMalformedUnownedOrUnbounded(t *testing.T) {
	for name, mutate := range map[string]func(*CompileRequest){
		"CIDR":          func(r *CompileRequest) { r.Policy.DNSClientPolicies[0].Rules[0].CIDR = "not-a-network" },
		"mapped prefix": func(r *CompileRequest) { r.Policy.DNSClientPolicies[0].Rules[0].CIDR = "::ffff:192.0.2.0/120" },
		"duplicate CIDR": func(r *CompileRequest) {
			r.Policy.DNSClientPolicies[0].Rules = append(r.Policy.DNSClientPolicies[0].Rules, r.Policy.DNSClientPolicies[0].Rules[0])
		},
		"control byte":  func(r *CompileRequest) { r.Policy.DNSClientPolicies[0].Rules[0].Region = "a\nb" },
		"large ASN":     func(r *CompileRequest) { r.Policy.DNSClientPolicies[0].Rules[0].ASN = strings.Repeat("x", 65) },
		"too many":      func(r *CompileRequest) { r.Policy.DNSClientPolicies[0].Rules = make([]DNSClientRule, 257) },
		"foreign owner": func(r *CompileRequest) { r.Policy.DNSClientPolicies[0].NodeID = "foreign" },
		"duplicate owner": func(r *CompileRequest) {
			r.Policy.DNSClientPolicies = append(r.Policy.DNSClientPolicies, r.Policy.DNSClientPolicies[0])
		},
		"incomplete topology": func(r *CompileRequest) {
			r.Intent.DNSConsumers = append(r.Intent.DNSConsumers, DNSConsumerIntent{NodeID: "dns-b"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := dnsQueryFixture()
			r.Policy.DNSClientPolicies = []DNSClientPolicy{{NodeID: "dns-a", Rules: []DNSClientRule{{CIDR: "192.0.2.0/24", Country: "aa"}}}}
			mutate(&r)
			rebindPlacement(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("invalid client policy accepted")
			}
		})
	}
}

func TestDNSClientNormalizationCopiesWithoutReorderingRules(t *testing.T) {
	in := PolicySnapshot{DNSClientPolicies: []DNSClientPolicy{{NodeID: "dns-b", Rules: []DNSClientRule{{CIDR: "192.0.2.99/24", Country: " AA "}, {CIDR: "192.0.2.128/25", Country: "bb"}}}, {NodeID: "dns-a", Rules: []DNSClientRule{}}}}
	raw, _ := json.Marshal(in)
	out := NormalizePolicySnapshot(in)
	if out.DNSClientPolicies[0].NodeID != "dns-a" || out.DNSClientPolicies[0].Rules == nil || out.DNSClientPolicies[1].Rules[0].Country != "aa" || out.DNSClientPolicies[1].Rules[0].CIDR != "192.0.2.0/24" {
		t.Fatal("normalization lost canonical precedence")
	}
	out.DNSClientPolicies[1].Rules[0].Country = "zz"
	after, _ := json.Marshal(in)
	if string(raw) != string(after) {
		t.Fatal("normalization changed caller")
	}
}
