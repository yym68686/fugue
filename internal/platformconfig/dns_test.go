package platformconfig

import (
	"reflect"
	"strings"
	"testing"
)

func TestDNSCompilerDeterministicRRsetsAndTXTBytes(t *testing.T) {
	intent := PlatformIntent{Generation: "dns", DNS: []DNSIntent{
		{Hostname: "records.example", Type: "TXT", Values: []string{"  exact text  ", strings.Repeat("x", 600)}, TTL: 60},
		{Hostname: "records.example", Type: "MX", Values: []string{"10 mail.example."}, TTL: 60},
		{Hostname: "records.example", Type: "A", Values: []string{"192.0.2.2", "192.0.2.1"}, TTL: 60},
	}}
	request := CompileRequest{Intent: intent, Policy: PolicySnapshot{Generation: "policy"}}
	first, err := Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Intent.DNS = append([]DNSIntent(nil), intent.DNS...)
	request.Intent.DNS[0], request.Intent.DNS[2] = request.Intent.DNS[2], request.Intent.DNS[0]
	second, err := Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.DNSArtifact.Content, second.DNSArtifact.Content) {
		t.Fatal("RRset input order changed digest")
	}
	normalized := NormalizePlatformIntent(intent)
	if normalized.DNS[2].Values[0] != "  exact text  " {
		t.Fatal("TXT whitespace lost")
	}
	normalized.DNS[2].Values[0] = "changed"
	if intent.DNS[0].Values[0] != "  exact text  " {
		t.Fatal("normalization mutated original values")
	}
}

func TestDNSCompilerRejectsUnresolvedAndMalformedRecords(t *testing.T) {
	for name, records := range map[string][]DNSIntent{
		"symbolic app":    {{Hostname: "app.example", Type: "FUGUE_APP", Values: []string{"app"}, TTL: 60}},
		"symbolic alias":  {{Hostname: "app.example", Type: "ALIAS", Values: []string{"target.example"}, TTL: 60}},
		"flatten CNAME":   {{Hostname: "app.example", Type: "CNAME", Values: []string{"target.example"}, TTL: 60, Flatten: &DNSFlattenIntent{Mode: "always"}}},
		"wrong IP family": {{Hostname: "app.example", Type: "AAAA", Values: []string{"192.0.2.1"}, TTL: 60}},
		"empty value":     {{Hostname: "app.example", Type: "A", Values: []string{"192.0.2.1", ""}, TTL: 60}},
		"zero TTL":        {{Hostname: "app.example", Type: "TXT", Values: []string{"value"}}},
		"bad MX":          {{Hostname: "app.example", Type: "MX", Values: []string{"mail.example"}, TTL: 60}},
		"bad SRV":         {{Hostname: "_svc._tcp.example", Type: "SRV", Values: []string{"70000 0 80 target.example"}, TTL: 60}},
		"bad CAA":         {{Hostname: "app.example", Type: "CAA", Values: []string{"999 issue \"ca.example\""}, TTL: 60}},
		"injected record": {{Hostname: "app.example", Type: "NS", Values: []string{"ns.example\nother.example IN A 192.0.2.1"}, TTL: 60}},
		"CNAME conflict":  {{Hostname: "app.example", Type: "CNAME", Values: []string{"target.example"}, TTL: 60}, {Hostname: "app.example", Type: "A", Values: []string{"192.0.2.1"}, TTL: 60}},
		"duplicate RRset": {{Hostname: "app.example", Type: "A", Values: []string{"192.0.2.1"}, TTL: 60}, {Hostname: "APP.EXAMPLE.", Type: "a", Values: []string{"192.0.2.2"}, TTL: 60}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Compile(CompileRequest{Intent: PlatformIntent{Generation: "invalid", DNS: records}, Policy: PolicySnapshot{Generation: "policy"}})
			if err == nil {
				t.Fatal("invalid DNS accepted")
			}
		})
	}
}
