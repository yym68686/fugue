package dnsserver

import (
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	dns "github.com/miekg/dns"
)

// Exercise the public query dispatcher after signed LKG reload, not just the
// RR helper: a compiler-approved RRset must survive all the way to the wire.
func TestCompilerDNSRecordsSurviveServingWireContract(t *testing.T) {
	const zone = "wire.example.test"
	const key = "synthetic-wire-contract-key"
	cases := []struct{ name, kind, value string }{
		{"v4", "A", "192.0.2.4"}, {"v6", "AAAA", "2001:db8::4"},
		{"alias", "CNAME", "target.wire.example.test."}, {"ns", "NS", "target.wire.example.test."},
		{"mail", "MX", "10 target.wire.example.test."}, {"no-mail", "MX", "0 ."},
		{"_service._tcp", "SRV", "10 20 443 target.wire.example.test."}, {"_disabled._tcp", "SRV", "0 0 0 ."},
		{"caa", "CAA", `0 issue "ca.example.test"`}, {"text", "TXT", "  keep whitespace  "},
		{"long-text", "TXT", " " + strings.Repeat("value", 70) + " "},
	}
	records := make([]model.EdgeDNSRecord, 0, len(cases))
	for _, c := range cases {
		name := c.name + "." + zone
		if err := platformconfig.ValidateDNSIntents([]platformconfig.DNSIntent{{Hostname: name, Type: c.kind, Values: []string{c.value}, TTL: 60}}); err != nil {
			t.Fatalf("compiler rejected %s: %v", c.kind, err)
		}
		records = append(records, model.EdgeDNSRecord{Name: name, Type: c.kind, Values: []string{c.value}, TTL: 60})
	}
	s := NewService(config.DNSConfig{Zone: zone, TTL: 60, CachePath: filepath.Join(t.TempDir(), "cache.json"), BundleSigningKey: key, BundleSigningKeyID: "test", MaxStale: time.Hour}, log.New(io.Discard, "", 0))
	bundle := bundleauth.SignEdgeDNSBundle(model.EdgeDNSBundle{Version: "wire-1", Generation: "wire-1", Zone: zone, GeneratedAt: time.Now(), Records: records}, key, "test", time.Hour)
	if err := s.writeCache(cacheFile{Version: cacheFileVersion, Bundle: bundle, CachedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := s.LoadCache(); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reply := dnsQuery(t, s, c.name+"."+zone+".", dns.StringToType[c.kind])
			raw, err := reply.Pack()
			if err != nil {
				t.Fatal(err)
			}
			wire := new(dns.Msg)
			if err := wire.Unpack(raw); err != nil {
				t.Fatal(err)
			}
			if wire.Rcode != dns.RcodeSuccess || !wire.Authoritative || len(wire.Answer) != 1 {
				t.Fatalf("compiler-approved record lost by executor: %s", wire)
			}
			if wire.Answer[0].Header().Ttl != 60 || wire.Answer[0].Header().Rrtype != dns.StringToType[c.kind] {
				t.Fatalf("wire metadata changed: %s", wire)
			}
			if c.kind == "TXT" {
				if got := strings.Join(wire.Answer[0].(*dns.TXT).Txt, ""); got != c.value {
					t.Fatalf("TXT bytes changed: got %q want %q", got, c.value)
				}
			} else {
				want, err := dns.NewRR(c.name + "." + zone + ". 60 IN " + c.kind + " " + c.value)
				if err != nil {
					t.Fatal(err)
				}
				if wire.Answer[0].String() != want.String() {
					t.Fatalf("wire value changed: got %s want %s", wire.Answer[0], want)
				}
			}
		})
	}
}

func TestAuthoritativeNODATAIncludesSOA(t *testing.T) {
	s := NewService(config.DNSConfig{Zone: "negative.example.test", TTL: 60}, log.New(io.Discard, "", 0))
	s.setBundle(model.EdgeDNSBundle{Zone: s.Config.Zone, Records: []model.EdgeDNSRecord{
		{Name: "existing.negative.example.test", Type: "A", Values: []string{"192.0.2.1"}, TTL: 60},
		{Name: "_acme-challenge.negative.example.test", Type: "TXT", Values: []string{"expired"}, TTL: 60, ValueExpirations: map[string]time.Time{"expired": time.Now().Add(-time.Minute)}},
	}}, "", false, "")
	for _, test := range []struct {
		name  string
		kind  uint16
		rcode int
	}{
		{"existing.negative.example.test.", dns.TypeAAAA, dns.RcodeSuccess},
		{"existing.negative.example.test.", dns.TypeSRV, dns.RcodeSuccess},
		{"existing.negative.example.test.", dns.TypePTR, dns.RcodeSuccess},
		{"_acme-challenge.negative.example.test.", dns.TypeTXT, dns.RcodeSuccess},
		{"missing.negative.example.test.", dns.TypeTXT, dns.RcodeNameError},
	} {
		t.Run(test.name+dns.TypeToString[test.kind], func(t *testing.T) {
			answer := dnsQuery(t, s, test.name, test.kind)
			if answer.Rcode != test.rcode || !answer.Authoritative || len(answer.Answer) != 0 || len(answer.Ns) != 1 {
				t.Fatalf("invalid negative answer: %s", answer)
			}
			if _, ok := answer.Ns[0].(*dns.SOA); !ok {
				t.Fatalf("missing authority SOA: %s", answer)
			}
		})
	}
}
