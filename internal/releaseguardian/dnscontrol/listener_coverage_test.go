package dnscontrol

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

func TestListenerCoverageRequiresBothTransportsOnEveryAddress(t *testing.T) {
	const zone, primary, v4, v6 = "example.test", "ns.example.test.", "8.8.8.8", "2606:4700:4700::1111"
	authority := platformconfig.DNSAuthorityPolicy{NodeID: "dns-a", Zone: zone, Nameservers: []string{primary}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}
	probe := func(kind, ip string) platformconfig.DNSIntent {
		return platformconfig.DNSIntent{Hostname: "probe." + zone, Type: kind, Values: []string{ip}, TTL: 60,
			Status: model.EdgeRouteStatusActive, RecordKind: model.EdgeDNSRecordKindProbe, EdgeGroupID: "group-a"}
	}
	p := platformconfig.DNSArtifactPayload{
		Policy: platformconfig.PolicySnapshot{DNSAuthorities: []platformconfig.DNSAuthorityPolicy{authority}},
		ConsumerViews: []platformconfig.DNSConsumerView{{NodeID: authority.NodeID, EdgeGroupID: "group-a", Zone: zone,
			ProbeLabel: "probe", Records: []platformconfig.DNSIntent{probe("AAAA", v6), probe("A", v4)}}},
	}
	// Authorities in verified artifacts are stored without terminal dots.
	p.Policy.DNSAuthorities[0].Nameservers[0] = "ns.example.test"
	checks, err := buildListenerChecks(p, 42)
	if err != nil || len(checks) != 12 {
		t.Fatalf("expected both transports and all three questions on each address, got %d: %v", len(checks), err)
	}
	var observed sync.Map
	respond := func(_ context.Context, transport, addr string, req *dns.Msg) (*dns.Msg, error) {
		q := req.Question[0]
		if _, dup := observed.LoadOrStore(fmt.Sprintf("%s/%s/%d", transport, addr, q.Qtype), true); dup {
			return nil, fmt.Errorf("duplicate check")
		}
		if req.RecursionDesired {
			return nil, fmt.Errorf("recursive probe")
		}
		r := new(dns.Msg).SetReply(req)
		r.Authoritative = true
		h := dns.RR_Header{Name: q.Name, Rrtype: q.Qtype, Class: dns.ClassINET, Ttl: 60}
		switch q.Qtype {
		case dns.TypeSOA:
			r.Answer = []dns.RR{&dns.SOA{Hdr: h, Serial: 42, Ns: primary}}
		case dns.TypeA:
			r.Answer = []dns.RR{&dns.A{Hdr: h, A: net.ParseIP(v4)}}
		case dns.TypeAAAA:
			r.Answer = []dns.RR{&dns.AAAA{Hdr: h, AAAA: net.ParseIP(v6)}}
		}
		return r, nil
	}
	if err := runListenerChecks(t.Context(), checks, respond); err != nil {
		t.Fatal(err)
	}
	for _, transport := range []string{"udp", "tcp"} {
		for _, addr := range []string{v4, v6} {
			for _, qtype := range []uint16{dns.TypeSOA, dns.TypeA, dns.TypeAAAA} {
				key := fmt.Sprintf("%s/%s/%d", transport, addr, qtype)
				if _, ok := observed.Load(key); !ok {
					t.Errorf("missing probe for %s", key)
				}
			}
		}
	}

	for _, check := range checks {
		observed.Clear()
		if err := check.observe(t.Context(), func(ctx context.Context, transport, addr string, req *dns.Msg) (*dns.Msg, error) {
			r, err := respond(ctx, transport, addr, req)
			switch a := r.Answer[0].(type) {
			case *dns.SOA:
				a.Serial--
			case *dns.A:
				a.A = net.ParseIP("9.9.9.9")
			case *dns.AAAA:
				a.AAAA = net.ParseIP("2001:4860:4860::8888")
			}
			return r, err
		}); err == nil {
			t.Errorf("stale SOA or incorrect probe was accepted for %s %d", check.Address, check.Type)
		}
	}

	// A missing zone report cannot be hidden by a second view of one zone.
	p.Policy.DNSAuthorities = append(p.Policy.DNSAuthorities, platformconfig.DNSAuthorityPolicy{
		NodeID: "dns-b", Zone: zone, Nameservers: []string{"ns-b.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600,
	})
	if _, err := buildListenerChecks(p, 42); err == nil {
		t.Fatal("accepted incomplete listener inventory")
	}
}
