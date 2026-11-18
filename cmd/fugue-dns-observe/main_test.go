package main

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/miekg/dns"
)

func TestDNSObservationProbesBothTransportsOnDeclaredDiagnosticSocket(t *testing.T) {
	tcp, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := tcp.Addr().(*net.TCPAddr)
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: address.IP, Port: address.Port})
	if err != nil {
		_ = tcp.Close()
		t.Fatal(err)
	}
	handler := dns.HandlerFunc(func(w dns.ResponseWriter, query *dns.Msg) {
		reply := new(dns.Msg).SetReply(query)
		reply.Authoritative = true
		reply.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: query.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}, A: net.IPv4(192, 0, 2, 2)}}
		_ = w.WriteMsg(reply)
	})
	var servers sync.WaitGroup
	for _, server := range []*dns.Server{{Listener: tcp, Handler: handler}, {PacketConn: udp, Handler: handler}} {
		started := make(chan struct{})
		server.NotifyStartedFunc = func() { close(started) }
		servers.Add(1)
		go func() { defer servers.Done(); _ = server.ActivateAndServe() }()
		<-started
		t.Cleanup(func() { _ = server.Shutdown() })
	}
	defer func() { _ = tcp.Close(); _ = udp.Close(); servers.Wait() }()
	cfg := configuration{Schema: "fugue.dns-public-observation/v2", Servers: []string{"127.0.0.1"},
		AuthorityPort: address.Port, TimeoutSeconds: 1, Queries: []queryIntent{{Hostname: "app.example.test", Type: "A", Allowed: []string{"192.0.2.2"}, Minimum: 1}}}
	if err := validate(cfg); err != nil {
		t.Fatal(err)
	}
	got, passed := observe(context.Background(), cfg)
	if !passed || len(got) != 2 || got[0].Transport != "udp" || got[1].Transport != "tcp" {
		t.Fatalf("did not verify both transports on the diagnostic port: %+v", got)
	}
	cfg.RecursiveServers = []string{"127.0.0.2"}
	if validate(cfg) == nil {
		t.Fatal("diagnostic port accepted an unrelated recursive observation")
	}
}

func TestDNSObservationNeverAcceptsUnexpectedIdentityOrAddress(t *testing.T) {
	query := queryIntent{Hostname: "app.example.test", Type: "A", Allowed: []string{"192.0.2.2"}, Minimum: 1}
	request := new(dns.Msg)
	request.SetQuestion(dns.Fqdn(query.Hostname), dns.TypeA)
	request.RecursionDesired = false
	for _, scenario := range []string{"valid", "wrong_id", "wrong_question", "old_address", "servfail", "not_authoritative", "empty", "truncated"} {
		answer := new(dns.Msg)
		answer.SetReply(request)
		answer.Authoritative = true
		record, err := dns.NewRR("app.example.test. 60 IN A 192.0.2.2")
		if err != nil {
			t.Fatal(err)
		}
		answer.Answer = []dns.RR{record}
		switch scenario {
		case "wrong_id":
			answer.Id++
		case "wrong_question":
			answer.Question[0].Name = "other.example.test."
		case "old_address":
			answer.Answer[0], _ = dns.NewRR("app.example.test. 60 IN A 192.0.2.1")
		case "servfail":
			answer.Rcode = dns.RcodeServerFailure
		case "not_authoritative":
			answer.Authoritative = false
		case "empty":
			answer.Answer = nil
		case "truncated":
			answer.Truncated = true
		}
		result := observation{}
		evaluate(answer, request, query, &result)
		if result.Passed != (scenario == "valid") {
			t.Fatal(scenario, result)
		}
	}
}

func TestRecursiveSamplesDoNotRequireAuthoritativeBitOrAuthorizeTraffic(t *testing.T) {
	request := new(dns.Msg)
	request.SetQuestion("app.example.test.", dns.TypeA)
	request.RecursionDesired = true
	answer := new(dns.Msg)
	answer.SetReply(request)
	answer.RecursionAvailable = true
	record, err := dns.NewRR("app.example.test. 17 IN A 192.0.2.2")
	if err != nil {
		t.Fatal(err)
	}
	answer.Answer = []dns.RR{record}
	intent := queryIntent{Hostname: "app.example.test", Type: "A", Allowed: []string{"192.0.2.2"}, Minimum: 1}
	result := observation{}
	evaluate(answer, request, intent, &result)
	if !result.Passed || result.Authoritative || len(result.TTLs) != 1 || result.TTLs[0] != 17 {
		t.Fatal("recursive sample lost cache TTL or authority boundary", result)
	}
	answer.RecursionAvailable = false
	result = observation{}
	evaluate(answer, request, intent, &result)
	if result.Passed {
		t.Fatal("non-recursive endpoint accepted")
	}
}
