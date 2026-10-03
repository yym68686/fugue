package cli

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
)

func authoritativeAnswer(query *dns.Msg, ip string) *dns.Msg {
	answer := new(dns.Msg)
	answer.SetReply(query)
	answer.Authoritative = true
	answer.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: query.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}, A: net.ParseIP(ip)}}
	return answer
}
func TestStaticEdgeAuthoritativeRejectsStaleAndRecursiveAnswers(t *testing.T) {
	q := new(dns.Msg)
	q.SetQuestion("example.test.", dns.TypeA)
	tests := []struct {
		name   string
		mutate func(*dns.Msg)
	}{
		{"stale", func(a *dns.Msg) { a.Answer[0].(*dns.A).A = net.ParseIP("192.0.2.10") }},
		{"recursive", func(a *dns.Msg) { a.Authoritative = false }},
		{"wrong-id", func(a *dns.Msg) { a.Id++ }},
		{"wrong-question", func(a *dns.Msg) {
			a.Question = []dns.Question{{Name: "other.test.", Qtype: dns.TypeA, Qclass: dns.ClassINET}}
		}},
		{"truncated", func(a *dns.Msg) { a.Truncated = true }},
		{"multiple", func(a *dns.Msg) { a.Answer = append(a.Answer, a.Answer[0]) }},
		{"servfail", func(a *dns.Msg) { a.Rcode = dns.RcodeServerFailure }},
		{"missing", func(a *dns.Msg) { a.Answer = nil }},
		{"cname", func(a *dns.Msg) {
			a.Answer = []dns.RR{&dns.CNAME{Hdr: dns.RR_Header{Name: "example.test.", Rrtype: dns.TypeCNAME, Class: dns.ClassINET}, Target: "other.test."}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := authoritativeAnswer(q, "192.0.2.20")
			test.mutate(a)
			if e := verifyStaticEdgeDNSAnswer(q, a, "192.0.2.20"); e == nil {
				t.Fatal("unsafe answer accepted")
			}
		})
	}
	if e := verifyStaticEdgeDNSAnswer(q, authoritativeAnswer(q, "192.0.2.20"), "192.0.2.20"); e != nil {
		t.Fatal(e)
	}
}
func TestStaticEdgeAuthoritativeWaitsForEveryNameserverAndProtocol(t *testing.T) {
	calls := map[string]int{}
	exchange := func(_ context.Context, address, network string, q *dns.Msg, _ time.Duration) (*dns.Msg, error) {
		if q.RecursionDesired {
			t.Fatal("recursive query")
		}
		key := address + "/" + network
		calls[key]++
		ip := "192.0.2.20"
		if address == "192.0.2.54:53" && network == "tcp" && calls[key] == 1 {
			ip = "192.0.2.10"
		}
		return authoritativeAnswer(q, ip), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e := waitStaticEdgeDNSAnswers(ctx, []string{"192.0.2.53:53", "192.0.2.54:53"}, []string{"example.test"}, "192.0.2.20", time.Second, time.Millisecond, exchange)
	if e != nil {
		t.Fatal(e)
	}
	for _, address := range []string{"192.0.2.53:53", "192.0.2.54:53"} {
		for _, network := range []string{"udp", "tcp"} {
			if calls[address+"/"+network] != 2 {
				t.Fatal(calls)
			}
		}
	}
}
func TestStaticEdgeAuthoritativeTimeoutDoesNotVerifyAPIWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	exchange := func(_ context.Context, _, _ string, q *dns.Msg, _ time.Duration) (*dns.Msg, error) {
		return authoritativeAnswer(q, "192.0.2.10"), nil
	}
	err := waitStaticEdgeDNSAnswers(ctx, []string{"192.0.2.53:53"}, []string{"example.test"}, "192.0.2.20", time.Second, time.Millisecond, exchange)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale answer did not preserve unverified state: %v", err)
	}
}
func TestStaticEdgeAuthoritativeRealUDPAndTCP(t *testing.T) {
	// Real isolated wire round trips exercise the DNS client's response handling.
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		packet.Close()
		t.Fatal(err)
	}
	handler := dns.HandlerFunc(func(w dns.ResponseWriter, q *dns.Msg) { _ = w.WriteMsg(authoritativeAnswer(q, "192.0.2.20")) })
	udp := &dns.Server{PacketConn: packet, Handler: handler}
	tcp := &dns.Server{Listener: listener, Handler: handler}
	go udp.ActivateAndServe()
	go tcp.ActivateAndServe()
	t.Cleanup(func() { _ = udp.Shutdown(); _ = tcp.Shutdown() })
	for network, address := range map[string]string{"udp": packet.LocalAddr().String(), "tcp": listener.Addr().String()} {
		q := new(dns.Msg)
		q.SetQuestion("example.test.", dns.TypeA)
		q.RecursionDesired = false
		a, err := exchangeStaticEdgeDNS(context.Background(), address, network, q, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err = verifyStaticEdgeDNSAnswer(q, a, "192.0.2.20"); err != nil {
			t.Fatal(err)
		}
	}
}
