package hosteddns

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"fugue/internal/model"

	dns "github.com/miekg/dns"
)

func TestFlattenResolverRetainsShortestCNAMEChainTTL(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &dns.Server{PacketConn: conn, Handler: dns.HandlerFunc(func(w dns.ResponseWriter, req *dns.Msg) {
		msg := new(dns.Msg)
		msg.SetReply(req)
		q := req.Question[0]
		if q.Name == "alias.example." {
			msg.Answer = []dns.RR{&dns.CNAME{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: 7}, Target: "target.example."}}
		} else if q.Name == "target.example." && q.Qtype == dns.TypeA {
			msg.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 120}, A: net.ParseIP("93.184.216.34")}}
		}
		_ = w.WriteMsg(msg)
	})}
	ready := make(chan struct{})
	srv.NotifyStartedFunc = func() { close(ready) }
	go srv.ActivateAndServe()
	<-ready
	t.Cleanup(func() { _ = srv.Shutdown() })
	resolver := Resolver{servers: []string{conn.LocalAddr().String()}, client: &dns.Client{Timeout: time.Second}}
	got := resolver.Resolve(context.Background(), "alias.example", model.DNSRecord{})
	if got.Err != nil || got.TTL != 7 || len(got.A) != 1 {
		t.Fatalf("CNAME chain TTL lost: %+v", got)
	}
}

func TestSuccessfulFlattenRefreshAdvancesEvidenceTime(t *testing.T) {
	oldTime := time.Now().UTC().Add(-time.Hour)
	record := model.DNSRecord{Status: model.DNSRecordStatusActive, FlattenStatus: model.DNSRecordFlattenStatusResolved, LastResolvedAt: &oldTime, FlattenedA: []string{"93.184.216.34"}}
	now := oldTime.Add(time.Minute)
	updated := applyResult(record, Result{A: record.FlattenedA, TTL: 60}, now)
	if sameObservation(record, updated) {
		t.Fatal("unchanged addresses prevented persistence of fresh successful observation")
	}
	failed := applyResult(updated, Result{Err: errors.New("temporary DNS failure")}, now.Add(time.Minute))
	if !failed.LastResolvedAt.Equal(now) {
		t.Fatal("failed query renewed evidence")
	}
}
