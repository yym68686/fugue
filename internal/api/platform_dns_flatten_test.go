package api

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	dns "github.com/miekg/dns"
)

func TestFlattenCaptureBindsFreshQueryWithoutReusingLegacyCache(t *testing.T) {
	now := time.Now().UTC()
	result := platformIntentProjectionResponse{CapturedAt: now, Intent: platformconfig.PlatformIntent{Generation: "intent", DNS: []platformconfig.DNSIntent{{Hostname: "alias.example", TenantID: "tenant", Type: "ALIAS", Values: []string{"target.example"}, TTL: 60, Flatten: &platformconfig.DNSFlattenIntent{Mode: "always", Target: "target.example", IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "target", FallbackPolicy: "fail_closed"}}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy", MaxStaleSeconds: 300}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}, Issues: []platformProjectionIssue{{Code: "dns_flatten_observation_not_captured", Hostname: "alias.example"}}}
	intent := platformconfig.NormalizePlatformIntent(result.Intent)
	captureDNSFlattenFacts(context.Background(), &result, func(ctx context.Context, target string, record model.DNSRecord) hostedDNSFlattenResult {
		if target != "target.example" || record.FlattenTTLPolicy != "target" || record.FlattenFallbackPolicy != "fail_closed" {
			t.Fatal("resolver received wrong configuration")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("unbounded resolver")
		}
		return hostedDNSFlattenResult{A: []string{"93.184.216.34"}, TTL: 120}
	})
	if len(result.RuntimeSnapshot.DNSFlatten) != 1 || len(result.Issues) != 0 {
		t.Fatalf("capture failed: %+v", result)
	}
	fact := result.RuntimeSnapshot.DNSFlatten[0]
	digest, _ := platformconfig.DNSFlattenInputDigest(result.Intent.DNS[0])
	if fact.InputDigest != digest || fact.TargetTTL != 120 || fact.ObservedAt.Before(now) || !fact.ObservedAt.Equal(fact.CheckedAt) {
		t.Fatal("missing query provenance")
	}
	if !reflect.DeepEqual(intent, platformconfig.NormalizePlatformIntent(result.Intent)) {
		t.Fatal("query changed intent")
	}
	result.RuntimeSnapshot.DNSFlatten = nil
	captureDNSFlattenFacts(context.Background(), &result, func(context.Context, string, model.DNSRecord) hostedDNSFlattenResult {
		return hostedDNSFlattenResult{Err: errors.New("resolution failed")}
	})
	fact = result.RuntimeSnapshot.DNSFlatten[0]
	if !fact.ObservedAt.IsZero() || len(fact.A) != 0 || fact.Status != "error" || len(result.Issues) == 0 {
		t.Fatal("failed query fabricated positive evidence")
	}
}

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
	resolver := hostedDNSFlattenResolver{servers: []string{conn.LocalAddr().String()}, client: &dns.Client{Timeout: time.Second}}
	got := resolver.resolve(context.Background(), "alias.example", model.DNSRecord{})
	if got.Err != nil || got.TTL != 7 || len(got.A) != 1 {
		t.Fatalf("CNAME chain TTL lost: %+v", got)
	}
}

func TestSuccessfulFlattenRefreshAdvancesEvidenceTime(t *testing.T) {
	oldTime := time.Now().UTC().Add(-time.Hour)
	record := model.DNSRecord{Status: model.DNSRecordStatusActive, FlattenStatus: model.DNSRecordFlattenStatusResolved, LastResolvedAt: &oldTime, FlattenedA: []string{"93.184.216.34"}}
	now := oldTime.Add(time.Minute)
	updated := applyHostedDNSFlattenResult(record, hostedDNSFlattenResult{A: record.FlattenedA, TTL: 60}, now)
	if hostedDNSFlattenRecordEqual(record, updated) {
		t.Fatal("unchanged addresses prevented persistence of fresh successful observation")
	}
	failed := applyHostedDNSFlattenResult(updated, hostedDNSFlattenResult{Err: errors.New("temporary DNS failure")}, now.Add(time.Minute))
	if !failed.LastResolvedAt.Equal(now) {
		t.Fatal("failed query renewed evidence")
	}
}
