package dnsserver

import (
	"bytes"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/localwal"
	"fugue/internal/model"
	dns "github.com/miekg/dns"
)

func TestDNSValueExpirationCapsEachResponseAndPreservesSignedInput(t *testing.T) {
	now := time.Now().UTC()
	r := model.EdgeDNSRecord{Name: "_acme-challenge.example.test", Type: "TXT", Values: []string{"first", "second", "permanent"}, TTL: 60, ValueExpirations: map[string]time.Time{"first": now.Add(10 * time.Second), "second": now.Add(30 * time.Second)}}
	for _, test := range []struct {
		elapsed time.Duration
		count   int
		ttl     uint32
	}{{0, 3, 10}, {10 * time.Second, 2, 20}, {30 * time.Second, 1, 60}} {
		got := rrForEdgeDNSRecordAt(r, r.Name, now.Add(test.elapsed))
		if len(got) != test.count {
			t.Fatalf("wrong answer count: %d", len(got))
		}
		for _, rr := range got {
			if rr.Header().Ttl != test.ttl {
				t.Fatalf("renewed expiry: %+v", rr)
			}
		}
	}
	if len(r.Values) != 3 || r.TTL != 60 || len(r.ValueExpirations) != 2 {
		t.Fatal("expiration rewrote signed record")
	}
}

func TestDNSValueExpirationSurvivesSignedLKGReloadAndEmitsFact(t *testing.T) {
	now := time.Now().UTC()
	const key = "synthetic-dns-expiry-signing-key"
	cfg := config.DNSConfig{Zone: "example.test", TTL: 60, DNSNodeID: "test-dns", CachePath: filepath.Join(t.TempDir(), "cache.json"), AutonomyWALPath: filepath.Join(t.TempDir(), "facts.jsonl"), BundleSigningKey: key, BundleSigningKeyID: "key", MaxStale: time.Hour}
	s := NewService(cfg, log.New(io.Discard, "", 0))
	r := model.EdgeDNSRecord{Name: "_acme-challenge.example.test", Type: "TXT", Values: []string{"expired-token"}, TTL: 60, Status: "active", RecordGeneration: "record-1", ValueExpirations: map[string]time.Time{"expired-token": now.Add(-time.Second)}}
	b := bundleauth.SignEdgeDNSBundle(model.EdgeDNSBundle{Version: "signed-expiry", Generation: "signed-expiry", Zone: cfg.Zone, GeneratedAt: now.Add(-2 * time.Minute), Records: []model.EdgeDNSRecord{r}}, key, "key", time.Minute)
	if err := s.writeCache(cacheFile{Version: cacheFileVersion, Bundle: b, CachedAt: now.Add(-2 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(cfg.CachePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.LoadCache(); err != nil {
		t.Fatal(err)
	}
	answer := dnsQuery(t, s, r.Name, dns.TypeTXT)
	if len(answer.Answer) != 0 || answer.Rcode != dns.RcodeSuccess {
		t.Fatalf("expired token served or authority lost: %+v", answer)
	}
	_ = dnsQuery(t, s, r.Name, dns.TypeTXT)
	after, err := os.ReadFile(cfg.CachePath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("query mutated LKG", err)
	}
	facts, err := localwal.ReadAll(cfg.AutonomyWALPath)
	if err != nil || len(facts) != 1 || facts[0].Action != "dns_value_expired" || facts[0].Evidence["expired_value_count"] != "1" {
		t.Fatalf("missing expiry fact: %+v %v", facts, err)
	}
	raw, err := os.ReadFile(cfg.AutonomyWALPath)
	if err != nil || bytes.Contains(raw, []byte("expired-token")) {
		t.Fatal("expiry fact leaked token", err)
	}
	// A former consumer that ignores the expiry map cannot validate this signature.
	b.Records = append([]model.EdgeDNSRecord(nil), b.Records...)
	b.Records[0].ValueExpirations = nil
	if err := s.verifyCachedBundle(b, now); err == nil {
		t.Fatal("signature did not cover value expiration")
	}
}

func TestDNSValueExpirationRejectsMalformedSignedBundle(t *testing.T) {
	now := time.Now().UTC()
	const key = "synthetic-dns-expiry-signing-key"
	s := NewService(config.DNSConfig{BundleSigningKey: key, BundleSigningKeyID: "key"}, log.New(io.Discard, "", 0))
	for _, record := range []model.EdgeDNSRecord{
		{Name: "app.example", Type: "A", Values: []string{"192.0.2.1"}, TTL: 60, ValueExpirations: map[string]time.Time{"192.0.2.1": now.Add(time.Minute)}},
		{Name: "app.example", Type: "TXT", Values: []string{"one"}, TTL: 60, ValueExpirations: map[string]time.Time{"unknown": now.Add(time.Minute)}},
		{Name: "app.example", Type: "TXT", Values: []string{"one"}, TTL: 0, ValueExpirations: map[string]time.Time{"one": now.Add(time.Minute)}},
	} {
		b := bundleauth.SignEdgeDNSBundle(model.EdgeDNSBundle{Version: "invalid", GeneratedAt: now, Records: []model.EdgeDNSRecord{record}}, key, "key", time.Hour)
		if err := s.verifyBundle(b, now); err == nil {
			t.Fatal("malformed signed expiry accepted")
		}
	}
}
