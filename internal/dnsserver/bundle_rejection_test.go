package dnsserver

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/model"

	miekgdns "github.com/miekg/dns"
)

func TestRejectedDNSCandidatePreservesServingLKG(t *testing.T) {
	t.Parallel()
	const key = "synthetic-signing-key-012345678901"
	for _, failure := range []string{"schema", "signature", "expired", "revoked-key"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			now := time.Now().UTC()
			makeBundle := func(version, address string, generated time.Time) model.EdgeDNSBundle {
				return bundleauth.SignEdgeDNSBundle(model.EdgeDNSBundle{Version: version, Generation: version, GeneratedAt: generated, Zone: "example.test", Records: []model.EdgeDNSRecord{{Name: "app.example.test", Type: model.EdgeDNSRecordTypeA, Values: []string{address}, Status: model.EdgeRouteStatusActive}}}, key, "active-key", time.Hour)
			}
			incoming := makeBundle("candidate", "192.0.2.3", now)
			switch failure {
			case "schema":
				incoming.SchemaVersion = "999.0"
			case "signature":
				incoming.Version = "tampered"
			case "expired":
				incoming = makeBundle("candidate", "192.0.2.3", now.Add(-2*time.Hour))
			case "revoked-key":
				incoming = bundleauth.SignEdgeDNSBundle(incoming, "retired-key-material-0123456789012", "retired-key", time.Hour)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(incoming) }))
			defer server.Close()
			cfg := config.DNSConfig{APIURL: server.URL, EdgeToken: "synthetic-token", Zone: "example.test", AnswerIPs: []string{"192.0.2.1"}, TTL: 60, CachePath: filepath.Join(t.TempDir(), "cache.json"), CacheArchiveLimit: 3, MaxStale: time.Hour, BundleSigningKey: key, BundleSigningKeyID: "active-key", BundleRevokedKeyIDs: []string{"retired-key"}}
			cfg.ListenAddr, cfg.UDPAddr = "127.0.0.1:0", "127.0.0.1:0"
			newService := func() *Service { return NewService(cfg, log.New(ioDiscard{}, "", 0)) }
			s := newService()
			for index, version := range []string{"previous", "current"} {
				address := []string{"192.0.2.1", "192.0.2.2"}[index]
				if err := s.writeCache(cacheFile{Version: cacheFileVersion, CachedAt: now, ETag: version, Bundle: makeBundle(version, address, now)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.LoadCache(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(cfg.CachePath)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.SyncOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "verify dns bundle") {
				t.Fatalf("want candidate verification rejection, got %v", err)
			}
			answer := dnsQuery(t, s, "app.example.test.", miekgdns.TypeA)
			if s.Status().ServingGeneration != "current" || s.Status().LastError == "" || len(answer.Answer) != 1 || answer.Answer[0].(*miekgdns.A).A.String() != "192.0.2.2" {
				t.Fatalf("rejection changed serving LKG: status=%+v answer=%+v", s.Status(), answer)
			}
			after, err := os.ReadFile(cfg.CachePath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("rejection changed durable LKG: %v", err)
			}
			restarted := newService()
			if err := restarted.LoadCache(); err != nil || restarted.Status().ServingGeneration != "current" {
				t.Fatalf("restart lost current LKG: %v %+v", err, restarted.Status())
			}
			if err := restarted.LoadPreviousCache(); err != nil || restarted.Status().ServingGeneration != "previous" {
				t.Fatalf("explicit recovery unavailable: %v %+v", err, restarted.Status())
			}
			empty := newService()
			if err := empty.SyncOnce(context.Background()); err == nil || empty.hasBundle() {
				t.Fatalf("candidate rejection implicitly loaded history: %v", err)
			}
		})
	}
}
