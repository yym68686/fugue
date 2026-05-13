package cli

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"

	miekgdns "github.com/miekg/dns"
)

func TestAdminDNSACMEPresentCommandWritesChallenge(t *testing.T) {
	t.Parallel()

	var gotRequest upsertDNSACMEChallengeClientRequest
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/dns/acme-challenges" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		writeJSONResponse(t, w, dnsACMEChallengeResponse{
			Challenge: model.DNSACMEChallenge{
				ID:        "dnsacme_123",
				Zone:      "fugue.pro",
				Name:      "_acme-challenge.fugue.pro",
				Value:     "token-value",
				TTL:       30,
				ExpiresAt: now.Add(2 * time.Hour),
				CreatedAt: now,
				UpdatedAt: now,
			},
		})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"admin", "dns", "acme", "present",
		"_acme-challenge.fugue.pro",
		"token-value",
		"--zone", "fugue.pro",
		"--ttl", "30",
		"--expires-in", "2h",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run acme present: %v", err)
	}
	if gotRequest.Zone != "fugue.pro" || gotRequest.Name != "_acme-challenge.fugue.pro" || gotRequest.Value != "token-value" || gotRequest.TTL != 30 || gotRequest.ExpiresInSeconds != 7200 {
		t.Fatalf("unexpected ACME request: %+v", gotRequest)
	}
	out := stdout.String()
	for _, want := range []string{"id=dnsacme_123", "zone=fugue.pro", "name=_acme-challenge.fugue.pro", "ttl=30"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestAdminDNSACMECleanupCommandCanDeleteByNameAndValue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	deletedID := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/dns/acme-challenges":
			if r.URL.Query().Get("zone") != "fugue.pro" || r.URL.Query().Get("include_expired") != "true" {
				t.Fatalf("unexpected ACME list query: %s", r.URL.RawQuery)
			}
			writeJSONResponse(t, w, dnsACMEChallengeListResponse{
				Challenges: []model.DNSACMEChallenge{{
					ID:        "dnsacme_123",
					Zone:      "fugue.pro",
					Name:      "_acme-challenge.fugue.pro",
					Value:     "token-value",
					TTL:       60,
					ExpiresAt: now.Add(time.Hour),
					CreatedAt: now,
					UpdatedAt: now,
				}},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/dns/acme-challenges/dnsacme_123":
			deletedID = "dnsacme_123"
			writeJSONResponse(t, w, deleteDNSACMEChallengeResponse{
				Deleted: true,
				Challenge: model.DNSACMEChallenge{
					ID:        "dnsacme_123",
					Zone:      "fugue.pro",
					Name:      "_acme-challenge.fugue.pro",
					Value:     "token-value",
					TTL:       60,
					ExpiresAt: now.Add(time.Hour),
					CreatedAt: now,
					UpdatedAt: now,
				},
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"admin", "dns", "acme", "cleanup",
		"--zone", "fugue.pro",
		"--name", "_acme-challenge.fugue.pro",
		"--value", "token-value",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run acme cleanup: %v", err)
	}
	if deletedID != "dnsacme_123" {
		t.Fatalf("expected cleanup to delete dnsacme_123, got %q", deletedID)
	}
	if !strings.Contains(stdout.String(), "dnsacme_123") {
		t.Fatalf("expected deleted challenge table, got %q", stdout.String())
	}
}

func TestQueryDNSACMETXTUsesTCP(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp dns: %v", err)
	}
	server := &miekgdns.Server{
		Listener: listener,
		Net:      "tcp",
		Handler: miekgdns.HandlerFunc(func(w miekgdns.ResponseWriter, r *miekgdns.Msg) {
			response := new(miekgdns.Msg)
			response.SetReply(r)
			response.Authoritative = true
			response.Answer = append(response.Answer, &miekgdns.TXT{
				Hdr: miekgdns.RR_Header{
					Name:   miekgdns.Fqdn("_acme-challenge.fugue.pro"),
					Rrtype: miekgdns.TypeTXT,
					Class:  miekgdns.ClassINET,
					Ttl:    60,
				},
				Txt: []string{"token-value"},
			})
			_ = w.WriteMsg(response)
		}),
	}
	go func() {
		_ = server.ActivateAndServe()
	}()
	t.Cleanup(func() {
		_ = server.Shutdown()
	})

	visible, err := queryDNSACMETXT(listener.Addr().String(), "_acme-challenge.fugue.pro", "token-value")
	if err != nil {
		t.Fatalf("query ACME TXT: %v", err)
	}
	if !visible {
		t.Fatal("expected ACME TXT to be visible over TCP")
	}
}
