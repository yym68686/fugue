package participation

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestParticipationDNSObserverTLSAndStrictRead(t *testing.T) {
	const token = "fugue_pc_v1.fixture"
	const good = `{"node":"node-a","group":"cell-a","public_ipv4":"192.0.2.4","evaluated_at":"2026-04-13T12:00:00Z","heartbeat_fresh":false,"dns_state":"allowed","dns_reason":"","dns_serving_evidence":"not_evaluated"}`
	for _, tc := range []struct {
		name, body string
		redirect   bool
		wantOK     bool
	}{
		{"explicit false", good, false, true},
		{"missing heartbeat", strings.ReplaceAll(good, `"heartbeat_fresh":false,`, ""), false, false},
		{"missing reason", strings.ReplaceAll(good, `"dns_reason":"",`, ""), false, false},
		{"duplicate field", strings.ReplaceAll(good, `"heartbeat_fresh":false`, `"heartbeat_fresh":false,"heartbeat_fresh":true`), false, false},
		{"unknown field", strings.ReplaceAll(good, `"node":`, `"unrecognized":null,"node":`), false, false},
		{"oversize", good + strings.Repeat(" ", maxDNSObservationBytes), false, false},
		{"redirect", "", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/v1/edge/nodes/node-a/participation-observation" ||
					r.URL.RawQuery != "" || r.Header.Get("Authorization") != "Bearer "+token {
					t.Errorf("unexpected DNS observation request: %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected", http.StatusBadRequest)
					return
				}
				if tc.redirect {
					http.Redirect(w, r, "/other", http.StatusFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			o, err := NewDNSObserver(server.URL, token)
			if err != nil {
				t.Fatal(err)
			}
			transport := o.client.Transport.(*http.Transport)
			if transport.Proxy != nil {
				t.Fatal("DNS observer inherited a proxy")
			}
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			transport.TLSClientConfig = &tls.Config{RootCAs: roots}
			defer transport.CloseIdleConnections()
			result, err := o.Observe(context.Background(), "node-a")
			if !tc.wantOK {
				if err == nil || result.Node != "" {
					t.Fatalf("accepted invalid response: %+v, %v", result, err)
				}
			} else if err != nil || result.HeartbeatFresh || result.Node != "node-a" || result.EvaluatedAt.IsZero() {
				t.Fatalf("did not retain explicit false and observation fields: %+v, %v", result, err)
			}
		})
	}
	for _, origin := range []string{"http://example.test", "https://user@example.test", "https://example.test/path", "https://example.test#"} {
		if _, err := NewDNSObserver(origin, token); err == nil {
			t.Errorf("accepted invalid origin %q", origin)
		}
	}
	if _, err := NewDNSObserver("https://example.test", "not-an-observer-token"); err == nil {
		t.Fatal("accepted an unrelated credential")
	}
}

func TestDNSObservationBoundToMemberAndCurrentEvidence(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	fresh := now.Add(time.Minute)
	base := model.EdgeParticipationObservation{Node: "node-a", Group: "cell-a", PublicIPv4: "192.0.2.4",
		EvaluatedAt: now, HeartbeatFresh: true, FreshUntil: &fresh, DNSState: "suspended", DNSReason: "network_unavailable",
		DNSServingEvidence: "excluded_from_verified_full"}
	for _, tc := range []struct {
		name   string
		change func(*model.EdgeParticipationObservation)
	}{
		{"physical member changed", func(o *model.EdgeParticipationObservation) { o.Node = "node-b" }},
		{"group changed", func(o *model.EdgeParticipationObservation) { o.Group = "cell-b" }},
		{"serving address changed", func(o *model.EdgeParticipationObservation) { o.PublicIPv4 = "192.0.2.5" }},
		{"stale", func(o *model.EdgeParticipationObservation) { o.EvaluatedAt = now.Add(-16 * time.Second) }},
		{"future", func(o *model.EdgeParticipationObservation) { o.EvaluatedAt = now.Add(6 * time.Second) }},
		{"unverified suspension", func(o *model.EdgeParticipationObservation) { o.DNSServingEvidence = "not_evaluated" }},
		{"no admission policy", func(o *model.EdgeParticipationObservation) { o.DNSState = "not_configured" }},
		{"fresh heartbeat without proof", func(o *model.EdgeParticipationObservation) { o.FreshUntil = nil }},
		{"expired positive proof", func(o *model.EdgeParticipationObservation) { o.FreshUntil = &now }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := base
			tc.change(&got)
			if _, err := bindDNSObservation(got, "node-a", "cell-a", "192.0.2.4", now); err == nil {
				t.Fatalf("accepted %s", tc.name)
			}
		})
	}
	got, err := bindDNSObservation(base, "node-a", "cell-a", "192.0.2.4", now)
	if err != nil || !got.HeartbeatFresh || len(got.ExternalIPs) != 1 || got.ExternalIPs[0] != base.PublicIPv4 {
		t.Fatalf("did not bind exact verified serving observation: %+v, %v", got, err)
	}
	next := base
	next.EvaluatedAt = now.Add(time.Second)
	if !sameDNSObservation(base, next) {
		t.Fatal("two reads should be allowed to differ in evaluation time")
	}
	next.HeartbeatFresh = false
	if sameDNSObservation(base, next) {
		t.Fatal("heartbeat drift was undetected")
	}
}
