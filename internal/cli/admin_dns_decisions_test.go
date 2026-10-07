package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/dnsserver"
	"github.com/miekg/dns"
)

func cliDNSDecisionFixture(t *testing.T) dnsserver.DNSDecisionReceipt {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	query := new(dns.Msg)
	query.SetQuestion("outside.test.", dns.TypeA)
	wire, err := query.Pack()
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(map[string]any{"version": dnsserver.DNSDecisionSchema, "query": wire, "stages": []any{map[string]any{
		"query_at": now, "applied_at": now, "max_stale_seconds": 3600, "authorities": []any{}, "entries": []any{}, "selections": []any{},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	receipt := dnsserver.DNSDecisionReceipt{Schema: dnsserver.DNSDecisionSchema, DecisionID: "test-1", ProcessID: "test", NodeID: "dns-test", ObservedAt: now,
		Hostname: "outside.test", QType: dns.TypeA, QueryID: query.Id, RCode: dns.RcodeRefused,
		AnswerPublication: &dnsserver.DNSDecisionPublication{},
		RRSet:             []string{}, Authority: []string{}, Additional: []string{}, Records: []dnsserver.DNSDecisionRecord{}, ReplayInput: input}
	raw, _ := json.Marshal(receipt)
	receipt.EvidenceDigest = fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
	return receipt
}

func TestAdminDNSDecisionsExplainOnlyReadsReceipts(t *testing.T) {
	t.Setenv("FUGUE_CONFIG_DIR", t.TempDir())
	t.Setenv("FUGUE_CLI_UPDATE_CHECK", "off")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Method != http.MethodGet || request.URL.Path != "/v1/admin/platform-state/dns-decisions/dns-test" {
			t.Errorf("unexpected request %s %s", request.Method, request.URL)
		}
		if request.URL.Query().Get("hostname") != "app.example.test" || request.URL.Query().Get("decision_id") != "test-1" || request.URL.Query().Get("limit") != "1" {
			t.Error("receipt filters changed")
		}
		fmt.Fprint(writer, `{"snapshot":{"receipts":[]}}`)
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", server.URL, "--token", "test", "admin", "dns", "decisions", "explain", "dns-test", "--hostname", "app.example.test", "--decision-id", "test-1", "--limit", "1"}, &stdout, &stderr)
	if err != nil || calls.Load() != 1 || !bytes.Contains(stdout.Bytes(), []byte(`"receipts"`)) {
		t.Fatalf("err=%v calls=%d output=%s", err, calls.Load(), stdout.String())
	}
}

func TestAdminDNSDecisionsReplayIsOffline(t *testing.T) {
	t.Setenv("FUGUE_CONFIG_DIR", t.TempDir())
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		writer.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	receipt := cliDNSDecisionFixture(t)
	for _, value := range []any{receipt, map[string]any{"snapshot": map[string]any{"receipts": []dnsserver.DNSDecisionReceipt{receipt}}}} {
		raw, _ := json.Marshal(value)
		filename := filepath.Join(t.TempDir(), "receipt.json")
		if err := os.WriteFile(filename, raw, 0600); err != nil {
			t.Fatal(err)
		}
		var stdout, stderr bytes.Buffer
		err := runWithStreams([]string{"--base-url", server.URL, "--token", "invalid", "admin", "dns", "decisions", "replay", filename}, &stdout, &stderr)
		if err != nil || calls.Load() != 0 || !bytes.Contains(stdout.Bytes(), []byte(`"matched": true`)) {
			t.Fatalf("err=%v calls=%d output=%s", err, calls.Load(), stdout.String())
		}
	}
}
