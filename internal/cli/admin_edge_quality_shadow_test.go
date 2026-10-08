package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/edgequality"
)

func TestPhysicalQualityReplayIsOffline(t *testing.T) {
	t.Setenv("FUGUE_CONFIG_DIR", t.TempDir())
	t.Setenv("FUGUE_CLI_UPDATE_CHECK", "on")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { calls.Add(1); writer.WriteHeader(401) }))
	defer server.Close()
	receipt, err := edgequality.Capture(edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: time.Now().UTC(), Hostname: "app.example.test", TrafficClass: "streaming", Scope: "global", Policy: edgequality.DefaultShadowPolicy(), Candidates: []edgequality.Candidate{}, Observations: []edgequality.Observation{}, Blockers: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(receipt)
	filename := filepath.Join(t.TempDir(), "receipt.json")
	if err := os.WriteFile(filename, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	err = runWithStreams([]string{"--base-url", server.URL, "--token", "invalid", "admin", "edge", "quality-shadow", "replay", filename}, &stdout, &stderr)
	if err != nil || calls.Load() != 0 || !bytes.Contains(stdout.Bytes(), []byte(`"matched": true`)) {
		t.Fatal(err, calls.Load(), stdout.String())
	}
}

func TestPhysicalQualityCaptureOutputPreservesEmbeddedReceiptIntegrity(t *testing.T) {
	t.Setenv("FUGUE_CONFIG_DIR", t.TempDir())
	t.Setenv("FUGUE_CLI_UPDATE_CHECK", "off")
	actual := cliDNSDecisionFixture(t)
	rawActual, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := edgequality.Capture(edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: actual.ObservedAt, Hostname: "app.example.test", TrafficClass: "streaming", Scope: "global", Policy: edgequality.DefaultShadowPolicy(), ActualDNSReceipt: rawActual})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Query().Get("dns_node_id") != "dns-test" {
			t.Error("capture did not request exact DNS backend")
		}
		if err := json.NewEncoder(writer).Encode(receipt); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	err = runWithStreams([]string{"--base-url", server.URL, "--token", "test", "admin", "edge", "quality-shadow", "capture", "app.example.test", "--traffic-class", "streaming", "--dns-node-id", "dns-test", "--json"}, &stdout, &stderr)
	if err != nil {
		t.Fatal(err, stderr.String())
	}
	var exported edgequality.Receipt
	if err := json.Unmarshal(stdout.Bytes(), &exported); err != nil {
		t.Fatal(err)
	}
	if _, err := edgequality.Replay(exported); err != nil {
		t.Fatal("CLI reordered embedded JSON and broke its integrity", err)
	}
}
