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
