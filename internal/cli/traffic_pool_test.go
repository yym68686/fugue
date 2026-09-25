package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestTrafficPoolMTLSIsIndependentOfFugueAPI(t *testing.T) {
	edge, credentials := tlsFixture(t)
	apiCalls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		apiCalls++
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer api.Close()
	t.Setenv("FUGUE_API_URL", api.URL)
	t.Setenv("FUGUE_API_KEY", "unusable")
	t.Setenv("FUGUE_CONTEXT_FILE", filepath.Join(t.TempDir(), "missing.json"))

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, grant, err := credentials.Authorize(r); err != nil || grant != "operator" {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		if r.Header.Get("Authorization") != "" || r.URL.Path != "/v1/entry-failover/status" {
			t.Error("request escaped the independent management contract")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"pool_id": "sample", "policy_generation": 2})
	}))
	server.TLS = credentials.Config()
	server.StartTLS()
	defer server.Close()

	contextFile := filepath.Join(t.TempDir(), "contexts.json")
	t.Setenv("FUGUE_TRAFFIC_POOL_CONTEXT_FILE", contextFile)
	cfg := trafficPoolContext{Name: "sample", Endpoint: server.URL, ServerName: edge.ServerName,
		CA: edge.CAFile, ClientCert: edge.ClientCert, ClientKey: edge.ClientKey}
	if err := (trafficPoolContexts{SchemaVersion: 1, Active: cfg.Name, Contexts: []trafficPoolContext{cfg}}).save(); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if err := runWithStreams([]string{"--json", "traffic-pool", "status"}, &out, &stderr); err != nil {
		t.Fatal(err, stderr.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(out.Bytes(), &payload); err != nil || payload["pool_id"] != "sample" {
		t.Fatalf("status output was not a JSON object: %v %s", err, out.String())
	}
	if apiCalls != 0 {
		t.Fatal("CLI contacted Fugue API")
	}
	cfg.ServerName = "wrong.example.test"
	var ignored json.RawMessage
	if err := trafficPoolCall(context.Background(), cfg, http.MethodGet, "/v1/entry-failover/status", nil, &ignored); err == nil {
		t.Fatal("incorrect executor server identity accepted")
	}
	if err := credentials.Select("b"); err != nil {
		t.Fatal(err)
	}
	cfg.ServerName = edge.ServerName
	if err := trafficPoolCall(context.Background(), cfg, http.MethodGet, "/v1/entry-failover/status", nil, &ignored); err == nil {
		t.Fatal("revoked client identity accepted")
	}
}
