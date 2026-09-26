package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	sc "fugue/internal/staticedgecontract"
)

func TestStaticEdgeRegistryObservationRequiresReadyManager(t *testing.T) {
	cfg, credentials := tlsFixture(t)
	ready := true
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, err := credentials.Authorize(r); err != nil {
			http.Error(w, "unauthorized", http.StatusForbidden)
			return
		}
		var request sc.Request
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		_ = json.NewEncoder(w).Encode(sc.Response{
			Schema: sc.RPCSchema, RequestID: request.RequestID, EdgeID: cfg.EdgeID, OK: true, Status: 200,
			Result: &sc.Observed{EdgeID: cfg.EdgeID, Ready: ready, RuntimeMatches: ready, RuntimeDigest: "sha256:" + strings.Repeat("a", 64)},
		})
	}))
	server.TLS = credentials.Config()
	server.StartTLS()
	defer server.Close()
	cfg.ManagerURL = server.URL
	t.Setenv("FUGUE_STATIC_EDGE_CONTEXT_FILE", filepath.Join(t.TempDir(), "contexts.json"))
	if err := saveStaticEdgeContexts(staticEdgeContextFile{SchemaVersion: 1, Contexts: []staticEdgeContext{cfg}}); err != nil {
		t.Fatal(err)
	}
	observed, digest, err := observeStaticEdgeForRegistry(context.Background(), cfg.Name)
	if err != nil || observed.EdgeID != cfg.EdgeID || observed.Transport != "mtls" || observed.ManagerURL != server.URL || !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("direct possession observation failed: observed=%+v digest=%s err=%v", observed, digest, err)
	}
	ready = false
	if _, _, err := observeStaticEdgeForRegistry(context.Background(), cfg.Name); err == nil {
		t.Fatal("unready manager accepted as possession observation")
	}
}
