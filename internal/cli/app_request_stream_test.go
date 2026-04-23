package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestRunAppRequestStreamClassifiesPublicHeadersOnlyStall(t *testing.T) {
	t.Parallel()

	var (
		publicMu      sync.Mutex
		publicAccepts []string
	)
	publicServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		publicMu.Lock()
		publicAccepts = append(publicAccepts, r.Header.Get("Accept"))
		publicMu.Unlock()

		if r.URL.Path != "/events" {
			t.Fatalf("unexpected public request path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("CF-Cache-Status", "DYNAMIC")
		flusher, _ := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer publicServer.Close()

	var gotInternalBody struct {
		Method    string   `json:"method"`
		Path      string   `json:"path"`
		Accepts   []string `json:"accepts"`
		MaxChunks int      `json:"max_chunks"`
	}
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","route":{"public_url":"` + publicServer.URL + `"},"spec":{"runtime_id":"runtime_shared","ports":[3000],"replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-21T00:00:00Z","updated_at":"2026-04-21T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","route":{"public_url":"` + publicServer.URL + `"},"internal_service":{"name":"demo","namespace":"tenant-123","host":"demo.tenant-123.svc.cluster.local","port":3000},"spec":{"runtime_id":"runtime_shared","ports":[3000],"replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_shared","current_replicas":1},"created_at":"2026-04-21T00:00:00Z","updated_at":"2026-04-21T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/request-stream":
			if err := json.NewDecoder(r.Body).Decode(&gotInternalBody); err != nil {
				t.Fatalf("decode internal stream request body: %v", err)
			}
			_, _ = w.Write([]byte(`{
				"probes":[
					{
						"target":"internal_service",
						"accept":"*/*",
						"url":"http://demo.tenant-123.svc.cluster.local:3000/events",
						"status":"200 OK",
						"status_code":200,
						"headers":{"Content-Type":["text/event-stream"]},
						"body_bytes":24,
						"chunk_count":2,
						"timing":{
							"headers_observed":true,
							"body_byte_observed":true,
							"sse_event_observed":true,
							"time_to_headers_ms":5,
							"time_to_first_body_byte_ms":7,
							"time_to_first_sse_event_ms":9,
							"total_time_ms":10
						},
						"first_chunks":[
							{"index":0,"kind":"sse_comment","encoding":"utf-8","payload":": keepalive\n\n","size_bytes":12},
							{"index":1,"kind":"sse_event","encoding":"utf-8","payload":"data: ready\n\n","size_bytes":12}
						]
					},
					{
						"target":"internal_service",
						"accept":"text/event-stream",
						"url":"http://demo.tenant-123.svc.cluster.local:3000/events",
						"status":"200 OK",
						"status_code":200,
						"headers":{"Content-Type":["text/event-stream"]},
						"body_bytes":24,
						"chunk_count":2,
						"timing":{
							"headers_observed":true,
							"body_byte_observed":true,
							"sse_event_observed":true,
							"time_to_headers_ms":5,
							"time_to_first_body_byte_ms":7,
							"time_to_first_sse_event_ms":9,
							"total_time_ms":10
						},
						"first_chunks":[
							{"index":0,"kind":"sse_comment","encoding":"utf-8","payload":": keepalive\n\n","size_bytes":12},
							{"index":1,"kind":"sse_event","encoding":"utf-8","payload":"data: ready\n\n","size_bytes":12}
						]
					}
				]
			}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer apiServer.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", apiServer.URL,
		"--token", "token",
		"--output", "json",
		"app", "request", "stream", "demo", "/events",
		"--timeout", "150ms",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected stream diagnosis to fail when public route stalls after headers")
	}

	if gotInternalBody.Method != http.MethodGet || gotInternalBody.Path != "/events" {
		t.Fatalf("unexpected internal request body %+v", gotInternalBody)
	}
	if gotInternalBody.MaxChunks != defaultStreamChunkLimit {
		t.Fatalf("expected default max_chunks %d, got %+v", defaultStreamChunkLimit, gotInternalBody)
	}
	if strings.Join(gotInternalBody.Accepts, ",") != "*/*,text/event-stream" {
		t.Fatalf("unexpected internal accepts %+v", gotInternalBody.Accepts)
	}

	var result struct {
		Category string           `json:"category"`
		Layer    string           `json:"layer"`
		Summary  string           `json:"summary"`
		Probes   []map[string]any `json:"probes"`
		Evidence []string         `json:"evidence"`
	}
	if decodeErr := json.Unmarshal(stdout.Bytes(), &result); decodeErr != nil {
		t.Fatalf("decode stdout json: %v body=%s", decodeErr, stdout.String())
	}
	if result.Category != "headers_only_stall" {
		t.Fatalf("expected headers_only_stall category, got %+v", result)
	}
	if result.Layer != "external_cdn" {
		t.Fatalf("expected external_cdn layer, got %+v", result)
	}
	if !strings.Contains(result.Summary, "public route returned 200-style headers") {
		t.Fatalf("expected public headers-only summary, got %+v", result)
	}
	if len(result.Probes) != 4 {
		t.Fatalf("expected 4 probes, got %+v", result.Probes)
	}
	foundStall := false
	for _, evidence := range result.Evidence {
		if strings.Contains(evidence, "headers_only_stall=true") && strings.Contains(evidence, "public_route") {
			foundStall = true
			break
		}
	}
	if !foundStall {
		t.Fatalf("expected evidence to mention public headers_only_stall, got %+v", result.Evidence)
	}

	publicMu.Lock()
	defer publicMu.Unlock()
	if strings.Join(publicAccepts, ",") != "*/*,text/event-stream" {
		t.Fatalf("unexpected public accepts %+v", publicAccepts)
	}
}
