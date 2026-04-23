package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"fugue/internal/model"
)

func TestRequestAppInternalHTTPStreamCapturesSSEFrames(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})

	var (
		mu      sync.Mutex
		accepts []string
	)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		accepts = append(accepts, r.Header.Get("Accept"))
		mu.Unlock()

		if r.URL.Path != "/events" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		_, _ = io.WriteString(w, ": keepalive\n\n")
		flusher.Flush()
		_, _ = io.WriteString(w, "data: ready\n\n")
		flusher.Flush()
	}))
	defer backend.Close()

	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("parse backend url: %v", err)
	}
	backendTransport := backend.Client().Transport
	server.appRequestHTTPClient = &http.Client{
		Transport: diagnosticRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			cloned := req.Clone(req.Context())
			cloned.URL.Scheme = backendURL.Scheme
			cloned.URL.Host = backendURL.Host
			cloned.Host = backendURL.Host
			return backendTransport.RoundTrip(cloned)
		}),
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/request-stream", apiKey, map[string]any{
		"method":          "GET",
		"path":            "/events",
		"accepts":         []string{"*/*", "text/event-stream"},
		"timeout_ms":      500,
		"max_chunks":      3,
		"max_chunk_bytes": 256,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Probes []model.HTTPStreamProbe `json:"probes"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.Probes) != 2 {
		t.Fatalf("expected 2 probes, got %+v", response.Probes)
	}

	for _, probe := range response.Probes {
		if !probe.Timing.HeadersObserved {
			t.Fatalf("expected headers observed, got %+v", probe)
		}
		if !probe.Timing.BodyByteObserved {
			t.Fatalf("expected first body byte observed, got %+v", probe)
		}
		if !probe.Timing.SSEEventObserved {
			t.Fatalf("expected first SSE event observed, got %+v", probe)
		}
		if len(probe.FirstChunks) < 2 {
			t.Fatalf("expected at least 2 chunk samples, got %+v", probe.FirstChunks)
		}
		if probe.FirstChunks[0].Kind != "sse_comment" || probe.FirstChunks[0].Payload != ": keepalive\n\n" {
			t.Fatalf("expected keepalive comment frame, got %+v", probe.FirstChunks[0])
		}
		if probe.FirstChunks[1].Kind != "sse_event" || probe.FirstChunks[1].Payload != "data: ready\n\n" {
			t.Fatalf("expected first SSE event frame, got %+v", probe.FirstChunks[1])
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(accepts) != 2 || accepts[0] != "*/*" || accepts[1] != "text/event-stream" {
		t.Fatalf("unexpected Accept headers %+v", accepts)
	}
}

func TestRequestAppInternalHTTPStreamMarksHeadersOnlyStall(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer backend.Close()

	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("parse backend url: %v", err)
	}
	backendTransport := backend.Client().Transport
	server.appRequestHTTPClient = &http.Client{
		Transport: diagnosticRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			cloned := req.Clone(req.Context())
			cloned.URL.Scheme = backendURL.Scheme
			cloned.URL.Host = backendURL.Host
			cloned.Host = backendURL.Host
			return backendTransport.RoundTrip(cloned)
		}),
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/request-stream", apiKey, map[string]any{
		"method":     "GET",
		"path":       "/events",
		"accepts":    []string{"text/event-stream"},
		"timeout_ms": 100,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Probes []model.HTTPStreamProbe `json:"probes"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.Probes) != 1 {
		t.Fatalf("expected 1 probe, got %+v", response.Probes)
	}
	probe := response.Probes[0]
	if probe.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %+v", probe)
	}
	if !probe.Timing.HeadersObserved || probe.Timing.BodyByteObserved {
		t.Fatalf("expected headers without body bytes, got %+v", probe)
	}
	if !probe.HeadersOnlyStall {
		t.Fatalf("expected headers_only_stall=true, got %+v", probe)
	}
	if probe.Timing.TimeToFirstBodyMS != 0 {
		t.Fatalf("expected no first body byte timing, got %+v", probe.Timing)
	}
	if probe.Timing.TotalTimeMS < 50 {
		t.Fatalf("expected total time to reflect timeout, got %+v", probe.Timing)
	}
}
