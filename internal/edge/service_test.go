package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"

	"github.com/gorilla/websocket"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type shortReadReader struct {
	data []byte
	err  error
	done bool
}

func (r *shortReadReader) Read(data []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	n := copy(data, r.data)
	return n, r.err
}

func TestEdgeProxyObservationRequestFactFieldsAreRedactedAndRouted(t *testing.T) {
	t.Parallel()
	observed := edgeProxyObservation{
		ReceivedAt:           time.Date(2026, 6, 6, 1, 2, 3, 0, time.UTC),
		Host:                 "demo.fugue.pro",
		Method:               http.MethodPost,
		Path:                 "/v1/chat?...",
		TraceID:              "4bf92f3577b34da6a3ce929d0e0e4736",
		RequestID:            "req_123",
		StatusCode:           http.StatusServiceUnavailable,
		Duration:             1500 * time.Millisecond,
		TTFB:                 200 * time.Millisecond,
		Upstream:             1200 * time.Millisecond,
		OriginDNS:            3 * time.Millisecond,
		OriginConnect:        9 * time.Millisecond,
		OriginGotConn:        true,
		OriginRemoteAddr:     "10.42.5.112:8000",
		OriginLocalAddr:      "10.42.1.10:45678",
		OriginWroteHeaders:   true,
		OriginWroteRequest:   true,
		OriginRequestWrite:   20 * time.Millisecond,
		OriginTTFB:           200 * time.Millisecond,
		OriginTotal:          1200 * time.Millisecond,
		RequestBytes:         123,
		RequestBodyReadBytes: 100,
		RequestBodyReadError: "unexpected EOF",
		EdgeRequestID:        "edge_req_123",
		Protocol:             "HTTP/1.1",
		ClientIP:             "203.0.113.10",
		ClientRemoteAddr:     "203.0.113.10:45678",
		ResponseBytes:        456,
		Streaming:            true,
		CacheStatus:          edgeCacheStatusBypass,
		Upload:               true,
		Route: model.EdgeRouteBinding{
			Hostname:             "demo.fugue.pro",
			PathPrefix:           "/v1",
			RouteKind:            model.EdgeRouteKindPlatform,
			AppID:                "app_123",
			TenantID:             "tenant_123",
			RuntimeID:            "runtime_123",
			EdgeGroupID:          "edge-group-us",
			RuntimeEdgeGroupID:   "edge-group-us",
			RouteGeneration:      "routegen_123",
			DeploymentGeneration: "deploygen_123",
		},
	}

	fields := edgeProxyObservationRequestFactFields(observed, config.EdgeConfig{EdgeID: "edge_123"})
	if fields["event_type"] != "request_fact" || fields["app_id"] != "app_123" || fields["status_class"] != "5xx" {
		t.Fatalf("unexpected request fact fields: %+v", fields)
	}
	if fields["path_template"] != "/v1" || fields["trace_id"] == "" || fields["request_id"] != "req_123" {
		t.Fatalf("missing route or trace fields: %+v", fields)
	}
	summary := fmt.Sprint(fields["summary_json"])
	if strings.Contains(summary, "token=") || strings.Contains(summary, "Authorization") || strings.Contains(summary, "upstream_url") {
		t.Fatalf("summary leaked sensitive data: %s", summary)
	}
	if !strings.Contains(summary, `"path":"/v1/chat?..."`) || !strings.Contains(summary, `"upload":true`) {
		t.Fatalf("summary missing expected safe details: %s", summary)
	}
	if !strings.Contains(summary, `"origin_wrote_request":true`) || !strings.Contains(summary, `"origin_response_wait_ms":180`) || !strings.Contains(summary, `"origin_remote_addr":"10.42.5.112:8000"`) {
		t.Fatalf("summary missing origin phase details: %s", summary)
	}
	if !strings.Contains(summary, `"edge_request_id":"edge_req_123"`) ||
		!strings.Contains(summary, `"request_body_read_bytes":100`) ||
		!strings.Contains(summary, `"request_body_missing_bytes":23`) ||
		!strings.Contains(summary, `"request_body_complete":false`) ||
		!strings.Contains(summary, `"request_body_read_error":"unexpected EOF"`) {
		t.Fatalf("summary missing request body observability details: %s", summary)
	}
}

func TestEdgeProxyObservationBackfillsRequestIDFromOriginResponse(t *testing.T) {
	t.Parallel()

	target, err := url.Parse("http://origin.internal")
	if err != nil {
		t.Fatalf("parse target: %v", err)
	}
	service := NewService(config.EdgeConfig{}, log.New(ioDiscard{}, "", 0))
	service.proxyBase = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"X-Request-Id": []string{"origin_req_123"},
			},
			Body:    io.NopCloser(strings.NewReader("ok")),
			Request: req,
		}, nil
	})
	observed := edgeProxyObservation{}
	proxy := service.newEdgeReverseProxy("demo.fugue.pro", target, model.EdgeRouteBinding{}, &observed, false, nil)

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/v1", nil))

	if observed.RequestID != "origin_req_123" {
		t.Fatalf("expected origin response request id to be backfilled, got %q", observed.RequestID)
	}
}

func TestEdgeProxyObservationKeepsInboundRequestID(t *testing.T) {
	t.Parallel()

	target, err := url.Parse("http://origin.internal")
	if err != nil {
		t.Fatalf("parse target: %v", err)
	}
	service := NewService(config.EdgeConfig{}, log.New(ioDiscard{}, "", 0))
	service.proxyBase = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"X-Request-Id": []string{"origin_req_123"},
			},
			Body:    io.NopCloser(strings.NewReader("ok")),
			Request: req,
		}, nil
	})
	observed := edgeProxyObservation{RequestID: "inbound_req_456"}
	proxy := service.newEdgeReverseProxy("demo.fugue.pro", target, model.EdgeRouteBinding{}, &observed, false, nil)

	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/v1", nil))

	if observed.RequestID != "inbound_req_456" {
		t.Fatalf("expected inbound request id to be preserved, got %q", observed.RequestID)
	}
}

func TestProxyHandlerEmitsAndForwardsEdgeRequestID(t *testing.T) {
	t.Parallel()

	var originEdgeRequestID string
	var originClientRemoteAddr string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originEdgeRequestID = r.Header.Get(edgeRequestIDHeader)
		originClientRemoteAddr = r.Header.Get(edgeClientRemoteAddrHeader)
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	bundle := testBundle("routegen_edge_request_id")
	bundle.Routes[0].UpstreamURL = backend.URL
	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_edge_request_id"`, now, false)

	req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/upload", strings.NewReader("payload"))
	req.Header.Set("X-Forwarded-For", "198.51.100.7")
	req.Header.Set(edgeClientRemoteAddrHeader, "198.51.100.7:45678")
	recorder := httptest.NewRecorder()
	service.ProxyHandler().ServeHTTP(recorder, req)

	edgeRequestID := recorder.Header().Get(edgeRequestIDHeader)
	if edgeRequestID == "" {
		t.Fatalf("expected edge request id response header")
	}
	if originEdgeRequestID != edgeRequestID {
		t.Fatalf("expected origin edge request id %q, got %q", edgeRequestID, originEdgeRequestID)
	}
	if originClientRemoteAddr != "" {
		t.Fatalf("expected internal client remote addr header not to reach origin, got %q", originClientRemoteAddr)
	}
}

func TestProxyHandlerBuffersJSONSSERequestBodyBeforeOrigin(t *testing.T) {
	t.Parallel()

	var originBody string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read origin body: %v", err)
			return
		}
		originBody = string(body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	bundle := testBundle("routegen_buffer_body")
	bundle.Routes[0].UpstreamURL = backend.URL
	service := NewService(config.EdgeConfig{
		APIURL:                         "https://api.example.invalid",
		EdgeToken:                      "edge-secret",
		RequestBodyBufferPath:          t.TempDir(),
		RequestBodyBufferMaxBytes:      1024,
		RequestBodyBufferTotalMaxBytes: 1024,
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_buffer_body"`, time.Now().UTC(), false)

	req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/v1/responses", strings.NewReader(`{"prompt":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	recorder := httptest.NewRecorder()
	service.ProxyHandler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%q", recorder.Code, recorder.Body.String())
	}
	if originBody != `{"prompt":"hello"}` {
		t.Fatalf("unexpected origin body %q", originBody)
	}
	if used := service.edgeRequestBodyBufferManager().usedBytes(); used != 0 {
		t.Fatalf("expected buffer reservation to be released, used=%d", used)
	}
	if active := service.edgeRequestBodyBufferManager().activeRequests(); active != 0 {
		t.Fatalf("expected no active buffer reservations, active=%d", active)
	}
}

func TestProxyHandlerDoesNotReachOriginWhenBufferedBodyShortReads(t *testing.T) {
	t.Parallel()

	var originHits atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originHits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	bundle := testBundle("routegen_buffer_short_read")
	bundle.Routes[0].UpstreamURL = backend.URL
	service := NewService(config.EdgeConfig{
		APIURL:                         "https://api.example.invalid",
		EdgeToken:                      "edge-secret",
		RequestBodyBufferPath:          t.TempDir(),
		RequestBodyBufferMaxBytes:      1024,
		RequestBodyBufferTotalMaxBytes: 1024,
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_buffer_short_read"`, time.Now().UTC(), false)

	req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/v1/responses", io.NopCloser(&shortReadReader{
		data: []byte(`{"bad"`),
		err:  io.ErrUnexpectedEOF,
	}))
	req.ContentLength = 20
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	recorder := httptest.NewRecorder()
	service.ProxyHandler().ServeHTTP(recorder, req)

	if recorder.Code != edgeStatusClientClosedRequest {
		t.Fatalf("expected status %d, got %d body=%q", edgeStatusClientClosedRequest, recorder.Code, recorder.Body.String())
	}
	if hits := originHits.Load(); hits != 0 {
		t.Fatalf("expected origin not to be reached, hits=%d", hits)
	}
}

func TestProxyHandlerRejectsBufferedBodyOverLimit(t *testing.T) {
	t.Parallel()

	var originHits atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originHits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	bundle := testBundle("routegen_buffer_too_large")
	bundle.Routes[0].UpstreamURL = backend.URL
	service := NewService(config.EdgeConfig{
		APIURL:                         "https://api.example.invalid",
		EdgeToken:                      "edge-secret",
		RequestBodyBufferPath:          t.TempDir(),
		RequestBodyBufferMaxBytes:      3,
		RequestBodyBufferTotalMaxBytes: 1024,
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_buffer_too_large"`, time.Now().UTC(), false)

	req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/v1/responses", strings.NewReader("abcd"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	recorder := httptest.NewRecorder()
	service.ProxyHandler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status %d, got %d body=%q", http.StatusRequestEntityTooLarge, recorder.Code, recorder.Body.String())
	}
	if hits := originHits.Load(); hits != 0 {
		t.Fatalf("expected origin not to be reached, hits=%d", hits)
	}
}

func TestEdgeProxyObservedRequestBodyRecordsShortRead(t *testing.T) {
	t.Parallel()

	observed := edgeProxyObservation{}
	body := &edgeProxyObservedRequestBody{
		ReadCloser:  io.NopCloser(&shortReadReader{data: []byte("abc"), err: io.ErrUnexpectedEOF}),
		observation: &observed,
	}
	data, err := io.ReadAll(body)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected unexpected EOF, got data=%q err=%v", string(data), err)
	}
	if string(data) != "abc" || observed.RequestBodyReadBytes != 3 {
		t.Fatalf("unexpected body observation data=%q read=%d", string(data), observed.RequestBodyReadBytes)
	}
	if observed.RequestBodyReadError != io.ErrUnexpectedEOF.Error() || observed.RequestBodyEOF {
		t.Fatalf("unexpected body error observation error=%q eof=%t", observed.RequestBodyReadError, observed.RequestBodyEOF)
	}
}

func TestEdgeProxyTransportRecordsOriginRequestPhases(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			return
		}
		if string(body) != "payload" {
			t.Errorf("unexpected body %q", string(body))
			return
		}
		time.Sleep(10 * time.Millisecond)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	observed := edgeProxyObservation{Streaming: true, Upload: true}
	proxyTransport := &edgeProxyTransport{
		base:        transport,
		observation: &observed,
	}
	req, err := http.NewRequest(http.MethodPost, backend.URL, strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := proxyTransport.RoundTrip(req)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	defer resp.Body.Close()
	if body, err := io.ReadAll(resp.Body); err != nil || string(body) != "ok" {
		t.Fatalf("unexpected response body=%q err=%v", string(body), err)
	}

	if !observed.OriginGotConn || !observed.OriginWroteHeaders || !observed.OriginWroteRequest {
		t.Fatalf("origin phases not recorded: %+v", observed)
	}
	if observed.OriginRemoteAddr == "" || observed.OriginLocalAddr == "" {
		t.Fatalf("origin addresses not recorded: remote=%q local=%q", observed.OriginRemoteAddr, observed.OriginLocalAddr)
	}
	if observed.OriginRequestWrite <= 0 || observed.OriginTTFB <= 0 || observed.OriginTotal <= 0 {
		t.Fatalf("origin durations not recorded: write=%s ttfb=%s total=%s", observed.OriginRequestWrite, observed.OriginTTFB, observed.OriginTotal)
	}
	if wait := originResponseHeaderWait(observed); wait <= 0 {
		t.Fatalf("expected positive response wait after request write, got %s from %+v", wait, observed)
	}
}

func TestSyncOnceWritesRouteBundleCache(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_first")
	var gotToken string
	var gotEdgeGroupID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/edge/routes" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		gotToken = r.URL.Query().Get("token")
		gotEdgeGroupID = r.URL.Query().Get("edge_group_id")
		if got := r.Header.Get("If-None-Match"); got != "" {
			t.Fatalf("expected no conditional header on first sync, got %q", got)
		}
		w.Header().Set("ETag", `"routegen_first"`)
		if err := json.NewEncoder(w).Encode(bundle); err != nil {
			t.Fatalf("encode bundle: %v", err)
		}
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "routes-cache.json")
	var logs bytes.Buffer
	service := NewService(config.EdgeConfig{
		APIURL:      server.URL,
		EdgeToken:   "edge-secret",
		EdgeGroupID: "edge-group-country-hk",
		CachePath:   cachePath,
	}, log.New(&logs, "", 0))

	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatalf("sync once: %v", err)
	}
	if gotToken != "edge-secret" {
		t.Fatalf("expected edge token to be sent, got %q", gotToken)
	}
	if gotEdgeGroupID != "edge-group-country-hk" {
		t.Fatalf("expected edge group filter, got %q", gotEdgeGroupID)
	}

	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	var cached cacheFile
	if err := json.Unmarshal(data, &cached); err != nil {
		t.Fatalf("decode cache: %v", err)
	}
	if cached.Bundle.Version != "routegen_first" || cached.ETag != `"routegen_first"` {
		t.Fatalf("unexpected cached bundle: %+v", cached)
	}
	status := service.Status()
	if !status.Healthy || status.Status != "ok" || status.BundleVersion != "routegen_first" || status.RouteCount != 1 {
		t.Fatalf("unexpected status after sync: %+v", status)
	}
	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_info{edge_id="",edge_group_id="edge-group-country-hk"} 1`,
		`fugue_edge_bundle_sync_total{result="success"} 1`,
		`fugue_edge_bundle_sync_total{result="not_modified"} 0`,
		`fugue_edge_bundle_sync_total{result="error"} 0`,
		`fugue_edge_cache_write_total{result="success"} 1`,
		`fugue_edge_cache_write_total{result="error"} 0`,
		`fugue_edge_bundle_sync_duration_seconds`,
		`fugue_edge_bundle_age_seconds`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
	if !strings.Contains(logs.String(), "edge route bundle sync success; version=routegen_first routes=1 tls_allowlist=1") {
		t.Fatalf("expected sync success log, got %s", logs.String())
	}
}

func TestLoadCacheFallsBackToPreviousVerifiedGeneration(t *testing.T) {
	t.Parallel()

	cachePath := filepath.Join(t.TempDir(), "routes-cache.json")
	service := NewService(config.EdgeConfig{
		CachePath:         cachePath,
		CacheArchiveLimit: 3,
		MaxStale:          time.Hour,
	}, log.New(ioDiscard{}, "", 0))
	if err := service.writeCache(cacheFile{
		Version:  cacheFileVersion,
		ETag:     `"routegen_old"`,
		CachedAt: time.Now().UTC(),
		Bundle:   testBundle("routegen_old"),
	}); err != nil {
		t.Fatalf("write old cache: %v", err)
	}
	if err := service.writeCache(cacheFile{
		Version:  cacheFileVersion,
		ETag:     `"routegen_new"`,
		CachedAt: time.Now().UTC(),
		Bundle:   testBundle("routegen_new"),
	}); err != nil {
		t.Fatalf("write new cache: %v", err)
	}
	if err := os.WriteFile(cachePath, []byte("{corrupt"), 0o600); err != nil {
		t.Fatalf("corrupt current cache: %v", err)
	}

	if err := service.LoadCache(); err != nil {
		t.Fatalf("load cache with fallback: %v", err)
	}
	status := service.Status()
	if status.ServingGeneration != "routegen_old" || status.CacheCorruptGeneration != "unknown" {
		t.Fatalf("expected previous LKG after corrupt current cache, got %+v", status)
	}
}

func TestHeartbeatOnceReportsEdgeInventory(t *testing.T) {
	t.Parallel()

	var gotToken string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/edge/heartbeat" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		gotToken = r.URL.Query().Get("token")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode heartbeat: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "node": gotBody})
	}))
	defer server.Close()

	service := NewService(config.EdgeConfig{
		APIURL:            server.URL,
		EdgeToken:         "edge-secret",
		EdgeID:            "edge-us-1",
		EdgeGroupID:       "edge-group-country-us",
		Region:            "us-east",
		Country:           "US",
		PublicIPv4:        "203.0.113.10",
		MeshIP:            "100.64.0.10",
		HeartbeatInterval: time.Minute,
	}, log.New(ioDiscard{}, "", 0))
	bundle := testBundle("routegen_heartbeat")
	bundle.EdgeID = "edge-us-1"
	bundle.EdgeGroupID = "edge-group-country-us"
	service.recordSyncSuccess(bundle, `"routegen_heartbeat"`, time.Now().UTC(), false)
	service.recordCaddyApply("routegen_heartbeat", 1, "routegen_heartbeat", nil)

	if err := service.HeartbeatOnce(context.Background()); err != nil {
		t.Fatalf("heartbeat once: %v", err)
	}
	if gotToken != "edge-secret" {
		t.Fatalf("expected edge token, got %q", gotToken)
	}
	for key, want := range map[string]any{
		"edge_id":               "edge-us-1",
		"edge_group_id":         "edge-group-country-us",
		"region":                "us-east",
		"country":               "US",
		"public_ipv4":           "203.0.113.10",
		"mesh_ip":               "100.64.0.10",
		"route_bundle_version":  "routegen_heartbeat",
		"caddy_applied_version": "routegen_heartbeat",
		"cache_status":          "ready",
		"tls_status":            "",
		"tls_last_message":      "",
		"status":                model.EdgeHealthHealthy,
		"healthy":               true,
		"draining":              false,
	} {
		if got := gotBody[key]; got != want {
			t.Fatalf("heartbeat field %s: expected %#v, got %#v in %#v", key, want, got, gotBody)
		}
	}
	if got := gotBody["tls_ready_at"]; got != nil {
		t.Fatalf("expected nil tls_ready_at for non-Caddy heartbeat, got %#v", got)
	}
	if got := gotBody["caddy_route_count"]; got != float64(1) {
		t.Fatalf("expected caddy_route_count 1, got %#v", got)
	}
}

func TestHeartbeatOnceReportsPerformanceSamples(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		PerformanceSamples []model.EdgePerformanceSample `json:"performance_samples"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/edge/heartbeat" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode heartbeat: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true})
	}))
	defer server.Close()

	service := NewService(config.EdgeConfig{
		APIURL:            server.URL,
		EdgeToken:         "edge-secret",
		EdgeID:            "edge-us-1",
		EdgeGroupID:       "edge-group-country-us",
		Region:            "us-east",
		Country:           "US",
		HeartbeatInterval: time.Minute,
	}, log.New(ioDiscard{}, "", 0))
	bundle := testBundle("routegen_perf")
	bundle.EdgeID = "edge-us-1"
	bundle.EdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].EdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].RuntimeEdgeGroupID = "edge-group-country-us"
	service.recordSyncSuccess(bundle, `"routegen_perf"`, time.Now().UTC(), false)
	service.recordProxyObservation(edgeProxyObservation{
		Host:        "demo.fugue.pro",
		Route:       bundle.Routes[0],
		StatusCode:  http.StatusOK,
		Duration:    120 * time.Millisecond,
		TTFB:        80 * time.Millisecond,
		Upstream:    70 * time.Millisecond,
		Proxied:     true,
		CacheStatus: edgeCacheStatusMiss,
	})
	service.recordProxyObservation(edgeProxyObservation{
		Host:        "demo.fugue.pro",
		Route:       bundle.Routes[0],
		StatusCode:  http.StatusOK,
		Duration:    10 * time.Millisecond,
		TTFB:        10 * time.Millisecond,
		CacheStatus: edgeCacheStatusHit,
	})

	if err := service.HeartbeatOnce(context.Background()); err != nil {
		t.Fatalf("heartbeat once: %v", err)
	}
	if len(gotBody.PerformanceSamples) != 1 {
		t.Fatalf("expected one performance sample, got %+v", gotBody.PerformanceSamples)
	}
	sample := gotBody.PerformanceSamples[0]
	if sample.EdgeID != "edge-us-1" ||
		sample.EdgeGroupID != "edge-group-country-us" ||
		sample.Hostname != "demo.fugue.pro" ||
		sample.ClientCountry != "us" ||
		sample.ClientRegion != "us-east" ||
		sample.RuntimeRegion != "us" ||
		sample.SampleCount != 2 ||
		sample.CacheHitCount != 1 ||
		sample.CacheObservationCount != 2 ||
		sample.TTFBMS != 80 ||
		sample.UpstreamMS != 70 ||
		sample.TotalMS != 80 {
		t.Fatalf("unexpected performance sample: %+v", sample)
	}
}

func TestSyncOnceNotModifiedKeepsExistingCacheFile(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_cached")
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		switch requests {
		case 1:
			w.Header().Set("ETag", `"routegen_cached"`)
			if err := json.NewEncoder(w).Encode(bundle); err != nil {
				t.Fatalf("encode bundle: %v", err)
			}
		case 2:
			if got := r.Header.Get("If-None-Match"); got != `"routegen_cached"` {
				t.Fatalf("expected If-None-Match header %q, got %q", `"routegen_cached"`, got)
			}
			w.WriteHeader(http.StatusNotModified)
		default:
			t.Fatalf("unexpected request %d", requests)
		}
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "routes-cache.json")
	service := NewService(config.EdgeConfig{
		APIURL:    server.URL,
		EdgeToken: "edge-secret",
		CachePath: cachePath,
	}, log.New(ioDiscard{}, "", 0))

	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	before, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read first cache: %v", err)
	}
	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	after, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read second cache: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("expected 304 sync to leave cache unchanged")
	}
	status := service.Status()
	if !status.Healthy || status.Status != "ok" || status.StaleCache {
		t.Fatalf("unexpected status after 304: %+v", status)
	}
	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_bundle_sync_total{result="success"} 1`,
		`fugue_edge_bundle_sync_total{result="not_modified"} 1`,
		`fugue_edge_bundle_sync_total{result="error"} 0`,
		`fugue_edge_cache_write_total{result="success"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestSyncOnceUsesStaleCacheWhenAPIUnavailable(t *testing.T) {
	t.Parallel()

	cachePath := filepath.Join(t.TempDir(), "routes-cache.json")
	writeTestCache(t, cachePath, testBundle("routegen_stale"), `"routegen_stale"`)
	var logs bytes.Buffer
	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
		CachePath: cachePath,
	}, log.New(&logs, "", 0))
	service.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("control plane unavailable")
	})}

	if err := service.LoadCache(); err != nil {
		t.Fatalf("load cache: %v", err)
	}
	if err := service.SyncOnce(context.Background()); err == nil {
		t.Fatal("expected sync error")
	}
	status := service.Status()
	if !status.Healthy || status.Status != "stale" || !status.StaleCache || status.BundleVersion != "routegen_stale" {
		t.Fatalf("expected stale but healthy status, got %+v", status)
	}
	bundle, ok := service.Bundle()
	if !ok || bundle.Version != "routegen_stale" {
		t.Fatalf("expected cached bundle to remain available, got ok=%v bundle=%+v", ok, bundle)
	}
	recorder := httptest.NewRecorder()
	service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected stale cache health to be 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	metrics := httptest.NewRecorder()
	service.Handler().ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metrics.Code != http.StatusOK {
		t.Fatalf("expected metrics status 200, got %d body=%s", metrics.Code, metrics.Body.String())
	}
	if !strings.Contains(metrics.Body.String(), `fugue_edge_stale_cache 1`) ||
		!strings.Contains(metrics.Body.String(), `fugue_edge_routes{bundle_version="routegen_stale"} 1`) ||
		!strings.Contains(metrics.Body.String(), `fugue_edge_bundle_sync_total{result="error"} 1`) ||
		!strings.Contains(metrics.Body.String(), `fugue_edge_cache_load_total{result="success"} 1`) {
		t.Fatalf("expected stale cache metrics, got %s", metrics.Body.String())
	}
	logOutput := logs.String()
	if !strings.Contains(logOutput, "edge route cache loaded; version=routegen_stale") ||
		!strings.Contains(logOutput, "edge route bundle sync failed; using stale cache; version=routegen_stale") {
		t.Fatalf("expected cache loaded and stale fallback logs, got %s", logOutput)
	}
}

func TestSyncOnceWithoutCacheIsUnhealthyWhenAPIUnavailable(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
		CachePath: filepath.Join(t.TempDir(), "routes-cache.json"),
	}, log.New(ioDiscard{}, "", 0))
	service.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("control plane unavailable")
	})}

	if err := service.SyncOnce(context.Background()); err == nil {
		t.Fatal("expected sync error")
	}
	status := service.Status()
	if status.Healthy || status.Status != "unhealthy" || status.BundleVersion != "" {
		t.Fatalf("expected unhealthy status without cache, got %+v", status)
	}
	recorder := httptest.NewRecorder()
	service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected health status %d, got %d body=%s", http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	}
	metrics := renderMetrics(t, service)
	if !strings.Contains(metrics, `fugue_edge_bundle_sync_total{result="error"} 1`) {
		t.Fatalf("expected sync error metric, got %s", metrics)
	}
}

func TestSyncErrorsAndLogsRedactEdgeToken(t *testing.T) {
	t.Parallel()

	const secret = "edge-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad token "+r.URL.Query().Get("token"), http.StatusInternalServerError)
	}))
	defer server.Close()

	var logs bytes.Buffer
	service := NewService(config.EdgeConfig{
		APIURL:    server.URL,
		EdgeToken: secret,
		CachePath: filepath.Join(t.TempDir(), "routes-cache.json"),
	}, log.New(&logs, "", 0))
	err := service.SyncOnce(context.Background())
	if err == nil {
		t.Fatal("expected sync error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("sync error leaked token: %v", err)
	}
	status := service.Status()
	if strings.Contains(status.LastError, secret) {
		t.Fatalf("status leaked token: %+v", status)
	}
	service.logSyncFailure(err)
	if strings.Contains(logs.String(), secret) {
		t.Fatalf("log leaked token: %s", logs.String())
	}
}

func TestLoadCacheMissAndErrorMetrics(t *testing.T) {
	t.Parallel()

	cachePath := filepath.Join(t.TempDir(), "routes-cache.json")
	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
		CachePath: cachePath,
	}, log.New(ioDiscard{}, "", 0))

	if err := service.LoadCache(); err != nil {
		t.Fatalf("missing cache should not fail: %v", err)
	}
	metrics := renderMetrics(t, service)
	if !strings.Contains(metrics, `fugue_edge_cache_load_total{result="miss"} 1`) {
		t.Fatalf("expected cache miss metric, got %s", metrics)
	}

	if err := os.WriteFile(cachePath, []byte("{"), 0o600); err != nil {
		t.Fatalf("write invalid cache: %v", err)
	}
	if err := service.LoadCache(); err == nil {
		t.Fatal("expected invalid cache to fail")
	}
	metrics = renderMetrics(t, service)
	if !strings.Contains(metrics, `fugue_edge_cache_load_total{result="error"} 1`) {
		t.Fatalf("expected cache error metric, got %s", metrics)
	}
}

func TestApplyCaddyConfigBuildsHostRoutesForBundle(t *testing.T) {
	t.Parallel()

	var loads []string
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Fatalf("unexpected caddy admin request %s %s", r.Method, r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read caddy config body: %v", err)
		}
		loads = append(loads, string(body))
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	bundle := testBundle("routegen_caddy")
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	bundle.Routes = append(bundle.Routes, custom)

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        admin.URL,
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: ":7833",
	}, log.New(ioDiscard{}, "", 0))

	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config: %v", err)
	}
	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply unchanged caddy config: %v", err)
	}
	if len(loads) != 1 {
		t.Fatalf("expected unchanged bundle version to be applied once, got %d loads", len(loads))
	}
	adminURL, err := url.Parse(admin.URL)
	if err != nil {
		t.Fatalf("parse admin url: %v", err)
	}
	for _, want := range []string{
		`"listen":"` + adminURL.Host + `"`,
		`"listen":["127.0.0.1:18080"]`,
		`"automatic_https":{"disable":true}`,
		`"host":["demo.fugue.pro"]`,
		`"host":["www.customer.com"]`,
		`"dial":"127.0.0.1:7833"`,
		`"X-Fugue-Edge-Route-Host":["demo.fugue.pro"]`,
		`"X-Fugue-Edge-Route-Host":["www.customer.com"]`,
		`"default_logger_name":"fugue_edge_access"`,
		`"include":["http.log.access.fugue_edge_access"]`,
	} {
		if !strings.Contains(loads[0], want) {
			t.Fatalf("caddy config missing %q:\n%s", want, loads[0])
		}
	}
	status := service.Status()
	if status.CaddyAppliedVersion != "routegen_caddy" || status.CaddyTLSMode != caddyTLSModeOff || status.CaddyLastError != "" {
		t.Fatalf("unexpected caddy status: %+v", status)
	}
	metrics := renderMetrics(t, service)
	if !strings.Contains(metrics, `fugue_edge_caddy_config_apply_total{result="success"} 1`) ||
		!strings.Contains(metrics, `fugue_edge_caddy_routes{bundle_version="routegen_caddy"} 2`) {
		t.Fatalf("expected caddy apply metrics, got %s", metrics)
	}
}

func TestBuildCaddyConfigRedactsSensitiveAccessLogHeaders(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	configBody, _, err := service.buildCaddyConfig(testBundle("routegen_caddy_redact"))
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(configBody, &parsed); err != nil {
		t.Fatalf("decode caddy config: %v", err)
	}
	logging := parsed["logging"].(map[string]any)
	logs := logging["logs"].(map[string]any)
	access := logs["fugue_edge_access"].(map[string]any)
	encoder := access["encoder"].(map[string]any)
	if encoder["format"] != "filter" {
		t.Fatalf("expected filter encoder, got %#v", encoder)
	}
	fields := encoder["fields"].(map[string]any)
	for _, header := range []string{
		"request>headers>Authorization",
		"request>headers>Cookie",
		"request>headers>Proxy-Authorization",
		"request>headers>X-Tailscale-Handshake",
	} {
		filter, ok := fields[header].(map[string]any)
		if !ok || filter["filter"] != "delete" {
			t.Fatalf("expected %s to be deleted by access log filter, got %#v", header, fields[header])
		}
	}
}

func TestApplyCaddyConfigWarmsPlatformHost(t *testing.T) {
	t.Parallel()

	var loads int
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Fatalf("unexpected caddy admin request %s %s", r.Method, r.URL.Path)
		}
		loads++
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	bundle := testBundle("routegen_caddy_warmup")
	platformDomain := bundle.Routes[0]
	platformDomain.Hostname = "www.fugue.pro"
	platformDomain.RouteKind = model.EdgeRouteKindPlatformDomain
	platformDomain.RouteGeneration = "routegen_platform_domain"
	bundle.Routes = append(bundle.Routes, platformDomain)
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	bundle.Routes = append(bundle.Routes, custom)

	var warmups []string
	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		ListenAddr:           "127.0.0.1:7832",
		CaddyEnabled:         true,
		CaddyAdminURL:        admin.URL,
		CaddyListenAddr:      ":18443",
		CaddyTLSMode:         caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr: ":7833",
	}, log.New(ioDiscard{}, "", 0))
	service.caddyWarmup = func(_ context.Context, addr, host string) error {
		warmups = append(warmups, addr+"|"+host)
		return nil
	}

	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config: %v", err)
	}
	if loads != 1 {
		t.Fatalf("expected one caddy config load, got %d", loads)
	}
	if got, want := fmt.Sprint(warmups), "[127.0.0.1:18443|demo.fugue.pro 127.0.0.1:18443|www.customer.com 127.0.0.1:18443|www.fugue.pro]"; got != want {
		t.Fatalf("unexpected warmups: got %s want %s", got, want)
	}
	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply unchanged caddy config: %v", err)
	}
	if loads != 1 || len(warmups) != 3 {
		t.Fatalf("expected unchanged config not to reload or rewarm, got loads=%d warmups=%v", loads, warmups)
	}
	metrics := renderMetrics(t, service)
	if !strings.Contains(metrics, `fugue_edge_caddy_tls_warmup_total{result="success"} 1`) ||
		!strings.Contains(metrics, `fugue_edge_caddy_tls_warmup_total{result="error"} 0`) {
		t.Fatalf("expected caddy warmup metrics, got %s", metrics)
	}
}

func TestApplyCaddyConfigSkipsDisabledCustomDomainTLSWork(t *testing.T) {
	t.Parallel()

	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Fatalf("unexpected caddy admin request %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	var apiCalls int
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalls++
		http.Error(w, "disabled custom domain must not request TLS work", http.StatusInternalServerError)
	}))
	defer api.Close()

	bundle := testBundle("routegen_disabled_custom_tls")
	custom := bundle.Routes[0]
	custom.Hostname = "disabled.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	custom.Status = model.EdgeRouteStatusDisabled
	custom.StatusReason = "desired replicas is 0"
	custom.UpstreamURL = ""
	bundle.Routes = []model.EdgeRouteBinding{custom}
	bundle.TLSAllowlist = []model.EdgeTLSAllowlistEntry{{
		Hostname:  custom.Hostname,
		AppID:     custom.AppID,
		TenantID:  custom.TenantID,
		Status:    model.AppDomainStatusVerified,
		TLSStatus: model.AppDomainTLSStatusReady,
	}}

	var warmups int
	service := NewService(config.EdgeConfig{
		APIURL:                api.URL,
		EdgeToken:             "edge-secret",
		EdgeGroupID:           "edge-group-default",
		ListenAddr:            "127.0.0.1:7832",
		CaddyEnabled:          true,
		CaddyAdminURL:         admin.URL,
		CaddyListenAddr:       ":18443",
		CaddyTLSMode:          caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr:  ":7833",
		CaddyDataDir:          t.TempDir(),
		CaddySharedTLSEnabled: true,
	}, log.New(ioDiscard{}, "", 0))
	service.caddyWarmup = func(context.Context, string, string) error {
		warmups++
		return nil
	}

	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config: %v", err)
	}
	if warmups != 0 || apiCalls != 0 {
		t.Fatalf("expected disabled custom domain to skip TLS work, got warmups=%d api_calls=%d", warmups, apiCalls)
	}
}

func TestApplyCaddyConfigDoesNotRepeatSharedTLSSyncForUnchangedBundle(t *testing.T) {
	t.Parallel()

	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	var bundleFetches int
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bundleFetches++
		w.WriteHeader(http.StatusNotFound)
	}))
	defer api.Close()

	bundle := testBundle("routegen_shared_tls_once")
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	bundle.Routes = []model.EdgeRouteBinding{custom}

	service := NewService(config.EdgeConfig{
		APIURL:                api.URL,
		EdgeToken:             "edge-secret",
		EdgeGroupID:           "edge-group-default",
		ListenAddr:            "127.0.0.1:7832",
		CaddyEnabled:          true,
		CaddyAdminURL:         admin.URL,
		CaddyListenAddr:       ":18443",
		CaddyTLSMode:          caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr:  ":7833",
		CaddyDataDir:          t.TempDir(),
		CaddySharedTLSEnabled: true,
	}, log.New(ioDiscard{}, "", 0))
	service.caddyWarmup = func(context.Context, string, string) error { return nil }

	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config: %v", err)
	}
	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply unchanged caddy config: %v", err)
	}
	if bundleFetches != 1 {
		t.Fatalf("expected one shared TLS fetch for unchanged bundle, got %d", bundleFetches)
	}
}

func TestApplyCaddyConfigWarmsPendingCustomDomainAndReportsReady(t *testing.T) {
	t.Parallel()

	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Fatalf("unexpected caddy admin request %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	dataDir := t.TempDir()
	writeLocalCaddyTLSBundle(t, dataDir, "www.customer.com", "local-cert-pem", "local-key-pem", `{"issuer":"test"}`)

	var reports []map[string]string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/edge/domains/www.customer.com/tls-bundle":
			w.WriteHeader(http.StatusNotFound)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/edge/domains/tls-report":
		default:
			t.Fatalf("unexpected API request %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("token"); got != "edge-secret" {
			t.Fatalf("unexpected edge token %q", got)
		}
		var report map[string]string
		if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
			t.Fatalf("decode TLS report: %v", err)
		}
		reports = append(reports, report)
		w.WriteHeader(http.StatusOK)
	}))
	defer api.Close()

	bundle := testBundle("routegen_custom_tls_warmup")
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	custom.Status = model.EdgeRouteStatusUnavailable
	custom.StatusReason = "custom domain ownership or TLS verification is pending"
	custom.UpstreamURL = ""
	custom.RouteGeneration = "routegen_custom_domain"
	bundle.Routes = []model.EdgeRouteBinding{custom}
	bundle.TLSAllowlist = []model.EdgeTLSAllowlistEntry{
		{
			Hostname:  "www.customer.com",
			AppID:     "app_demo",
			TenantID:  "tenant_demo",
			Status:    model.AppDomainStatusVerified,
			TLSStatus: model.AppDomainTLSStatusPending,
		},
	}

	var warmups []string
	service := NewService(config.EdgeConfig{
		APIURL:                api.URL,
		EdgeToken:             "edge-secret",
		EdgeGroupID:           "edge-group-default",
		ListenAddr:            "127.0.0.1:7832",
		CaddyEnabled:          true,
		CaddyAdminURL:         admin.URL,
		CaddyListenAddr:       ":18443",
		CaddyTLSMode:          caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr:  ":7833",
		CaddyDataDir:          dataDir,
		CaddySharedTLSEnabled: true,
	}, log.New(ioDiscard{}, "", 0))
	service.caddyWarmup = func(_ context.Context, addr, host string) error {
		warmups = append(warmups, addr+"|"+host)
		return nil
	}

	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config: %v", err)
	}
	if got, want := fmt.Sprint(warmups), "[127.0.0.1:18443|www.customer.com]"; got != want {
		t.Fatalf("unexpected warmups: got %s want %s", got, want)
	}
	if len(reports) != 1 {
		t.Fatalf("expected one TLS report, got %d reports=%v", len(reports), reports)
	}
	if reports[0]["hostname"] != "www.customer.com" || reports[0]["tls_status"] != model.AppDomainTLSStatusReady || reports[0]["tls_last_message"] != "" {
		t.Fatalf("unexpected TLS report: %+v", reports[0])
	}
	if reports[0]["certificate_pem"] != "local-cert-pem" || reports[0]["private_key_pem"] != "local-key-pem" {
		t.Fatalf("expected TLS report to include local Caddy certificate bundle, got %+v", reports[0])
	}
	if reports[0]["issuer_storage"] != defaultCaddyIssuerStorage {
		t.Fatalf("expected TLS report issuer storage %q, got %+v", defaultCaddyIssuerStorage, reports[0])
	}
}

func TestApplyCaddyConfigBackfillsSharedCertificateForReadyCustomDomain(t *testing.T) {
	t.Parallel()

	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Fatalf("unexpected caddy admin request %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	dataDir := t.TempDir()
	writeLocalCaddyTLSBundle(t, dataDir, "www.customer.com", "ready-cert-pem", "ready-key-pem", `{"issuer":"test"}`)

	var reports []map[string]string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/edge/domains/www.customer.com/tls-bundle":
			w.WriteHeader(http.StatusNotFound)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/edge/domains/tls-report":
		default:
			t.Fatalf("unexpected API request %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("token"); got != "edge-secret" {
			t.Fatalf("unexpected edge token %q", got)
		}
		var report map[string]string
		if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
			t.Fatalf("decode TLS report: %v", err)
		}
		reports = append(reports, report)
		w.WriteHeader(http.StatusOK)
	}))
	defer api.Close()

	bundle := testBundle("routegen_custom_tls_backfill")
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	custom.RouteGeneration = "routegen_custom_domain"
	bundle.Routes = []model.EdgeRouteBinding{custom}
	bundle.TLSAllowlist = []model.EdgeTLSAllowlistEntry{
		{
			Hostname:  "www.customer.com",
			AppID:     "app_demo",
			TenantID:  "tenant_demo",
			Status:    model.AppDomainStatusVerified,
			TLSStatus: model.AppDomainTLSStatusReady,
		},
	}

	var warmups []string
	service := NewService(config.EdgeConfig{
		APIURL:                api.URL,
		EdgeToken:             "edge-secret",
		EdgeGroupID:           "edge-group-default",
		ListenAddr:            "127.0.0.1:7832",
		CaddyEnabled:          true,
		CaddyAdminURL:         admin.URL,
		CaddyListenAddr:       ":18443",
		CaddyTLSMode:          caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr:  ":7833",
		CaddyDataDir:          dataDir,
		CaddySharedTLSEnabled: true,
	}, log.New(ioDiscard{}, "", 0))
	service.caddyWarmup = func(_ context.Context, addr, host string) error {
		warmups = append(warmups, addr+"|"+host)
		return nil
	}

	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config: %v", err)
	}
	if got, want := fmt.Sprint(warmups), "[127.0.0.1:18443|www.customer.com]"; got != want {
		t.Fatalf("unexpected warmups: got %s want %s", got, want)
	}
	if len(reports) != 1 {
		t.Fatalf("expected one TLS report, got %d reports=%v", len(reports), reports)
	}
	if reports[0]["hostname"] != "www.customer.com" || reports[0]["tls_status"] != model.AppDomainTLSStatusReady || reports[0]["tls_last_message"] != "" {
		t.Fatalf("unexpected TLS report: %+v", reports[0])
	}
	if reports[0]["certificate_pem"] != "ready-cert-pem" || reports[0]["private_key_pem"] != "ready-key-pem" {
		t.Fatalf("expected TLS report to include local Caddy certificate bundle, got %+v", reports[0])
	}
}

func TestApplyCaddyConfigDoesNotDowngradeReadyCustomDomainOnWarmupFailure(t *testing.T) {
	t.Parallel()

	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Fatalf("unexpected caddy admin request %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	var reports []map[string]string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/edge/domains/tls-report" {
			t.Fatalf("unexpected API request %s %s", r.Method, r.URL.Path)
		}
		var report map[string]string
		if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
			t.Fatalf("decode TLS report: %v", err)
		}
		reports = append(reports, report)
		w.WriteHeader(http.StatusOK)
	}))
	defer api.Close()

	bundle := testBundle("routegen_custom_tls_no_downgrade")
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	custom.RouteGeneration = "routegen_custom_domain"
	bundle.Routes = []model.EdgeRouteBinding{custom}
	bundle.TLSAllowlist = []model.EdgeTLSAllowlistEntry{
		{
			Hostname:  "www.customer.com",
			AppID:     "app_demo",
			TenantID:  "tenant_demo",
			Status:    model.AppDomainStatusVerified,
			TLSStatus: model.AppDomainTLSStatusReady,
		},
	}

	service := NewService(config.EdgeConfig{
		APIURL:               api.URL,
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		ListenAddr:           "127.0.0.1:7832",
		CaddyEnabled:         true,
		CaddyAdminURL:        admin.URL,
		CaddyListenAddr:      ":18443",
		CaddyTLSMode:         caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr: ":7833",
	}, log.New(ioDiscard{}, "", 0))
	service.caddyWarmup = func(context.Context, string, string) error {
		return errors.New("warmup failed")
	}

	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config: %v", err)
	}
	if len(reports) != 0 {
		t.Fatalf("expected ready custom domain not to be downgraded on warmup failure, got reports=%v", reports)
	}
}

func TestApplyCaddyConfigInstallsSharedCustomDomainCertificate(t *testing.T) {
	t.Parallel()

	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Fatalf("unexpected caddy admin request %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/edge/domains/www.customer.com/tls-bundle" {
			t.Fatalf("unexpected API request %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("token"); got != "edge-secret" {
			t.Fatalf("unexpected edge token %q", got)
		}
		if err := json.NewEncoder(w).Encode(map[string]any{
			"certificate": caddyTLSCertificateBundle{
				CertificatePEM: "shared-cert-pem",
				PrivateKeyPEM:  "shared-key-pem",
				MetadataJSON:   `{"issuer":"shared"}`,
				IssuerStorage:  defaultCaddyIssuerStorage,
			},
		}); err != nil {
			t.Fatalf("encode shared TLS bundle: %v", err)
		}
	}))
	defer api.Close()

	bundle := testBundle("routegen_custom_tls_sync")
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	custom.RouteGeneration = "routegen_custom_domain"
	bundle.Routes = []model.EdgeRouteBinding{custom}
	bundle.TLSAllowlist = nil

	dataDir := t.TempDir()
	var warmups []string
	service := NewService(config.EdgeConfig{
		APIURL:                api.URL,
		EdgeToken:             "edge-secret",
		EdgeGroupID:           "edge-group-default",
		ListenAddr:            "127.0.0.1:7832",
		CaddyEnabled:          true,
		CaddyAdminURL:         admin.URL,
		CaddyListenAddr:       ":18443",
		CaddyTLSMode:          caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr:  ":7833",
		CaddyDataDir:          dataDir,
		CaddySharedTLSEnabled: true,
	}, log.New(ioDiscard{}, "", 0))
	service.caddyWarmup = func(_ context.Context, addr, host string) error {
		warmups = append(warmups, addr+"|"+host)
		return nil
	}

	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config: %v", err)
	}
	if got, want := fmt.Sprint(warmups), "[127.0.0.1:18443|www.customer.com]"; got != want {
		t.Fatalf("unexpected warmups: got %s want %s", got, want)
	}
	hostDir := filepath.Join(dataDir, "certificates", defaultCaddyIssuerStorage, "www.customer.com")
	for _, item := range []struct {
		Path string
		Want string
	}{
		{Path: filepath.Join(hostDir, "www.customer.com.crt"), Want: "shared-cert-pem\n"},
		{Path: filepath.Join(hostDir, "www.customer.com.key"), Want: "shared-key-pem\n"},
		{Path: filepath.Join(hostDir, "www.customer.com.json"), Want: `{"issuer":"shared"}` + "\n"},
	} {
		data, err := os.ReadFile(item.Path)
		if err != nil {
			t.Fatalf("read installed cert file %s: %v", item.Path, err)
		}
		if string(data) != item.Want {
			t.Fatalf("unexpected installed file %s content %q", item.Path, string(data))
		}
	}
}

func TestSyncSharedCaddyTLSCertificatesSkipsUnchangedInstall(t *testing.T) {
	t.Parallel()

	var apiCalls int
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalls++
		if r.Method != http.MethodGet || r.URL.Path != "/v1/edge/domains/www.customer.com/tls-bundle" {
			t.Fatalf("unexpected API request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewEncoder(w).Encode(map[string]any{
			"certificate": caddyTLSCertificateBundle{
				CertificatePEM: "shared-cert-pem",
				PrivateKeyPEM:  "shared-key-pem",
				MetadataJSON:   `{"issuer":"shared"}`,
				IssuerStorage:  defaultCaddyIssuerStorage,
			},
		}); err != nil {
			t.Fatalf("encode shared TLS bundle: %v", err)
		}
	}))
	defer api.Close()

	var logs bytes.Buffer
	service := NewService(config.EdgeConfig{
		APIURL:                api.URL,
		EdgeToken:             "edge-secret",
		CaddyDataDir:          t.TempDir(),
		CaddySharedTLSEnabled: true,
	}, log.New(&logs, "", 0))

	for range 2 {
		if err := service.syncSharedCaddyTLSCertificates(context.Background(), []string{"www.customer.com"}); err != nil {
			t.Fatalf("sync shared TLS certificate: %v", err)
		}
	}
	if apiCalls != 2 {
		t.Fatalf("expected shared TLS certificate to be fetched twice, got %d", apiCalls)
	}
	if installs := strings.Count(logs.String(), "shared TLS certificate installed"); installs != 1 {
		t.Fatalf("expected unchanged shared TLS certificate to be installed once, got %d logs=%q", installs, logs.String())
	}
}

func TestApplyCaddyConfigWarmsHTTPAssetCache(t *testing.T) {
	t.Parallel()

	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Fatalf("unexpected caddy admin request %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	bundle := testBundle("routegen_cache_warmup")
	bundle.Routes[0].CachePolicyID = "static-assets-immutable-v1"
	bundle.Routes[0].CacheNamespace = "app_demo_deploy_1"

	var requests []string
	service := NewService(config.EdgeConfig{
		APIURL:                "https://api.example.invalid",
		EdgeToken:             "edge-secret",
		EdgeGroupID:           "edge-group-default",
		ListenAddr:            "127.0.0.1:7832",
		CaddyEnabled:          true,
		CaddyAdminURL:         admin.URL,
		CaddyListenAddr:       "127.0.0.1:18080",
		CaddyTLSMode:          caddyTLSModeOff,
		CaddyProxyListenAddr:  "127.0.0.1:7833",
		CacheWarmupEnabled:    true,
		CacheWarmupTimeout:    time.Second,
		CacheWarmupMaxTargets: 16,
		CacheWarmupMaxDepth:   2,
		AssetCacheMaxBytes:    1024 * 1024,
	}, log.New(ioDiscard{}, "", 0))
	service.cacheWarmupClientFactory = func(_, _ string) *http.Client {
		return &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				requests = append(requests, req.URL.Path+"|"+req.Header.Get("Accept-Encoding")+"|"+req.Header.Get(edgeCacheWarmupDiscoveryHeader))
				switch req.URL.Path {
				case "/":
					return warmupTestResponse(http.StatusOK, "text/html; charset=utf-8", `<!doctype html>
<link rel="stylesheet" href="/_next/static/chunks/app.css">
<script src="/_next/static/chunks/app.js"></script>
<link rel="preload" as="font" href="/_next/static/media/font.woff2">`), nil
				case "/_next/static/chunks/app.css":
					return warmupTestResponse(http.StatusOK, "text/css", `@font-face{font-family:test;src:url("../media/font2.woff2") format("woff2")}`), nil
				case "/_next/static/chunks/app.js":
					return warmupTestResponse(http.StatusOK, "application/javascript", `console.log("ok")`), nil
				case "/_next/static/media/font.woff2", "/_next/static/media/font2.woff2":
					return warmupTestResponse(http.StatusOK, "font/woff2", "font"), nil
				default:
					return warmupTestResponse(http.StatusNotFound, "text/plain", "missing"), nil
				}
			}),
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}

	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config: %v", err)
	}
	wantRequests := []string{
		"/|identity|1",
		"/_next/static/chunks/app.css|identity|1",
		"/|br, gzip|",
		"/_next/static/chunks/app.css|br, gzip|",
		"/_next/static/chunks/app.js|br, gzip|",
		"/_next/static/media/font.woff2|br, gzip|",
		"/_next/static/media/font2.woff2|br, gzip|",
	}
	for _, want := range wantRequests {
		if countString(requests, want) == 0 {
			t.Fatalf("expected warmup request %q in %v", want, requests)
		}
	}

	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply unchanged caddy config: %v", err)
	}
	if count := countString(requests, "/|identity|1"); count != 1 {
		t.Fatalf("expected unchanged config not to rerun cache warmup, root discovery count=%d requests=%v", count, requests)
	}

	metrics := renderMetrics(t, service)
	if !strings.Contains(metrics, `fugue_edge_http_cache_warmup_total{result="success"} 1`) ||
		!strings.Contains(metrics, `fugue_edge_http_cache_warmup_total{result="error"} 0`) {
		t.Fatalf("expected cache warmup metrics, got %s", metrics)
	}
}

func TestApplyCaddyConfigReappliesWhenStaticTLSFilesChange(t *testing.T) {
	t.Parallel()

	var loads int
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Fatalf("unexpected caddy admin request %s %s", r.Method, r.URL.Path)
		}
		loads++
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	tmpdir := t.TempDir()
	certFile := filepath.Join(tmpdir, "tls.crt")
	keyFile := filepath.Join(tmpdir, "tls.key")
	if err := os.WriteFile(certFile, []byte("cert-one"), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, []byte("key-one"), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	var warmups int
	service := NewService(config.EdgeConfig{
		APIURL:                 "https://api.example.invalid",
		EdgeToken:              "edge-secret",
		EdgeGroupID:            "edge-group-default",
		ListenAddr:             "127.0.0.1:7832",
		CaddyEnabled:           true,
		CaddyAdminURL:          admin.URL,
		CaddyListenAddr:        ":18443",
		CaddyTLSMode:           caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr:   ":7833",
		CaddyStaticTLSCertFile: certFile,
		CaddyStaticTLSKeyFile:  keyFile,
	}, log.New(ioDiscard{}, "", 0))
	service.caddyWarmup = func(_ context.Context, _, _ string) error {
		warmups++
		return nil
	}

	bundle := testBundle("routegen_caddy_static_tls_reload")
	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config: %v", err)
	}
	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply unchanged caddy config: %v", err)
	}
	if loads != 1 || warmups != 1 {
		t.Fatalf("expected unchanged static TLS files not to reload or rewarm, got loads=%d warmups=%d", loads, warmups)
	}
	if err := os.WriteFile(certFile, []byte("cert-two"), 0o600); err != nil {
		t.Fatalf("rewrite cert: %v", err)
	}
	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config after cert update: %v", err)
	}
	if loads != 2 || warmups != 2 {
		t.Fatalf("expected static TLS file change to reload and rewarm, got loads=%d warmups=%d", loads, warmups)
	}
}

func TestSyncFailureReappliesCachedCaddyConfig(t *testing.T) {
	t.Parallel()

	var loads int
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Fatalf("unexpected caddy admin request %s %s", r.Method, r.URL.Path)
		}
		loads++
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invariant failed", http.StatusInternalServerError)
	}))
	defer api.Close()

	bundle := testBundle("routegen_cached_reapply")
	service := NewService(config.EdgeConfig{
		APIURL:               api.URL,
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        admin.URL,
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: ":7833",
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_cached_reapply"`, time.Now().UTC(), false)
	service.recordCaddyApply(bundle.Version, 0, "", errors.New("apply caddy config: connect: connection refused"))

	if err := service.SyncOnce(context.Background()); err == nil {
		t.Fatal("expected route sync failure")
	}
	if loads != 1 {
		t.Fatalf("expected cached Caddy config to be replayed once, got %d loads", loads)
	}
	status := service.Status()
	if status.CaddyLastError != "" || status.CaddyAppliedVersion != bundle.Version {
		t.Fatalf("expected Caddy config to recover from cached bundle, got %+v", status)
	}
}

func TestBuildCaddyConfigSupportsInternalTLSCanary(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_tls")
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	bundle.Routes = append(bundle.Routes, custom)

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      ":18443",
		CaddyTLSMode:         caddyTLSModeInternal,
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	configBody, routeCount, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	if routeCount != 2 {
		t.Fatalf("expected 2 caddy routes, got %d", routeCount)
	}
	if strings.Contains(string(configBody), `"automatic_https":{"disable":true}`) {
		t.Fatalf("internal tls config should not disable automatic https:\n%s", configBody)
	}

	var parsed map[string]any
	if err := json.Unmarshal(configBody, &parsed); err != nil {
		t.Fatalf("decode caddy config: %v", err)
	}
	apps := parsed["apps"].(map[string]any)
	httpApp := apps["http"].(map[string]any)
	servers := httpApp["servers"].(map[string]any)
	server := servers["fugue_edge"].(map[string]any)
	policies := server["tls_connection_policies"].([]any)
	if len(policies) != 1 {
		t.Fatalf("expected one TLS connection policy, got %#v", policies)
	}
	tlsApp := apps["tls"].(map[string]any)
	automation := tlsApp["automation"].(map[string]any)
	automationPolicies := automation["policies"].([]any)
	if len(automationPolicies) != 1 {
		t.Fatalf("expected one TLS automation policy, got %#v", automationPolicies)
	}
	policy := automationPolicies[0].(map[string]any)
	subjects := policy["subjects"].([]any)
	if got := fmt.Sprint(subjects); got != "[demo.fugue.pro www.customer.com]" {
		t.Fatalf("unexpected TLS subjects: %s", got)
	}
	issuers := policy["issuers"].([]any)
	issuer := issuers[0].(map[string]any)
	if issuer["module"] != caddyTLSModeInternal {
		t.Fatalf("unexpected TLS issuer: %#v", issuer)
	}
}

func TestBuildCaddyConfigEnablesProxyProtocolBeforeTLS(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{
		APIURL:                         "https://api.example.invalid",
		EdgeToken:                      "edge-secret",
		EdgeGroupID:                    "edge-group-default",
		ListenAddr:                     ":7832",
		CaddyEnabled:                   true,
		CaddyAdminURL:                  "http://127.0.0.1:2019",
		CaddyListenAddr:                ":18443",
		CaddyTLSMode:                   caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr:           "127.0.0.1:7833",
		CaddyProxyProtocolEnabled:      true,
		CaddyProxyProtocolTrustedCIDRs: []string{"127.0.0.1/32", "10.0.0.0/8"},
	}, log.New(ioDiscard{}, "", 0))

	configBody, _, err := service.buildCaddyConfig(testBundle("routegen_caddy_proxy_protocol"))
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(configBody, &parsed); err != nil {
		t.Fatalf("decode caddy config: %v", err)
	}
	apps := parsed["apps"].(map[string]any)
	httpApp := apps["http"].(map[string]any)
	servers := httpApp["servers"].(map[string]any)
	server := servers["fugue_edge"].(map[string]any)
	wrappers := server["listener_wrappers"].([]any)
	if len(wrappers) != 2 {
		t.Fatalf("expected proxy_protocol and tls listener wrappers, got %#v", wrappers)
	}
	proxyWrapper := wrappers[0].(map[string]any)
	if proxyWrapper["wrapper"] != "proxy_protocol" || proxyWrapper["fallback_policy"] != "USE" {
		t.Fatalf("unexpected proxy protocol wrapper: %#v", proxyWrapper)
	}
	if fmt.Sprint(proxyWrapper["allow"]) != "[127.0.0.1/32 10.0.0.0/8]" {
		t.Fatalf("unexpected proxy protocol allow list: %#v", proxyWrapper["allow"])
	}
	tlsWrapper := wrappers[1].(map[string]any)
	if tlsWrapper["wrapper"] != "tls" {
		t.Fatalf("expected tls wrapper after proxy_protocol, got %#v", tlsWrapper)
	}
}

func TestBuildCaddyConfigSkipsRouteAOnlyHosts(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_route_a_only_caddy")
	routeAOnly := bundle.Routes[0]
	routeAOnly.Hostname = "legacy.fugue.pro"
	routeAOnly.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
	routeAOnly.RouteGeneration = "routegen_legacy"
	bundle.Routes = append(bundle.Routes, routeAOnly)

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	configBody, routeCount, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	if routeCount != 1 {
		t.Fatalf("expected only the opt-in route to be emitted, got %d", routeCount)
	}
	configText := string(configBody)
	if !strings.Contains(configText, `"host":["demo.fugue.pro"]`) {
		t.Fatalf("expected canary host in caddy config:\n%s", configText)
	}
	if strings.Contains(configText, "legacy.fugue.pro") {
		t.Fatalf("Route A-only host must not be emitted to caddy config:\n%s", configText)
	}
}

func TestBuildCaddyConfigIncludesDifferentEdgeGroupRoutes(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_edge_group_caddy")
	bundle.Routes[0].EdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].RuntimeEdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].PolicyEdgeGroupID = "edge-group-country-us"
	hkRoute := bundle.Routes[0]
	hkRoute.Hostname = "hk.fugue.pro"
	hkRoute.EdgeGroupID = "edge-group-country-hk"
	hkRoute.RuntimeEdgeGroupID = "edge-group-country-hk"
	hkRoute.PolicyEdgeGroupID = "edge-group-country-hk"
	hkRoute.RouteGeneration = "routegen_hk"
	remoteRuntimeRoute := bundle.Routes[0]
	remoteRuntimeRoute.Hostname = "remote-runtime.fugue.pro"
	remoteRuntimeRoute.RuntimeEdgeGroupID = "edge-group-country-hk"
	remoteRuntimeRoute.PolicyEdgeGroupID = "edge-group-country-us"
	remoteRuntimeRoute.RouteGeneration = "routegen_remote_runtime"
	bundle.Routes = append(bundle.Routes, hkRoute, remoteRuntimeRoute)

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-country-us",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	configBody, routeCount, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	if routeCount != 3 {
		t.Fatalf("expected all active host routes to be emitted, got %d", routeCount)
	}
	configText := string(configBody)
	if !strings.Contains(configText, `"host":["demo.fugue.pro"]`) {
		t.Fatalf("expected local edge group host in caddy config:\n%s", configText)
	}
	if !strings.Contains(configText, "hk.fugue.pro") {
		t.Fatalf("expected different edge group host in caddy config:\n%s", configText)
	}
	if !strings.Contains(configText, `"host":["remote-runtime.fugue.pro"]`) {
		t.Fatalf("expected nearest-edge fallback route to be emitted by serving edge group:\n%s", configText)
	}
}

func TestBuildCaddyConfigAddsNotFoundFallbackForUnmatchedHosts(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_fallback")
	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	configBody, routeCount, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	if routeCount != 1 {
		t.Fatalf("expected one emitted host route, got %d", routeCount)
	}
	configText := string(configBody)
	if !strings.Contains(configText, `"handler":"static_response"`) ||
		!strings.Contains(configText, `"status_code":404`) ||
		!strings.Contains(configText, "fugue route not found") {
		t.Fatalf("expected 404 fallback route in caddy config:\n%s", configText)
	}
}

func TestRouteForRequestPrefersLongestPathPrefix(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_path_prefix")
	apiRoute := bundle.Routes[0]
	apiRoute.PathPrefix = "/api"
	apiRoute.AppID = "app_api"
	apiRoute.UpstreamURL = "http://api.fg-tenant-demo.svc.cluster.local:8080"
	bundle.Routes = append(bundle.Routes, apiRoute)

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_path_prefix"`, time.Now().UTC(), false)

	route, ok, fallbackHit := service.routeForRequest("demo.fugue.pro", "/api/users")
	if !ok || fallbackHit {
		t.Fatalf("expected path-prefixed route without fallback, ok=%t fallback=%t route=%+v", ok, fallbackHit, route)
	}
	if route.AppID != "app_api" || route.PathPrefix != "/api" {
		t.Fatalf("expected /api route, got %+v", route)
	}

	rootRoute, ok, fallbackHit := service.routeForRequest("demo.fugue.pro", "/")
	if !ok || fallbackHit {
		t.Fatalf("expected root route without fallback, ok=%t fallback=%t route=%+v", ok, fallbackHit, rootRoute)
	}
	if rootRoute.AppID != "app_demo" || model.NormalizeAppRoutePathPrefix(rootRoute.PathPrefix) != "/" {
		t.Fatalf("expected root route, got %+v", rootRoute)
	}
}

func TestBuildCaddyConfigSupportsPublicOnDemandTLSCanary(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_public_tls")
	service := NewService(config.EdgeConfig{
		APIURL:                 "https://api.example.invalid",
		EdgeToken:              "edge-secret",
		EdgeGroupID:            "edge-group-default",
		ListenAddr:             ":7832",
		CaddyEnabled:           true,
		CaddyAdminURL:          "http://127.0.0.1:2019",
		CaddyListenAddr:        ":443",
		CaddyTLSMode:           caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr:   "127.0.0.1:7833",
		CaddyStaticTLSCertFile: "/etc/caddy/static-tls/tls.crt",
		CaddyStaticTLSKeyFile:  "/etc/caddy/static-tls/tls.key",
	}, log.New(ioDiscard{}, "", 0))

	configBody, routeCount, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	if routeCount != 1 {
		t.Fatalf("expected 1 caddy route, got %d", routeCount)
	}
	if strings.Contains(string(configBody), "edge-secret") {
		t.Fatalf("public on-demand caddy config must not contain edge token:\n%s", configBody)
	}

	var parsed map[string]any
	if err := json.Unmarshal(configBody, &parsed); err != nil {
		t.Fatalf("decode caddy config: %v", err)
	}
	apps := parsed["apps"].(map[string]any)
	httpApp := apps["http"].(map[string]any)
	servers := httpApp["servers"].(map[string]any)
	server := servers["fugue_edge"].(map[string]any)
	if fmt.Sprint(server["listen"]) != "[:443]" {
		t.Fatalf("unexpected caddy listen: %#v", server["listen"])
	}
	if policies := server["tls_connection_policies"].([]any); len(policies) != 1 {
		t.Fatalf("expected one TLS connection policy, got %#v", policies)
	}
	tlsApp := apps["tls"].(map[string]any)
	automation := tlsApp["automation"].(map[string]any)
	automationPolicies := automation["policies"].([]any)
	if len(automationPolicies) != 1 || automationPolicies[0].(map[string]any)["on_demand"] != true {
		t.Fatalf("expected on-demand TLS automation policy, got %#v", automationPolicies)
	}
	onDemand := automation["on_demand"].(map[string]any)
	permission := onDemand["permission"].(map[string]any)
	if permission["module"] != "http" || permission["endpoint"] != "http://127.0.0.1:7832/edge/tls/ask" {
		t.Fatalf("unexpected on-demand permission: %#v", permission)
	}
	certificates := tlsApp["certificates"].(map[string]any)
	loadFiles := certificates["load_files"].([]any)
	if len(loadFiles) != 1 {
		t.Fatalf("expected one static certificate load file entry, got %#v", loadFiles)
	}
	staticCert := loadFiles[0].(map[string]any)
	if staticCert["certificate"] != "/etc/caddy/static-tls/tls.crt" || staticCert["key"] != "/etc/caddy/static-tls/tls.key" {
		t.Fatalf("unexpected static certificate config: %#v", staticCert)
	}
}

func TestCaddyAdminURLMustBeLoopback(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://10.0.0.10:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	err := service.validateConfig()
	if err == nil || !strings.Contains(err.Error(), "localhost") {
		t.Fatalf("expected non-loopback caddy admin URL to be rejected, got %v", err)
	}
}

func TestCaddyModeRequiresEdgeGroupID(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	err := service.validateConfig()
	if err == nil || !strings.Contains(err.Error(), "FUGUE_EDGE_GROUP_ID") {
		t.Fatalf("expected caddy mode without edge group to be rejected, got %v", err)
	}
}

func TestTLSAskOnlyAllowsActiveBundleHosts(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_tls_ask")
	custom := bundle.Routes[0]
	custom.Hostname = "pending.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	custom.Status = model.EdgeRouteStatusUnavailable
	custom.StatusReason = "custom domain ownership or TLS verification is pending"
	custom.UpstreamURL = ""
	custom.RouteGeneration = "routegen_tls_ask_custom"
	bundle.Routes = append(bundle.Routes, custom)

	disabled := bundle.Routes[0]
	disabled.Hostname = "disabled.fugue.pro"
	disabled.Status = model.EdgeRouteStatusDisabled
	bundle.Routes = append(bundle.Routes, disabled)

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_tls_ask"`, now, false)

	for _, tc := range []struct {
		name string
		host string
		code int
	}{
		{name: "active", host: "demo.fugue.pro", code: http.StatusOK},
		{name: "pending custom domain", host: "pending.customer.com", code: http.StatusOK},
		{name: "unknown", host: "unknown.fugue.pro", code: http.StatusForbidden},
		{name: "disabled", host: "disabled.fugue.pro", code: http.StatusForbidden},
		{name: "missing", host: "", code: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/edge/tls/ask?domain="+url.QueryEscape(tc.host), nil)
			recorder := httptest.NewRecorder()
			service.Handler().ServeHTTP(recorder, req)
			if recorder.Code != tc.code {
				t.Fatalf("expected %d, got %d body=%s", tc.code, recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestCaddyTLSModeMustBeKnown(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyTLSMode:         "public",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	err := service.validateConfig()
	if err == nil || !strings.Contains(err.Error(), "FUGUE_EDGE_CADDY_TLS_MODE") {
		t.Fatalf("expected unknown caddy tls mode to be rejected, got %v", err)
	}
}

func TestCaddyStaticTLSFilesMustBePairedAndRequireTLS(t *testing.T) {
	t.Parallel()

	base := config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyTLSMode:         caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}
	onlyCert := base
	onlyCert.CaddyStaticTLSCertFile = "/etc/caddy/static-tls/tls.crt"
	if err := NewService(onlyCert, log.New(ioDiscard{}, "", 0)).validateConfig(); err == nil || !strings.Contains(err.Error(), "must be configured together") {
		t.Fatalf("expected unpaired static TLS file to be rejected, got %v", err)
	}

	offTLS := base
	offTLS.CaddyTLSMode = caddyTLSModeOff
	offTLS.CaddyStaticTLSCertFile = "/etc/caddy/static-tls/tls.crt"
	offTLS.CaddyStaticTLSKeyFile = "/etc/caddy/static-tls/tls.key"
	if err := NewService(offTLS, log.New(ioDiscard{}, "", 0)).validateConfig(); err == nil || !strings.Contains(err.Error(), "requires FUGUE_EDGE_CADDY_TLS_MODE") {
		t.Fatalf("expected static TLS with TLS off to be rejected, got %v", err)
	}
}

func TestProxyHandlerRoutesPlatformAndCustomDomainsWithStreamingMetrics(t *testing.T) {
	t.Parallel()

	var uploadRemoteAddr string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Fugue-Edge-App-ID") != "app_demo" {
			t.Fatalf("expected app id header, got %q", r.Header.Get("X-Fugue-Edge-App-ID"))
		}
		switch r.URL.Path {
		case "/upload":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read upload body: %v", err)
			}
			uploadRemoteAddr = r.RemoteAddr
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, "uploaded:%s:%s", r.Header.Get("X-Forwarded-Host"), body)
		case "/events":
			if !r.Close {
				t.Errorf("expected streaming upload to use a fresh origin connection")
			}
			if r.RemoteAddr == uploadRemoteAddr {
				t.Errorf("expected streaming upload to avoid reusing origin connection %s", r.RemoteAddr)
			}
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "data: ready\n\n")
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()

	bundle := testBundle("routegen_proxy")
	bundle.Routes[0].UpstreamURL = backend.URL
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	bundle.Routes = append(bundle.Routes, custom)

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_proxy"`, now, false)

	upload := httptest.NewRecorder()
	uploadReq := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/upload", strings.NewReader("payload"))
	service.ProxyHandler().ServeHTTP(upload, uploadReq)
	if upload.Code != http.StatusCreated || upload.Body.String() != "uploaded:demo.fugue.pro:payload" {
		t.Fatalf("unexpected upload response status=%d body=%q", upload.Code, upload.Body.String())
	}

	events := httptest.NewRecorder()
	eventsReq := httptest.NewRequest(http.MethodPost, "http://www.customer.com/events", strings.NewReader("payload"))
	eventsReq.Header.Set("Accept", "text/event-stream")
	service.ProxyHandler().ServeHTTP(events, eventsReq)
	if events.Code != http.StatusOK || !strings.Contains(events.Body.String(), "data: ready") {
		t.Fatalf("unexpected sse response status=%d body=%q", events.Code, events.Body.String())
	}

	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_route_requests_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 1`,
		`fugue_edge_route_requests_total{hostname="www.customer.com",path_prefix="/",app="app_demo",route_kind="custom-domain"} 1`,
		`fugue_edge_route_status_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform",status="201"} 1`,
		`fugue_edge_route_status_total{hostname="www.customer.com",path_prefix="/",app="app_demo",route_kind="custom-domain",status="200"} 1`,
		`fugue_edge_route_upload_requests_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 1`,
		`fugue_edge_route_sse_total{hostname="www.customer.com",path_prefix="/",app="app_demo",route_kind="custom-domain",result="success"} 1`,
		`fugue_edge_route_streaming_total{hostname="www.customer.com",path_prefix="/",app="app_demo",route_kind="custom-domain",result="success"} 1`,
		`fugue_edge_route_upstream_latency_seconds_count{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 1`,
		`fugue_edge_route_upstream_latency_seconds_count{hostname="www.customer.com",path_prefix="/",app="app_demo",route_kind="custom-domain"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestProxyHandlerPreservesCaddyForwardedFor(t *testing.T) {
	t.Parallel()

	var gotForwardedFor string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/whoami" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		gotForwardedFor = r.Header.Get("X-Forwarded-For")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()

	bundle := testBundle("routegen_forwarded_for")
	bundle.Routes[0].UpstreamURL = backend.URL

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_forwarded_for"`, time.Now().UTC(), false)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/whoami", nil)
	req.RemoteAddr = "127.0.0.1:54321"
	req.Header.Set("X-Forwarded-For", "203.0.113.250, 198.51.100.42")
	service.ProxyHandler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("unexpected response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	if want := "198.51.100.42, 127.0.0.1"; gotForwardedFor != want {
		t.Fatalf("expected X-Forwarded-For %q, got %q", want, gotForwardedFor)
	}
}

func TestProxyHandlerCachesStaticAssets(t *testing.T) {
	t.Parallel()

	var upstreamHits atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		if r.URL.Path != "/_next/static/chunks/app.js" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/javascript")
		time.Sleep(10 * time.Millisecond)
		_, _ = fmt.Fprintf(w, "console.log(%d)", upstreamHits.Load())
	}))
	defer backend.Close()

	bundle := testBundle("routegen_cache")
	bundle.Routes[0].UpstreamURL = backend.URL
	bundle.Routes[0].CachePolicyID = "static-assets-immutable-v1"
	bundle.Routes[0].CacheNamespace = "app_demo_deploy_1"
	bundle.CachePolicies = []model.CachePolicy{
		{
			ID:                    "static-assets-immutable-v1",
			Kind:                  model.CachePolicyKindStaticAssets,
			PathPatterns:          []string{"/_next/static/*", "*.js"},
			MethodAllowlist:       []string{http.MethodGet, http.MethodHead},
			StatusAllowlist:       []int{http.StatusOK},
			TTLSeconds:            31536000,
			EdgeCacheControl:      "public, max-age=31536000, immutable",
			BypassOnAuthorization: true,
			VaryAllowlist:         []string{"Accept-Encoding"},
			PurgeMode:             model.CachePolicyPurgeModeGeneration,
		},
	}

	service := NewService(config.EdgeConfig{
		APIURL:             "https://api.example.invalid",
		EdgeToken:          "edge-secret",
		AssetCachePath:     filepath.Join(t.TempDir(), "http-cache"),
		AssetCacheMaxBytes: 1024 * 1024,
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_cache"`, time.Now().UTC(), false)

	first := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/_next/static/chunks/app.js", nil)
	service.ProxyHandler().ServeHTTP(first, firstReq)
	if first.Code != http.StatusOK || first.Header().Get("X-Fugue-Cache") != "miss" {
		t.Fatalf("expected first request to miss cache, got status=%d cache=%q body=%q", first.Code, first.Header().Get("X-Fugue-Cache"), first.Body.String())
	}
	if timing := strings.Join(first.Header().Values("Server-Timing"), ","); !strings.Contains(timing, "fugue_cache_lookup") || !strings.Contains(timing, "fugue_origin_connect") || !strings.Contains(timing, "fugue_origin_ttfb") {
		t.Fatalf("expected first response to expose cache and origin timing, got %q", timing)
	}

	second := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/_next/static/chunks/app.js", nil)
	service.ProxyHandler().ServeHTTP(second, secondReq)
	if second.Code != http.StatusOK || second.Header().Get("X-Fugue-Cache") != "hit" {
		t.Fatalf("expected second request to hit cache, got status=%d cache=%q body=%q", second.Code, second.Header().Get("X-Fugue-Cache"), second.Body.String())
	}
	if timing := strings.Join(second.Header().Values("Server-Timing"), ","); !strings.Contains(timing, "fugue_cache_lookup") || strings.Contains(timing, "fugue_origin_connect") || strings.Contains(timing, "fugue_origin_ttfb") {
		t.Fatalf("expected cached response to expose only cache timing, got %q", timing)
	}
	if upstreamHits.Load() != 1 {
		t.Fatalf("expected one upstream hit after cache hit, got %d", upstreamHits.Load())
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("cached response body changed: first=%q second=%q", first.Body.String(), second.Body.String())
	}

	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_route_cache_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform",cache_status="hit",cache_policy_id="static-assets-immutable-v1",asset_class="next_static"} 1`,
		`fugue_edge_route_cache_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform",cache_status="miss",cache_policy_id="static-assets-immutable-v1",asset_class="next_static"} 1`,
		`fugue_edge_route_cache_lookup_duration_seconds_count{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 2`,
		`fugue_edge_route_origin_connect_duration_seconds_count{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 1`,
		`fugue_edge_route_origin_ttfb_seconds_count{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 1`,
		`fugue_edge_route_origin_total_duration_seconds_count{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 1`,
		`fugue_edge_route_response_write_duration_seconds_count{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 2`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestProxyHandlerReusesOriginConnections(t *testing.T) {
	t.Parallel()

	var upstreamHits atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	bundle := testBundle("routegen_reuse")
	bundle.Routes[0].UpstreamURL = backend.URL

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_reuse"`, time.Now().UTC(), false)

	first := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(first, firstReq)
	if first.Code != http.StatusOK || first.Body.String() != "ok" {
		t.Fatalf("unexpected first response status=%d body=%q", first.Code, first.Body.String())
	}
	if timing := strings.Join(first.Header().Values("Server-Timing"), ","); !strings.Contains(timing, "fugue_origin_connect") || !strings.Contains(timing, "fugue_origin_ttfb") {
		t.Fatalf("expected first response to include origin connect and TTFB timing, got %q", timing)
	}

	second := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(second, secondReq)
	if second.Code != http.StatusOK || second.Body.String() != "ok" {
		t.Fatalf("unexpected second response status=%d body=%q", second.Code, second.Body.String())
	}
	if timing := strings.Join(second.Header().Values("Server-Timing"), ","); strings.Contains(timing, "fugue_origin_connect") || !strings.Contains(timing, "fugue_origin_ttfb") {
		t.Fatalf("expected reused origin connection to skip connect timing and keep TTFB timing, got %q", timing)
	}
	if upstreamHits.Load() != 2 {
		t.Fatalf("expected both uncached requests to reach upstream, got %d", upstreamHits.Load())
	}

	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_route_origin_connect_duration_seconds_count{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 1`,
		`fugue_edge_route_origin_ttfb_seconds_count{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 2`,
		`fugue_edge_route_origin_total_duration_seconds_count{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 2`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestProxyHandlerCachesHTMLDocumentsWithShortTTL(t *testing.T) {
	t.Parallel()

	var upstreamHits atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/zh-CN" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("RSC") != "" {
			w.Header().Set("Content-Type", "text/x-component")
			_, _ = fmt.Fprintf(w, "rsc shell %d", upstreamHits.Add(1))
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Vary", "RSC, Next-Router-State-Tree, Next-Router-Prefetch, Next-Router-Segment-Prefetch, Accept-Encoding")
		_, _ = fmt.Fprintf(w, "<!doctype html><title>shell %d</title>", upstreamHits.Add(1))
	}))
	defer backend.Close()

	bundle := testBundle("routegen_html_cache")
	bundle.Routes[0].UpstreamURL = backend.URL
	bundle.Routes[0].CachePolicyID = "static-assets-immutable-v1"
	bundle.Routes[0].CacheNamespace = "app_demo_deploy_1"
	bundle.CachePolicies = []model.CachePolicy{
		{
			ID:                    "static-assets-immutable-v1",
			Kind:                  model.CachePolicyKindStaticAssets,
			PathPatterns:          []string{"/_next/static/*", "*.js"},
			MethodAllowlist:       []string{http.MethodGet, http.MethodHead},
			StatusAllowlist:       []int{http.StatusOK},
			TTLSeconds:            31536000,
			EdgeCacheControl:      "public, max-age=31536000, immutable",
			BypassOnAuthorization: true,
			VaryAllowlist:         []string{"Accept-Encoding"},
			PurgeMode:             model.CachePolicyPurgeModeGeneration,
		},
		{
			ID:                          "html-documents-short-v1",
			Kind:                        model.CachePolicyKindHTMLDocuments,
			PathPatterns:                []string{"/", "/index.html", "*.html"},
			MethodAllowlist:             []string{http.MethodGet, http.MethodHead},
			StatusAllowlist:             []int{http.StatusOK},
			TTLSeconds:                  60,
			StaleWhileRevalidateSeconds: 300,
			EdgeCacheControl:            "public, max-age=60, stale-while-revalidate=300",
			BypassOnAuthorization:       true,
			BypassOnCookie:              true,
			VaryAllowlist:               nextDocumentVaryAllowlist,
			PurgeMode:                   model.CachePolicyPurgeModeGeneration,
		},
	}

	service := NewService(config.EdgeConfig{
		APIURL:             "https://api.example.invalid",
		EdgeToken:          "edge-secret",
		AssetCachePath:     filepath.Join(t.TempDir(), "http-cache"),
		AssetCacheMaxBytes: 1024 * 1024,
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_html_cache"`, time.Now().UTC(), false)

	first := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(first, firstReq)
	if first.Code != http.StatusOK || first.Header().Get("X-Fugue-Cache") != "miss" {
		t.Fatalf("expected first HTML request to miss cache, got status=%d cache=%q body=%q", first.Code, first.Header().Get("X-Fugue-Cache"), first.Body.String())
	}

	second := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(second, secondReq)
	if second.Code != http.StatusOK || second.Header().Get("X-Fugue-Cache") != "hit" {
		t.Fatalf("expected second HTML request to hit cache, got status=%d cache=%q body=%q", second.Code, second.Header().Get("X-Fugue-Cache"), second.Body.String())
	}
	if first.Body.String() != second.Body.String() || upstreamHits.Load() != 1 {
		t.Fatalf("expected cached HTML shell, hits=%d first=%q second=%q", upstreamHits.Load(), first.Body.String(), second.Body.String())
	}
	if timing := strings.Join(second.Header().Values("Server-Timing"), ","); !strings.Contains(timing, "fugue_cache_lookup") || strings.Contains(timing, "fugue_origin_ttfb") {
		t.Fatalf("expected HTML cache hit to expose only cache timing, got %q", timing)
	}

	decision := service.edgeCacheDecision(secondReq, bundle.Routes[0])
	entry, ok := service.edgeCacheLoad(decision)
	if !ok {
		t.Fatal("expected cached HTML entry")
	}
	entry.ExpiresAt = time.Now().Add(-time.Second)
	if err := service.edgeCacheStore(decision, entry); err != nil {
		t.Fatalf("mark cached HTML stale: %v", err)
	}
	stale := httptest.NewRecorder()
	staleReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(stale, staleReq)
	if stale.Code != http.StatusOK || stale.Header().Get("X-Fugue-Cache") != "stale" {
		t.Fatalf("expected stale HTML response while revalidating, got status=%d cache=%q body=%q", stale.Code, stale.Header().Get("X-Fugue-Cache"), stale.Body.String())
	}
	if stale.Body.String() != second.Body.String() {
		t.Fatalf("expected stale response to serve previous shell, stale=%q second=%q", stale.Body.String(), second.Body.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	for upstreamHits.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if upstreamHits.Load() != 2 {
		t.Fatalf("expected stale response to trigger one background revalidation, hits=%d", upstreamHits.Load())
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entry, ok = service.edgeCacheLoad(decision)
		if ok && strings.Contains(string(entry.Body), "shell 2") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	entry, ok = service.edgeCacheLoad(decision)
	if !ok || !strings.Contains(string(entry.Body), "shell 2") {
		t.Fatalf("expected background revalidation to refresh cached shell, ok=%t body=%q", ok, string(entry.Body))
	}
	refreshed := httptest.NewRecorder()
	refreshedReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(refreshed, refreshedReq)
	if refreshed.Code != http.StatusOK || refreshed.Header().Get("X-Fugue-Cache") != "hit" {
		t.Fatalf("expected refreshed HTML cache hit, got status=%d cache=%q body=%q", refreshed.Code, refreshed.Header().Get("X-Fugue-Cache"), refreshed.Body.String())
	}
	if !strings.Contains(refreshed.Body.String(), "shell 2") {
		t.Fatalf("expected refreshed cache body from background revalidation, got %q", refreshed.Body.String())
	}

	rsc := httptest.NewRecorder()
	rscReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	rscReq.Header.Set("RSC", "1")
	service.ProxyHandler().ServeHTTP(rsc, rscReq)
	if rsc.Code != http.StatusOK || rsc.Header().Get("X-Fugue-Cache") != "" || upstreamHits.Load() != 3 {
		t.Fatalf("expected Next RSC request to bypass HTML cache, status=%d cache=%q hits=%d body=%q", rsc.Code, rsc.Header().Get("X-Fugue-Cache"), upstreamHits.Load(), rsc.Body.String())
	}
	if rsc.Body.String() != "rsc shell 3" {
		t.Fatalf("expected uncached RSC response, got %q", rsc.Body.String())
	}

	cookie := httptest.NewRecorder()
	cookieReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	cookieReq.Header.Set("Cookie", "session=abc")
	service.ProxyHandler().ServeHTTP(cookie, cookieReq)
	if cookie.Code != http.StatusOK || cookie.Header().Get("X-Fugue-Cache") != "" || upstreamHits.Load() != 4 {
		t.Fatalf("expected cookie request to bypass HTML cache, status=%d cache=%q hits=%d body=%q", cookie.Code, cookie.Header().Get("X-Fugue-Cache"), upstreamHits.Load(), cookie.Body.String())
	}

	localizedFirst := httptest.NewRecorder()
	localizedFirstReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/zh-CN", nil)
	service.ProxyHandler().ServeHTTP(localizedFirst, localizedFirstReq)
	if localizedFirst.Code != http.StatusOK || localizedFirst.Header().Get("X-Fugue-Cache") != "miss" {
		t.Fatalf("expected extensionless localized HTML request to miss cache, got status=%d cache=%q body=%q", localizedFirst.Code, localizedFirst.Header().Get("X-Fugue-Cache"), localizedFirst.Body.String())
	}

	localizedSecond := httptest.NewRecorder()
	localizedSecondReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/zh-CN", nil)
	service.ProxyHandler().ServeHTTP(localizedSecond, localizedSecondReq)
	if localizedSecond.Code != http.StatusOK || localizedSecond.Header().Get("X-Fugue-Cache") != "hit" {
		t.Fatalf("expected extensionless localized HTML request to hit cache, got status=%d cache=%q body=%q", localizedSecond.Code, localizedSecond.Header().Get("X-Fugue-Cache"), localizedSecond.Body.String())
	}
	if localizedFirst.Body.String() != localizedSecond.Body.String() || upstreamHits.Load() != 5 {
		t.Fatalf("expected cached localized HTML shell, hits=%d first=%q second=%q", upstreamHits.Load(), localizedFirst.Body.String(), localizedSecond.Body.String())
	}

	apiDecision := service.edgeCacheDecision(httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/api/status", nil), bundle.Routes[0])
	if apiDecision.Enabled || apiDecision.AssetClass == "html_document" {
		t.Fatalf("expected common API path to stay outside HTML document cache, enabled=%v asset=%q reason=%q", apiDecision.Enabled, apiDecision.AssetClass, apiDecision.Reason)
	}
	v1Decision := service.edgeCacheDecision(httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/v1/chat/completions", nil), bundle.Routes[0])
	if v1Decision.Enabled || v1Decision.AssetClass == "html_document" {
		t.Fatalf("expected common versioned API path to stay outside HTML document cache, enabled=%v asset=%q reason=%q", v1Decision.Enabled, v1Decision.AssetClass, v1Decision.Reason)
	}

	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_route_cache_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform",cache_status="hit",cache_policy_id="html-documents-short-v1",asset_class="html_document"} 3`,
		`fugue_edge_route_cache_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform",cache_status="miss",cache_policy_id="html-documents-short-v1",asset_class="html_document"} 2`,
		`fugue_edge_route_cache_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform",cache_status="stale",cache_policy_id="html-documents-short-v1",asset_class="html_document"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestProxyHandlerDoesNotCacheNoStoreHTMLDocuments(t *testing.T) {
	t.Parallel()

	var upstreamHits atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = fmt.Fprintf(w, "<!doctype html><title>dynamic %d</title>", upstreamHits.Add(1))
	}))
	defer backend.Close()

	bundle := testBundle("routegen_html_no_store")
	bundle.Routes[0].UpstreamURL = backend.URL
	bundle.Routes[0].CachePolicyID = "static-assets-immutable-v1"
	bundle.Routes[0].CacheNamespace = "app_demo_deploy_1"
	bundle.CachePolicies = []model.CachePolicy{
		{
			ID:                    "static-assets-immutable-v1",
			Kind:                  model.CachePolicyKindStaticAssets,
			PathPatterns:          []string{"/_next/static/*"},
			MethodAllowlist:       []string{http.MethodGet, http.MethodHead},
			StatusAllowlist:       []int{http.StatusOK},
			TTLSeconds:            31536000,
			BypassOnAuthorization: true,
			VaryAllowlist:         []string{"Accept-Encoding"},
			PurgeMode:             model.CachePolicyPurgeModeGeneration,
		},
		{
			ID:                    "html-documents-short-v1",
			Kind:                  model.CachePolicyKindHTMLDocuments,
			PathPatterns:          []string{"/"},
			MethodAllowlist:       []string{http.MethodGet, http.MethodHead},
			StatusAllowlist:       []int{http.StatusOK},
			TTLSeconds:            60,
			EdgeCacheControl:      "public, max-age=60, stale-while-revalidate=300",
			BypassOnAuthorization: true,
			BypassOnCookie:        true,
			VaryAllowlist:         []string{"Accept-Encoding"},
			PurgeMode:             model.CachePolicyPurgeModeGeneration,
		},
	}

	service := NewService(config.EdgeConfig{
		APIURL:             "https://api.example.invalid",
		EdgeToken:          "edge-secret",
		AssetCachePath:     filepath.Join(t.TempDir(), "http-cache"),
		AssetCacheMaxBytes: 1024 * 1024,
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_html_no_store"`, time.Now().UTC(), false)

	first := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(first, firstReq)
	second := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(second, secondReq)

	if upstreamHits.Load() != 2 || first.Header().Get("X-Fugue-Cache") != "bypass" || second.Header().Get("X-Fugue-Cache") != "bypass" {
		t.Fatalf("expected no-store HTML to bypass cache twice, hits=%d firstCache=%q secondCache=%q", upstreamHits.Load(), first.Header().Get("X-Fugue-Cache"), second.Header().Get("X-Fugue-Cache"))
	}
	if first.Header().Get("Cache-Control") != "no-store" || second.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected origin no-store cache-control to survive, first=%q second=%q", first.Header().Get("Cache-Control"), second.Header().Get("Cache-Control"))
	}
	if first.Body.String() == second.Body.String() {
		t.Fatalf("expected dynamic no-store HTML to be fetched twice, first=%q second=%q", first.Body.String(), second.Body.String())
	}
}

func TestProxyHandlerIgnoresRouteAOnlyHosts(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	bundle := testBundle("routegen_route_a_only_proxy")
	bundle.Routes[0].UpstreamURL = backend.URL
	routeAOnly := bundle.Routes[0]
	routeAOnly.Hostname = "legacy.fugue.pro"
	routeAOnly.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
	routeAOnly.RouteGeneration = "routegen_legacy"
	bundle.Routes = append(bundle.Routes, routeAOnly)

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_route_a_only_proxy"`, time.Now().UTC(), false)

	canary := httptest.NewRecorder()
	canaryReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(canary, canaryReq)
	if canary.Code != http.StatusOK || canary.Body.String() != "ok" {
		t.Fatalf("unexpected canary response status=%d body=%q", canary.Code, canary.Body.String())
	}

	legacy := httptest.NewRecorder()
	legacyReq := httptest.NewRequest(http.MethodGet, "http://legacy.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(legacy, legacyReq)
	if legacy.Code != http.StatusNotFound {
		t.Fatalf("expected Route A-only route to be ignored by edge proxy, got %d body=%s", legacy.Code, legacy.Body.String())
	}
}

func TestProxyHandlerServesDifferentEdgeGroupRoutes(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	bundle := testBundle("routegen_edge_group_proxy")
	bundle.Routes[0].EdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].RuntimeEdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].PolicyEdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].UpstreamURL = backend.URL
	hkRoute := bundle.Routes[0]
	hkRoute.Hostname = "hk.fugue.pro"
	hkRoute.EdgeGroupID = "edge-group-country-hk"
	hkRoute.RuntimeEdgeGroupID = "edge-group-country-hk"
	hkRoute.PolicyEdgeGroupID = "edge-group-country-hk"
	hkRoute.RouteGeneration = "routegen_hk"
	remoteRuntimeRoute := bundle.Routes[0]
	remoteRuntimeRoute.Hostname = "remote-runtime.fugue.pro"
	remoteRuntimeRoute.RuntimeEdgeGroupID = "edge-group-country-hk"
	remoteRuntimeRoute.PolicyEdgeGroupID = "edge-group-country-us"
	remoteRuntimeRoute.RouteGeneration = "routegen_remote_runtime"
	bundle.Routes = append(bundle.Routes, hkRoute, remoteRuntimeRoute)

	service := NewService(config.EdgeConfig{
		APIURL:      "https://api.example.invalid",
		EdgeToken:   "edge-secret",
		EdgeGroupID: "edge-group-country-us",
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_edge_group_proxy"`, time.Now().UTC(), false)

	local := httptest.NewRecorder()
	localReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(local, localReq)
	if local.Code != http.StatusOK || local.Body.String() != "ok" {
		t.Fatalf("unexpected local edge group response status=%d body=%q", local.Code, local.Body.String())
	}

	otherGroup := httptest.NewRecorder()
	otherGroupReq := httptest.NewRequest(http.MethodGet, "http://hk.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(otherGroup, otherGroupReq)
	if otherGroup.Code != http.StatusOK || otherGroup.Body.String() != "ok" {
		t.Fatalf("expected different edge group route to be served as fallback, got %d body=%q", otherGroup.Code, otherGroup.Body.String())
	}

	remoteRuntime := httptest.NewRecorder()
	remoteRuntimeReq := httptest.NewRequest(http.MethodGet, "http://remote-runtime.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(remoteRuntime, remoteRuntimeReq)
	if remoteRuntime.Code != http.StatusOK || remoteRuntime.Body.String() != "ok" {
		t.Fatalf("expected nearest-edge fallback route to be served by local edge group, got %d body=%s", remoteRuntime.Code, remoteRuntime.Body.String())
	}
}

func TestProxyHandlerReturnsUnavailableForInactiveRoute(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_disabled")
	bundle.Routes[0].Status = model.EdgeRouteStatusDisabled
	bundle.Routes[0].StatusReason = "app is disabled"

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_disabled"`, now, false)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected disabled route to return 503, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_route_status_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform",status="503"} 1`,
		`fugue_edge_route_fallback_hits_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestProxyHandlerRecordsUpstreamErrors(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	backendURL := backend.URL
	backend.Close()

	bundle := testBundle("routegen_upstream_error")
	bundle.Routes[0].UpstreamURL = backendURL

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_upstream_error"`, now, false)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("expected upstream error to return 502, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_route_status_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform",status="502"} 1`,
		`fugue_edge_route_upstream_errors_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestProxyHandlerClassifiesClientCanceledOriginAs499(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_client_canceled")
	bundle.Routes[0].UpstreamURL = "http://origin.example.invalid"

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	service.proxyBase = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, context.Canceled
	})
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_client_canceled"`, now, false)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/v1/images/generations", strings.NewReader("payload"))
	service.ProxyHandler().ServeHTTP(recorder, req)
	if recorder.Code != edgeStatusClientClosedRequest {
		t.Fatalf("expected client-canceled origin request to return 499, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_route_status_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform",status="499"} 1`,
		`fugue_edge_route_upload_requests_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
	if strings.Contains(metrics, `fugue_edge_route_upstream_errors_total{hostname="demo.fugue.pro"`) {
		t.Fatalf("client cancellation should not increment upstream error metrics:\n%s", metrics)
	}
}

func TestProxyHandlerSupportsWebSocket(t *testing.T) {
	t.Parallel()

	backendErrors := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			backendErrors <- err
			return
		}
		defer conn.Close()
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			backendErrors <- err
			return
		}
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			backendErrors <- err
			return
		}
		if string(payload) != "ping" {
			backendErrors <- fmt.Errorf("unexpected websocket payload %q", string(payload))
			return
		}
		if err := conn.WriteMessage(messageType, []byte("pong")); err != nil {
			backendErrors <- err
			return
		}
		backendErrors <- nil
	}))
	defer backend.Close()

	bundle := testBundle("routegen_websocket")
	bundle.Routes[0].UpstreamURL = backend.URL
	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_websocket"`, now, false)

	proxy := httptest.NewServer(service.ProxyHandler())
	defer proxy.Close()
	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy url: %v", err)
	}
	dialer := websocket.Dialer{
		NetDialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, proxyURL.Host)
		},
	}

	conn, resp, err := dialer.Dial("ws://demo.fugue.pro/ws", nil)
	if err != nil {
		responseBody := ""
		if resp != nil && resp.Body != nil {
			bodyBytes, _ := io.ReadAll(resp.Body)
			responseBody = string(bodyBytes)
			resp.Body.Close()
		}
		t.Fatalf("dial proxied websocket: %v body=%s", err, responseBody)
	}
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set websocket read deadline: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write websocket message: %v", err)
	}
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket response: %v", err)
	}
	if string(payload) != "pong" {
		t.Fatalf("expected websocket payload pong, got %q", string(payload))
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close websocket: %v", err)
	}
	select {
	case err := <-backendErrors:
		if err != nil {
			t.Fatalf("backend websocket validation failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for backend websocket validation")
	}

	wantMetric := `fugue_edge_route_websocket_total{hostname="demo.fugue.pro",path_prefix="/",app="app_demo",route_kind="platform",result="success"} 1`
	deadline := time.Now().Add(2 * time.Second)
	for {
		metrics := renderMetrics(t, service)
		if strings.Contains(metrics, wantMetric) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected websocket 101 metric, got %s", metrics)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func testBundle(version string) model.EdgeRouteBundle {
	now := time.Date(2026, 5, 10, 1, 2, 3, 0, time.UTC)
	return model.EdgeRouteBundle{
		Version:     version,
		GeneratedAt: now,
		Routes: []model.EdgeRouteBinding{
			{
				Hostname:        "demo.fugue.pro",
				RouteKind:       model.EdgeRouteKindPlatform,
				AppID:           "app_demo",
				TenantID:        "tenant_demo",
				RuntimeID:       model.DefaultManagedRuntimeID,
				EdgeGroupID:     "edge-group-default",
				RoutePolicy:     model.EdgeRoutePolicyCanary,
				UpstreamKind:    model.EdgeRouteUpstreamKindKubernetesService,
				UpstreamURL:     "http://demo.fg-tenant-demo.svc.cluster.local:8080",
				ServicePort:     8080,
				TLSPolicy:       model.EdgeRouteTLSPolicyPlatform,
				Streaming:       true,
				Status:          model.EdgeRouteStatusActive,
				RouteGeneration: "routegen_demo",
				CreatedAt:       now,
				UpdatedAt:       now,
			},
		},
		TLSAllowlist: []model.EdgeTLSAllowlistEntry{
			{
				Hostname: "www.example.com",
				AppID:    "app_demo",
				TenantID: "tenant_demo",
				Status:   model.AppDomainStatusVerified,
			},
		},
	}
}

func writeLocalCaddyTLSBundle(t *testing.T, dataDir, hostname, certPEM, keyPEM, metadataJSON string) {
	t.Helper()
	hostDir := filepath.Join(dataDir, "certificates", defaultCaddyIssuerStorage, hostname)
	if err := os.MkdirAll(hostDir, 0o700); err != nil {
		t.Fatalf("create local caddy cert dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(hostDir, hostname+".crt"), []byte(certPEM+"\n"), 0o644); err != nil {
		t.Fatalf("write local caddy cert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(hostDir, hostname+".key"), []byte(keyPEM+"\n"), 0o600); err != nil {
		t.Fatalf("write local caddy key: %v", err)
	}
	if err := os.WriteFile(filepath.Join(hostDir, hostname+".json"), []byte(metadataJSON+"\n"), 0o644); err != nil {
		t.Fatalf("write local caddy metadata: %v", err)
	}
}

func writeTestCache(t *testing.T, path string, bundle model.EdgeRouteBundle, etag string) {
	t.Helper()
	cached := cacheFile{
		Version:  cacheFileVersion,
		ETag:     etag,
		CachedAt: time.Date(2026, 5, 10, 2, 0, 0, 0, time.UTC),
		Bundle:   bundle,
	}
	data, err := json.MarshalIndent(cached, "", "  ")
	if err != nil {
		t.Fatalf("marshal cache: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create cache dir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write cache: %v", err)
	}
}

func renderMetrics(t *testing.T, service *Service) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected metrics status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	return recorder.Body.String()
}

func warmupTestResponse(status int, contentType, body string) *http.Response {
	resp := &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	resp.Header.Set("Content-Type", contentType)
	return resp
}

func countString(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}
