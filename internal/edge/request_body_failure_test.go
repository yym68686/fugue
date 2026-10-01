package edge

import (
	"bufio"
	"bytes"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBufferedPolicyLimitsReturn413And408InsteadOfClientCancel(t *testing.T) {
	var hits atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(204) }))
	defer origin.Close()
	cfg := recoveryConfig(t)
	cfg.RequestBodyBufferTotalMaxBytes = 4096
	s, _ := recoveryService(t, cfg, origin.URL, recoveryPolicy(4))
	r, _ := recoveryRequest([]byte("12345"), "text/event-stream", "application/json")
	r.ContentLength = -1
	r.TransferEncoding = []string{"chunked"}
	w := httptest.NewRecorder()
	s.ProxyHandler().ServeHTTP(w, r)
	if w.Code != 413 || hits.Load() != 0 {
		t.Fatalf("status=%d hits=%d", w.Code, hits.Load())
	}
	recoveryNoReservation(t, s)
	edge := httptest.NewServer(s.ProxyHandler())
	defer edge.Close()
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(edge.URL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(4 * time.Second))
	io.WriteString(conn, "POST /v1/tasks HTTP/1.1\r\nHost: app.example.test\r\nContent-Type: application/json\r\nAccept: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1\r\nx\r\n")
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "POST"})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 408 || hits.Load() != 0 {
		t.Fatalf("status=%d hits=%d", response.StatusCode, hits.Load())
	}
}

func TestBodyBufferRecoveryRetainsLimitWithoutRoutePolicy(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "known", true: "chunked"}[unknown], func(t *testing.T) {
			var hits, received atomic.Int64
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				n, _ := io.Copy(io.Discard, r.Body)
				received.Store(n)
				w.WriteHeader(201)
			}))
			defer origin.Close()
			cfg := recoveryConfig(t)
			cfg.RequestBodyBufferMaxBytes = 1024
			s, _ := recoveryService(t, cfg, origin.URL)
			r, _ := recoveryRequest(bytes.Repeat([]byte("x"), 2048), "text/event-stream", "application/json")
			if unknown {
				r.ContentLength = -1
				r.TransferEncoding = []string{"chunked"}
			}
			w := httptest.NewRecorder()
			s.ProxyHandler().ServeHTTP(w, r)
			if w.Code != 413 || received.Load() > 1024 || (!unknown && hits.Load() != 0) {
				t.Fatalf("status=%d hits=%d bytes=%d", w.Code, hits.Load(), received.Load())
			}
			recoveryNoReservation(t, s)
		})
	}
}

func TestBodyBufferSetupFailureStreamsBeforeReading(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if string(b) != "body" {
			t.Errorf("body=%q", b)
		}
		w.WriteHeader(204)
	}))
	defer origin.Close()
	cfg := recoveryConfig(t)
	cfg.RequestBodyBufferTotalMaxBytes = 1024
	cfg.RequestBodyBufferPath = filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(cfg.RequestBodyBufferPath, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	s, l := recoveryService(t, cfg, origin.URL)
	r, _ := recoveryRequest([]byte("body"), "text/event-stream", "application/json")
	w := httptest.NewRecorder()
	s.ProxyHandler().ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatalf("status=%d", w.Code)
	}
	f := recoverySummary(t, l)
	if f["request_body_buffer_reason"] != "directory_create_failed" || f["request_body_buffer_stream_fallback"] != true {
		t.Fatalf("fact=%v", f)
	}
	recoveryNoReservation(t, s)
	b, _ := os.ReadFile(cfg.RequestBodyBufferPath)
	if string(b) != "existing" {
		t.Fatal("existing file altered")
	}
}

type changeBudgetReader struct {
	callback func()
	done     bool
}

func (r *changeBudgetReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	r.callback()
	return copy(p, "payload"), io.EOF
}

func TestBodyBufferMidReadResourceFailureDoesNotReplayOrBlameClient(t *testing.T) {
	var hits atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer origin.Close()
	cfg := recoveryConfig(t)
	cfg.RequestBodyBufferTotalMaxBytes = 1024
	s, l := recoveryService(t, cfg, origin.URL)
	reader := &changeBudgetReader{callback: func() {
		s.bodyBuffer.mu.Lock()
		defer s.bodyBuffer.mu.Unlock()
		s.bodyBuffer.dynamic = true
		s.bodyBuffer.reserveBytes = math.MaxInt64
	}}
	r := httptest.NewRequest("POST", "http://app.example.test/v1/tasks", io.NopCloser(reader))
	r.ContentLength = -1
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "text/event-stream")
	w := httptest.NewRecorder()
	s.ProxyHandler().ServeHTTP(w, r)
	if w.Code != 503 || hits.Load() != 0 {
		t.Fatalf("status=%d hits=%d", w.Code, hits.Load())
	}
	f := recoverySummary(t, l)
	if f["platform_error_class"] != "edge_body_buffer" || f["request_body_buffer_stream_fallback"] != false || f["request_body_buffer_reason"] != "disk_reserve_threshold" {
		t.Fatalf("facts=%v", f)
	}
	if !strings.Contains(l.String(), "client_canceled=false") {
		t.Fatal("resource failure blamed client")
	}
	recoveryNoReservation(t, s)
}

func TestBodyBufferMetricsExposeZeroBudgetAndStreamFallback(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); w.WriteHeader(204) }))
	defer origin.Close()
	s, _ := recoveryService(t, recoveryConfig(t), origin.URL)
	r, _ := recoveryRequest([]byte("body"), "text/event-stream", "application/json")
	s.ProxyHandler().ServeHTTP(httptest.NewRecorder(), r)
	w := httptest.NewRecorder()
	s.handleMetrics(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{"fugue_edge_body_buffer_budget_bytes 0", "fugue_edge_body_buffer_used_bytes 0", `fugue_edge_body_buffer_stream_fallback_total{reason="disk_reserve_threshold"} 1`} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatalf("missing %s", want)
		}
	}
}
