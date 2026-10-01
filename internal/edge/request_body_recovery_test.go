package edge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

// Recovery tests use loopback origins, real filesystem calls and synthetic bodies.
// Reserve above available space without filling the filesystem.
type recoveryLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *recoveryLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *recoveryLog) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

type recoveryReadCounter struct {
	io.ReadCloser
	n atomic.Int64
}

func (r *recoveryReadCounter) Read(p []byte) (int, error) {
	n, e := r.ReadCloser.Read(p)
	r.n.Add(int64(n))
	return n, e
}

func recoveryConfig(t *testing.T) config.EdgeConfig {
	t.Helper()
	return config.EdgeConfig{APIURL: "http://127.0.0.1:1", EdgeToken: "synthetic", RequestBodyBufferPath: t.TempDir(), RequestBodyBufferMaxBytes: 16 << 20, RequestBodyBufferMaxBudgetRatio: 1, RequestBodyBufferReserveBytes: math.MaxInt64, RequestBodyBufferDiskRatio: 0.25, PeerFallbackEnabled: true}
}
func recoveryService(t *testing.T, cfg config.EdgeConfig, origin string, policies ...model.EdgeRequestBodyPolicy) (*Service, *recoveryLog) {
	t.Helper()
	b := testBundle("recovery-bundle")
	b.Routes[0].Hostname = "app.example.test"
	b.Routes[0].AppID = "app_recovery"
	b.Routes[0].DecisionID = "decision-recovery"
	b.Routes[0].UpstreamURL = origin
	b.Routes[0].RequestBodyPolicies = policies
	l := &recoveryLog{}
	s := NewService(cfg, log.New(l, "", 0))
	s.recordSyncSuccess(b, `"recovery-bundle"`, time.Now().UTC(), false)
	t.Cleanup(s.closeEdgeProxyTransports)
	return s, l
}
func recoveryRequest(body []byte, accept, contentType string) (*http.Request, *recoveryReadCounter) {
	r := httptest.NewRequest("POST", "http://app.example.test/v1/tasks", bytes.NewReader(body))
	r.Header.Set("Accept", accept)
	r.Header.Set("Content-Type", contentType)
	r.Header.Set("Authorization", "Bearer synthetic-credential")
	r.Header.Set("Idempotency-Key", "synthetic-idempotency-key")
	c := &recoveryReadCounter{ReadCloser: r.Body}
	r.Body = c
	return r, c
}
func recoverySummary(t *testing.T, l *recoveryLog) map[string]any {
	t.Helper()
	var summary map[string]any
	for _, line := range strings.Split(l.String(), "\n") {
		var f map[string]any
		if json.Unmarshal([]byte(line), &f) != nil || f["event_type"] != "request_fact" {
			continue
		}
		if v, ok := f["summary_json"].(string); ok {
			if err := json.Unmarshal([]byte(v), &summary); err != nil {
				t.Fatal(err)
			}
		}
	}
	if summary == nil {
		t.Fatalf("no request fact in logs: %s", l.String())
	}
	return summary
}
func recoveryNoReservation(t *testing.T, s *Service) {
	t.Helper()
	_, used, active := s.bodyBuffer.stats()
	if used != 0 || active != 0 {
		t.Fatalf("reservation leaked: used=%d active=%d", used, active)
	}
	if es, err := os.ReadDir(s.Config.RequestBodyBufferPath); err == nil && len(es) > 0 {
		t.Fatalf("buffer files remain: %v", es)
	}
}
func recoveryPolicy(max int64) model.EdgeRequestBodyPolicy {
	return model.EdgeRequestBodyPolicy{Name: "tasks", Methods: []string{"POST"}, Paths: []string{"/v1/tasks"}, MaxBytes: max, MaxConcurrent: 1, TimeoutSeconds: 1, RetryAfterSeconds: 2}
}

func TestBodyBufferRecoveryAdmissionMatrix(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
		sse  bool
		mode string
		want int
	}{
		{"zero-budget-small-sse", 128, true, "zero", 200},
		{"zero-budget-64k-sse", 64 << 10, true, "zero", 200},
		{"zero-budget-200k-sse", 200 << 10, true, "zero", 200},
		{"zero-budget-256k-sse", 256 << 10, true, "zero", 200},
		{"zero-budget-non-sse", 256 << 10, false, "zero", 200},
		{"healthy-dynamic-sse", 200 << 10, true, "healthy", 200},
		{"missing-path-and-parent", 200 << 10, true, "missing", 200},
		{"existing-parent-missing-leaf", 200 << 10, true, "leaf", 200},
		{"streaming-no-disk-budget", 200 << 10, true, "stream", 200},
		{"streaming-origin-auth-denied", 256 << 10, true, "auth", 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"input":"` + strings.Repeat("x", tc.size-12) + `"}`)
			if !json.Valid(body) || len(body) != tc.size {
				t.Fatalf("invalid fixture: %d", len(body))
			}
			var hits atomic.Int64
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				if r.Header.Get("Authorization") != "Bearer synthetic-credential" || r.Header.Get("Idempotency-Key") != "synthetic-idempotency-key" {
					t.Error("application headers changed")
				}
				if tc.mode == "auth" {
					http.Error(w, "unauthorized", 401)
					return
				}
				got, err := io.ReadAll(r.Body)
				if err != nil || sha256.Sum256(got) != sha256.Sum256(body) {
					t.Errorf("origin body mismatch: len=%d err=%v", len(got), err)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: ok\n\n")
			}))
			defer origin.Close()
			cfg := recoveryConfig(t)
			switch tc.mode {
			case "healthy":
				cfg.RequestBodyBufferReserveBytes = 0
			case "missing":
				cfg.RequestBodyBufferReserveBytes = 0
				cfg.RequestBodyBufferPath = filepath.Join(cfg.RequestBodyBufferPath, "missing-parent", "body")
			case "leaf":
				cfg.RequestBodyBufferReserveBytes = 0
				cfg.RequestBodyBufferPath = filepath.Join(cfg.RequestBodyBufferPath, "body")
			case "stream", "auth":
				cfg.RequestBodyBufferMaxBytes = 0
			}
			s, l := recoveryService(t, cfg, origin.URL)
			accept := "application/json"
			if tc.sse {
				accept = "text/event-stream"
			}
			r, reads := recoveryRequest(body, accept, "application/json")
			w := httptest.NewRecorder()
			s.ProxyHandler().ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%q logs=%s", w.Code, tc.want, w.Body.String(), l.String())
			}
			fact := recoverySummary(t, l)
			if hits.Load() != 1 {
				t.Fatalf("origin hits=%d", hits.Load())
			}
			if (tc.mode == "zero" && tc.sse) || tc.mode == "missing" {
				wantReason := "disk_reserve_threshold"
				if tc.mode == "missing" {
					wantReason = "filesystem_stat_failed"
				}
				if fact["request_body_buffer_stream_fallback"] != true || fact["request_body_buffer_reason"] != wantReason || fact["platform_error_class"] != "" {
					t.Fatalf("recovery facts=%v", fact)
				}
				snapshot, ok := fact["request_body_buffer_budget_snapshot"].(map[string]any)
				if !ok || snapshot["budget_bytes"] != float64(0) || snapshot["used_bytes"] != float64(0) || snapshot["active_requests"] != float64(0) {
					t.Fatalf("snapshot=%v", snapshot)
				}
				if tc.mode == "missing" {
					if _, known := snapshot["available_bytes"]; known {
						t.Fatal("unknown available fabricated")
					}
				} else if snapshot["available_bytes"] == nil {
					t.Fatal("missing available measurement")
				}
			}

			recoveryNoReservation(t, s)
			t.Logf("RESULT mode=%s size=%d sse=%t status=%d origin_hits=%d read_bytes=%d cache=%v error=%v", tc.mode, len(body), tc.sse, w.Code, hits.Load(), reads.n.Load(), fact["cache_status"], fact["request_body_buffer_error"])
		})
	}
}

func TestBodyBufferRecoveryRetryAndRecovery(t *testing.T) {
	var hits atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(204)
	}))
	defer origin.Close()
	cfg := recoveryConfig(t)
	s, _ := recoveryService(t, cfg, origin.URL)
	for i := 0; i < 30; i++ {
		r, _ := recoveryRequest([]byte(`{"input":"x"}`), "text/event-stream", "application/json")
		w := httptest.NewRecorder()
		s.ProxyHandler().ServeHTTP(w, r)
		if w.Code != 204 {
			t.Fatalf("attempt %d status=%d", i, w.Code)
		}
		recoveryNoReservation(t, s)
	}
	if hits.Load() != 30 {
		t.Fatal("streaming recovery lost requests")
	}
	// Change only local test resource configuration, not code or live nodes.
	s.bodyBuffer.mu.Lock()
	s.bodyBuffer.reserveBytes = 0
	s.bodyBuffer.mu.Unlock()
	r, _ := recoveryRequest([]byte(`{"input":"x"}`), "text/event-stream", "application/json")
	w := httptest.NewRecorder()
	s.ProxyHandler().ServeHTTP(w, r)
	if w.Code != 204 || hits.Load() != 31 {
		t.Fatalf("dynamic recovery status=%d hits=%d", w.Code, hits.Load())
	}
	recoveryNoReservation(t, s)
	t.Log("RESULT 30 requests streamed with zero occupancy; restored budget resumes spooling on same Service")
}

func TestBodyBufferRecoveryConcurrentBudget(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); w.WriteHeader(204) }))
	defer origin.Close()
	cfg := recoveryConfig(t)
	cfg.RequestBodyBufferTotalMaxBytes = 128 << 10
	s, l := recoveryService(t, cfg, origin.URL)
	reservation, err := s.bodyBuffer.reserve(96 << 10)
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.release()
	r, _ := recoveryRequest(bytes.Repeat([]byte("x"), 64<<10), "text/event-stream", "application/json")
	w := httptest.NewRecorder()
	s.ProxyHandler().ServeHTTP(w, r)
	fact := recoverySummary(t, l)
	if w.Code != 204 || fact["request_body_buffer_reason"] != "budget_in_use" {
		t.Fatalf("status=%d fact=%v", w.Code, fact)
	}
	if s.bodyBuffer.usedBytes() != 96<<10 {
		t.Fatal("recovery released another request reservation")
	}
	reservation.release()
	reservation.release()
	recoveryNoReservation(t, s)
}

func TestBodyBufferRecoveryEnvDisableIsSupported(t *testing.T) {
	t.Setenv("FUGUE_EDGE_REQUEST_BODY_BUFFER_MAX_BYTES", "0")
	cfg := config.EdgeFromEnv()
	if cfg.RequestBodyBufferMaxBytes != 0 {
		t.Fatalf("env zero overwritten: %d", cfg.RequestBodyBufferMaxBytes)
	}
	t.Log("RESULT existing environment loader accepts MAX_BYTES=0 as disabled spooling")
}

func TestBodyBufferRecoveryStreamingStartsBeforeUploadCompletes(t *testing.T) {
	first := make(chan struct{})
	received := make(chan string, 1)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 1)
		if _, e := io.ReadFull(r.Body, b); e != nil {
			t.Error(e)
			return
		}
		close(first)
		rest, e := io.ReadAll(r.Body)
		if e != nil {
			t.Error(e)
		}
		received <- string(append(b, rest...))
		w.WriteHeader(204)
	}))
	defer origin.Close()
	cfg := recoveryConfig(t)
	// Retain spooling configuration: zero budget must recover into bounded streaming.
	s, _ := recoveryService(t, cfg, origin.URL)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	r := httptest.NewRequest("POST", "http://app.example.test/v1/tasks", reader)
	r.ContentLength = -1
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "text/event-stream")
	done := make(chan int, 1)
	go func() { w := httptest.NewRecorder(); s.ProxyHandler().ServeHTTP(w, r); done <- w.Code }()
	if _, e := writer.Write([]byte("{")); e != nil {
		t.Fatal(e)
	}
	select {
	case <-first:
	case <-time.After(3 * time.Second):
		t.Fatal("origin did not receive prefix before upload completed")
	}
	writer.Write([]byte(`"input":"x"}`))
	writer.Close()
	select {
	case code := <-done:
		if code != 204 {
			t.Fatalf("status=%d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("proxy did not finish")
	}
	if got := <-received; got != `{"input":"x"}` {
		t.Fatalf("body=%q", got)
	}
	recoveryNoReservation(t, s)
	t.Log("RESULT origin received first byte while client upload was still open")
}

func TestBodyBufferRecoverySSEFlushesBeforeOriginFinishes(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-release
		fmt.Fprint(w, "data: last\n\n")
	}))
	defer origin.Close()
	defer unblock()
	cfg := recoveryConfig(t)
	// Retain spooling configuration: zero budget must recover into bounded streaming.
	s, _ := recoveryService(t, cfg, origin.URL)
	edge := httptest.NewServer(s.ProxyHandler())
	defer edge.Close()
	defer unblock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "POST", edge.URL+"/v1/tasks", strings.NewReader(`{}`))
	r.Host = "app.example.test"
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "text/event-stream")
	resp, e := edge.Client().Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	line, e := br.ReadString('\n')
	if e != nil || line != "data: first\n" {
		t.Fatalf("first SSE line=%q error=%v", line, e)
	}
	unblock()
	rest, e := io.ReadAll(br)
	if e != nil || !strings.Contains(string(rest), "data: last") {
		t.Fatalf("rest=%q error=%v", rest, e)
	}
	t.Log("RESULT client received first SSE event before origin response completed")
}

func TestBodyBufferRecoveryStreamingKeepsExplicitLimits(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown_length_%t", unknown), func(t *testing.T) {
			var hits, received atomic.Int64
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				n, _ := io.Copy(io.Discard, r.Body)
				received.Store(n)
				w.WriteHeader(201)
			}))
			defer origin.Close()
			cfg := recoveryConfig(t)
			// Retain spooling configuration: zero budget must recover into bounded streaming.
			s, _ := recoveryService(t, cfg, origin.URL, recoveryPolicy(4))
			r, _ := recoveryRequest([]byte("12345"), "text/event-stream", "application/json")
			if unknown {
				r.ContentLength = -1
				r.TransferEncoding = []string{"chunked"}
			}
			w := httptest.NewRecorder()
			s.ProxyHandler().ServeHTTP(w, r)
			if w.Code != 413 || received.Load() > 4 || (!unknown && hits.Load() != 0) {
				t.Fatalf("status=%d hits=%d bytes=%d", w.Code, hits.Load(), received.Load())
			}
			guard := s.edgeRequestBodyPolicyGuard("app_recovery", recoveryPolicy(4))
			if len(guard.slots) != 0 {
				t.Fatal("policy slot leaked")
			}
			t.Logf("RESULT status=413 explicit max=4 unknown_length=%t", unknown)
		})
	}
}

func TestBodyBufferRecoveryStreamingHasNoImplicitSizeCap(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); w.WriteHeader(204) }))
	defer origin.Close()
	for _, spool := range []bool{true, false} {
		cfg := recoveryConfig(t)
		cfg.RequestBodyBufferMaxBytes = 32 << 10
		cfg.RequestBodyBufferMaxBudgetRatio = 0
		cfg.RequestBodyBufferTotalMaxBytes = 1 << 20
		if !spool {
			cfg.RequestBodyBufferMaxBytes = 0
		}
		s, _ := recoveryService(t, cfg, origin.URL)
		r, _ := recoveryRequest(bytes.Repeat([]byte("x"), 64<<10), "text/event-stream", "application/json")
		w := httptest.NewRecorder()
		s.ProxyHandler().ServeHTTP(w, r)
		want := 204
		if spool {
			want = 413
		}
		if w.Code != want {
			t.Fatalf("spool=%t status=%d", spool, w.Code)
		}
		t.Logf("RESULT spool=%t policy=none body=64KiB previous_buffer_cap=32KiB status=%d", spool, w.Code)
	}
}

func TestBodyBufferRecoveryStreamingConcurrencyAndCancellation(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		entered <- struct{}{}
		<-release
		w.WriteHeader(204)
	}))
	defer origin.Close()
	defer unblock()
	cfg := recoveryConfig(t)
	// Retain spooling configuration: zero budget must recover into bounded streaming.
	p := recoveryPolicy(1 << 20)
	p.TimeoutSeconds = 5
	s, _ := recoveryService(t, cfg, origin.URL, p)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, _ := recoveryRequest([]byte(`{}`), "text/event-stream", "application/json")
	r = r.WithContext(ctx)
	done := make(chan int, 1)
	go func() { w := httptest.NewRecorder(); s.ProxyHandler().ServeHTTP(w, r); done <- w.Code }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("origin not entered")
	}
	r2, _ := recoveryRequest([]byte(`{}`), "text/event-stream", "application/json")
	w := httptest.NewRecorder()
	s.ProxyHandler().ServeHTTP(w, r2)
	if w.Code != 429 || w.Header().Get("Retry-After") != "2" {
		t.Fatalf("concurrency response=%d headers=%v", w.Code, w.Header())
	}
	cancel()
	select {
	case code := <-done:
		if code != 499 {
			t.Fatalf("cancel status=%d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not return")
	}
	if len(s.edgeRequestBodyPolicyGuard("app_recovery", p).slots) != 0 {
		t.Fatal("cancellation leaked policy slot")
	}
	unblock()
	r3, _ := recoveryRequest([]byte(`{}`), "text/event-stream", "application/json")
	w3 := httptest.NewRecorder()
	s.ProxyHandler().ServeHTTP(w3, r3)
	if w3.Code != 204 {
		t.Fatalf("after cancel=%d", w3.Code)
	}
	recoveryNoReservation(t, s)
	t.Log("RESULT concurrent request=429; canceled request=499; slot released; next request=204")
}

func TestBodyBufferRecoveryStreamingUploadTimeout(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); w.WriteHeader(201) }))
	defer origin.Close()
	cfg := recoveryConfig(t)
	// Retain spooling configuration: zero budget must recover into bounded streaming.
	p := recoveryPolicy(1024)
	s, _ := recoveryService(t, cfg, origin.URL, p)
	edge := httptest.NewServer(s.ProxyHandler())
	defer edge.Close()
	conn, e := net.DialTimeout("tcp", strings.TrimPrefix(edge.URL, "http://"), time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(4 * time.Second))
	fmt.Fprint(conn, "POST /v1/tasks HTTP/1.1\r\nHost: app.example.test\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n1\r\nx\r\n")
	resp, e := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "POST"})
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 408 {
		t.Fatalf("timeout status=%d", resp.StatusCode)
	}
	if len(s.edgeRequestBodyPolicyGuard("app_recovery", p).slots) != 0 {
		t.Fatal("timeout leaked policy slot")
	}
	t.Log("RESULT stalled SSE JSON upload returned 408 with explicit one-second policy")
}

func TestBodyBufferRecoverySSEPostNeverUsesPeerFallback(t *testing.T) {
	cfg := recoveryConfig(t)
	s, _ := recoveryService(t, cfg, "http://127.0.0.1:1")
	r, _ := recoveryRequest([]byte(`{}`), "text/event-stream", "application/json")
	for _, buffered := range []bool{false, true} {
		if s.peerFallbackAllowed(r, edgeProxyObservation{SSE: true, Streaming: true, Upload: true, RequestBodyBuffered: buffered}) {
			t.Fatal("SSE POST became eligible for replay")
		}
	}
	t.Log("RESULT peer fallback is forbidden for SSE/upload POST both before and after spooling")
}

func TestBodyBufferRecoveryOriginFailureDoesNotReplayPost(t *testing.T) {
	for _, spool := range []bool{true, false} {
		t.Run(fmt.Sprintf("spool_%t", spool), func(t *testing.T) {
			var hits atomic.Int64
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				io.Copy(io.Discard, r.Body)
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				conn.Close()
			}))
			defer origin.Close()
			cfg := recoveryConfig(t)
			cfg.RequestBodyBufferReserveBytes = 0
			if !spool {
				cfg.RequestBodyBufferMaxBytes = 0
			}
			s, l := recoveryService(t, cfg, origin.URL)
			r, _ := recoveryRequest([]byte(`{"input":"x"}`), "text/event-stream", "application/json")
			w := httptest.NewRecorder()
			s.ProxyHandler().ServeHTTP(w, r)
			if w.Code != 502 || hits.Load() != 1 {
				t.Fatalf("status=%d origin_hits=%d", w.Code, hits.Load())
			}
			if recoverySummary(t, l)["peer_fallback"] != false {
				t.Fatal("POST replayed to peer")
			}
			recoveryNoReservation(t, s)
			t.Logf("RESULT spool=%t origin closes after consuming POST: 502, one origin hit, no peer fallback, no reservation leak", spool)
		})
	}
}

func TestBodyBufferRecoveryBufferedFailureCleanup(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelled_%t", cancelled), func(t *testing.T) {
			cfg := recoveryConfig(t)
			cfg.RequestBodyBufferTotalMaxBytes = 1 << 20
			s, _ := recoveryService(t, cfg, "http://127.0.0.1:1")
			errToReturn := io.ErrUnexpectedEOF
			if cancelled {
				errToReturn = context.Canceled
			}
			r := httptest.NewRequest("POST", "http://app.example.test/v1/tasks", io.NopCloser(&shortReadReader{data: []byte(`{"input":`), err: errToReturn}))
			r.ContentLength = 100
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Accept", "text/event-stream")
			if cancelled {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				r = r.WithContext(ctx)
			}
			w := httptest.NewRecorder()
			s.ProxyHandler().ServeHTTP(w, r)
			if w.Code != 499 {
				t.Fatalf("status=%d", w.Code)
			}
			recoveryNoReservation(t, s)
			t.Logf("RESULT buffered cancelled=%t returns 499 and releases all reservation/temp files", cancelled)
		})
	}
}
