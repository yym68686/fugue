package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

type abortObservationBody struct {
	body   string
	err    error
	closed bool
}

func (b *abortObservationBody) Read(p []byte) (int, error) {
	if b.body == "" {
		return 0, b.err
	}
	n := copy(p, b.body)
	b.body = b.body[n:]
	return n, nil
}

func (b *abortObservationBody) Close() error { b.closed = true; return nil }

type abortObservationWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
	err    error
}

func (w *abortObservationWriter) Write(p []byte) (int, error) {
	if w.cancel != nil {
		w.cancel()
	}
	n, _ := w.ResponseRecorder.Write(p[:2])
	return n, w.err
}

func TestProxyPreservesObservationWhenResponseCopyAborts(t *testing.T) {
	for _, name := range []string{"origin_truncated", "origin_truncated_with_policy", "downstream_canceled", "downstream_timeout", "short_write"} {
		t.Run(name, func(t *testing.T) {
			originFailure := strings.HasPrefix(name, "origin_truncated")
			var logs bytes.Buffer
			service := NewService(config.EdgeConfig{PeerFallbackEnabled: true}, log.New(&logs, "", 0))
			defer service.closeEdgeProxyTransports()
			bundle := testBundle("response-abort-observation")
			bundle.Routes[0].UpstreamURL = "http://origin.example.invalid"
			if name == "origin_truncated_with_policy" {
				bundle.Routes[0].RequestBodyPolicies = []model.EdgeRequestBodyPolicy{{
					Name: "upload-bound", Methods: []string{http.MethodPost}, Paths: []string{"/result"},
					MaxBytes: 1024, MaxConcurrent: 1, TimeoutSeconds: 10,
				}}
			}
			service.recordSyncSuccess(bundle, "response-abort-observation", time.Now().UTC(), false)
			body := &abortObservationBody{body: "partial", err: io.ErrUnexpectedEOF}
			calls := 0
			service.proxyBase = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				payload, err := io.ReadAll(req.Body)
				if err != nil || string(payload) != "request" {
					t.Errorf("request changed: %q %v", payload, err)
				}
				return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: body, ContentLength: 50, Request: req}, nil
			})
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), http.ServerContextKey, &http.Server{}))
			defer cancel()
			req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/result", strings.NewReader("request")).WithContext(ctx)
			recorder := httptest.NewRecorder()
			var writer http.ResponseWriter = recorder
			if !originFailure {
				w := &abortObservationWriter{ResponseRecorder: recorder}
				switch name {
				case "downstream_canceled":
					w.cancel, w.err = cancel, io.ErrClosedPipe
				case "downstream_timeout":
					w.err = context.DeadlineExceeded
				}
				writer = w
			}
			var panicked any
			func() {
				defer func() { panicked = recover() }()
				service.ProxyHandler().ServeHTTP(writer, req)
			}()
			if panicked != http.ErrAbortHandler {
				t.Fatalf("must preserve net/http abort: %v", panicked)
			}
			if calls != 1 || !body.closed {
				t.Fatalf("request replayed or origin body leaked: calls=%d closed=%t", calls, body.closed)
			}
			if recorder.Code != http.StatusCreated {
				t.Fatalf("committed status changed: %d", recorder.Code)
			}
			var facts []map[string]any
			for _, line := range strings.Split(logs.String(), "\n") {
				var fact map[string]any
				if json.Unmarshal([]byte(line), &fact) == nil && fact["event_type"] == "request_fact" {
					facts = append(facts, fact)
				}
			}
			if len(facts) != 1 {
				t.Fatalf("got %d request facts: %s", len(facts), logs.String())
			}
			fact := facts[0]
			if fact["status_code"] != float64(http.StatusCreated) || fact["bytes_out"] != float64(recorder.Body.Len()) {
				t.Errorf("lost committed response on abort: status=%v bytes=%v accepted=%d", fact["status_code"], fact["bytes_out"], recorder.Body.Len())
			}
			var summary map[string]any
			if err := json.Unmarshal([]byte(fact["summary_json"].(string)), &summary); err != nil {
				t.Fatal(err)
			}
			if name == "downstream_canceled" {
				if fact["error_type"] != "client_closed" || summary["client_canceled"] != true || summary["upstream_error"] == true || fact["platform_error_class"] != "" {
					t.Errorf("downstream cancellation mislabeled: fact=%v summary=%v", fact, summary)
				}
			} else if originFailure {
				if fact["error_type"] != "upstream_error" || summary["upstream_error"] != true || summary["origin_transport_error_kind"] != "unexpected_eof" || summary["origin_response_body_error_kind"] != "unexpected_eof" {
					t.Errorf("origin truncation missing: fact=%v summary=%v", fact, summary)
				}
			} else {
				want := "timeout"
				if name == "short_write" {
					want = "short_write"
				}
				if fact["error_type"] != "response_write_error" || summary["response_write_error_kind"] != want || summary["upstream_error"] == true || summary["client_canceled"] == true || fact["platform_error_class"] != model.PlatformErrorClassEvidenceUnknown {
					t.Errorf("downstream failure mislabeled: fact=%v summary=%v", fact, summary)
				}
			}
		})
	}
}

func TestObservationWriterCountsAcceptedBytesOnShortWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := &abortObservationWriter{ResponseRecorder: httptest.NewRecorder(), cancel: cancel, err: io.ErrClosedPipe}
	w := newEdgeProxyObservationResponseWriter(base, time.Now(), &edgeProxyObservation{})
	n, err := w.Write([]byte("payload"))
	if n != 2 || !errors.Is(err, io.ErrClosedPipe) || ctx.Err() == nil {
		t.Fatal("fixture did not short-write")
	}
	if w.bytesWritten != 2 {
		t.Fatalf("counted requested bytes instead of accepted bytes: %d", w.bytesWritten)
	}
}

func TestResponseBodyDiagnosticsPreserveErrorsAndHideDetails(t *testing.T) {
	for _, err := range []error{io.EOF, fmt.Errorf("private-response-detail: %w", io.ErrUnexpectedEOF)} {
		observed := &edgeProxyObservation{}
		transport := &edgeProxyTransport{observation: observed, base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: &abortObservationBody{body: "body", err: err}, Request: req}, nil
		})}
		resp, roundTripErr := transport.RoundTrip(httptest.NewRequest(http.MethodGet, "http://origin.example.invalid", nil))
		if roundTripErr != nil {
			t.Fatal(roundTripErr)
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(body) != "body" {
			t.Fatal("response data changed")
		}
		snapshot := observed.originTraceSnapshot()
		if err == io.EOF {
			if readErr != nil || snapshot.OriginResponseBodyErrorKind != "" || snapshot.UpstreamError != "" {
				t.Fatalf("EOF treated as failure: %+v", snapshot)
			}
		} else if readErr != err || snapshot.OriginResponseBodyErrorKind != "unexpected_eof" {
			t.Fatalf("error altered or lost: %v %+v", readErr, snapshot)
		}
		encoded, marshalErr := json.Marshal(edgeProxyObservationRequestFactFields(snapshot, config.EdgeConfig{}, ""))
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if strings.Contains(string(encoded), "private-response-detail") || strings.Contains(snapshot.UpstreamError, "private-response-detail") {
			t.Fatal("raw response error leaked")
		}
	}
}

func TestIncompleteResponseCannotPassCacheOrStreamOutcome(t *testing.T) {
	service := NewService(config.EdgeConfig{}, nil)
	decision := edgeHTTPCacheDecision{PolicyID: "cache-policy", Cacheable: true}
	for _, observed := range []edgeProxyObservation{
		{StatusCode: 200, OriginResponseBodyErrorKind: "unexpected_eof", UpstreamError: "origin response body: unexpected_eof"},
		{StatusCode: 200, ResponseWriteErrorKind: "timeout"},
		{StatusCode: 200, ResponseWriteErrorKind: "other", ClientCanceled: true},
	} {
		w := newEdgeProxyObservationResponseWriter(httptest.NewRecorder(), time.Now(), &observed)
		w.WriteHeader(http.StatusOK)
		if service.edgeCacheShouldStore(decision, w) {
			t.Errorf("incomplete response accepted for cache: %+v", observed)
		}
		if edgeProxyObservationResult(observed) == "success" {
			t.Errorf("incomplete stream counted successful: %+v", observed)
		}
	}
	w := newEdgeProxyObservationResponseWriter(httptest.NewRecorder(), time.Now(), &edgeProxyObservation{})
	w.WriteHeader(http.StatusOK)
	if !service.edgeCacheShouldStore(decision, w) {
		t.Fatal("ordinary successful cache response changed")
	}
}

func TestRealProxyAbortsTruncatedResponseWithoutReplay(t *testing.T) {
	var originCalls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Length", "32")
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "partial")
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer origin.Close()
	var logs bytes.Buffer
	service := NewService(config.EdgeConfig{}, log.New(&logs, "", 0))
	defer service.closeEdgeProxyTransports()
	bundle := testBundle("real-response-abort")
	bundle.Routes[0].UpstreamURL = origin.URL
	service.recordSyncSuccess(bundle, "real-response-abort", time.Now().UTC(), false)
	done := make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		service.ProxyHandler().ServeHTTP(w, r)
	}))
	defer proxy.Close()
	req, err := http.NewRequest(http.MethodPost, proxy.URL+"/result", strings.NewReader("request"))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "demo.fugue.pro"
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("proxy handler did not terminate")
	}
	if resp.StatusCode != http.StatusAccepted || string(data) != "partial" || !errors.Is(readErr, io.ErrUnexpectedEOF) {
		t.Fatalf("abort semantics changed: code=%d body=%q err=%v", resp.StatusCode, data, readErr)
	}
	if originCalls.Load() != 1 {
		t.Fatal("POST was replayed")
	}
	for _, want := range []string{`"status_code":202`, `"bytes_out":7`, `"error_type":"upstream_error"`, `origin_transport_error_kind=unexpected_eof`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("missing %s: %s", want, logs.String())
		}
	}
}
