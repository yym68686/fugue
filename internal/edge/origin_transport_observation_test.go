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
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fugue/internal/config"
)

func TestOriginTransportErrorsRetainBoundedDiagnostics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"success", nil, ""},
		{"eof", io.EOF, "eof"},
		{"truncated", io.ErrUnexpectedEOF, "unexpected_eof"},
		{"reset", &net.OpError{Op: "read", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}, "connection_reset"},
		{"pipe", syscall.EPIPE, "broken_pipe"},
		{"refused", syscall.ECONNREFUSED, "connection_refused"},
		{"closed", net.ErrClosed, "connection_closed"},
		{"canceled", context.Canceled, "canceled"},
		{"deadline", context.DeadlineExceeded, "timeout"},
		{"socket_timeout", &net.OpError{Op: "read", Err: syscall.ETIMEDOUT}, "timeout"},
		{"dns_timeout", &net.DNSError{IsTimeout: true}, "timeout"},
		{"joined_timeout", errors.Join(errors.New("diagnostic"), &net.DNSError{IsTimeout: true}), "timeout"},
		{"unknown", errors.New("EOF in unrelated diagnostic text"), "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var transportErr error
			if tc.err != nil {
				transportErr = &url.Error{Op: "Post", URL: "https://origin.invalid/?token=private-value", Err: fmt.Errorf("sensitive-detail: %w", tc.err)}
			}
			observed := &edgeProxyObservation{}
			transport := &edgeProxyTransport{observation: observed, base: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, transportErr
			})}
			_, got := transport.RoundTrip(httptest.NewRequest(http.MethodPost, "http://origin.invalid/", nil))
			if got != transportErr {
				t.Fatal("observation changed the transport error")
			}
			if observed.OriginTransportErrorKind != tc.want {
				t.Fatalf("error kind = %q, want %q", observed.OriginTransportErrorKind, tc.want)
			}
			if got != nil {
				observed.UpstreamError = got.Error()
			}
			var logs bytes.Buffer
			service := NewService(config.EdgeConfig{}, log.New(&logs, "", 0))
			service.logProxyObservation(observed.originTraceSnapshot())
			for _, secret := range []string{"private-value", "sensitive-detail", "unrelated diagnostic"} {
				if strings.Contains(logs.String(), secret) {
					t.Fatalf("transport diagnostic leaked %q", secret)
				}
			}
			if tc.want != "" && (!strings.Contains(logs.String(), "origin_transport_error_kind="+tc.want) ||
				!strings.Contains(logs.String(), `\"origin_transport_error_kind\":\"`+tc.want+`\"`)) {
				t.Fatalf("missing diagnostic in text or durable request fact: %s", logs.String())
			}
		})
	}
}

func TestProxyHandlerObservesIdleConnectionEOFWithoutReplayingPOST(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != "payload" {
			t.Errorf("origin did not receive complete POST: body=%q err=%v", body, err)
		}
		if requests.Add(1) == 1 {
			_, _ = io.WriteString(w, "ok")
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close() // The origin consumed the POST but sent no response.
	}))
	defer backend.Close()
	var logs bytes.Buffer
	service := NewService(config.EdgeConfig{}, log.New(&logs, "", 0))
	defer service.closeEdgeProxyTransports()
	bundle := testBundle("routegen_transport_observation")
	bundle.Routes[0].UpstreamURL = backend.URL
	service.recordSyncSuccess(bundle, `"routegen_transport_observation"`, time.Now().UTC(), false)
	handler := service.ProxyHandler()
	for i, wantStatus := range []int{http.StatusOK, http.StatusBadGateway} {
		if i > 0 {
			time.Sleep(2 * time.Millisecond)
		}
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/", strings.NewReader("payload"))
		handler.ServeHTTP(recorder, req)
		if recorder.Code != wantStatus {
			t.Fatalf("request %d status=%d body=%q, want %d", i, recorder.Code, recorder.Body.String(), wantStatus)
		}
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("POST was replayed: origin requests=%d, want 2", got)
	}
	var summaries []map[string]any
	for _, line := range strings.Split(logs.String(), "\n") {
		var fact map[string]any
		if json.Unmarshal([]byte(line), &fact) != nil || fact["event_type"] != "request_fact" {
			continue
		}
		var summary map[string]any
		if err := json.Unmarshal([]byte(fact["summary_json"].(string)), &summary); err != nil {
			t.Fatal(err)
		}
		summaries = append(summaries, summary)
	}
	if len(summaries) != 2 {
		t.Fatalf("got %d request facts, want 2: %s", len(summaries), logs.String())
	}
	first, failed := summaries[0], summaries[1]
	if first["origin_conn_reused"] != false || first["origin_conn_was_idle"] != false || first["origin_conn_idle_ms"] != float64(0) {
		t.Fatalf("incorrect fresh connection diagnostic: %+v", first)
	}
	if failed["origin_transport_error_kind"] != "eof" || failed["origin_conn_reused"] != true ||
		failed["origin_conn_was_idle"] != true || failed["origin_conn_idle_ms"].(float64) < 1 ||
		failed["origin_wrote_request"] != true || failed["origin_first_byte"] != false {
		t.Fatalf("missing idle connection failure diagnostic: %+v", failed)
	}
}
