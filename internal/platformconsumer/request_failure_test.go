package platformconsumer

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type failureTestBody struct {
	io.Reader
	reads, closes int
}

func (b *failureTestBody) Read(p []byte) (int, error) { b.reads++; return b.Reader.Read(p) }
func (b *failureTestBody) Close() error               { b.closes++; return nil }

func TestRequestFailureTransportIsPrivateDetachedAndCompatible(t *testing.T) {
	for _, trial := range []struct {
		name  string
		err   error
		class string
	}{
		{"transport", errors.New("private transport error"), "transport"},
		{"deadline", context.DeadlineExceeded, "deadline"},
		{"canceled", context.Canceled, "canceled"},
		{"dns", &net.DNSError{Err: "private resolver", Name: "private-host"}, "dns"},
	} {
		t.Run(trial.name, func(t *testing.T) {
			var calls, existing atomic.Int32
			var late *httptrace.ClientTrace
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotFirstResponseByte: func() { existing.Add(1) }})
			shared := &http.Client{Timeout: time.Hour, Transport: sourceBudgetTransport(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				before, _ := ctx.Deadline()
				after, ok := req.Context().Deadline()
				if !ok || !after.Equal(before) || req.Header.Get("Authorization") != "Bearer private-token" || req.Header.Get("Content-Type") != "application/json" || req.URL.Query().Get("token") != "private-query" {
					t.Fatal("request or caller deadline changed")
				}
				late = httptrace.ContextClientTrace(req.Context())
				late.DNSStart(httptrace.DNSStartInfo{Host: "private-host"})
				late.DNSDone(httptrace.DNSDoneInfo{Err: errors.New("private resolver error")})
				late.ConnectStart("private-network", "private-address")
				late.ConnectDone("private-network", "private-address", errors.New("private connect error"))
				late.TLSHandshakeStart()
				late.TLSHandshakeDone(tls.ConnectionState{ServerName: "private-host"}, errors.New("private TLS error"))
				late.GotConn(httptrace.GotConnInfo{Reused: true})
				late.WroteRequest(httptrace.WroteRequestInfo{})
				late.GotFirstResponseByte()
				return nil, trial.err
			})}
			err := (Client{BaseURL: "https://private-host", HTTPClient: shared}).GetJSON(ctx, "/v1/platform-state/consumers/assignment?token=private-query", "private-token", &struct{}{})
			if !errors.Is(err, ErrUnavailable) || !IsUnavailable(err) || err.Error() != ErrUnavailable.Error() || errors.Is(err, trial.err) || calls.Load() != 1 || existing.Load() != 1 || shared.Timeout != time.Hour || shared.CheckRedirect != nil {
				t.Fatal("outage semantics, attempts, trace composition or shared client changed", err, calls.Load(), existing.Load())
			}
			if httptrace.ContextClientTrace(ctx).DNSStart != nil {
				t.Fatal("caller trace mutated")
			}
			fact := RequestFailure(fmt.Errorf("outer: %w", err))
			if fact == nil || fact.Operation != "assignment" || fact.Phase != "request" || fact.Class != trial.class || fact.StartedAt.IsZero() || fact.DurationMS < 0 || fact.HTTPStatus != 0 || !fact.DNSStarted || !fact.DNSCompleted || !fact.ConnectStarted || !fact.ConnectCompleted || !fact.TLSStarted || !fact.TLSCompleted || !fact.ConnectionReady || !fact.ConnectionReused || !fact.RequestWritten || !fact.FirstResponseByte {
				t.Fatal("missing real transport milestones", fact)
			}
			before, _ := json.Marshal(fact)
			fact.Operation = "private mutation"
			var workers sync.WaitGroup
			for range 4 {
				workers.Add(1)
				go func() {
					defer workers.Done()
					for range 100 {
						late.GotConn(httptrace.GotConnInfo{})
						late.WroteRequest(httptrace.WroteRequestInfo{Err: errors.New("private late error")})
						_ = RequestFailure(err)
					}
				}()
			}
			workers.Wait()
			after, _ := json.Marshal(RequestFailure(err))
			if string(before) != string(after) || strings.Contains(string(after), "private") {
				t.Fatal("late callback/snapshot mutation changed evidence or private material escaped", string(after))
			}
		})
	}
}

func TestRequestFailureStatusAndDecodePreserveExistingBehavior(t *testing.T) {
	for _, code := range []int{200, 401, 403, 404, 409, 429, 500, 503} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			body := &failureTestBody{Reader: strings.NewReader(`{"private-field":"private-body"}`)}
			calls := 0
			client := Client{BaseURL: "https://private-host", HTTPClient: &http.Client{Transport: sourceBudgetTransport(func(*http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: code, Body: body, Header: make(http.Header)}, nil
			})}}
			var out map[string]any
			err := client.GetJSON(t.Context(), "/private-path?secret=private-query", "private-token", &out)
			if calls != 1 || body.closes != 1 || IsUnavailable(err) != (code >= 500) {
				t.Fatal("response behavior changed", calls, body, err)
			}
			if code == 200 {
				if err != nil || out["private-field"] != "private-body" || body.reads == 0 || RequestFailure(err) != nil {
					t.Fatal("successful response changed", err, out)
				}
				return
			}
			var status *responseStatusError
			fact := RequestFailure(err)
			if !errors.As(err, &status) || status.Code != code || body.reads != 0 || fact == nil || fact.HTTPStatus != code || fact.Phase != "status" || fact.Class != "http_status" || fact.Operation != "other" || fact.ConnectionReady || fact.FirstResponseByte {
				t.Fatal("status or absent callbacks misreported", err, fact)
			}
			raw, _ := json.Marshal(fact)
			if strings.Contains(string(raw), "private") {
				t.Fatal("private material escaped", string(raw))
			}
		})
	}
	body := &failureTestBody{Reader: timeoutSourceBody{}}
	client := Client{BaseURL: "https://private-host", HTTPClient: &http.Client{Transport: sourceBudgetTransport(func(req *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(req.Context())
		trace.GotConn(httptrace.GotConnInfo{Reused: true})
		trace.WroteRequest(httptrace.WroteRequestInfo{})
		trace.GotFirstResponseByte()
		return &http.Response{StatusCode: 200, Body: body, Header: make(http.Header)}, nil
	})}}
	err := client.GetJSON(t.Context(), "/v1/platform-state/consumers/assignment", "private-token", &struct{}{})
	fact := RequestFailure(err)
	if !errors.Is(err, ErrUnavailable) || !IsUnavailable(err) || body.reads != 1 || body.closes != 1 || fact == nil || fact.Phase != "decode" || fact.Class != "deadline" || fact.HTTPStatus != 200 || !fact.FirstResponseByte {
		t.Fatal("body timeout changed or lost phase", err, fact, body)
	}
}

func TestRequestFailureDoesNotChangeConflictOrValidatorSemantics(t *testing.T) {
	for _, code := range []string{"dns_backend_not_selected", "platform_assignment_changed", "unknown-private-code"} {
		client := Client{BaseURL: "https://private-host", HTTPClient: &http.Client{Transport: sourceBudgetTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 409, Body: io.NopCloser(strings.NewReader(`{"code":"` + code + `"}`)), Header: make(http.Header)}, nil
		})}}
		err := client.PostJSON(t.Context(), "/v1/platform-state/consumers/trusted-heartbeat", "private-token", struct{}{}, &struct{}{})
		if IsUnavailable(err) || errors.Is(err, ErrAssignmentChanged) != (code == "platform_assignment_changed") || errors.Is(err, ErrDNSBackendNotSelected) != (code == "dns_backend_not_selected") {
			t.Fatal("conflict semantics changed", code, err)
		}
	}
	want := errors.New("validator-result")
	client := Client{HTTPClient: &http.Client{Transport: sourceBudgetTransport(func(req *http.Request) (*http.Response, error) {
		if req.Header.Get("If-None-Match") != "validator" {
			t.Fatal("validator changed")
		}
		return &http.Response{StatusCode: 304, Body: http.NoBody, Header: http.Header{"Etag": []string{"validator"}}}, nil
	})}}
	err := client.jsonRevalidated(t.Context(), "https://private-host/read", "private-token", http.MethodGet, nil, &struct{}{}, 100, "validator", func(http.Header) error { return want })
	if err != want || RequestFailure(err) != nil {
		t.Fatal("validator result changed", err)
	}
}
