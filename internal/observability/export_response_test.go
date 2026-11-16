package observability

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"testing"
	"time"
)

func responseTestExporter(name string, client *http.Client, endpoint string) func(context.Context, int) error {
	switch name {
	case "metrics":
		e := &PrometheusRemoteWriteExporter{Client: client, RemoteWriteURL: endpoint}
		return func(ctx context.Context, index int) error {
			return e.Export(ctx, []Event{prometheusMetricEvent(time.Unix(100+int64(index), 0), "1")})
		}
	case "logs":
		e := LokiExporter{Client: client, PushURL: endpoint}
		return func(ctx context.Context, _ int) error {
			return e.Export(ctx, []Event{{Timestamp: time.Unix(100, 0), Kind: EventKindLog, Message: "synthetic"}})
		}
	case "analytics":
		e := NewClickHouseExporter(endpoint, client)
		return func(ctx context.Context, _ int) error {
			return e.insertRows(ctx, "app_events", [][]byte{[]byte(`{"synthetic":true}`)})
		}
	default:
		panic("unknown test exporter")
	}
}

// Even an empty chunked response needs an EOF read: Close alone makes the
// HTTP/1 transport discard the connection before its framing is consumed.
func TestExportersReuseConnectionAfterEmptyChunkedSuccess(t *testing.T) {
	for _, name := range []string{"metrics", "logs", "analytics"} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush() // No Content-Length; force chunked framing.
			}))
			defer server.Close()
			client := server.Client()
			client.Timeout = time.Second
			var reused []bool
			ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{
				GotConn: func(info httptrace.GotConnInfo) { reused = append(reused, info.Reused) },
			})
			export := responseTestExporter(name, client, server.URL)
			for i := range 2 {
				if err := export(ctx, i); err != nil {
					t.Fatal(err)
				}
			}
			if len(reused) != 2 || reused[0] || !reused[1] {
				t.Fatalf("expected a fresh connection followed by reuse, got %v", reused)
			}
		})
	}
}

type exporterResponseBudgetBody struct {
	bytes, closes int
	err           error
}

func (b *exporterResponseBudgetBody) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	clear(p)
	b.bytes += len(p)
	return len(p), nil // Unbounded body: consumer must enforce its own limit.
}

func (b *exporterResponseBudgetBody) Close() error { b.closes++; return nil }

func TestExporterSuccessDrainBoundsAndAcknowledgement(t *testing.T) {
	for _, name := range []string{"metrics", "logs", "analytics"} {
		for _, readError := range []bool{false, true} {
			t.Run(name+"/"+map[bool]string{false: "large", true: "read_error"}[readError], func(t *testing.T) {
				body := &exporterResponseBudgetBody{}
				if readError {
					body.err = errors.New("synthetic body error")
				}
				var requests int
				client := &http.Client{Transport: exportHTTPTestTransport(func(r *http.Request) (*http.Response, error) {
					requests++
					_, _ = io.Copy(io.Discard, r.Body)
					_ = r.Body.Close()
					return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
				})}
				export := responseTestExporter(name, client, "http://example.test")
				if err := export(t.Context(), 0); err != nil {
					t.Fatal("acknowledged write became retryable", err)
				}
				wantBytes := 4096
				if readError {
					wantBytes = 0
				}
				if requests != 1 || body.bytes != wantBytes || body.closes != 1 {
					t.Fatalf("unbounded read or changed body ownership: requests=%d body=%+v", requests, body)
				}
				if name == "metrics" {
					// The successful timestamp must still be committed even if
					// optional draining fails, so an identical sample is not sent again.
					if err := export(t.Context(), 0); err != nil || requests != 1 {
						t.Fatal("metric acknowledgement was lost", err, requests)
					}
				}
			})
		}
	}
}

func TestExporterSuccessDrainHonorsExistingClientTimeout(t *testing.T) {
	for _, name := range []string{"metrics", "logs", "analytics"} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				<-r.Context().Done() // Headers acknowledged; body never completes.
			}))
			defer server.Close()
			client := server.Client()
			client.Timeout = 100 * time.Millisecond
			export := responseTestExporter(name, client, server.URL)
			started := time.Now()
			if err := export(t.Context(), 0); err != nil {
				t.Fatal("body timeout changed acknowledged status", err)
			}
			if time.Since(started) > 2*time.Second {
				t.Fatal("draining ignored existing timeout")
			}
		})
	}
}
