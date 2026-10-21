package cli

import (
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
)

// net/http can invoke trace hooks concurrently, including after Do returns
// (for example a losing speculative dial on a reused connection).
func TestClientTraceConcurrentAndLateCallbacks(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "response", true: "transport-error"}[fail], func(t *testing.T) {
			var workers sync.WaitGroup
			startLate := make(chan struct{})
			client, _ := newClientWithOptions("https://api.example.test", "test", clientOptions{})
			var observed httpObservedRequest
			client.observer = func(r httpObservedRequest) { observed = r }
			client.httpClient = &http.Client{Transport: retryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				trace := httptrace.ContextClientTrace(req.Context())
				invoke := func() {
					trace.DNSStart(httptrace.DNSStartInfo{})
					trace.DNSDone(httptrace.DNSDoneInfo{})
					trace.ConnectStart("tcp", "example")
					trace.ConnectDone("tcp", "example", nil)
					trace.GotConn(httptrace.GotConnInfo{Reused: true})
					trace.TLSHandshakeStart()
					trace.TLSHandshakeDone(tls.ConnectionState{}, nil)
					trace.GotFirstResponseByte()
				}
				for i := 0; i < 8; i++ {
					workers.Add(1)
					go func() {
						defer workers.Done()
						for j := 0; j < 32; j++ {
							invoke()
						}
						<-startLate
						invoke()
					}()
				}
				if fail {
					return nil, io.EOF
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
			})}
			req, _ := http.NewRequest(http.MethodGet, "https://api.example.test", nil)
			result, err := client.doPreparedOnce(req)
			before := observed.Timing
			close(startLate)
			workers.Wait()
			if (err != nil) != fail || observed.Timing != before || before.Total <= 0 {
				t.Fatalf("request result or immutable timing changed: err=%v before=%+v after=%+v", err, before, observed.Timing)
			}
			if !fail && (string(result.Payload) != "ok" || result.Timing != before) {
				t.Fatal("trace changed response or observer timing")
			}
		})
	}
}
