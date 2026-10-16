package edge

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
)

func TestProxyObservesSSEFromResponseWithoutRequestAccept(t *testing.T) {
	for _, contentType := range []string{"text/event-stream", "text/event-stream; charset=utf-8", "application/json"} {
		t.Run(contentType, func(t *testing.T) {
			const body = "data: first\n\ndata: done\n\n"
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Accept") != "*/*" {
					t.Error("proxy changed request negotiation")
				}
				w.Header().Set("Content-Type", contentType)
				fmt.Fprint(w, body)
			}))
			defer backend.Close()
			bundle := testBundle("response-stream-observation")
			bundle.Routes[0].UpstreamURL = backend.URL
			var logs bytes.Buffer
			s := NewService(config.EdgeConfig{}, log.New(&logs, "", 0))
			s.recordSyncSuccess(bundle, `"response-stream-observation"`, time.Now().UTC(), false)
			r := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/events", nil)
			r.Header.Set("Accept", "*/*")
			w := httptest.NewRecorder()
			s.ProxyHandler().ServeHTTP(w, r)
			if w.Code != 200 || w.Body.String() != body || w.Header().Get("Content-Type") != contentType {
				t.Fatalf("response changed: status=%d content-type=%q body=%q", w.Code, w.Header().Get("Content-Type"), w.Body.String())
			}
			wantSSE := strings.HasPrefix(contentType, "text/event-stream")
			if !strings.Contains(logs.String(), fmt.Sprintf("sse=%t streaming=%t", wantSSE, wantSSE)) {
				t.Fatalf("response stream misclassified: %s", logs.String())
			}
		})
	}
}
