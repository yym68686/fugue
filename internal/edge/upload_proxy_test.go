package edge

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
)

func TestProxyStreamsMultipartWithoutApplyingSSEJSONBufferLimit(t *testing.T) {
	t.Parallel()

	const payload = "multipart-payload"
	var originBody string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read origin body: %v", err)
			return
		}
		originBody = string(body)
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprint(w, "accepted")
	}))
	defer backend.Close()

	bundle := testBundle("routegen_stream_multipart")
	bundle.Routes[0].UpstreamURL = backend.URL
	service := NewService(config.EdgeConfig{
		APIURL:                         "https://api.example.invalid",
		EdgeToken:                      "edge-secret",
		RequestBodyBufferPath:          t.TempDir(),
		RequestBodyBufferMaxBytes:      3,
		RequestBodyBufferTotalMaxBytes: 3,
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_stream_multipart"`, time.Now().UTC(), false)

	req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/api/upload", strings.NewReader(payload))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=upload-boundary")
	recorder := httptest.NewRecorder()
	service.ProxyHandler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%q", http.StatusCreated, recorder.Code, recorder.Body.String())
	}
	if originBody != payload {
		t.Fatalf("expected streamed origin body %q, got %q", payload, originBody)
	}
	if service.edgeRequestBodyBufferManager().activeRequests() != 0 {
		t.Fatal("multipart upload unexpectedly acquired an edge replay-buffer reservation")
	}
}
