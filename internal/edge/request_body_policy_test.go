package edge

import (
	"bufio"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestEdgeRequestBodyPolicyRejectsKnownOversizeBeforeOrigin(t *testing.T) {
	t.Parallel()

	var originRequests atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()

	service := newRequestBodyPolicyTestService(t, backend.URL, model.EdgeRequestBodyPolicy{
		Name: "upload", Methods: []string{"POST"}, Paths: []string{"/upload"},
		MaxBytes: 4, TimeoutSeconds: 10, MaxConcurrent: 2, RetryAfterSeconds: 5,
	})
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/upload", strings.NewReader("12345"))
	service.ProxyHandler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d body=%q", recorder.Code, recorder.Body.String())
	}
	if originRequests.Load() != 0 {
		t.Fatalf("expected content-length preflight before origin, got %d origin requests", originRequests.Load())
	}
}

func TestEdgeRequestBodyPolicyRejectsChunkedOversizeWhileStreaming(t *testing.T) {
	t.Parallel()

	var originBytes atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		originBytes.Store(int64(len(body)))
		w.WriteHeader(http.StatusCreated)
	}))
	defer backend.Close()

	service := newRequestBodyPolicyTestService(t, backend.URL, model.EdgeRequestBodyPolicy{
		Name: "upload", Methods: []string{"POST"}, Paths: []string{"/upload"},
		MaxBytes: 4, TimeoutSeconds: 10, MaxConcurrent: 2, RetryAfterSeconds: 5,
	})
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/upload", io.NopCloser(strings.NewReader("12345")))
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	service.ProxyHandler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected streaming 413, got %d body=%q", recorder.Code, recorder.Body.String())
	}
	if got := originBytes.Load(); got > 4 {
		t.Fatalf("edge forwarded %d bytes beyond the 4-byte policy", got)
	}
}

func TestEdgeRequestBodyPolicyLeavesOtherMultipartRoutesUnchanged(t *testing.T) {
	t.Parallel()

	originBodies := make(chan string, 2)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read origin body: %v", err)
			return
		}
		originBodies <- r.URL.Path + ":" + string(body)
		w.WriteHeader(http.StatusCreated)
	}))
	defer backend.Close()

	service := newRequestBodyPolicyTestService(t, backend.URL, model.EdgeRequestBodyPolicy{
		Name: "upload", Methods: []string{"POST"}, Paths: []string{"/protected-upload"},
		MaxBytes: 4, TimeoutSeconds: 10, MaxConcurrent: 2, RetryAfterSeconds: 5,
	})
	for _, test := range []struct {
		path string
		body string
	}{
		{path: "/protected-upload", body: "1234"},
		{path: "/ordinary-upload", body: "ordinary multipart payload"},
	} {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro"+test.path, strings.NewReader(test.body))
		req.Header.Set("Content-Type", "multipart/form-data; boundary=test")
		service.ProxyHandler().ServeHTTP(recorder, req)
		if recorder.Code != http.StatusCreated {
			t.Fatalf("expected %s to retain streaming behavior, got %d body=%q", test.path, recorder.Code, recorder.Body.String())
		}
		if got := <-originBodies; got != test.path+":"+test.body {
			t.Fatalf("unexpected origin body %q", got)
		}
	}
}

func TestEdgeRequestBodyPolicyStreamsToOriginWithoutPrereadingBody(t *testing.T) {
	t.Parallel()

	originStartedReading := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buffer := make([]byte, 1)
		if _, err := io.ReadFull(r.Body, buffer); err != nil {
			t.Errorf("read first origin byte: %v", err)
			return
		}
		close(originStartedReading)
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Errorf("read remaining origin body: %v", err)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer backend.Close()

	service := newRequestBodyPolicyTestService(t, backend.URL, model.EdgeRequestBodyPolicy{
		Name: "upload", Methods: []string{"POST"}, Paths: []string{"/upload"},
		MaxBytes: 1024, TimeoutSeconds: 10, MaxConcurrent: 2, RetryAfterSeconds: 5,
	})
	reader, writer := io.Pipe()
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/upload", reader)
		req.ContentLength = -1
		req.TransferEncoding = []string{"chunked"}
		req.Header.Set("Content-Type", "multipart/form-data; boundary=test")
		service.ProxyHandler().ServeHTTP(recorder, req)
		result <- recorder
	}()

	if _, err := writer.Write([]byte("a")); err != nil {
		t.Fatalf("write first request byte: %v", err)
	}
	select {
	case <-originStartedReading:
	case <-time.After(2 * time.Second):
		t.Fatal("origin did not receive the first byte before the client finished; request appears buffered")
	}
	if _, err := writer.Write([]byte("remaining")); err != nil {
		t.Fatalf("write remaining request body: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close request body writer: %v", err)
	}
	recorder := <-result
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected streamed request to succeed, got %d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestEdgeRequestBodyPolicySharesConcurrencyAcrossExactPathsAndReleasesSlots(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()

	service := newRequestBodyPolicyTestService(t, backend.URL, model.EdgeRequestBodyPolicy{
		Name: "source-imports", Methods: []string{"POST"}, Paths: []string{"/one", "/two"},
		MaxBytes: 1024, TimeoutSeconds: 10, MaxConcurrent: 2, RetryAfterSeconds: 7,
	})
	results := make(chan *httptest.ResponseRecorder, 2)
	for _, requestPath := range []string{"/one", "/two"} {
		requestPath := requestPath
		go func() {
			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro"+requestPath, strings.NewReader("x"))
			service.ProxyHandler().ServeHTTP(recorder, req)
			results <- recorder
		}()
	}
	for index := 0; index < 2; index++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for protected requests to reach origin")
		}
	}

	rejected := httptest.NewRecorder()
	rejectedRequest := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/one", strings.NewReader("x"))
	service.ProxyHandler().ServeHTTP(rejected, rejectedRequest)
	if rejected.Code != http.StatusTooManyRequests || rejected.Header().Get("Retry-After") != "7" {
		t.Fatalf("expected shared concurrency 429 with Retry-After 7, got status=%d headers=%v body=%q", rejected.Code, rejected.Header(), rejected.Body.String())
	}

	close(release)
	for index := 0; index < 2; index++ {
		if recorder := <-results; recorder.Code != http.StatusNoContent {
			t.Fatalf("expected in-flight request to finish, got %d body=%q", recorder.Code, recorder.Body.String())
		}
	}

	afterRelease := httptest.NewRecorder()
	afterReleaseRequest := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/one", strings.NewReader("x"))
	service.ProxyHandler().ServeHTTP(afterRelease, afterReleaseRequest)
	if afterRelease.Code != http.StatusNoContent {
		t.Fatalf("expected concurrency slot release after completion, got %d body=%q", afterRelease.Code, afterRelease.Body.String())
	}
}

func TestEdgeRequestBodyPolicyMapsDeadlineToRequestTimeout(t *testing.T) {
	t.Parallel()

	backendRelease := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-backendRelease:
		}
	}))
	defer backend.Close()
	defer close(backendRelease)

	service := newRequestBodyPolicyTestService(t, backend.URL, model.EdgeRequestBodyPolicy{
		Name: "upload", Methods: []string{"POST"}, Paths: []string{"/upload"},
		MaxBytes: 1024, TimeoutSeconds: 1, MaxConcurrent: 2, RetryAfterSeconds: 5,
	})
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/upload", strings.NewReader("x"))
	service.ProxyHandler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusRequestTimeout {
		t.Fatalf("expected 408, got %d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestEdgeRequestBodyPolicyInterruptsBlockedClientBodyRead(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusCreated)
	}))
	defer backend.Close()

	service := newRequestBodyPolicyTestService(t, backend.URL, model.EdgeRequestBodyPolicy{
		Name: "upload", Methods: []string{"POST"}, Paths: []string{"/upload"},
		MaxBytes: 1024, TimeoutSeconds: 1, MaxConcurrent: 2, RetryAfterSeconds: 5,
	})
	edgeServer := httptest.NewServer(service.ProxyHandler())
	defer edgeServer.Close()

	edgeAddress := strings.TrimPrefix(edgeServer.URL, "http://")
	connection, err := net.DialTimeout("tcp", edgeAddress, time.Second)
	if err != nil {
		t.Fatalf("dial edge server: %v", err)
	}
	defer connection.Close()
	if _, err := io.WriteString(connection, "POST /upload HTTP/1.1\r\nHost: demo.fugue.pro\r\nTransfer-Encoding: chunked\r\nContent-Type: multipart/form-data; boundary=test\r\nConnection: close\r\n\r\n1\r\nx\r\n"); err != nil {
		t.Fatalf("write partial request body: %v", err)
	}

	started := time.Now()
	response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatalf("read edge response: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestTimeout {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("expected blocked body read to terminate with 408, got %d body=%q", response.StatusCode, body)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("body read was not interrupted near the configured timeout: %s", elapsed)
	}
}

func newRequestBodyPolicyTestService(t *testing.T, backendURL string, policy model.EdgeRequestBodyPolicy) *Service {
	t.Helper()
	bundle := testBundle("routegen_request_body_policy")
	bundle.Routes[0].UpstreamURL = backendURL
	bundle.Routes[0].RequestBodyPolicies = []model.EdgeRequestBodyPolicy{policy}
	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_request_body_policy"`, time.Now().UTC(), false)
	return service
}
