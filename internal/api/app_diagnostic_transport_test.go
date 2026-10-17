package api

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAppDiagnosticRequestsFollowServiceHandoff(t *testing.T) {
	_, server, _, _ := setupAppConfigTestServer(t, model.AppSpec{Image: "example.invalid/service:one", Ports: []int{8080}, Replicas: 1, RuntimeID: "runtime_managed_shared"})
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "previous") }))
	defer old.Close()
	next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "promoted") }))
	defer next.Close()
	var target atomic.Value
	target.Store(old.Listener.Addr().String())
	configureDiagnosticTestDial(t, server.appRequestHTTPClient, &target)
	request := func() string {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://service.internal/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		result, err := doObservedHTTPRequest(server.appRequestHTTPClient, req, 1024)
		if err != nil {
			t.Fatal(err)
		}
		return result.Body
	}
	if body := request(); body != "previous" {
		t.Fatalf("first request = %q", body)
	}
	// The Service name is unchanged; new TCP connections resolve to its promoted target.
	target.Store(next.Listener.Addr().String())
	if body := request(); body != "promoted" {
		t.Fatalf("diagnostic stayed on withdrawn workload: %q", body)
	}
}

func TestAppDiagnosticHandoffPreservesActiveStream(t *testing.T) {
	_, server, _, _ := setupAppConfigTestServer(t, model.AppSpec{Image: "example.invalid/service:one", Ports: []int{8080}, Replicas: 1, RuntimeID: "runtime_managed_shared"})
	release := make(chan struct{})
	defer close(release)
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "data: final\n\n")
		case <-r.Context().Done():
		}
	}))
	defer old.Close()
	next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "promoted") }))
	defer next.Close()
	var target atomic.Value
	target.Store(old.Listener.Addr().String())
	configureDiagnosticTestDial(t, server.appRequestHTTPClient, &target)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://service.internal/events", nil)
	response, err := server.appRequestHTTPClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	if first, err := reader.ReadString('\n'); err != nil || first != "data: first\n" {
		t.Fatalf("first frame = %q, %v", first, err)
	}
	target.Store(next.Listener.Addr().String())
	nextReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://service.internal/healthz", nil)
	result, err := doObservedHTTPRequest(server.appRequestHTTPClient, nextReq, 1024)
	if err != nil || result.Body != "promoted" {
		t.Fatalf("new probe = %+v, %v", result, err)
	}
	release <- struct{}{}
	rest, err := io.ReadAll(reader)
	if err != nil || !strings.Contains(string(rest), "data: final\n\n") {
		t.Fatalf("active stream truncated: %q, %v", rest, err)
	}
}

func configureDiagnosticTestDial(t *testing.T, client *http.Client, target *atomic.Value) {
	t.Helper()
	base := http.DefaultTransport.(*http.Transport)
	if client.Transport != nil {
		var ok bool
		base, ok = client.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("unexpected diagnostic transport %T", client.Transport)
		}
	}
	transport := base.Clone()
	transport.Proxy = nil
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, target.Load().(string))
	}
	client.Transport = transport
	t.Cleanup(transport.CloseIdleConnections)
}
