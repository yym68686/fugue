package edge

import (
	"bufio"
	"context"
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

func TestAppServiceOriginFollowsEndpointHandoffWithinSameRoute(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	releaseStream := make(chan struct{})
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: start\n\n")
			w.(http.Flusher).Flush()
			select {
			case <-releaseStream:
				_, _ = io.WriteString(w, "data: finish\n\n")
			case <-r.Context().Done():
			}
			return
		}
		_, _ = io.WriteString(w, "old pod")
	}))
	defer old.Close()
	defer cancel()
	var posts atomic.Int32
	next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
			payload, err := io.ReadAll(r.Body)
			if err != nil || string(payload) != "payload" {
				t.Errorf("unexpected POST body: %q, %v", payload, err)
			}
		}
		_, _ = io.WriteString(w, "new pod")
	}))
	defer next.Close()

	service := NewService(config.EdgeConfig{}, log.New(io.Discard, "", 0))
	defer service.closeEdgeProxyTransports()
	var cutover atomic.Bool
	// Model a Kubernetes Service: a new dial follows the current endpoint, but
	// established sockets keep talking to the Pod they originally selected.
	service.proxyBase.(*http.Transport).DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		endpoint := old.Listener.Addr()
		if cutover.Load() {
			endpoint = next.Listener.Addr()
		}
		return (&net.Dialer{}).DialContext(ctx, network, endpoint.String())
	}
	route := model.EdgeRouteBinding{
		AppID: "app", UpstreamURL: "http://backend.default.svc.cluster.local",
		UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService,
		DeploymentGeneration: "unchanged",
	}
	service.reconcileEdgeProxyTransports(model.EdgeRouteBundle{Routes: []model.EdgeRouteBinding{route}})
	request := func(method, path string, body io.Reader) (*http.Response, *edgeProxyObservation) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, route.UpstreamURL+path, body)
		if err != nil {
			t.Fatal(err)
		}
		req.GetBody = nil
		observed := &edgeProxyObservation{Route: route}
		response, err := service.newEdgeProxyTransport(observed).RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		return response, observed
	}
	verify := func(method string, body io.Reader, want string) {
		t.Helper()
		response, observed := request(method, "/", body)
		defer response.Body.Close()
		got, err := io.ReadAll(response.Body)
		if err != nil || string(got) != want {
			t.Fatalf("response body=%q err=%v; want %q", got, err, want)
		}
		if observed.originTraceSnapshot().OriginConnectionReused {
			t.Fatal("app request reused a Service connection")
		}
	}

	stream, _ := request(http.MethodGet, "/stream", nil)
	defer stream.Body.Close()
	reader := bufio.NewReader(stream.Body)
	if line, err := reader.ReadString('\n'); err != nil || line != "data: start\n" {
		t.Fatalf("stream start = %q, %v", line, err)
	}
	verify(http.MethodGet, nil, "old pod") // Would leave an idle socket to the old Pod.
	cutover.Store(true)
	verify(http.MethodGet, nil, "new pod")
	verify(http.MethodPost, strings.NewReader("payload"), "new pod")
	if got := posts.Load(); got != 1 {
		t.Fatalf("new Pod received %d writes; want exactly one", got)
	}
	close(releaseStream)
	if tail, err := io.ReadAll(reader); err != nil || string(tail) != "\ndata: finish\n\n" {
		t.Fatalf("in-flight stream did not finish on the old Pod: %q, %v", tail, err)
	}
}

func TestAppServiceOriginTransportScope(t *testing.T) {
	t.Parallel()
	service := NewService(config.EdgeConfig{}, log.New(io.Discard, "", 0))
	defer service.closeEdgeProxyTransports()
	route := model.EdgeRouteBinding{
		AppID: "app", UpstreamURL: "http://backend.default.svc.cluster.local",
		UpstreamKind: model.EdgeRouteUpstreamKindKubernetesService, UpstreamScope: model.EdgeRouteUpstreamScopeLocalService,
	}
	local := service.edgeProxyBaseForObservation(&edgeProxyObservation{Route: route}).(*http.Transport)
	route.UpstreamScope = model.EdgeRouteUpstreamScopeCluster // Also covers a release-owned Service.
	cluster := service.edgeProxyBaseForObservation(&edgeProxyObservation{Route: route}).(*http.Transport)
	if local != cluster || !local.DisableKeepAlives {
		t.Fatal("managed Services must share the transport that dials for each request")
	}
	peer := service.edgeProxyBaseForObservation(&edgeProxyObservation{Route: route, PeerFallback: true}).(*http.Transport)
	route.AppID = ""
	route.RouteKind = model.EdgeRouteKindControlPlaneAPI
	api := service.edgeProxyBaseForObservation(&edgeProxyObservation{Route: route}).(*http.Transport)
	route.RouteKind = model.EdgeRouteKindPlatformRoute
	platform := service.edgeProxyBaseForObservation(&edgeProxyObservation{Route: route}).(*http.Transport)
	route.AppID = "app"
	route.UpstreamKind = model.EdgeRouteUpstreamKindMesh
	mesh := service.edgeProxyBaseForObservation(&edgeProxyObservation{Route: route}).(*http.Transport)
	for name, transport := range map[string]*http.Transport{"peer": peer, "api": api, "platform": platform, "mesh": mesh} {
		if transport.DisableKeepAlives {
			t.Errorf("%s transport lost connection reuse", name)
		}
	}
}
