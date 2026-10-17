package edge

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

// An unused speculative dial can sit in the transport's idle pool even though
// the server is still waiting for its first request. Hold the server's EOF until
// the next write to reproduce a close notification delayed in transit.
type delayedOriginEOFConn struct {
	net.Conn
	ctx        context.Context
	eofSeen    chan struct{}
	releaseEOF chan struct{}
	closed     chan struct{}
	writeOnce  sync.Once
	eofOnce    sync.Once
	closeOnce  sync.Once
}

func (c *delayedOriginEOFConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if errors.Is(err, io.EOF) {
		c.eofOnce.Do(func() { close(c.eofSeen) })
		select {
		case <-c.releaseEOF:
		case <-c.closed:
		case <-c.ctx.Done():
		}
	}
	return n, err
}

func (c *delayedOriginEOFConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.writeOnce.Do(func() { close(c.releaseEOF) })
	return n, err
}

func (c *delayedOriginEOFConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func TestControlPlaneOriginRetiresUnusedDialBeforeInitialHeaderDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	warmEntered, warmRelease := make(chan struct{}), make(chan struct{})
	var posts atomic.Int32
	var uses sync.Map
	backend := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count, _ := uses.LoadOrStore(r.RemoteAddr, new(atomic.Int32))
		count.(*atomic.Int32).Add(1)
		if r.Method == http.MethodPost {
			posts.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil || string(body) != "payload" {
				t.Errorf("POST body=%q err=%v", body, err)
			}
		}
		if r.URL.Path == "/warm" {
			close(warmEntered)
			select {
			case <-warmRelease:
			case <-ctx.Done():
			}
		}
		_, _ = io.WriteString(w, "ok")
	}))
	// Matches the API listener's initial request-header deadline.
	backend.Config.ReadHeaderTimeout = 10 * time.Second
	backend.Start()
	defer backend.Close()
	defer cancel()

	dialStarted, returnDial := make(chan struct{}), make(chan struct{})
	second := make(chan *delayedOriginEOFConn, 1)
	var dials atomic.Int32
	prototype := newDefaultEdgeProxyTransport().(*http.Transport)
	prototype.DialContext = func(dialCtx context.Context, network, address string) (net.Conn, error) {
		n := dials.Add(1)
		conn, err := (&net.Dialer{}).DialContext(dialCtx, network, address)
		if err != nil || n != 2 {
			return conn, err
		}
		wrapped := &delayedOriginEOFConn{Conn: conn, ctx: ctx, eofSeen: make(chan struct{}), releaseEOF: make(chan struct{}), closed: make(chan struct{})}
		second <- wrapped
		close(dialStarted)
		select {
		case <-returnDial:
			return wrapped, nil
		case <-ctx.Done():
			_ = wrapped.Close()
			return nil, ctx.Err()
		}
	}
	service := NewService(config.EdgeConfig{}, log.New(io.Discard, "", 0))
	service.proxyBase = prototype
	defer service.closeEdgeProxyTransports()
	route := model.EdgeRouteBinding{RouteKind: model.EdgeRouteKindControlPlaneAPI, UpstreamURL: backend.URL, RouteGeneration: "generation_fixture"}
	doRequest := func(method, path string, body io.Reader) (*edgeProxyObservation, error) {
		observed := &edgeProxyObservation{Route: route}
		request, err := http.NewRequestWithContext(ctx, method, backend.URL+path, body)
		if err != nil {
			return observed, err
		}
		request.GetBody = nil // A proxied POST has no replayable body.
		resp, err := service.newEdgeProxyTransport(observed).RoundTrip(request)
		if err == nil {
			_, err = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		return observed, err
	}
	wait := func(ch <-chan struct{}, name string) {
		t.Helper()
		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", name, ctx.Err())
		}
	}
	warmDone, nextDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := doRequest(http.MethodGet, "/warm", nil); warmDone <- err }()
	wait(warmEntered, "first request")
	go func() { _, err := doRequest(http.MethodGet, "/next", nil); nextDone <- err }()
	wait(dialStarted, "speculative dial")
	close(warmRelease)
	for _, done := range []chan error{warmDone, nextDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	unused := <-second
	close(returnDial)
	retiredBeforeEOF := false
	select {
	case <-unused.closed:
		retiredBeforeEOF = true
	case <-unused.eofSeen:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	observed, err := doRequest(http.MethodPost, "/mutation", strings.NewReader("payload"))
	if err != nil {
		snapshot := observed.originTraceSnapshot()
		t.Fatalf("POST after unused origin dial: %v; retired=%t reused=%t idle=%s wrote=%t kind=%s", err, retiredBeforeEOF,
			snapshot.OriginConnectionReused, snapshot.OriginConnectionIdle, snapshot.OriginWroteRequest, snapshot.OriginTransportErrorKind)
	}
	if !retiredBeforeEOF {
		t.Fatal("unused origin connection reached the server's initial header deadline")
	}
	if count, ok := uses.Load(unused.LocalAddr().String()); ok && count.(*atomic.Int32).Load() != 0 {
		t.Fatal("unused connection unexpectedly carried a request")
	}
	if got := posts.Load(); got != 1 {
		t.Fatalf("POST count=%d, want exactly one", got)
	}
}

func TestControlPlaneOriginIdlePolicyKeepsActiveRequestsAndReuse(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			// This request remains active beyond the idle pool's timeout.
			time.Sleep(edgeControlPlaneOriginIdleTimeout + 250*time.Millisecond)
		}
		_, _ = io.WriteString(w, r.RemoteAddr)
	}))
	defer backend.Close()
	service := NewService(config.EdgeConfig{}, log.New(io.Discard, "", 0))
	defer service.closeEdgeProxyTransports()
	route := model.EdgeRouteBinding{RouteKind: model.EdgeRouteKindControlPlaneAPI, UpstreamURL: backend.URL, RouteGeneration: "generation_active"}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var previous string
	for i := 0; i < 2; i++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, backend.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := service.newEdgeProxyTransport(&edgeProxyObservation{Route: route}).RoundTrip(request)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || len(body) == 0 {
			t.Fatalf("response %d body=%q err=%v", i, body, err)
		}
		if i > 0 && string(body) != previous {
			t.Fatalf("active connection lost reuse: %q -> %q", previous, body)
		}
		previous = string(body)
	}
}

func TestControlPlaneOriginIdlePolicySeparatesRouteKinds(t *testing.T) {
	t.Parallel()
	service := NewService(config.EdgeConfig{}, log.New(io.Discard, "", 0))
	defer service.closeEdgeProxyTransports()
	prototype := service.proxyBase.(*http.Transport)
	route := model.EdgeRouteBinding{RouteKind: model.EdgeRouteKindPlatform, UpstreamURL: "http://origin.invalid", RouteGeneration: "generation_policy"}
	app := service.edgeProxyBaseForObservation(&edgeProxyObservation{Route: route}).(*http.Transport)
	route.RouteKind = model.EdgeRouteKindControlPlaneAPI
	api := service.edgeProxyBaseForObservation(&edgeProxyObservation{Route: route}).(*http.Transport)
	if api == app || api.IdleConnTimeout <= 0 || api.IdleConnTimeout >= 10*time.Second || app.IdleConnTimeout != prototype.IdleConnTimeout {
		t.Fatalf("origin policy mixed: app=%s API=%s prototype=%s", app.IdleConnTimeout, api.IdleConnTimeout, prototype.IdleConnTimeout)
	}
	// Retiring an artifact still closes the obsolete route pool. The API pool
	// remains scoped to the new artifact even when it uses the same origin.
	service.reconcileEdgeProxyTransports(model.EdgeRouteBundle{Routes: []model.EdgeRouteBinding{route}})
	service.proxyTransportMu.Lock()
	defer service.proxyTransportMu.Unlock()
	if len(service.proxyTransports) != 1 || service.proxyTransports[edgeOriginTransportKeyForRoute(route, "")] != api {
		t.Fatal("artifact reconciliation did not preserve exactly its active API pool")
	}
}
