package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"

	"github.com/gorilla/websocket"
)

type fakeServiceResolver struct {
	ips map[string][]net.IPAddr
}

type countingServiceResolver struct {
	fakeServiceResolver
	lookupCount map[string]int
}

func (f fakeServiceResolver) LookupCNAME(context.Context, string) (string, error) {
	return "", nil
}

func (f fakeServiceResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	if addrs, ok := f.ips[host]; ok {
		return addrs, nil
	}
	return nil, &net.DNSError{IsNotFound: true}
}

func (f *countingServiceResolver) LookupCNAME(context.Context, string) (string, error) {
	return "", nil
}

func (f *countingServiceResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if f.lookupCount == nil {
		f.lookupCount = map[string]int{}
	}
	f.lookupCount[host]++
	return f.fakeServiceResolver.LookupIPAddr(ctx, host)
}

func TestServiceURLForAppPrefersIDScopedService(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Route: &model.AppRoute{
			ServicePort: 8080,
		},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	host := "app-demo." + namespace + ".svc.cluster.local"
	server := &Server{
		dnsResolver: fakeServiceResolver{
			ips: map[string][]net.IPAddr{
				host: {
					{IP: net.ParseIP("10.0.0.10")},
				},
			},
		},
	}

	got := server.serviceURLForApp(context.Background(), app)
	want := "http://" + host + ":8080"
	if got != want {
		t.Fatalf("expected service url %q, got %q", want, got)
	}
}

func TestServiceURLForAppFallsBackToLegacyServiceDuringMigration(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Route: &model.AppRoute{
			ServicePort: 8080,
		},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	legacyHost := "demo." + namespace + ".svc.cluster.local"
	server := &Server{
		dnsResolver: fakeServiceResolver{
			ips: map[string][]net.IPAddr{
				legacyHost: {
					{IP: net.ParseIP("10.0.0.20")},
				},
			},
		},
	}

	got := server.serviceURLForApp(context.Background(), app)
	want := "http://" + legacyHost + ":8080"
	if got != want {
		t.Fatalf("expected legacy service url %q, got %q", want, got)
	}
}

func TestServiceURLForAppCachesResolvedServiceHost(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Route: &model.AppRoute{
			ServicePort: 8080,
		},
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	host := "app-demo." + namespace + ".svc.cluster.local"
	resolver := &countingServiceResolver{
		fakeServiceResolver: fakeServiceResolver{
			ips: map[string][]net.IPAddr{
				host: {
					{IP: net.ParseIP("10.0.0.10")},
				},
			},
		},
	}
	server := &Server{
		dnsResolver:              resolver,
		appProxyServiceHostCache: newExpiringResponseCache[string](time.Minute),
	}

	first := server.serviceURLForApp(context.Background(), app)
	second := server.serviceURLForApp(context.Background(), app)
	want := "http://" + host + ":8080"
	if first != want || second != want {
		t.Fatalf("expected cached service url %q, got first=%q second=%q", want, first, second)
	}
	if got := resolver.lookupCount[host]; got != 1 {
		t.Fatalf("expected one DNS lookup for %q, got %d", host, got)
	}
}

func TestLoadAppByHostnameCachedUsesShortTTLCache(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServerWithDomains(t, "apps.example.com")
	server.appProxyAppCache = newExpiringResponseCache[model.App](time.Minute)

	first, err := server.loadAppByHostnameCached("demo.apps.example.com")
	if err != nil {
		t.Fatalf("load app by hostname: %v", err)
	}
	if first.Route == nil || first.Route.ServicePort != 8080 {
		t.Fatalf("expected first lookup to use port 8080, got %+v", first.Route)
	}

	if _, err := storeState.UpdateAppRoute(app.ID, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 9090,
	}); err != nil {
		t.Fatalf("update app route: %v", err)
	}

	second, err := server.loadAppByHostnameCached("demo.apps.example.com")
	if err != nil {
		t.Fatalf("reload cached app by hostname: %v", err)
	}
	if second.Route == nil || second.Route.ServicePort != 8080 {
		t.Fatalf("expected cached lookup to keep port 8080, got %+v", second.Route)
	}

	server.appProxyAppCache.clear("demo.apps.example.com")

	third, err := server.loadAppByHostnameCached("demo.apps.example.com")
	if err != nil {
		t.Fatalf("reload uncached app by hostname: %v", err)
	}
	if third.Route == nil || third.Route.ServicePort != 9090 {
		t.Fatalf("expected refreshed lookup to use port 9090, got %+v", third.Route)
	}
}

func TestMaybeHandleAppProxyUsesCustomDomainLookup(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, app, resolver := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	expectedTarget := server.primaryCustomDomainTarget(app)
	resolver.cname["fugue.pro"] = expectedTarget + "."

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/domains", platformAdminKey, map[string]any{
		"hostname": "fugue.pro",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected domain attach status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	loaded, err := server.loadAppByHostnameCached("fugue.pro")
	if err != nil {
		t.Fatalf("load custom-domain app by hostname: %v", err)
	}
	if loaded.ID != app.ID {
		t.Fatalf("expected custom-domain lookup to resolve app %q, got %q", app.ID, loaded.ID)
	}

	if _, err := storeState.GetAppDomain("fugue.pro"); err != nil {
		t.Fatalf("expected custom domain to be stored: %v", err)
	}
}

func TestAppProxyProxiesWebsocketUpgrades(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServer(t)
	specCopy := app.Spec
	deployOp, err := storeState.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		ExecutionMode:   model.ExecutionModeManaged,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := storeState.CompleteManagedOperationWithResult(deployOp.ID, "", "deployed", &specCopy, nil); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}
	app, err = storeState.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload deployed app: %v", err)
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	serviceHost := "app-demo." + namespace + ".svc.cluster.local"
	server.dnsResolver = fakeServiceResolver{
		ips: map[string][]net.IPAddr{
			serviceHost: {
				{IP: net.ParseIP("127.0.0.1")},
			},
		},
	}

	backendErrors := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			backendErrors <- err
			return
		}
		defer conn.Close()
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			backendErrors <- err
			return
		}
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			backendErrors <- err
			return
		}
		if string(payload) != "ping" {
			backendErrors <- errors.New("unexpected websocket payload: " + string(payload))
			return
		}
		if err := conn.WriteMessage(messageType, []byte("pong")); err != nil {
			backendErrors <- err
			return
		}
		backendErrors <- nil
	}))
	defer backend.Close()

	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("parse backend url: %v", err)
	}
	server.appProxyTransport = &http.Transport{
		Proxy:             nil,
		ForceAttemptHTTP2: false,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, backendURL.Host)
		},
	}

	public := httptest.NewServer(server.Handler())
	defer public.Close()

	clientURL, err := url.Parse(public.URL)
	if err != nil {
		t.Fatalf("parse public url: %v", err)
	}
	dialer := websocket.Dialer{
		NetDialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, clientURL.Host)
		},
	}

	conn, resp, err := dialer.Dial("ws://"+app.Route.Hostname+"/ws", nil)
	if err != nil {
		responseBody := ""
		if resp != nil && resp.Body != nil {
			bodyBytes, _ := io.ReadAll(resp.Body)
			responseBody = string(bodyBytes)
			resp.Body.Close()
		}
		t.Fatalf("dial proxied websocket: %v body=%s", err, responseBody)
	}
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set client read deadline: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write proxied websocket message: %v", err)
	}
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read proxied websocket response: %v", err)
	}
	if string(payload) != "pong" {
		t.Fatalf("expected proxied websocket payload %q, got %q", "pong", string(payload))
	}
	if err := <-backendErrors; err != nil {
		t.Fatalf("backend websocket validation failed: %v", err)
	}
}

func TestAppProxyLogsUpstreamProxyErrors(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServer(t)
	specCopy := app.Spec
	deployOp, err := storeState.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		ExecutionMode:   model.ExecutionModeManaged,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := storeState.CompleteManagedOperationWithResult(deployOp.ID, "", "deployed", &specCopy, nil); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}
	app, err = storeState.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload deployed app: %v", err)
	}
	var logBuffer bytes.Buffer
	server.log = log.New(&logBuffer, "", 0)
	server.appProxyTransport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial upstream exploded")
	})

	req := httptest.NewRequest(http.MethodGet, "http://"+app.Route.Hostname+"/healthz", nil)
	req.Host = app.Route.Hostname
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadGateway, recorder.Code, recorder.Body.String())
	}
	logged := logBuffer.String()
	if !strings.Contains(logged, "app proxy failed") {
		t.Fatalf("expected proxy failure log entry, got %q", logged)
	}
	if !strings.Contains(logged, "dial upstream exploded") {
		t.Fatalf("expected proxy error to be logged, got %q", logged)
	}
	if !strings.Contains(logged, "route_a_app_proxy_request") {
		t.Fatalf("expected Route A observation log entry, got %q", logged)
	}
	if !strings.Contains(logged, "status=502") || !strings.Contains(logged, "upstream_error=true") {
		t.Fatalf("expected observed 502 upstream error, got %q", logged)
	}
}

func TestAppProxyRetriesTransientUpstreamErrorsWithReplayableBody(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServer(t)
	specCopy := app.Spec
	deployOp, err := storeState.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		ExecutionMode:   model.ExecutionModeManaged,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := storeState.CompleteManagedOperationWithResult(deployOp.ID, "", "deployed", &specCopy, nil); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}
	app, err = storeState.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload deployed app: %v", err)
	}

	const requestBody = `{"model":"demo","input":"hello"}`
	attempts := 0
	server.appProxyTransport = appProxyRetryTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("read attempt body: %v", err)
			}
			if got := string(body); got != requestBody {
				t.Fatalf("attempt %d body = %q, want %q", attempts, got, requestBody)
			}
			if attempts == 1 {
				return nil, errors.New("read tcp 10.42.0.1:1234->10.43.0.2:8000: read: connection reset by peer")
			}
			return &http.Response{
				StatusCode:    http.StatusOK,
				Status:        "200 OK",
				Header:        make(http.Header),
				Body:          io.NopCloser(strings.NewReader("ok")),
				ContentLength: 2,
				Request:       req,
			}, nil
		}),
		maxAttempts: 2,
	}

	req := httptest.NewRequest(http.MethodPost, "http://"+app.Route.Hostname+"/api/small-json", strings.NewReader(requestBody))
	req.Host = app.Route.Hostname
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected retry to recover with status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	if attempts != 2 {
		t.Fatalf("expected two upstream attempts, got %d", attempts)
	}
	if got := recorder.Body.String(); got != "ok" {
		t.Fatalf("expected proxied response body %q, got %q", "ok", got)
	}
}

func TestAppProxyDoesNotPrepareReplayBodyForHighRiskRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		accept string
		body   string
	}{
		{
			name: "responses",
			path: "/v1/responses",
			body: `{"model":"demo","input":"hello"}`,
		},
		{
			name: "image generation",
			path: "/v1/images/generations",
			body: `{"model":"demo","prompt":"hello"}`,
		},
		{
			name:   "sse accept",
			path:   "/api/streaming",
			accept: "text/event-stream",
			body:   `{"stream":true}`,
		},
		{
			name: "stream path",
			path: "/events/stream",
			body: `{"stream":true}`,
		},
		{
			name: "large body",
			path: "/api/small-json",
			body: strings.Repeat("x", defaultAppProxyReplayBodyLimit+1),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "http://app.example"+tt.path, strings.NewReader(tt.body))
			if tt.accept != "" {
				req.Header.Set("Accept", tt.accept)
			}
			if err := prepareAppProxyRequestForRetries(req); err != nil {
				t.Fatalf("prepare request for retries: %v", err)
			}
			if req.GetBody != nil {
				t.Fatal("expected request body to remain non-replayable")
			}
		})
	}
}

func TestAppProxyDoesNotRetryResponsesRequestBody(t *testing.T) {
	t.Parallel()

	storeState, server, _, _, app, _ := setupAppDomainTestServer(t)
	specCopy := app.Spec
	deployOp, err := storeState.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredSpec:     &specCopy,
		ExecutionMode:   model.ExecutionModeManaged,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := storeState.CompleteManagedOperationWithResult(deployOp.ID, "", "deployed", &specCopy, nil); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}
	app, err = storeState.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload deployed app: %v", err)
	}

	attempts := 0
	server.appProxyTransport = appProxyRetryTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			if _, err := io.ReadAll(req.Body); err != nil {
				t.Fatalf("read attempt body: %v", err)
			}
			return nil, errors.New("read tcp 10.42.0.1:1234->10.43.0.2:8000: read: connection reset by peer")
		}),
		maxAttempts: 3,
	}

	req := httptest.NewRequest(http.MethodPost, "http://"+app.Route.Hostname+"/v1/responses", strings.NewReader(`{"input":"hello"}`))
	req.Host = app.Route.Hostname
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadGateway, recorder.Code, recorder.Body.String())
	}
	if attempts != 1 {
		t.Fatalf("expected responses request to be attempted once, got %d attempts", attempts)
	}
}

func TestAppProxyRetryTransportDoesNotRetryNonReplayableBody(t *testing.T) {
	t.Parallel()

	attempts := 0
	transport := appProxyRetryTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			if _, err := io.ReadAll(req.Body); err != nil {
				t.Fatalf("read attempt body: %v", err)
			}
			return nil, errors.New("EOF")
		}),
		maxAttempts: 3,
	}
	req := httptest.NewRequest(http.MethodPost, "http://app.example/v1/responses", io.NopCloser(strings.NewReader("payload")))
	req.GetBody = nil

	_, err := transport.RoundTrip(req)
	if err == nil {
		t.Fatal("expected transient upstream error")
	}
	if attempts != 1 {
		t.Fatalf("expected non-replayable body to be attempted once, got %d attempts", attempts)
	}
}

func TestMaybeHandleAppProxyReturnsLiveFailureDetailsWhenReplicasAreUnavailable(t *testing.T) {
	t.Parallel()

	_, server, _, _, app, _ := setupAppDomainTestServer(t)
	managedMap := runtime.BuildManagedAppObject(app, runtime.SchedulingConstraints{})
	managed, err := runtime.ManagedAppObjectFromMap(managedMap)
	if err != nil {
		t.Fatalf("decode managed app object: %v", err)
	}
	managed.Status.Phase = runtime.ManagedAppPhaseError
	managed.Status.Message = "image pull backoff"
	managed.Status.ReadyReplicas = 0
	now := time.Now()
	server.managedAppStatusCache.setApp(managedAppStatusCacheKey(app), managedAppStatusCacheEntry{
		managed:     managed,
		found:       true,
		ok:          true,
		refreshedAt: now,
		expiresAt:   now.Add(time.Minute),
	})

	req := httptest.NewRequest(http.MethodGet, "http://"+app.Route.Hostname+"/healthz", nil)
	req.Host = app.Route.Hostname
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "app is unavailable: failed: image pull backoff") {
		t.Fatalf("expected live failure detail in body, got %q", body)
	}
}

func TestMaybeHandleAppProxyReturnsDisabledWhenDesiredReplicasAreZero(t *testing.T) {
	t.Parallel()

	stateStore, server, _, _, app, _ := setupAppDomainTestServer(t)
	replicas := 0
	scaleOp, err := stateStore.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeScale,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "test-key",
		AppID:           app.ID,
		DesiredReplicas: &replicas,
	})
	if err != nil {
		t.Fatalf("create disable operation: %v", err)
	}
	if _, err := stateStore.CompleteManagedOperation(scaleOp.ID, "", "disabled"); err != nil {
		t.Fatalf("complete disable operation: %v", err)
	}
	app, err = stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload disabled app: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://"+app.Route.Hostname+"/healthz", nil)
	req.Host = app.Route.Hostname
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "app is disabled") {
		t.Fatalf("expected disabled message, got %q", recorder.Body.String())
	}
}
