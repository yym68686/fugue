package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"

	"github.com/gorilla/websocket"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestSyncOnceWritesRouteBundleCache(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_first")
	var gotToken string
	var gotEdgeGroupID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/edge/routes" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		gotToken = r.URL.Query().Get("token")
		gotEdgeGroupID = r.URL.Query().Get("edge_group_id")
		if got := r.Header.Get("If-None-Match"); got != "" {
			t.Fatalf("expected no conditional header on first sync, got %q", got)
		}
		w.Header().Set("ETag", `"routegen_first"`)
		if err := json.NewEncoder(w).Encode(bundle); err != nil {
			t.Fatalf("encode bundle: %v", err)
		}
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "routes-cache.json")
	var logs bytes.Buffer
	service := NewService(config.EdgeConfig{
		APIURL:      server.URL,
		EdgeToken:   "edge-secret",
		EdgeGroupID: "edge-group-country-hk",
		CachePath:   cachePath,
	}, log.New(&logs, "", 0))

	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatalf("sync once: %v", err)
	}
	if gotToken != "edge-secret" {
		t.Fatalf("expected edge token to be sent, got %q", gotToken)
	}
	if gotEdgeGroupID != "edge-group-country-hk" {
		t.Fatalf("expected edge group filter, got %q", gotEdgeGroupID)
	}

	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	var cached cacheFile
	if err := json.Unmarshal(data, &cached); err != nil {
		t.Fatalf("decode cache: %v", err)
	}
	if cached.Bundle.Version != "routegen_first" || cached.ETag != `"routegen_first"` {
		t.Fatalf("unexpected cached bundle: %+v", cached)
	}
	status := service.Status()
	if !status.Healthy || status.Status != "ok" || status.BundleVersion != "routegen_first" || status.RouteCount != 1 {
		t.Fatalf("unexpected status after sync: %+v", status)
	}
	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_info{edge_id="",edge_group_id="edge-group-country-hk"} 1`,
		`fugue_edge_bundle_sync_total{result="success"} 1`,
		`fugue_edge_bundle_sync_total{result="not_modified"} 0`,
		`fugue_edge_bundle_sync_total{result="error"} 0`,
		`fugue_edge_cache_write_total{result="success"} 1`,
		`fugue_edge_cache_write_total{result="error"} 0`,
		`fugue_edge_bundle_sync_duration_seconds`,
		`fugue_edge_bundle_age_seconds`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
	if !strings.Contains(logs.String(), "edge route bundle sync success; version=routegen_first routes=1 tls_allowlist=1") {
		t.Fatalf("expected sync success log, got %s", logs.String())
	}
}

func TestLoadCacheFallsBackToPreviousVerifiedGeneration(t *testing.T) {
	t.Parallel()

	cachePath := filepath.Join(t.TempDir(), "routes-cache.json")
	service := NewService(config.EdgeConfig{
		CachePath:         cachePath,
		CacheArchiveLimit: 3,
		MaxStale:          time.Hour,
	}, log.New(ioDiscard{}, "", 0))
	if err := service.writeCache(cacheFile{
		Version:  cacheFileVersion,
		ETag:     `"routegen_old"`,
		CachedAt: time.Now().UTC(),
		Bundle:   testBundle("routegen_old"),
	}); err != nil {
		t.Fatalf("write old cache: %v", err)
	}
	if err := service.writeCache(cacheFile{
		Version:  cacheFileVersion,
		ETag:     `"routegen_new"`,
		CachedAt: time.Now().UTC(),
		Bundle:   testBundle("routegen_new"),
	}); err != nil {
		t.Fatalf("write new cache: %v", err)
	}
	if err := os.WriteFile(cachePath, []byte("{corrupt"), 0o600); err != nil {
		t.Fatalf("corrupt current cache: %v", err)
	}

	if err := service.LoadCache(); err != nil {
		t.Fatalf("load cache with fallback: %v", err)
	}
	status := service.Status()
	if status.ServingGeneration != "routegen_old" || status.CacheCorruptGeneration != "unknown" {
		t.Fatalf("expected previous LKG after corrupt current cache, got %+v", status)
	}
}

func TestHeartbeatOnceReportsEdgeInventory(t *testing.T) {
	t.Parallel()

	var gotToken string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/edge/heartbeat" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		gotToken = r.URL.Query().Get("token")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode heartbeat: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "node": gotBody})
	}))
	defer server.Close()

	service := NewService(config.EdgeConfig{
		APIURL:            server.URL,
		EdgeToken:         "edge-secret",
		EdgeID:            "edge-us-1",
		EdgeGroupID:       "edge-group-country-us",
		Region:            "us-east",
		Country:           "US",
		PublicIPv4:        "203.0.113.10",
		MeshIP:            "100.64.0.10",
		HeartbeatInterval: time.Minute,
	}, log.New(ioDiscard{}, "", 0))
	bundle := testBundle("routegen_heartbeat")
	bundle.EdgeID = "edge-us-1"
	bundle.EdgeGroupID = "edge-group-country-us"
	service.recordSyncSuccess(bundle, `"routegen_heartbeat"`, time.Now().UTC(), false)
	service.recordCaddyApply("routegen_heartbeat", 1, nil)

	if err := service.HeartbeatOnce(context.Background()); err != nil {
		t.Fatalf("heartbeat once: %v", err)
	}
	if gotToken != "edge-secret" {
		t.Fatalf("expected edge token, got %q", gotToken)
	}
	for key, want := range map[string]any{
		"edge_id":               "edge-us-1",
		"edge_group_id":         "edge-group-country-us",
		"region":                "us-east",
		"country":               "US",
		"public_ipv4":           "203.0.113.10",
		"mesh_ip":               "100.64.0.10",
		"route_bundle_version":  "routegen_heartbeat",
		"caddy_applied_version": "routegen_heartbeat",
		"cache_status":          "ready",
		"status":                model.EdgeHealthHealthy,
		"healthy":               true,
		"draining":              false,
	} {
		if got := gotBody[key]; got != want {
			t.Fatalf("heartbeat field %s: expected %#v, got %#v in %#v", key, want, got, gotBody)
		}
	}
	if got := gotBody["caddy_route_count"]; got != float64(1) {
		t.Fatalf("expected caddy_route_count 1, got %#v", got)
	}
}

func TestSyncOnceNotModifiedKeepsExistingCacheFile(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_cached")
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		switch requests {
		case 1:
			w.Header().Set("ETag", `"routegen_cached"`)
			if err := json.NewEncoder(w).Encode(bundle); err != nil {
				t.Fatalf("encode bundle: %v", err)
			}
		case 2:
			if got := r.Header.Get("If-None-Match"); got != `"routegen_cached"` {
				t.Fatalf("expected If-None-Match header %q, got %q", `"routegen_cached"`, got)
			}
			w.WriteHeader(http.StatusNotModified)
		default:
			t.Fatalf("unexpected request %d", requests)
		}
	}))
	defer server.Close()

	cachePath := filepath.Join(t.TempDir(), "routes-cache.json")
	service := NewService(config.EdgeConfig{
		APIURL:    server.URL,
		EdgeToken: "edge-secret",
		CachePath: cachePath,
	}, log.New(ioDiscard{}, "", 0))

	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	before, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read first cache: %v", err)
	}
	if err := service.SyncOnce(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	after, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("read second cache: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("expected 304 sync to leave cache unchanged")
	}
	status := service.Status()
	if !status.Healthy || status.Status != "ok" || status.StaleCache {
		t.Fatalf("unexpected status after 304: %+v", status)
	}
	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_bundle_sync_total{result="success"} 1`,
		`fugue_edge_bundle_sync_total{result="not_modified"} 1`,
		`fugue_edge_bundle_sync_total{result="error"} 0`,
		`fugue_edge_cache_write_total{result="success"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestSyncOnceUsesStaleCacheWhenAPIUnavailable(t *testing.T) {
	t.Parallel()

	cachePath := filepath.Join(t.TempDir(), "routes-cache.json")
	writeTestCache(t, cachePath, testBundle("routegen_stale"), `"routegen_stale"`)
	var logs bytes.Buffer
	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
		CachePath: cachePath,
	}, log.New(&logs, "", 0))
	service.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("control plane unavailable")
	})}

	if err := service.LoadCache(); err != nil {
		t.Fatalf("load cache: %v", err)
	}
	if err := service.SyncOnce(context.Background()); err == nil {
		t.Fatal("expected sync error")
	}
	status := service.Status()
	if !status.Healthy || status.Status != "stale" || !status.StaleCache || status.BundleVersion != "routegen_stale" {
		t.Fatalf("expected stale but healthy status, got %+v", status)
	}
	bundle, ok := service.Bundle()
	if !ok || bundle.Version != "routegen_stale" {
		t.Fatalf("expected cached bundle to remain available, got ok=%v bundle=%+v", ok, bundle)
	}
	recorder := httptest.NewRecorder()
	service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected stale cache health to be 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	metrics := httptest.NewRecorder()
	service.Handler().ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metrics.Code != http.StatusOK {
		t.Fatalf("expected metrics status 200, got %d body=%s", metrics.Code, metrics.Body.String())
	}
	if !strings.Contains(metrics.Body.String(), `fugue_edge_stale_cache 1`) ||
		!strings.Contains(metrics.Body.String(), `fugue_edge_routes{bundle_version="routegen_stale"} 1`) ||
		!strings.Contains(metrics.Body.String(), `fugue_edge_bundle_sync_total{result="error"} 1`) ||
		!strings.Contains(metrics.Body.String(), `fugue_edge_cache_load_total{result="success"} 1`) {
		t.Fatalf("expected stale cache metrics, got %s", metrics.Body.String())
	}
	logOutput := logs.String()
	if !strings.Contains(logOutput, "edge route cache loaded; version=routegen_stale") ||
		!strings.Contains(logOutput, "edge route bundle sync failed; using stale cache; version=routegen_stale") {
		t.Fatalf("expected cache loaded and stale fallback logs, got %s", logOutput)
	}
}

func TestSyncOnceWithoutCacheIsUnhealthyWhenAPIUnavailable(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
		CachePath: filepath.Join(t.TempDir(), "routes-cache.json"),
	}, log.New(ioDiscard{}, "", 0))
	service.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("control plane unavailable")
	})}

	if err := service.SyncOnce(context.Background()); err == nil {
		t.Fatal("expected sync error")
	}
	status := service.Status()
	if status.Healthy || status.Status != "unhealthy" || status.BundleVersion != "" {
		t.Fatalf("expected unhealthy status without cache, got %+v", status)
	}
	recorder := httptest.NewRecorder()
	service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected health status %d, got %d body=%s", http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	}
	metrics := renderMetrics(t, service)
	if !strings.Contains(metrics, `fugue_edge_bundle_sync_total{result="error"} 1`) {
		t.Fatalf("expected sync error metric, got %s", metrics)
	}
}

func TestSyncErrorsAndLogsRedactEdgeToken(t *testing.T) {
	t.Parallel()

	const secret = "edge-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad token "+r.URL.Query().Get("token"), http.StatusInternalServerError)
	}))
	defer server.Close()

	var logs bytes.Buffer
	service := NewService(config.EdgeConfig{
		APIURL:    server.URL,
		EdgeToken: secret,
		CachePath: filepath.Join(t.TempDir(), "routes-cache.json"),
	}, log.New(&logs, "", 0))
	err := service.SyncOnce(context.Background())
	if err == nil {
		t.Fatal("expected sync error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("sync error leaked token: %v", err)
	}
	status := service.Status()
	if strings.Contains(status.LastError, secret) {
		t.Fatalf("status leaked token: %+v", status)
	}
	service.logSyncFailure(err)
	if strings.Contains(logs.String(), secret) {
		t.Fatalf("log leaked token: %s", logs.String())
	}
}

func TestLoadCacheMissAndErrorMetrics(t *testing.T) {
	t.Parallel()

	cachePath := filepath.Join(t.TempDir(), "routes-cache.json")
	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
		CachePath: cachePath,
	}, log.New(ioDiscard{}, "", 0))

	if err := service.LoadCache(); err != nil {
		t.Fatalf("missing cache should not fail: %v", err)
	}
	metrics := renderMetrics(t, service)
	if !strings.Contains(metrics, `fugue_edge_cache_load_total{result="miss"} 1`) {
		t.Fatalf("expected cache miss metric, got %s", metrics)
	}

	if err := os.WriteFile(cachePath, []byte("{"), 0o600); err != nil {
		t.Fatalf("write invalid cache: %v", err)
	}
	if err := service.LoadCache(); err == nil {
		t.Fatal("expected invalid cache to fail")
	}
	metrics = renderMetrics(t, service)
	if !strings.Contains(metrics, `fugue_edge_cache_load_total{result="error"} 1`) {
		t.Fatalf("expected cache error metric, got %s", metrics)
	}
}

func TestApplyCaddyConfigBuildsHostRoutesForBundle(t *testing.T) {
	t.Parallel()

	var loads []string
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/load" {
			t.Fatalf("unexpected caddy admin request %s %s", r.Method, r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read caddy config body: %v", err)
		}
		loads = append(loads, string(body))
		w.WriteHeader(http.StatusOK)
	}))
	defer admin.Close()

	bundle := testBundle("routegen_caddy")
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	bundle.Routes = append(bundle.Routes, custom)

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        admin.URL,
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: ":7833",
	}, log.New(ioDiscard{}, "", 0))

	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply caddy config: %v", err)
	}
	if err := service.applyCaddyConfig(context.Background(), bundle); err != nil {
		t.Fatalf("apply unchanged caddy config: %v", err)
	}
	if len(loads) != 1 {
		t.Fatalf("expected unchanged bundle version to be applied once, got %d loads", len(loads))
	}
	adminURL, err := url.Parse(admin.URL)
	if err != nil {
		t.Fatalf("parse admin url: %v", err)
	}
	for _, want := range []string{
		`"listen":"` + adminURL.Host + `"`,
		`"listen":["127.0.0.1:18080"]`,
		`"automatic_https":{"disable":true}`,
		`"host":["demo.fugue.pro"]`,
		`"host":["www.customer.com"]`,
		`"dial":"127.0.0.1:7833"`,
		`"X-Fugue-Edge-Route-Host":["demo.fugue.pro"]`,
		`"X-Fugue-Edge-Route-Host":["www.customer.com"]`,
		`"default_logger_name":"fugue_edge_access"`,
		`"include":["http.log.access.fugue_edge_access"]`,
	} {
		if !strings.Contains(loads[0], want) {
			t.Fatalf("caddy config missing %q:\n%s", want, loads[0])
		}
	}
	status := service.Status()
	if status.CaddyAppliedVersion != "routegen_caddy" || status.CaddyTLSMode != caddyTLSModeOff || status.CaddyLastError != "" {
		t.Fatalf("unexpected caddy status: %+v", status)
	}
	metrics := renderMetrics(t, service)
	if !strings.Contains(metrics, `fugue_edge_caddy_config_apply_total{result="success"} 1`) ||
		!strings.Contains(metrics, `fugue_edge_caddy_routes{bundle_version="routegen_caddy"} 2`) {
		t.Fatalf("expected caddy apply metrics, got %s", metrics)
	}
}

func TestBuildCaddyConfigSupportsInternalTLSCanary(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_tls")
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	bundle.Routes = append(bundle.Routes, custom)

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      ":18443",
		CaddyTLSMode:         caddyTLSModeInternal,
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	configBody, routeCount, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	if routeCount != 2 {
		t.Fatalf("expected 2 caddy routes, got %d", routeCount)
	}
	if strings.Contains(string(configBody), `"automatic_https":{"disable":true}`) {
		t.Fatalf("internal tls config should not disable automatic https:\n%s", configBody)
	}

	var parsed map[string]any
	if err := json.Unmarshal(configBody, &parsed); err != nil {
		t.Fatalf("decode caddy config: %v", err)
	}
	apps := parsed["apps"].(map[string]any)
	httpApp := apps["http"].(map[string]any)
	servers := httpApp["servers"].(map[string]any)
	server := servers["fugue_edge"].(map[string]any)
	policies := server["tls_connection_policies"].([]any)
	if len(policies) != 1 {
		t.Fatalf("expected one TLS connection policy, got %#v", policies)
	}
	tlsApp := apps["tls"].(map[string]any)
	automation := tlsApp["automation"].(map[string]any)
	automationPolicies := automation["policies"].([]any)
	if len(automationPolicies) != 1 {
		t.Fatalf("expected one TLS automation policy, got %#v", automationPolicies)
	}
	policy := automationPolicies[0].(map[string]any)
	subjects := policy["subjects"].([]any)
	if got := fmt.Sprint(subjects); got != "[demo.fugue.pro www.customer.com]" {
		t.Fatalf("unexpected TLS subjects: %s", got)
	}
	issuers := policy["issuers"].([]any)
	issuer := issuers[0].(map[string]any)
	if issuer["module"] != caddyTLSModeInternal {
		t.Fatalf("unexpected TLS issuer: %#v", issuer)
	}
}

func TestBuildCaddyConfigSkipsRouteAOnlyHosts(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_route_a_only_caddy")
	routeAOnly := bundle.Routes[0]
	routeAOnly.Hostname = "legacy.fugue.pro"
	routeAOnly.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
	routeAOnly.RouteGeneration = "routegen_legacy"
	bundle.Routes = append(bundle.Routes, routeAOnly)

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	configBody, routeCount, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	if routeCount != 1 {
		t.Fatalf("expected only the opt-in route to be emitted, got %d", routeCount)
	}
	configText := string(configBody)
	if !strings.Contains(configText, `"host":["demo.fugue.pro"]`) {
		t.Fatalf("expected canary host in caddy config:\n%s", configText)
	}
	if strings.Contains(configText, "legacy.fugue.pro") {
		t.Fatalf("Route A-only host must not be emitted to caddy config:\n%s", configText)
	}
}

func TestBuildCaddyConfigSkipsDifferentEdgeGroupRoutes(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_edge_group_caddy")
	bundle.Routes[0].EdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].RuntimeEdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].PolicyEdgeGroupID = "edge-group-country-us"
	hkRoute := bundle.Routes[0]
	hkRoute.Hostname = "hk.fugue.pro"
	hkRoute.EdgeGroupID = "edge-group-country-hk"
	hkRoute.RuntimeEdgeGroupID = "edge-group-country-hk"
	hkRoute.PolicyEdgeGroupID = "edge-group-country-hk"
	hkRoute.RouteGeneration = "routegen_hk"
	remoteRuntimeRoute := bundle.Routes[0]
	remoteRuntimeRoute.Hostname = "remote-runtime.fugue.pro"
	remoteRuntimeRoute.RuntimeEdgeGroupID = "edge-group-country-hk"
	remoteRuntimeRoute.PolicyEdgeGroupID = "edge-group-country-us"
	remoteRuntimeRoute.RouteGeneration = "routegen_remote_runtime"
	bundle.Routes = append(bundle.Routes, hkRoute, remoteRuntimeRoute)

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-country-us",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	configBody, routeCount, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	if routeCount != 2 {
		t.Fatalf("expected local serving edge group routes to be emitted, got %d", routeCount)
	}
	configText := string(configBody)
	if !strings.Contains(configText, `"host":["demo.fugue.pro"]`) {
		t.Fatalf("expected local edge group host in caddy config:\n%s", configText)
	}
	if strings.Contains(configText, "hk.fugue.pro") {
		t.Fatalf("different edge group host must not be emitted to caddy config:\n%s", configText)
	}
	if !strings.Contains(configText, `"host":["remote-runtime.fugue.pro"]`) {
		t.Fatalf("expected nearest-edge fallback route to be emitted by serving edge group:\n%s", configText)
	}
}

func TestBuildCaddyConfigAddsNotFoundFallbackForUnmatchedHosts(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_fallback")
	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	configBody, routeCount, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	if routeCount != 1 {
		t.Fatalf("expected one emitted host route, got %d", routeCount)
	}
	configText := string(configBody)
	if !strings.Contains(configText, `"handler":"static_response"`) ||
		!strings.Contains(configText, `"status_code":404`) ||
		!strings.Contains(configText, "fugue route not found") {
		t.Fatalf("expected 404 fallback route in caddy config:\n%s", configText)
	}
}

func TestBuildCaddyConfigSupportsPublicOnDemandTLSCanary(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_public_tls")
	service := NewService(config.EdgeConfig{
		APIURL:                 "https://api.example.invalid",
		EdgeToken:              "edge-secret",
		EdgeGroupID:            "edge-group-default",
		ListenAddr:             ":7832",
		CaddyEnabled:           true,
		CaddyAdminURL:          "http://127.0.0.1:2019",
		CaddyListenAddr:        ":443",
		CaddyTLSMode:           caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr:   "127.0.0.1:7833",
		CaddyStaticTLSCertFile: "/etc/caddy/static-tls/tls.crt",
		CaddyStaticTLSKeyFile:  "/etc/caddy/static-tls/tls.key",
	}, log.New(ioDiscard{}, "", 0))

	configBody, routeCount, err := service.buildCaddyConfig(bundle)
	if err != nil {
		t.Fatalf("build caddy config: %v", err)
	}
	if routeCount != 1 {
		t.Fatalf("expected 1 caddy route, got %d", routeCount)
	}
	if strings.Contains(string(configBody), "edge-secret") {
		t.Fatalf("public on-demand caddy config must not contain edge token:\n%s", configBody)
	}

	var parsed map[string]any
	if err := json.Unmarshal(configBody, &parsed); err != nil {
		t.Fatalf("decode caddy config: %v", err)
	}
	apps := parsed["apps"].(map[string]any)
	httpApp := apps["http"].(map[string]any)
	servers := httpApp["servers"].(map[string]any)
	server := servers["fugue_edge"].(map[string]any)
	if fmt.Sprint(server["listen"]) != "[:443]" {
		t.Fatalf("unexpected caddy listen: %#v", server["listen"])
	}
	if policies := server["tls_connection_policies"].([]any); len(policies) != 1 {
		t.Fatalf("expected one TLS connection policy, got %#v", policies)
	}
	tlsApp := apps["tls"].(map[string]any)
	automation := tlsApp["automation"].(map[string]any)
	automationPolicies := automation["policies"].([]any)
	if len(automationPolicies) != 1 || automationPolicies[0].(map[string]any)["on_demand"] != true {
		t.Fatalf("expected on-demand TLS automation policy, got %#v", automationPolicies)
	}
	onDemand := automation["on_demand"].(map[string]any)
	permission := onDemand["permission"].(map[string]any)
	if permission["module"] != "http" || permission["endpoint"] != "http://127.0.0.1:7832/edge/tls/ask" {
		t.Fatalf("unexpected on-demand permission: %#v", permission)
	}
	certificates := tlsApp["certificates"].(map[string]any)
	loadFiles := certificates["load_files"].([]any)
	if len(loadFiles) != 1 {
		t.Fatalf("expected one static certificate load file entry, got %#v", loadFiles)
	}
	staticCert := loadFiles[0].(map[string]any)
	if staticCert["certificate"] != "/etc/caddy/static-tls/tls.crt" || staticCert["key"] != "/etc/caddy/static-tls/tls.key" {
		t.Fatalf("unexpected static certificate config: %#v", staticCert)
	}
}

func TestCaddyAdminURLMustBeLoopback(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://10.0.0.10:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	err := service.validateConfig()
	if err == nil || !strings.Contains(err.Error(), "localhost") {
		t.Fatalf("expected non-loopback caddy admin URL to be rejected, got %v", err)
	}
}

func TestCaddyModeRequiresEdgeGroupID(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	err := service.validateConfig()
	if err == nil || !strings.Contains(err.Error(), "FUGUE_EDGE_GROUP_ID") {
		t.Fatalf("expected caddy mode without edge group to be rejected, got %v", err)
	}
}

func TestTLSAskOnlyAllowsActiveBundleHosts(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_tls_ask")
	disabled := bundle.Routes[0]
	disabled.Hostname = "disabled.fugue.pro"
	disabled.Status = model.EdgeRouteStatusDisabled
	bundle.Routes = append(bundle.Routes, disabled)

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_tls_ask"`, now, false)

	for _, tc := range []struct {
		name string
		host string
		code int
	}{
		{name: "active", host: "demo.fugue.pro", code: http.StatusOK},
		{name: "unknown", host: "unknown.fugue.pro", code: http.StatusForbidden},
		{name: "disabled", host: "disabled.fugue.pro", code: http.StatusForbidden},
		{name: "missing", host: "", code: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/edge/tls/ask?domain="+url.QueryEscape(tc.host), nil)
			recorder := httptest.NewRecorder()
			service.Handler().ServeHTTP(recorder, req)
			if recorder.Code != tc.code {
				t.Fatalf("expected %d, got %d body=%s", tc.code, recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestCaddyTLSModeMustBeKnown(t *testing.T) {
	t.Parallel()

	service := NewService(config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyTLSMode:         "public",
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}, log.New(ioDiscard{}, "", 0))

	err := service.validateConfig()
	if err == nil || !strings.Contains(err.Error(), "FUGUE_EDGE_CADDY_TLS_MODE") {
		t.Fatalf("expected unknown caddy tls mode to be rejected, got %v", err)
	}
}

func TestCaddyStaticTLSFilesMustBePairedAndRequireTLS(t *testing.T) {
	t.Parallel()

	base := config.EdgeConfig{
		APIURL:               "https://api.example.invalid",
		EdgeToken:            "edge-secret",
		EdgeGroupID:          "edge-group-default",
		CaddyEnabled:         true,
		CaddyAdminURL:        "http://127.0.0.1:2019",
		CaddyListenAddr:      "127.0.0.1:18080",
		CaddyTLSMode:         caddyTLSModePublicOnDemand,
		CaddyProxyListenAddr: "127.0.0.1:7833",
	}
	onlyCert := base
	onlyCert.CaddyStaticTLSCertFile = "/etc/caddy/static-tls/tls.crt"
	if err := NewService(onlyCert, log.New(ioDiscard{}, "", 0)).validateConfig(); err == nil || !strings.Contains(err.Error(), "must be configured together") {
		t.Fatalf("expected unpaired static TLS file to be rejected, got %v", err)
	}

	offTLS := base
	offTLS.CaddyTLSMode = caddyTLSModeOff
	offTLS.CaddyStaticTLSCertFile = "/etc/caddy/static-tls/tls.crt"
	offTLS.CaddyStaticTLSKeyFile = "/etc/caddy/static-tls/tls.key"
	if err := NewService(offTLS, log.New(ioDiscard{}, "", 0)).validateConfig(); err == nil || !strings.Contains(err.Error(), "requires FUGUE_EDGE_CADDY_TLS_MODE") {
		t.Fatalf("expected static TLS with TLS off to be rejected, got %v", err)
	}
}

func TestProxyHandlerRoutesPlatformAndCustomDomainsWithStreamingMetrics(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Fugue-Edge-App-ID") != "app_demo" {
			t.Fatalf("expected app id header, got %q", r.Header.Get("X-Fugue-Edge-App-ID"))
		}
		switch r.URL.Path {
		case "/upload":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read upload body: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, "uploaded:%s:%s", r.Header.Get("X-Forwarded-Host"), body)
		case "/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "data: ready\n\n")
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()

	bundle := testBundle("routegen_proxy")
	bundle.Routes[0].UpstreamURL = backend.URL
	custom := bundle.Routes[0]
	custom.Hostname = "www.customer.com"
	custom.RouteKind = model.EdgeRouteKindCustomDomain
	custom.TLSPolicy = model.EdgeRouteTLSPolicyCustomDomain
	bundle.Routes = append(bundle.Routes, custom)

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_proxy"`, now, false)

	upload := httptest.NewRecorder()
	uploadReq := httptest.NewRequest(http.MethodPost, "http://demo.fugue.pro/upload", strings.NewReader("payload"))
	service.ProxyHandler().ServeHTTP(upload, uploadReq)
	if upload.Code != http.StatusCreated || upload.Body.String() != "uploaded:demo.fugue.pro:payload" {
		t.Fatalf("unexpected upload response status=%d body=%q", upload.Code, upload.Body.String())
	}

	events := httptest.NewRecorder()
	eventsReq := httptest.NewRequest(http.MethodGet, "http://www.customer.com/events", nil)
	eventsReq.Header.Set("Accept", "text/event-stream")
	service.ProxyHandler().ServeHTTP(events, eventsReq)
	if events.Code != http.StatusOK || !strings.Contains(events.Body.String(), "data: ready") {
		t.Fatalf("unexpected sse response status=%d body=%q", events.Code, events.Body.String())
	}

	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_route_requests_total{hostname="demo.fugue.pro",app="app_demo",route_kind="platform"} 1`,
		`fugue_edge_route_requests_total{hostname="www.customer.com",app="app_demo",route_kind="custom-domain"} 1`,
		`fugue_edge_route_status_total{hostname="demo.fugue.pro",app="app_demo",route_kind="platform",status="201"} 1`,
		`fugue_edge_route_status_total{hostname="www.customer.com",app="app_demo",route_kind="custom-domain",status="200"} 1`,
		`fugue_edge_route_upload_requests_total{hostname="demo.fugue.pro",app="app_demo",route_kind="platform"} 1`,
		`fugue_edge_route_sse_total{hostname="www.customer.com",app="app_demo",route_kind="custom-domain",result="success"} 1`,
		`fugue_edge_route_streaming_total{hostname="www.customer.com",app="app_demo",route_kind="custom-domain",result="success"} 1`,
		`fugue_edge_route_upstream_latency_seconds_count{hostname="demo.fugue.pro",app="app_demo",route_kind="platform"} 1`,
		`fugue_edge_route_upstream_latency_seconds_count{hostname="www.customer.com",app="app_demo",route_kind="custom-domain"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestProxyHandlerIgnoresRouteAOnlyHosts(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	bundle := testBundle("routegen_route_a_only_proxy")
	bundle.Routes[0].UpstreamURL = backend.URL
	routeAOnly := bundle.Routes[0]
	routeAOnly.Hostname = "legacy.fugue.pro"
	routeAOnly.RoutePolicy = model.EdgeRoutePolicyRouteAOnly
	routeAOnly.RouteGeneration = "routegen_legacy"
	bundle.Routes = append(bundle.Routes, routeAOnly)

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_route_a_only_proxy"`, time.Now().UTC(), false)

	canary := httptest.NewRecorder()
	canaryReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(canary, canaryReq)
	if canary.Code != http.StatusOK || canary.Body.String() != "ok" {
		t.Fatalf("unexpected canary response status=%d body=%q", canary.Code, canary.Body.String())
	}

	legacy := httptest.NewRecorder()
	legacyReq := httptest.NewRequest(http.MethodGet, "http://legacy.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(legacy, legacyReq)
	if legacy.Code != http.StatusNotFound {
		t.Fatalf("expected Route A-only route to be ignored by edge proxy, got %d body=%s", legacy.Code, legacy.Body.String())
	}
}

func TestProxyHandlerSkipsDifferentEdgeGroupRoutes(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	bundle := testBundle("routegen_edge_group_proxy")
	bundle.Routes[0].EdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].RuntimeEdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].PolicyEdgeGroupID = "edge-group-country-us"
	bundle.Routes[0].UpstreamURL = backend.URL
	hkRoute := bundle.Routes[0]
	hkRoute.Hostname = "hk.fugue.pro"
	hkRoute.EdgeGroupID = "edge-group-country-hk"
	hkRoute.RuntimeEdgeGroupID = "edge-group-country-hk"
	hkRoute.PolicyEdgeGroupID = "edge-group-country-hk"
	hkRoute.RouteGeneration = "routegen_hk"
	remoteRuntimeRoute := bundle.Routes[0]
	remoteRuntimeRoute.Hostname = "remote-runtime.fugue.pro"
	remoteRuntimeRoute.RuntimeEdgeGroupID = "edge-group-country-hk"
	remoteRuntimeRoute.PolicyEdgeGroupID = "edge-group-country-us"
	remoteRuntimeRoute.RouteGeneration = "routegen_remote_runtime"
	bundle.Routes = append(bundle.Routes, hkRoute, remoteRuntimeRoute)

	service := NewService(config.EdgeConfig{
		APIURL:      "https://api.example.invalid",
		EdgeToken:   "edge-secret",
		EdgeGroupID: "edge-group-country-us",
	}, log.New(ioDiscard{}, "", 0))
	service.recordSyncSuccess(bundle, `"routegen_edge_group_proxy"`, time.Now().UTC(), false)

	local := httptest.NewRecorder()
	localReq := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(local, localReq)
	if local.Code != http.StatusOK || local.Body.String() != "ok" {
		t.Fatalf("unexpected local edge group response status=%d body=%q", local.Code, local.Body.String())
	}

	otherGroup := httptest.NewRecorder()
	otherGroupReq := httptest.NewRequest(http.MethodGet, "http://hk.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(otherGroup, otherGroupReq)
	if otherGroup.Code != http.StatusNotFound {
		t.Fatalf("expected different edge group route to be ignored by edge proxy, got %d body=%s", otherGroup.Code, otherGroup.Body.String())
	}

	remoteRuntime := httptest.NewRecorder()
	remoteRuntimeReq := httptest.NewRequest(http.MethodGet, "http://remote-runtime.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(remoteRuntime, remoteRuntimeReq)
	if remoteRuntime.Code != http.StatusOK || remoteRuntime.Body.String() != "ok" {
		t.Fatalf("expected nearest-edge fallback route to be served by local edge group, got %d body=%s", remoteRuntime.Code, remoteRuntime.Body.String())
	}
}

func TestProxyHandlerReturnsUnavailableForInactiveRoute(t *testing.T) {
	t.Parallel()

	bundle := testBundle("routegen_disabled")
	bundle.Routes[0].Status = model.EdgeRouteStatusDisabled
	bundle.Routes[0].StatusReason = "app is disabled"

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_disabled"`, now, false)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected disabled route to return 503, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_route_status_total{hostname="demo.fugue.pro",app="app_demo",route_kind="platform",status="503"} 1`,
		`fugue_edge_route_fallback_hits_total{hostname="demo.fugue.pro",app="app_demo",route_kind="platform"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestProxyHandlerRecordsUpstreamErrors(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	backendURL := backend.URL
	backend.Close()

	bundle := testBundle("routegen_upstream_error")
	bundle.Routes[0].UpstreamURL = backendURL

	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_upstream_error"`, now, false)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://demo.fugue.pro/", nil)
	service.ProxyHandler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("expected upstream error to return 502, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	metrics := renderMetrics(t, service)
	for _, want := range []string{
		`fugue_edge_route_status_total{hostname="demo.fugue.pro",app="app_demo",route_kind="platform",status="502"} 1`,
		`fugue_edge_route_upstream_errors_total{hostname="demo.fugue.pro",app="app_demo",route_kind="platform"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics)
		}
	}
}

func TestProxyHandlerSupportsWebSocket(t *testing.T) {
	t.Parallel()

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
			backendErrors <- fmt.Errorf("unexpected websocket payload %q", string(payload))
			return
		}
		if err := conn.WriteMessage(messageType, []byte("pong")); err != nil {
			backendErrors <- err
			return
		}
		backendErrors <- nil
	}))
	defer backend.Close()

	bundle := testBundle("routegen_websocket")
	bundle.Routes[0].UpstreamURL = backend.URL
	service := NewService(config.EdgeConfig{
		APIURL:    "https://api.example.invalid",
		EdgeToken: "edge-secret",
	}, log.New(ioDiscard{}, "", 0))
	now := time.Now().UTC()
	service.recordSyncSuccess(bundle, `"routegen_websocket"`, now, false)

	proxy := httptest.NewServer(service.ProxyHandler())
	defer proxy.Close()
	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy url: %v", err)
	}
	dialer := websocket.Dialer{
		NetDialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, proxyURL.Host)
		},
	}

	conn, resp, err := dialer.Dial("ws://demo.fugue.pro/ws", nil)
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
		t.Fatalf("set websocket read deadline: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write websocket message: %v", err)
	}
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket response: %v", err)
	}
	if string(payload) != "pong" {
		t.Fatalf("expected websocket payload pong, got %q", string(payload))
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close websocket: %v", err)
	}
	select {
	case err := <-backendErrors:
		if err != nil {
			t.Fatalf("backend websocket validation failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for backend websocket validation")
	}

	wantMetric := `fugue_edge_route_websocket_total{hostname="demo.fugue.pro",app="app_demo",route_kind="platform",result="success"} 1`
	deadline := time.Now().Add(2 * time.Second)
	for {
		metrics := renderMetrics(t, service)
		if strings.Contains(metrics, wantMetric) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected websocket 101 metric, got %s", metrics)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func testBundle(version string) model.EdgeRouteBundle {
	now := time.Date(2026, 5, 10, 1, 2, 3, 0, time.UTC)
	return model.EdgeRouteBundle{
		Version:     version,
		GeneratedAt: now,
		Routes: []model.EdgeRouteBinding{
			{
				Hostname:        "demo.fugue.pro",
				RouteKind:       model.EdgeRouteKindPlatform,
				AppID:           "app_demo",
				TenantID:        "tenant_demo",
				RuntimeID:       model.DefaultManagedRuntimeID,
				EdgeGroupID:     "edge-group-default",
				RoutePolicy:     model.EdgeRoutePolicyCanary,
				UpstreamKind:    model.EdgeRouteUpstreamKindKubernetesService,
				UpstreamURL:     "http://demo.fg-tenant-demo.svc.cluster.local:8080",
				ServicePort:     8080,
				TLSPolicy:       model.EdgeRouteTLSPolicyPlatform,
				Streaming:       true,
				Status:          model.EdgeRouteStatusActive,
				RouteGeneration: "routegen_demo",
				CreatedAt:       now,
				UpdatedAt:       now,
			},
		},
		TLSAllowlist: []model.EdgeTLSAllowlistEntry{
			{
				Hostname: "www.example.com",
				AppID:    "app_demo",
				TenantID: "tenant_demo",
				Status:   model.AppDomainStatusVerified,
			},
		},
	}
}

func writeTestCache(t *testing.T, path string, bundle model.EdgeRouteBundle, etag string) {
	t.Helper()
	cached := cacheFile{
		Version:  cacheFileVersion,
		ETag:     etag,
		CachedAt: time.Date(2026, 5, 10, 2, 0, 0, 0, time.UTC),
		Bundle:   bundle,
	}
	data, err := json.MarshalIndent(cached, "", "  ")
	if err != nil {
		t.Fatalf("marshal cache: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create cache dir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write cache: %v", err)
	}
}

func renderMetrics(t *testing.T, service *Service) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected metrics status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	return recorder.Body.String()
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}
