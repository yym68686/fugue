package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
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
				RoutePolicy:     model.EdgeRoutePolicyPrimary,
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
