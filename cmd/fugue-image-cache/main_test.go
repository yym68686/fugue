package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/registry"
)

func TestHydrateDeduplicatesConcurrentRequests(t *testing.T) {
	t.Parallel()

	var copies atomic.Int32
	copyStarted := make(chan struct{})
	releaseCopy := make(chan struct{})
	cache := &imageCache{
		registryBase:   "registry.fugue.internal:5000",
		localBase:      "127.0.0.1:5000",
		upstreamBase:   "upstream.example.com:5000",
		cacheEndpoint:  "http://127.0.0.1:5000",
		hydrateTimeout: 5 * time.Second,
		copyImageFn: func(ctx context.Context, src, dst string) error {
			if copies.Add(1) == 1 {
				close(copyStarted)
			}
			select {
			case <-releaseCopy:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}

	const waiters = 16
	ready := make(chan struct{}, waiters)
	start := make(chan struct{})
	errs := make(chan error, waiters)
	for range waiters {
		go func() {
			ready <- struct{}{}
			<-start
			errs <- cache.hydrate(context.Background(), "library/nginx", "latest")
		}()
	}
	for range waiters {
		<-ready
	}
	close(start)

	select {
	case <-copyStarted:
	case <-time.After(time.Second):
		t.Fatal("expected first hydrate copy to start")
	}
	time.Sleep(50 * time.Millisecond)
	if got := copies.Load(); got != 1 {
		t.Fatalf("expected one in-flight copy before release, got %d", got)
	}

	close(releaseCopy)
	for range waiters {
		if err := <-errs; err != nil {
			t.Fatalf("hydrate returned error: %v", err)
		}
	}
	if got := copies.Load(); got != 1 {
		t.Fatalf("expected concurrent hydrates to share one copy, got %d", got)
	}
}

func TestHydrateCancelsCopyWhenAllWaitersCancel(t *testing.T) {
	t.Parallel()

	copyStarted := make(chan struct{})
	copyDone := make(chan error, 1)
	cache := &imageCache{
		registryBase:   "registry.fugue.internal:5000",
		localBase:      "127.0.0.1:5000",
		upstreamBase:   "upstream.example.com:5000",
		cacheEndpoint:  "http://127.0.0.1:5000",
		hydrateTimeout: 5 * time.Second,
		copyImageFn: func(ctx context.Context, src, dst string) error {
			close(copyStarted)
			<-ctx.Done()
			copyDone <- ctx.Err()
			return ctx.Err()
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		errs <- cache.hydrate(ctx, "library/nginx", "latest")
	}()

	select {
	case <-copyStarted:
	case <-time.After(time.Second):
		t.Fatal("expected hydrate copy to start")
	}
	cancel()
	if err := <-errs; !errors.Is(err, context.Canceled) {
		t.Fatalf("hydrate error = %v, want context canceled", err)
	}
	select {
	case err := <-copyDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("copy error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected copy to be canceled after last waiter left")
	}
}

func TestHydrateKeepsCopyWhileAnotherWaiterIsActive(t *testing.T) {
	t.Parallel()

	copyStarted := make(chan struct{})
	releaseCopy := make(chan struct{})
	copyDone := make(chan error, 1)
	cache := &imageCache{
		registryBase:   "registry.fugue.internal:5000",
		localBase:      "127.0.0.1:5000",
		upstreamBase:   "upstream.example.com:5000",
		cacheEndpoint:  "http://127.0.0.1:5000",
		hydrateTimeout: 5 * time.Second,
		copyImageFn: func(ctx context.Context, src, dst string) error {
			close(copyStarted)
			select {
			case <-releaseCopy:
				copyDone <- nil
				return nil
			case <-ctx.Done():
				copyDone <- ctx.Err()
				return ctx.Err()
			}
		},
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstErr := make(chan error, 1)
	secondErr := make(chan error, 1)
	go func() {
		firstErr <- cache.hydrate(firstCtx, "library/nginx", "latest")
	}()
	select {
	case <-copyStarted:
	case <-time.After(time.Second):
		t.Fatal("expected hydrate copy to start")
	}
	go func() {
		secondErr <- cache.hydrate(context.Background(), "library/nginx", "latest")
	}()
	waitForHydrateWaiters(t, cache, 2)

	cancelFirst()
	if err := <-firstErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("first hydrate error = %v, want context canceled", err)
	}
	select {
	case err := <-copyDone:
		t.Fatalf("copy finished while second waiter was active: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCopy)
	if err := <-secondErr; err != nil {
		t.Fatalf("second hydrate returned error: %v", err)
	}
	if err := <-copyDone; err != nil {
		t.Fatalf("copy error = %v, want nil", err)
	}
}

func waitForHydrateWaiters(t *testing.T, cache *imageCache, want int) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		cache.hydrateMu.Lock()
		got := 0
		for _, call := range cache.hydrateCalls {
			if call.waiters > got {
				got = call.waiters
			}
		}
		cache.hydrateMu.Unlock()
		if got >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("expected %d hydrate waiters, got %d", want, got)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestImageCacheRejectsNonRegistryPathsWithoutPanic(t *testing.T) {
	cache := &imageCache{registry: registry.New()}
	for _, path := range []string{"", "*", "v2", "/", "/sitemap.xml", "/.well-known/security.txt"} {
		t.Run(path, func(t *testing.T) {
			req := &http.Request{
				Method: http.MethodGet,
				URL:    &url.URL{Path: path},
				Header: http.Header{},
			}
			rec := httptest.NewRecorder()

			cache.ServeHTTP(rec, req)

			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
			}
		})
	}
}

func TestImageCacheStillServesRegistryAPIBase(t *testing.T) {
	cache := &imageCache{registry: registry.New()}
	req := httptest.NewRequest(http.MethodGet, "http://image-cache.test/v2/", nil)
	rec := httptest.NewRecorder()

	cache.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Docker-Distribution-API-Version"); got != "registry/2.0" {
		t.Fatalf("Docker-Distribution-API-Version = %q, want registry/2.0", got)
	}
}

func TestBlobMissProxiesUpstreamWithoutHydrate(t *testing.T) {
	t.Parallel()

	const blobDigest = "sha256:6a0ac1617861a677b045b7ff88545213ec31c0ff08763195a70a4a5adda577bb"
	var upstreamRange string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/library/demo/blobs/"+blobDigest {
			t.Fatalf("upstream path = %q", r.URL.Path)
		}
		upstreamRange = r.Header.Get("Range")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "11")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "hello layer")
	}))
	t.Cleanup(upstream.Close)

	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	var copies atomic.Int32
	cache := &imageCache{
		registry:     registry.New(),
		upstreamBase: upstreamURL.Host,
		copyImageFn: func(context.Context, string, string) error {
			copies.Add(1)
			return nil
		},
	}
	req := httptest.NewRequest(http.MethodGet, "http://image-cache.test/v2/library/demo/blobs/"+blobDigest, nil)
	req.Header.Set("Range", "bytes=0-10")
	rec := httptest.NewRecorder()

	cache.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want %d; body=%q", rec.Code, http.StatusPartialContent, rec.Body.String())
	}
	if got := rec.Body.String(); got != "hello layer" {
		t.Fatalf("body = %q", got)
	}
	if upstreamRange != "bytes=0-10" {
		t.Fatalf("upstream Range = %q", upstreamRange)
	}
	if got := copies.Load(); got != 0 {
		t.Fatalf("copyImage calls = %d, want 0", got)
	}
}

func TestReportRegistryWriteReportsLogicalImageLocation(t *testing.T) {
	reported := make(chan url.Values, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/image-locations" {
			t.Fatalf("path = %s, want /v1/image-locations", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("Authorization = %q", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		reported <- r.Form
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	cache := &imageCache{
		apiBase:       server.URL,
		apiToken:      "token",
		reportPath:    "/v1/image-locations",
		registryBase:  "registry.fugue.internal:5000",
		cacheEndpoint: "http://10.0.0.2:5000",
		httpClient:    server.Client(),
	}
	req := httptest.NewRequest(http.MethodPut, "http://127.0.0.1:5000/v2/fugue-apps/demo/manifests/git-abc123", nil)

	cache.reportRegistryWrite(req, http.StatusCreated)

	select {
	case form := <-reported:
		if got := form.Get("image_ref"); got != "registry.fugue.internal:5000/fugue-apps/demo:git-abc123" {
			t.Fatalf("image_ref = %q", got)
		}
		if got := form.Get("status"); got != "present" {
			t.Fatalf("status = %q", got)
		}
		if got := form.Get("cache_endpoint"); got != "http://10.0.0.2:5000" {
			t.Fatalf("cache_endpoint = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected image location report")
	}
}

func TestIsRegistryAPIPath(t *testing.T) {
	tests := map[string]bool{
		"":             false,
		"*":            false,
		"/":            false,
		"/v1":          false,
		"/v2":          true,
		"/v2/":         true,
		"/v2/repo":     true,
		"/v2something": false,
		"v2":           false,
	}
	for path, want := range tests {
		if got := isRegistryAPIPath(path); got != want {
			t.Fatalf("isRegistryAPIPath(%q) = %v, want %v", path, got, want)
		}
	}
}
