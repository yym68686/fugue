package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/registry"
)

func TestRunImageCacheHTTPServerShutsDownAndDrainsBackgroundWork(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	cache := &imageCache{lifecycle: lifecycle}
	backgroundStarted := make(chan struct{})
	backgroundStopped := make(chan struct{})
	if !cache.startBackground(func(ctx context.Context) {
		close(backgroundStarted)
		<-ctx.Done()
		close(backgroundStopped)
	}) {
		t.Fatal("expected background task to start")
	}
	<-backgroundStarted

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})}
	result := make(chan error, 1)
	go func() {
		result <- runImageCacheHTTPServer(lifecycle, server, listener, time.Second, cache.waitForBackground)
	}()

	response, err := (&http.Client{Timeout: time.Second}).Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatalf("GET image cache test server: %v", err)
	}
	_ = response.Body.Close()
	cancel()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("runImageCacheHTTPServer() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("image cache server did not shut down")
	}
	select {
	case <-backgroundStopped:
	case <-time.After(time.Second):
		t.Fatal("image cache background work was not canceled and drained")
	}
	if cache.startBackground(func(context.Context) {}) {
		t.Fatal("canceled image cache accepted new background work")
	}
}

func TestRunImageCacheHTTPServerDrainsActiveRequest(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var started sync.Once
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started.Do(func() { close(requestStarted) })
		<-releaseRequest
		w.WriteHeader(http.StatusNoContent)
	})}
	result := make(chan error, 1)
	go func() {
		result <- runImageCacheHTTPServer(lifecycle, server, listener, time.Second, nil)
	}()
	requestResult := make(chan error, 1)
	go func() {
		response, requestErr := (&http.Client{Timeout: 2 * time.Second}).Get("http://" + listener.Addr().String())
		if response != nil {
			_ = response.Body.Close()
		}
		requestResult <- requestErr
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("active request did not start")
	}
	cancel()
	select {
	case err := <-result:
		t.Fatalf("server returned before active request drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseRequest)
	if err := <-requestResult; err != nil {
		t.Fatalf("active request failed during graceful shutdown: %v", err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("graceful active-request shutdown error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not finish after active request drained")
	}
}

func TestRunImageCacheHTTPServerFailsClosedOnShutdownTimeout(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(requestStarted)
		<-releaseRequest
	})}
	result := make(chan error, 1)
	go func() {
		result <- runImageCacheHTTPServer(lifecycle, server, listener, 50*time.Millisecond, nil)
	}()
	go func() {
		response, _ := (&http.Client{Timeout: time.Second}).Get("http://" + listener.Addr().String())
		if response != nil {
			_ = response.Body.Close()
		}
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking request did not start")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "HTTP shutdown") || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown timeout error = %v, want fail-closed deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not enforce its shutdown deadline")
	}
	close(releaseRequest)
}

func TestRunImageCacheHTTPServerRejectsInvalidBoundary(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	server := &http.Server{}
	for name, run := range map[string]func() error{
		"nil context":  func() error { return runImageCacheHTTPServer(nil, server, listener, time.Second, nil) },
		"nil server":   func() error { return runImageCacheHTTPServer(context.Background(), nil, listener, time.Second, nil) },
		"nil listener": func() error { return runImageCacheHTTPServer(context.Background(), server, nil, time.Second, nil) },
		"zero timeout": func() error { return runImageCacheHTTPServer(context.Background(), server, listener, 0, nil) },
		"negative timeout": func() error {
			return runImageCacheHTTPServer(context.Background(), server, listener, -time.Second, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); err == nil {
				t.Fatal("runImageCacheHTTPServer() unexpectedly succeeded")
			}
		})
	}
}

func TestFilesystemUsageFromStatfsCountsReservedBlocksAsUnavailable(t *testing.T) {
	t.Parallel()

	stats := syscall.Statfs_t{
		Blocks: 1_000,
		Bfree:  400,
		Bavail: 100,
		Bsize:  4_096,
	}
	total, used, free, err := filesystemUsageFromStatfs(stats)
	if err != nil {
		t.Fatalf("filesystem usage from statfs: %v", err)
	}
	if total != 4_096_000 || free != 409_600 || used != 3_686_400 {
		t.Fatalf("filesystem usage total=%d used=%d free=%d, want total=4096000 used=3686400 free=409600", total, used, free)
	}
	if total != used+free {
		t.Fatalf("filesystem usage is inconsistent: total=%d used=%d free=%d", total, used, free)
	}
}

func TestHydrateInheritsImagePlaneLifecycle(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	copyStarted := make(chan struct{})
	cache := &imageCache{
		lifecycle:      lifecycle,
		registryBase:   "registry.fugue.internal:5000",
		localBase:      "127.0.0.1:5000",
		upstreamBase:   "upstream.example.com:5000",
		cacheEndpoint:  "http://127.0.0.1:5000",
		hydrateTimeout: time.Minute,
		hydrateSlots:   newSemaphore(1),
		copyJobs:       1,
		copyImageFn: func(ctx context.Context, _, _ string) error {
			close(copyStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	result := make(chan error, 1)
	go func() {
		result <- cache.hydrate(context.Background(), "library/nginx", "latest")
	}()
	select {
	case <-copyStarted:
	case <-time.After(time.Second):
		t.Fatal("hydrate copy did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("hydrate error = %v, want lifecycle cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("hydrate ignored image-plane lifecycle cancellation")
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Second)
	defer drainCancel()
	if err := cache.waitForBackground(drainCtx); err != nil {
		t.Fatalf("drain canceled hydrate: %v", err)
	}
}

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

func TestManagementAPIRejectsUnauthenticatedRemoteRequests(t *testing.T) {
	t.Parallel()

	cache := &imageCache{
		managementToken: "secret",
		manifestDir:     filepath.Join(t.TempDir(), "manifests"),
	}
	req := httptest.NewRequest(http.MethodGet, "/fugue/cache/v1/inventory", nil)
	req.RemoteAddr = "198.51.100.10:43210"
	recorder := httptest.NewRecorder()
	cache.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized remote management request, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/fugue/cache/v1/inventory", nil)
	req.RemoteAddr = "198.51.100.10:43210"
	req.Header.Set("Authorization", "Bearer secret")
	recorder = httptest.NewRecorder()
	cache.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected authorized inventory request, got %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestManagementReplicateFailsWhenLocationReportFails(t *testing.T) {
	t.Parallel()

	const manifest = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.empty.v1+json","digest":"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","size":2},"layers":[]}`
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v2/fugue-apps/demo/manifests/image-test" {
			t.Fatalf("unexpected source request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		_, _ = io.WriteString(w, manifest)
	}))
	t.Cleanup(source.Close)

	reportAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/image-locations" {
			t.Fatalf("unexpected report request %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "report backend unavailable")
	}))
	t.Cleanup(reportAPI.Close)

	cache := &imageCache{
		apiBase:       reportAPI.URL,
		apiToken:      "token",
		reportPath:    "/v1/image-locations",
		registryBase:  "registry.fugue.internal:5000",
		localBase:     "127.0.0.1:5000",
		cacheEndpoint: "http://10.0.0.2:5000",
		manifestDir:   filepath.Join(t.TempDir(), "manifests"),
		httpClient:    reportAPI.Client(),
		registry:      registry.New(),
		copyImageFn:   func(context.Context, string, string) error { return nil },
	}
	body := `{"image_ref":"registry.fugue.internal:5000/fugue-apps/demo:image-test","source_cache_endpoint":"` + source.URL + `"}`
	req := httptest.NewRequest(http.MethodPost, "/fugue/cache/v1/replicate", strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	cache.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("replicate status = %d, want %d; body=%q", rec.Code, http.StatusBadGateway, rec.Body.String())
	}
}

func TestManagementReplicateReportsCopyFailure(t *testing.T) {
	t.Parallel()

	var statuses atomic.Int32
	reportAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/image-locations" {
			t.Fatalf("unexpected report request %s %s", r.Method, r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse report form: %v", err)
		}
		if r.Form.Get("status") == "failed" {
			statuses.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(reportAPI.Close)

	cache := &imageCache{
		apiBase:         reportAPI.URL,
		apiToken:        "token",
		reportPath:      "/v1/image-locations",
		registryBase:    "registry.fugue.internal:5000",
		localBase:       "127.0.0.1:5000",
		cacheEndpoint:   "http://10.0.0.2:5000",
		managementToken: "token",
		httpClient:      reportAPI.Client(),
		copyImageFn: func(context.Context, string, string) error {
			return errors.New("source cache unavailable")
		},
		registry: registry.New(),
	}
	req := httptest.NewRequest(http.MethodPost, "/fugue/cache/v1/replicate", strings.NewReader(`{"image_ref":"registry.fugue.internal:5000/fugue-apps/demo:image-copy","source_cache_endpoint":"10.0.0.3:5000"}`))
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	cache.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("replicate copy failure status = %d, want %d; body=%q", rec.Code, http.StatusBadGateway, rec.Body.String())
	}
	if statuses.Load() != 1 {
		t.Fatalf("failed copy report count = %d, want 1", statuses.Load())
	}
}

func TestManagementRepoTargetPreservesRepositoryPathWithoutRegistryHost(t *testing.T) {
	t.Parallel()

	cache := &imageCache{
		registryBase:  "registry.fugue.internal:5000",
		localBase:     "127.0.0.1:5000",
		upstreamBase:  "",
		cacheEndpoint: "http://127.0.0.1:5000",
	}
	repo, target := cache.managementRepoTarget("fugue-apps/demo:git-abc", "", "", "")
	if repo != "fugue-apps/demo" || target != "git-abc" {
		t.Fatalf("expected repo fugue-apps/demo target git-abc, got repo=%q target=%q", repo, target)
	}
	repo, target = cache.managementRepoTarget("registry.fugue.internal:5000/fugue-apps/demo@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "", "", "")
	if repo != "fugue-apps/demo" || target != "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("expected registry host stripped, got repo=%q target=%q", repo, target)
	}
}

func TestHydrateLimitsConcurrentCopiesAcrossTargets(t *testing.T) {
	t.Parallel()

	var copies atomic.Int32
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	cache := &imageCache{
		registryBase:   "registry.fugue.internal:5000",
		localBase:      "127.0.0.1:5000",
		upstreamBase:   "upstream.example.com:5000",
		cacheEndpoint:  "http://127.0.0.1:5000",
		hydrateTimeout: 5 * time.Second,
		hydrateSlots:   newSemaphore(1),
		copyImageFn: func(ctx context.Context, src, dst string) error {
			switch copies.Add(1) {
			case 1:
				close(firstStarted)
				select {
				case <-releaseFirst:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			case 2:
				close(secondStarted)
				select {
				case <-releaseSecond:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			default:
				return nil
			}
		},
	}

	firstErr := make(chan error, 1)
	secondErr := make(chan error, 1)
	go func() {
		firstErr <- cache.hydrate(context.Background(), "library/nginx", "first")
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("expected first copy to start")
	}
	go func() {
		secondErr <- cache.hydrate(context.Background(), "library/nginx", "second")
	}()
	select {
	case <-secondStarted:
		t.Fatal("second copy started before the hydrate slot was released")
	case <-time.After(50 * time.Millisecond):
	}
	if got := copies.Load(); got != 1 {
		t.Fatalf("copies before release = %d, want 1", got)
	}

	close(releaseFirst)
	if err := <-firstErr; err != nil {
		t.Fatalf("first hydrate returned error: %v", err)
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("expected second copy to start after first released the slot")
	}
	close(releaseSecond)
	if err := <-secondErr; err != nil {
		t.Fatalf("second hydrate returned error: %v", err)
	}
}

func TestHydrateContinuesCopyWhenAllWaitersCancel(t *testing.T) {
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
		t.Fatalf("copy finished after waiter cancellation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	secondErr := make(chan error, 1)
	go func() {
		secondErr <- cache.hydrate(context.Background(), "library/nginx", "latest")
	}()
	waitForHydrateWaiters(t, cache, 1)
	close(releaseCopy)
	if err := <-secondErr; err != nil {
		t.Fatalf("second hydrate returned error: %v", err)
	}
	if err := <-copyDone; err != nil {
		t.Fatalf("copy error = %v, want nil", err)
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

func TestProxyRegistryPullLimitsConcurrentStreams(t *testing.T) {
	t.Parallel()

	firstHit := make(chan struct{})
	secondHit := make(chan struct{})
	releaseFirst := make(chan struct{})
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch hits.Add(1) {
		case 1:
			close(firstHit)
			<-releaseFirst
		case 2:
			close(secondHit)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "layer")
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	cache := &imageCache{proxySlots: newSemaphore(1)}
	req1 := httptest.NewRequest(http.MethodGet, "http://image-cache.test/v2/library/demo/blobs/sha256:111", nil)
	req2 := httptest.NewRequest(http.MethodGet, "http://image-cache.test/v2/library/demo/blobs/sha256:222", nil)

	firstResult := make(chan proxyPullResult, 1)
	secondResult := make(chan proxyPullResult, 1)
	go func() {
		firstResult <- cache.proxyRegistryPull(httptest.NewRecorder(), req1, upstreamURL.Host)
	}()
	select {
	case <-firstHit:
	case <-time.After(time.Second):
		t.Fatal("expected first proxy request to reach upstream")
	}
	go func() {
		secondResult <- cache.proxyRegistryPull(httptest.NewRecorder(), req2, upstreamURL.Host)
	}()
	select {
	case <-secondHit:
		t.Fatal("second proxy request reached upstream before the proxy slot was released")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFirst)
	if result := <-firstResult; !result.ok || result.err != nil {
		t.Fatalf("first proxy result = %+v", result)
	}
	select {
	case <-secondHit:
	case <-time.After(time.Second):
		t.Fatal("expected second proxy request to reach upstream after first released the slot")
	}
	if result := <-secondResult; !result.ok || result.err != nil {
		t.Fatalf("second proxy result = %+v", result)
	}
}

func TestManifestMissProxiesPeerAndHydratesInBackground(t *testing.T) {
	t.Parallel()

	const blobDigest = "sha256:6a0ac1617861a677b045b7ff88545213ec31c0ff08763195a70a4a5adda577bb"
	const manifest = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.empty.v1+json","digest":"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","size":2},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"` + blobDigest + `","size":11}]}`
	var peerRange string
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(imageCacheLocalOnlyHeader); got != "1" {
			t.Errorf("%s local-only header = %q, want 1", r.URL.Path, got)
		}
		switch r.URL.Path {
		case "/v2/fugue-apps/demo/manifests/image-test":
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			_, _ = io.WriteString(w, manifest)
		case "/v2/fugue-apps/demo/blobs/" + blobDigest:
			peerRange = r.Header.Get("Range")
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, "hello layer")
		default:
			t.Errorf("unexpected peer request %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(peer.Close)

	copyStarted := make(chan struct{})
	var copies atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/image-locations":
			if got := r.URL.Query().Get("image_ref"); got != "registry.fugue.internal:5000/fugue-apps/demo:image-test" {
				t.Errorf("image_ref query = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"image_locations": []imageLocation{{
					CacheEndpoint: peer.URL,
					Status:        "present",
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/image-locations":
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected API request %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)

	cache := &imageCache{
		apiBase:        api.URL,
		apiToken:       "token",
		reportPath:     "/v1/image-locations",
		lookupPath:     "/v1/image-locations",
		registryBase:   "registry.fugue.internal:5000",
		localBase:      "127.0.0.1:5000",
		cacheEndpoint:  "http://10.0.0.2:5000",
		httpClient:     api.Client(),
		registry:       registry.New(),
		hydrateTimeout: 5 * time.Second,
		sourceTTL:      time.Minute,
		copyImageFn: func(context.Context, string, string) error {
			if copies.Add(1) == 1 {
				close(copyStarted)
			}
			return nil
		},
	}
	manifestReq := httptest.NewRequest(http.MethodGet, "http://image-cache.test/v2/fugue-apps/demo/manifests/image-test", nil)
	manifestRec := httptest.NewRecorder()

	cache.ServeHTTP(manifestRec, manifestReq)

	if manifestRec.Code != http.StatusOK {
		t.Fatalf("manifest status = %d, want %d; body=%q", manifestRec.Code, http.StatusOK, manifestRec.Body.String())
	}
	if got := manifestRec.Body.String(); got != manifest {
		t.Fatalf("manifest body = %q", got)
	}
	select {
	case <-copyStarted:
	case <-time.After(time.Second):
		t.Fatal("expected background hydrate to start")
	}

	blobReq := httptest.NewRequest(http.MethodGet, "http://image-cache.test/v2/fugue-apps/demo/blobs/"+blobDigest, nil)
	blobReq.Header.Set("Range", "bytes=0-10")
	blobRec := httptest.NewRecorder()

	cache.ServeHTTP(blobRec, blobReq)

	if blobRec.Code != http.StatusPartialContent {
		t.Fatalf("blob status = %d, want %d; body=%q", blobRec.Code, http.StatusPartialContent, blobRec.Body.String())
	}
	if got := blobRec.Body.String(); got != "hello layer" {
		t.Fatalf("blob body = %q", got)
	}
	if peerRange != "bytes=0-10" {
		t.Fatalf("peer Range = %q", peerRange)
	}
}

func TestLocalOnlyRegistryPullDoesNotCascade(t *testing.T) {
	t.Parallel()

	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	var copies atomic.Int32
	cache := &imageCache{
		registry:       registry.New(),
		upstreamBase:   upstreamURL.Host,
		hydrateTimeout: 5 * time.Second,
		copyImageFn: func(context.Context, string, string) error {
			copies.Add(1)
			return nil
		},
	}
	req := httptest.NewRequest(http.MethodGet, "http://image-cache.test/v2/fugue-apps/demo/manifests/image-missing", nil)
	req.Header.Set(imageCacheLocalOnlyHeader, "1")
	rec := httptest.NewRecorder()

	cache.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%q", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	if got := upstreamHits.Load(); got != 0 {
		t.Fatalf("upstream hits = %d, want 0", got)
	}
	if got := copies.Load(); got != 0 {
		t.Fatalf("copyImage calls = %d, want 0", got)
	}
}

func TestManagementVerifyRequiresCompleteOCIReferences(t *testing.T) {
	t.Parallel()

	const configBody = "{}"
	const layerBody = "x"
	configDigest := testImageCacheBlobDigest([]byte(configBody))
	layerDigest := testImageCacheBlobDigest([]byte(layerBody))
	manifest := testImageCacheManifest(configDigest, layerDigest)

	missing := &imageCache{
		registry: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/manifests/image-missing") && r.Method == http.MethodGet:
				w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
				_, _ = io.WriteString(w, manifest)
			case strings.HasSuffix(r.URL.Path, "/manifests/image-missing") && r.Method == http.MethodHead:
				w.WriteHeader(http.StatusOK)
			default:
				http.NotFound(w, r)
			}
		}),
	}
	missingReq := httptest.NewRequest(http.MethodPost, "http://image-cache.test/fugue/cache/v1/verify", strings.NewReader(`{"image_ref":"registry.fugue.internal:5000/fugue-apps/demo:image-missing"}`))
	missingRec := httptest.NewRecorder()
	missing.handleManagementVerify(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("manifest-only verify status = %d, want %d; body=%s", missingRec.Code, http.StatusNotFound, missingRec.Body.String())
	}
	var missingResponse map[string]any
	if err := json.Unmarshal(missingRec.Body.Bytes(), &missingResponse); err != nil {
		t.Fatalf("decode manifest-only verify response: %v", err)
	}
	if available, _ := missingResponse["available"].(bool); available {
		t.Fatalf("manifest-only image was reported available: %v", missingResponse)
	}

	storeDir := t.TempDir()
	complete := &imageCache{
		registry: registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
	}
	writeTestImageCacheBlob(t, storeDir, []byte(configBody))
	writeTestImageCacheBlob(t, storeDir, []byte(layerBody))
	put := httptest.NewRequest(http.MethodPut, "http://image-cache.test/v2/fugue-apps/demo/manifests/image-complete", strings.NewReader(manifest))
	put.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	putRec := httptest.NewRecorder()
	complete.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusCreated {
		t.Fatalf("complete manifest put status = %d, want %d; body=%s", putRec.Code, http.StatusCreated, putRec.Body.String())
	}
	completeReq := httptest.NewRequest(http.MethodPost, "http://image-cache.test/fugue/cache/v1/verify", strings.NewReader(`{"image_ref":"registry.fugue.internal:5000/fugue-apps/demo:image-complete"}`))
	completeRec := httptest.NewRecorder()
	complete.handleManagementVerify(completeRec, completeReq)
	if completeRec.Code != http.StatusOK {
		t.Fatalf("complete verify status = %d, want %d; body=%s", completeRec.Code, http.StatusOK, completeRec.Body.String())
	}
	var completeResponse struct {
		Available           bool     `json:"available"`
		CanonicalDigest     string   `json:"canonical_digest"`
		ReferencedBlobs     []string `json:"referenced_blobs"`
		ReferencedBlobBytes int64    `json:"referenced_blob_bytes"`
	}
	if err := json.Unmarshal(completeRec.Body.Bytes(), &completeResponse); err != nil {
		t.Fatalf("decode complete verify response: %v", err)
	}
	if !completeResponse.Available || completeResponse.CanonicalDigest != manifestBodyDigest([]byte(manifest)) {
		t.Fatalf("complete graph verification = %+v", completeResponse)
	}
	if len(completeResponse.ReferencedBlobs) != 2 || completeResponse.ReferencedBlobBytes != int64(len(configBody)+len(layerBody)) {
		t.Fatalf("complete graph references = %+v", completeResponse)
	}
}

func TestManagementVerifyRejectsRemoteFallbackWhenLocalGraphIsMissing(t *testing.T) {
	t.Parallel()

	var upstreamHits atomic.Int32
	var copyCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits.Add(1)
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		_, _ = io.WriteString(w, `{"schemaVersion":2}`)
	}))
	t.Cleanup(upstream.Close)
	cache := &imageCache{
		managementToken: "management-secret",
		upstreamBase:    strings.TrimPrefix(upstream.URL, "http://"),
		registry: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.NotFound(w, nil)
		}),
		copyImageFn: func(context.Context, string, string) error {
			copyCalls.Add(1)
			return nil
		},
	}
	req := httptest.NewRequest(
		http.MethodPost,
		"http://image-cache.test/fugue/cache/v1/verify",
		strings.NewReader(`{"image_ref":"registry.fugue.internal:5000/fugue-apps/demo:image-remote-only"}`),
	)
	req.Header.Set("Authorization", "Bearer management-secret")
	rec := httptest.NewRecorder()
	cache.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("remote-only verification status = %d, want %d; body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	if got := upstreamHits.Load(); got != 0 {
		t.Fatalf("management verification reached upstream %d times", got)
	}
	if got := copyCalls.Load(); got != 0 {
		t.Fatalf("management verification attempted hydration %d times", got)
	}
}

func TestManagementManifestInventoryRequiresCompleteImageGraph(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir:    storeDir,
		manifestDir: filepath.Join(storeDir, "_manifests"),
		registry:    registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
	}
	configDigest := testImageCacheBlobDigest([]byte("{}"))
	layerDigest := testImageCacheBlobDigest([]byte("x"))
	manifest := []byte(testImageCacheManifest(configDigest, layerDigest))
	if err := cache.persistManifest("fugue-apps/demo", "image-incomplete", "application/vnd.oci.image.manifest.v1+json", manifest); err != nil {
		t.Fatalf("persist manifest journal: %v", err)
	}
	if err := cache.replayManifest(persistedManifest{Repo: "fugue-apps/demo", Target: "image-incomplete", ContentType: "application/vnd.oci.image.manifest.v1+json", Body: manifest}); err != nil {
		t.Fatalf("replay manifest: %v", err)
	}
	inventory, err := cache.managementManifestInventory(context.Background())
	if err != nil {
		t.Fatalf("incomplete inventory: %v", err)
	}
	if len(inventory) != 0 {
		t.Fatalf("manifest-only record was advertised: %+v", inventory)
	}

	writeTestImageCacheBlob(t, storeDir, []byte("{}"))
	writeTestImageCacheBlob(t, storeDir, []byte("x"))
	inventory, err = cache.managementManifestInventory(context.Background())
	if err != nil {
		t.Fatalf("complete inventory: %v", err)
	}
	if len(inventory) != 1 {
		t.Fatalf("complete manifest inventory = %d, want 1: %+v", len(inventory), inventory)
	}
	for _, entry := range inventory {
		if bytes, _ := entry["referenced_blob_bytes"].(int64); bytes != 3 {
			t.Fatalf("referenced blob bytes = %v, want 3: %+v", entry["referenced_blob_bytes"], entry)
		}
		if digest, _ := entry["digest"].(string); digest != manifestBodyDigest(manifest) {
			t.Fatalf("canonical digest = %q, want %q", digest, manifestBodyDigest(manifest))
		}
	}
}

func TestManagementManifestInventoryHonorsCanceledRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cache := &imageCache{manifestDir: t.TempDir()}
	if _, err := cache.managementManifestInventory(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("managementManifestInventory() error = %v, want request cancellation", err)
	}
}

func TestManagementManifestInventoryRejectsMissingChildBlob(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir:    storeDir,
		manifestDir: filepath.Join(storeDir, "_manifests"),
		registry:    registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
	}
	configDigest := testImageCacheBlobDigest([]byte("{}"))
	layerDigest := testImageCacheBlobDigest([]byte("x"))
	child := []byte(testImageCacheManifest(configDigest, layerDigest))
	childDigest := manifestBodyDigest(child)
	parent := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":%d,"platform":{"os":"linux","architecture":"amd64"}}]}`, childDigest, len(child)))
	if err := cache.replayManifest(persistedManifest{Repo: "fugue-apps/demo", Target: childDigest, ContentType: "application/vnd.oci.image.manifest.v1+json", Body: child}); err != nil {
		t.Fatalf("replay child manifest: %v", err)
	}
	if err := cache.replayManifest(persistedManifest{Repo: "fugue-apps/demo", Target: "image-index", ContentType: "application/vnd.docker.distribution.manifest.list.v2+json", Body: parent}); err != nil {
		t.Fatalf("replay parent manifest: %v", err)
	}
	if err := cache.persistManifest("fugue-apps/demo", childDigest, "application/vnd.oci.image.manifest.v1+json", child); err != nil {
		t.Fatalf("persist child journal: %v", err)
	}
	if err := cache.persistManifest("fugue-apps/demo", "image-index", "application/vnd.docker.distribution.manifest.list.v2+json", parent); err != nil {
		t.Fatalf("persist parent journal: %v", err)
	}
	inventory, err := cache.managementManifestInventory(context.Background())
	if err != nil {
		t.Fatalf("missing child blob inventory: %v", err)
	}
	if len(inventory) != 0 {
		t.Fatalf("index with missing child blobs was advertised: %+v", inventory)
	}
}

func TestCopyImageForcesLocalOnlyOnSourceAndDestination(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	sourceCache := &imageCache{
		registry: registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
	}
	configDigest := writeTestImageCacheBlob(t, storeDir, []byte("{}"))
	layerDigest := writeTestImageCacheBlob(t, storeDir, []byte("x"))
	manifest := testImageCacheManifest(configDigest, layerDigest)
	put := httptest.NewRequest(http.MethodPut, "http://image-cache.test/v2/fugue-apps/demo/manifests/image-copy", strings.NewReader(manifest))
	put.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	putRec := httptest.NewRecorder()
	sourceCache.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusCreated {
		t.Fatalf("source manifest put status = %d, want %d; body=%s", putRec.Code, http.StatusCreated, putRec.Body.String())
	}

	destinationDir := t.TempDir()
	destinationCache := &imageCache{
		storeDir: destinationDir,
		registry: registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(destinationDir))),
	}
	var nonLocalSource atomic.Int32
	var nonLocalDestination atomic.Int32
	var destinationProxyHits atomic.Int32
	active := atomic.Bool{}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if active.Load() && r.Header.Get(imageCacheLocalOnlyHeader) != "1" {
			nonLocalSource.Add(1)
		}
		sourceCache.ServeHTTP(w, r)
	}))
	t.Cleanup(source.Close)
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if active.Load() && r.Header.Get(imageCacheLocalOnlyHeader) != "1" {
			nonLocalDestination.Add(1)
			if strings.Contains(r.URL.Path, "/blobs/") {
				destinationProxyHits.Add(1)
				sourceCache.ServeHTTP(w, r)
				return
			}
		}
		destinationCache.ServeHTTP(w, r)
	}))
	t.Cleanup(destination.Close)

	active.Store(true)
	err := copyImage(context.Background(), strings.TrimPrefix(source.URL, "http://")+"/fugue-apps/demo:image-copy", strings.TrimPrefix(destination.URL, "http://")+"/fugue-apps/demo:image-copy", 1)
	if err != nil {
		t.Fatalf("copy image: %v", err)
	}
	if got := nonLocalSource.Load(); got != 0 {
		t.Fatalf("source requests without local-only marker = %d", got)
	}
	if got := nonLocalDestination.Load(); got != 0 || destinationProxyHits.Load() != 0 {
		t.Fatalf("destination requests bypassed local-only marker: non_local=%d proxy=%d", got, destinationProxyHits.Load())
	}
	if _, err := destinationCache.checkLocalImageGraph(context.Background(), "fugue-apps/demo", "image-copy", true); err != nil {
		t.Fatalf("destination image graph incomplete after copy: %v", err)
	}
}

func TestLocalOnlyRoundTripperRejectsCrossHostRedirect(t *testing.T) {
	t.Parallel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(imageCacheLocalOnlyHeader) != "1" {
			t.Errorf("redirected request local-only header = %q, want 1", r.Header.Get(imageCacheLocalOnlyHeader))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(upstream.Close)
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(imageCacheLocalOnlyHeader) != "1" {
			t.Errorf("initial request local-only header = %q, want 1", r.Header.Get(imageCacheLocalOnlyHeader))
		}
		http.Redirect(w, r, upstream.URL+"/v2/fugue-apps/demo/blobs/sha256:test", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirect.Close)

	client := &http.Client{Transport: localOnlyRoundTripper{base: http.DefaultTransport}}
	resp, err := client.Get(redirect.URL + "/v2/fugue-apps/demo/blobs/sha256:test")
	if resp != nil {
		resp.Body.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "local-only registry request redirected") {
		t.Fatalf("cross-host redirect error = %v, want local-only redirect rejection", err)
	}
}

func TestLocalOnlyRoundTripperRejectsSchemeRelativeCrossHostRedirect(t *testing.T) {
	t.Parallel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("scheme-relative cross-host redirect reached upstream: %s", r.URL.String())
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "//"+upstreamURL.Host+"/v2/fugue-apps/demo/blobs/sha256:test")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirect.Close)

	client := &http.Client{Transport: localOnlyRoundTripper{base: http.DefaultTransport}}
	resp, err := client.Get(redirect.URL + "/v2/fugue-apps/demo/blobs/sha256:test")
	if resp != nil {
		resp.Body.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "local-only registry request redirected") {
		t.Fatalf("scheme-relative cross-host redirect error = %v, want local-only rejection", err)
	}
}

func TestLocalOnlyRoundTripperAllowsSameHostRedirect(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/final", http.StatusTemporaryRedirect)
			return
		}
		if r.URL.Path != "/final" || r.Header.Get(imageCacheLocalOnlyHeader) != "1" {
			t.Fatalf("same-host redirected request path=%q local-only=%q", r.URL.Path, r.Header.Get(imageCacheLocalOnlyHeader))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	client := &http.Client{Transport: localOnlyRoundTripper{base: http.DefaultTransport}}
	resp, err := client.Get(server.URL + "/start")
	if err != nil {
		t.Fatalf("same-host redirect: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("same-host redirect status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestLocalOnlyRoundTripperRejectsInvalidRedirectLocation(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "://invalid host")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(server.Close)

	client := &http.Client{Transport: localOnlyRoundTripper{base: http.DefaultTransport}}
	resp, err := client.Get(server.URL + "/start")
	if resp != nil {
		resp.Body.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "local-only registry redirect has invalid location") {
		t.Fatalf("invalid redirect error = %v, want invalid-location rejection", err)
	}
}

func TestManifestMissMarksPeerMissingWithoutRecursiveProxy(t *testing.T) {
	t.Parallel()

	var peerHits atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerHits.Add(1)
		if got := r.Header.Get(imageCacheLocalOnlyHeader); got != "1" {
			t.Errorf("peer local-only header = %q, want 1", got)
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(peer.Close)

	reports := []url.Values{}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/image-locations":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"image_locations": []imageLocation{{
					TenantID:      "tenant-1",
					RuntimeID:     "runtime-1",
					CacheEndpoint: peer.URL,
					Status:        "present",
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/image-locations":
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			reports = append(reports, r.Form)
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected API request %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)

	cache := &imageCache{
		apiBase:        api.URL,
		apiToken:       "token",
		reportPath:     "/v1/image-locations",
		lookupPath:     "/v1/image-locations",
		registryBase:   "registry.fugue.internal:5000",
		localBase:      "127.0.0.1:5000",
		cacheEndpoint:  "http://10.0.0.2:5000",
		httpClient:     api.Client(),
		registry:       registry.New(),
		hydrateTimeout: 5 * time.Second,
		copyImageFn: func(context.Context, string, string) error {
			return errors.New("GET peer manifest: NAME_UNKNOWN: Unknown name")
		},
	}
	req := httptest.NewRequest(http.MethodGet, "http://image-cache.test/v2/fugue-apps/demo/manifests/image-missing", nil)
	rec := httptest.NewRecorder()

	cache.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%q", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	if got := peerHits.Load(); got != 1 {
		t.Fatalf("peer hits = %d, want 1", got)
	}
	foundMissingPeer := false
	for _, report := range reports {
		if report.Get("status") == "missing" && report.Get("cache_endpoint") == peer.URL {
			foundMissingPeer = true
			break
		}
	}
	if !foundMissingPeer {
		t.Fatalf("expected missing peer report, got %v", reports)
	}
}

func TestManifestReferencedTargetsIncludesDockerSchemaV1Layers(t *testing.T) {
	t.Parallel()

	const manifest = `{"schemaVersion":1,"name":"fugue-apps/demo","tag":"image-test","fsLayers":[{"blobSum":"sha256:1111111111111111111111111111111111111111111111111111111111111111"},{"blobSum":"sha256:2222222222222222222222222222222222222222222222222222222222222222"}]}`

	targets := manifestReferencedTargets([]byte(manifest))

	got := map[string]registryTargetKind{}
	for _, target := range targets {
		got[target.target] = target.kind
	}
	for _, digest := range []string{
		"sha256:1111111111111111111111111111111111111111111111111111111111111111",
		"sha256:2222222222222222222222222222222222222222222222222222222222222222",
	} {
		if got[digest] != registryTargetBlob {
			t.Fatalf("digest %s kind = %q, want %q; targets=%v", digest, got[digest], registryTargetBlob, targets)
		}
	}
}

func TestHydrateEnsuresManifestWhenCopyDoesNotPopulateTag(t *testing.T) {
	t.Parallel()

	// This fixture exercises manifest journaling, not blob hydration.  Keep the
	// schema-1 graph empty so the stricter post-hydration integrity check can
	// prove the tag was persisted without inventing a fake local layer.
	const manifest = `{"schemaVersion":1,"name":"fugue-apps/demo","tag":"image-test","fsLayers":[]}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/fugue-apps/demo/manifests/image-test" {
			t.Fatalf("upstream path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v1+prettyjws")
		_, _ = io.WriteString(w, manifest)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	cache := &imageCache{
		registry:       registry.New(),
		registryBase:   "registry.fugue.internal:5000",
		localBase:      "127.0.0.1:5000",
		upstreamBase:   upstreamURL.Host,
		hydrateTimeout: 5 * time.Second,
		copyImageFn: func(context.Context, string, string) error {
			return nil
		},
	}

	if err := cache.hydrate(context.Background(), "fugue-apps/demo", "image-test"); err != nil {
		t.Fatalf("hydrate: %v", err)
	}

	req := httptest.NewRequest(http.MethodHead, "http://image-cache.test/v2/fugue-apps/demo/manifests/image-test", nil)
	rec := httptest.NewRecorder()
	cache.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("local manifest status = %d, want %d; body=%q", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestHydrateEnsuresManifestListChildrenBeforeTag(t *testing.T) {
	t.Parallel()

	child := []byte(`{"schemaVersion":1,"name":"fugue-apps/demo","tag":"child","fsLayers":[]}`)
	childSum := sha256.Sum256(child)
	childDigest := fmt.Sprintf("sha256:%x", childSum[:])
	parent := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[{"mediaType":"application/vnd.docker.distribution.manifest.v1+json","digest":%q,"size":%d,"platform":{"os":"linux","architecture":"amd64"}}]}`, childDigest, len(child)))

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/fugue-apps/demo/manifests/image-list":
			w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.list.v2+json")
			_, _ = w.Write(parent)
		case "/v2/fugue-apps/demo/manifests/" + childDigest:
			w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v1+json")
			_, _ = w.Write(child)
		default:
			t.Fatalf("upstream path = %q", r.URL.Path)
		}
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}

	cache := &imageCache{
		registry:       registry.New(),
		registryBase:   "registry.fugue.internal:5000",
		localBase:      "127.0.0.1:5000",
		upstreamBase:   upstreamURL.Host,
		hydrateTimeout: 5 * time.Second,
		copyImageFn: func(context.Context, string, string) error {
			return nil
		},
	}

	if err := cache.hydrate(context.Background(), "fugue-apps/demo", "image-list"); err != nil {
		t.Fatalf("hydrate: %v", err)
	}

	for _, target := range []string{"image-list", childDigest} {
		req := httptest.NewRequest(http.MethodHead, "http://image-cache.test/v2/fugue-apps/demo/manifests/"+target, nil)
		rec := httptest.NewRecorder()
		cache.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("local manifest %s status = %d, want %d; body=%q", target, rec.Code, http.StatusOK, rec.Body.String())
		}
	}
}

func TestImageCachePersistsManifestsAcrossRegistryRestart(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	manifestDir := filepath.Join(storeDir, "_manifests")
	cache := &imageCache{
		registry:    registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
		manifestDir: manifestDir,
	}
	const manifest = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.empty.v1+json","digest":"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","size":2},"layers":[]}`
	if got := writeTestImageCacheBlob(t, storeDir, []byte("{}")); got != "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a" {
		t.Fatalf("config blob digest = %s", got)
	}
	manifestDigest := manifestBodyDigest([]byte(manifest))
	put := httptest.NewRequest(http.MethodPut, "http://image-cache.test/v2/fugue-apps/demo/manifests/image-test", strings.NewReader(manifest))
	put.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	putRec := httptest.NewRecorder()

	cache.ServeHTTP(putRec, put)

	if putRec.Code != http.StatusCreated {
		t.Fatalf("put status = %d, want %d; body=%q", putRec.Code, http.StatusCreated, putRec.Body.String())
	}
	for _, target := range []string{"image-test", manifestDigest} {
		head := httptest.NewRequest(http.MethodHead, "http://image-cache.test/v2/fugue-apps/demo/manifests/"+target, nil)
		headRec := httptest.NewRecorder()
		cache.ServeHTTP(headRec, head)
		if headRec.Code != http.StatusOK {
			t.Fatalf("head %s status after put = %d, want %d; body=%q", target, headRec.Code, http.StatusOK, headRec.Body.String())
		}
	}
	files, err := filepath.Glob(filepath.Join(manifestDir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("persisted manifests = %d, want 2", len(files))
	}

	restarted := &imageCache{
		registry:    registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
		manifestDir: manifestDir,
	}
	if err := restarted.loadPersistedManifests(); err != nil {
		t.Fatalf("load persisted manifests: %v", err)
	}
	for _, target := range []string{"image-test", manifestDigest} {
		head := httptest.NewRequest(http.MethodHead, "http://image-cache.test/v2/fugue-apps/demo/manifests/"+target, nil)
		headRec := httptest.NewRecorder()
		restarted.ServeHTTP(headRec, head)
		if headRec.Code != http.StatusOK {
			t.Fatalf("head %s status after restart = %d, want %d; body=%q", target, headRec.Code, http.StatusOK, headRec.Body.String())
		}
	}
	inventory, err := restarted.managementManifestInventory(context.Background())
	if err != nil {
		t.Fatalf("management manifest inventory: %v", err)
	}
	if len(inventory) != 2 {
		t.Fatalf("servable manifest inventory = %d, want 2: %+v", len(inventory), inventory)
	}
}

func TestManagementManifestInventoryExcludesPersistedIndexThatFailedReplay(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	manifestDir := filepath.Join(storeDir, "_manifests")
	cache := &imageCache{
		registry:    registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
		storeDir:    storeDir,
		manifestDir: manifestDir,
	}
	const missingChildDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	index := fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[{"mediaType":"application/vnd.docker.distribution.manifest.v2+json","digest":%q,"size":123,"platform":{"os":"linux","architecture":"amd64"}}]}`, missingChildDigest)
	if err := cache.persistManifest("fugue-apps/demo", "image-index", "application/vnd.docker.distribution.manifest.list.v2+json", []byte(index)); err != nil {
		t.Fatalf("persist index: %v", err)
	}

	if err := cache.loadPersistedManifests(); err != nil {
		t.Fatalf("stale replay metadata should be discarded without failing startup: %v", err)
	}
	records, err := cache.managementManifestRecords()
	if err != nil {
		t.Fatalf("persisted manifest records: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("stale persisted manifest records = %d, want 0: %+v", len(records), records)
	}
	if cache.localManifestAvailable("fugue-apps/demo", "image-index") {
		t.Fatal("index with a missing child must not be locally servable")
	}
	inventory, err := cache.managementManifestInventory(context.Background())
	if err != nil {
		t.Fatalf("management manifest inventory: %v", err)
	}
	if len(inventory) != 0 {
		t.Fatalf("unservable persisted index must not be reported present: %+v", inventory)
	}
}

func TestImageCacheLoadsManifestJournalInDependencyOrder(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	manifestDir := filepath.Join(storeDir, "_manifests")
	cache := &imageCache{
		registry:    registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
		storeDir:    storeDir,
		manifestDir: manifestDir,
	}
	child := []byte(`{"schemaVersion":1,"name":"fugue-apps/demo","tag":"child","fsLayers":[]}`)
	childDigest := manifestBodyDigest(child)
	parent := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[{"mediaType":"application/vnd.docker.distribution.manifest.v1+json","digest":%q,"size":%d,"platform":{"os":"linux","architecture":"amd64"}}]}`, childDigest, len(child)))
	if err := os.MkdirAll(manifestDir, 0o755); err != nil {
		t.Fatalf("mkdir manifest dir: %v", err)
	}
	// Force the parent to be read first.  The loader must defer it until the
	// child journal entry has been replayed rather than discarding valid data.
	for name, manifest := range map[string]persistedManifest{
		"001-parent.json": {Repo: "fugue-apps/demo", Target: "image-index", ContentType: "application/vnd.docker.distribution.manifest.list.v2+json", Body: parent},
		"002-child.json":  {Repo: "fugue-apps/demo", Target: childDigest, ContentType: "application/vnd.docker.distribution.manifest.v1+json", Body: child},
	} {
		raw, err := json.Marshal(manifest)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(manifestDir, name), raw, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	if err := cache.loadPersistedManifests(); err != nil {
		t.Fatalf("load persisted manifests: %v", err)
	}
	for _, target := range []string{"image-index", childDigest} {
		if !cache.localManifestAvailable("fugue-apps/demo", target) {
			t.Fatalf("manifest %s was not replayed", target)
		}
	}
}

func TestImageCacheStaleReplayRemovesManifestAliases(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	manifestDir := filepath.Join(storeDir, "_manifests")
	cache := &imageCache{
		registry:    registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
		storeDir:    storeDir,
		manifestDir: manifestDir,
	}
	const missingChildDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	index := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[{"mediaType":"application/vnd.docker.distribution.manifest.v2+json","digest":%q,"size":123,"platform":{"os":"linux","architecture":"amd64"}}]}`, missingChildDigest))
	if err := cache.persistManifest("fugue-apps/demo", "image-index", "application/vnd.docker.distribution.manifest.list.v2+json", index); err != nil {
		t.Fatalf("persist index tag: %v", err)
	}
	if err := cache.persistManifest("fugue-apps/demo", manifestBodyDigest(index), "application/vnd.docker.distribution.manifest.list.v2+json", index); err != nil {
		t.Fatalf("persist index digest: %v", err)
	}

	if err := cache.loadPersistedManifests(); err != nil {
		t.Fatalf("stale replay should be discarded: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(manifestDir, "*.json"))
	if err != nil {
		t.Fatalf("glob manifest journal: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("stale manifest aliases remain: %v", files)
	}
}

func TestImageCachePruneProtectsManifestListChildren(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	manifestDir := filepath.Join(storeDir, "_manifests")
	cache := &imageCache{
		registry:    registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
		storeDir:    storeDir,
		manifestDir: manifestDir,
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 0.01,
			LowWatermarkPercent:  0.01,
			MinFreeBytes:         0,
			MaxDeleteBytesPerRun: 1 << 30,
		},
	}
	configDigest := writeTestImageCacheBlob(t, storeDir, []byte(`{"config":"child"}`))
	layerDigest := writeTestImageCacheBlob(t, storeDir, []byte("child-layer"))
	child := []byte(testImageCacheManifest(configDigest, layerDigest))
	childDigest := manifestBodyDigest(child)
	parent := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":%d,"platform":{"os":"linux","architecture":"amd64"}}]}`, childDigest, len(child)))

	for _, manifest := range []persistedManifest{
		{Repo: "fugue-apps/demo", Target: childDigest, ContentType: "application/vnd.oci.image.manifest.v1+json", Body: child},
		{Repo: "fugue-apps/demo", Target: "image-index", ContentType: "application/vnd.docker.distribution.manifest.list.v2+json", Body: parent},
	} {
		if err := cache.replayManifest(manifest); err != nil {
			t.Fatalf("replay %s: %v", manifest.Target, err)
		}
		if err := cache.persistManifest(manifest.Repo, manifest.Target, manifest.ContentType, manifest.Body); err != nil {
			t.Fatalf("persist %s: %v", manifest.Target, err)
		}
	}

	result := postImageCachePrune(t, cache, fmt.Sprintf(`{"dry_run":false,"allow_delete":true,"targets":[{"repo":"fugue-apps/demo","target":%q}],"max_delete_bytes":"1Gi"}`, childDigest))
	if result.Deleted || result.SelectedCount != 0 {
		t.Fatalf("referenced child manifest was selected for deletion: %+v", result)
	}
	foundProtected := false
	for _, entry := range result.SkippedManifests {
		if entry.Target == childDigest && entry.SkipReason == "referenced_manifest" {
			foundProtected = true
			break
		}
	}
	if !foundProtected {
		t.Fatalf("expected referenced child skip detail, got %+v", result.SkippedManifests)
	}
	if !cache.localManifestAvailable("fugue-apps/demo", childDigest) {
		t.Fatal("referenced child manifest was removed")
	}
}

func TestImageCachePruneBudgetKeepsChildWhenParentSurvives(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	manifestDir := filepath.Join(storeDir, "_manifests")
	cache := &imageCache{
		storeDir:    storeDir,
		manifestDir: manifestDir,
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 0.01,
			LowWatermarkPercent:  0.01,
			MinFreeBytes:         0,
			MaxDeleteBytesPerRun: 1 << 30,
		},
	}
	configDigest := writeTestImageCacheBlob(t, storeDir, []byte(`{}`))
	layerDigest := writeTestImageCacheBlob(t, storeDir, []byte("layer"))
	child := []byte(testImageCacheManifest(configDigest, layerDigest))
	childDigest := manifestBodyDigest(child)
	// Make the parent too large for this run's budget and arrange for the
	// child to be considered first.  The old planner would delete the child
	// and then classify its blobs as unreferenced even though the parent stayed.
	parent := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","annotations":{"padding":%q},"manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":%d,"platform":{"os":"linux","architecture":"amd64"}}]}`, strings.Repeat("x", 4096), childDigest, len(child)))
	if err := cache.persistManifest("fugue-apps/demo", childDigest, "application/vnd.oci.image.manifest.v1+json", child); err != nil {
		t.Fatalf("persist child: %v", err)
	}
	if err := cache.persistManifest("fugue-apps/demo", "image-index", "application/vnd.docker.distribution.manifest.list.v2+json", parent); err != nil {
		t.Fatalf("persist parent: %v", err)
	}
	childPath := filepath.Join(manifestDir, manifestStoreKey("fugue-apps/demo", childDigest)+".json")
	parentPath := filepath.Join(manifestDir, manifestStoreKey("fugue-apps/demo", "image-index")+".json")
	base := time.Now().Add(-time.Hour)
	if err := os.Chtimes(childPath, base, base); err != nil {
		t.Fatalf("touch child journal: %v", err)
	}
	if err := os.Chtimes(parentPath, base.Add(time.Minute), base.Add(time.Minute)); err != nil {
		t.Fatalf("touch parent journal: %v", err)
	}
	records, err := cache.managementManifestRecords()
	if err != nil {
		t.Fatalf("manifest records: %v", err)
	}
	var childSize, parentSize int64
	for _, record := range records {
		switch record.Target {
		case childDigest:
			childSize = record.SizeBytes
		case "image-index":
			parentSize = record.SizeBytes
		}
	}
	if childSize <= 0 || parentSize <= childSize {
		t.Fatalf("unexpected manifest sizes child=%d parent=%d", childSize, parentSize)
	}
	blobs, err := cache.imageCacheBlobRecords()
	if err != nil {
		t.Fatalf("blob records: %v", err)
	}
	var blobBytes int64
	for _, blob := range blobs {
		if blob.Digest == configDigest || blob.Digest == layerDigest {
			blobBytes += blob.SizeBytes
		}
	}
	budget := childSize + blobBytes
	if budget >= parentSize {
		t.Fatalf("test budget %d must be smaller than parent %d", budget, parentSize)
	}

	result := postImageCachePrune(t, cache, fmt.Sprintf(`{"dry_run":false,"allow_delete":true,"include_unreferenced_blobs":true,"targets":[{"repo":"fugue-apps/demo","target":"%s"},{"repo":"fugue-apps/demo","target":"%s"}],"max_delete_bytes":"%d"}`, childDigest, "image-index", budget))
	if result.Deleted {
		t.Fatalf("prune deleted data while parent graph survived: %+v", result)
	}
	if _, err := os.Stat(childPath); err != nil {
		t.Fatalf("child journal was removed: %v", err)
	}
	if _, err := os.Stat(parentPath); err != nil {
		t.Fatalf("parent journal was removed: %v", err)
	}
	for _, digest := range []string{configDigest, layerDigest} {
		if _, err := os.Stat(imageCacheBlobPath(storeDir, digest)); err != nil {
			t.Fatalf("child blob %s was removed: %v", digest, err)
		}
	}
	foundProtected := false
	for _, entry := range result.SkippedManifests {
		if entry.Target == childDigest && entry.SkipReason == "referenced_manifest" {
			foundProtected = true
			break
		}
	}
	if !foundProtected {
		t.Fatalf("expected budget-surviving parent to protect child, got %+v", result.SkippedManifests)
	}
}

func TestImageCacheStreamsBlobUploadToDisk(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir: storeDir,
		registry: registry.New(registry.WithBlobHandler(
			registry.NewDiskBlobHandler(storeDir),
		)),
	}
	firstChunk := bytes.Repeat([]byte("a"), 256*1024)
	secondChunk := bytes.Repeat([]byte("b"), 256*1024)
	blob := append(append([]byte(nil), firstChunk...), secondChunk...)
	digest := testImageCacheBlobDigest(blob)

	uploadLocation, uploadID := startImageCacheBlobUpload(t, cache, "fugue-apps/demo")
	patch := httptest.NewRequest(http.MethodPatch, "http://image-cache.test"+uploadLocation, bytes.NewReader(firstChunk))
	patchRec := httptest.NewRecorder()
	cache.ServeHTTP(patchRec, patch)
	if patchRec.Code != http.StatusAccepted {
		t.Fatalf("patch status = %d, want %d; body=%q", patchRec.Code, http.StatusAccepted, patchRec.Body.String())
	}
	if got, want := patchRec.Header().Get("Range"), fmt.Sprintf("0-%d", len(firstChunk)-1); got != want {
		t.Fatalf("patch range = %q, want %q", got, want)
	}

	putURL := "http://image-cache.test" + uploadLocation + "?digest=" + url.QueryEscape(digest)
	put := httptest.NewRequest(http.MethodPut, putURL, bytes.NewReader(secondChunk))
	putRec := httptest.NewRecorder()
	cache.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusCreated {
		t.Fatalf("put status = %d, want %d; body=%q", putRec.Code, http.StatusCreated, putRec.Body.String())
	}
	if got := putRec.Header().Get("Docker-Content-Digest"); got != digest {
		t.Fatalf("Docker-Content-Digest = %q, want %q", got, digest)
	}
	if _, err := os.Stat(imageCacheBlobPath(storeDir, digest)); err != nil {
		t.Fatalf("expected blob to be stored on disk: %v", err)
	}
	if _, err := os.Stat(cache.blobUploadDataPath(uploadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected upload data to be removed, stat err=%v", err)
	}

	head := httptest.NewRequest(http.MethodHead, "http://image-cache.test/v2/fugue-apps/demo/blobs/"+digest, nil)
	headRec := httptest.NewRecorder()
	cache.ServeHTTP(headRec, head)
	if headRec.Code != http.StatusOK {
		t.Fatalf("head status = %d, want %d; body=%q", headRec.Code, http.StatusOK, headRec.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "http://image-cache.test/v2/fugue-apps/demo/blobs/"+digest, nil)
	getRec := httptest.NewRecorder()
	cache.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d; body=%q", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	if !bytes.Equal(getRec.Body.Bytes(), blob) {
		t.Fatalf("get body length = %d, want %d", getRec.Body.Len(), len(blob))
	}
}

func TestImageCacheCompletesBlobUploadWithPutBody(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir: storeDir,
		registry: registry.New(registry.WithBlobHandler(
			registry.NewDiskBlobHandler(storeDir),
		)),
	}
	blob := []byte("single put body")
	digest := testImageCacheBlobDigest(blob)
	uploadLocation, _ := startImageCacheBlobUpload(t, cache, "fugue-apps/demo")

	put := httptest.NewRequest(http.MethodPut, "http://image-cache.test"+uploadLocation+"?digest="+url.QueryEscape(digest), bytes.NewReader(blob))
	putRec := httptest.NewRecorder()
	cache.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusCreated {
		t.Fatalf("put status = %d, want %d; body=%q", putRec.Code, http.StatusCreated, putRec.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "http://image-cache.test/v2/fugue-apps/demo/blobs/"+digest, nil)
	getRec := httptest.NewRecorder()
	cache.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d; body=%q", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	if !bytes.Equal(getRec.Body.Bytes(), blob) {
		t.Fatalf("get body = %q, want %q", getRec.Body.String(), string(blob))
	}
}

func TestImageCacheRejectsBlobUploadDigestMismatch(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir: storeDir,
		registry: registry.New(registry.WithBlobHandler(
			registry.NewDiskBlobHandler(storeDir),
		)),
	}
	blob := []byte("actual blob")
	wrongDigest := testImageCacheBlobDigest([]byte("different blob"))
	uploadLocation, uploadID := startImageCacheBlobUpload(t, cache, "fugue-apps/demo")

	put := httptest.NewRequest(http.MethodPut, "http://image-cache.test"+uploadLocation+"?digest="+url.QueryEscape(wrongDigest), bytes.NewReader(blob))
	putRec := httptest.NewRecorder()
	cache.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusBadRequest {
		t.Fatalf("put status = %d, want %d; body=%q", putRec.Code, http.StatusBadRequest, putRec.Body.String())
	}
	if _, err := os.Stat(imageCacheBlobPath(storeDir, wrongDigest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mismatched blob should not be stored, stat err=%v", err)
	}
	if _, err := os.Stat(cache.blobUploadDataPath(uploadID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mismatched upload data should be removed, stat err=%v", err)
	}
}

func TestImageCachePruneSkipsDeleteBelowWatermark(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir:    storeDir,
		manifestDir: filepath.Join(storeDir, "_manifests"),
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 100,
			LowWatermarkPercent:  99,
			MinFreeBytes:         0,
			MaxDeleteBytesPerRun: 1 << 20,
		},
	}
	layerDigest := writeTestImageCacheBlob(t, storeDir, []byte("layer-a"))
	configDigest := writeTestImageCacheBlob(t, storeDir, []byte("{}"))
	manifest := testImageCacheManifest(configDigest, layerDigest)
	if err := cache.persistManifest("fugue-apps/demo", "image-a", "application/vnd.oci.image.manifest.v1+json", []byte(manifest)); err != nil {
		t.Fatalf("persist manifest: %v", err)
	}

	result := postImageCachePrune(t, cache, `{"dry_run":false,"allow_delete":true,"max_delete_bytes":"1Mi"}`)

	if result.Deleted {
		t.Fatalf("expected prune to skip deletion below watermark, got %+v", result)
	}
	if result.SkippedReason != "below_watermark" {
		t.Fatalf("skipped_reason = %q, want below_watermark", result.SkippedReason)
	}
	if _, err := os.Stat(imageCacheBlobPath(storeDir, layerDigest)); err != nil {
		t.Fatalf("expected layer blob to remain: %v", err)
	}
	records, err := cache.managementManifestRecords()
	if err != nil {
		t.Fatalf("manifest records: %v", err)
	}
	if len(records) != 1 || records[0].Target != "image-a" {
		t.Fatalf("expected manifest to remain, got %+v", records)
	}
}

func TestImageCachePruneDeletesUnreferencedBlobsBelowWatermarkWhenIncluded(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir:    storeDir,
		manifestDir: filepath.Join(storeDir, "_manifests"),
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 100,
			LowWatermarkPercent:  99,
			MinFreeBytes:         0,
			MaxDeleteBytesPerRun: 1 << 20,
		},
	}
	orphanDigest := writeTestImageCacheBlob(t, storeDir, []byte("orphan-layer"))

	result := postImageCachePrune(t, cache, `{"dry_run":false,"allow_delete":true,"include_unreferenced_blobs":true,"max_delete_bytes":"1Mi"}`)

	if !result.Deleted {
		t.Fatalf("expected include_unreferenced_blobs prune to delete below watermark, got %+v", result)
	}
	if result.DeletedBytes == 0 || result.PlannedDeleteBytes == 0 {
		t.Fatalf("expected deleted/planned bytes, got %+v", result)
	}
	if _, err := os.Stat(imageCacheBlobPath(storeDir, orphanDigest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected orphan blob to be deleted, stat err=%v", err)
	}
}

func TestImageCachePruneDeletesOnlySelectedUnpinnedManifestAndUnsharedBlobs(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir:    storeDir,
		manifestDir: filepath.Join(storeDir, "_manifests"),
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 0.01,
			LowWatermarkPercent:  0.01,
			MinFreeBytes:         0,
			MaxDeleteBytesPerRun: 1 << 30,
		},
	}
	sharedDigest := writeTestImageCacheBlob(t, storeDir, []byte("shared-layer"))
	onlyADigest := writeTestImageCacheBlob(t, storeDir, []byte("only-a"))
	configADigest := writeTestImageCacheBlob(t, storeDir, []byte(`{"config":"a"}`))
	configBDigest := writeTestImageCacheBlob(t, storeDir, []byte(`{"config":"b"}`))
	manifestA := testImageCacheManifest(configADigest, onlyADigest, sharedDigest)
	manifestB := testImageCacheManifest(configBDigest, sharedDigest)
	if err := cache.persistManifest("fugue-apps/demo", "image-a", "application/vnd.oci.image.manifest.v1+json", []byte(manifestA)); err != nil {
		t.Fatalf("persist manifest a: %v", err)
	}
	if err := cache.persistManifest("fugue-apps/demo", "image-b", "application/vnd.oci.image.manifest.v1+json", []byte(manifestB)); err != nil {
		t.Fatalf("persist manifest b: %v", err)
	}

	result := postImageCachePrune(t, cache, `{"dry_run":false,"allow_delete":true,"image_ref":"registry.fugue.internal:5000/fugue-apps/demo:image-a","max_delete_bytes":"1Gi"}`)

	if !result.Deleted {
		t.Fatalf("expected prune to delete selected image, got %+v", result)
	}
	for _, digest := range []string{onlyADigest, configADigest} {
		if _, err := os.Stat(imageCacheBlobPath(storeDir, digest)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected blob %s to be deleted, stat err=%v", digest, err)
		}
	}
	for _, digest := range []string{sharedDigest, configBDigest} {
		if _, err := os.Stat(imageCacheBlobPath(storeDir, digest)); err != nil {
			t.Fatalf("expected blob %s to remain: %v", digest, err)
		}
	}
	records, err := cache.managementManifestRecords()
	if err != nil {
		t.Fatalf("manifest records: %v", err)
	}
	if len(records) != 1 || records[0].Target != "image-b" {
		t.Fatalf("expected only image-b manifest to remain, got %+v", records)
	}
}

func TestImageCachePruneDoesNotExpandDigestTargetToUntargetedAlias(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir:    storeDir,
		manifestDir: filepath.Join(storeDir, "_manifests"),
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 0.01,
			LowWatermarkPercent:  0.01,
			MinFreeBytes:         0,
			MaxDeleteBytesPerRun: 1 << 30,
		},
	}
	layerDigest := writeTestImageCacheBlob(t, storeDir, []byte("live-layer"))
	configDigest := writeTestImageCacheBlob(t, storeDir, []byte(`{"config":"live"}`))
	manifest := testImageCacheManifest(configDigest, layerDigest)
	manifestDigest := manifestBodyDigest([]byte(manifest))
	for _, target := range []string{"current", manifestDigest} {
		if err := cache.persistManifest("fugue-apps/demo", target, "application/vnd.oci.image.manifest.v1+json", []byte(manifest)); err != nil {
			t.Fatalf("persist manifest target %s: %v", target, err)
		}
	}

	result := postImageCachePrune(t, cache, fmt.Sprintf(`{"dry_run":false,"allow_delete":true,"targets":[{"repo":"fugue-apps/demo","target":%q,"digest":%q}],"max_delete_bytes":"1Gi"}`, manifestDigest, manifestDigest))

	if !result.Deleted || result.SelectedCount != 1 {
		t.Fatalf("expected only the explicit digest target to be deleted, got %+v", result)
	}
	if len(result.SelectedManifests) != 1 || result.SelectedManifests[0].Target != manifestDigest {
		t.Fatalf("unexpected selected manifests: %+v", result.SelectedManifests)
	}
	records, err := cache.managementManifestRecords()
	if err != nil {
		t.Fatalf("manifest records: %v", err)
	}
	if len(records) != 1 || records[0].Target != "current" {
		t.Fatalf("expected untargeted current alias to remain, got %+v", records)
	}
	for _, digest := range []string{configDigest, layerDigest} {
		if _, err := os.Stat(imageCacheBlobPath(storeDir, digest)); err != nil {
			t.Fatalf("expected blob %s to remain while current alias exists: %v", digest, err)
		}
	}
}

func TestImageCachePruneKeepsBlobsWhenDigestManifestStillServed(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	manifestDir := filepath.Join(storeDir, "_manifests")
	cache := &imageCache{
		registry:    registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
		storeDir:    storeDir,
		manifestDir: manifestDir,
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 0.01,
			LowWatermarkPercent:  0.01,
			MinFreeBytes:         0,
			MaxDeleteBytesPerRun: 1 << 30,
		},
	}
	configDigest := writeTestImageCacheBlob(t, storeDir, []byte(`{"config":"live"}`))
	layerDigest := writeTestImageCacheBlob(t, storeDir, []byte("live-layer"))
	manifest := testImageCacheManifest(configDigest, layerDigest)
	manifestDigest := manifestBodyDigest([]byte(manifest))
	put := httptest.NewRequest(http.MethodPut, "http://image-cache.test/v2/fugue-apps/demo/manifests/fugue-live-abcdef123456", strings.NewReader(manifest))
	put.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	putRec := httptest.NewRecorder()
	cache.registry.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusCreated {
		t.Fatalf("put status = %d, want %d; body=%q", putRec.Code, http.StatusCreated, putRec.Body.String())
	}
	if err := cache.persistManifest("fugue-apps/demo", "fugue-live-abcdef123456", "application/vnd.oci.image.manifest.v1+json", []byte(manifest)); err != nil {
		t.Fatalf("persist tag manifest: %v", err)
	}

	result := postImageCachePrune(t, cache, fmt.Sprintf(`{"dry_run":false,"allow_delete":true,"targets":[{"repo":"fugue-apps/demo","target":"fugue-live-abcdef123456","digest":%q}],"max_delete_bytes":"1Gi"}`, manifestDigest))

	if !result.Deleted {
		t.Fatalf("expected prune to delete selected manifest entries, got %+v", result)
	}
	head := httptest.NewRequest(http.MethodHead, "http://image-cache.test/v2/fugue-apps/demo/manifests/"+manifestDigest, nil)
	headRec := httptest.NewRecorder()
	cache.ServeHTTP(headRec, head)
	if headRec.Code != http.StatusOK {
		t.Fatalf("digest manifest status = %d, want %d; body=%q", headRec.Code, http.StatusOK, headRec.Body.String())
	}
	for _, digest := range []string{configDigest, layerDigest} {
		if _, err := os.Stat(imageCacheBlobPath(storeDir, digest)); err != nil {
			t.Fatalf("expected blob %s to remain while digest manifest is served: %v", digest, err)
		}
	}
}

func TestImageCacheBatchPruneDryRunDoesNotMutateFilesystem(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir:    storeDir,
		manifestDir: filepath.Join(storeDir, "_manifests"),
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 100,
			LowWatermarkPercent:  99,
			MinFreeBytes:         0,
			MaxDeleteBytesPerRun: 1 << 30,
		},
	}
	layerDigest := writeTestImageCacheBlob(t, storeDir, []byte("layer-a"))
	configDigest := writeTestImageCacheBlob(t, storeDir, []byte("{}"))
	manifest := testImageCacheManifest(configDigest, layerDigest)
	if err := cache.persistManifest("fugue-apps/demo", "image-a", "application/vnd.oci.image.manifest.v1+json", []byte(manifest)); err != nil {
		t.Fatalf("persist manifest: %v", err)
	}

	result := postImageCachePrune(t, cache, `{"dry_run":true,"allow_delete":false,"targets":[{"repo":"fugue-apps/demo","target":"image-a"}],"max_delete_bytes":"1Gi"}`)

	if result.Deleted {
		t.Fatalf("dry-run unexpectedly deleted data: %+v", result)
	}
	if result.SelectedCount != 1 {
		t.Fatalf("selected_count = %d, want 1; result=%+v", result.SelectedCount, result)
	}
	if result.PlannedDeleteBytes <= 0 {
		t.Fatalf("planned_delete_bytes = %d, want positive; result=%+v", result.PlannedDeleteBytes, result)
	}
	if _, err := os.Stat(imageCacheBlobPath(storeDir, layerDigest)); err != nil {
		t.Fatalf("expected layer blob to remain after dry-run: %v", err)
	}
	records, err := cache.managementManifestRecords()
	if err != nil {
		t.Fatalf("manifest records: %v", err)
	}
	if len(records) != 1 || records[0].Target != "image-a" {
		t.Fatalf("expected manifest to remain after dry-run, got %+v", records)
	}
}

func TestImageCachePruneAllowDeleteFalseDoesNotMutateFilesystem(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir:    storeDir,
		manifestDir: filepath.Join(storeDir, "_manifests"),
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 0.01,
			LowWatermarkPercent:  0.01,
			MinFreeBytes:         0,
			MaxDeleteBytesPerRun: 1 << 30,
		},
	}
	layerDigest := writeTestImageCacheBlob(t, storeDir, []byte("layer-a"))
	configDigest := writeTestImageCacheBlob(t, storeDir, []byte("{}"))
	manifest := testImageCacheManifest(configDigest, layerDigest)
	if err := cache.persistManifest("fugue-apps/demo", "image-a", "application/vnd.oci.image.manifest.v1+json", []byte(manifest)); err != nil {
		t.Fatalf("persist manifest: %v", err)
	}

	result := postImageCachePrune(t, cache, `{"dry_run":false,"allow_delete":false,"targets":[{"repo":"fugue-apps/demo","target":"image-a"}],"max_delete_bytes":"1Gi"}`)

	if result.Deleted {
		t.Fatalf("allow_delete=false unexpectedly deleted data: %+v", result)
	}
	if result.SkippedReason != "allow_delete_false" {
		t.Fatalf("skipped_reason = %q, want allow_delete_false", result.SkippedReason)
	}
	if _, err := os.Stat(imageCacheBlobPath(storeDir, layerDigest)); err != nil {
		t.Fatalf("expected layer blob to remain: %v", err)
	}
	records, err := cache.managementManifestRecords()
	if err != nil {
		t.Fatalf("manifest records: %v", err)
	}
	if len(records) != 1 || records[0].Target != "image-a" {
		t.Fatalf("expected manifest to remain, got %+v", records)
	}
}

func TestImageCachePruneMinManifestAgeProtectsFreshManifest(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir:    storeDir,
		manifestDir: filepath.Join(storeDir, "_manifests"),
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 0.01,
			LowWatermarkPercent:  0.01,
			MinFreeBytes:         0,
			MaxDeleteBytesPerRun: 1 << 30,
		},
	}
	layerDigest := writeTestImageCacheBlob(t, storeDir, []byte("layer-a"))
	configDigest := writeTestImageCacheBlob(t, storeDir, []byte("{}"))
	manifest := testImageCacheManifest(configDigest, layerDigest)
	if err := cache.persistManifest("fugue-apps/demo", "image-a", "application/vnd.oci.image.manifest.v1+json", []byte(manifest)); err != nil {
		t.Fatalf("persist manifest: %v", err)
	}

	result := postImageCachePrune(t, cache, `{"dry_run":false,"allow_delete":true,"targets":[{"repo":"fugue-apps/demo","target":"image-a"}],"max_delete_bytes":"1Gi","min_manifest_age":"24h"}`)

	if result.Deleted {
		t.Fatalf("fresh manifest unexpectedly deleted: %+v", result)
	}
	if result.SelectedCount != 0 {
		t.Fatalf("selected_count = %d, want 0", result.SelectedCount)
	}
	if len(result.SkippedManifests) != 1 {
		t.Fatalf("skipped manifest count = %d, want 1; result=%+v", len(result.SkippedManifests), result)
	}
}

func TestImageCachePruneBudgetExhaustedBeforeManifestDelete(t *testing.T) {
	t.Parallel()

	storeDir := t.TempDir()
	cache := &imageCache{
		storeDir:    storeDir,
		manifestDir: filepath.Join(storeDir, "_manifests"),
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 0.01,
			LowWatermarkPercent:  0.01,
			MinFreeBytes:         0,
			MaxDeleteBytesPerRun: 1,
		},
	}
	layerDigest := writeTestImageCacheBlob(t, storeDir, []byte("layer-a"))
	configDigest := writeTestImageCacheBlob(t, storeDir, []byte("{}"))
	manifest := testImageCacheManifest(configDigest, layerDigest)
	if err := cache.persistManifest("fugue-apps/demo", "image-a", "application/vnd.oci.image.manifest.v1+json", []byte(manifest)); err != nil {
		t.Fatalf("persist manifest: %v", err)
	}

	result := postImageCachePrune(t, cache, `{"dry_run":false,"allow_delete":true,"targets":[{"repo":"fugue-apps/demo","target":"image-a"}],"max_delete_bytes":"1"}`)

	if result.Deleted {
		t.Fatalf("budget-exhausted prune unexpectedly deleted data: %+v", result)
	}
	if !result.BudgetExhausted {
		t.Fatalf("budget_exhausted = false, want true; result=%+v", result)
	}
	if _, err := os.Stat(imageCacheBlobPath(storeDir, layerDigest)); err != nil {
		t.Fatalf("expected layer blob to remain: %v", err)
	}
}

func TestImageCacheDiskLimitNormalizationClampsPercentages(t *testing.T) {
	t.Parallel()

	cache := &imageCache{
		diskLimit: imageCacheDiskLimit{
			Enabled:              true,
			HighWatermarkPercent: 150,
			LowWatermarkPercent:  125,
			MinFreeBytes:         -1,
			MaxDeleteBytesPerRun: 0,
		},
	}
	limit := cache.normalizedDiskLimit()

	if limit.HighWatermarkPercent != 100 {
		t.Fatalf("high watermark = %.2f, want 100", limit.HighWatermarkPercent)
	}
	if limit.LowWatermarkPercent != 100 {
		t.Fatalf("low watermark = %.2f, want 100", limit.LowWatermarkPercent)
	}
	if limit.MinFreeBytes != 0 {
		t.Fatalf("min free bytes = %d, want 0", limit.MinFreeBytes)
	}
	if limit.MaxDeleteBytesPerRun != defaultImageCacheMaxDeleteBytesPerRun {
		t.Fatalf("max delete bytes = %d, want default %d", limit.MaxDeleteBytesPerRun, defaultImageCacheMaxDeleteBytesPerRun)
	}
}

func TestEffectiveImageCacheMinFreeBytesClampsToReachableLowWatermark(t *testing.T) {
	t.Parallel()

	const giB = int64(1024 * 1024 * 1024)
	limit := imageCacheDiskLimit{
		LowWatermarkPercent: 45,
		MinFreeBytes:        50 * giB,
	}
	totalBytes := int64(42 * giB)
	want := totalBytes - int64(float64(totalBytes)*0.45)

	if got := effectiveImageCacheMinFreeBytes(limit, totalBytes); got != want {
		t.Fatalf("effective min free bytes = %d, want %d", got, want)
	}

	largeDisk := int64(400 * giB)
	if got := effectiveImageCacheMinFreeBytes(limit, largeDisk); got != limit.MinFreeBytes {
		t.Fatalf("effective min free bytes on large disk = %d, want configured %d", got, limit.MinFreeBytes)
	}
}

func TestHydrateMarksMissingPeerLocationStale(t *testing.T) {
	t.Parallel()

	reports := []url.Values{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/image-locations":
			if got := r.URL.Query().Get("image_ref"); got != "registry.fugue.internal:5000/fugue-apps/demo:image-missing" {
				t.Fatalf("image_ref query = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"image_locations": []imageLocation{{
					TenantID:          "tenant-1",
					AppID:             "app-1",
					SourceOperationID: "op-1",
					RuntimeID:         "runtime-1",
					ClusterNodeName:   "worker-1",
					CacheEndpoint:     "http://10.0.0.1:5000",
					Status:            "present",
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/image-locations":
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			reports = append(reports, r.Form)
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(server.Close)

	cache := &imageCache{
		apiBase:        server.URL,
		apiToken:       "token",
		reportPath:     "/v1/image-locations",
		lookupPath:     "/v1/image-locations",
		registryBase:   "registry.fugue.internal:5000",
		localBase:      "127.0.0.1:5000",
		cacheEndpoint:  "http://10.0.0.2:5000",
		httpClient:     server.Client(),
		hydrateTimeout: 5 * time.Second,
		copyImageFn: func(context.Context, string, string) error {
			return errors.New("GET http://10.0.0.1:5000/v2/fugue-apps/demo/manifests/image-missing: NAME_UNKNOWN: Unknown name")
		},
	}

	if err := cache.hydrate(context.Background(), "fugue-apps/demo", "image-missing"); err == nil {
		t.Fatal("expected hydrate to fail")
	}

	found := false
	for _, form := range reports {
		if form.Get("status") != "missing" || form.Get("cache_endpoint") != "http://10.0.0.1:5000" {
			continue
		}
		found = true
		if form.Get("tenant_id") != "tenant-1" || form.Get("app_id") != "app-1" || form.Get("source_operation_id") != "op-1" {
			t.Fatalf("stale peer report lost app identity: %v", form)
		}
		if form.Get("runtime_id") != "runtime-1" || form.Get("cluster_node_name") != "worker-1" {
			t.Fatalf("stale peer report lost node identity: %v", form)
		}
	}
	if !found {
		t.Fatalf("expected missing peer location report, got %v", reports)
	}
}

func TestReportRegistryWriteReportsLogicalImageLocation(t *testing.T) {
	reported := make(chan url.Values, 2)
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
	const manifest = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.empty.v1+json","digest":"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","size":2},"layers":[]}`
	manifestDigest := manifestBodyDigest([]byte(manifest))
	req := httptest.NewRequest(http.MethodPut, "http://127.0.0.1:5000/v2/fugue-apps/demo/manifests/git-abc123", nil)

	cache.reportRegistryWrite(req, http.StatusCreated, []byte(manifest))

	seen := map[string]url.Values{}
	for len(seen) < 2 {
		select {
		case form := <-reported:
			seen[form.Get("image_ref")] = form
		case <-time.After(time.Second):
			t.Fatal("expected image location reports")
		}
	}
	for _, imageRef := range []string{
		"registry.fugue.internal:5000/fugue-apps/demo:git-abc123",
		"registry.fugue.internal:5000/fugue-apps/demo@" + manifestDigest,
	} {
		form := seen[imageRef]
		if form == nil {
			t.Fatalf("missing image location report for %s; got %v", imageRef, seen)
		}
		if got := form.Get("digest"); got != manifestDigest {
			t.Fatalf("digest for %s = %q, want %q", imageRef, got, manifestDigest)
		}
		if got := form.Get("status"); got != "present" {
			t.Fatalf("status = %q", got)
		}
		if got := form.Get("cache_endpoint"); got != "http://10.0.0.2:5000" {
			t.Fatalf("cache_endpoint = %q", got)
		}
	}
}

func TestReportIncludesClusterNodeIdentity(t *testing.T) {
	reported := make(chan url.Values, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
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
		cacheEndpoint: "http://10.0.0.2:5000",
		clusterNode:   "worker-2",
		httpClient:    server.Client(),
	}

	if err := cache.report(context.Background(), "registry.fugue.internal:5000/fugue-apps/demo:image-test", "", "present", ""); err != nil {
		t.Fatalf("report: %v", err)
	}

	select {
	case form := <-reported:
		if got := form.Get("cache_endpoint"); got != "http://10.0.0.2:5000" {
			t.Fatalf("cache_endpoint = %q", got)
		}
		if got := form.Get("cluster_node_name"); got != "worker-2" {
			t.Fatalf("cluster_node_name = %q, want worker-2", got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected image location report")
	}
}

type imageCachePruneTestResult struct {
	Deleted            bool                      `json:"deleted"`
	SkippedReason      string                    `json:"skipped_reason"`
	DeletedBytes       int64                     `json:"deleted_bytes"`
	PlannedDeleteBytes int64                     `json:"planned_delete_bytes"`
	SelectedCount      int                       `json:"selected_count"`
	BudgetExhausted    bool                      `json:"budget_exhausted"`
	SkippedManifests   []imageCacheManifestEntry `json:"skipped_manifests"`
	SelectedManifests  []imageCacheManifestEntry `json:"selected_manifests"`
}

func postImageCachePrune(t *testing.T, cache *imageCache, body string) imageCachePruneTestResult {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://image-cache.test/fugue/cache/v1/prune", strings.NewReader(body))
	rec := httptest.NewRecorder()
	cache.handleManagementPrune(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("prune status = %d, body=%q", rec.Code, rec.Body.String())
	}
	var result imageCachePruneTestResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode prune response: %v; body=%s", err, rec.Body.String())
	}
	return result
}

func startImageCacheBlobUpload(t *testing.T, cache *imageCache, repo string) (string, string) {
	t.Helper()
	post := httptest.NewRequest(http.MethodPost, "http://image-cache.test/v2/"+strings.Trim(repo, "/")+"/blobs/uploads/", nil)
	postRec := httptest.NewRecorder()
	cache.ServeHTTP(postRec, post)
	if postRec.Code != http.StatusAccepted {
		t.Fatalf("post status = %d, want %d; body=%q", postRec.Code, http.StatusAccepted, postRec.Body.String())
	}
	location := postRec.Header().Get("Location")
	uploadID := postRec.Header().Get("Docker-Upload-UUID")
	if location == "" || uploadID == "" {
		t.Fatalf("missing upload headers: Location=%q Docker-Upload-UUID=%q", location, uploadID)
	}
	return location, uploadID
}

func testImageCacheBlobDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return fmt.Sprintf("sha256:%x", sum[:])
}

func writeTestImageCacheBlob(t *testing.T, storeDir string, body []byte) string {
	t.Helper()
	digest := testImageCacheBlobDigest(body)
	path := imageCacheBlobPath(storeDir, digest)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir blob dir: %v", err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write blob: %v", err)
	}
	return digest
}

func imageCacheBlobPath(storeDir, digest string) string {
	parts := strings.SplitN(digest, ":", 2)
	if len(parts) != 2 {
		return filepath.Join(storeDir, "invalid")
	}
	return filepath.Join(storeDir, parts[0], parts[1])
}

func testImageCacheManifest(configDigest string, layerDigests ...string) string {
	layers := make([]string, 0, len(layerDigests))
	for _, digest := range layerDigests {
		layers = append(layers, fmt.Sprintf(`{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":%q,"size":1}`, digest))
	}
	return fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":%q,"size":2},"layers":[%s]}`, configDigest, strings.Join(layers, ","))
}

func TestFetchKubernetesPodNodeName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if got := r.URL.Path; got != "/api/v1/namespaces/fugue-system/pods/fugue-image-cache-abc" {
			t.Fatalf("path = %s", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer service-account-token" {
			t.Fatalf("Authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"spec": map[string]any{
				"nodeName": "worker-image-1",
			},
		})
	}))
	t.Cleanup(server.Close)

	nodeName, err := fetchKubernetesPodNodeName(context.Background(), server.Client(), server.URL, "service-account-token", "fugue-system", "fugue-image-cache-abc")
	if err != nil {
		t.Fatalf("fetch pod node name: %v", err)
	}
	if nodeName != "worker-image-1" {
		t.Fatalf("nodeName = %q, want worker-image-1", nodeName)
	}
}

func TestFetchKubernetesPodNodeNameByHostIP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if got := r.URL.Path; got != "/api/v1/namespaces/fugue-system/pods" {
			t.Fatalf("path = %s", got)
		}
		if got := r.URL.Query().Get("labelSelector"); got != "app.kubernetes.io/component=image-cache" {
			t.Fatalf("labelSelector = %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer service-account-token" {
			t.Fatalf("Authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"metadata": map[string]any{"name": "fugue-image-cache-a"},
					"spec":     map[string]any{"nodeName": "worker-image-1"},
					"status":   map[string]any{"hostIP": "10.0.0.1"},
				},
				{
					"metadata": map[string]any{"name": "fugue-image-cache-b"},
					"spec":     map[string]any{"nodeName": "worker-image-2"},
					"status":   map[string]any{"hostIP": "10.0.0.2"},
				},
			},
		})
	}))
	t.Cleanup(server.Close)

	nodeName, err := fetchKubernetesPodNodeNameByHostIP(context.Background(), server.Client(), server.URL, "service-account-token", "fugue-system", "10.0.0.2")
	if err != nil {
		t.Fatalf("fetch pod node name by host IP: %v", err)
	}
	if nodeName != "worker-image-2" {
		t.Fatalf("nodeName = %q, want worker-image-2", nodeName)
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
