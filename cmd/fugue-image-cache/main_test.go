package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
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

	const manifest = `{"schemaVersion":1,"name":"fugue-apps/demo","tag":"image-test","fsLayers":[{"blobSum":"sha256:1111111111111111111111111111111111111111111111111111111111111111"}]}`
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
