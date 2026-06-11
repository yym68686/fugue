package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/registry"
)

type imageCache struct {
	apiBase        string
	apiToken       string
	reportPath     string
	lookupPath     string
	registryBase   string
	localBase      string
	upstreamBase   string
	cacheEndpoint  string
	httpClient     *http.Client
	registry       http.Handler
	hydrateTimeout time.Duration
	copyImageFn    func(context.Context, string, string) error
	hydrateMu      sync.Mutex
	hydrateCalls   map[string]*hydrateCall
}

type hydrateCall struct {
	done    chan struct{}
	cancel  context.CancelFunc
	err     error
	waiters int
}

type imageLocation struct {
	ImageRef      string `json:"image_ref"`
	Digest        string `json:"digest"`
	CacheEndpoint string `json:"cache_endpoint"`
	Status        string `json:"status"`
}

func main() {
	listenAddr := env("FUGUE_IMAGE_CACHE_LISTEN_ADDR", ":5000")
	storeDir := env("FUGUE_IMAGE_CACHE_STORE_DIR", "/var/lib/fugue/image-cache/registry")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		log.Fatalf("create cache store: %v", err)
	}
	apiToken := strings.TrimSpace(os.Getenv("FUGUE_NODE_UPDATER_TOKEN"))
	reportPath := "/v1/node-updater/image-locations"
	lookupPath := "/v1/node-updater/image-locations"
	if apiToken == "" {
		apiToken = strings.TrimSpace(os.Getenv("FUGUE_API_KEY"))
		reportPath = "/v1/image-locations"
		lookupPath = "/v1/image-locations"
	}
	cache := &imageCache{
		apiBase:        strings.TrimRight(env("FUGUE_API_BASE", os.Getenv("FUGUE_API_URL")), "/"),
		apiToken:       apiToken,
		reportPath:     reportPath,
		lookupPath:     lookupPath,
		registryBase:   trimRegistryBase(env("FUGUE_IMAGE_CACHE_REGISTRY_BASE", "registry.fugue.internal:5000")),
		localBase:      trimRegistryBase(env("FUGUE_IMAGE_CACHE_LOCAL_BASE", "127.0.0.1:5000")),
		upstreamBase:   trimRegistryBase(os.Getenv("FUGUE_IMAGE_CACHE_UPSTREAM_BASE")),
		cacheEndpoint:  strings.TrimRight(os.Getenv("FUGUE_IMAGE_CACHE_ENDPOINT"), "/"),
		httpClient:     &http.Client{Timeout: 15 * time.Second},
		registry:       registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
		hydrateTimeout: envDuration("FUGUE_IMAGE_CACHE_HYDRATE_TIMEOUT", 30*time.Minute),
	}
	if cache.apiBase == "" || cache.apiToken == "" {
		log.Print("control-plane API credentials are not configured; cache will serve local registry storage only")
	}
	if cache.cacheEndpoint == "" {
		cache.cacheEndpoint = "http://" + cache.localBase
	}
	log.Printf("fugue-image-cache listening on %s store=%s registry_base=%s local_base=%s endpoint=%s upstream=%s", listenAddr, filepath.Clean(storeDir), cache.registryBase, cache.localBase, cache.cacheEndpoint, cache.upstreamBase)
	server := &http.Server{
		Addr:              listenAddr,
		Handler:           cache,
		ReadHeaderTimeout: 15 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func (c *imageCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := ""
	if r.URL != nil {
		path = r.URL.Path
	}
	if path == "/healthz" || path == "/readyz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	if !isRegistryAPIPath(path) {
		http.NotFound(w, r)
		return
	}
	if !isRegistryPull(r) {
		c.registry.ServeHTTP(w, r)
		return
	}
	rec := newDeferredNotFoundWriter(w)
	c.registry.ServeHTTP(rec, r)
	if !rec.notFound() {
		rec.flush()
		return
	}
	repo, target, targetKind, ok := parseRegistryTarget(r.URL.Path)
	if !ok {
		rec.flush()
		return
	}
	if targetKind == registryTargetBlob && c.proxyBlobFromUpstream(w, r, repo, target) {
		return
	}
	if err := c.hydrate(r.Context(), repo, target); err != nil {
		log.Printf("hydrate repo=%s target=%s failed: %v", repo, target, err)
		rec.flush()
		return
	}
	c.registry.ServeHTTP(w, r)
}

type registryTargetKind string

const (
	registryTargetManifest registryTargetKind = "manifest"
	registryTargetBlob     registryTargetKind = "blob"
)

func (c *imageCache) hydrate(parent context.Context, repo, target string) error {
	key := repo + "\x00" + target
	call := c.joinHydrate(key, repo, target)
	defer c.leaveHydrate(call)

	select {
	case <-call.done:
		return call.err
	case <-parent.Done():
		return parent.Err()
	}
}

func (c *imageCache) joinHydrate(key, repo, target string) *hydrateCall {
	c.hydrateMu.Lock()
	defer c.hydrateMu.Unlock()

	if c.hydrateCalls == nil {
		c.hydrateCalls = make(map[string]*hydrateCall)
	}
	if call := c.hydrateCalls[key]; call != nil {
		call.waiters++
		return call
	}

	ctx, cancel := context.WithCancel(context.Background())
	call := &hydrateCall{
		done:    make(chan struct{}),
		cancel:  cancel,
		waiters: 1,
	}
	c.hydrateCalls[key] = call
	go func() {
		err := c.hydrateOnce(ctx, repo, target)

		c.hydrateMu.Lock()
		call.err = err
		if c.hydrateCalls[key] == call {
			delete(c.hydrateCalls, key)
		}
		c.hydrateMu.Unlock()
		close(call.done)
	}()
	return call
}

func (c *imageCache) leaveHydrate(call *hydrateCall) {
	if call == nil {
		return
	}

	c.hydrateMu.Lock()
	defer c.hydrateMu.Unlock()

	if call.waiters > 0 {
		call.waiters--
	}
	if call.waiters != 0 {
		return
	}
	select {
	case <-call.done:
	default:
		call.cancel()
	}
}

func (c *imageCache) hydrateOnce(parent context.Context, repo, target string) error {
	ctx, cancel := context.WithTimeout(parent, c.hydrateTimeout)
	defer cancel()
	logicalRef, digest := imageRef(c.registryBase, repo, target)
	localRef, _ := imageRef(c.localBase, repo, target)
	_ = c.report(ctx, logicalRef, digest, "pulling", "")
	for _, location := range c.lookup(ctx, logicalRef, digest) {
		if strings.TrimSpace(location.CacheEndpoint) == "" || !strings.EqualFold(location.Status, "present") {
			continue
		}
		if sameEndpoint(location.CacheEndpoint, c.cacheEndpoint) {
			continue
		}
		peerBase := trimRegistryBase(location.CacheEndpoint)
		peerRef, _ := imageRef(peerBase, repo, target)
		if err := c.copyImage(ctx, peerRef, localRef); err == nil {
			log.Printf("hydrated %s from peer %s", logicalRef, peerBase)
			_ = c.report(ctx, logicalRef, digest, "present", "")
			return nil
		} else {
			log.Printf("peer hydrate %s from %s failed: %v", logicalRef, peerBase, err)
		}
	}
	if c.upstreamBase != "" {
		upstreamRef, _ := imageRef(c.upstreamBase, repo, target)
		if err := c.copyImage(ctx, upstreamRef, localRef); err == nil {
			log.Printf("hydrated %s from upstream %s", logicalRef, c.upstreamBase)
			_ = c.report(ctx, logicalRef, digest, "present", "")
			return nil
		} else {
			_ = c.report(ctx, logicalRef, digest, "failed", err.Error())
			return err
		}
	}
	_ = c.report(ctx, logicalRef, digest, "missing", "")
	return fmt.Errorf("no peer or upstream location for %s", logicalRef)
}

func (c *imageCache) copyImage(ctx context.Context, src, dst string) error {
	if c.copyImageFn != nil {
		return c.copyImageFn(ctx, src, dst)
	}
	return copyImage(ctx, src, dst)
}

func (c *imageCache) lookup(ctx context.Context, imageRef, digest string) []imageLocation {
	if c.apiBase == "" || c.apiToken == "" {
		return nil
	}
	values := url.Values{}
	values.Set("image_ref", imageRef)
	values.Set("status", "present")
	if digest != "" {
		values.Set("digest", digest)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiBase+c.lookupPath+"?"+values.Encode(), nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("lookup image locations failed: %v", err)
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		log.Printf("lookup image locations status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		return nil
	}
	var decoded struct {
		ImageLocations []imageLocation `json:"image_locations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		log.Printf("decode image locations failed: %v", err)
		return nil
	}
	return decoded.ImageLocations
}

func (c *imageCache) report(ctx context.Context, imageRef, digest, status, lastError string) error {
	if c.apiBase == "" || c.apiToken == "" {
		return nil
	}
	values := url.Values{}
	values.Set("image_ref", imageRef)
	values.Set("digest", digest)
	values.Set("status", status)
	values.Set("cache_endpoint", c.cacheEndpoint)
	values.Set("last_error", lastError)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiBase+c.reportPath, strings.NewReader(values.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("report image location status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func copyImage(ctx context.Context, src, dst string) error {
	return crane.Copy(src, dst, crane.WithContext(ctx), crane.Insecure)
}

func isRegistryPull(r *http.Request) bool {
	if r == nil || r.URL == nil {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	return strings.Contains(r.URL.Path, "/manifests/") || strings.Contains(r.URL.Path, "/blobs/")
}

func isRegistryAPIPath(path string) bool {
	return path == "/v2" || strings.HasPrefix(path, "/v2/")
}

func parseRegistryTarget(path string) (string, string, registryTargetKind, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 4 || parts[0] != "v2" {
		return "", "", "", false
	}
	for idx := 2; idx < len(parts)-1; idx++ {
		switch parts[idx] {
		case "manifests":
			repo := strings.Join(parts[1:idx], "/")
			target := parts[idx+1]
			return repo, target, registryTargetManifest, repo != "" && target != ""
		case "blobs":
			repo := strings.Join(parts[1:idx], "/")
			target := parts[idx+1]
			return repo, target, registryTargetBlob, repo != "" && target != ""
		}
	}
	return "", "", "", false
}

func (c *imageCache) proxyBlobFromUpstream(w http.ResponseWriter, r *http.Request, repo, digest string) bool {
	if c.upstreamBase == "" {
		return false
	}
	upstreamURL := "http://" + trimRegistryBase(c.upstreamBase) + "/v2/" + repo + "/blobs/" + digest
	req, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL, nil)
	if err != nil {
		log.Printf("build upstream blob request repo=%s digest=%s failed: %v", repo, digest, err)
		return false
	}
	if value := r.Header.Get("Range"); value != "" {
		req.Header.Set("Range", value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("upstream blob proxy repo=%s digest=%s failed: %v", repo, digest, err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("upstream blob proxy repo=%s digest=%s status=%d", repo, digest, resp.StatusCode)
		return false
	}
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if r.Method == http.MethodHead {
		return true
	}
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("stream upstream blob repo=%s digest=%s failed: %v", repo, digest, err)
	}
	return true
}

func imageRef(base, repo, target string) (string, string) {
	digest := ""
	sep := ":"
	if strings.HasPrefix(target, "sha256:") {
		digest = target
		sep = "@"
	}
	return trimRegistryBase(base) + "/" + repo + sep + target, digest
}

func trimRegistryBase(raw string) string {
	raw = strings.TrimRight(strings.TrimSpace(raw), "/")
	raw = strings.TrimPrefix(raw, "http://")
	raw = strings.TrimPrefix(raw, "https://")
	return raw
}

func sameEndpoint(left, right string) bool {
	return strings.EqualFold(trimRegistryBase(left), trimRegistryBase(right))
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return strings.TrimSpace(fallback)
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

type deferredNotFoundWriter struct {
	target      http.ResponseWriter
	header      http.Header
	status      int
	buffer      bytes.Buffer
	passthrough bool
}

func newDeferredNotFoundWriter(target http.ResponseWriter) *deferredNotFoundWriter {
	return &deferredNotFoundWriter{target: target, header: http.Header{}}
}

func (w *deferredNotFoundWriter) Header() http.Header {
	if w.passthrough {
		return w.target.Header()
	}
	return w.header
}

func (w *deferredNotFoundWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	if status == http.StatusNotFound {
		return
	}
	copyHeader(w.target.Header(), w.header)
	w.target.WriteHeader(status)
	w.passthrough = true
}

func (w *deferredNotFoundWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.passthrough {
		return w.target.Write(data)
	}
	return w.buffer.Write(data)
}

func (w *deferredNotFoundWriter) notFound() bool {
	return w.status == http.StatusNotFound
}

func (w *deferredNotFoundWriter) flush() {
	if w.passthrough {
		return
	}
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	copyHeader(w.target.Header(), w.header)
	w.target.WriteHeader(status)
	_, _ = io.Copy(w.target, &w.buffer)
}

func copyHeader(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}
