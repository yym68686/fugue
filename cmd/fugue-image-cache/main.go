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
		hydrateTimeout: 10 * time.Minute,
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
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
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
	repo, target, ok := parseRegistryTarget(r.URL.Path)
	if !ok {
		rec.flush()
		return
	}
	if err := c.hydrate(r.Context(), repo, target); err != nil {
		log.Printf("hydrate repo=%s target=%s failed: %v", repo, target, err)
		rec.flush()
		return
	}
	c.registry.ServeHTTP(w, r)
}

func (c *imageCache) hydrate(parent context.Context, repo, target string) error {
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
		if err := copyImage(ctx, peerRef, localRef); err == nil {
			log.Printf("hydrated %s from peer %s", logicalRef, peerBase)
			_ = c.report(ctx, logicalRef, digest, "present", "")
			return nil
		} else {
			log.Printf("peer hydrate %s from %s failed: %v", logicalRef, peerBase, err)
		}
	}
	if c.upstreamBase != "" {
		upstreamRef, _ := imageRef(c.upstreamBase, repo, target)
		if err := copyImage(ctx, upstreamRef, localRef); err == nil {
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
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	return strings.Contains(r.URL.Path, "/manifests/") || strings.Contains(r.URL.Path, "/blobs/")
}

func parseRegistryTarget(path string) (string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 4 || parts[0] != "v2" {
		return "", "", false
	}
	for idx := 2; idx < len(parts)-1; idx++ {
		if parts[idx] == "manifests" || parts[idx] == "blobs" {
			repo := strings.Join(parts[1:idx], "/")
			target := parts[idx+1]
			return repo, target, repo != "" && target != ""
		}
	}
	return "", "", false
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
