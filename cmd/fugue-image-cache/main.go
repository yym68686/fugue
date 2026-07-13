package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/registry"
)

const (
	serviceAccountTokenPath     = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	serviceAccountCAPath        = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	serviceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

const (
	defaultImageCacheHighWatermarkPercent = 55
	defaultImageCacheLowWatermarkPercent  = 45
	defaultImageCacheMinFreeBytes         = int64(50 * 1024 * 1024 * 1024)
	defaultImageCacheMaxDeleteBytesPerRun = int64(10 * 1024 * 1024 * 1024)
	imageCacheUploadDirName               = "_uploads"
)

type imageCache struct {
	apiBase         string
	apiToken        string
	reportPath      string
	lookupPath      string
	registryBase    string
	localBase       string
	upstreamBase    string
	cacheEndpoint   string
	clusterNode     string
	storeDir        string
	manifestDir     string
	pinStorePath    string
	managementToken string
	httpClient      *http.Client
	registry        http.Handler
	hydrateTimeout  time.Duration
	hydrateSlots    chan struct{}
	proxySlots      chan struct{}
	copyJobs        int
	copyImageFn     func(context.Context, string, string) error
	diskLimit       imageCacheDiskLimit
	hydrateMu       sync.Mutex
	hydrateCalls    map[string]*hydrateCall
	sourceMu        sync.RWMutex
	sourceByTarget  map[string]sourceCacheEntry
	sourceTTL       time.Duration
}

type imageCacheDiskLimit struct {
	Enabled              bool
	HighWatermarkPercent float64
	LowWatermarkPercent  float64
	MinFreeBytes         int64
	MaxDeleteBytesPerRun int64
}

type hydrateCall struct {
	done    chan struct{}
	err     error
	waiters int
}

type sourceCacheEntry struct {
	base      string
	expiresAt time.Time
}

type manifestDescriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
}

type imageLocation struct {
	TenantID          string `json:"tenant_id"`
	AppID             string `json:"app_id"`
	ImageRef          string `json:"image_ref"`
	Digest            string `json:"digest"`
	SourceOperationID string `json:"source_operation_id"`
	NodeID            string `json:"node_id"`
	RuntimeID         string `json:"runtime_id"`
	ClusterNodeName   string `json:"cluster_node_name"`
	CacheEndpoint     string `json:"cache_endpoint"`
	Status            string `json:"status"`
}

type imageCacheBlobUploadState struct {
	Repo      string    `json:"repo"`
	UUID      string    `json:"uuid"`
	SizeBytes int64     `json:"size_bytes"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
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
	clusterNode := env("FUGUE_IMAGE_CACHE_CLUSTER_NODE_NAME", os.Getenv("NODE_NAME"))
	if clusterNode == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		discovered, err := discoverKubernetesPodNodeName(ctx)
		cancel()
		if err != nil {
			log.Printf("discover image cache cluster node failed: %v", err)
		} else {
			clusterNode = discovered
		}
	}
	cache := &imageCache{
		apiBase:         strings.TrimRight(env("FUGUE_API_BASE", os.Getenv("FUGUE_API_URL")), "/"),
		apiToken:        apiToken,
		reportPath:      reportPath,
		lookupPath:      lookupPath,
		registryBase:    trimRegistryBase(env("FUGUE_IMAGE_CACHE_REGISTRY_BASE", "registry.fugue.internal:5000")),
		localBase:       trimRegistryBase(env("FUGUE_IMAGE_CACHE_LOCAL_BASE", "127.0.0.1:5000")),
		upstreamBase:    trimRegistryBase(os.Getenv("FUGUE_IMAGE_CACHE_UPSTREAM_BASE")),
		cacheEndpoint:   strings.TrimRight(os.Getenv("FUGUE_IMAGE_CACHE_ENDPOINT"), "/"),
		clusterNode:     clusterNode,
		storeDir:        storeDir,
		manifestDir:     filepath.Join(storeDir, "_manifests"),
		pinStorePath:    env("FUGUE_IMAGE_CACHE_PIN_STORE", filepath.Join(filepath.Dir(storeDir), "pins.json")),
		managementToken: strings.TrimSpace(env("FUGUE_IMAGE_CACHE_MANAGEMENT_TOKEN", apiToken)),
		httpClient:      &http.Client{Timeout: 15 * time.Second},
		registry:        registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir))),
		hydrateTimeout:  envDuration("FUGUE_IMAGE_CACHE_HYDRATE_TIMEOUT", 30*time.Minute),
		hydrateSlots:    newSemaphore(envInt("FUGUE_IMAGE_CACHE_HYDRATE_CONCURRENCY", 1)),
		proxySlots:      newSemaphore(envInt("FUGUE_IMAGE_CACHE_PROXY_CONCURRENCY", 4)),
		copyJobs:        envInt("FUGUE_IMAGE_CACHE_COPY_JOBS", 1),
		sourceTTL:       envDuration("FUGUE_IMAGE_CACHE_SOURCE_TTL", 10*time.Minute),
		diskLimit: imageCacheDiskLimit{
			Enabled:              envBool("FUGUE_IMAGE_CACHE_DISK_LIMIT_ENABLED", true),
			HighWatermarkPercent: envFloat("FUGUE_IMAGE_CACHE_HIGH_WATERMARK_PERCENT", defaultImageCacheHighWatermarkPercent),
			LowWatermarkPercent:  envFloat("FUGUE_IMAGE_CACHE_LOW_WATERMARK_PERCENT", defaultImageCacheLowWatermarkPercent),
			MinFreeBytes:         envBytes("FUGUE_IMAGE_CACHE_MIN_FREE_BYTES", defaultImageCacheMinFreeBytes),
			MaxDeleteBytesPerRun: envBytes("FUGUE_IMAGE_CACHE_MAX_DELETE_BYTES_PER_RUN", defaultImageCacheMaxDeleteBytesPerRun),
		},
	}
	if cache.apiBase == "" || cache.apiToken == "" {
		log.Print("control-plane API credentials are not configured; cache will serve local registry storage only")
	}
	if cache.cacheEndpoint == "" {
		cache.cacheEndpoint = "http://" + cache.localBase
	}
	if err := cache.loadPersistedManifests(); err != nil {
		log.Printf("load persisted image cache manifests failed: %v", err)
	}
	log.Printf("fugue-image-cache listening on %s store=%s registry_base=%s local_base=%s endpoint=%s cluster_node=%s upstream=%s hydrate_concurrency=%d proxy_concurrency=%d copy_jobs=%d disk_limit_enabled=%t high_watermark=%.2f low_watermark=%.2f min_free_bytes=%d max_delete_bytes_per_run=%d", listenAddr, filepath.Clean(storeDir), cache.registryBase, cache.localBase, cache.cacheEndpoint, cache.clusterNode, cache.upstreamBase, cap(cache.hydrateSlots), cap(cache.proxySlots), cache.copyJobs, cache.diskLimit.Enabled, cache.diskLimit.HighWatermarkPercent, cache.diskLimit.LowWatermarkPercent, cache.diskLimit.MinFreeBytes, cache.diskLimit.MaxDeleteBytesPerRun)
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
	if strings.HasPrefix(path, "/fugue/cache/v1/") {
		c.serveManagement(w, r)
		return
	}
	if !isRegistryAPIPath(path) {
		http.NotFound(w, r)
		return
	}
	if !isRegistryPull(r) {
		c.serveRegistryWrite(w, r)
		return
	}
	rec := newDeferredNotFoundWriter(w)
	c.registry.ServeHTTP(rec, r)
	if !rec.notFound() {
		rec.flush()
		return
	}
	if isLocalOnlyRegistryPull(r) {
		rec.flush()
		return
	}
	repo, target, targetKind, ok := parseRegistryTarget(r.URL.Path)
	if !ok {
		rec.flush()
		return
	}
	if targetKind == registryTargetBlob && c.proxyBlobFromKnownSource(w, r, repo, target) {
		return
	}
	if targetKind == registryTargetBlob && c.proxyBlobFromUpstream(w, r, repo, target) {
		return
	}
	if targetKind == registryTargetBlob {
		rec.flush()
		return
	}
	if targetKind == registryTargetManifest && c.proxyManifestFromRemote(w, r, repo, target) {
		c.startHydrate(repo, target)
		return
	}
	if err := c.hydrate(r.Context(), repo, target); err != nil {
		log.Printf("hydrate repo=%s target=%s failed: %v", repo, target, err)
		rec.flush()
		return
	}
	c.registry.ServeHTTP(w, r)
}

type cachePin struct {
	ImageRef  string     `json:"image_ref"`
	Repo      string     `json:"repo"`
	Target    string     `json:"target"`
	Digest    string     `json:"digest"`
	Reason    string     `json:"reason"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

type cachePinsFile struct {
	Pins []cachePin `json:"pins"`
}

func (c *imageCache) serveManagement(w http.ResponseWriter, r *http.Request) {
	path := ""
	if r != nil && r.URL != nil {
		path = strings.TrimRight(r.URL.Path, "/")
	}
	if path == "/fugue/cache/v1/health" && r.Method == http.MethodGet {
		writeManagementJSON(w, http.StatusOK, map[string]any{
			"status":       "ok",
			"endpoint":     c.cacheEndpoint,
			"cluster_node": c.clusterNode,
		})
		return
	}
	if !c.authorizeManagement(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch {
	case path == "/fugue/cache/v1/inventory" && r.Method == http.MethodGet:
		c.handleManagementInventory(w, r)
	case path == "/fugue/cache/v1/verify" && r.Method == http.MethodPost:
		c.handleManagementVerify(w, r)
	case path == "/fugue/cache/v1/replicate" && r.Method == http.MethodPost:
		c.handleManagementReplicate(w, r)
	case path == "/fugue/cache/v1/pin" && r.Method == http.MethodPost:
		c.handleManagementPin(w, r)
	case path == "/fugue/cache/v1/unpin" && r.Method == http.MethodPost:
		c.handleManagementUnpin(w, r)
	case path == "/fugue/cache/v1/prune" && r.Method == http.MethodPost:
		c.handleManagementPrune(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (c *imageCache) authorizeManagement(r *http.Request) bool {
	if host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr)); err == nil {
		if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil && ip.IsLoopback() {
			return true
		}
	}
	token := strings.TrimSpace(c.managementToken)
	if token == "" {
		return false
	}
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(header), "bearer ") {
		return strings.TrimSpace(header[len("bearer "):]) == token
	}
	return false
}

func (c *imageCache) handleManagementInventory(w http.ResponseWriter, _ *http.Request) {
	manifests, err := c.managementManifestInventory()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pins, err := c.readPins()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	disk, err := c.imageCacheDiskStats(c.normalizedDiskLimit())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	unreferenced, err := c.managementUnreferencedBlobInventory()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeManagementJSON(w, http.StatusOK, map[string]any{
		"endpoint":           c.cacheEndpoint,
		"cluster_node":       c.clusterNode,
		"manifests":          manifests,
		"unreferenced_blobs": unreferenced,
		"pins":               pins.Pins,
		"disk":               disk,
	})
}

func (c *imageCache) handleManagementVerify(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ImageRef string `json:"image_ref"`
		Repo     string `json:"repo"`
		Target   string `json:"target"`
		Digest   string `json:"digest"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	repo, target := c.managementRepoTarget(req.ImageRef, req.Repo, req.Target, req.Digest)
	available := repo != "" && target != "" && c.localManifestAvailable(repo, target)
	status := http.StatusOK
	if !available {
		status = http.StatusNotFound
	}
	writeManagementJSON(w, status, map[string]any{
		"repo":      repo,
		"target":    target,
		"available": available,
	})
}

func (c *imageCache) handleManagementReplicate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ImageRef            string `json:"image_ref"`
		Repo                string `json:"repo"`
		Target              string `json:"target"`
		Digest              string `json:"digest"`
		SourceCacheEndpoint string `json:"source_cache_endpoint"`
		TaskID              string `json:"task_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	repo, target := c.managementRepoTarget(req.ImageRef, req.Repo, req.Target, req.Digest)
	if repo == "" || target == "" {
		http.Error(w, "repo and target are required", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	if source := trimRegistryBase(req.SourceCacheEndpoint); source != "" {
		peerRef, _ := imageRef(source, repo, target)
		localRef, _ := imageRef(c.localBase, repo, target)
		if err := c.copyImage(ctx, peerRef, localRef); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if err := c.ensureLocalManifest(ctx, source, repo, target); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	} else if err := c.hydrate(ctx, repo, target); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	logicalRef, digest := imageRef(c.registryBase, repo, target)
	if digest == "" {
		digest = strings.TrimSpace(req.Digest)
	}
	_ = c.report(ctx, logicalRef, digest, "present", "")
	writeManagementJSON(w, http.StatusOK, map[string]any{
		"repo":      repo,
		"target":    target,
		"task_id":   strings.TrimSpace(req.TaskID),
		"available": c.localManifestAvailable(repo, target),
	})
}

func (c *imageCache) handleManagementPin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ImageRef  string     `json:"image_ref"`
		Repo      string     `json:"repo"`
		Target    string     `json:"target"`
		Digest    string     `json:"digest"`
		Reason    string     `json:"reason"`
		ExpiresAt *time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	repo, target := c.managementRepoTarget(req.ImageRef, req.Repo, req.Target, req.Digest)
	if repo == "" || target == "" {
		http.Error(w, "repo and target are required", http.StatusBadRequest)
		return
	}
	pins, err := c.readPins()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pin := cachePin{
		ImageRef:  strings.TrimSpace(req.ImageRef),
		Repo:      repo,
		Target:    target,
		Digest:    strings.TrimSpace(req.Digest),
		Reason:    firstNonEmptyCacheString(req.Reason, "user_pin"),
		ExpiresAt: req.ExpiresAt,
		CreatedAt: time.Now().UTC(),
	}
	replaced := false
	for idx := range pins.Pins {
		if pins.Pins[idx].Repo == repo && pins.Pins[idx].Target == target && pins.Pins[idx].Reason == pin.Reason {
			pins.Pins[idx] = pin
			replaced = true
			break
		}
	}
	if !replaced {
		pins.Pins = append(pins.Pins, pin)
	}
	if err := c.writePins(pins); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeManagementJSON(w, http.StatusOK, map[string]any{"pin": pin})
}

func (c *imageCache) handleManagementUnpin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ImageRef string `json:"image_ref"`
		Repo     string `json:"repo"`
		Target   string `json:"target"`
		Digest   string `json:"digest"`
		Reason   string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	repo, target := c.managementRepoTarget(req.ImageRef, req.Repo, req.Target, req.Digest)
	pins, err := c.readPins()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reason := strings.TrimSpace(req.Reason)
	next := pins.Pins[:0]
	removed := 0
	for _, pin := range pins.Pins {
		matches := pin.Repo == repo && pin.Target == target && (reason == "" || pin.Reason == reason)
		if matches {
			removed++
			continue
		}
		next = append(next, pin)
	}
	pins.Pins = next
	if err := c.writePins(pins); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeManagementJSON(w, http.StatusOK, map[string]any{"removed": removed})
}

func (c *imageCache) handleManagementPrune(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DryRun                   *bool                   `json:"dry_run"`
		AllowDelete              bool                    `json:"allow_delete"`
		ImageRef                 string                  `json:"image_ref"`
		Repo                     string                  `json:"repo"`
		Target                   string                  `json:"target"`
		Digest                   string                  `json:"digest"`
		Targets                  []imageCachePruneTarget `json:"targets"`
		MaxDeleteBytes           string                  `json:"max_delete_bytes"`
		MinManifestAge           string                  `json:"min_manifest_age"`
		IncludeUnreferencedBlobs bool                    `json:"include_unreferenced_blobs"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	dryRun := true
	if req.DryRun != nil {
		dryRun = *req.DryRun
	}
	records, err := c.managementManifestRecords()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pins, err := c.readPins()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pinned := map[string]struct{}{}
	now := time.Now().UTC()
	for _, pin := range pins.Pins {
		if pin.ExpiresAt != nil && pin.ExpiresAt.Before(now) {
			continue
		}
		pinned[pin.Repo+"\x00"+pin.Target] = struct{}{}
	}
	repo, target := c.managementRepoTarget(req.ImageRef, req.Repo, req.Target, req.Digest)
	requestMaxDeleteBytes, err := parseImageCacheByteSize(req.MaxDeleteBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	minManifestAge, err := parseImageCacheDuration(req.MinManifestAge)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	targets := append([]imageCachePruneTarget(nil), req.Targets...)
	if repo != "" && (target != "" || strings.TrimSpace(req.Digest) != "") {
		targets = append(targets, imageCachePruneTarget{
			ImageRef: strings.TrimSpace(req.ImageRef),
			Repo:     repo,
			Target:   target,
			Digest:   normalizeImageCacheDigest(req.Digest),
		})
	}
	plan, err := c.planImageCachePrune(imageCachePruneRequest{
		repo:                     repo,
		target:                   target,
		digest:                   normalizeImageCacheDigest(req.Digest),
		targets:                  targets,
		allowDelete:              req.AllowDelete,
		requestMaxBytes:          requestMaxDeleteBytes,
		minManifestAge:           minManifestAge,
		includeUnreferencedBlobs: req.IncludeUnreferencedBlobs,
	}, records, pinned)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !dryRun && req.AllowDelete {
		if err := c.executeImageCachePrunePlan(&plan); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	writeManagementJSON(w, http.StatusOK, map[string]any{
		"dry_run":              dryRun,
		"allow_delete":         req.AllowDelete,
		"deleted":              !dryRun && req.AllowDelete && plan.Deleted,
		"planned_delete_bytes": plan.PlannedDeleteBytes,
		"deleted_bytes":        plan.DeletedBytes,
		"deleted_manifests":    plan.DeletedManifests,
		"deleted_blobs":        plan.DeletedBlobs,
		"candidate_count":      len(plan.Candidates),
		"selected_count":       len(plan.SelectedManifests),
		"candidates":           plan.Candidates,
		"selected_manifests":   plan.SelectedManifests,
		"unreferenced_blobs":   plan.UnreferencedBlobs,
		"skipped_manifests":    plan.SkippedManifests,
		"delete_budget_bytes":  plan.DeleteBudgetBytes,
		"needed_delete_bytes":  plan.NeededDeleteBytes,
		"budget_exhausted":     plan.BudgetExhausted,
		"skipped_reason":       plan.SkippedReason,
		"disk":                 plan.Disk,
		"target_repo":          repo,
		"target":               target,
		"target_digest":        normalizeImageCacheDigest(req.Digest),
	})
}

type imageCachePruneTarget struct {
	ImageRef string `json:"image_ref"`
	Repo     string `json:"repo"`
	Target   string `json:"target"`
	Digest   string `json:"digest"`
}

type imageCachePruneRequest struct {
	repo                     string
	target                   string
	digest                   string
	targets                  []imageCachePruneTarget
	allowDelete              bool
	requestMaxBytes          int64
	minManifestAge           time.Duration
	includeUnreferencedBlobs bool
}

type imageCachePrunePlan struct {
	Disk               imageCacheDiskStats       `json:"disk"`
	Candidates         []imageCacheManifestEntry `json:"candidates"`
	SelectedManifests  []imageCacheManifestEntry `json:"selected_manifests"`
	SkippedManifests   []imageCacheManifestEntry `json:"skipped_manifests"`
	UnreferencedBlobs  []imageCacheBlobEntry     `json:"unreferenced_blobs"`
	DeleteBudgetBytes  int64                     `json:"delete_budget_bytes"`
	NeededDeleteBytes  int64                     `json:"needed_delete_bytes"`
	PlannedDeleteBytes int64                     `json:"planned_delete_bytes"`
	DeletedBytes       int64                     `json:"deleted_bytes"`
	Deleted            bool                      `json:"deleted"`
	BudgetExhausted    bool                      `json:"budget_exhausted"`
	DeletedManifests   []imageCacheManifestEntry `json:"deleted_manifests"`
	DeletedBlobs       []imageCacheBlobEntry     `json:"deleted_blobs"`
	SkippedReason      string                    `json:"skipped_reason,omitempty"`

	deleteManifests []imageCacheManifestRecord
	deleteBlobs     []imageCacheBlobRecord
}

type imageCacheDiskStats struct {
	Enabled              bool    `json:"enabled"`
	TotalBytes           int64   `json:"total_bytes"`
	UsedBytes            int64   `json:"used_bytes"`
	FreeBytes            int64   `json:"free_bytes"`
	UsedPercent          float64 `json:"used_percent"`
	CacheBytes           int64   `json:"cache_bytes"`
	HighWatermarkPercent float64 `json:"high_watermark_percent"`
	LowWatermarkPercent  float64 `json:"low_watermark_percent"`
	MinFreeBytes         int64   `json:"min_free_bytes"`
	MaxDeleteBytesPerRun int64   `json:"max_delete_bytes_per_run"`
	OverHighWatermark    bool    `json:"over_high_watermark"`
	BelowMinFree         bool    `json:"below_min_free"`
	NeededDeleteBytes    int64   `json:"needed_delete_bytes"`
}

type imageCacheManifestRecord struct {
	Repo            string
	Target          string
	Digest          string
	ContentType     string
	Path            string
	SizeBytes       int64
	ModifiedAt      time.Time
	ReferencedBlobs []string
}

type imageCacheManifestEntry struct {
	Repo                string   `json:"repo"`
	Target              string   `json:"target"`
	Digest              string   `json:"digest"`
	ContentType         string   `json:"content_type"`
	SizeBytes           int64    `json:"size_bytes"`
	ReferencedBlobs     []string `json:"referenced_blobs,omitempty"`
	ReferencedBlobBytes int64    `json:"referenced_blob_bytes"`
	ModifiedAt          string   `json:"modified_at,omitempty"`
}

type imageCacheBlobRecord struct {
	Digest     string
	Path       string
	SizeBytes  int64
	ModifiedAt time.Time
}

type imageCacheBlobEntry struct {
	Digest     string `json:"digest"`
	SizeBytes  int64  `json:"size_bytes"`
	ModifiedAt string `json:"modified_at,omitempty"`
}

func (c *imageCache) planImageCachePrune(req imageCachePruneRequest, records []imageCacheManifestRecord, pinned map[string]struct{}) (imageCachePrunePlan, error) {
	limit := c.normalizedDiskLimit()
	disk, err := c.imageCacheDiskStats(limit)
	if err != nil {
		return imageCachePrunePlan{}, err
	}
	blobs, err := c.imageCacheBlobRecords()
	if err != nil {
		return imageCachePrunePlan{}, err
	}
	blobByDigest := make(map[string]imageCacheBlobRecord, len(blobs))
	for _, blob := range blobs {
		blobByDigest[blob.Digest] = blob
	}
	candidates := make([]imageCacheManifestRecord, 0, len(records))
	skipped := make([]imageCacheManifestRecord, 0)
	now := time.Now().UTC()
	for _, record := range records {
		if _, ok := pinned[record.Repo+"\x00"+record.Target]; ok {
			skipped = append(skipped, record)
			continue
		}
		if req.minManifestAge > 0 && !record.ModifiedAt.IsZero() && now.Sub(record.ModifiedAt) < req.minManifestAge {
			skipped = append(skipped, record)
			continue
		}
		candidates = append(candidates, record)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].ModifiedAt.Before(candidates[j].ModifiedAt)
	})

	targets := normalizeImageCachePruneTargets(req)
	targeted := len(targets) > 0
	selected := []imageCacheManifestRecord{}
	if targeted {
		selectedDigestByRepo := map[string]map[string]struct{}{}
		for _, candidate := range candidates {
			if !manifestMatchesAnyPruneTarget(candidate, targets) {
				continue
			}
			if selectedDigestByRepo[candidate.Repo] == nil {
				selectedDigestByRepo[candidate.Repo] = map[string]struct{}{}
			}
			selectedDigestByRepo[candidate.Repo][candidate.Digest] = struct{}{}
		}
		for _, candidate := range candidates {
			if digests := selectedDigestByRepo[candidate.Repo]; digests != nil {
				if _, ok := digests[candidate.Digest]; ok {
					selected = append(selected, candidate)
				}
			}
		}
	}

	selectedKeys := make(map[string]struct{}, len(selected))
	for _, record := range selected {
		selectedKeys[record.Repo+"\x00"+record.Target] = struct{}{}
	}
	remainingReferenced := map[string]struct{}{}
	for _, record := range records {
		if _, ok := selectedKeys[record.Repo+"\x00"+record.Target]; ok {
			continue
		}
		for _, digest := range record.ReferencedBlobs {
			remainingReferenced[digest] = struct{}{}
		}
	}
	unreferenced := make([]imageCacheBlobRecord, 0, len(blobs))
	for _, blob := range blobs {
		if _, ok := remainingReferenced[blob.Digest]; ok {
			continue
		}
		unreferenced = append(unreferenced, blob)
	}
	sort.SliceStable(unreferenced, func(i, j int) bool {
		if unreferenced[i].ModifiedAt.Equal(unreferenced[j].ModifiedAt) {
			return unreferenced[i].SizeBytes > unreferenced[j].SizeBytes
		}
		return unreferenced[i].ModifiedAt.Before(unreferenced[j].ModifiedAt)
	})

	budget := limit.MaxDeleteBytesPerRun
	if req.requestMaxBytes > 0 && (budget <= 0 || req.requestMaxBytes < budget) {
		budget = req.requestMaxBytes
	}
	plan := imageCachePrunePlan{
		Disk:              disk,
		Candidates:        manifestEntries(candidates, blobByDigest),
		SelectedManifests: manifestEntries(selected, blobByDigest),
		SkippedManifests:  manifestEntries(skipped, blobByDigest),
		UnreferencedBlobs: blobEntries(unreferenced),
		DeleteBudgetBytes: budget,
		NeededDeleteBytes: disk.NeededDeleteBytes,
	}
	if budget <= 0 {
		plan.SkippedReason = "delete_budget_zero"
		return plan, nil
	}
	if targeted && len(selected) == 0 && (!req.includeUnreferencedBlobs || len(unreferenced) == 0) {
		plan.SkippedReason = "no_matching_unpinned_manifest_or_unreferenced_blob"
		return plan, nil
	}
	if !targeted && !limit.Enabled {
		plan.SkippedReason = "disk_limit_disabled"
		return plan, nil
	}
	if !targeted && disk.NeededDeleteBytes <= 0 && !req.includeUnreferencedBlobs {
		plan.SkippedReason = "below_watermark"
		return plan, nil
	}
	planned := int64(0)
	if targeted {
		for _, record := range selected {
			if planned+record.SizeBytes > budget {
				plan.BudgetExhausted = true
				continue
			}
			plan.deleteManifests = append(plan.deleteManifests, record)
			planned += record.SizeBytes
		}
	}
	blobBudget := budget - planned
	if blobBudget < 0 {
		blobBudget = 0
	}
	blobNeed := disk.NeededDeleteBytes - planned
	if blobNeed < 0 {
		blobNeed = 0
	}
	if targeted || req.includeUnreferencedBlobs {
		blobNeed = blobBudget
	}
	for _, blob := range unreferenced {
		if blobBudget <= 0 || blobNeed <= 0 {
			if blobNeed > 0 {
				plan.BudgetExhausted = true
			}
			break
		}
		if blob.SizeBytes > blobBudget {
			plan.BudgetExhausted = true
			continue
		}
		plan.deleteBlobs = append(plan.deleteBlobs, blob)
		planned += blob.SizeBytes
		blobBudget -= blob.SizeBytes
		blobNeed -= blob.SizeBytes
	}
	plan.PlannedDeleteBytes = planned
	if !req.allowDelete {
		plan.SkippedReason = "allow_delete_false"
	}
	if len(plan.deleteManifests) == 0 && len(plan.deleteBlobs) == 0 {
		plan.SkippedReason = "no_deletable_candidate_within_budget"
	}
	return plan, nil
}

func (c *imageCache) executeImageCachePrunePlan(plan *imageCachePrunePlan) error {
	if plan == nil {
		return nil
	}
	stillReferenced := map[string]struct{}{}
	for _, record := range plan.deleteManifests {
		if err := c.deleteLocalManifest(record.Repo, record.Target); err != nil {
			return err
		}
		plan.Deleted = true
		plan.DeletedBytes += record.SizeBytes
		plan.DeletedManifests = append(plan.DeletedManifests, manifestEntry(record, nil))
		for _, digest := range c.localManifestReferencedBlobDigests(record.Repo, record.Digest) {
			stillReferenced[digest] = struct{}{}
		}
	}
	for _, blob := range plan.deleteBlobs {
		if _, ok := stillReferenced[blob.Digest]; ok {
			continue
		}
		if err := os.Remove(blob.Path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("delete blob %s: %w", blob.Digest, err)
		}
		plan.Deleted = true
		plan.DeletedBytes += blob.SizeBytes
		plan.DeletedBlobs = append(plan.DeletedBlobs, blobEntry(blob))
	}
	return nil
}

func (c *imageCache) deleteLocalManifest(repo, target string) error {
	if c != nil && c.registry != nil {
		req := httptestRequest(http.MethodDelete, "/v2/"+strings.Trim(strings.TrimSpace(repo), "/")+"/manifests/"+strings.TrimSpace(target), "", nil)
		rec := &memoryResponseWriter{header: http.Header{}}
		c.registry.ServeHTTP(rec, req)
		status := rec.statusCode()
		if status < 200 || status >= 300 {
			body := strings.TrimSpace(rec.body.String())
			if status != http.StatusNotFound {
				return fmt.Errorf("delete registry manifest repo=%s target=%s status=%d body=%s", repo, target, status, body)
			}
		}
	}
	return c.deletePersistedManifest(repo, target)
}

func (c *imageCache) localManifestReferencedBlobDigests(repo, target string) []string {
	if c == nil || c.registry == nil {
		return nil
	}
	target = normalizeImageCacheDigest(target)
	if target == "" {
		return nil
	}
	path := "/v2/" + strings.Trim(strings.TrimSpace(repo), "/") + "/manifests/" + target
	req := httptestRequest(http.MethodGet, path, "", nil)
	rec := &memoryResponseWriter{header: http.Header{}}
	c.registry.ServeHTTP(rec, req)
	if rec.statusCode() < 200 || rec.statusCode() >= 300 || rec.body.Len() == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := []string{}
	for _, descriptor := range manifestReferencedTargets(rec.body.Bytes()) {
		if descriptor.kind != registryTargetBlob {
			continue
		}
		digest := normalizeImageCacheDigest(descriptor.target)
		if digest == "" {
			continue
		}
		if _, ok := seen[digest]; ok {
			continue
		}
		seen[digest] = struct{}{}
		out = append(out, digest)
	}
	return out
}

func manifestMatchesPruneTarget(record imageCacheManifestRecord, req imageCachePruneRequest) bool {
	if strings.TrimSpace(record.Repo) != strings.Trim(strings.TrimSpace(req.repo), "/") {
		return false
	}
	target := strings.TrimSpace(req.target)
	digest := normalizeImageCacheDigest(req.digest)
	switch {
	case target != "" && record.Target == target:
		return true
	case target != "" && record.Digest == normalizeImageCacheDigest(target):
		return true
	case digest != "" && record.Target == digest:
		return true
	case digest != "" && record.Digest == digest:
		return true
	default:
		return false
	}
}

func normalizeImageCachePruneTargets(req imageCachePruneRequest) []imageCachePruneTarget {
	out := make([]imageCachePruneTarget, 0, len(req.targets)+1)
	appendTarget := func(target imageCachePruneTarget) {
		repo, parsedTarget := "", ""
		if target.Repo != "" {
			repo, parsedTarget = strings.Trim(strings.TrimSpace(target.Repo), "/"), strings.TrimSpace(target.Target)
			if parsedTarget == "" && strings.TrimSpace(target.Digest) != "" {
				parsedTarget = strings.TrimSpace(target.Digest)
			}
		}
		if repo == "" || parsedTarget == "" {
			repo, parsedTarget = parseImageCacheRepoTarget(target.ImageRef, target.Repo, target.Target, target.Digest)
		}
		digest := normalizeImageCacheDigest(target.Digest)
		if repo == "" || (parsedTarget == "" && digest == "") {
			return
		}
		out = append(out, imageCachePruneTarget{
			ImageRef: strings.TrimSpace(target.ImageRef),
			Repo:     repo,
			Target:   parsedTarget,
			Digest:   digest,
		})
	}
	for _, target := range req.targets {
		appendTarget(target)
	}
	if strings.TrimSpace(req.repo) != "" && (strings.TrimSpace(req.target) != "" || strings.TrimSpace(req.digest) != "") {
		appendTarget(imageCachePruneTarget{Repo: req.repo, Target: req.target, Digest: req.digest})
	}
	return out
}

func parseImageCacheRepoTarget(imageRefValue, repoValue, targetValue, digestValue string) (string, string) {
	repo := strings.Trim(strings.TrimSpace(repoValue), "/")
	target := strings.TrimSpace(targetValue)
	if repo != "" && target != "" {
		return repo, target
	}
	if repo != "" && strings.TrimSpace(digestValue) != "" {
		return repo, strings.TrimSpace(digestValue)
	}
	ref := trimRegistryBase(imageRefValue)
	if strings.Contains(ref, "@") {
		parts := strings.SplitN(ref, "@", 2)
		return strings.Trim(parts[0], "/"), strings.TrimSpace(parts[1])
	}
	if strings.Contains(ref, ":") {
		idx := strings.LastIndex(ref, ":")
		if idx > 0 && idx+1 < len(ref) {
			return strings.Trim(ref[:idx], "/"), strings.TrimSpace(ref[idx+1:])
		}
	}
	if repo != "" {
		return repo, firstNonEmptyCacheString(target, digestValue, "latest")
	}
	return strings.Trim(ref, "/"), firstNonEmptyCacheString(target, digestValue, "latest")
}

func manifestMatchesAnyPruneTarget(record imageCacheManifestRecord, targets []imageCachePruneTarget) bool {
	for _, target := range targets {
		req := imageCachePruneRequest{
			repo:   target.Repo,
			target: target.Target,
			digest: target.Digest,
		}
		if manifestMatchesPruneTarget(record, req) {
			return true
		}
	}
	return false
}

func manifestEntries(records []imageCacheManifestRecord, blobByDigest map[string]imageCacheBlobRecord) []imageCacheManifestEntry {
	out := make([]imageCacheManifestEntry, 0, len(records))
	for _, record := range records {
		out = append(out, manifestEntry(record, blobByDigest))
	}
	return out
}

func manifestEntry(record imageCacheManifestRecord, blobByDigest map[string]imageCacheBlobRecord) imageCacheManifestEntry {
	referencedBytes := int64(0)
	if blobByDigest != nil {
		seen := map[string]struct{}{}
		for _, digest := range record.ReferencedBlobs {
			if _, ok := seen[digest]; ok {
				continue
			}
			seen[digest] = struct{}{}
			referencedBytes += blobByDigest[digest].SizeBytes
		}
	}
	modifiedAt := ""
	if !record.ModifiedAt.IsZero() {
		modifiedAt = record.ModifiedAt.UTC().Format(time.RFC3339)
	}
	return imageCacheManifestEntry{
		Repo:                record.Repo,
		Target:              record.Target,
		Digest:              record.Digest,
		ContentType:         record.ContentType,
		SizeBytes:           record.SizeBytes,
		ReferencedBlobs:     append([]string(nil), record.ReferencedBlobs...),
		ReferencedBlobBytes: referencedBytes,
		ModifiedAt:          modifiedAt,
	}
}

func blobEntries(records []imageCacheBlobRecord) []imageCacheBlobEntry {
	out := make([]imageCacheBlobEntry, 0, len(records))
	for _, record := range records {
		out = append(out, blobEntry(record))
	}
	return out
}

func blobEntry(record imageCacheBlobRecord) imageCacheBlobEntry {
	modifiedAt := ""
	if !record.ModifiedAt.IsZero() {
		modifiedAt = record.ModifiedAt.UTC().Format(time.RFC3339)
	}
	return imageCacheBlobEntry{Digest: record.Digest, SizeBytes: record.SizeBytes, ModifiedAt: modifiedAt}
}

func (c *imageCache) managementManifestInventory() ([]map[string]any, error) {
	records, err := c.managementManifestRecords()
	if err != nil {
		return nil, err
	}
	blobByDigest := map[string]imageCacheBlobRecord{}
	if blobs, err := c.imageCacheBlobRecords(); err == nil {
		for _, blob := range blobs {
			blobByDigest[blob.Digest] = blob
		}
	}
	out := []map[string]any{}
	for _, record := range records {
		// Persisted metadata is only a restart journal. A manifest list/index can
		// remain on disk after one of its child manifests has disappeared, in
		// which case replay rejects it and the local registry cannot serve it.
		// Inventory is consumed as an availability signal, so only report records
		// that the in-memory registry can actually serve.
		if !c.localManifestAvailable(record.Repo, record.Target) {
			continue
		}
		entry := manifestEntry(record, blobByDigest)
		out = append(out, map[string]any{
			"repo":                  entry.Repo,
			"target":                entry.Target,
			"digest":                entry.Digest,
			"content_type":          entry.ContentType,
			"size_bytes":            entry.SizeBytes,
			"referenced_blobs":      entry.ReferencedBlobs,
			"referenced_blob_bytes": entry.ReferencedBlobBytes,
			"modified_at":           entry.ModifiedAt,
		})
	}
	return out, nil
}

func (c *imageCache) managementUnreferencedBlobInventory() ([]imageCacheBlobEntry, error) {
	records, err := c.managementManifestRecords()
	if err != nil {
		return nil, err
	}
	blobs, err := c.imageCacheBlobRecords()
	if err != nil {
		return nil, err
	}
	referenced := map[string]struct{}{}
	for _, record := range records {
		for _, digest := range record.ReferencedBlobs {
			if digest != "" {
				referenced[digest] = struct{}{}
			}
		}
	}
	unreferenced := make([]imageCacheBlobRecord, 0, len(blobs))
	for _, blob := range blobs {
		if _, ok := referenced[blob.Digest]; ok {
			continue
		}
		unreferenced = append(unreferenced, blob)
	}
	sort.SliceStable(unreferenced, func(i, j int) bool {
		if unreferenced[i].ModifiedAt.Equal(unreferenced[j].ModifiedAt) {
			return unreferenced[i].SizeBytes > unreferenced[j].SizeBytes
		}
		return unreferenced[i].ModifiedAt.Before(unreferenced[j].ModifiedAt)
	})
	return blobEntries(unreferenced), nil
}

func (c *imageCache) managementManifestRecords() ([]imageCacheManifestRecord, error) {
	entries, err := os.ReadDir(c.manifestDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := []imageCacheManifestRecord{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(c.manifestDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var manifest persistedManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return nil, err
		}
		referenced := []string{}
		seenReferenced := map[string]struct{}{}
		for _, descriptor := range manifestReferencedTargets(manifest.Body) {
			if descriptor.kind != registryTargetBlob {
				continue
			}
			digest := normalizeImageCacheDigest(descriptor.target)
			if digest == "" {
				continue
			}
			if _, ok := seenReferenced[digest]; ok {
				continue
			}
			seenReferenced[digest] = struct{}{}
			referenced = append(referenced, digest)
		}
		sort.Strings(referenced)
		out = append(out, imageCacheManifestRecord{
			Repo:            strings.Trim(strings.TrimSpace(manifest.Repo), "/"),
			Target:          strings.TrimSpace(manifest.Target),
			Digest:          normalizeImageCacheDigest(manifestBodyDigest(manifest.Body)),
			ContentType:     strings.TrimSpace(manifest.ContentType),
			Path:            path,
			SizeBytes:       info.Size(),
			ModifiedAt:      info.ModTime(),
			ReferencedBlobs: referenced,
		})
	}
	return out, nil
}

func (c *imageCache) normalizedDiskLimit() imageCacheDiskLimit {
	limit := c.diskLimit
	if limit.HighWatermarkPercent <= 0 {
		limit.HighWatermarkPercent = defaultImageCacheHighWatermarkPercent
	}
	if limit.HighWatermarkPercent > 100 {
		limit.HighWatermarkPercent = 100
	}
	if limit.LowWatermarkPercent <= 0 {
		limit.LowWatermarkPercent = defaultImageCacheLowWatermarkPercent
	}
	if limit.LowWatermarkPercent > 100 {
		limit.LowWatermarkPercent = 100
	}
	if limit.LowWatermarkPercent > limit.HighWatermarkPercent {
		limit.LowWatermarkPercent = limit.HighWatermarkPercent
	}
	if limit.MinFreeBytes < 0 {
		limit.MinFreeBytes = 0
	}
	if limit.MaxDeleteBytesPerRun <= 0 {
		limit.MaxDeleteBytesPerRun = defaultImageCacheMaxDeleteBytesPerRun
	}
	return limit
}

func (c *imageCache) imageCacheDiskStats(limit imageCacheDiskLimit) (imageCacheDiskStats, error) {
	total, used, free, err := filesystemUsage(c.storeDir)
	if err != nil {
		return imageCacheDiskStats{}, err
	}
	cacheBytes, err := directorySize(c.storeDir)
	if err != nil {
		return imageCacheDiskStats{}, err
	}
	usedPercent := 0.0
	if total > 0 {
		usedPercent = (float64(used) / float64(total)) * 100
	}
	limit.MinFreeBytes = effectiveImageCacheMinFreeBytes(limit, total)
	overHigh := limit.HighWatermarkPercent > 0 && usedPercent >= limit.HighWatermarkPercent
	belowFree := limit.MinFreeBytes > 0 && free < limit.MinFreeBytes
	neededForWatermark := int64(0)
	if overHigh && total > 0 {
		targetUsed := int64(float64(total) * (limit.LowWatermarkPercent / 100))
		if used > targetUsed {
			neededForWatermark = used - targetUsed
		}
	}
	neededForReserve := int64(0)
	if belowFree {
		neededForReserve = limit.MinFreeBytes - free
	}
	needed := neededForWatermark
	if neededForReserve > needed {
		needed = neededForReserve
	}
	return imageCacheDiskStats{
		Enabled:              limit.Enabled,
		TotalBytes:           total,
		UsedBytes:            used,
		FreeBytes:            free,
		UsedPercent:          usedPercent,
		CacheBytes:           cacheBytes,
		HighWatermarkPercent: limit.HighWatermarkPercent,
		LowWatermarkPercent:  limit.LowWatermarkPercent,
		MinFreeBytes:         limit.MinFreeBytes,
		MaxDeleteBytesPerRun: limit.MaxDeleteBytesPerRun,
		OverHighWatermark:    overHigh,
		BelowMinFree:         belowFree,
		NeededDeleteBytes:    needed,
	}, nil
}

func effectiveImageCacheMinFreeBytes(limit imageCacheDiskLimit, totalBytes int64) int64 {
	minFreeBytes := limit.MinFreeBytes
	if minFreeBytes <= 0 || totalBytes <= 0 {
		return minFreeBytes
	}
	maxReachableFreeBytes := totalBytes
	if limit.LowWatermarkPercent > 0 {
		targetUsedBytes := int64(float64(totalBytes) * (limit.LowWatermarkPercent / 100))
		maxReachableFreeBytes = totalBytes - targetUsedBytes
		if maxReachableFreeBytes < 0 {
			maxReachableFreeBytes = 0
		}
	}
	if minFreeBytes > maxReachableFreeBytes {
		return maxReachableFreeBytes
	}
	return minFreeBytes
}

func filesystemUsage(path string) (totalBytes, usedBytes, freeBytes int64, err error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" || path == "." {
		path = "."
	}
	statPath := path
	for {
		var stats syscall.Statfs_t
		if err := syscall.Statfs(statPath, &stats); err == nil {
			blockSize := int64(stats.Bsize)
			total := int64(stats.Blocks) * blockSize
			used := int64(stats.Blocks-stats.Bfree) * blockSize
			free := int64(stats.Bavail) * blockSize
			return total, used, free, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return 0, 0, 0, fmt.Errorf("stat image-cache filesystem %s: %w", statPath, err)
		}
		parent := filepath.Dir(statPath)
		if parent == statPath || parent == "." {
			return 0, 0, 0, fmt.Errorf("stat image-cache filesystem %s: %w", path, os.ErrNotExist)
		}
		statPath = parent
	}
}

func directorySize(root string) (int64, error) {
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "" || root == "." {
		return 0, nil
	}
	total := int64(0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		_ = path
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	return total, err
}

func (c *imageCache) imageCacheBlobRecords() ([]imageCacheBlobRecord, error) {
	storeDir := filepath.Clean(strings.TrimSpace(c.storeDir))
	if storeDir == "" || storeDir == "." {
		return nil, nil
	}
	entries, err := os.ReadDir(storeDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := []imageCacheBlobRecord{}
	for _, algorithmEntry := range entries {
		if !algorithmEntry.IsDir() {
			continue
		}
		algorithm := strings.TrimSpace(algorithmEntry.Name())
		if strings.HasPrefix(algorithm, "_") || algorithm == "" {
			continue
		}
		algorithmDir := filepath.Join(storeDir, algorithm)
		blobEntries, err := os.ReadDir(algorithmDir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		for _, blobEntry := range blobEntries {
			if blobEntry.IsDir() {
				continue
			}
			digest := normalizeImageCacheDigest(algorithm + ":" + blobEntry.Name())
			if digest == "" {
				continue
			}
			info, err := blobEntry.Info()
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return nil, err
			}
			if !info.Mode().IsRegular() {
				continue
			}
			out = append(out, imageCacheBlobRecord{
				Digest:     digest,
				Path:       filepath.Join(algorithmDir, blobEntry.Name()),
				SizeBytes:  info.Size(),
				ModifiedAt: info.ModTime(),
			})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Digest < out[j].Digest
	})
	return out, nil
}

func normalizeImageCacheDigest(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return ""
	}
	for _, r := range parts[1] {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return ""
		}
	}
	return strings.TrimSpace(parts[0]) + ":" + strings.TrimSpace(parts[1])
}

func (c *imageCache) managementRepoTarget(imageRefValue, repoValue, targetValue, digestValue string) (string, string) {
	repo := strings.Trim(strings.TrimSpace(repoValue), "/")
	target := strings.TrimSpace(targetValue)
	if repo != "" && target != "" {
		return repo, target
	}
	if repo != "" && strings.TrimSpace(digestValue) != "" {
		return repo, strings.TrimSpace(digestValue)
	}
	ref := trimRegistryBase(imageRefValue)
	if c.refStartsWithKnownRegistryBase(ref) {
		if idx := strings.Index(ref, "/"); idx >= 0 && idx+1 < len(ref) {
			ref = ref[idx+1:]
		}
	}
	if strings.Contains(ref, "@") {
		parts := strings.SplitN(ref, "@", 2)
		return strings.Trim(parts[0], "/"), strings.TrimSpace(parts[1])
	}
	if strings.Contains(ref, ":") {
		idx := strings.LastIndex(ref, ":")
		if idx > 0 && idx+1 < len(ref) {
			return strings.Trim(ref[:idx], "/"), strings.TrimSpace(ref[idx+1:])
		}
	}
	if repo != "" {
		return repo, firstNonEmptyCacheString(target, digestValue, "latest")
	}
	return strings.Trim(ref, "/"), firstNonEmptyCacheString(target, digestValue, "latest")
}

func (c *imageCache) refStartsWithKnownRegistryBase(ref string) bool {
	ref = trimRegistryBase(ref)
	if ref == "" {
		return false
	}
	for _, base := range []string{c.registryBase, c.localBase, c.upstreamBase} {
		base = trimRegistryBase(base)
		if base != "" && (ref == base || strings.HasPrefix(ref, base+"/")) {
			return true
		}
	}
	if idx := strings.Index(ref, "/"); idx > 0 {
		host := ref[:idx]
		return strings.Contains(host, ".") || strings.Contains(host, ":") || host == "localhost"
	}
	return false
}

func (c *imageCache) readPins() (cachePinsFile, error) {
	var pins cachePinsFile
	if c == nil || strings.TrimSpace(c.pinStorePath) == "" {
		return pins, nil
	}
	raw, err := os.ReadFile(c.pinStorePath)
	if errors.Is(err, os.ErrNotExist) {
		return pins, nil
	}
	if err != nil {
		return pins, err
	}
	if len(raw) == 0 {
		return pins, nil
	}
	if err := json.Unmarshal(raw, &pins); err != nil {
		return pins, err
	}
	return pins, nil
}

func (c *imageCache) writePins(pins cachePinsFile) error {
	if c == nil || strings.TrimSpace(c.pinStorePath) == "" {
		return nil
	}
	dir := filepath.Dir(c.pinStorePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "pins-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, c.pinStorePath)
}

func writeManagementJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func firstNonEmptyCacheString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (c *imageCache) serveRegistryWrite(w http.ResponseWriter, r *http.Request) {
	if c.serveBlobUpload(w, r) {
		return
	}

	var manifestBody []byte
	var manifestRepo, manifestTarget, manifestContentType string
	if r != nil && r.URL != nil && (r.Method == http.MethodPut || r.Method == http.MethodDelete) {
		repo, target, targetKind, ok := parseRegistryTarget(r.URL.Path)
		if ok && targetKind == registryTargetManifest {
			if r.Method == http.MethodPut {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, fmt.Sprintf("read manifest body: %v", err), http.StatusInternalServerError)
					return
				}
				_ = r.Body.Close()
				r.Body = io.NopCloser(bytes.NewReader(body))
				manifestBody = body
				manifestContentType = r.Header.Get("Content-Type")
			}
			manifestRepo = repo
			manifestTarget = target
		}
	}

	rec := &statusRecordingWriter{ResponseWriter: w}
	c.registry.ServeHTTP(rec, r)
	status := rec.statusCode()
	if status >= 200 && status < 300 && len(manifestBody) > 0 {
		for _, target := range manifestPersistTargets(manifestTarget, manifestBody) {
			if target != manifestTarget {
				manifest := persistedManifest{
					Repo:        strings.Trim(strings.TrimSpace(manifestRepo), "/"),
					Target:      target,
					ContentType: manifestContentType,
					Body:        manifestBody,
				}
				if err := c.replayManifest(manifest); err != nil {
					log.Printf("replay manifest alias repo=%s target=%s failed: %v", manifestRepo, target, err)
				}
			}
			if err := c.persistManifest(manifestRepo, target, manifestContentType, manifestBody); err != nil {
				log.Printf("persist manifest repo=%s target=%s failed: %v", manifestRepo, target, err)
			}
		}
	} else if status >= 200 && status < 300 && r.Method == http.MethodDelete && manifestRepo != "" && manifestTarget != "" {
		if err := c.deletePersistedManifest(manifestRepo, manifestTarget); err != nil {
			log.Printf("delete persisted manifest repo=%s target=%s failed: %v", manifestRepo, manifestTarget, err)
		}
	}
	c.reportRegistryWrite(r, status, manifestBody)
}

func (c *imageCache) serveBlobUpload(w http.ResponseWriter, r *http.Request) bool {
	if c == nil || r == nil || r.URL == nil {
		return false
	}
	repo, uploadID, ok := parseImageCacheBlobUploadPath(r.URL.Path)
	if !ok {
		return false
	}
	switch r.Method {
	case http.MethodPost:
		if uploadID != "" {
			return false
		}
		state, err := c.createBlobUpload(repo, r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("create blob upload: %v", err), http.StatusInternalServerError)
			return true
		}
		writeImageCacheUploadAccepted(w, repo, state.UUID, state.SizeBytes)
		return true
	case http.MethodPatch:
		uploadID = cleanImageCacheUploadID(uploadID)
		if uploadID == "" {
			http.Error(w, "invalid blob upload id", http.StatusBadRequest)
			return true
		}
		state, err := c.appendBlobUpload(repo, uploadID, r.Body)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				http.Error(w, "blob upload not found", http.StatusNotFound)
				return true
			}
			http.Error(w, fmt.Sprintf("append blob upload: %v", err), http.StatusInternalServerError)
			return true
		}
		writeImageCacheUploadAccepted(w, repo, state.UUID, state.SizeBytes)
		return true
	case http.MethodPut:
		uploadID = cleanImageCacheUploadID(uploadID)
		if uploadID == "" {
			http.Error(w, "invalid blob upload id", http.StatusBadRequest)
			return true
		}
		digest, err := validateImageCacheBlobDigest(r.URL.Query().Get("digest"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return true
		}
		if err := c.completeBlobUpload(repo, uploadID, digest, r.Body); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				http.Error(w, "blob upload not found", http.StatusNotFound)
				return true
			}
			var digestErr *imageCacheBlobDigestMismatchError
			if errors.As(err, &digestErr) {
				http.Error(w, digestErr.Error(), http.StatusBadRequest)
				return true
			}
			http.Error(w, fmt.Sprintf("complete blob upload: %v", err), http.StatusInternalServerError)
			return true
		}
		w.Header().Set("Docker-Content-Digest", digest)
		w.Header().Set("Location", imageCacheBlobLocation(repo, digest))
		w.WriteHeader(http.StatusCreated)
		return true
	case http.MethodDelete:
		uploadID = cleanImageCacheUploadID(uploadID)
		if uploadID == "" {
			http.Error(w, "invalid blob upload id", http.StatusBadRequest)
			return true
		}
		if err := c.deleteBlobUpload(uploadID); err != nil {
			http.Error(w, fmt.Sprintf("delete blob upload: %v", err), http.StatusInternalServerError)
			return true
		}
		w.WriteHeader(http.StatusAccepted)
		return true
	default:
		return false
	}
}

func parseImageCacheBlobUploadPath(pathValue string) (string, string, bool) {
	parts := strings.Split(strings.Trim(pathValue, "/"), "/")
	if len(parts) < 4 || parts[0] != "v2" {
		return "", "", false
	}
	for i := 2; i+1 < len(parts); i++ {
		if parts[i] != "blobs" || parts[i+1] != "uploads" {
			continue
		}
		repo := strings.Trim(strings.Join(parts[1:i], "/"), "/")
		if repo == "" {
			return "", "", false
		}
		uploadID := ""
		if i+2 < len(parts) {
			uploadID = parts[i+2]
		}
		if i+3 < len(parts) {
			return "", "", false
		}
		return repo, uploadID, true
	}
	return "", "", false
}

func (c *imageCache) createBlobUpload(repo string, body io.Reader) (imageCacheBlobUploadState, error) {
	id := newImageCacheUploadID()
	state := imageCacheBlobUploadState{
		Repo:      strings.Trim(repo, "/"),
		UUID:      id,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := os.MkdirAll(c.blobUploadDir(), 0o755); err != nil {
		return state, err
	}
	file, err := os.OpenFile(c.blobUploadDataPath(id), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return state, err
	}
	if body != nil {
		written, copyErr := io.Copy(file, body)
		state.SizeBytes += written
		if copyErr != nil {
			_ = file.Close()
			_ = os.Remove(c.blobUploadDataPath(id))
			return state, copyErr
		}
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(c.blobUploadDataPath(id))
		return state, err
	}
	if err := c.writeBlobUploadState(state); err != nil {
		_ = os.Remove(c.blobUploadDataPath(id))
		return state, err
	}
	return state, nil
}

func (c *imageCache) appendBlobUpload(repo, uploadID string, body io.Reader) (imageCacheBlobUploadState, error) {
	state, err := c.readBlobUploadState(uploadID)
	if err != nil {
		return state, err
	}
	if !imageCacheUploadRepoMatches(state.Repo, repo) {
		return state, fmt.Errorf("blob upload repo mismatch: got %q want %q", repo, state.Repo)
	}
	file, err := os.OpenFile(c.blobUploadDataPath(uploadID), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return state, err
	}
	if body != nil {
		written, copyErr := io.Copy(file, body)
		state.SizeBytes += written
		if copyErr != nil {
			_ = file.Close()
			return state, copyErr
		}
	}
	if err := file.Close(); err != nil {
		return state, err
	}
	state.UpdatedAt = time.Now().UTC()
	if err := c.writeBlobUploadState(state); err != nil {
		return state, err
	}
	return state, nil
}

func (c *imageCache) completeBlobUpload(repo, uploadID, digest string, body io.Reader) error {
	state, err := c.appendBlobUpload(repo, uploadID, body)
	if err != nil {
		return err
	}
	if !imageCacheUploadRepoMatches(state.Repo, repo) {
		return fmt.Errorf("blob upload repo mismatch: got %q want %q", repo, state.Repo)
	}
	blobPath, err := imageCacheBlobStorePath(c.storeDir, digest)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(blobPath), 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(blobPath); err == nil {
		return c.deleteBlobUpload(uploadID)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	src, err := os.Open(c.blobUploadDataPath(uploadID))
	if err != nil {
		return err
	}
	defer src.Close()
	tmp, err := os.CreateTemp(filepath.Dir(blobPath), "blob-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(tmp, hasher), src)
	closeErr := tmp.Close()
	if copyErr != nil {
		_ = os.Remove(tmpName)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpName)
		return closeErr
	}
	actualDigest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if actualDigest != digest {
		_ = os.Remove(tmpName)
		_ = c.deleteBlobUpload(uploadID)
		return &imageCacheBlobDigestMismatchError{
			Expected: digest,
			Actual:   actualDigest,
			Size:     written,
		}
	}
	if err := os.Rename(tmpName, blobPath); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return c.deleteBlobUpload(uploadID)
}

func (c *imageCache) deleteBlobUpload(uploadID string) error {
	uploadID = cleanImageCacheUploadID(uploadID)
	if uploadID == "" {
		return nil
	}
	var errs []error
	for _, pathValue := range []string{c.blobUploadDataPath(uploadID), c.blobUploadStatePath(uploadID)} {
		if err := os.Remove(pathValue); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *imageCache) readBlobUploadState(uploadID string) (imageCacheBlobUploadState, error) {
	uploadID = cleanImageCacheUploadID(uploadID)
	if uploadID == "" {
		return imageCacheBlobUploadState{}, os.ErrNotExist
	}
	raw, err := os.ReadFile(c.blobUploadStatePath(uploadID))
	if err != nil {
		return imageCacheBlobUploadState{}, err
	}
	var state imageCacheBlobUploadState
	if err := json.Unmarshal(raw, &state); err != nil {
		return imageCacheBlobUploadState{}, err
	}
	if state.UUID == "" {
		state.UUID = uploadID
	}
	return state, nil
}

func (c *imageCache) writeBlobUploadState(state imageCacheBlobUploadState) error {
	state.UUID = cleanImageCacheUploadID(state.UUID)
	if state.UUID == "" {
		return fmt.Errorf("missing blob upload id")
	}
	state.Repo = strings.Trim(state.Repo, "/")
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(c.blobUploadDir(), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(c.blobUploadDir(), "upload-*.json.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, c.blobUploadStatePath(state.UUID))
}

func (c *imageCache) blobUploadDir() string {
	return filepath.Join(c.storeDir, imageCacheUploadDirName)
}

func (c *imageCache) blobUploadDataPath(uploadID string) string {
	return filepath.Join(c.blobUploadDir(), cleanImageCacheUploadID(uploadID)+".data")
}

func (c *imageCache) blobUploadStatePath(uploadID string) string {
	return filepath.Join(c.blobUploadDir(), cleanImageCacheUploadID(uploadID)+".json")
}

func newImageCacheUploadID() string {
	var random [16]byte
	if _, err := rand.Read(random[:]); err == nil {
		return hex.EncodeToString(random[:])
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	return hex.EncodeToString(sum[:16])
}

func cleanImageCacheUploadID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return ""
		}
	}
	return value
}

func imageCacheUploadRepoMatches(expected, actual string) bool {
	return strings.Trim(expected, "/") == strings.Trim(actual, "/")
}

func validateImageCacheBlobDigest(value string) (string, error) {
	digest := normalizeImageCacheDigest(value)
	parts := strings.SplitN(digest, ":", 2)
	if len(parts) != 2 || parts[0] != "sha256" || len(parts[1]) != sha256.Size*2 {
		return "", fmt.Errorf("invalid blob digest")
	}
	return digest, nil
}

func imageCacheBlobStorePath(storeDir, digest string) (string, error) {
	digest, err := validateImageCacheBlobDigest(digest)
	if err != nil {
		return "", err
	}
	parts := strings.SplitN(digest, ":", 2)
	return filepath.Join(storeDir, parts[0], parts[1]), nil
}

func imageCacheBlobLocation(repo, digest string) string {
	return "/" + strings.Join([]string{"v2", strings.Trim(repo, "/"), "blobs", digest}, "/")
}

func writeImageCacheUploadAccepted(w http.ResponseWriter, repo, uploadID string, size int64) {
	w.Header().Set("Docker-Upload-UUID", uploadID)
	w.Header().Set("Location", "/"+strings.Join([]string{"v2", strings.Trim(repo, "/"), "blobs/uploads", uploadID}, "/"))
	w.Header().Set("Range", imageCacheUploadRange(size))
	w.WriteHeader(http.StatusAccepted)
}

func imageCacheUploadRange(size int64) string {
	if size <= 0 {
		return "0-0"
	}
	return fmt.Sprintf("0-%d", size-1)
}

type imageCacheBlobDigestMismatchError struct {
	Expected string
	Actual   string
	Size     int64
}

func (e *imageCacheBlobDigestMismatchError) Error() string {
	if e == nil {
		return "blob digest mismatch"
	}
	return fmt.Sprintf("blob digest mismatch: expected %s actual %s size=%d", e.Expected, e.Actual, e.Size)
}

type persistedManifest struct {
	Repo        string `json:"repo"`
	Target      string `json:"target"`
	ContentType string `json:"content_type"`
	Body        []byte `json:"body"`
}

func (c *imageCache) persistManifest(repo, target, contentType string, body []byte) error {
	if c == nil || strings.TrimSpace(c.manifestDir) == "" {
		return nil
	}
	repo = strings.Trim(strings.TrimSpace(repo), "/")
	target = strings.TrimSpace(target)
	if repo == "" || target == "" || len(body) == 0 {
		return nil
	}
	if err := os.MkdirAll(c.manifestDir, 0o755); err != nil {
		return err
	}
	raw, err := json.Marshal(persistedManifest{
		Repo:        repo,
		Target:      target,
		ContentType: strings.TrimSpace(contentType),
		Body:        append([]byte(nil), body...),
	})
	if err != nil {
		return err
	}
	path := filepath.Join(c.manifestDir, manifestStoreKey(repo, target)+".json")
	tmp, err := os.CreateTemp(c.manifestDir, "manifest-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}

func manifestPersistTargets(target string, body []byte) []string {
	target = strings.TrimSpace(target)
	if target == "" || len(body) == 0 {
		return nil
	}
	targets := []string{target}
	digest := manifestBodyDigest(body)
	if digest != "" && digest != target {
		targets = append(targets, digest)
	}
	return targets
}

func manifestBodyDigest(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (c *imageCache) deletePersistedManifest(repo, target string) error {
	if c == nil || strings.TrimSpace(c.manifestDir) == "" {
		return nil
	}
	path := filepath.Join(c.manifestDir, manifestStoreKey(strings.Trim(strings.TrimSpace(repo), "/"), strings.TrimSpace(target))+".json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (c *imageCache) loadPersistedManifests() error {
	if c == nil || c.registry == nil || strings.TrimSpace(c.manifestDir) == "" {
		return nil
	}
	entries, err := os.ReadDir(c.manifestDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	pending := make([]persistedManifest, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(c.manifestDir, entry.Name()))
		if err != nil {
			return err
		}
		var manifest persistedManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return fmt.Errorf("decode %s: %w", entry.Name(), err)
		}
		if strings.TrimSpace(manifest.Repo) == "" || strings.TrimSpace(manifest.Target) == "" || len(manifest.Body) == 0 {
			continue
		}
		pending = append(pending, manifest)
	}

	loaded := 0
	var lastErr error
	for len(pending) > 0 {
		next := make([]persistedManifest, 0, len(pending))
		for _, manifest := range pending {
			if err := c.replayManifest(manifest); err != nil {
				lastErr = err
				next = append(next, manifest)
				continue
			}
			loaded++
		}
		if len(next) == 0 {
			break
		}
		if len(next) == len(pending) {
			return fmt.Errorf("replay %d persisted manifests failed after loading %d: %w", len(next), loaded, lastErr)
		}
		pending = next
	}
	if loaded > 0 {
		log.Printf("loaded %d persisted image cache manifests", loaded)
	}
	return nil
}

func (c *imageCache) replayManifest(manifest persistedManifest) error {
	path := "/v2/" + strings.Trim(strings.TrimSpace(manifest.Repo), "/") + "/manifests/" + strings.TrimSpace(manifest.Target)
	req := httptestRequest(http.MethodPut, path, manifest.ContentType, manifest.Body)
	rec := &memoryResponseWriter{header: http.Header{}}
	c.registry.ServeHTTP(rec, req)
	if rec.statusCode() < 200 || rec.statusCode() >= 300 {
		return fmt.Errorf("status=%d body=%s", rec.statusCode(), strings.TrimSpace(rec.body.String()))
	}
	return nil
}

func manifestStoreKey(repo, target string) string {
	sum := sha256.Sum256([]byte(repo + "\x00" + target))
	return hex.EncodeToString(sum[:])
}

type memoryResponseWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *memoryResponseWriter) Header() http.Header {
	return w.header
}

func (w *memoryResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *memoryResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(data)
}

func (w *memoryResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func httptestRequest(method, path, contentType string, body []byte) *http.Request {
	req, err := http.NewRequest(method, path, bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req
}

type statusRecordingWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusRecordingWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecordingWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(data)
}

func (w *statusRecordingWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (c *imageCache) reportRegistryWrite(r *http.Request, status int, manifestBody []byte) {
	if r == nil || r.URL == nil || status < 200 || status >= 300 {
		return
	}
	if r.Method != http.MethodPut {
		return
	}
	repo, target, targetKind, ok := parseRegistryTarget(r.URL.Path)
	if !ok || targetKind != registryTargetManifest {
		return
	}
	logicalRef, digest := imageRef(c.registryBase, repo, target)
	bodyDigest := manifestBodyDigest(manifestBody)
	if digest == "" {
		digest = bodyDigest
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := c.report(ctx, logicalRef, digest, "present", ""); err != nil {
			log.Printf("report pushed image location ref=%s failed: %v", logicalRef, err)
		}
		if bodyDigest == "" || bodyDigest == target {
			return
		}
		digestRef, digest := imageRef(c.registryBase, repo, bodyDigest)
		if err := c.report(ctx, digestRef, digest, "present", ""); err != nil {
			log.Printf("report pushed image location ref=%s failed: %v", digestRef, err)
		}
	}()
}

type registryTargetKind string

const (
	registryTargetManifest registryTargetKind = "manifest"
	registryTargetBlob     registryTargetKind = "blob"
)

const maxProxiedManifestBytes = 64 << 20
const imageCacheLocalOnlyHeader = "X-Fugue-Image-Cache-Local-Only"

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

func (c *imageCache) startHydrate(repo, target string) {
	call := c.joinHydrate(repo+"\x00"+target, repo, target)
	c.leaveHydrate(call)
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

	call := &hydrateCall{
		done:    make(chan struct{}),
		waiters: 1,
	}
	c.hydrateCalls[key] = call
	go func() {
		err := c.hydrateOnce(context.Background(), repo, target)

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
			if err := c.ensureLocalManifest(ctx, peerBase, repo, target); err != nil {
				_ = c.report(ctx, logicalRef, digest, "failed", err.Error())
				return err
			}
			log.Printf("hydrated %s from peer %s", logicalRef, peerBase)
			_ = c.report(ctx, logicalRef, digest, "present", "")
			return nil
		} else {
			log.Printf("peer hydrate %s from %s failed: %v", logicalRef, peerBase, err)
			if registryObjectMissing(err) {
				if reportErr := c.reportLocation(ctx, location, logicalRef, digest, "missing", err.Error()); reportErr != nil {
					log.Printf("report stale peer image location ref=%s endpoint=%s failed: %v", logicalRef, location.CacheEndpoint, reportErr)
				}
			}
		}
	}
	if c.upstreamBase != "" {
		upstreamRef, _ := imageRef(c.upstreamBase, repo, target)
		if err := c.copyImage(ctx, upstreamRef, localRef); err == nil {
			if err := c.ensureLocalManifest(ctx, c.upstreamBase, repo, target); err != nil {
				_ = c.report(ctx, logicalRef, digest, "failed", err.Error())
				return err
			}
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
	release, err := acquireSemaphore(ctx, c.hydrateSlots)
	if err != nil {
		return err
	}
	defer release()
	if c.copyImageFn != nil {
		return c.copyImageFn(ctx, src, dst)
	}
	return copyImage(ctx, src, dst, c.copyJobs)
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
	return c.reportLocation(ctx, imageLocation{CacheEndpoint: c.cacheEndpoint, ClusterNodeName: c.clusterNode}, imageRef, digest, status, lastError)
}

func (c *imageCache) reportLocation(ctx context.Context, location imageLocation, imageRef, digest, status, lastError string) error {
	if c.apiBase == "" || c.apiToken == "" {
		return nil
	}
	values := url.Values{}
	values.Set("image_ref", imageRef)
	values.Set("digest", digest)
	values.Set("status", status)
	values.Set("last_error", lastError)
	if location.TenantID != "" {
		values.Set("tenant_id", location.TenantID)
	}
	if location.AppID != "" {
		values.Set("app_id", location.AppID)
	}
	if location.SourceOperationID != "" {
		values.Set("source_operation_id", location.SourceOperationID)
	}
	if location.NodeID != "" {
		values.Set("node_id", location.NodeID)
	}
	if location.RuntimeID != "" {
		values.Set("runtime_id", location.RuntimeID)
	}
	if location.ClusterNodeName != "" {
		values.Set("cluster_node_name", location.ClusterNodeName)
	}
	values.Set("cache_endpoint", strings.TrimRight(strings.TrimSpace(location.CacheEndpoint), "/"))
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

func discoverKubernetesPodNodeName(ctx context.Context) (string, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	if host == "" {
		return "", errors.New("KUBERNETES_SERVICE_HOST is not set")
	}
	port := env("KUBERNETES_SERVICE_PORT", "443")
	namespaceData, err := os.ReadFile(serviceAccountNamespacePath)
	if err != nil {
		return "", fmt.Errorf("read service account namespace: %w", err)
	}
	tokenData, err := os.ReadFile(serviceAccountTokenPath)
	if err != nil {
		return "", fmt.Errorf("read service account token: %w", err)
	}
	caData, err := os.ReadFile(serviceAccountCAPath)
	if err != nil {
		return "", fmt.Errorf("read service account CA: %w", err)
	}
	podName, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("get hostname: %w", err)
	}
	client, err := kubernetesAPIClient(caData)
	if err != nil {
		return "", err
	}
	baseURL := "https://" + net.JoinHostPort(host, port)
	token := strings.TrimSpace(string(tokenData))
	namespace := strings.TrimSpace(string(namespaceData))
	nodeName, err := fetchKubernetesPodNodeName(ctx, client, baseURL, token, namespace, strings.TrimSpace(podName))
	if err == nil {
		return nodeName, nil
	}
	directErr := err
	hostIP := strings.TrimSpace(os.Getenv("NODE_IP"))
	if hostIP == "" {
		return "", directErr
	}
	nodeName, err = fetchKubernetesPodNodeNameByHostIP(ctx, client, baseURL, token, namespace, hostIP)
	if err != nil {
		return "", fmt.Errorf("get pod by hostname: %v; get pod by host IP: %w", directErr, err)
	}
	return nodeName, nil
}

func kubernetesAPIClient(caPEM []byte) (*http.Client, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("parse service account CA")
	}
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    roots,
			},
		},
	}, nil
}

func fetchKubernetesPodNodeName(ctx context.Context, client *http.Client, baseURL, token, namespace, podName string) (string, error) {
	if client == nil {
		client = http.DefaultClient
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	token = strings.TrimSpace(token)
	namespace = strings.TrimSpace(namespace)
	podName = strings.TrimSpace(podName)
	if baseURL == "" || token == "" || namespace == "" || podName == "" {
		return "", errors.New("kubernetes pod lookup is missing base URL, token, namespace, or pod name")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v1/namespaces/"+url.PathEscape(namespace)+"/pods/"+url.PathEscape(podName), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("get pod status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var decoded struct {
		Spec struct {
			NodeName string `json:"nodeName"`
		} `json:"spec"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", err
	}
	nodeName := strings.TrimSpace(decoded.Spec.NodeName)
	if nodeName == "" {
		return "", errors.New("pod spec.nodeName is empty")
	}
	return nodeName, nil
}

func fetchKubernetesPodNodeNameByHostIP(ctx context.Context, client *http.Client, baseURL, token, namespace, hostIP string) (string, error) {
	if client == nil {
		client = http.DefaultClient
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	token = strings.TrimSpace(token)
	namespace = strings.TrimSpace(namespace)
	hostIP = strings.TrimSpace(hostIP)
	if baseURL == "" || token == "" || namespace == "" || hostIP == "" {
		return "", errors.New("kubernetes pod lookup is missing base URL, token, namespace, or host IP")
	}
	endpoint := baseURL + "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	query := req.URL.Query()
	query.Set("labelSelector", "app.kubernetes.io/component=image-cache")
	req.URL.RawQuery = query.Encode()
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("list pods status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var decoded struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				NodeName string `json:"nodeName"`
			} `json:"spec"`
			Status struct {
				HostIP string `json:"hostIP"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", err
	}
	for _, item := range decoded.Items {
		if strings.TrimSpace(item.Status.HostIP) != hostIP {
			continue
		}
		nodeName := strings.TrimSpace(item.Spec.NodeName)
		if nodeName == "" {
			return "", fmt.Errorf("pod %s on host IP %s has empty spec.nodeName", strings.TrimSpace(item.Metadata.Name), hostIP)
		}
		return nodeName, nil
	}
	return "", fmt.Errorf("no image-cache pod found with host IP %s", hostIP)
}

func registryObjectMissing(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, token := range []string{
		"name_unknown",
		"manifest_unknown",
		"blob_unknown",
		"unknown name",
		"manifest unknown",
		"blob unknown",
		"404",
		"not found",
	} {
		if strings.Contains(message, token) {
			return true
		}
	}
	return false
}

func copyImage(ctx context.Context, src, dst string, jobs int) error {
	opts := []crane.Option{crane.WithContext(ctx), crane.Insecure}
	if jobs > 0 {
		opts = append(opts, crane.WithJobs(jobs))
	}
	return crane.Copy(src, dst, opts...)
}

func (c *imageCache) ensureLocalManifest(ctx context.Context, sourceBase, repo, target string) error {
	return c.ensureLocalManifestTree(ctx, sourceBase, repo, target, map[string]struct{}{})
}

func (c *imageCache) ensureLocalManifestTree(ctx context.Context, sourceBase, repo, target string, seen map[string]struct{}) error {
	if c == nil || c.registry == nil {
		return nil
	}
	if c.localManifestAvailable(repo, target) {
		return nil
	}
	key := sourceCacheKey(registryTargetManifest, repo, target)
	if _, ok := seen[key]; ok {
		return nil
	}
	seen[key] = struct{}{}
	contentType, body, err := c.fetchManifest(ctx, sourceBase, repo, target)
	if err != nil {
		return err
	}
	for _, descriptor := range manifestReferencedTargets(body) {
		if descriptor.kind != registryTargetManifest {
			continue
		}
		if err := c.ensureLocalManifestTree(ctx, sourceBase, repo, descriptor.target, seen); err != nil {
			return err
		}
	}
	manifest := persistedManifest{
		Repo:        strings.Trim(strings.TrimSpace(repo), "/"),
		Target:      strings.TrimSpace(target),
		ContentType: contentType,
		Body:        body,
	}
	if err := c.replayManifest(manifest); err != nil {
		return fmt.Errorf("replay hydrated manifest: %w", err)
	}
	if err := c.persistManifest(manifest.Repo, manifest.Target, manifest.ContentType, manifest.Body); err != nil {
		return fmt.Errorf("persist hydrated manifest: %w", err)
	}
	if !c.localManifestAvailable(repo, target) {
		return fmt.Errorf("hydrated manifest is still unavailable locally for %s:%s", repo, target)
	}
	return nil
}

func (c *imageCache) localManifestAvailable(repo, target string) bool {
	if c == nil || c.registry == nil {
		return false
	}
	path := "/v2/" + strings.Trim(strings.TrimSpace(repo), "/") + "/manifests/" + strings.TrimSpace(target)
	req := httptestRequest(http.MethodHead, path, "", nil)
	rec := &memoryResponseWriter{header: http.Header{}}
	c.registry.ServeHTTP(rec, req)
	return rec.statusCode() >= 200 && rec.statusCode() < 300
}

func (c *imageCache) fetchManifest(ctx context.Context, sourceBase, repo, target string) (string, []byte, error) {
	sourceBase = trimRegistryBase(sourceBase)
	if sourceBase == "" {
		return "", nil, fmt.Errorf("manifest source is empty")
	}
	targetURL := "http://" + sourceBase + "/v2/" + strings.Trim(strings.TrimSpace(repo), "/") + "/manifests/" + strings.TrimSpace(target)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Accept", strings.Join([]string{
		"application/vnd.oci.image.index.v1+json",
		"application/vnd.oci.image.manifest.v1+json",
		"application/vnd.docker.distribution.manifest.list.v2+json",
		"application/vnd.docker.distribution.manifest.v2+json",
		"application/vnd.docker.distribution.manifest.v1+prettyjws",
		"application/vnd.docker.distribution.manifest.v1+json",
	}, ", "))
	req.Header.Set(imageCacheLocalOnlyHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", nil, fmt.Errorf("fetch manifest from %s status=%d", sourceBase, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProxiedManifestBytes+1))
	if err != nil {
		return "", nil, err
	}
	if len(body) > maxProxiedManifestBytes {
		return "", nil, fmt.Errorf("manifest from %s exceeds %d bytes", sourceBase, maxProxiedManifestBytes)
	}
	return resp.Header.Get("Content-Type"), body, nil
}

type registrySource struct {
	base     string
	location imageLocation
}

type proxyPullResult struct {
	body    []byte
	ok      bool
	missing bool
	status  int
	err     error
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

func isLocalOnlyRegistryPull(r *http.Request) bool {
	if r == nil {
		return false
	}
	value := strings.TrimSpace(r.Header.Get(imageCacheLocalOnlyHeader))
	return value == "1" || strings.EqualFold(value, "true")
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
	result := c.proxyRegistryPull(w, r, trimRegistryBase(c.upstreamBase))
	if result.ok {
		return true
	}
	if result.err != nil {
		log.Printf("upstream blob proxy repo=%s digest=%s failed: %v", repo, digest, result.err)
	} else if result.status != 0 {
		log.Printf("upstream blob proxy repo=%s digest=%s status=%d", repo, digest, result.status)
	}
	return false
}

func (c *imageCache) proxyBlobFromKnownSource(w http.ResponseWriter, r *http.Request, repo, digest string) bool {
	sourceBase, ok := c.knownSource(registryTargetBlob, repo, digest)
	if !ok || sourceBase == "" {
		return false
	}
	result := c.proxyRegistryPull(w, r, sourceBase)
	if result.ok {
		return true
	}
	if result.missing {
		c.forgetSource(registryTargetBlob, repo, digest, sourceBase)
	}
	if result.err != nil {
		log.Printf("known-source blob proxy repo=%s digest=%s source=%s failed: %v", repo, digest, sourceBase, result.err)
	} else if result.status != 0 {
		log.Printf("known-source blob proxy repo=%s digest=%s source=%s status=%d", repo, digest, sourceBase, result.status)
	}
	return false
}

func (c *imageCache) proxyManifestFromRemote(w http.ResponseWriter, r *http.Request, repo, target string) bool {
	logicalRef, digest := imageRef(c.registryBase, repo, target)
	if sourceBase, ok := c.knownSource(registryTargetManifest, repo, target); ok {
		result := c.proxyRegistryPull(w, r, sourceBase)
		if result.ok {
			c.rememberManifestSource(repo, target, sourceBase, result.body)
			return true
		}
		if result.missing {
			c.forgetSource(registryTargetManifest, repo, target, sourceBase)
		}
		if result.err != nil {
			log.Printf("known-source manifest proxy %s from %s failed: %v", logicalRef, sourceBase, result.err)
		} else if result.status != 0 {
			log.Printf("known-source manifest proxy %s from %s status=%d", logicalRef, sourceBase, result.status)
		}
	}
	for _, source := range c.remoteSources(r.Context(), logicalRef, digest) {
		result := c.proxyRegistryPull(w, r, source.base)
		if result.ok {
			c.rememberManifestSource(repo, target, source.base, result.body)
			return true
		}
		if result.missing && source.location.CacheEndpoint != "" {
			reportCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			if err := c.reportLocation(reportCtx, source.location, logicalRef, digest, "missing", fmt.Sprintf("proxy status=%d", result.status)); err != nil {
				log.Printf("report stale peer image location ref=%s endpoint=%s failed: %v", logicalRef, source.location.CacheEndpoint, err)
			}
			cancel()
		}
		if result.err != nil {
			log.Printf("remote manifest proxy %s from %s failed: %v", logicalRef, source.base, result.err)
		} else if result.status != 0 {
			log.Printf("remote manifest proxy %s from %s status=%d", logicalRef, source.base, result.status)
		}
	}
	return false
}

func (c *imageCache) remoteSources(ctx context.Context, imageRef, digest string) []registrySource {
	sources := make([]registrySource, 0, 4)
	seen := map[string]struct{}{}
	for _, location := range c.lookup(ctx, imageRef, digest) {
		if strings.TrimSpace(location.CacheEndpoint) == "" || !strings.EqualFold(location.Status, "present") {
			continue
		}
		if sameEndpoint(location.CacheEndpoint, c.cacheEndpoint) {
			continue
		}
		base := trimRegistryBase(location.CacheEndpoint)
		if base == "" {
			continue
		}
		if _, ok := seen[base]; ok {
			continue
		}
		seen[base] = struct{}{}
		sources = append(sources, registrySource{base: base, location: location})
	}
	if c.upstreamBase != "" {
		base := trimRegistryBase(c.upstreamBase)
		if _, ok := seen[base]; base != "" && !ok {
			sources = append(sources, registrySource{base: base})
		}
	}
	return sources
}

func (c *imageCache) proxyRegistryPull(w http.ResponseWriter, r *http.Request, sourceBase string) proxyPullResult {
	if r == nil || r.URL == nil || strings.TrimSpace(sourceBase) == "" {
		return proxyPullResult{}
	}
	release, err := acquireSemaphore(r.Context(), c.proxySlots)
	if err != nil {
		return proxyPullResult{err: err}
	}
	defer release()
	targetURL := "http://" + trimRegistryBase(sourceBase) + r.URL.EscapedPath()
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, nil)
	if err != nil {
		return proxyPullResult{err: err}
	}
	copyRequestHeader(req.Header, r.Header)
	req.Header.Set(imageCacheLocalOnlyHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return proxyPullResult{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return proxyPullResult{status: resp.StatusCode, missing: resp.StatusCode == http.StatusNotFound}
	}
	if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/manifests/") {
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxProxiedManifestBytes+1))
		if err != nil {
			return proxyPullResult{status: resp.StatusCode, err: err}
		}
		if len(body) > maxProxiedManifestBytes {
			return proxyPullResult{status: resp.StatusCode, err: fmt.Errorf("proxied manifest exceeds %d bytes", maxProxiedManifestBytes)}
		}
		copyHeader(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		if _, err := w.Write(body); err != nil {
			log.Printf("write proxied registry manifest source=%s path=%s failed: %v", sourceBase, r.URL.Path, err)
		}
		return proxyPullResult{body: body, ok: true, status: resp.StatusCode}
	}
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if r.Method == http.MethodHead {
		return proxyPullResult{ok: true, status: resp.StatusCode}
	}
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("stream proxied registry pull source=%s path=%s failed: %v", sourceBase, r.URL.Path, err)
	}
	return proxyPullResult{ok: true, status: resp.StatusCode}
}

func copyRequestHeader(dst, src http.Header) {
	for key, values := range src {
		if strings.EqualFold(key, "Host") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func (c *imageCache) rememberManifestSource(repo, target, sourceBase string, manifestBody []byte) {
	c.rememberSource(registryTargetManifest, repo, target, sourceBase)
	for _, descriptor := range manifestReferencedTargets(manifestBody) {
		c.rememberSource(descriptor.kind, repo, descriptor.target, sourceBase)
	}
}

type referencedRegistryTarget struct {
	kind   registryTargetKind
	target string
}

func manifestReferencedTargets(body []byte) []referencedRegistryTarget {
	if len(body) == 0 {
		return nil
	}
	var decoded struct {
		Config    *manifestDescriptor  `json:"config"`
		Layers    []manifestDescriptor `json:"layers"`
		Manifests []manifestDescriptor `json:"manifests"`
		FSLayers  []struct {
			BlobSum string `json:"blobSum"`
		} `json:"fsLayers"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil
	}
	targets := make([]referencedRegistryTarget, 0, 1+len(decoded.Layers)+len(decoded.Manifests))
	if decoded.Config != nil && strings.TrimSpace(decoded.Config.Digest) != "" {
		targets = append(targets, referencedRegistryTarget{kind: registryTargetBlob, target: decoded.Config.Digest})
	}
	for _, layer := range decoded.Layers {
		if strings.TrimSpace(layer.Digest) != "" {
			targets = append(targets, referencedRegistryTarget{kind: registryTargetBlob, target: layer.Digest})
		}
	}
	for _, layer := range decoded.FSLayers {
		if strings.TrimSpace(layer.BlobSum) != "" {
			targets = append(targets, referencedRegistryTarget{kind: registryTargetBlob, target: layer.BlobSum})
		}
	}
	for _, manifest := range decoded.Manifests {
		if strings.TrimSpace(manifest.Digest) != "" {
			targets = append(targets, referencedRegistryTarget{kind: registryTargetManifest, target: manifest.Digest})
		}
	}
	return targets
}

func (c *imageCache) rememberSource(kind registryTargetKind, repo, target, sourceBase string) {
	if c == nil {
		return
	}
	repo = strings.Trim(strings.TrimSpace(repo), "/")
	target = strings.TrimSpace(target)
	sourceBase = trimRegistryBase(sourceBase)
	if repo == "" || target == "" || sourceBase == "" {
		return
	}
	ttl := c.sourceTTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	c.sourceMu.Lock()
	defer c.sourceMu.Unlock()
	if c.sourceByTarget == nil {
		c.sourceByTarget = make(map[string]sourceCacheEntry)
	}
	c.sourceByTarget[sourceCacheKey(kind, repo, target)] = sourceCacheEntry{base: sourceBase, expiresAt: time.Now().Add(ttl)}
}

func (c *imageCache) knownSource(kind registryTargetKind, repo, target string) (string, bool) {
	if c == nil {
		return "", false
	}
	repo = strings.Trim(strings.TrimSpace(repo), "/")
	target = strings.TrimSpace(target)
	c.sourceMu.RLock()
	entry, ok := c.sourceByTarget[sourceCacheKey(kind, repo, target)]
	c.sourceMu.RUnlock()
	if !ok || entry.base == "" {
		return "", false
	}
	if time.Now().After(entry.expiresAt) {
		c.forgetSource(kind, repo, target, entry.base)
		return "", false
	}
	return entry.base, true
}

func (c *imageCache) forgetSource(kind registryTargetKind, repo, target, sourceBase string) {
	if c == nil {
		return
	}
	repo = strings.Trim(strings.TrimSpace(repo), "/")
	target = strings.TrimSpace(target)
	sourceBase = trimRegistryBase(sourceBase)
	c.sourceMu.Lock()
	defer c.sourceMu.Unlock()
	key := sourceCacheKey(kind, repo, target)
	if entry, ok := c.sourceByTarget[key]; ok && (sourceBase == "" || entry.base == sourceBase) {
		delete(c.sourceByTarget, key)
	}
}

func sourceCacheKey(kind registryTargetKind, repo, target string) string {
	return string(kind) + "\x00" + strings.Trim(strings.TrimSpace(repo), "/") + "\x00" + strings.TrimSpace(target)
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

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envFloat(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func envBytes(key string, fallback int64) int64 {
	parsed, err := parseImageCacheByteSize(os.Getenv(key))
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func parseImageCacheByteSize(raw string) (int64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, nil
	}
	compact := strings.ToUpper(strings.ReplaceAll(value, " ", ""))
	units := []struct {
		suffix     string
		multiplier float64
	}{
		{"TIB", 1024 * 1024 * 1024 * 1024},
		{"TI", 1024 * 1024 * 1024 * 1024},
		{"TB", 1024 * 1024 * 1024 * 1024},
		{"T", 1024 * 1024 * 1024 * 1024},
		{"GIB", 1024 * 1024 * 1024},
		{"GI", 1024 * 1024 * 1024},
		{"GB", 1024 * 1024 * 1024},
		{"G", 1024 * 1024 * 1024},
		{"MIB", 1024 * 1024},
		{"MI", 1024 * 1024},
		{"MB", 1024 * 1024},
		{"M", 1024 * 1024},
		{"KIB", 1024},
		{"KI", 1024},
		{"KB", 1024},
		{"K", 1024},
		{"B", 1},
	}
	multiplier := float64(1)
	number := compact
	for _, unit := range units {
		if strings.HasSuffix(compact, unit.suffix) {
			multiplier = unit.multiplier
			number = strings.TrimSuffix(compact, unit.suffix)
			break
		}
	}
	parsed, err := strconv.ParseFloat(number, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("invalid byte size %q", raw)
	}
	return int64(parsed * multiplier), nil
}

func parseImageCacheDuration(raw string) (time.Duration, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("invalid duration %q", raw)
	}
	return parsed, nil
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return fallback
	}
	return parsed
}

func newSemaphore(limit int) chan struct{} {
	if limit < 1 {
		return nil
	}
	return make(chan struct{}, limit)
}

func acquireSemaphore(ctx context.Context, slots chan struct{}) (func(), error) {
	if slots == nil {
		return func() {}, nil
	}
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, nil
	case <-ctx.Done():
		return func() {}, ctx.Err()
	}
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
