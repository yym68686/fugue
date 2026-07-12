package edge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/httpx"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/tcpdiag"
	"fugue/internal/weightedselector"
)

const cacheFileVersion = 1

const edgePeerFallbackHeader = "X-Fugue-Edge-Peer-Fallback"
const edgeClientRemoteAddrHeader = "X-Fugue-Edge-Client-Remote-Addr"
const edgeRequestIDHeader = "X-Fugue-Edge-Request-Id"
const edgeTraceIDHeader = "X-Fugue-Trace-Id"
const edgeStatusClientClosedRequest = 499

const edgeRequestBodyCopyBufferSize = 32 * 1024

type Service struct {
	Config                   config.EdgeConfig
	HTTPClient               *http.Client
	Logger                   *log.Logger
	caddyWarmup              func(context.Context, string, string) error
	cacheWarmupClientFactory func(string, string) *http.Client
	proxyBase                http.RoundTripper
	proxyTransportMu         sync.Mutex
	proxyTransportPrototype  *http.Transport
	proxyTransports          map[string]*http.Transport
	proxyTransportActiveKeys map[string]struct{}
	proxyTransportBundleSet  bool
	bodyBuffer               *edgeRequestBodyBufferManager
	requestBodyPolicyMu      sync.Mutex
	requestBodyPolicyGuards  map[string]*edgeRequestBodyPolicyGuard

	mu                    sync.Mutex
	snapshot              Status
	bundle                *model.EdgeRouteBundle
	etag                  string
	metrics               telemetry
	performanceBaseline   telemetry
	cacheRevalidating     map[string]struct{}
	bodyBufferActiveMu    sync.Mutex
	activeBodyBufferReads map[string]edgeActiveRequestBodyBuffer
	activeProxyRequests   int64
	walMu                 sync.Mutex
	walActionLast         map[string]time.Time
}

type Status struct {
	Status                 string     `json:"status"`
	Healthy                bool       `json:"healthy"`
	EdgeID                 string     `json:"edge_id,omitempty"`
	EdgeGroupID            string     `json:"edge_group_id,omitempty"`
	BundleVersion          string     `json:"bundle_version,omitempty"`
	ServingGeneration      string     `json:"serving_generation,omitempty"`
	LKGGeneration          string     `json:"lkg_generation,omitempty"`
	LastGoodGeneration     string     `json:"last_good_generation,omitempty"`
	CacheCorruptGeneration string     `json:"cache_corrupt_generation,omitempty"`
	BundleValidUntil       *time.Time `json:"bundle_valid_until,omitempty"`
	RouteCount             int        `json:"route_count"`
	TLSAllowlistCount      int        `json:"tls_allowlist_count"`
	LastSyncAt             *time.Time `json:"last_sync_at,omitempty"`
	LastSuccessAt          *time.Time `json:"last_success_at,omitempty"`
	LastError              string     `json:"last_error,omitempty"`
	DegradedReason         string     `json:"degraded_reason,omitempty"`
	StaleCache             bool       `json:"stale_cache"`
	MaxStaleExceeded       bool       `json:"max_stale_exceeded,omitempty"`
	CachePath              string     `json:"cache_path,omitempty"`
	CaddyEnabled           bool       `json:"caddy_enabled,omitempty"`
	CaddyListenAddr        string     `json:"caddy_listen_addr,omitempty"`
	CaddyTLSMode           string     `json:"caddy_tls_mode,omitempty"`
	CaddyAppliedVersion    string     `json:"caddy_applied_version,omitempty"`
	CaddyLastApplyAt       *time.Time `json:"caddy_last_apply_at,omitempty"`
	CaddyLastError         string     `json:"caddy_last_error,omitempty"`
}

type edgeDesiredStateEnvelope struct {
	DesiredState edgeDesiredState `json:"desired_state"`
}

type edgeDesiredState struct {
	EdgeID            string     `json:"edge_id"`
	EdgeGroupID       string     `json:"edge_group_id"`
	WorkloadMode      string     `json:"workload_mode"`
	CanaryState       string     `json:"canary_state"`
	CanaryWeight      int        `json:"canary_weight"`
	PublicProbeStatus string     `json:"public_probe_status"`
	DNSEligible       bool       `json:"dns_eligible"`
	Draining          bool       `json:"draining"`
	RouteReady        bool       `json:"route_ready"`
	TLSReady          bool       `json:"tls_ready"`
	LastHeartbeatAt   *time.Time `json:"last_heartbeat_at,omitempty"`
}

type cacheFile struct {
	Version  int                   `json:"version"`
	ETag     string                `json:"etag,omitempty"`
	CachedAt time.Time             `json:"cached_at"`
	Bundle   model.EdgeRouteBundle `json:"bundle"`
}

type telemetry struct {
	BundleSyncSuccess           uint64
	BundleSyncNotModified       uint64
	BundleSyncError             uint64
	LastSyncDuration            time.Duration
	CacheWriteSuccess           uint64
	CacheWriteError             uint64
	CacheLoadSuccess            uint64
	CacheLoadMiss               uint64
	CacheLoadError              uint64
	CaddyConfigSuccess          uint64
	CaddyConfigError            uint64
	CaddyAppliedVersion         string
	CaddyAppliedSignature       string
	CaddyRouteCount             int
	CaddyLastApplyAt            *time.Time
	CaddyLastError              string
	CaddyWarmupSuccess          uint64
	CaddyWarmupError            uint64
	CaddyWarmupSignature        string
	CaddyWarmupHost             string
	CaddyWarmupDuration         time.Duration
	CaddyWarmupAt               *time.Time
	CaddyWarmupLastError        string
	CacheWarmupSuccess          uint64
	CacheWarmupError            uint64
	CacheWarmupSignature        string
	CacheWarmupTargets          string
	CacheWarmupDuration         time.Duration
	CacheWarmupAt               *time.Time
	CacheWarmupLastError        string
	RouteRequests               map[routeMetricKey]uint64
	RouteStatuses               map[routeStatusMetricKey]uint64
	RouteUpstreamErrors         map[routeMetricKey]uint64
	RouteFallbackHits           map[routeMetricKey]uint64
	RouteWebSocketResults       map[routeResultMetricKey]uint64
	RouteSSEResults             map[routeResultMetricKey]uint64
	RouteStreamingResults       map[routeResultMetricKey]uint64
	RouteUploadRequests         map[routeMetricKey]uint64
	RouteDurationCount          map[routeMetricKey]uint64
	RouteDurationSum            map[routeMetricKey]float64
	RouteLatencyCount           map[routeMetricKey]uint64
	RouteLatencySum             map[routeMetricKey]float64
	RouteTTFBCount              map[routeMetricKey]uint64
	RouteTTFBSum                map[routeMetricKey]float64
	RouteCacheLookupCount       map[routeMetricKey]uint64
	RouteCacheLookupSum         map[routeMetricKey]float64
	RouteOriginConnCount        map[routeMetricKey]uint64
	RouteOriginConnSum          map[routeMetricKey]float64
	RouteOriginDNSCount         map[routeMetricKey]uint64
	RouteOriginDNSSum           map[routeMetricKey]float64
	RouteOriginTTFBCount        map[routeMetricKey]uint64
	RouteOriginTTFBSum          map[routeMetricKey]float64
	RouteOriginTotalCount       map[routeMetricKey]uint64
	RouteOriginTotalSum         map[routeMetricKey]float64
	RouteOriginWriteCount       map[routeMetricKey]uint64
	RouteOriginWriteSum         map[routeMetricKey]float64
	RouteOriginWaitCount        map[routeMetricKey]uint64
	RouteOriginWaitSum          map[routeMetricKey]float64
	RouteBodyReadCount          map[routeMetricKey]uint64
	RouteBodyReadSum            map[routeMetricKey]float64
	RouteBodyWriteCount         map[routeMetricKey]uint64
	RouteBodyWriteSum           map[routeMetricKey]float64
	RouteBodyThroughputCount    map[routeMetricKey]uint64
	RouteBodyThroughputSum      map[routeMetricKey]float64
	RouteBodyMinThroughputCount map[routeMetricKey]uint64
	RouteBodyMinThroughputSum   map[routeMetricKey]float64
	RouteBodyMaxReadGapCount    map[routeMetricKey]uint64
	RouteBodyMaxReadGapSum      map[routeMetricKey]float64
	RouteBodyRequestBytes       map[routeMetricKey]uint64
	RouteBodyReadBytes          map[routeMetricKey]uint64
	RouteBodyIncompleteCount    map[routeMetricKey]uint64
	RouteBodyReadErrorCount     map[routeMetricKey]uint64
	RouteResponseBytes          map[routeMetricKey]uint64
	RouteClientCancelCount      map[routeMetricKey]uint64
	RouteWriteCount             map[routeMetricKey]uint64
	RouteWriteSum               map[routeMetricKey]float64
	RouteCacheStatus            map[routeCacheMetricKey]uint64
}

type metricSnapshot struct {
	Status            Status
	Metrics           telemetry
	BundleGeneratedAt *time.Time
}

type statusError struct {
	StatusCode int
	Body       string
}

type routeMetricKey struct {
	Hostname      string
	PathPrefix    string
	Method        string
	AppID         string
	RouteKind     string
	ClientCountry string
	ClientRegion  string
	ClientASN     string
}

func routeMetricIdentityKey(key routeMetricKey) routeMetricKey {
	key.Method = ""
	key.ClientCountry = ""
	key.ClientRegion = ""
	key.ClientASN = ""
	return key
}

type routeStatusMetricKey struct {
	RouteMetricKey routeMetricKey
	StatusCode     int
}

type routeResultMetricKey struct {
	RouteMetricKey routeMetricKey
	Result         string
}

type routeCacheMetricKey struct {
	RouteMetricKey routeMetricKey
	CacheStatus    string
	CachePolicyID  string
	AssetClass     string
}

type edgeProxyObservation struct {
	ReceivedAt              time.Time
	Host                    string
	Route                   model.EdgeRouteBinding
	ReleaseID               string
	ReleaseRole             string
	TrafficWeight           int
	Method                  string
	Path                    string
	TraceID                 string
	RequestID               string
	EdgeRequestID           string
	Protocol                string
	ClientIP                string
	ClientRemoteAddr        string
	ClientCountry           string
	ClientRegion            string
	ClientASN               string
	StatusCode              int
	Duration                time.Duration
	TTFB                    time.Duration
	Upstream                time.Duration
	CacheLookup             time.Duration
	OriginDNS               time.Duration
	OriginDNSError          string
	OriginConnect           time.Duration
	OriginConnectError      string
	OriginGotConn           bool
	OriginConnectionReused  bool
	OriginRemoteAddr        string
	OriginLocalAddr         string
	OriginWroteHeaders      bool
	OriginWroteRequest      bool
	OriginRequestWrite      time.Duration
	OriginRequestWriteErr   string
	OriginTTFB              time.Duration
	OriginTotal             time.Duration
	ResponseWrite           time.Duration
	UpstreamError           string
	Proxied                 bool
	FallbackHit             bool
	WebSocket               bool
	SSE                     bool
	Streaming               bool
	Upload                  bool
	PeerFallback            bool
	ClientCanceled          bool
	CacheStatus             string
	CachePolicyID           string
	CacheKeyHash            string
	AssetClass              string
	RequestBytes            int64
	RequestBodyReadBytes    int64
	RequestBodyReadError    string
	RequestBodyEOF          bool
	RequestBodyBuffered     bool
	RequestBodyBufferBytes  int64
	RequestBodyBuffer       time.Duration
	RequestBodyBufferError  string
	RequestBodyBufferPath   string
	RequestBodyBufferBudget int64
	RequestBodyBufferUsed   int64
	RequestBodyBufferActive int64
	BodyReadBlock           time.Duration
	FileWrite               time.Duration
	FirstBodyByte           time.Duration
	LastBodyByte            time.Duration
	MaxReadGap              time.Duration
	ReadCalls               int64
	AvgBPS                  int64
	MinWindowBPS            int64
	EdgeProxyTCPInfo        tcpdiag.Snapshot
	ResponseBytes           int64
}

func (e statusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("edge routes returned status %d", e.StatusCode)
	}
	return fmt.Sprintf("edge routes returned status %d: %s", e.StatusCode, e.Body)
}

func NewService(cfg config.EdgeConfig, logger *log.Logger) *Service {
	if logger == nil {
		logger = log.Default()
	}
	timeout := cfg.HTTPTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	service := &Service{
		Config: cfg,
		HTTPClient: &http.Client{
			Timeout: timeout,
		},
		Logger:                   logger,
		caddyWarmup:              warmupCaddyTLS,
		cacheWarmupClientFactory: newEdgeCacheWarmupClient,
		proxyBase:                newDefaultEdgeProxyTransport(),
		proxyTransports:          map[string]*http.Transport{},
		proxyTransportActiveKeys: map[string]struct{}{},
		bodyBuffer:               newEdgeRequestBodyBufferManager(cfg),
		requestBodyPolicyGuards:  map[string]*edgeRequestBodyPolicyGuard{},
		walActionLast:            map[string]time.Time{},
		snapshot: Status{
			Status:      "unhealthy",
			EdgeID:      strings.TrimSpace(cfg.EdgeID),
			EdgeGroupID: strings.TrimSpace(cfg.EdgeGroupID),
			CachePath:   strings.TrimSpace(cfg.CachePath),
		},
	}
	service.snapshot.CaddyEnabled = cfg.CaddyEnabled
	service.snapshot.CaddyListenAddr = strings.TrimSpace(cfg.CaddyListenAddr)
	service.snapshot.CaddyTLSMode = strings.TrimSpace(cfg.CaddyTLSMode)
	return service
}

func (s *Service) Run(ctx context.Context) error {
	if s.Logger == nil {
		s.Logger = log.Default()
	}
	if s.HTTPClient == nil {
		s.HTTPClient = &http.Client{Timeout: s.Config.HTTPTimeout}
	}
	if s.proxyBase == nil {
		s.proxyBase = newDefaultEdgeProxyTransport()
	}
	defer s.closeEdgeProxyTransports()
	if err := s.RefreshDesiredState(ctx); err != nil && s.Logger != nil {
		s.Logger.Printf("edge desired state refresh failed on startup: %s", s.redact(err.Error()))
	}
	if err := s.validateConfig(); err != nil {
		return err
	}

	if err := s.LoadCache(); err != nil {
		s.Logger.Printf("edge route cache unavailable: %v", err)
	}

	proxyShutdown, err := s.startProxyServer()
	if err != nil {
		return err
	}
	if proxyShutdown != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := proxyShutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.Logger.Printf("edge data proxy shutdown failed: %v", err)
			}
		}()
	}

	shutdown, err := s.startHTTPServer()
	if err != nil {
		return err
	}
	if shutdown != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.Logger.Printf("edge health server shutdown failed: %v", err)
			}
		}()
	}

	if err := s.applyCurrentCaddyConfig(ctx); err != nil {
		s.Logger.Printf("edge caddy config apply failed on startup: %v", err)
	}

	s.Logger.Printf("fugue-edge shadow started; api=%s edge_id=%s edge_group_id=%s cache=%s listen=%s interval=%s caddy_enabled=%t caddy_listen=%s caddy_tls_mode=%s proxy_listen=%s", safeBaseURL(s.Config.APIURL), s.Config.EdgeID, s.Config.EdgeGroupID, s.Config.CachePath, s.Config.ListenAddr, s.syncInterval(), s.Config.CaddyEnabled, s.Config.CaddyListenAddr, s.normalizedCaddyTLSMode(), s.Config.CaddyProxyListenAddr)
	_ = s.SyncOnce(ctx)
	s.startHeartbeatLoop(ctx)

	ticker := time.NewTicker(s.syncInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = s.SyncOnce(ctx)
		}
	}
}

func (s *Service) SyncOnce(ctx context.Context) (err error) {
	started := time.Now()
	result := "error"
	defer func() {
		duration := time.Since(started)
		s.recordSyncAttempt(result, duration)
		switch result {
		case "success":
			status := s.Status()
			s.logSyncSuccess(status.BundleVersion, status.RouteCount, status.TLSAllowlistCount, duration)
		case "not_modified":
			status := s.Status()
			s.logSyncNotModified(status.BundleVersion, duration)
		default:
			if err != nil {
				s.logSyncFailure(err)
				s.recordEdgeServeLKGWAL("control_plane_sync_failed", err)
				s.retryCurrentCaddyConfig(ctx, "route sync failed")
			}
		}
	}()

	if desiredErr := s.RefreshDesiredState(ctx); desiredErr != nil {
		if strings.TrimSpace(s.Config.EdgeGroupID) == "" {
			err = desiredErr
			s.recordSyncError(err)
			return err
		}
		if s.Logger != nil {
			s.Logger.Printf("edge desired state refresh failed; continuing with current state: %s", s.redact(desiredErr.Error()))
		}
	}
	if err := s.validateConfig(); err != nil {
		s.recordSyncError(err)
		return err
	}
	req, err := s.newRoutesRequest(ctx)
	if err != nil {
		s.recordSyncError(err)
		return err
	}

	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		err = fmt.Errorf("fetch edge route bundle: %s", s.redact(err.Error()))
		s.recordSyncError(err)
		return err
	}
	defer resp.Body.Close()

	now := time.Now().UTC()
	switch resp.StatusCode {
	case http.StatusOK:
		var bundle model.EdgeRouteBundle
		if err := json.NewDecoder(resp.Body).Decode(&bundle); err != nil {
			err = fmt.Errorf("decode edge route bundle: %w", err)
			s.recordSyncError(err)
			return err
		}
		if err := s.verifyBundle(bundle, now); err != nil {
			if fallbackErr := s.LoadPreviousCache(); fallbackErr != nil && s.Logger != nil {
				s.Logger.Printf("edge route previous cache fallback failed: %v", fallbackErr)
			}
			err = fmt.Errorf("verify edge route bundle: %w", err)
			s.recordSyncError(err)
			return err
		}
		etag := strings.TrimSpace(resp.Header.Get("ETag"))
		if etag == "" {
			etag = quoteETag(bundle.Version)
		}
		if err := s.applyCaddyConfig(ctx, bundle); err != nil {
			s.recordEdgeCaddyReloadWAL("bundle_sync", err)
			err = fmt.Errorf("apply caddy config: %w", err)
			s.recordSyncError(err)
			return err
		}
		if err := s.writeCache(cacheFile{
			Version:  cacheFileVersion,
			ETag:     etag,
			CachedAt: now,
			Bundle:   bundle,
		}); err != nil {
			s.recordSyncError(err)
			return err
		}
		s.recordSyncSuccess(bundle, etag, now, false)
		result = "success"
		return nil
	case http.StatusNotModified:
		if !s.hasBundle() {
			err := fmt.Errorf("edge routes returned 304 without a cached bundle")
			s.recordSyncError(err)
			return err
		}
		s.recordNotModified(now)
		if err := s.applyCurrentCaddyConfig(ctx); err != nil {
			s.recordEdgeCaddyReloadWAL("not_modified", err)
			if s.Logger != nil {
				status := s.Status()
				s.Logger.Printf("edge caddy config apply failed; version=%s error=%s", status.BundleVersion, s.redact(err.Error()))
			}
		}
		result = "not_modified"
		return nil
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		err := statusError{
			StatusCode: resp.StatusCode,
			Body:       s.redact(strings.TrimSpace(string(body))),
		}
		s.recordSyncError(err)
		return err
	}
}

func (s *Service) retryCurrentCaddyConfig(ctx context.Context, reason string) {
	if !s.Config.CaddyEnabled || !s.hasBundle() {
		return
	}
	if err := s.applyCurrentCaddyConfig(ctx); err != nil {
		s.recordEdgeCaddyReloadWAL(reason, err)
		if s.Logger != nil {
			status := s.Status()
			s.Logger.Printf("edge caddy cached config reapply failed; reason=%s version=%s error=%s", strings.TrimSpace(reason), status.BundleVersion, s.redact(err.Error()))
		}
		return
	}
	s.recordEdgeCaddyReloadWAL(reason, nil)
}

func (s *Service) LoadCache() error {
	path := strings.TrimSpace(s.Config.CachePath)
	if path == "" {
		s.recordCacheLoad("miss")
		s.logCacheMiss("path not configured")
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			s.recordCacheLoad("miss")
			s.logCacheMiss(path)
			return nil
		}
		err = fmt.Errorf("read edge route cache: %w", err)
		s.recordCacheLoad("error")
		return err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		s.recordCacheLoad("miss")
		s.logCacheMiss(path)
		return nil
	}
	var cached cacheFile
	if err := json.Unmarshal(data, &cached); err != nil {
		err = fmt.Errorf("decode edge route cache: %w", err)
		s.recordCacheLoad("error")
		if fallbackErr := s.LoadPreviousCache(); fallbackErr == nil {
			s.recordCacheCorruptGeneration("unknown")
			return nil
		}
		return err
	}
	if cached.Bundle.Version == "" {
		err := fmt.Errorf("edge route cache missing bundle version")
		s.recordCacheLoad("error")
		if fallbackErr := s.LoadPreviousCache(); fallbackErr == nil {
			s.recordCacheCorruptGeneration(edgeCacheGeneration(cached.Bundle))
			return nil
		}
		return err
	}
	if err := s.verifyCachedBundle(cached.Bundle, time.Now().UTC()); err != nil {
		err = fmt.Errorf("verify edge route cache: %w", err)
		s.recordCacheLoad("error")
		if fallbackErr := s.LoadPreviousCache(); fallbackErr == nil {
			s.recordCacheCorruptGeneration(edgeCacheGeneration(cached.Bundle))
			return nil
		}
		return err
	}
	s.recordCacheLoaded(cached)
	s.recordCacheLoad("success")
	s.logCacheLoaded(cached)
	s.recordEdgeServeLKGWAL("load_cache", nil)
	return nil
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /edge/bundle", s.handleBundle)
	mux.HandleFunc("GET /edge/tls/ask", s.handleTLSAsk)
	mux.HandleFunc("GET /edge/request-body-buffers", s.handleRequestBodyBuffers)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return mux
}

func (s *Service) ProxyHandler() http.Handler {
	return http.HandlerFunc(s.handleProxy)
}

func (s *Service) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

func (s *Service) Bundle() (model.EdgeRouteBundle, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bundle == nil {
		return model.EdgeRouteBundle{}, false
	}
	return *s.bundle, true
}

func (s *Service) routeForHost(host string) (model.EdgeRouteBinding, bool, bool) {
	host = normalizeRouteHost(host)
	if host == "" {
		return model.EdgeRouteBinding{}, false, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bundle == nil {
		return model.EdgeRouteBinding{}, false, false
	}
	localEdgeGroupID := strings.TrimSpace(s.Config.EdgeGroupID)
	var fallbackActive model.EdgeRouteBinding
	var fallbackInactive model.EdgeRouteBinding
	for _, route := range s.bundle.Routes {
		if normalizeRouteHost(route.Hostname) != host {
			continue
		}
		if !s.routeAllowedForThisEdge(route) {
			continue
		}
		if routeMatchesCurrentEdgeGroup(route, localEdgeGroupID) {
			if strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive) {
				return route, true, false
			}
			if fallbackInactive.Hostname == "" {
				fallbackInactive = route
			}
			continue
		}
		if strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive) {
			if fallbackActive.Hostname == "" {
				fallbackActive = route
			}
			continue
		}
		if fallbackInactive.Hostname == "" {
			fallbackInactive = route
		}
	}
	if fallbackActive.Hostname != "" {
		return fallbackActive, true, true
	}
	if fallbackInactive.Hostname != "" {
		return fallbackInactive, true, true
	}
	return model.EdgeRouteBinding{}, false, false
}

func (s *Service) routeForRequest(host, requestPath string) (model.EdgeRouteBinding, bool, bool) {
	host = normalizeRouteHost(host)
	requestPath = model.NormalizeAppRoutePathPrefix(requestPath)
	if host == "" {
		return model.EdgeRouteBinding{}, false, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bundle == nil {
		return model.EdgeRouteBinding{}, false, false
	}

	localEdgeGroupID := strings.TrimSpace(s.Config.EdgeGroupID)
	bestPrefixLen := -1
	var currentActive model.EdgeRouteBinding
	var fallbackActive model.EdgeRouteBinding
	var inactive model.EdgeRouteBinding
	inactiveFallbackHit := false
	for _, route := range s.bundle.Routes {
		if normalizeRouteHost(route.Hostname) != host {
			continue
		}
		if !s.routeAllowedForThisEdge(route) {
			continue
		}
		prefix := model.NormalizeAppRoutePathPrefix(route.PathPrefix)
		if !routePathPrefixMatches(prefix, requestPath) {
			continue
		}
		prefixLen := len(prefix)
		if prefixLen > bestPrefixLen {
			bestPrefixLen = prefixLen
			currentActive = model.EdgeRouteBinding{}
			fallbackActive = model.EdgeRouteBinding{}
			inactive = model.EdgeRouteBinding{}
			inactiveFallbackHit = false
		}
		if prefixLen < bestPrefixLen {
			continue
		}

		currentEdgeGroup := routeMatchesCurrentEdgeGroup(route, localEdgeGroupID)
		active := strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive)
		switch {
		case currentEdgeGroup && active:
			if currentActive.Hostname == "" {
				currentActive = route
			}
		case active:
			if fallbackActive.Hostname == "" {
				fallbackActive = route
			}
		case inactive.Hostname == "":
			inactive = route
			inactiveFallbackHit = true
		}
	}
	if currentActive.Hostname != "" {
		return currentActive, true, false
	}
	if fallbackActive.Hostname != "" {
		return fallbackActive, true, true
	}
	if inactive.Hostname != "" {
		return inactive, true, inactiveFallbackHit
	}
	return model.EdgeRouteBinding{}, false, false
}

func selectWeightedEdgeRouteUpstream(r *http.Request, route model.EdgeRouteBinding, host, traceID, edgeRequestID string) (model.EdgeRouteBinding, model.EdgeRouteUpstream) {
	if len(route.Upstreams) == 0 {
		return route, model.EdgeRouteUpstream{}
	}
	candidates := make([]model.EdgeRouteUpstream, 0, len(route.Upstreams))
	weighted := make([]weightedselector.Candidate, 0, len(route.Upstreams))
	for _, upstream := range route.Upstreams {
		candidates = append(candidates, upstream)
		active := strings.TrimSpace(upstream.UpstreamURL) != "" &&
			upstream.Weight > 0 &&
			(strings.TrimSpace(upstream.Status) == "" || strings.EqualFold(strings.TrimSpace(upstream.Status), model.EdgeRouteStatusActive))
		weighted = append(weighted, weightedselector.Candidate{
			ID:     firstNonEmpty(upstream.ReleaseID, upstream.Role, upstream.UpstreamURL),
			Weight: upstream.Weight,
			Active: active,
		})
	}
	if len(candidates) == 0 {
		return route, model.EdgeRouteUpstream{}
	}
	stickinessKey := weightedReleaseStickinessKey(r, host, route, traceID, edgeRequestID)
	selection, ok := weightedselector.Select(weighted, stickinessKey)
	if !ok || selection.Index < 0 || selection.Index >= len(candidates) {
		return route, model.EdgeRouteUpstream{}
	}
	selected := candidates[selection.Index]
	return edgeRouteWithUpstream(route, selected), selected
}

func edgeRouteWithUpstream(route model.EdgeRouteBinding, selected model.EdgeRouteUpstream) model.EdgeRouteBinding {
	route.UpstreamURL = selected.UpstreamURL
	route.UpstreamKind = firstNonEmpty(selected.UpstreamKind, route.UpstreamKind)
	route.UpstreamScope = firstNonEmpty(selected.UpstreamScope, route.UpstreamScope)
	route.RuntimeID = firstNonEmpty(selected.RuntimeID, route.RuntimeID)
	route.ServicePort = firstNonEmptyInt(selected.ServicePort, route.ServicePort)
	route.DeploymentGeneration = firstNonEmpty(selected.DeploymentGeneration, route.DeploymentGeneration)
	return route
}

func weightedReleaseStickinessKey(r *http.Request, host string, route model.EdgeRouteBinding, traceID, edgeRequestID string) string {
	parts := []string{
		normalizeRouteHost(host),
		model.NormalizeAppRoutePathPrefix(route.PathPrefix),
		strings.TrimSpace(route.AppID),
	}
	identity := ""
	if r != nil {
		if cookie, err := r.Cookie("Fugue-Release-Stickiness"); err == nil {
			identity = strings.TrimSpace(cookie.Value)
		}
		identity = firstNonEmpty(identity,
			strings.TrimSpace(r.Header.Get("X-Fugue-Release-Stickiness")),
			strings.TrimSpace(r.Header.Get("X-API-Key")),
			strings.TrimSpace(r.Header.Get("Authorization")),
		)
	}
	if identity == "" {
		identity = firstNonEmpty(strings.TrimSpace(traceID), strings.TrimSpace(edgeRequestID), time.Now().UTC().Format("200601021504"))
	}
	parts = append(parts, identity)
	return strings.Join(parts, "\x00")
}

func weightedReleaseBucket(key string, total int) int {
	return weightedselector.Bucket(key, total)
}

func (s *Service) hasBundle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bundle != nil
}

func (s *Service) handleHealthz(w http.ResponseWriter, r *http.Request) {
	status := s.Status()
	code := http.StatusOK
	if !status.Healthy {
		code = http.StatusServiceUnavailable
	}
	httpx.WriteJSON(w, code, status)
}

func (s *Service) handleBundle(w http.ResponseWriter, r *http.Request) {
	bundle, ok := s.Bundle()
	if !ok {
		httpx.WriteError(w, http.StatusServiceUnavailable, "edge route bundle is unavailable")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, bundle)
}

func (s *Service) handleTLSAsk(w http.ResponseWriter, r *http.Request) {
	host := normalizeRouteHost(r.URL.Query().Get("domain"))
	if host == "" {
		http.Error(w, "domain is required", http.StatusBadRequest)
		return
	}
	route, ok, _ := s.routeForHost(host)
	if !ok {
		http.Error(w, "domain is not in the current route bundle", http.StatusForbidden)
		return
	}
	if !s.routeCanIssueTLS(route) {
		http.Error(w, "route is not active", http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) routeCanIssueTLS(route model.EdgeRouteBinding) bool {
	if strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive) {
		return true
	}
	// Custom domains need a certificate before their route can become active.
	// Allow on-demand TLS permission for those routes while they are still
	// warming up so certificate issuance can complete and the readiness report
	// can advance the route to active.
	if strings.EqualFold(strings.TrimSpace(route.RouteKind), model.EdgeRouteKindCustomDomain) &&
		!strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusDisabled) {
		return true
	}
	return false
}

func (s *Service) handleProxy(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&s.activeProxyRequests, 1)
	defer atomic.AddInt64(&s.activeProxyRequests, -1)
	startedAt := time.Now()
	host := normalizeRouteHost(firstNonEmptyHeader(r, "X-Fugue-Edge-Route-Host", r.Host))
	route, ok, fallbackHit := s.routeForRequest(host, r.URL.Path)
	edgeRequestID := edgeRequestIDForProxy(r)
	traceID := edgeTraceIDForProxy(r)
	selectedRoute := route
	selectedUpstream := model.EdgeRouteUpstream{}
	if ok {
		selectedRoute, selectedUpstream = selectWeightedEdgeRouteUpstream(r, route, host, traceID, edgeRequestID)
	}
	if edgeRequestID != "" {
		w.Header().Set(edgeRequestIDHeader, edgeRequestID)
	}
	if traceID != "" {
		w.Header().Set(edgeTraceIDHeader, traceID)
	}
	observed := edgeProxyObservation{
		ReceivedAt:       startedAt.UTC(),
		Host:             host,
		Route:            selectedRoute,
		ReleaseID:        strings.TrimSpace(selectedUpstream.ReleaseID),
		ReleaseRole:      strings.TrimSpace(selectedUpstream.Role),
		TrafficWeight:    selectedUpstream.Weight,
		Method:           r.Method,
		Path:             safeProxyLogPath(r),
		TraceID:          traceID,
		RequestID:        edgeRequestIDFromRequest(r),
		EdgeRequestID:    edgeRequestID,
		Protocol:         strings.TrimSpace(r.Proto),
		ClientIP:         edgeClientIPFromRequest(r),
		ClientRemoteAddr: edgeClientRemoteAddrFromRequest(r),
		ClientCountry:    edgeClientCountryFromRequest(r),
		ClientRegion:     edgeClientRegionFromRequest(r),
		ClientASN:        edgeClientASNFromRequest(r),
		FallbackHit:      fallbackHit,
		WebSocket:        edgeRequestIsWebSocket(r),
		SSE:              edgeRequestWantsSSE(r),
		Upload:           edgeRequestHasUpload(r),
	}
	if r.ContentLength > 0 {
		observed.RequestBytes = r.ContentLength
	}
	observed.Streaming = observed.WebSocket || observed.SSE
	cacheDecision := s.edgeCacheDecision(r, selectedRoute)
	if len(route.Upstreams) > 0 {
		cacheDecision.Enabled = false
		cacheDecision.Status = edgeCacheStatusBypass
		cacheDecision.Reason = "weighted release route"
	}
	observed.CacheStatus = cacheDecision.Status
	observed.CachePolicyID = cacheDecision.PolicyID
	observed.CacheKeyHash = cacheDecision.KeyHash
	observed.AssetClass = cacheDecision.AssetClass
	defer func() {
		observed.Duration = time.Since(startedAt)
		s.recordProxyObservation(observed)
		s.logProxyObservation(observed)
	}()

	if !ok {
		observed.StatusCode = http.StatusNotFound
		http.Error(w, "edge route not found", http.StatusNotFound)
		return
	}
	if !strings.EqualFold(strings.TrimSpace(selectedRoute.Status), model.EdgeRouteStatusActive) {
		observed.StatusCode = http.StatusServiceUnavailable
		message := strings.TrimSpace(selectedRoute.StatusReason)
		if message == "" {
			message = "edge route is not active"
		}
		http.Error(w, message, http.StatusServiceUnavailable)
		return
	}
	target, err := url.Parse(strings.TrimSpace(selectedRoute.UpstreamURL))
	if err != nil || target.Scheme == "" || target.Host == "" {
		observed.StatusCode = http.StatusServiceUnavailable
		http.Error(w, "edge route upstream is unavailable", http.StatusServiceUnavailable)
		return
	}
	var releaseRequestBodyPolicy func()
	r, releaseRequestBodyPolicy, handled, policyStatus := s.applyEdgeRequestBodyPolicy(w, r, selectedRoute)
	if handled {
		observed.StatusCode = policyStatus
		return
	}
	if releaseRequestBodyPolicy != nil {
		defer releaseRequestBodyPolicy()
	}
	observeEdgeProxyRequestBody(r, &observed)
	observedWriter := newEdgeProxyObservationResponseWriter(w, startedAt, &observed)
	if cacheDecision.Enabled {
		observedWriter.cacheDecision = &cacheDecision
		maxBytes := s.Config.AssetCacheMaxBytes
		if maxBytes <= 0 {
			maxBytes = 32 * 1024 * 1024
		}
		observedWriter.maxBytes = int64(maxBytes)
	}
	observed.Proxied = true
	if cacheDecision.Enabled {
		cacheLookupStarted := time.Now()
		entry, ok := s.edgeCacheLoad(cacheDecision)
		observed.CacheLookup = time.Since(cacheLookupStarted)
		if ok {
			if served, status, cacheStatus := s.edgeCacheServeIfFresh(observedWriter, cacheDecision, entry, edgeProxyServerTiming(observed, false)); served {
				if cacheStatus == edgeCacheStatusStale {
					s.edgeCacheRevalidateAsync(r, target, selectedRoute, cacheDecision, host)
				}
				observed.Proxied = false
				observed.StatusCode = status
				observed.CacheStatus = cacheStatus
				observed.ResponseBytes = entry.BodySize
				return
			}
		}
	}
	peerFallbackAllowed := s.peerFallbackAllowed(r, observed)
	bufferedBody := false
	if handled, status := s.bufferRequestBodyForOrigin(observedWriter, r, &observed); handled {
		observed.StatusCode = status
		return
	} else if observed.RequestBodyBuffered {
		bufferedBody = true
	}
	if bufferedBody && r.Body != nil {
		defer r.Body.Close()
	}
	proxy := s.newEdgeReverseProxy(host, target, selectedRoute, &observed, peerFallbackAllowed, &cacheDecision)
	proxy.ServeHTTP(observedWriter, r)
	if observed.ClientCanceled {
		if observed.StatusCode == 0 {
			observed.StatusCode = edgeStatusClientClosedRequest
		}
		return
	}
	if observed.UpstreamError != "" && !observedWriter.wroteHeader && peerFallbackAllowed {
		if s.proxyPeerFallback(w, r, host, selectedRoute, &observed) {
			return
		}
	}
	if observed.UpstreamError != "" && !observedWriter.wroteHeader {
		http.Error(w, "upstream app is unavailable", http.StatusBadGateway)
		observed.StatusCode = http.StatusBadGateway
		return
	}
	observed.StatusCode = observedWriter.statusCode()
	observed.ResponseBytes = observedWriter.bytesWritten
	if cacheDecision.Enabled {
		if s.edgeCacheShouldStore(cacheDecision, observedWriter) {
			if err := s.edgeCacheStore(cacheDecision, edgeHTTPCacheEntry{
				Header:     cloneHTTPHeader(observedWriter.headerSnapshot),
				StatusCode: observedWriter.statusCode(),
				Body:       append([]byte(nil), observedWriter.body...),
				BodySize:   observedWriter.bytesWritten,
			}); err != nil && s.Logger != nil {
				s.Logger.Printf("edge http cache store failed; host=%s policy=%s key=%s error=%v", host, cacheDecision.PolicyID, cacheDecision.KeyHash, err)
				observed.CacheStatus = edgeCacheStatusError
			} else if observed.CacheStatus != edgeCacheStatusStale {
				observed.CacheStatus = edgeCacheStatusMiss
			}
		} else if observed.CacheStatus == "" || observed.CacheStatus == edgeCacheStatusMiss {
			observed.CacheStatus = edgeCacheStatusBypass
		}
	}
	if !observedWriter.wroteHeader && observed.WebSocket && observed.UpstreamError == "" {
		observed.StatusCode = http.StatusSwitchingProtocols
	}
}

func (s *Service) edgeCacheRevalidateAsync(r *http.Request, target *url.URL, route model.EdgeRouteBinding, decision edgeHTTPCacheDecision, host string) {
	if s == nil || r == nil || target == nil || strings.TrimSpace(decision.KeyHash) == "" {
		return
	}
	if !s.startEdgeCacheRevalidation(decision.KeyHash) {
		return
	}
	targetCopy := *target
	routeCopy := route
	decisionCopy := decision
	host = strings.TrimSpace(host)
	go func() {
		defer s.finishEdgeCacheRevalidation(decisionCopy.KeyHash)
		if err := s.edgeCacheRevalidate(r, &targetCopy, routeCopy, decisionCopy, host); err != nil && s.Logger != nil {
			s.Logger.Printf("edge http cache revalidation failed; host=%s policy=%s key=%s error=%v", host, decisionCopy.PolicyID, decisionCopy.KeyHash, err)
		}
	}()
}

func (s *Service) edgeCacheRevalidate(r *http.Request, target *url.URL, route model.EdgeRouteBinding, decision edgeHTTPCacheDecision, host string) error {
	timeout := s.edgeCacheRevalidationTimeout()
	ctx := context.Background()
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	req := r.Clone(ctx)
	req.Body = nil
	req.GetBody = nil
	req.Header = r.Header.Clone()
	req.Header.Del("If-None-Match")
	req.Header.Del("If-Modified-Since")
	req.Header.Del("If-Range")
	req.Header.Del("Range")

	observed := edgeProxyObservation{
		ReceivedAt:    time.Now().UTC(),
		Host:          host,
		Route:         route,
		Method:        req.Method,
		Path:          safeProxyLogPath(req),
		TraceID:       edgeTraceIDForProxy(req),
		RequestID:     edgeRequestIDFromRequest(req),
		CacheStatus:   edgeCacheStatusMiss,
		CachePolicyID: decision.PolicyID,
		CacheKeyHash:  decision.KeyHash,
		AssetClass:    decision.AssetClass,
	}
	writer := newEdgeProxyObservationResponseWriter(newEdgeCacheRevalidationResponseWriter(), time.Now(), &observed)
	maxBytes := s.Config.AssetCacheMaxBytes
	if maxBytes <= 0 {
		maxBytes = 32 * 1024 * 1024
	}
	writer.maxBytes = int64(maxBytes)
	cacheDecision := decision
	cacheDecision.Status = edgeCacheStatusMiss
	cacheDecision.Cacheable = true
	proxy := s.newEdgeReverseProxy(host, target, route, &observed, false, &cacheDecision)
	proxy.ServeHTTP(writer, req)
	if observed.UpstreamError != "" {
		return fmt.Errorf("origin request failed: %s", observed.UpstreamError)
	}
	if !writer.wroteHeader {
		return fmt.Errorf("origin response did not write headers")
	}
	if !s.edgeCacheShouldStore(cacheDecision, writer) {
		return fmt.Errorf("origin response is not cacheable: status=%d content_type=%q cache_control=%q vary=%q set_cookie=%t", writer.statusCode(), cacheDecision.OriginContentType, cacheDecision.OriginCacheControl, strings.Join(cacheDecision.OriginVary, ","), cacheDecision.OriginSetCookie)
	}
	return s.edgeCacheStore(cacheDecision, edgeHTTPCacheEntry{
		Header:     cloneHTTPHeader(writer.headerSnapshot),
		StatusCode: writer.statusCode(),
		Body:       append([]byte(nil), writer.body...),
		BodySize:   writer.bytesWritten,
	})
}

func (s *Service) edgeCacheRevalidationTimeout() time.Duration {
	if s == nil {
		return 10 * time.Second
	}
	if s.Config.HTTPTimeout > 0 {
		return s.Config.HTTPTimeout
	}
	if s.HTTPClient != nil && s.HTTPClient.Timeout > 0 {
		return s.HTTPClient.Timeout
	}
	return 10 * time.Second
}

func (s *Service) startEdgeCacheRevalidation(key string) bool {
	key = strings.TrimSpace(key)
	if s == nil || key == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cacheRevalidating == nil {
		s.cacheRevalidating = make(map[string]struct{})
	}
	if _, ok := s.cacheRevalidating[key]; ok {
		return false
	}
	s.cacheRevalidating[key] = struct{}{}
	return true
}

func (s *Service) finishEdgeCacheRevalidation(key string) {
	key = strings.TrimSpace(key)
	if s == nil || key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cacheRevalidating, key)
}

func (s *Service) newEdgeReverseProxy(host string, target *url.URL, route model.EdgeRouteBinding, observed *edgeProxyObservation, suppressErrorResponse bool, cacheDecision *edgeHTTPCacheDecision) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(req *httputil.ProxyRequest) {
			req.SetURL(target)
			setEdgeXForwarded(req)
			req.Out.Header.Del(edgeClientRemoteAddrHeader)
			req.Out.Host = target.Host
			req.Out.Header.Set("X-Forwarded-Host", host)
			req.Out.Header.Set("X-Fugue-Edge-Route", strings.TrimSpace(route.Hostname))
			req.Out.Header.Set("X-Fugue-Edge-App-ID", strings.TrimSpace(route.AppID))
			if observed != nil {
				if releaseID := strings.TrimSpace(observed.ReleaseID); releaseID != "" {
					req.Out.Header.Set("X-Fugue-Release-ID", releaseID)
				}
				if releaseRole := strings.TrimSpace(observed.ReleaseRole); releaseRole != "" {
					req.Out.Header.Set("X-Fugue-Release-Role", releaseRole)
				}
			}
			if observed != nil && strings.TrimSpace(observed.EdgeRequestID) != "" {
				req.Out.Header.Set(edgeRequestIDHeader, strings.TrimSpace(observed.EdgeRequestID))
			}
			if observed != nil && strings.TrimSpace(observed.TraceID) != "" {
				traceID := strings.TrimSpace(observed.TraceID)
				req.Out.Header.Set(edgeTraceIDHeader, traceID)
				if strings.TrimSpace(req.Out.Header.Get("traceparent")) == "" {
					req.Out.Header.Set("traceparent", edgeTraceparentForProxy(traceID, observed.EdgeRequestID))
				}
			}
			if observed != nil && observed.Streaming && observed.Upload {
				req.Out.Close = true
			}
		},
		Transport: s.newEdgeProxyTransport(observed),
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
			if observed != nil {
				observed.UpstreamError = strings.TrimSpace(proxyErr.Error())
				if edgeRequestBodyPolicyErrorIsTooLarge(proxyErr) {
					observed.StatusCode = http.StatusRequestEntityTooLarge
					http.Error(rw, "request body too large", http.StatusRequestEntityTooLarge)
					return
				}
				if edgeRequestBodyPolicyErrorIsTimeout(req, proxyErr) {
					observed.StatusCode = http.StatusRequestTimeout
					http.Error(rw, "request timeout", http.StatusRequestTimeout)
					return
				}
				if edgeProxyErrorIsClientCanceled(req, proxyErr) {
					observed.ClientCanceled = true
					observed.StatusCode = edgeStatusClientClosedRequest
					if !suppressErrorResponse {
						rw.WriteHeader(edgeStatusClientClosedRequest)
					}
					return
				}
			}
			if suppressErrorResponse {
				return
			}
			http.Error(rw, "upstream app is unavailable", http.StatusBadGateway)
		},
		ModifyResponse: func(resp *http.Response) error {
			if observed != nil && resp != nil {
				if strings.TrimSpace(observed.RequestID) == "" {
					observed.RequestID = edgeRequestIDFromHeader(resp.Header)
				}
				addEdgeServerTiming(resp.Header, edgeProxyServerTiming(*observed, true))
			}
			if resp == nil || cacheDecision == nil || !cacheDecision.Enabled {
				return nil
			}
			cacheDecision.observeOriginResponse(resp)
			if !cacheDecision.originResponseAllowsStore(resp.StatusCode) {
				cacheDecision.Cacheable = false
				cacheDecision.Status = edgeCacheStatusBypass
				resp.Header.Set("X-Fugue-Cache", cacheDecision.Status)
				return nil
			}
			resp.Header.Set("X-Fugue-Cache", cacheDecision.Status)
			if control := strings.TrimSpace(cacheDecision.Policy.EdgeCacheControl); control != "" {
				resp.Header.Set("Cache-Control", control)
			}
			if resp.StatusCode == http.StatusNotModified {
				resp.Header.Set("X-Fugue-Cache", edgeCacheStatusRevalidated)
			}
			return nil
		},
	}
}

func edgeProxyErrorIsClientCanceled(req *http.Request, proxyErr error) bool {
	if errors.Is(proxyErr, context.Canceled) {
		return true
	}
	if req != nil && errors.Is(req.Context().Err(), context.Canceled) {
		return true
	}
	return false
}

type edgeProxyObservedRequestBody struct {
	io.ReadCloser
	observation *edgeProxyObservation
}

func observeEdgeProxyRequestBody(r *http.Request, observed *edgeProxyObservation) {
	if r == nil || r.Body == nil || observed == nil {
		return
	}
	r.Body = &edgeProxyObservedRequestBody{
		ReadCloser:  r.Body,
		observation: observed,
	}
}

func (s *Service) bufferRequestBodyForOrigin(w http.ResponseWriter, r *http.Request, observed *edgeProxyObservation) (bool, int) {
	if !s.requestBodyBufferEligible(r, observed) {
		return false, 0
	}
	manager := s.edgeRequestBodyBufferManager()
	maxBytes := s.requestBodyBufferMaxBytes(manager)
	if maxBytes <= 0 {
		return false, 0
	}
	if r.ContentLength > maxBytes {
		err := fmt.Errorf("request body exceeds edge buffer limit: content_length=%d max_bytes=%d", r.ContentLength, maxBytes)
		observed.RequestBodyBufferError = err.Error()
		observed.RequestBodyBufferPath = manager.path
		observed.RequestBodyBufferBudget, observed.RequestBodyBufferUsed, observed.RequestBodyBufferActive = manager.stats()
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return true, http.StatusRequestEntityTooLarge
	}
	reservationBytes := int64(1)
	if r.ContentLength > 0 {
		reservationBytes = r.ContentLength
	}
	reservation, err := manager.reserve(reservationBytes)
	if err != nil {
		observed.RequestBodyBufferError = err.Error()
		observed.RequestBodyBufferPath = manager.path
		observed.RequestBodyBufferBudget, observed.RequestBodyBufferUsed, observed.RequestBodyBufferActive = manager.stats()
		http.Error(w, "edge request body buffer unavailable", http.StatusServiceUnavailable)
		return true, http.StatusServiceUnavailable
	}
	keepReservation := false
	defer func() {
		if reservation != nil && !keepReservation {
			reservation.release()
		}
	}()
	started := time.Now()
	observed.RequestBodyBuffered = true
	observed.RequestBodyBufferPath = manager.path
	observed.RequestBodyBufferBudget, observed.RequestBodyBufferUsed, observed.RequestBodyBufferActive = manager.stats()
	if err := os.MkdirAll(observed.RequestBodyBufferPath, 0o700); err != nil {
		observed.RequestBodyBufferError = err.Error()
		http.Error(w, "edge request body buffer unavailable", http.StatusServiceUnavailable)
		return true, http.StatusServiceUnavailable
	}
	file, err := os.CreateTemp(observed.RequestBodyBufferPath, "edge-request-body-*")
	if err != nil {
		observed.RequestBodyBufferError = err.Error()
		http.Error(w, "edge request body buffer unavailable", http.StatusServiceUnavailable)
		return true, http.StatusServiceUnavailable
	}
	path := file.Name()
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(path)
	}
	bufferWriter := &edgeRequestBodyBufferWriter{
		file:        file,
		reservation: reservation,
		maxBytes:    maxBytes,
	}
	observed.EdgeProxyTCPInfo = edgeTCPInfoSnapshotFromContext(r.Context())
	activeID := s.startActiveRequestBodyBufferRead(*observed, r.ContentLength, started)
	stopProgress := s.startRequestBodyBufferProgressLogger(r.Context(), *observed, activeID)
	defer func() {
		if stopProgress != nil {
			stopProgress()
		}
		s.finishActiveRequestBodyBufferRead(activeID)
	}()
	copyResult, copyErr := s.copyRequestBodyToBuffer(r.Context(), bufferWriter, r.Body, observed, activeID, started)
	observed.EdgeProxyTCPInfo = edgeTCPInfoSnapshotFromContext(r.Context())
	s.updateActiveRequestBodyBufferTCPInfo(activeID, observed.EdgeProxyTCPInfo)
	written := copyResult.Written
	observed.RequestBodyBuffer = time.Since(started)
	observed.RequestBodyBufferBytes = written
	observed.BodyReadBlock = copyResult.BodyReadBlock
	observed.FileWrite = copyResult.FileWrite
	observed.FirstBodyByte = copyResult.FirstBodyByte
	observed.LastBodyByte = copyResult.LastBodyByte
	observed.MaxReadGap = copyResult.MaxReadGap
	observed.ReadCalls = copyResult.ReadCalls
	observed.AvgBPS = copyResult.AvgBPS
	observed.MinWindowBPS = copyResult.MinWindowBPS
	s.logRequestBodyBufferSlowIfNeeded(*observed, activeID)
	closeErr := r.Body.Close()
	var tooLargeErr edgeRequestBodyBufferTooLargeError
	if errors.As(copyErr, &tooLargeErr) {
		cleanup()
		err := fmt.Errorf("request body exceeds edge buffer limit: read_bytes=%d max_bytes=%d", tooLargeErr.readBytes, tooLargeErr.maxBytes)
		observed.RequestBodyBufferError = err.Error()
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return true, http.StatusRequestEntityTooLarge
	}
	if reservation != nil {
		reservation.resize(written)
		observed.RequestBodyBufferBudget, observed.RequestBodyBufferUsed, observed.RequestBodyBufferActive = manager.stats()
	}
	if copyErr != nil || closeErr != nil {
		err := copyErr
		if err == nil {
			err = closeErr
		}
		cleanup()
		observed.RequestBodyBufferError = err.Error()
		observed.ClientCanceled = true
		if w != nil {
			w.WriteHeader(edgeStatusClientClosedRequest)
		}
		return true, edgeStatusClientClosedRequest
	}
	if r.ContentLength >= 0 && written != r.ContentLength {
		cleanup()
		err := fmt.Errorf("request body length mismatch: read %d of %d bytes", written, r.ContentLength)
		observed.RequestBodyBufferError = err.Error()
		observed.ClientCanceled = true
		if w != nil {
			w.WriteHeader(edgeStatusClientClosedRequest)
		}
		return true, edgeStatusClientClosedRequest
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		cleanup()
		observed.RequestBodyBufferError = err.Error()
		http.Error(w, "edge request body buffer unavailable", http.StatusInternalServerError)
		return true, http.StatusInternalServerError
	}
	r.Body = &edgeBufferedRequestBody{
		file:        file,
		path:        path,
		reservation: reservation,
	}
	keepReservation = true
	r.ContentLength = written
	return false, 0
}

type edgeRequestBodyBufferCopyResult struct {
	Written       int64
	BytesRead     int64
	BodyReadBlock time.Duration
	FileWrite     time.Duration
	FirstBodyByte time.Duration
	LastBodyByte  time.Duration
	MaxReadGap    time.Duration
	ReadCalls     int64
	AvgBPS        int64
	MinWindowBPS  int64
}

func (s *Service) copyRequestBodyToBuffer(ctx context.Context, writer io.Writer, reader io.Reader, observed *edgeProxyObservation, activeID string, started time.Time) (edgeRequestBodyBufferCopyResult, error) {
	var result edgeRequestBodyBufferCopyResult
	if reader == nil {
		return result, nil
	}
	if writer == nil {
		return result, io.ErrClosedPipe
	}
	if started.IsZero() {
		started = time.Now()
	}
	progressEvery := s.requestBodyBufferProgressEvery()
	buf := make([]byte, edgeRequestBodyCopyBufferSize)
	var lastReadAt time.Time
	windowStarted := started
	var windowBytes int64
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			default:
			}
		}
		readStarted := time.Now()
		n, readErr := reader.Read(buf)
		readFinished := time.Now()
		result.ReadCalls++
		result.BodyReadBlock += readFinished.Sub(readStarted)
		if n > 0 {
			result.BytesRead += int64(n)
			if result.FirstBodyByte <= 0 {
				result.FirstBodyByte = readFinished.Sub(started)
			}
			if !lastReadAt.IsZero() {
				if gap := readFinished.Sub(lastReadAt); gap > result.MaxReadGap {
					result.MaxReadGap = gap
				}
			} else if result.FirstBodyByte > result.MaxReadGap {
				result.MaxReadGap = result.FirstBodyByte
			}
			lastReadAt = readFinished
			result.LastBodyByte = readFinished.Sub(started)
			windowBytes += int64(n)

			writeStarted := time.Now()
			written, writeErr := writer.Write(buf[:n])
			result.FileWrite += time.Since(writeStarted)
			result.Written += int64(written)
			if written != n && writeErr == nil {
				writeErr = io.ErrShortWrite
			}
			if elapsed := time.Since(started); elapsed > 0 {
				result.AvgBPS = int64(float64(result.BytesRead) / elapsed.Seconds())
			}
			if windowElapsed := readFinished.Sub(windowStarted); windowElapsed >= progressEvery {
				windowBPS := int64(float64(windowBytes) / windowElapsed.Seconds())
				if result.MinWindowBPS <= 0 || windowBPS < result.MinWindowBPS {
					result.MinWindowBPS = windowBPS
				}
				windowStarted = readFinished
				windowBytes = 0
			}
			s.updateActiveRequestBodyBufferRead(activeID, result, lastReadAt)
			if writeErr != nil {
				return result, writeErr
			}
		}
		if errors.Is(readErr, io.EOF) {
			if windowBytes > 0 {
				if windowElapsed := time.Since(windowStarted); windowElapsed > 0 {
					windowBPS := int64(float64(windowBytes) / windowElapsed.Seconds())
					if result.MinWindowBPS <= 0 || windowBPS < result.MinWindowBPS {
						result.MinWindowBPS = windowBPS
					}
				}
			}
			if result.MinWindowBPS <= 0 {
				result.MinWindowBPS = result.AvgBPS
			}
			if snapshot, ok := s.activeRequestBodyBufferReadSnapshot(activeID, time.Now().UTC()); ok && snapshot.MinWindowObserved {
				result.MinWindowBPS = snapshot.MinWindowBPS
			}
			s.updateActiveRequestBodyBufferRead(activeID, result, lastReadAt)
			if observed != nil {
				observed.BodyReadBlock = result.BodyReadBlock
				observed.FileWrite = result.FileWrite
				observed.FirstBodyByte = result.FirstBodyByte
				observed.LastBodyByte = result.LastBodyByte
				observed.MaxReadGap = result.MaxReadGap
				observed.ReadCalls = result.ReadCalls
				observed.AvgBPS = result.AvgBPS
				observed.MinWindowBPS = result.MinWindowBPS
			}
			return result, nil
		}
		if readErr != nil {
			return result, readErr
		}
	}
}

func (s *Service) requestBodyBufferSlowThreshold() time.Duration {
	if s != nil && s.Config.RequestBodyBufferSlowThreshold > 0 {
		return s.Config.RequestBodyBufferSlowThreshold
	}
	return 30 * time.Second
}

func (s *Service) requestBodyBufferProgressEvery() time.Duration {
	if s != nil && s.Config.RequestBodyBufferProgressEvery > 0 {
		return s.Config.RequestBodyBufferProgressEvery
	}
	return 10 * time.Second
}

func (s *Service) requestBodyBufferMaxBytes(manager *edgeRequestBodyBufferManager) int64 {
	if s == nil {
		return 0
	}
	baseMaxBytes := int64(s.Config.RequestBodyBufferMaxBytes)
	if baseMaxBytes <= 0 {
		return 0
	}
	if manager == nil {
		return baseMaxBytes
	}
	ratio := s.Config.RequestBodyBufferMaxBudgetRatio
	if ratio <= 0 {
		return baseMaxBytes
	}
	if ratio > 1 {
		ratio = 1
	}
	budget := manager.currentBudget()
	if budget <= 0 {
		return baseMaxBytes
	}
	dynamicMaxBytes := int64(float64(budget) * ratio)
	if dynamicMaxBytes > baseMaxBytes {
		return dynamicMaxBytes
	}
	return baseMaxBytes
}

func (s *Service) requestBodyBufferEligible(r *http.Request, observed *edgeProxyObservation) bool {
	if s == nil || r == nil || r.Body == nil || observed == nil {
		return false
	}
	if s.Config.RequestBodyBufferMaxBytes <= 0 || observed.WebSocket || !observed.SSE || !observed.Upload {
		return false
	}
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
	default:
		return false
	}
	if r.ContentLength == 0 {
		return false
	}
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if contentType == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType = strings.TrimSpace(strings.Split(contentType, ";")[0])
	}
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	return mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
}

func (b *edgeProxyObservedRequestBody) Read(data []byte) (int, error) {
	n, err := b.ReadCloser.Read(data)
	if b.observation != nil {
		if n > 0 {
			b.observation.RequestBodyReadBytes += int64(n)
		}
		if errors.Is(err, io.EOF) {
			b.observation.RequestBodyEOF = true
		} else if err != nil && strings.TrimSpace(b.observation.RequestBodyReadError) == "" {
			b.observation.RequestBodyReadError = err.Error()
		}
	}
	return n, err
}

func (b *edgeProxyObservedRequestBody) Close() error {
	return b.ReadCloser.Close()
}

type edgeBufferedRequestBody struct {
	file        *os.File
	path        string
	reservation *edgeRequestBodyBufferReservation
	once        sync.Once
	closeErr    error
}

type edgeRequestBodyBufferTooLargeError struct {
	readBytes int64
	maxBytes  int64
}

func (e edgeRequestBodyBufferTooLargeError) Error() string {
	return fmt.Sprintf("request body exceeds edge buffer limit: read_bytes=%d max_bytes=%d", e.readBytes, e.maxBytes)
}

type edgeRequestBodyBufferWriter struct {
	file        *os.File
	reservation *edgeRequestBodyBufferReservation
	maxBytes    int64
	written     int64
}

func (w *edgeRequestBodyBufferWriter) Write(data []byte) (int, error) {
	if w == nil || w.file == nil {
		return 0, io.ErrClosedPipe
	}
	if len(data) == 0 {
		return 0, nil
	}
	target := w.written + int64(len(data))
	if w.maxBytes > 0 && target > w.maxBytes {
		allowed := w.maxBytes - w.written
		if allowed <= 0 {
			return 0, edgeRequestBodyBufferTooLargeError{readBytes: target, maxBytes: w.maxBytes}
		}
		if w.reservation != nil {
			if err := w.reservation.grow(w.written + allowed); err != nil {
				return 0, err
			}
		}
		n, err := w.file.Write(data[:allowed])
		w.written += int64(n)
		if err != nil {
			return n, err
		}
		return n, edgeRequestBodyBufferTooLargeError{readBytes: target, maxBytes: w.maxBytes}
	}
	if w.reservation != nil {
		if err := w.reservation.grow(target); err != nil {
			return 0, err
		}
	}
	n, err := w.file.Write(data)
	w.written += int64(n)
	return n, err
}

func (b *edgeBufferedRequestBody) Read(data []byte) (int, error) {
	if b == nil || b.file == nil {
		return 0, io.EOF
	}
	return b.file.Read(data)
}

func (b *edgeBufferedRequestBody) Close() error {
	if b == nil {
		return nil
	}
	b.once.Do(func() {
		if b.file != nil {
			b.closeErr = b.file.Close()
		}
		if strings.TrimSpace(b.path) != "" {
			if err := os.Remove(b.path); err != nil && !errors.Is(err, os.ErrNotExist) && b.closeErr == nil {
				b.closeErr = err
			}
		}
		if b.reservation != nil {
			b.reservation.release()
		}
	})
	return b.closeErr
}

type edgeRequestBodyBufferManager struct {
	mu           sync.Mutex
	path         string
	budget       int64
	dynamic      bool
	reserveBytes int64
	diskRatio    float64
	used         int64
	active       int64
	disabled     bool
	reason       string
}

type edgeRequestBodyBufferReservation struct {
	manager *edgeRequestBodyBufferManager
	bytes   int64
	once    sync.Once
}

func newEdgeRequestBodyBufferManager(cfg config.EdgeConfig) *edgeRequestBodyBufferManager {
	path := strings.TrimSpace(cfg.RequestBodyBufferPath)
	if path == "" {
		path = "/var/lib/fugue/edge/request-body-buffer"
	}
	dynamic := cfg.RequestBodyBufferTotalMaxBytes <= 0
	budget := cfg.RequestBodyBufferTotalMaxBytes
	if dynamic {
		budget = dynamicEdgeRequestBodyBufferBudget(path, cfg.RequestBodyBufferReserveBytes, cfg.RequestBodyBufferDiskRatio)
	}
	manager := &edgeRequestBodyBufferManager{
		path:         path,
		budget:       budget,
		dynamic:      dynamic,
		reserveBytes: cfg.RequestBodyBufferReserveBytes,
		diskRatio:    cfg.RequestBodyBufferDiskRatio,
	}
	if !dynamic && budget <= 0 {
		manager.disabled = true
		manager.reason = "no request body buffer budget available"
	}
	return manager
}

func dynamicEdgeRequestBodyBufferBudget(path string, reserveBytes int64, ratio float64) int64 {
	if reserveBytes < 0 {
		reserveBytes = 0
	}
	if ratio <= 0 || ratio > 1 {
		ratio = 0.25
	}
	available, err := filesystemAvailableBytes(path)
	if err != nil {
		parent := filepath.Dir(strings.TrimSpace(path))
		if parent == "" || parent == "." {
			parent = "/"
		}
		available, err = filesystemAvailableBytes(parent)
	}
	if err != nil || available <= reserveBytes {
		return 0
	}
	return int64(float64(available-reserveBytes) * ratio)
}

func filesystemAvailableBytes(path string) (int64, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = "/"
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}

func (s *Service) edgeRequestBodyBufferManager() *edgeRequestBodyBufferManager {
	if s == nil {
		return newEdgeRequestBodyBufferManager(config.EdgeConfig{})
	}
	if s.bodyBuffer == nil {
		s.bodyBuffer = newEdgeRequestBodyBufferManager(s.Config)
	}
	return s.bodyBuffer
}

func (m *edgeRequestBodyBufferManager) reserve(bytes int64) (*edgeRequestBodyBufferReservation, error) {
	if m == nil {
		return nil, fmt.Errorf("edge request body buffer manager is unavailable")
	}
	if bytes <= 0 {
		bytes = 1
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshBudgetLocked()
	if m.disabled || m.budget <= 0 {
		reason := strings.TrimSpace(m.reason)
		if reason == "" {
			reason = "edge request body buffer disabled"
		}
		return nil, fmt.Errorf("%s", reason)
	}
	if bytes > m.budget {
		return nil, fmt.Errorf("request body buffer reservation exceeds budget: bytes=%d budget=%d", bytes, m.budget)
	}
	if m.used+bytes > m.budget {
		return nil, fmt.Errorf("request body buffer budget exhausted: requested=%d used=%d budget=%d active=%d", bytes, m.used, m.budget, m.active)
	}
	m.used += bytes
	m.active++
	return &edgeRequestBodyBufferReservation{
		manager: m,
		bytes:   bytes,
	}, nil
}

func (m *edgeRequestBodyBufferManager) currentBudget() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshBudgetLocked()
	return m.budget
}

func (m *edgeRequestBodyBufferManager) refreshBudgetLocked() {
	if m == nil || !m.dynamic {
		return
	}
	m.budget = dynamicEdgeRequestBodyBufferBudget(m.path, m.reserveBytes, m.diskRatio)
	if m.budget <= 0 {
		m.reason = "no request body buffer budget available"
		return
	}
	m.reason = ""
}

func (r *edgeRequestBodyBufferReservation) resize(bytes int64) {
	if r == nil || r.manager == nil || bytes < 0 {
		return
	}
	r.manager.mu.Lock()
	defer r.manager.mu.Unlock()
	delta := bytes - r.bytes
	r.manager.used += delta
	if r.manager.used < 0 {
		r.manager.used = 0
	}
	r.bytes = bytes
}

func (r *edgeRequestBodyBufferReservation) grow(bytes int64) error {
	if r == nil || r.manager == nil {
		return nil
	}
	if bytes <= r.bytes {
		return nil
	}
	r.manager.mu.Lock()
	defer r.manager.mu.Unlock()
	r.manager.refreshBudgetLocked()
	if r.manager.disabled || r.manager.budget <= 0 {
		reason := strings.TrimSpace(r.manager.reason)
		if reason == "" {
			reason = "edge request body buffer disabled"
		}
		return fmt.Errorf("%s", reason)
	}
	if bytes > r.manager.budget {
		return fmt.Errorf("request body buffer reservation exceeds budget: bytes=%d budget=%d", bytes, r.manager.budget)
	}
	delta := bytes - r.bytes
	if r.manager.used+delta > r.manager.budget {
		return fmt.Errorf("request body buffer budget exhausted: requested=%d used=%d budget=%d active=%d", delta, r.manager.used, r.manager.budget, r.manager.active)
	}
	r.manager.used += delta
	r.bytes = bytes
	return nil
}

func (r *edgeRequestBodyBufferReservation) release() {
	if r == nil || r.manager == nil {
		return
	}
	r.once.Do(func() {
		r.manager.mu.Lock()
		defer r.manager.mu.Unlock()
		r.manager.used -= r.bytes
		if r.manager.used < 0 {
			r.manager.used = 0
		}
		if r.manager.active > 0 {
			r.manager.active--
		}
	})
}

func (m *edgeRequestBodyBufferManager) usedBytes() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.used
}

func (m *edgeRequestBodyBufferManager) activeRequests() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active
}

func (m *edgeRequestBodyBufferManager) stats() (int64, int64, int64) {
	if m == nil {
		return 0, 0, 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.budget, m.used, m.active
}

func setEdgeXForwarded(req *httputil.ProxyRequest) {
	if req == nil {
		return
	}
	if forwardedFor := lastForwardedForValue(req.In.Header.Values("X-Forwarded-For")); forwardedFor != "" {
		req.Out.Header.Set("X-Forwarded-For", forwardedFor)
	}
	req.SetXForwarded()
}

func lastForwardedForValue(values []string) string {
	for i := len(values) - 1; i >= 0; i-- {
		parts := strings.Split(values[i], ",")
		for j := len(parts) - 1; j >= 0; j-- {
			if candidate := strings.TrimSpace(parts[j]); candidate != "" {
				return candidate
			}
		}
	}
	return ""
}

func edgeClientCountryFromRequest(r *http.Request) string {
	return normalizeEdgeClientScopeValue(firstNonEmptyHeaders(r,
		"X-Fugue-Client-Country",
		"CF-IPCountry",
		"CloudFront-Viewer-Country",
		"X-Vercel-IP-Country",
		"X-Client-Country",
	))
}

func edgeClientRegionFromRequest(r *http.Request) string {
	return normalizeEdgeClientScopeValue(firstNonEmptyHeaders(r,
		"X-Fugue-Client-Region",
		"CloudFront-Viewer-Country-Region",
		"X-Vercel-IP-Country-Region",
		"CF-Region",
		"X-Client-Region",
	))
}

func edgeClientASNFromRequest(r *http.Request) string {
	value := normalizeEdgeClientScopeValue(firstNonEmptyHeaders(r,
		"X-Fugue-Client-ASN",
		"CF-Connecting-ASN",
		"X-Vercel-IP-ASN",
		"Fastly-Client-ASN",
		"X-Client-ASN",
	))
	value = strings.TrimPrefix(strings.ToLower(value), "as")
	if value == "" {
		return ""
	}
	return "as" + value
}

func normalizeEdgeClientScopeValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "unknown") || strings.EqualFold(value, "xx") {
		return ""
	}
	return strings.ToLower(value)
}

type edgeProxyTransport struct {
	base        http.RoundTripper
	observation *edgeProxyObservation
}

func (s *Service) newEdgeProxyTransport(observation *edgeProxyObservation) http.RoundTripper {
	base := http.RoundTripper(nil)
	if s != nil {
		base = s.edgeProxyBaseForObservation(observation)
	}
	if base == nil {
		base = newDefaultEdgeProxyTransport()
	}
	return &edgeProxyTransport{
		base:        base,
		observation: observation,
	}
}

func (s *Service) edgeProxyBaseForObservation(observation *edgeProxyObservation) http.RoundTripper {
	if s == nil || s.proxyBase == nil {
		return nil
	}
	prototype, ok := s.proxyBase.(*http.Transport)
	if !ok {
		// Tests and specialized callers can inject a complete RoundTripper.
		return s.proxyBase
	}
	key := edgeOriginTransportKey(observation)
	if key == "" {
		return prototype
	}

	s.proxyTransportMu.Lock()
	defer s.proxyTransportMu.Unlock()
	if s.proxyTransportPrototype != prototype {
		s.closeEdgeProxyTransportsLocked()
		s.proxyTransportPrototype = prototype
	}
	if transport := s.proxyTransports[key]; transport != nil {
		return transport
	}
	transport := prototype.Clone()
	if s.proxyTransportBundleSet {
		if _, active := s.proxyTransportActiveKeys[key]; !active {
			// A request can race a bundle swap after reading the old route. Let it
			// finish without making its obsolete connection reusable.
			transport.DisableKeepAlives = true
			return transport
		}
	}
	if s.proxyTransports == nil {
		s.proxyTransports = map[string]*http.Transport{}
	}
	s.proxyTransports[key] = transport
	return transport
}

func edgeOriginTransportKey(observation *edgeProxyObservation) string {
	if observation == nil {
		return ""
	}
	return edgeOriginTransportKeyForRoute(observation.Route, observation.ReleaseID)
}

func edgeOriginTransportKeyForRoute(route model.EdgeRouteBinding, releaseID string) string {
	upstreamURL := strings.TrimSpace(route.UpstreamURL)
	if upstreamURL == "" {
		return ""
	}
	generation := strings.TrimSpace(route.DeploymentGeneration)
	if generation == "" {
		generation = strings.TrimSpace(route.RouteGeneration)
	}
	return strings.Join([]string{
		strings.TrimSpace(route.AppID),
		strings.TrimSpace(route.RuntimeID),
		upstreamURL,
		generation,
		strings.TrimSpace(releaseID),
	}, "\x00")
}

func edgeOriginTransportKeys(bundle model.EdgeRouteBundle) map[string]struct{} {
	keys := make(map[string]struct{}, len(bundle.Routes))
	for _, route := range bundle.Routes {
		if key := edgeOriginTransportKeyForRoute(route, ""); key != "" {
			keys[key] = struct{}{}
		}
		for _, upstream := range route.Upstreams {
			active := strings.TrimSpace(upstream.UpstreamURL) != "" &&
				upstream.Weight > 0 &&
				(strings.TrimSpace(upstream.Status) == "" || strings.EqualFold(strings.TrimSpace(upstream.Status), model.EdgeRouteStatusActive))
			if !active {
				continue
			}
			selectedRoute := edgeRouteWithUpstream(route, upstream)
			if key := edgeOriginTransportKeyForRoute(selectedRoute, upstream.ReleaseID); key != "" {
				keys[key] = struct{}{}
			}
		}
	}
	return keys
}

func (s *Service) reconcileEdgeProxyTransports(bundle model.EdgeRouteBundle) {
	if s == nil {
		return
	}
	activeKeys := edgeOriginTransportKeys(bundle)
	s.proxyTransportMu.Lock()
	defer s.proxyTransportMu.Unlock()
	s.proxyTransportActiveKeys = activeKeys
	s.proxyTransportBundleSet = true
	for key, transport := range s.proxyTransports {
		if _, active := activeKeys[key]; active {
			continue
		}
		transport.CloseIdleConnections()
		delete(s.proxyTransports, key)
	}
}

func (s *Service) closeEdgeProxyTransports() {
	if s == nil {
		return
	}
	s.proxyTransportMu.Lock()
	defer s.proxyTransportMu.Unlock()
	s.closeEdgeProxyTransportsLocked()
}

func (s *Service) closeEdgeProxyTransportsLocked() {
	for key, transport := range s.proxyTransports {
		transport.CloseIdleConnections()
		delete(s.proxyTransports, key)
	}
}

func (t *edgeProxyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t == nil || t.base == nil {
		return nil, fmt.Errorf("edge proxy transport is unavailable")
	}
	base := t.base
	if t.observation != nil && t.observation.Streaming && t.observation.Upload {
		if transport, ok := base.(*http.Transport); ok {
			// Request.Close only closes after the request; a private transport also avoids selecting a stale idle connection.
			fresh := transport.Clone()
			fresh.DisableKeepAlives = true
			base = fresh
		}
	}
	started := time.Now()
	if t.observation != nil {
		var dnsStarted time.Time
		var connectStarted time.Time
		trace := &httptrace.ClientTrace{
			DNSStart: func(httptrace.DNSStartInfo) {
				dnsStarted = time.Now()
			},
			DNSDone: func(info httptrace.DNSDoneInfo) {
				if !dnsStarted.IsZero() {
					t.observation.OriginDNS = time.Since(dnsStarted)
				}
				if info.Err != nil {
					t.observation.OriginDNSError = info.Err.Error()
				}
			},
			GotConn: func(info httptrace.GotConnInfo) {
				t.observation.OriginGotConn = true
				t.observation.OriginConnectionReused = info.Reused
				if info.Conn != nil {
					t.observation.OriginRemoteAddr = info.Conn.RemoteAddr().String()
					t.observation.OriginLocalAddr = info.Conn.LocalAddr().String()
				}
			},
			ConnectStart: func(_, _ string) {
				connectStarted = time.Now()
			},
			ConnectDone: func(_, _ string, err error) {
				if !connectStarted.IsZero() {
					t.observation.OriginConnect = time.Since(connectStarted)
				}
				if err != nil {
					t.observation.OriginConnectError = err.Error()
				}
			},
			WroteHeaders: func() {
				t.observation.OriginWroteHeaders = true
			},
			WroteRequest: func(info httptrace.WroteRequestInfo) {
				t.observation.OriginWroteRequest = true
				t.observation.OriginRequestWrite = time.Since(started)
				if info.Err != nil {
					t.observation.OriginRequestWriteErr = info.Err.Error()
				}
			},
			GotFirstResponseByte: func() {
				if t.observation.OriginTTFB <= 0 {
					t.observation.OriginTTFB = time.Since(started)
				}
			},
		}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	}
	resp, err := base.RoundTrip(req)
	if t.observation != nil {
		t.observation.Upstream = time.Since(started)
		if resp != nil && t.observation.OriginTTFB <= 0 {
			t.observation.OriginTTFB = t.observation.Upstream
		}
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols && resp.Body != nil {
			resp.Body = &edgeOriginTimingBody{
				ReadCloser:  resp.Body,
				observation: t.observation,
				startedAt:   started,
			}
		}
	}
	return resp, err
}

type edgeOriginTimingBody struct {
	io.ReadCloser
	observation *edgeProxyObservation
	startedAt   time.Time
	once        sync.Once
}

func (b *edgeOriginTimingBody) Read(data []byte) (int, error) {
	n, err := b.ReadCloser.Read(data)
	if errors.Is(err, io.EOF) {
		b.finish()
	}
	return n, err
}

func (b *edgeOriginTimingBody) Close() error {
	err := b.ReadCloser.Close()
	b.finish()
	return err
}

func (b *edgeOriginTimingBody) finish() {
	if b == nil || b.observation == nil || b.startedAt.IsZero() {
		return
	}
	b.once.Do(func() {
		b.observation.OriginTotal = time.Since(b.startedAt)
	})
}

func (s *Service) peerFallbackAllowed(r *http.Request, observed edgeProxyObservation) bool {
	if !s.Config.PeerFallbackEnabled || r == nil {
		return false
	}
	if strings.TrimSpace(r.Header.Get(edgePeerFallbackHeader)) != "" {
		return false
	}
	if observed.Streaming || observed.Upload {
		return false
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func (s *Service) proxyPeerFallback(w http.ResponseWriter, r *http.Request, host string, route model.EdgeRouteBinding, observed *edgeProxyObservation) bool {
	target := &url.URL{Scheme: "https", Host: host}
	if strings.TrimSpace(host) == "" {
		return false
	}
	peerRoute := route
	peerRoute.UpstreamURL = target.String()
	if observed != nil {
		observed.PeerFallback = true
		observed.FallbackHit = true
		observed.UpstreamError = ""
	}
	peerRequest := r.Clone(r.Context())
	peerRequest.Header = r.Header.Clone()
	peerRequest.Header.Set(edgePeerFallbackHeader, "1")
	peerRequest.Header.Set("X-Fugue-Edge-Route-Host", host)
	proxy := s.newEdgeReverseProxy(host, target, peerRoute, observed, false, nil)
	writer := newEdgeProxyObservationResponseWriter(w, time.Now(), observed)
	proxy.ServeHTTP(writer, peerRequest)
	if observed != nil {
		observed.StatusCode = writer.statusCode()
	}
	return writer.wroteHeader
}

func newDefaultEdgeProxyTransport() http.RoundTripper {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &http.Transport{
			Proxy:               nil,
			ForceAttemptHTTP2:   false,
			TLSHandshakeTimeout: 10 * time.Second,
		}
	}
	transport := base.Clone()
	transport.Proxy = nil
	transport.ForceAttemptHTTP2 = false
	return transport
}

func (s *Service) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot := s.metricSnapshot()
	status := snapshot.Status
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintln(w, "# HELP fugue_edge_info Static fugue-edge identity labels.")
	fmt.Fprintln(w, "# TYPE fugue_edge_info gauge")
	fmt.Fprintf(w, "fugue_edge_info{edge_id=\"%s\",edge_group_id=\"%s\"} 1\n", prometheusLabelValue(status.EdgeID), prometheusLabelValue(status.EdgeGroupID))
	fmt.Fprintln(w, "# HELP fugue_edge_health Whether fugue-edge has a usable route bundle.")
	fmt.Fprintln(w, "# TYPE fugue_edge_health gauge")
	fmt.Fprintf(w, "fugue_edge_health %d\n", boolGauge(status.Healthy))
	fmt.Fprintln(w, "# HELP fugue_edge_stale_cache Whether fugue-edge is serving a last-known-good cached bundle after sync failure.")
	fmt.Fprintln(w, "# TYPE fugue_edge_stale_cache gauge")
	fmt.Fprintf(w, "fugue_edge_stale_cache %d\n", boolGauge(status.StaleCache))
	fmt.Fprintln(w, "# HELP fugue_edge_stale_age_seconds Seconds since the last successful bundle sync while serving a stale cache.")
	fmt.Fprintln(w, "# TYPE fugue_edge_stale_age_seconds gauge")
	fmt.Fprintf(w, "fugue_edge_stale_age_seconds %.0f\n", staleAgeSeconds(status.StaleCache, status.LastSuccessAt, time.Now().UTC()))
	if strings.TrimSpace(status.DegradedReason) != "" {
		fmt.Fprintln(w, "# HELP fugue_edge_degraded_reason Current fugue-edge degraded reason.")
		fmt.Fprintln(w, "# TYPE fugue_edge_degraded_reason gauge")
		fmt.Fprintf(w, "fugue_edge_degraded_reason{reason=\"%s\"} 1\n", prometheusLabelValue(status.DegradedReason))
	}
	fmt.Fprintln(w, "# HELP fugue_edge_routes Number of routes in the current bundle.")
	fmt.Fprintln(w, "# TYPE fugue_edge_routes gauge")
	fmt.Fprintf(w, "fugue_edge_routes{bundle_version=\"%s\"} %d\n", prometheusLabelValue(status.BundleVersion), status.RouteCount)
	fmt.Fprintln(w, "# HELP fugue_edge_tls_allowlist_entries Number of TLS allowlist entries in the current bundle.")
	fmt.Fprintln(w, "# TYPE fugue_edge_tls_allowlist_entries gauge")
	fmt.Fprintf(w, "fugue_edge_tls_allowlist_entries{bundle_version=\"%s\"} %d\n", prometheusLabelValue(status.BundleVersion), status.TLSAllowlistCount)
	fmt.Fprintln(w, "# HELP fugue_edge_last_sync_timestamp_seconds Last route bundle sync attempt time.")
	fmt.Fprintln(w, "# TYPE fugue_edge_last_sync_timestamp_seconds gauge")
	fmt.Fprintf(w, "fugue_edge_last_sync_timestamp_seconds %.0f\n", unixSeconds(status.LastSyncAt))
	fmt.Fprintln(w, "# HELP fugue_edge_last_success_timestamp_seconds Last successful route bundle sync time.")
	fmt.Fprintln(w, "# TYPE fugue_edge_last_success_timestamp_seconds gauge")
	fmt.Fprintf(w, "fugue_edge_last_success_timestamp_seconds %.0f\n", unixSeconds(status.LastSuccessAt))
	fmt.Fprintln(w, "# HELP fugue_edge_bundle_sync_total Route bundle sync attempts by result.")
	fmt.Fprintln(w, "# TYPE fugue_edge_bundle_sync_total counter")
	fmt.Fprintf(w, "fugue_edge_bundle_sync_total{result=\"success\"} %d\n", snapshot.Metrics.BundleSyncSuccess)
	fmt.Fprintf(w, "fugue_edge_bundle_sync_total{result=\"not_modified\"} %d\n", snapshot.Metrics.BundleSyncNotModified)
	fmt.Fprintf(w, "fugue_edge_bundle_sync_total{result=\"error\"} %d\n", snapshot.Metrics.BundleSyncError)
	fmt.Fprintln(w, "# HELP fugue_edge_bundle_sync_duration_seconds Duration of the last route bundle sync attempt.")
	fmt.Fprintln(w, "# TYPE fugue_edge_bundle_sync_duration_seconds gauge")
	fmt.Fprintf(w, "fugue_edge_bundle_sync_duration_seconds %.6f\n", durationSeconds(snapshot.Metrics.LastSyncDuration))
	fmt.Fprintln(w, "# HELP fugue_edge_bundle_age_seconds Age of the current route bundle based on generated_at.")
	fmt.Fprintln(w, "# TYPE fugue_edge_bundle_age_seconds gauge")
	fmt.Fprintf(w, "fugue_edge_bundle_age_seconds %.0f\n", bundleAgeSeconds(snapshot.BundleGeneratedAt, time.Now().UTC()))
	fmt.Fprintln(w, "# HELP fugue_edge_cache_write_total Route bundle cache write attempts by result.")
	fmt.Fprintln(w, "# TYPE fugue_edge_cache_write_total counter")
	fmt.Fprintf(w, "fugue_edge_cache_write_total{result=\"success\"} %d\n", snapshot.Metrics.CacheWriteSuccess)
	fmt.Fprintf(w, "fugue_edge_cache_write_total{result=\"error\"} %d\n", snapshot.Metrics.CacheWriteError)
	fmt.Fprintln(w, "# HELP fugue_edge_cache_load_total Route bundle cache load attempts by result.")
	fmt.Fprintln(w, "# TYPE fugue_edge_cache_load_total counter")
	fmt.Fprintf(w, "fugue_edge_cache_load_total{result=\"success\"} %d\n", snapshot.Metrics.CacheLoadSuccess)
	fmt.Fprintf(w, "fugue_edge_cache_load_total{result=\"miss\"} %d\n", snapshot.Metrics.CacheLoadMiss)
	fmt.Fprintf(w, "fugue_edge_cache_load_total{result=\"error\"} %d\n", snapshot.Metrics.CacheLoadError)
	fmt.Fprintln(w, "# HELP fugue_edge_caddy_config_apply_total Caddy dynamic config apply attempts by result.")
	fmt.Fprintln(w, "# TYPE fugue_edge_caddy_config_apply_total counter")
	fmt.Fprintf(w, "fugue_edge_caddy_config_apply_total{result=\"success\"} %d\n", snapshot.Metrics.CaddyConfigSuccess)
	fmt.Fprintf(w, "fugue_edge_caddy_config_apply_total{result=\"error\"} %d\n", snapshot.Metrics.CaddyConfigError)
	fmt.Fprintln(w, "# HELP fugue_edge_caddy_tls_warmup_total Caddy local SNI TLS warmup attempts by result.")
	fmt.Fprintln(w, "# TYPE fugue_edge_caddy_tls_warmup_total counter")
	fmt.Fprintf(w, "fugue_edge_caddy_tls_warmup_total{result=\"success\"} %d\n", snapshot.Metrics.CaddyWarmupSuccess)
	fmt.Fprintf(w, "fugue_edge_caddy_tls_warmup_total{result=\"error\"} %d\n", snapshot.Metrics.CaddyWarmupError)
	fmt.Fprintln(w, "# HELP fugue_edge_caddy_tls_warmup_duration_seconds Duration of the last local SNI TLS warmup.")
	fmt.Fprintln(w, "# TYPE fugue_edge_caddy_tls_warmup_duration_seconds gauge")
	fmt.Fprintf(w, "fugue_edge_caddy_tls_warmup_duration_seconds %.6f\n", durationSeconds(snapshot.Metrics.CaddyWarmupDuration))
	fmt.Fprintln(w, "# HELP fugue_edge_http_cache_warmup_total HTTP edge cache warmup attempts by result.")
	fmt.Fprintln(w, "# TYPE fugue_edge_http_cache_warmup_total counter")
	fmt.Fprintf(w, "fugue_edge_http_cache_warmup_total{result=\"success\"} %d\n", snapshot.Metrics.CacheWarmupSuccess)
	fmt.Fprintf(w, "fugue_edge_http_cache_warmup_total{result=\"error\"} %d\n", snapshot.Metrics.CacheWarmupError)
	fmt.Fprintln(w, "# HELP fugue_edge_http_cache_warmup_duration_seconds Duration of the last HTTP edge cache warmup.")
	fmt.Fprintln(w, "# TYPE fugue_edge_http_cache_warmup_duration_seconds gauge")
	fmt.Fprintf(w, "fugue_edge_http_cache_warmup_duration_seconds %.6f\n", durationSeconds(snapshot.Metrics.CacheWarmupDuration))
	fmt.Fprintln(w, "# HELP fugue_edge_caddy_routes Number of host routes in the last applied Caddy config.")
	fmt.Fprintln(w, "# TYPE fugue_edge_caddy_routes gauge")
	fmt.Fprintf(w, "fugue_edge_caddy_routes{bundle_version=\"%s\"} %d\n", prometheusLabelValue(snapshot.Metrics.CaddyAppliedVersion), snapshot.Metrics.CaddyRouteCount)
	fmt.Fprintln(w, "# HELP fugue_edge_caddy_last_apply_timestamp_seconds Last successful Caddy config apply time.")
	fmt.Fprintln(w, "# TYPE fugue_edge_caddy_last_apply_timestamp_seconds gauge")
	fmt.Fprintf(w, "fugue_edge_caddy_last_apply_timestamp_seconds %.0f\n", unixSeconds(snapshot.Metrics.CaddyLastApplyAt))
	fmt.Fprintln(w, "# HELP fugue_edge_route_requests_total Edge data-plane requests by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_requests_total counter")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteRequests) {
		fmt.Fprintf(w, "fugue_edge_route_requests_total{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteRequests[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_status_total Edge data-plane responses by route and status code.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_status_total counter")
	for _, key := range sortedRouteStatusMetricKeys(snapshot.Metrics.RouteStatuses) {
		fmt.Fprintf(w, "fugue_edge_route_status_total{%s,status=\"%d\"} %d\n", routeMetricLabels(key.RouteMetricKey), key.StatusCode, snapshot.Metrics.RouteStatuses[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_upstream_errors_total Edge data-plane upstream proxy errors by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_upstream_errors_total counter")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteUpstreamErrors) {
		fmt.Fprintf(w, "fugue_edge_route_upstream_errors_total{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteUpstreamErrors[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_fallback_hits_total Edge data-plane requests that matched a non-active fallback route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_fallback_hits_total counter")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteFallbackHits) {
		fmt.Fprintf(w, "fugue_edge_route_fallback_hits_total{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteFallbackHits[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_websocket_total Edge data-plane websocket requests by route and result.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_websocket_total counter")
	for _, key := range sortedRouteResultMetricKeys(snapshot.Metrics.RouteWebSocketResults) {
		fmt.Fprintf(w, "fugue_edge_route_websocket_total{%s,result=\"%s\"} %d\n", routeMetricLabels(key.RouteMetricKey), prometheusLabelValue(key.Result), snapshot.Metrics.RouteWebSocketResults[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_sse_total Edge data-plane SSE requests by route and result.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_sse_total counter")
	for _, key := range sortedRouteResultMetricKeys(snapshot.Metrics.RouteSSEResults) {
		fmt.Fprintf(w, "fugue_edge_route_sse_total{%s,result=\"%s\"} %d\n", routeMetricLabels(key.RouteMetricKey), prometheusLabelValue(key.Result), snapshot.Metrics.RouteSSEResults[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_streaming_total Edge data-plane streaming requests by route and result.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_streaming_total counter")
	for _, key := range sortedRouteResultMetricKeys(snapshot.Metrics.RouteStreamingResults) {
		fmt.Fprintf(w, "fugue_edge_route_streaming_total{%s,result=\"%s\"} %d\n", routeMetricLabels(key.RouteMetricKey), prometheusLabelValue(key.Result), snapshot.Metrics.RouteStreamingResults[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_upload_requests_total Edge data-plane upload requests by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_upload_requests_total counter")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteUploadRequests) {
		fmt.Fprintf(w, "fugue_edge_route_upload_requests_total{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteUploadRequests[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_request_body_read_seconds Time spent blocked reading buffered request bodies at the edge.")
	fmt.Fprintln(w, "# TYPE fugue_edge_request_body_read_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteBodyReadCount) {
		fmt.Fprintf(w, "fugue_edge_request_body_read_seconds_sum{%s} %.6f\n", edgeRouteMetricLabels(status, key), snapshot.Metrics.RouteBodyReadSum[key])
		fmt.Fprintf(w, "fugue_edge_request_body_read_seconds_count{%s} %d\n", edgeRouteMetricLabels(status, key), snapshot.Metrics.RouteBodyReadCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_request_body_file_write_seconds Time spent writing buffered request bodies to the edge temp file.")
	fmt.Fprintln(w, "# TYPE fugue_edge_request_body_file_write_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteBodyWriteCount) {
		fmt.Fprintf(w, "fugue_edge_request_body_file_write_seconds_sum{%s} %.6f\n", edgeRouteMetricLabels(status, key), snapshot.Metrics.RouteBodyWriteSum[key])
		fmt.Fprintf(w, "fugue_edge_request_body_file_write_seconds_count{%s} %d\n", edgeRouteMetricLabels(status, key), snapshot.Metrics.RouteBodyWriteCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_request_body_throughput_bps Buffered request body read throughput in bytes per second at the edge.")
	fmt.Fprintln(w, "# TYPE fugue_edge_request_body_throughput_bps summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteBodyThroughputCount) {
		fmt.Fprintf(w, "fugue_edge_request_body_throughput_bps_sum{%s} %.6f\n", edgeRouteMetricLabels(status, key), snapshot.Metrics.RouteBodyThroughputSum[key])
		fmt.Fprintf(w, "fugue_edge_request_body_throughput_bps_count{%s} %d\n", edgeRouteMetricLabels(status, key), snapshot.Metrics.RouteBodyThroughputCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_total_duration_seconds Edge data-plane request duration by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_total_duration_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteDurationCount) {
		fmt.Fprintf(w, "fugue_edge_route_total_duration_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteDurationSum[key])
		fmt.Fprintf(w, "fugue_edge_route_total_duration_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteDurationCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_upstream_latency_seconds Edge data-plane upstream proxy latency by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_upstream_latency_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteLatencyCount) {
		fmt.Fprintf(w, "fugue_edge_route_upstream_latency_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteLatencySum[key])
		fmt.Fprintf(w, "fugue_edge_route_upstream_latency_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteLatencyCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_ttfb_seconds Edge data-plane proxy time-to-first-byte by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_ttfb_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteTTFBCount) {
		fmt.Fprintf(w, "fugue_edge_route_ttfb_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteTTFBSum[key])
		fmt.Fprintf(w, "fugue_edge_route_ttfb_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteTTFBCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_cache_lookup_duration_seconds Edge data-plane local HTTP cache lookup duration by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_cache_lookup_duration_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteCacheLookupCount) {
		fmt.Fprintf(w, "fugue_edge_route_cache_lookup_duration_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteCacheLookupSum[key])
		fmt.Fprintf(w, "fugue_edge_route_cache_lookup_duration_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteCacheLookupCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_origin_dns_duration_seconds Edge data-plane origin DNS lookup duration by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_origin_dns_duration_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteOriginDNSCount) {
		fmt.Fprintf(w, "fugue_edge_route_origin_dns_duration_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginDNSSum[key])
		fmt.Fprintf(w, "fugue_edge_route_origin_dns_duration_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginDNSCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_origin_connect_duration_seconds Edge data-plane origin TCP connect duration by route for non-reused upstream connections.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_origin_connect_duration_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteOriginConnCount) {
		fmt.Fprintf(w, "fugue_edge_route_origin_connect_duration_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginConnSum[key])
		fmt.Fprintf(w, "fugue_edge_route_origin_connect_duration_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginConnCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_origin_request_write_duration_seconds Edge data-plane time until the origin request, including body, is written by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_origin_request_write_duration_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteOriginWriteCount) {
		fmt.Fprintf(w, "fugue_edge_route_origin_request_write_duration_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginWriteSum[key])
		fmt.Fprintf(w, "fugue_edge_route_origin_request_write_duration_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginWriteCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_origin_response_header_wait_seconds Edge data-plane time spent waiting for the first origin response byte after writing the origin request by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_origin_response_header_wait_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteOriginWaitCount) {
		fmt.Fprintf(w, "fugue_edge_route_origin_response_header_wait_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginWaitSum[key])
		fmt.Fprintf(w, "fugue_edge_route_origin_response_header_wait_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginWaitCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_origin_ttfb_seconds Edge data-plane origin time-to-first-byte by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_origin_ttfb_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteOriginTTFBCount) {
		fmt.Fprintf(w, "fugue_edge_route_origin_ttfb_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginTTFBSum[key])
		fmt.Fprintf(w, "fugue_edge_route_origin_ttfb_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginTTFBCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_origin_total_duration_seconds Edge data-plane origin response read duration by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_origin_total_duration_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteOriginTotalCount) {
		fmt.Fprintf(w, "fugue_edge_route_origin_total_duration_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginTotalSum[key])
		fmt.Fprintf(w, "fugue_edge_route_origin_total_duration_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteOriginTotalCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_response_write_duration_seconds Edge data-plane time spent writing response bytes by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_response_write_duration_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteWriteCount) {
		fmt.Fprintf(w, "fugue_edge_route_response_write_duration_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteWriteSum[key])
		fmt.Fprintf(w, "fugue_edge_route_response_write_duration_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteWriteCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_route_cache_total Edge data-plane cache decisions by route and cache status.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_cache_total counter")
	for _, key := range sortedRouteCacheMetricKeys(snapshot.Metrics.RouteCacheStatus) {
		fmt.Fprintf(w, "fugue_edge_route_cache_total{%s,cache_status=\"%s\",cache_policy_id=\"%s\",asset_class=\"%s\"} %d\n",
			routeMetricLabels(key.RouteMetricKey),
			prometheusLabelValue(key.CacheStatus),
			prometheusLabelValue(key.CachePolicyID),
			prometheusLabelValue(key.AssetClass),
			snapshot.Metrics.RouteCacheStatus[key],
		)
	}
}

func (s *Service) startHTTPServer() (func(context.Context) error, error) {
	addr := strings.TrimSpace(s.Config.ListenAddr)
	if addr == "" {
		return nil, nil
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen edge health server on %s: %w", addr, err)
	}
	server := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.Logger.Printf("edge health server failed: %v", err)
		}
	}()
	return server.Shutdown, nil
}

func (s *Service) startProxyServer() (func(context.Context) error, error) {
	if !s.Config.CaddyEnabled {
		return nil, nil
	}
	addr := strings.TrimSpace(s.Config.CaddyProxyListenAddr)
	if addr == "" {
		return nil, fmt.Errorf("FUGUE_EDGE_PROXY_LISTEN_ADDR is required when caddy mode is enabled")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen edge data proxy on %s: %w", addr, err)
	}
	server := &http.Server{
		Handler:           s.ProxyHandler(),
		ReadHeaderTimeout: 10 * time.Second,
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			return edgeContextWithDownstreamConn(ctx, conn)
		},
	}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.Logger.Printf("edge data proxy failed: %v", err)
		}
	}()
	return server.Shutdown, nil
}

func (s *Service) newRoutesRequest(ctx context.Context) (*http.Request, error) {
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(s.Config.APIURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("invalid FUGUE_API_URL")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/v1/edge/routes"
	query := base.Query()
	query.Set("token", strings.TrimSpace(s.Config.EdgeToken))
	if edgeID := strings.TrimSpace(s.Config.EdgeID); edgeID != "" {
		query.Set("edge_id", edgeID)
	}
	if edgeGroupID := strings.TrimSpace(s.Config.EdgeGroupID); edgeGroupID != "" {
		query.Set("edge_group_id", edgeGroupID)
	}
	base.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build edge routes request: %w", err)
	}
	if etag := s.currentETag(); etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	return req, nil
}

func (s *Service) RefreshDesiredState(ctx context.Context) error {
	req, err := s.newDesiredStateRequest(ctx)
	if err != nil {
		if errors.Is(err, errEdgeDesiredStateDisabled) {
			return nil
		}
		return err
	}
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch edge desired state: %s", s.redact(err.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return statusError{
			StatusCode: resp.StatusCode,
			Body:       s.redact(strings.TrimSpace(string(body))),
		}
	}
	var envelope edgeDesiredStateEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return fmt.Errorf("decode edge desired state: %w", err)
	}
	desired := envelope.DesiredState
	if strings.TrimSpace(desired.EdgeGroupID) == "" {
		return fmt.Errorf("edge desired state missing edge_group_id")
	}
	s.applyDesiredState(desired)
	return nil
}

var errEdgeDesiredStateDisabled = errors.New("edge desired state disabled")

func (s *Service) newDesiredStateRequest(ctx context.Context) (*http.Request, error) {
	rawURL := strings.TrimSpace(s.Config.EdgeDesiredStateURL)
	if rawURL == "" && strings.EqualFold(strings.TrimSpace(s.Config.WorkloadMode), model.EdgeWorkloadModeDynamic) {
		base := strings.TrimRight(strings.TrimSpace(s.Config.APIURL), "/")
		edgeID := strings.TrimSpace(s.Config.EdgeID)
		if base != "" && edgeID != "" {
			rawURL = base + "/v1/edge/nodes/" + url.PathEscape(edgeID) + "/desired-state"
		}
	}
	if rawURL == "" {
		return nil, errEdgeDesiredStateDisabled
	}
	if strings.TrimSpace(s.Config.EdgeToken) == "" {
		return nil, fmt.Errorf("FUGUE_EDGE_TOKEN is required to fetch desired state")
	}
	desiredURL, err := url.Parse(rawURL)
	if err != nil || desiredURL.Scheme == "" || desiredURL.Host == "" {
		return nil, fmt.Errorf("invalid FUGUE_EDGE_DESIRED_STATE_URL")
	}
	query := desiredURL.Query()
	query.Set("token", strings.TrimSpace(s.Config.EdgeToken))
	desiredURL.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, desiredURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build edge desired state request: %w", err)
	}
	return req, nil
}

func (s *Service) applyDesiredState(desired edgeDesiredState) {
	edgeGroupID := strings.TrimSpace(desired.EdgeGroupID)
	if edgeGroupID == "" {
		return
	}
	workloadMode := model.NormalizeEdgeWorkloadMode(desired.WorkloadMode)
	s.mu.Lock()
	s.Config.EdgeGroupID = edgeGroupID
	if workloadMode != "" {
		s.Config.WorkloadMode = workloadMode
	}
	s.Config.Draining = desired.Draining
	s.snapshot.EdgeGroupID = edgeGroupID
	s.mu.Unlock()
}

func (s *Service) startHeartbeatLoop(ctx context.Context) {
	if !s.heartbeatEnabled() {
		if s.Logger != nil {
			s.Logger.Printf("edge heartbeat disabled; edge_id=%t edge_group_id=%t token=%t", strings.TrimSpace(s.Config.EdgeID) != "", strings.TrimSpace(s.Config.EdgeGroupID) != "", strings.TrimSpace(s.Config.EdgeToken) != "")
		}
		return
	}
	go func() {
		_ = s.HeartbeatOnce(ctx)
		ticker := time.NewTicker(s.heartbeatInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = s.HeartbeatOnce(ctx)
			}
		}
	}()
}

func (s *Service) HeartbeatOnce(ctx context.Context) error {
	if !s.heartbeatEnabled() {
		if err := s.RefreshDesiredState(ctx); err != nil && s.Logger != nil {
			s.Logger.Printf("edge desired state refresh before heartbeat failed: %s", s.redact(err.Error()))
		}
	}
	if !s.heartbeatEnabled() {
		return nil
	}
	req, performanceMetrics, err := s.newHeartbeatRequest(ctx)
	if err != nil {
		s.logHeartbeatFailure(err)
		return err
	}
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		err = fmt.Errorf("send edge heartbeat: %s", s.redact(err.Error()))
		s.logHeartbeatFailure(err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		err := statusError{
			StatusCode: resp.StatusCode,
			Body:       s.redact(strings.TrimSpace(string(body))),
		}
		s.logHeartbeatFailure(err)
		return err
	}
	s.commitHeartbeatPerformanceBaseline(performanceMetrics)
	if s.Logger != nil {
		status := s.Status()
		s.Logger.Printf("edge heartbeat success; edge_id=%s edge_group_id=%s status=%s route_bundle=%s caddy_routes=%d", strings.TrimSpace(s.Config.EdgeID), strings.TrimSpace(s.Config.EdgeGroupID), edgeHealthStatus(status), status.BundleVersion, s.metricSnapshot().Metrics.CaddyRouteCount)
	}
	return nil
}

func (s *Service) newHeartbeatRequest(ctx context.Context) (*http.Request, telemetry, error) {
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(s.Config.APIURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, telemetry{}, fmt.Errorf("invalid FUGUE_API_URL")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/v1/edge/heartbeat"
	query := base.Query()
	query.Set("token", strings.TrimSpace(s.Config.EdgeToken))
	base.RawQuery = query.Encode()

	snapshot := s.metricSnapshot()
	performanceSamples := s.edgePerformanceSamplesForHeartbeat(snapshot)
	status := snapshot.Status
	cacheStatus := "missing"
	if status.BundleVersion != "" {
		cacheStatus = "ready"
	}
	if status.StaleCache {
		cacheStatus = "stale"
	}
	tlsStatus, tlsLastMessage, tlsReadyAt := s.edgeTLSHeartbeatStatus(status)
	body := map[string]any{
		"edge_id":                  strings.TrimSpace(s.Config.EdgeID),
		"edge_group_id":            strings.TrimSpace(s.Config.EdgeGroupID),
		"workload_mode":            strings.TrimSpace(s.Config.WorkloadMode),
		"region":                   strings.TrimSpace(s.Config.Region),
		"country":                  strings.TrimSpace(s.Config.Country),
		"public_hostname":          strings.TrimSpace(s.Config.PublicHostname),
		"public_ipv4":              strings.TrimSpace(s.Config.PublicIPv4),
		"public_ipv6":              strings.TrimSpace(s.Config.PublicIPv6),
		"mesh_ip":                  strings.TrimSpace(s.Config.MeshIP),
		"route_bundle_version":     strings.TrimSpace(status.BundleVersion),
		"dns_bundle_version":       "",
		"serving_generation":       strings.TrimSpace(status.ServingGeneration),
		"lkg_generation":           strings.TrimSpace(status.LKGGeneration),
		"last_good_generation":     strings.TrimSpace(status.LastGoodGeneration),
		"cache_corrupt_generation": strings.TrimSpace(status.CacheCorruptGeneration),
		"caddy_route_count":        snapshot.Metrics.CaddyRouteCount,
		"caddy_applied_version":    strings.TrimSpace(status.CaddyAppliedVersion),
		"caddy_last_error":         strings.TrimSpace(status.CaddyLastError),
		"cache_status":             cacheStatus,
		"tls_status":               tlsStatus,
		"tls_last_message":         tlsLastMessage,
		"tls_ready_at":             tlsReadyAt,
		"max_stale_exceeded":       status.MaxStaleExceeded,
		"status":                   edgeHealthStatus(status),
		"healthy":                  status.Healthy,
		"draining":                 s.Config.Draining,
		"last_error":               firstNonEmpty(strings.TrimSpace(status.LastError), strings.TrimSpace(status.CaddyLastError)),
	}
	if len(performanceSamples) > 0 {
		body["performance_samples"] = performanceSamples
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, telemetry{}, fmt.Errorf("marshal edge heartbeat: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, telemetry{}, fmt.Errorf("build edge heartbeat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req, snapshot.Metrics, nil
}

func (s *Service) commitHeartbeatPerformanceBaseline(metrics telemetry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.performanceBaseline = metrics
}

func (s *Service) edgePerformanceSamplesForHeartbeat(snapshot metricSnapshot) []model.EdgePerformanceSample {
	now := time.Now().UTC()
	s.mu.Lock()
	baseline := cloneTelemetryForEdgePerformanceHeartbeat(s.performanceBaseline)
	bundle := s.bundle
	edgeID := strings.TrimSpace(s.Config.EdgeID)
	edgeGroupID := strings.TrimSpace(s.Config.EdgeGroupID)
	s.mu.Unlock()

	if bundle == nil {
		return nil
	}

	routesByKey := make(map[routeMetricKey]model.EdgeRouteBinding, len(bundle.Routes))
	fallbackRoutesByKey := make(map[routeMetricKey]model.EdgeRouteBinding, len(bundle.Routes))
	for _, route := range bundle.Routes {
		key := routeMetricKey{
			Hostname:   normalizeRouteHost(route.Hostname),
			PathPrefix: model.NormalizeAppRoutePathPrefix(route.PathPrefix),
			AppID:      strings.TrimSpace(route.AppID),
			RouteKind:  strings.TrimSpace(route.RouteKind),
		}
		if key.Hostname == "" {
			continue
		}
		if strings.TrimSpace(route.EdgeGroupID) == edgeGroupID || strings.TrimSpace(route.FallbackEdgeGroupID) == edgeGroupID {
			routesByKey[key] = route
			continue
		}
		if _, ok := fallbackRoutesByKey[key]; !ok {
			fallbackRoutesByKey[key] = route
		}
	}
	for key, route := range fallbackRoutesByKey {
		if _, ok := routesByKey[key]; !ok {
			routesByKey[key] = route
		}
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	saturation := edgePerformanceSaturationSnapshot{
		ActiveRequests:    int(atomic.LoadInt64(&s.activeProxyRequests)),
		ActiveBodyBuffers: len(s.activeRequestBodyBufferReadSnapshots(now)),
		GoroutineCount:    runtime.NumGoroutine(),
		MemoryAllocBytes:  int64(mem.Alloc),
	}
	samples := buildEdgePerformanceSamples(snapshot.Metrics, baseline, routesByKey, edgeID, edgeGroupID, now, saturation)
	if len(samples) > edgePerformanceSampleHeartbeatLimit {
		samples = samples[:edgePerformanceSampleHeartbeatLimit]
	}
	return samples
}

type edgePerformanceSaturationSnapshot struct {
	ActiveRequests    int
	ActiveBodyBuffers int
	GoroutineCount    int
	MemoryAllocBytes  int64
}

func buildEdgePerformanceSamples(current, baseline telemetry, routesByKey map[routeMetricKey]model.EdgeRouteBinding, edgeID, edgeGroupID string, sampledAt time.Time, saturation edgePerformanceSaturationSnapshot) []model.EdgePerformanceSample {
	keys := sortedRouteMetricKeys(current.RouteRequests)
	if len(keys) == 0 {
		return nil
	}
	samples := make([]model.EdgePerformanceSample, 0, len(keys))
	for _, key := range keys {
		requestCount := int(deltaUint64(current.RouteRequests[key], baseline.RouteRequests[key]))
		if requestCount <= 0 {
			continue
		}
		totalCount := int(deltaUint64(current.RouteDurationCount[key], baseline.RouteDurationCount[key]))
		totalSum := deltaFloat64(current.RouteDurationSum[key], baseline.RouteDurationSum[key])
		ttfbCount := int(deltaUint64(current.RouteTTFBCount[key], baseline.RouteTTFBCount[key]))
		ttfbSum := deltaFloat64(current.RouteTTFBSum[key], baseline.RouteTTFBSum[key])
		upstreamCount := int(deltaUint64(current.RouteLatencyCount[key], baseline.RouteLatencyCount[key]))
		upstreamSum := deltaFloat64(current.RouteLatencySum[key], baseline.RouteLatencySum[key])
		originDNSCount := int(deltaUint64(current.RouteOriginDNSCount[key], baseline.RouteOriginDNSCount[key]))
		originDNSSum := deltaFloat64(current.RouteOriginDNSSum[key], baseline.RouteOriginDNSSum[key])
		originConnectCount := int(deltaUint64(current.RouteOriginConnCount[key], baseline.RouteOriginConnCount[key]))
		originConnectSum := deltaFloat64(current.RouteOriginConnSum[key], baseline.RouteOriginConnSum[key])
		originWriteCount := int(deltaUint64(current.RouteOriginWriteCount[key], baseline.RouteOriginWriteCount[key]))
		originWriteSum := deltaFloat64(current.RouteOriginWriteSum[key], baseline.RouteOriginWriteSum[key])
		originWaitCount := int(deltaUint64(current.RouteOriginWaitCount[key], baseline.RouteOriginWaitCount[key]))
		originWaitSum := deltaFloat64(current.RouteOriginWaitSum[key], baseline.RouteOriginWaitSum[key])
		originTTFBCount := int(deltaUint64(current.RouteOriginTTFBCount[key], baseline.RouteOriginTTFBCount[key]))
		originTTFBSum := deltaFloat64(current.RouteOriginTTFBSum[key], baseline.RouteOriginTTFBSum[key])
		originTotalCount := int(deltaUint64(current.RouteOriginTotalCount[key], baseline.RouteOriginTotalCount[key]))
		originTotalSum := deltaFloat64(current.RouteOriginTotalSum[key], baseline.RouteOriginTotalSum[key])
		bodyReadCount := int(deltaUint64(current.RouteBodyReadCount[key], baseline.RouteBodyReadCount[key]))
		bodyReadSum := deltaFloat64(current.RouteBodyReadSum[key], baseline.RouteBodyReadSum[key])
		bodyWriteCount := int(deltaUint64(current.RouteBodyWriteCount[key], baseline.RouteBodyWriteCount[key]))
		bodyWriteSum := deltaFloat64(current.RouteBodyWriteSum[key], baseline.RouteBodyWriteSum[key])
		bodyThroughputCount := int(deltaUint64(current.RouteBodyThroughputCount[key], baseline.RouteBodyThroughputCount[key]))
		bodyThroughputSum := deltaFloat64(current.RouteBodyThroughputSum[key], baseline.RouteBodyThroughputSum[key])
		bodyMinThroughputCount := int(deltaUint64(current.RouteBodyMinThroughputCount[key], baseline.RouteBodyMinThroughputCount[key]))
		bodyMinThroughputSum := deltaFloat64(current.RouteBodyMinThroughputSum[key], baseline.RouteBodyMinThroughputSum[key])
		bodyMaxReadGapCount := int(deltaUint64(current.RouteBodyMaxReadGapCount[key], baseline.RouteBodyMaxReadGapCount[key]))
		bodyMaxReadGapSum := deltaFloat64(current.RouteBodyMaxReadGapSum[key], baseline.RouteBodyMaxReadGapSum[key])
		bodyRequestBytes := deltaUint64(current.RouteBodyRequestBytes[key], baseline.RouteBodyRequestBytes[key])
		bodyReadBytes := deltaUint64(current.RouteBodyReadBytes[key], baseline.RouteBodyReadBytes[key])
		bodyIncompleteCount := int(deltaUint64(current.RouteBodyIncompleteCount[key], baseline.RouteBodyIncompleteCount[key]))
		bodyReadErrorCount := int(deltaUint64(current.RouteBodyReadErrorCount[key], baseline.RouteBodyReadErrorCount[key]))
		uploadRequestCount := int(deltaUint64(current.RouteUploadRequests[key], baseline.RouteUploadRequests[key]))
		responseWriteCount := int(deltaUint64(current.RouteWriteCount[key], baseline.RouteWriteCount[key]))
		responseWriteSum := deltaFloat64(current.RouteWriteSum[key], baseline.RouteWriteSum[key])
		responseBytes := deltaUint64(current.RouteResponseBytes[key], baseline.RouteResponseBytes[key])
		clientCancelCount := int(deltaUint64(current.RouteClientCancelCount[key], baseline.RouteClientCancelCount[key]))
		streamingRequestCount := edgePerformanceRouteResultCount(current.RouteStreamingResults, baseline.RouteStreamingResults, key)
		webSocketRequestCount := edgePerformanceRouteResultCount(current.RouteWebSocketResults, baseline.RouteWebSocketResults, key)
		sseRequestCount := edgePerformanceRouteResultCount(current.RouteSSEResults, baseline.RouteSSEResults, key)

		if totalCount <= 0 {
			totalCount = requestCount
		}
		totalAvg := edgePerformanceAverageMilliseconds(totalSum, totalCount)
		ttfbAvg := edgePerformanceAverageMilliseconds(ttfbSum, ttfbCount)
		upstreamAvg := edgePerformanceAverageMilliseconds(upstreamSum, upstreamCount)
		if ttfbCount == 0 {
			ttfbAvg = totalAvg
		}
		if upstreamCount == 0 {
			upstreamAvg = 0
		}
		if totalAvg < ttfbAvg {
			totalAvg = ttfbAvg
		}

		cacheHitCount, cacheObservationCount, dominantCacheStatus := edgePerformanceCacheSummary(current.RouteCacheStatus, baseline.RouteCacheStatus, key)
		errorCount, dominantStatusCode := edgePerformanceStatusSummary(current.RouteStatuses, baseline.RouteStatuses, current.RouteUpstreamErrors, baseline.RouteUpstreamErrors, key)
		route := routesByKey[routeMetricIdentityKey(key)]
		runtimeRegion := firstNonEmpty(edgeGroupRegion(firstNonEmpty(route.RuntimeEdgeGroupID, route.RuntimeEdgeGroup)), edgeGroupRegion(strings.TrimSpace(route.EdgeGroupID)))
		trafficClass := edgePerformanceTrafficClass(key, route, uploadRequestCount, bodyReadCount, bodyRequestBytes, cacheObservationCount, streamingRequestCount, webSocketRequestCount, sseRequestCount)
		sample := model.EdgePerformanceSample{
			ID:                    edgePerformanceSampleID(edgeID, edgeGroupID, key, sampledAt),
			EdgeID:                edgeID,
			EdgeGroupID:           edgeGroupID,
			Hostname:              key.Hostname,
			PathPrefix:            model.NormalizeAppRoutePathPrefix(key.PathPrefix),
			Method:                key.Method,
			TrafficClass:          trafficClass,
			ClientCountry:         key.ClientCountry,
			ClientRegion:          key.ClientRegion,
			ClientASN:             key.ClientASN,
			RuntimeRegion:         runtimeRegion,
			RouteGeneration:       strings.TrimSpace(route.RouteGeneration),
			CacheStatus:           dominantCacheStatus,
			DNSPolicy:             edgePerformanceSampleDNSPolicy(key),
			TTFBMS:                ttfbAvg,
			UpstreamMS:            upstreamAvg,
			TotalMS:               totalAvg,
			StatusCode:            dominantStatusCode,
			SampleCount:           requestCount,
			CacheHitCount:         cacheHitCount,
			CacheObservationCount: cacheObservationCount,
			ErrorCount:            errorCount,
			UploadRequestCount:    uploadRequestCount,
			BodyBufferCount:       bodyReadCount,
			BodyReadBlockMS:       edgePerformanceAverageMilliseconds(bodyReadSum, bodyReadCount),
			FileWriteMS:           edgePerformanceAverageMilliseconds(bodyWriteSum, bodyWriteCount),
			UploadEffectiveBPS:    edgePerformanceAverageInt64(bodyThroughputSum, bodyThroughputCount),
			MinWindowBPS:          edgePerformanceAverageInt64(bodyMinThroughputSum, bodyMinThroughputCount),
			MaxReadGapMS:          edgePerformanceAverageMilliseconds(bodyMaxReadGapSum, bodyMaxReadGapCount),
			RequestBodyBytes:      int64(bodyRequestBytes),
			RequestBodyReadBytes:  int64(bodyReadBytes),
			BodyIncompleteCount:   bodyIncompleteCount,
			BodyReadErrorCount:    bodyReadErrorCount,
			ResponseWriteMS:       edgePerformanceAverageMilliseconds(responseWriteSum, responseWriteCount),
			ResponseBytes:         int64(responseBytes),
			ResponseEgressBPS:     edgePerformanceBytesPerSecond(responseBytes, responseWriteSum),
			OriginDNSMS:           edgePerformanceAverageMilliseconds(originDNSSum, originDNSCount),
			OriginConnectMS:       edgePerformanceAverageMilliseconds(originConnectSum, originConnectCount),
			OriginRequestWriteMS:  edgePerformanceAverageMilliseconds(originWriteSum, originWriteCount),
			OriginResponseWaitMS:  edgePerformanceAverageMilliseconds(originWaitSum, originWaitCount),
			OriginTTFBMS:          edgePerformanceAverageMilliseconds(originTTFBSum, originTTFBCount),
			OriginTotalMS:         edgePerformanceAverageMilliseconds(originTotalSum, originTotalCount),
			OriginFailureClass:    edgePerformanceOriginFailureClass(dominantStatusCode, errorCount, originDNSCount, originConnectCount, originWriteCount, originWaitCount, originTTFBCount),
			StreamingRequestCount: streamingRequestCount,
			WebSocketRequestCount: webSocketRequestCount,
			SSERequestCount:       sseRequestCount,
			ClientCancelCount:     clientCancelCount,
			ActiveRequests:        saturation.ActiveRequests,
			ActiveBodyBuffers:     saturation.ActiveBodyBuffers,
			GoroutineCount:        saturation.GoroutineCount,
			MemoryAllocBytes:      saturation.MemoryAllocBytes,
			SampledAt:             sampledAt,
		}
		samples = append(samples, sample)
	}
	return samples
}

func edgePerformanceSampleDNSPolicy(key routeMetricKey) string {
	if strings.TrimSpace(key.ClientCountry) != "" || strings.TrimSpace(key.ClientRegion) != "" || strings.TrimSpace(key.ClientASN) != "" {
		return "client_scope_header"
	}
	return "global"
}

func edgePerformanceTrafficClass(key routeMetricKey, route model.EdgeRouteBinding, uploadRequests, bodyBufferCount int, bodyBytes uint64, cacheObservations, streamingRequests, websocketRequests, sseRequests int) string {
	if streamingRequests > 0 || websocketRequests > 0 || sseRequests > 0 {
		return "streaming"
	}
	method := strings.ToUpper(strings.TrimSpace(key.Method))
	if method == "" {
		method = http.MethodGet
	}
	if bodyBufferCount > 0 || uploadRequests > 0 {
		if bodyBytes >= 256*1024 || bodyBufferCount > 0 {
			return "large_body_api"
		}
		return "small_api"
	}
	if method != http.MethodGet && method != http.MethodHead {
		return "small_api"
	}
	if cacheObservations > 0 {
		return "static_cacheable"
	}
	if strings.Contains(strings.ToLower(strings.TrimSpace(route.RouteKind)), "api") {
		return "small_api"
	}
	return "html_dynamic"
}

func edgePerformanceRouteResultCount(current, baseline map[routeResultMetricKey]uint64, key routeMetricKey) int {
	count := uint64(0)
	for resultKey, value := range current {
		if resultKey.RouteMetricKey != key {
			continue
		}
		count += deltaUint64(value, baseline[resultKey])
	}
	if count > uint64(^uint(0)>>1) {
		return int(^uint(0) >> 1)
	}
	return int(count)
}

func cloneTelemetryForEdgePerformanceHeartbeat(in telemetry) telemetry {
	out := in
	out.RouteRequests = cloneRouteCounterMap(in.RouteRequests)
	out.RouteStatuses = cloneRouteStatusCounterMap(in.RouteStatuses)
	out.RouteUpstreamErrors = cloneRouteCounterMap(in.RouteUpstreamErrors)
	out.RouteFallbackHits = cloneRouteCounterMap(in.RouteFallbackHits)
	out.RouteWebSocketResults = cloneRouteResultCounterMap(in.RouteWebSocketResults)
	out.RouteSSEResults = cloneRouteResultCounterMap(in.RouteSSEResults)
	out.RouteStreamingResults = cloneRouteResultCounterMap(in.RouteStreamingResults)
	out.RouteUploadRequests = cloneRouteCounterMap(in.RouteUploadRequests)
	out.RouteDurationCount = cloneRouteCounterMap(in.RouteDurationCount)
	out.RouteDurationSum = cloneRouteFloatMap(in.RouteDurationSum)
	out.RouteLatencyCount = cloneRouteCounterMap(in.RouteLatencyCount)
	out.RouteLatencySum = cloneRouteFloatMap(in.RouteLatencySum)
	out.RouteTTFBCount = cloneRouteCounterMap(in.RouteTTFBCount)
	out.RouteTTFBSum = cloneRouteFloatMap(in.RouteTTFBSum)
	out.RouteCacheLookupCount = cloneRouteCounterMap(in.RouteCacheLookupCount)
	out.RouteCacheLookupSum = cloneRouteFloatMap(in.RouteCacheLookupSum)
	out.RouteOriginConnCount = cloneRouteCounterMap(in.RouteOriginConnCount)
	out.RouteOriginConnSum = cloneRouteFloatMap(in.RouteOriginConnSum)
	out.RouteOriginDNSCount = cloneRouteCounterMap(in.RouteOriginDNSCount)
	out.RouteOriginDNSSum = cloneRouteFloatMap(in.RouteOriginDNSSum)
	out.RouteOriginTTFBCount = cloneRouteCounterMap(in.RouteOriginTTFBCount)
	out.RouteOriginTTFBSum = cloneRouteFloatMap(in.RouteOriginTTFBSum)
	out.RouteOriginTotalCount = cloneRouteCounterMap(in.RouteOriginTotalCount)
	out.RouteOriginTotalSum = cloneRouteFloatMap(in.RouteOriginTotalSum)
	out.RouteOriginWriteCount = cloneRouteCounterMap(in.RouteOriginWriteCount)
	out.RouteOriginWriteSum = cloneRouteFloatMap(in.RouteOriginWriteSum)
	out.RouteOriginWaitCount = cloneRouteCounterMap(in.RouteOriginWaitCount)
	out.RouteOriginWaitSum = cloneRouteFloatMap(in.RouteOriginWaitSum)
	out.RouteBodyReadCount = cloneRouteCounterMap(in.RouteBodyReadCount)
	out.RouteBodyReadSum = cloneRouteFloatMap(in.RouteBodyReadSum)
	out.RouteBodyWriteCount = cloneRouteCounterMap(in.RouteBodyWriteCount)
	out.RouteBodyWriteSum = cloneRouteFloatMap(in.RouteBodyWriteSum)
	out.RouteBodyThroughputCount = cloneRouteCounterMap(in.RouteBodyThroughputCount)
	out.RouteBodyThroughputSum = cloneRouteFloatMap(in.RouteBodyThroughputSum)
	out.RouteBodyMinThroughputCount = cloneRouteCounterMap(in.RouteBodyMinThroughputCount)
	out.RouteBodyMinThroughputSum = cloneRouteFloatMap(in.RouteBodyMinThroughputSum)
	out.RouteBodyMaxReadGapCount = cloneRouteCounterMap(in.RouteBodyMaxReadGapCount)
	out.RouteBodyMaxReadGapSum = cloneRouteFloatMap(in.RouteBodyMaxReadGapSum)
	out.RouteBodyRequestBytes = cloneRouteCounterMap(in.RouteBodyRequestBytes)
	out.RouteBodyReadBytes = cloneRouteCounterMap(in.RouteBodyReadBytes)
	out.RouteBodyIncompleteCount = cloneRouteCounterMap(in.RouteBodyIncompleteCount)
	out.RouteBodyReadErrorCount = cloneRouteCounterMap(in.RouteBodyReadErrorCount)
	out.RouteResponseBytes = cloneRouteCounterMap(in.RouteResponseBytes)
	out.RouteClientCancelCount = cloneRouteCounterMap(in.RouteClientCancelCount)
	out.RouteWriteCount = cloneRouteCounterMap(in.RouteWriteCount)
	out.RouteWriteSum = cloneRouteFloatMap(in.RouteWriteSum)
	out.RouteCacheStatus = cloneRouteCacheCounterMap(in.RouteCacheStatus)
	return out
}

func edgePerformanceSampleID(edgeID, edgeGroupID string, key routeMetricKey, sampledAt time.Time) string {
	payload := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%d", strings.TrimSpace(edgeID), strings.TrimSpace(edgeGroupID), key.Hostname, key.PathPrefix, key.Method, key.AppID, key.RouteKind, key.ClientCountry, key.ClientRegion, key.ClientASN, sampledAt.UnixNano())
	sum := sha256.Sum256([]byte(payload))
	return "edge_perf_" + hex.EncodeToString(sum[:])[:16]
}

func edgePerformanceAverageMilliseconds(sum float64, count int) int64 {
	if count <= 0 || sum <= 0 {
		return 0
	}
	return int64((sum * 1000) / float64(count))
}

func edgePerformanceAverageInt64(sum float64, count int) int64 {
	if count <= 0 || sum <= 0 {
		return 0
	}
	return int64(sum / float64(count))
}

func edgePerformanceBytesPerSecond(bytes uint64, seconds float64) int64 {
	if bytes == 0 || seconds <= 0 {
		return 0
	}
	return int64(float64(bytes) / seconds)
}

func edgePerformanceOriginFailureClass(statusCode, errorCount, originDNSCount, originConnectCount, originWriteCount, originWaitCount, originTTFBCount int) string {
	if errorCount <= 0 && statusCode < 500 {
		return ""
	}
	switch {
	case originDNSCount > 0 && originConnectCount == 0 && originTTFBCount == 0:
		return "origin_dns_failed"
	case originConnectCount > 0 && originTTFBCount == 0:
		return "origin_connect_failed"
	case originWriteCount > 0 && originWaitCount == 0 && originTTFBCount == 0:
		return "origin_request_write_failed"
	case originWaitCount > 0 && originTTFBCount == 0:
		return "origin_ttfb_timeout"
	case originTTFBCount > 0:
		return "origin_5xx_or_slow"
	default:
		return "origin_unavailable"
	}
}

func deltaUint64(current, baseline uint64) uint64 {
	if current < baseline {
		return current
	}
	if current == baseline {
		return 0
	}
	return current - baseline
}

func deltaFloat64(current, baseline float64) float64 {
	if current < baseline {
		return current
	}
	if current == baseline {
		return 0
	}
	return current - baseline
}

func edgePerformanceCacheSummary(current, baseline map[routeCacheMetricKey]uint64, key routeMetricKey) (int, int, string) {
	counts := make(map[string]int)
	merged := deltaCacheStatusCounts(current, baseline, key)
	order := make([]string, 0, len(merged))
	for status := range merged {
		order = append(order, status)
	}
	sort.Strings(order)
	cacheHitCount := 0
	cacheObservationCount := 0
	for _, status := range order {
		count := merged[status]
		cacheObservationCount += count
		counts[status] = count
		if status == edgeCacheStatusHit || status == edgeCacheStatusRevalidated {
			cacheHitCount += count
		}
	}
	return cacheHitCount, cacheObservationCount, dominantStringCount(counts, "bypass")
}

func deltaCacheStatusCounts(current, baseline map[routeCacheMetricKey]uint64, key routeMetricKey) map[string]int {
	out := make(map[string]int)
	for metricKey, currentValue := range current {
		if metricKey.RouteMetricKey != key {
			continue
		}
		baselineValue := baseline[metricKey]
		delta := int(deltaUint64(currentValue, baselineValue))
		if delta <= 0 {
			continue
		}
		status := strings.TrimSpace(metricKey.CacheStatus)
		if status == "" {
			status = edgeCacheStatusBypass
		}
		out[status] += delta
	}
	return out
}

func edgePerformanceStatusSummary(current, baseline map[routeStatusMetricKey]uint64, upstreamCurrent, upstreamBaseline map[routeMetricKey]uint64, key routeMetricKey) (int, int) {
	statusCounts := make(map[int]int)
	errorCount := 0
	for metricKey, currentValue := range current {
		if metricKey.RouteMetricKey != key {
			continue
		}
		delta := int(deltaUint64(currentValue, baseline[metricKey]))
		if delta <= 0 {
			continue
		}
		statusCounts[metricKey.StatusCode] += delta
		if metricKey.StatusCode >= 400 {
			errorCount += delta
		}
	}
	return errorCount, dominantStatusCodeCount(statusCounts)
}

func dominantStringCount(counts map[string]int, fallback string) string {
	bestKey := strings.TrimSpace(fallback)
	bestCount := -1
	for key, count := range counts {
		if count > bestCount || (count == bestCount && key < bestKey) {
			bestKey = key
			bestCount = count
		}
	}
	if bestKey == "" {
		return fallback
	}
	return bestKey
}

func dominantStatusCodeCount(counts map[int]int) int {
	bestCode := 0
	bestCount := -1
	for code, count := range counts {
		if count > bestCount || (count == bestCount && code < bestCode) {
			bestCode = code
			bestCount = count
		}
	}
	return bestCode
}

const edgePerformanceSampleHeartbeatLimit = 128

func (s *Service) edgeTLSHeartbeatStatus(status Status) (string, string, *time.Time) {
	if !status.CaddyEnabled {
		return "", "", nil
	}
	if lastError := strings.TrimSpace(status.CaddyLastError); lastError != "" {
		return model.EdgeTLSStatusError, lastError, nil
	}
	if strings.TrimSpace(s.Config.CaddyStaticTLSCertFile) != "" && strings.TrimSpace(s.Config.CaddyStaticTLSKeyFile) != "" {
		now := time.Now().UTC()
		return model.EdgeTLSStatusReady, "static platform certificate loaded", &now
	}
	switch s.normalizedCaddyTLSMode() {
	case caddyTLSModeInternal:
		now := time.Now().UTC()
		return model.EdgeTLSStatusReady, "caddy internal certificate cache", &now
	case caddyTLSModePublicOnDemand:
		return model.EdgeTLSStatusPending, "public on-demand TLS configured; active hostnames still require warmup", nil
	case caddyTLSModeOff:
		return model.EdgeTLSStatusPending, "TLS is disabled", nil
	default:
		return model.EdgeTLSStatusError, "unknown Caddy TLS mode", nil
	}
}

func (s *Service) heartbeatEnabled() bool {
	return strings.TrimSpace(s.Config.APIURL) != "" &&
		strings.TrimSpace(s.Config.EdgeToken) != "" &&
		strings.TrimSpace(s.Config.EdgeID) != "" &&
		strings.TrimSpace(s.Config.EdgeGroupID) != "" &&
		s.heartbeatInterval() > 0
}

func (s *Service) heartbeatInterval() time.Duration {
	if s.Config.HeartbeatInterval > 0 {
		return s.Config.HeartbeatInterval
	}
	return 30 * time.Second
}

func edgeHealthStatus(status Status) string {
	switch {
	case status.Healthy && !status.StaleCache && strings.TrimSpace(status.CaddyLastError) == "":
		return model.EdgeHealthHealthy
	case status.Healthy:
		return model.EdgeHealthDegraded
	default:
		return model.EdgeHealthUnhealthy
	}
}

func (s *Service) currentETag() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.etag
}

func (s *Service) validateConfig() error {
	if strings.TrimSpace(s.Config.APIURL) == "" {
		return fmt.Errorf("FUGUE_API_URL is required")
	}
	if strings.TrimSpace(s.Config.EdgeToken) == "" {
		return fmt.Errorf("FUGUE_EDGE_TOKEN is required")
	}
	staticTLSCertFile := strings.TrimSpace(s.Config.CaddyStaticTLSCertFile)
	staticTLSKeyFile := strings.TrimSpace(s.Config.CaddyStaticTLSKeyFile)
	if (staticTLSCertFile == "") != (staticTLSKeyFile == "") {
		return fmt.Errorf("FUGUE_EDGE_CADDY_STATIC_TLS_CERT_FILE and FUGUE_EDGE_CADDY_STATIC_TLS_KEY_FILE must be configured together")
	}
	if s.Config.CaddyEnabled {
		if strings.TrimSpace(s.Config.EdgeGroupID) == "" {
			return fmt.Errorf("FUGUE_EDGE_GROUP_ID is required when caddy mode is enabled")
		}
		if strings.TrimSpace(s.Config.CaddyListenAddr) == "" {
			return fmt.Errorf("FUGUE_EDGE_CADDY_LISTEN_ADDR is required when caddy mode is enabled")
		}
		if strings.TrimSpace(s.Config.CaddyProxyListenAddr) == "" {
			return fmt.Errorf("FUGUE_EDGE_PROXY_LISTEN_ADDR is required when caddy mode is enabled")
		}
		if !edgeProxyListenAddrIsLoopback(s.Config.CaddyProxyListenAddr) {
			return fmt.Errorf("FUGUE_EDGE_PROXY_LISTEN_ADDR must bind an explicit loopback IP when caddy mode is enabled")
		}
		switch s.normalizedCaddyTLSMode() {
		case caddyTLSModeOff, caddyTLSModeInternal:
		case caddyTLSModePublicOnDemand:
			if _, err := s.normalizedCaddyTLSAskURL(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("FUGUE_EDGE_CADDY_TLS_MODE must be off, internal, or public-on-demand")
		}
		if staticTLSCertFile != "" && s.normalizedCaddyTLSMode() == caddyTLSModeOff {
			return fmt.Errorf("FUGUE_EDGE_CADDY_STATIC_TLS_CERT_FILE requires FUGUE_EDGE_CADDY_TLS_MODE to be internal or public-on-demand")
		}
		if _, err := s.caddyAdminEndpoint("/load"); err != nil {
			return err
		}
	} else if staticTLSCertFile != "" {
		return fmt.Errorf("FUGUE_EDGE_CADDY_STATIC_TLS_CERT_FILE requires FUGUE_EDGE_CADDY_ENABLED=true")
	}
	return nil
}

func edgeProxyListenAddrIsLoopback(raw string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	ip := net.ParseIP(strings.TrimSpace(host))
	return ip != nil && ip.IsLoopback()
}

func (s *Service) routeAllowedForThisEdge(route model.EdgeRouteBinding) bool {
	// Edge group IDs steer DNS and telemetry. They are not a data-plane
	// authorization boundary; every edge should be able to serve every
	// edge-enabled public route when traffic lands there.
	return model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy)
}

func routeMatchesCurrentEdgeGroup(route model.EdgeRouteBinding, edgeGroupID string) bool {
	edgeGroupID = strings.TrimSpace(edgeGroupID)
	if edgeGroupID == "" {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(route.EdgeGroupID), edgeGroupID) ||
		strings.EqualFold(strings.TrimSpace(route.FallbackEdgeGroupID), edgeGroupID)
}

func (s *Service) writeCache(cached cacheFile) error {
	path := strings.TrimSpace(s.Config.CachePath)
	if path == "" {
		return nil
	}
	data, err := json.MarshalIndent(cached, "", "  ")
	if err != nil {
		s.recordCacheWrite("error")
		return fmt.Errorf("marshal edge route cache: %w", err)
	}
	s.preservePreviousCache(path)
	if err := lkgcache.WriteCurrent(path, edgeCacheGeneration(cached.Bundle), data, s.cacheArchiveLimit()); err != nil {
		s.recordCacheWrite("error")
		return fmt.Errorf("replace edge route cache: %w", err)
	}
	s.recordCacheWrite("success")
	s.recordEdgeLKGWriteWAL(cached)
	return nil
}

func (s *Service) LoadPreviousCache() error {
	cachePath := strings.TrimSpace(s.Config.CachePath)
	if cachePath == "" {
		return fmt.Errorf("previous edge route cache path is not configured")
	}
	candidates := lkgcache.FallbackCandidates(cachePath)
	if len(candidates) == 0 {
		s.recordCacheLoad("miss")
		return os.ErrNotExist
	}
	var lastErr error
	for _, candidate := range candidates {
		var cached cacheFile
		if err := json.Unmarshal(candidate.Data, &cached); err != nil {
			lastErr = fmt.Errorf("decode previous edge route cache %s: %w", candidate.Path, err)
			s.recordCacheLoad("error")
			continue
		}
		if cached.Bundle.Version == "" {
			lastErr = fmt.Errorf("previous edge route cache %s missing bundle version", candidate.Path)
			s.recordCacheLoad("error")
			continue
		}
		if err := s.verifyCachedBundle(cached.Bundle, time.Now().UTC()); err != nil {
			lastErr = fmt.Errorf("verify previous edge route cache %s: %w", candidate.Path, err)
			s.recordCacheLoad("error")
			continue
		}
		s.recordCacheLoaded(cached)
		s.recordCacheLoad("success")
		s.recordEdgeServeLKGWAL("load_previous_cache", nil)
		if s.Logger != nil {
			s.Logger.Printf("edge route previous cache loaded; version=%s etag=%s path=%s", cached.Bundle.Version, cached.ETag, candidate.Path)
		}
		return nil
	}
	if lastErr == nil {
		lastErr = os.ErrNotExist
	}
	return lastErr
}

func (s *Service) preservePreviousCache(path string) {
	err := lkgcache.PreservePrevious(path, func(data []byte) bool {
		var cached cacheFile
		if err := json.Unmarshal(data, &cached); err != nil || cached.Bundle.Version == "" {
			return false
		}
		return s.verifyCachedBundle(cached.Bundle, time.Now().UTC()) == nil
	})
	if err != nil && !os.IsNotExist(err) && s.Logger != nil {
		s.Logger.Printf("preserve previous edge route cache failed: %v", err)
	}
}

func previousCachePath(path string) string {
	return lkgcache.PreviousPath(path)
}

func (s *Service) verifyBundle(bundle model.EdgeRouteBundle, now time.Time) error {
	keyring := bundleauth.NewKeyring(
		s.Config.BundleSigningKey,
		s.Config.BundleSigningKeyID,
		s.Config.BundleSigningPreviousKey,
		s.Config.BundleSigningPreviousKeyID,
		s.Config.BundleRevokedKeyIDs,
	)
	if err := bundleauth.VerifyEdgeRouteBundleWithKeyring(bundle, keyring, now); err != nil {
		return err
	}
	if strings.TrimSpace(bundle.Version) == "" {
		return fmt.Errorf("edge route bundle version is required")
	}
	return nil
}

func (s *Service) verifyCachedBundle(bundle model.EdgeRouteBundle, now time.Time) error {
	verifyAt, err := staleBundleVerificationTime(bundle.ValidUntil, now, s.Config.MaxStale)
	if err != nil {
		return err
	}
	return s.verifyBundle(bundle, verifyAt)
}

func staleBundleVerificationTime(validUntil, now time.Time, maxStale time.Duration) (time.Time, error) {
	if validUntil.IsZero() || now.IsZero() || !now.After(validUntil) {
		return now, nil
	}
	if maxStale <= 0 || now.Sub(validUntil) > maxStale {
		return now, bundleauth.ErrExpiredBundle
	}
	return validUntil.Add(-time.Nanosecond), nil
}

func (s *Service) cacheArchiveLimit() int {
	if s.Config.CacheArchiveLimit <= 0 {
		return 5
	}
	return s.Config.CacheArchiveLimit
}

func edgeCacheGeneration(bundle model.EdgeRouteBundle) string {
	return firstNonEmpty(bundle.Generation, bundle.Version)
}

func (s *Service) recordCacheLoaded(cached cacheFile) {
	bundle := cached.Bundle
	s.mu.Lock()
	s.bundle = &bundle
	s.etag = strings.TrimSpace(cached.ETag)
	if s.etag == "" {
		s.etag = quoteETag(bundle.Version)
	}
	s.snapshot = s.statusForBundleLocked(bundle, cached.CachedAt, nil, true)
	s.mu.Unlock()
	s.reconcileEdgeProxyTransports(bundle)
}

func (s *Service) recordCacheCorruptGeneration(generation string) {
	generation = strings.TrimSpace(generation)
	if generation == "" {
		generation = "unknown"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.CacheCorruptGeneration = generation
}

func (s *Service) recordSyncSuccess(bundle model.EdgeRouteBundle, etag string, now time.Time, stale bool) {
	s.mu.Lock()
	s.bundle = &bundle
	s.etag = strings.TrimSpace(etag)
	s.snapshot = s.statusForBundleLocked(bundle, now, &now, stale)
	s.mu.Unlock()
	s.reconcileEdgeProxyTransports(bundle)
}

func (s *Service) recordNotModified(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bundle == nil {
		err := fmt.Errorf("edge routes returned 304 without a cached bundle")
		s.snapshot.LastSyncAt = &now
		s.snapshot.LastError = err.Error()
		s.snapshot.Status = "unhealthy"
		s.snapshot.Healthy = false
		return
	}
	bundle := *s.bundle
	s.snapshot = s.statusForBundleLocked(bundle, now, &now, false)
}

func (s *Service) recordSyncError(err error) {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	message := s.redact(err.Error())
	s.snapshot.LastSyncAt = &now
	s.snapshot.LastError = message
	if s.bundle != nil {
		s.snapshot.StaleCache = true
		s.snapshot.Status = "stale"
		s.snapshot.Healthy = true
		if !s.bundle.ValidUntil.IsZero() && now.After(s.bundle.ValidUntil) {
			s.snapshot.Status = "degraded"
			s.snapshot.DegradedReason = "route bundle valid_until expired"
			if s.maxStaleExceeded(s.bundle.ValidUntil, now) {
				s.snapshot.Status = "unhealthy"
				s.snapshot.Healthy = false
				s.snapshot.MaxStaleExceeded = true
				s.snapshot.DegradedReason = "route bundle valid_until exceeded max_stale"
			}
		} else if strings.TrimSpace(s.snapshot.DegradedReason) == "" {
			s.snapshot.DegradedReason = "route bundle sync failed; serving cache"
		}
		s.decorateCaddyStatusLocked(&s.snapshot)
		return
	}
	s.snapshot.Status = "unhealthy"
	s.snapshot.Healthy = false
	s.decorateCaddyStatusLocked(&s.snapshot)
}

func (s *Service) recordSyncAttempt(result string, duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch result {
	case "success":
		s.metrics.BundleSyncSuccess++
	case "not_modified":
		s.metrics.BundleSyncNotModified++
	default:
		s.metrics.BundleSyncError++
	}
	s.metrics.LastSyncDuration = duration
}

func (s *Service) recordCacheWrite(result string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch result {
	case "success":
		s.metrics.CacheWriteSuccess++
	default:
		s.metrics.CacheWriteError++
	}
}

func (s *Service) recordCacheLoad(result string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch result {
	case "success":
		s.metrics.CacheLoadSuccess++
	case "miss":
		s.metrics.CacheLoadMiss++
	default:
		s.metrics.CacheLoadError++
	}
}

func (s *Service) recordCaddyApply(bundleVersion string, routeCount int, configSignature string, err error) {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.metrics.CaddyConfigError++
		s.metrics.CaddyLastError = s.redact(err.Error())
		s.snapshot.CaddyLastError = s.metrics.CaddyLastError
		s.snapshot.Status = "caddy-error"
		s.snapshot.Healthy = false
		return
	}
	s.metrics.CaddyConfigSuccess++
	s.metrics.CaddyAppliedVersion = strings.TrimSpace(bundleVersion)
	s.metrics.CaddyAppliedSignature = strings.TrimSpace(configSignature)
	s.metrics.CaddyRouteCount = routeCount
	s.metrics.CaddyLastApplyAt = &now
	s.metrics.CaddyLastError = ""
	s.snapshot.CaddyEnabled = s.Config.CaddyEnabled
	s.snapshot.CaddyListenAddr = strings.TrimSpace(s.Config.CaddyListenAddr)
	s.snapshot.CaddyTLSMode = s.normalizedCaddyTLSMode()
	s.snapshot.CaddyAppliedVersion = s.metrics.CaddyAppliedVersion
	s.snapshot.CaddyLastApplyAt = &now
	s.snapshot.CaddyLastError = ""
	if s.snapshot.Status == "caddy-error" {
		s.snapshot.Status = "ok"
		s.snapshot.Healthy = s.bundle != nil
	}
}

func (s *Service) needsCaddyWarmup(signature string) bool {
	signature = strings.TrimSpace(signature)
	if signature == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(s.metrics.CaddyWarmupSignature) != signature {
		return true
	}
	if strings.TrimSpace(s.metrics.CaddyWarmupLastError) != "" {
		return true
	}
	return s.metrics.CaddyWarmupAt == nil
}

func (s *Service) recordCaddyWarmup(signature, host string, duration time.Duration, err error) {
	if duration < 0 {
		duration = 0
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics.CaddyWarmupSignature = strings.TrimSpace(signature)
	s.metrics.CaddyWarmupHost = strings.TrimSpace(host)
	s.metrics.CaddyWarmupDuration = duration
	s.metrics.CaddyWarmupAt = &now
	if err != nil {
		s.metrics.CaddyWarmupError++
		s.metrics.CaddyWarmupLastError = s.redact(err.Error())
		return
	}
	s.metrics.CaddyWarmupSuccess++
	s.metrics.CaddyWarmupLastError = ""
}

func (s *Service) recordProxyObservation(observed edgeProxyObservation) {
	if observed.StatusCode == 0 {
		observed.StatusCode = http.StatusOK
	}
	if observed.TTFB <= 0 {
		observed.TTFB = observed.Duration
	}
	if observed.Upstream <= 0 && observed.Proxied {
		observed.Upstream = observed.Duration
	}
	key := routeMetricKey{
		Hostname:      firstNonEmpty(strings.TrimSpace(observed.Route.Hostname), observed.Host),
		PathPrefix:    model.NormalizeAppRoutePathPrefix(observed.Route.PathPrefix),
		Method:        strings.ToUpper(strings.TrimSpace(observed.Method)),
		AppID:         strings.TrimSpace(observed.Route.AppID),
		RouteKind:     strings.TrimSpace(observed.Route.RouteKind),
		ClientCountry: strings.ToLower(strings.TrimSpace(observed.ClientCountry)),
		ClientRegion:  strings.TrimSpace(observed.ClientRegion),
		ClientASN:     strings.TrimSpace(observed.ClientASN),
	}
	statusKey := routeStatusMetricKey{RouteMetricKey: key, StatusCode: observed.StatusCode}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metrics.RouteRequests == nil {
		s.metrics.RouteRequests = make(map[routeMetricKey]uint64)
	}
	if s.metrics.RouteStatuses == nil {
		s.metrics.RouteStatuses = make(map[routeStatusMetricKey]uint64)
	}
	s.metrics.RouteRequests[key]++
	s.metrics.RouteStatuses[statusKey]++
	if observed.FallbackHit {
		if s.metrics.RouteFallbackHits == nil {
			s.metrics.RouteFallbackHits = make(map[routeMetricKey]uint64)
		}
		s.metrics.RouteFallbackHits[key]++
	}
	if strings.TrimSpace(observed.UpstreamError) != "" && !observed.ClientCanceled {
		if s.metrics.RouteUpstreamErrors == nil {
			s.metrics.RouteUpstreamErrors = make(map[routeMetricKey]uint64)
		}
		s.metrics.RouteUpstreamErrors[key]++
	}
	if observed.ClientCanceled {
		if s.metrics.RouteClientCancelCount == nil {
			s.metrics.RouteClientCancelCount = make(map[routeMetricKey]uint64)
		}
		s.metrics.RouteClientCancelCount[key]++
	}
	result := edgeProxyObservationResult(observed)
	if observed.WebSocket {
		if s.metrics.RouteWebSocketResults == nil {
			s.metrics.RouteWebSocketResults = make(map[routeResultMetricKey]uint64)
		}
		s.metrics.RouteWebSocketResults[routeResultMetricKey{RouteMetricKey: key, Result: result}]++
	}
	if observed.SSE {
		if s.metrics.RouteSSEResults == nil {
			s.metrics.RouteSSEResults = make(map[routeResultMetricKey]uint64)
		}
		s.metrics.RouteSSEResults[routeResultMetricKey{RouteMetricKey: key, Result: result}]++
	}
	if observed.Streaming {
		if s.metrics.RouteStreamingResults == nil {
			s.metrics.RouteStreamingResults = make(map[routeResultMetricKey]uint64)
		}
		s.metrics.RouteStreamingResults[routeResultMetricKey{RouteMetricKey: key, Result: result}]++
	}
	if observed.Upload {
		if s.metrics.RouteUploadRequests == nil {
			s.metrics.RouteUploadRequests = make(map[routeMetricKey]uint64)
		}
		s.metrics.RouteUploadRequests[key]++
	}
	if observed.RequestBodyBuffered {
		if s.metrics.RouteBodyRequestBytes == nil {
			s.metrics.RouteBodyRequestBytes = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteBodyReadBytes == nil {
			s.metrics.RouteBodyReadBytes = make(map[routeMetricKey]uint64)
		}
		if observed.RequestBytes > 0 {
			s.metrics.RouteBodyRequestBytes[key] += uint64(observed.RequestBytes)
		}
		if observed.RequestBodyReadBytes > 0 {
			s.metrics.RouteBodyReadBytes[key] += uint64(observed.RequestBodyReadBytes)
		}
		if observed.RequestBytes > 0 && observed.RequestBodyReadBytes < observed.RequestBytes {
			if s.metrics.RouteBodyIncompleteCount == nil {
				s.metrics.RouteBodyIncompleteCount = make(map[routeMetricKey]uint64)
			}
			s.metrics.RouteBodyIncompleteCount[key]++
		}
		if strings.TrimSpace(observed.RequestBodyReadError) != "" || strings.TrimSpace(observed.RequestBodyBufferError) != "" {
			if s.metrics.RouteBodyReadErrorCount == nil {
				s.metrics.RouteBodyReadErrorCount = make(map[routeMetricKey]uint64)
			}
			s.metrics.RouteBodyReadErrorCount[key]++
		}
		if s.metrics.RouteBodyReadCount == nil {
			s.metrics.RouteBodyReadCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteBodyReadSum == nil {
			s.metrics.RouteBodyReadSum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteBodyReadCount[key]++
		s.metrics.RouteBodyReadSum[key] += durationSeconds(observed.BodyReadBlock)
		if s.metrics.RouteBodyWriteCount == nil {
			s.metrics.RouteBodyWriteCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteBodyWriteSum == nil {
			s.metrics.RouteBodyWriteSum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteBodyWriteCount[key]++
		s.metrics.RouteBodyWriteSum[key] += durationSeconds(observed.FileWrite)
		if observed.AvgBPS > 0 {
			if s.metrics.RouteBodyThroughputCount == nil {
				s.metrics.RouteBodyThroughputCount = make(map[routeMetricKey]uint64)
			}
			if s.metrics.RouteBodyThroughputSum == nil {
				s.metrics.RouteBodyThroughputSum = make(map[routeMetricKey]float64)
			}
			s.metrics.RouteBodyThroughputCount[key]++
			s.metrics.RouteBodyThroughputSum[key] += float64(observed.AvgBPS)
		}
		if observed.MinWindowBPS > 0 {
			if s.metrics.RouteBodyMinThroughputCount == nil {
				s.metrics.RouteBodyMinThroughputCount = make(map[routeMetricKey]uint64)
			}
			if s.metrics.RouteBodyMinThroughputSum == nil {
				s.metrics.RouteBodyMinThroughputSum = make(map[routeMetricKey]float64)
			}
			s.metrics.RouteBodyMinThroughputCount[key]++
			s.metrics.RouteBodyMinThroughputSum[key] += float64(observed.MinWindowBPS)
		}
		if observed.MaxReadGap > 0 {
			if s.metrics.RouteBodyMaxReadGapCount == nil {
				s.metrics.RouteBodyMaxReadGapCount = make(map[routeMetricKey]uint64)
			}
			if s.metrics.RouteBodyMaxReadGapSum == nil {
				s.metrics.RouteBodyMaxReadGapSum = make(map[routeMetricKey]float64)
			}
			s.metrics.RouteBodyMaxReadGapCount[key]++
			s.metrics.RouteBodyMaxReadGapSum[key] += durationSeconds(observed.MaxReadGap)
		}
	}
	if s.metrics.RouteDurationCount == nil {
		s.metrics.RouteDurationCount = make(map[routeMetricKey]uint64)
	}
	if s.metrics.RouteDurationSum == nil {
		s.metrics.RouteDurationSum = make(map[routeMetricKey]float64)
	}
	s.metrics.RouteDurationCount[key]++
	s.metrics.RouteDurationSum[key] += durationSeconds(observed.Duration)
	if observed.CacheLookup > 0 {
		if s.metrics.RouteCacheLookupCount == nil {
			s.metrics.RouteCacheLookupCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteCacheLookupSum == nil {
			s.metrics.RouteCacheLookupSum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteCacheLookupCount[key]++
		s.metrics.RouteCacheLookupSum[key] += durationSeconds(observed.CacheLookup)
	}
	if observed.OriginDNS > 0 {
		if s.metrics.RouteOriginDNSCount == nil {
			s.metrics.RouteOriginDNSCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteOriginDNSSum == nil {
			s.metrics.RouteOriginDNSSum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteOriginDNSCount[key]++
		s.metrics.RouteOriginDNSSum[key] += durationSeconds(observed.OriginDNS)
	}
	if observed.Proxied {
		if s.metrics.RouteLatencyCount == nil {
			s.metrics.RouteLatencyCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteLatencySum == nil {
			s.metrics.RouteLatencySum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteLatencyCount[key]++
		s.metrics.RouteLatencySum[key] += durationSeconds(observed.Upstream)
		if s.metrics.RouteTTFBCount == nil {
			s.metrics.RouteTTFBCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteTTFBSum == nil {
			s.metrics.RouteTTFBSum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteTTFBCount[key]++
		s.metrics.RouteTTFBSum[key] += durationSeconds(observed.TTFB)
	}
	if observed.OriginConnect > 0 {
		if s.metrics.RouteOriginConnCount == nil {
			s.metrics.RouteOriginConnCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteOriginConnSum == nil {
			s.metrics.RouteOriginConnSum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteOriginConnCount[key]++
		s.metrics.RouteOriginConnSum[key] += durationSeconds(observed.OriginConnect)
	}
	if observed.OriginRequestWrite > 0 {
		if s.metrics.RouteOriginWriteCount == nil {
			s.metrics.RouteOriginWriteCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteOriginWriteSum == nil {
			s.metrics.RouteOriginWriteSum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteOriginWriteCount[key]++
		s.metrics.RouteOriginWriteSum[key] += durationSeconds(observed.OriginRequestWrite)
	}
	if wait := originResponseHeaderWait(observed); wait > 0 {
		if s.metrics.RouteOriginWaitCount == nil {
			s.metrics.RouteOriginWaitCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteOriginWaitSum == nil {
			s.metrics.RouteOriginWaitSum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteOriginWaitCount[key]++
		s.metrics.RouteOriginWaitSum[key] += durationSeconds(wait)
	}
	if observed.OriginTTFB > 0 {
		if s.metrics.RouteOriginTTFBCount == nil {
			s.metrics.RouteOriginTTFBCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteOriginTTFBSum == nil {
			s.metrics.RouteOriginTTFBSum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteOriginTTFBCount[key]++
		s.metrics.RouteOriginTTFBSum[key] += durationSeconds(observed.OriginTTFB)
	}
	if observed.OriginTotal > 0 {
		if s.metrics.RouteOriginTotalCount == nil {
			s.metrics.RouteOriginTotalCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteOriginTotalSum == nil {
			s.metrics.RouteOriginTotalSum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteOriginTotalCount[key]++
		s.metrics.RouteOriginTotalSum[key] += durationSeconds(observed.OriginTotal)
	}
	if observed.ResponseWrite > 0 {
		if s.metrics.RouteWriteCount == nil {
			s.metrics.RouteWriteCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteWriteSum == nil {
			s.metrics.RouteWriteSum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteWriteCount[key]++
		s.metrics.RouteWriteSum[key] += durationSeconds(observed.ResponseWrite)
	}
	if observed.ResponseBytes > 0 {
		if s.metrics.RouteResponseBytes == nil {
			s.metrics.RouteResponseBytes = make(map[routeMetricKey]uint64)
		}
		s.metrics.RouteResponseBytes[key] += uint64(observed.ResponseBytes)
	}
	if s.metrics.RouteCacheStatus == nil {
		s.metrics.RouteCacheStatus = make(map[routeCacheMetricKey]uint64)
	}
	s.metrics.RouteCacheStatus[routeCacheMetricKey{
		RouteMetricKey: key,
		CacheStatus:    firstNonEmpty(strings.TrimSpace(observed.CacheStatus), "bypass"),
		CachePolicyID:  strings.TrimSpace(observed.CachePolicyID),
		AssetClass:     strings.TrimSpace(observed.AssetClass),
	}]++
}

func edgeProxyObservationResult(observed edgeProxyObservation) string {
	if observed.ClientCanceled {
		return "client_closed"
	}
	if strings.TrimSpace(observed.UpstreamError) != "" {
		return "error"
	}
	if observed.StatusCode >= 200 && observed.StatusCode < 400 {
		return "success"
	}
	if observed.WebSocket && observed.StatusCode == http.StatusSwitchingProtocols {
		return "success"
	}
	return "error"
}

func (s *Service) statusForBundleLocked(bundle model.EdgeRouteBundle, syncAt time.Time, successAt *time.Time, stale bool) Status {
	status := "ok"
	if stale {
		status = "stale"
	}
	validUntil := bundle.ValidUntil
	degradedReason := ""
	if !validUntil.IsZero() && time.Now().UTC().After(validUntil) {
		status = "degraded"
		degradedReason = "route bundle valid_until expired"
		stale = true
	}
	now := time.Now().UTC()
	maxStaleExceeded := s.maxStaleExceeded(validUntil, now)
	healthy := true
	if maxStaleExceeded {
		status = "unhealthy"
		healthy = false
		degradedReason = "route bundle valid_until exceeded max_stale"
		stale = true
	}
	generation := edgeCacheGeneration(bundle)
	out := Status{
		Status:             status,
		Healthy:            healthy,
		EdgeID:             strings.TrimSpace(s.Config.EdgeID),
		EdgeGroupID:        strings.TrimSpace(s.Config.EdgeGroupID),
		BundleVersion:      bundle.Version,
		ServingGeneration:  generation,
		LKGGeneration:      generation,
		LastGoodGeneration: generation,
		RouteCount:         len(bundle.Routes),
		TLSAllowlistCount:  len(bundle.TLSAllowlist),
		LastSyncAt:         &syncAt,
		LastSuccessAt:      successAt,
		DegradedReason:     degradedReason,
		StaleCache:         stale,
		MaxStaleExceeded:   maxStaleExceeded,
		CachePath:          strings.TrimSpace(s.Config.CachePath),
	}
	if !validUntil.IsZero() {
		out.BundleValidUntil = &validUntil
	}
	s.decorateCaddyStatusLocked(&out)
	return out
}

func (s *Service) maxStaleExceeded(validUntil, now time.Time) bool {
	maxStale := s.Config.MaxStale
	if maxStale <= 0 || validUntil.IsZero() || now.IsZero() || !now.After(validUntil) {
		return false
	}
	return now.Sub(validUntil) > maxStale
}

func (s *Service) decorateCaddyStatusLocked(out *Status) {
	out.CaddyEnabled = s.Config.CaddyEnabled
	if !s.Config.CaddyEnabled {
		return
	}
	out.CaddyListenAddr = strings.TrimSpace(s.Config.CaddyListenAddr)
	out.CaddyTLSMode = s.normalizedCaddyTLSMode()
	out.CaddyAppliedVersion = strings.TrimSpace(s.metrics.CaddyAppliedVersion)
	out.CaddyLastApplyAt = s.metrics.CaddyLastApplyAt
	out.CaddyLastError = strings.TrimSpace(s.metrics.CaddyLastError)
	if out.CaddyLastError != "" {
		if strings.TrimSpace(out.CaddyAppliedVersion) != "" {
			out.Status = "caddy-degraded"
			out.DegradedReason = "caddy reload failed; last applied config retained"
			return
		}
		out.Status = "caddy-error"
		out.Healthy = false
	}
}

func (s *Service) metricSnapshot() metricSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := metricSnapshot{
		Status:  s.snapshot,
		Metrics: s.metrics,
	}
	out.Metrics.RouteRequests = cloneRouteCounterMap(s.metrics.RouteRequests)
	out.Metrics.RouteStatuses = cloneRouteStatusCounterMap(s.metrics.RouteStatuses)
	out.Metrics.RouteUpstreamErrors = cloneRouteCounterMap(s.metrics.RouteUpstreamErrors)
	out.Metrics.RouteFallbackHits = cloneRouteCounterMap(s.metrics.RouteFallbackHits)
	out.Metrics.RouteWebSocketResults = cloneRouteResultCounterMap(s.metrics.RouteWebSocketResults)
	out.Metrics.RouteSSEResults = cloneRouteResultCounterMap(s.metrics.RouteSSEResults)
	out.Metrics.RouteStreamingResults = cloneRouteResultCounterMap(s.metrics.RouteStreamingResults)
	out.Metrics.RouteUploadRequests = cloneRouteCounterMap(s.metrics.RouteUploadRequests)
	out.Metrics.RouteDurationCount = cloneRouteCounterMap(s.metrics.RouteDurationCount)
	out.Metrics.RouteDurationSum = cloneRouteFloatMap(s.metrics.RouteDurationSum)
	out.Metrics.RouteLatencyCount = cloneRouteCounterMap(s.metrics.RouteLatencyCount)
	out.Metrics.RouteLatencySum = cloneRouteFloatMap(s.metrics.RouteLatencySum)
	out.Metrics.RouteTTFBCount = cloneRouteCounterMap(s.metrics.RouteTTFBCount)
	out.Metrics.RouteTTFBSum = cloneRouteFloatMap(s.metrics.RouteTTFBSum)
	out.Metrics.RouteCacheLookupCount = cloneRouteCounterMap(s.metrics.RouteCacheLookupCount)
	out.Metrics.RouteCacheLookupSum = cloneRouteFloatMap(s.metrics.RouteCacheLookupSum)
	out.Metrics.RouteOriginConnCount = cloneRouteCounterMap(s.metrics.RouteOriginConnCount)
	out.Metrics.RouteOriginConnSum = cloneRouteFloatMap(s.metrics.RouteOriginConnSum)
	out.Metrics.RouteOriginTTFBCount = cloneRouteCounterMap(s.metrics.RouteOriginTTFBCount)
	out.Metrics.RouteOriginTTFBSum = cloneRouteFloatMap(s.metrics.RouteOriginTTFBSum)
	out.Metrics.RouteOriginTotalCount = cloneRouteCounterMap(s.metrics.RouteOriginTotalCount)
	out.Metrics.RouteOriginTotalSum = cloneRouteFloatMap(s.metrics.RouteOriginTotalSum)
	out.Metrics.RouteOriginDNSCount = cloneRouteCounterMap(s.metrics.RouteOriginDNSCount)
	out.Metrics.RouteOriginDNSSum = cloneRouteFloatMap(s.metrics.RouteOriginDNSSum)
	out.Metrics.RouteOriginWriteCount = cloneRouteCounterMap(s.metrics.RouteOriginWriteCount)
	out.Metrics.RouteOriginWriteSum = cloneRouteFloatMap(s.metrics.RouteOriginWriteSum)
	out.Metrics.RouteOriginWaitCount = cloneRouteCounterMap(s.metrics.RouteOriginWaitCount)
	out.Metrics.RouteOriginWaitSum = cloneRouteFloatMap(s.metrics.RouteOriginWaitSum)
	out.Metrics.RouteBodyReadCount = cloneRouteCounterMap(s.metrics.RouteBodyReadCount)
	out.Metrics.RouteBodyReadSum = cloneRouteFloatMap(s.metrics.RouteBodyReadSum)
	out.Metrics.RouteBodyWriteCount = cloneRouteCounterMap(s.metrics.RouteBodyWriteCount)
	out.Metrics.RouteBodyWriteSum = cloneRouteFloatMap(s.metrics.RouteBodyWriteSum)
	out.Metrics.RouteBodyThroughputCount = cloneRouteCounterMap(s.metrics.RouteBodyThroughputCount)
	out.Metrics.RouteBodyThroughputSum = cloneRouteFloatMap(s.metrics.RouteBodyThroughputSum)
	out.Metrics.RouteBodyMinThroughputCount = cloneRouteCounterMap(s.metrics.RouteBodyMinThroughputCount)
	out.Metrics.RouteBodyMinThroughputSum = cloneRouteFloatMap(s.metrics.RouteBodyMinThroughputSum)
	out.Metrics.RouteBodyMaxReadGapCount = cloneRouteCounterMap(s.metrics.RouteBodyMaxReadGapCount)
	out.Metrics.RouteBodyMaxReadGapSum = cloneRouteFloatMap(s.metrics.RouteBodyMaxReadGapSum)
	out.Metrics.RouteBodyRequestBytes = cloneRouteCounterMap(s.metrics.RouteBodyRequestBytes)
	out.Metrics.RouteBodyReadBytes = cloneRouteCounterMap(s.metrics.RouteBodyReadBytes)
	out.Metrics.RouteBodyIncompleteCount = cloneRouteCounterMap(s.metrics.RouteBodyIncompleteCount)
	out.Metrics.RouteBodyReadErrorCount = cloneRouteCounterMap(s.metrics.RouteBodyReadErrorCount)
	out.Metrics.RouteResponseBytes = cloneRouteCounterMap(s.metrics.RouteResponseBytes)
	out.Metrics.RouteClientCancelCount = cloneRouteCounterMap(s.metrics.RouteClientCancelCount)
	out.Metrics.RouteWriteCount = cloneRouteCounterMap(s.metrics.RouteWriteCount)
	out.Metrics.RouteWriteSum = cloneRouteFloatMap(s.metrics.RouteWriteSum)
	out.Metrics.RouteCacheStatus = cloneRouteCacheCounterMap(s.metrics.RouteCacheStatus)
	if s.bundle != nil && !s.bundle.GeneratedAt.IsZero() {
		generatedAt := s.bundle.GeneratedAt
		out.BundleGeneratedAt = &generatedAt
	}
	return out
}

func (s *Service) logCacheLoaded(cached cacheFile) {
	if s.Logger == nil {
		return
	}
	s.Logger.Printf("edge route cache loaded; version=%s etag=%s cached_at=%s routes=%d tls_allowlist=%d path=%s", cached.Bundle.Version, cached.ETag, cached.CachedAt.Format(time.RFC3339Nano), len(cached.Bundle.Routes), len(cached.Bundle.TLSAllowlist), strings.TrimSpace(s.Config.CachePath))
}

func (s *Service) logCacheMiss(path string) {
	if s.Logger == nil {
		return
	}
	s.Logger.Printf("edge route cache miss; path=%s", path)
}

func (s *Service) logSyncSuccess(bundleVersion string, routes int, tlsAllowlist int, duration time.Duration) {
	if s.Logger == nil {
		return
	}
	s.Logger.Printf("edge route bundle sync success; version=%s routes=%d tls_allowlist=%d duration_ms=%d", bundleVersion, routes, tlsAllowlist, duration.Milliseconds())
}

func (s *Service) logSyncNotModified(bundleVersion string, duration time.Duration) {
	if s.Logger == nil {
		return
	}
	s.Logger.Printf("edge route bundle sync not_modified; version=%s duration_ms=%d", bundleVersion, duration.Milliseconds())
}

func (s *Service) logSyncFailure(err error) {
	if s.Logger == nil || err == nil {
		return
	}
	status := s.Status()
	if status.Healthy && status.StaleCache {
		s.Logger.Printf("edge route bundle sync failed; using stale cache; version=%s error=%s", status.BundleVersion, s.redact(err.Error()))
		return
	}
	s.Logger.Printf("edge route bundle sync failed; error=%s", s.redact(err.Error()))
}

func (s *Service) logHeartbeatFailure(err error) {
	if s.Logger == nil || err == nil {
		return
	}
	s.Logger.Printf("edge heartbeat failed; error=%s", s.redact(err.Error()))
}

func (s *Service) logProxyObservation(observed edgeProxyObservation) {
	if s.Logger == nil {
		return
	}
	if observed.StatusCode == 0 {
		observed.StatusCode = http.StatusOK
	}
	edgeGroupID := firstNonEmpty(observed.Route.EdgeGroupID, s.Config.EdgeGroupID)
	runtimeEdgeGroupID := firstNonEmpty(observed.Route.RuntimeEdgeGroupID, observed.Route.RuntimeEdgeGroup)
	upstreamError := strings.TrimSpace(observed.UpstreamError) != "" && !observed.ClientCanceled
	originWait := originResponseHeaderWait(observed)
	requestBodyComplete := true
	requestBodyMissing := int64(0)
	if observed.RequestBytes > 0 && observed.RequestBodyReadBytes < observed.RequestBytes {
		requestBodyComplete = false
		requestBodyMissing = observed.RequestBytes - observed.RequestBodyReadBytes
	}
	s.Logger.Printf(
		"edge_proxy_request received_at=%s edge_request_id=%s trace_id=%s request_id=%s host=%s app=%s tenant=%s runtime=%s route_kind=%s edge_group_id=%s runtime_region=%s runtime_edge_group_id=%s route_generation=%s fallback_reason=%s client_ip=%s client_remote_addr=%s protocol=%s method=%s path=%s status=%d duration_ms=%d request_bytes=%d request_body_read_bytes=%d request_body_missing_bytes=%d request_body_complete=%t request_body_eof=%t request_body_read_error=%s request_body_buffered=%t request_body_buffer_bytes=%d request_body_buffer_ms=%d request_body_buffer_error=%s request_body_buffer_budget_bytes=%d request_body_buffer_used_bytes=%d request_body_buffer_active_requests=%d body_read_block_ms=%d file_write_ms=%d first_body_byte_ms=%d last_body_byte_ms=%d max_read_gap_ms=%d read_calls=%d avg_bps=%d min_window_bps=%d response_bytes=%d cache_status=%s cache_policy_id=%s cache_key_hash=%s asset_class=%s cache_lookup_ms=%d origin_dns_ms=%d origin_dns_error=%s origin_connect_ms=%d origin_connect_error=%s origin_got_conn=%t origin_conn_reused=%t origin_remote_addr=%s origin_local_addr=%s origin_wrote_headers=%t origin_wrote_request=%t origin_request_write_ms=%d origin_request_write_error=%s origin_response_wait_ms=%d origin_ttfb_ms=%d origin_total_ms=%d response_write_ms=%d upstream=%s upstream_error=%t fallback_hit=%t websocket=%t sse=%t streaming=%t upload=%t client_canceled=%t",
		observed.ReceivedAt.Format(time.RFC3339Nano),
		logSafeValue(observed.EdgeRequestID),
		logSafeValue(observed.TraceID),
		logSafeValue(observed.RequestID),
		observed.Host,
		strings.TrimSpace(observed.Route.AppID),
		strings.TrimSpace(observed.Route.TenantID),
		strings.TrimSpace(observed.Route.RuntimeID),
		strings.TrimSpace(observed.Route.RouteKind),
		edgeGroupID,
		edgeGroupRegion(runtimeEdgeGroupID),
		runtimeEdgeGroupID,
		strings.TrimSpace(observed.Route.RouteGeneration),
		strings.TrimSpace(observed.Route.FallbackReason),
		logSafeValue(observed.ClientIP),
		logSafeValue(observed.ClientRemoteAddr),
		logSafeValue(observed.Protocol),
		observed.Method,
		observed.Path,
		observed.StatusCode,
		observed.Duration.Milliseconds(),
		observed.RequestBytes,
		observed.RequestBodyReadBytes,
		requestBodyMissing,
		requestBodyComplete,
		observed.RequestBodyEOF,
		logSafeValue(observed.RequestBodyReadError),
		observed.RequestBodyBuffered,
		observed.RequestBodyBufferBytes,
		observed.RequestBodyBuffer.Milliseconds(),
		logSafeValue(observed.RequestBodyBufferError),
		observed.RequestBodyBufferBudget,
		observed.RequestBodyBufferUsed,
		observed.RequestBodyBufferActive,
		observed.BodyReadBlock.Milliseconds(),
		observed.FileWrite.Milliseconds(),
		observed.FirstBodyByte.Milliseconds(),
		observed.LastBodyByte.Milliseconds(),
		observed.MaxReadGap.Milliseconds(),
		observed.ReadCalls,
		observed.AvgBPS,
		observed.MinWindowBPS,
		observed.ResponseBytes,
		strings.TrimSpace(observed.CacheStatus),
		strings.TrimSpace(observed.CachePolicyID),
		strings.TrimSpace(observed.CacheKeyHash),
		strings.TrimSpace(observed.AssetClass),
		observed.CacheLookup.Milliseconds(),
		observed.OriginDNS.Milliseconds(),
		logSafeValue(observed.OriginDNSError),
		observed.OriginConnect.Milliseconds(),
		logSafeValue(observed.OriginConnectError),
		observed.OriginGotConn,
		observed.OriginConnectionReused,
		logSafeValue(observed.OriginRemoteAddr),
		logSafeValue(observed.OriginLocalAddr),
		observed.OriginWroteHeaders,
		observed.OriginWroteRequest,
		observed.OriginRequestWrite.Milliseconds(),
		logSafeValue(observed.OriginRequestWriteErr),
		originWait.Milliseconds(),
		observed.OriginTTFB.Milliseconds(),
		observed.OriginTotal.Milliseconds(),
		observed.ResponseWrite.Milliseconds(),
		strings.TrimSpace(observed.Route.UpstreamURL),
		upstreamError,
		observed.FallbackHit,
		observed.WebSocket,
		observed.SSE,
		observed.Streaming,
		observed.Upload,
		observed.ClientCanceled,
	)
	s.logProxyObservationFact(observed)
}

func (s *Service) syncInterval() time.Duration {
	if s.Config.SyncInterval > 0 {
		return s.Config.SyncInterval
	}
	return 15 * time.Second
}

func (s *Service) redact(value string) string {
	token := strings.TrimSpace(s.Config.EdgeToken)
	if token == "" {
		return value
	}
	return strings.ReplaceAll(value, token, "[redacted]")
}

func safeBaseURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return strings.TrimSpace(raw)
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	parsed.User = nil
	return parsed.String()
}

func quoteETag(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return ""
	}
	if strings.HasPrefix(version, `"`) && strings.HasSuffix(version, `"`) {
		return version
	}
	return fmt.Sprintf("%q", version)
}

func boolGauge(value bool) int {
	if value {
		return 1
	}
	return 0
}

func unixSeconds(value *time.Time) float64 {
	if value == nil || value.IsZero() {
		return 0
	}
	return float64(value.Unix())
}

func durationSeconds(value time.Duration) float64 {
	if value <= 0 {
		return 0
	}
	return value.Seconds()
}

func originResponseHeaderWait(observed edgeProxyObservation) time.Duration {
	if !observed.OriginWroteRequest || observed.OriginRequestWrite <= 0 {
		return 0
	}
	end := observed.OriginTTFB
	if end <= 0 {
		end = observed.Upstream
	}
	if end <= observed.OriginRequestWrite {
		return 0
	}
	return end - observed.OriginRequestWrite
}

func logSafeValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	value = strings.Map(func(r rune) rune {
		switch r {
		case '\n', '\r', '\t':
			return ' '
		default:
			return r
		}
	}, value)
	const maxLogValueLength = 256
	if len(value) > maxLogValueLength {
		return value[:maxLogValueLength] + "..."
	}
	return value
}

func edgeProxyServerTiming(observed edgeProxyObservation, proxied bool) string {
	parts := make([]string, 0, 4)
	if observed.CacheLookup > 0 {
		parts = append(parts, edgeServerTimingMetric("fugue_cache_lookup", observed.CacheLookup))
	}
	if proxied {
		if observed.OriginConnect > 0 {
			parts = append(parts, edgeServerTimingMetric("fugue_origin_connect", observed.OriginConnect))
		}
		if observed.OriginTTFB > 0 {
			parts = append(parts, edgeServerTimingMetric("fugue_origin_ttfb", observed.OriginTTFB))
		}
	}
	return strings.Join(parts, ", ")
}

func edgeServerTimingMetric(name string, duration time.Duration) string {
	if duration < 0 {
		duration = 0
	}
	return fmt.Sprintf("%s;dur=%.3f", name, duration.Seconds()*1000)
}

func addEdgeServerTiming(header http.Header, value string) {
	if header == nil || strings.TrimSpace(value) == "" {
		return
	}
	header.Add("Server-Timing", value)
}

func bundleAgeSeconds(generatedAt *time.Time, now time.Time) float64 {
	if generatedAt == nil || generatedAt.IsZero() || now.IsZero() {
		return 0
	}
	age := now.Sub(*generatedAt)
	if age < 0 {
		return 0
	}
	return age.Seconds()
}

func staleAgeSeconds(stale bool, lastSuccessAt *time.Time, now time.Time) float64 {
	if !stale || lastSuccessAt == nil || lastSuccessAt.IsZero() || now.IsZero() {
		return 0
	}
	age := now.Sub(*lastSuccessAt)
	if age < 0 {
		return 0
	}
	return age.Seconds()
}

type edgeProxyObservationResponseWriter struct {
	http.ResponseWriter
	startedAt      time.Time
	observation    *edgeProxyObservation
	wroteHeader    bool
	status         int
	headerSnapshot http.Header
	body           []byte
	bytesWritten   int64
	maxBytes       int64
	overflow       bool
	cacheDecision  *edgeHTTPCacheDecision
}

type edgeCacheRevalidationResponseWriter struct {
	header http.Header
}

func newEdgeCacheRevalidationResponseWriter() *edgeCacheRevalidationResponseWriter {
	return &edgeCacheRevalidationResponseWriter{header: make(http.Header)}
}

func (w *edgeCacheRevalidationResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *edgeCacheRevalidationResponseWriter) WriteHeader(int) {}

func (w *edgeCacheRevalidationResponseWriter) Write(data []byte) (int, error) {
	return len(data), nil
}

func newEdgeProxyObservationResponseWriter(w http.ResponseWriter, startedAt time.Time, observation *edgeProxyObservation) *edgeProxyObservationResponseWriter {
	return &edgeProxyObservationResponseWriter{
		ResponseWriter: w,
		startedAt:      startedAt,
		observation:    observation,
		maxBytes:       int64(32 * 1024 * 1024),
	}
}

func (w *edgeProxyObservationResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *edgeProxyObservationResponseWriter) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}
	writeStarted := time.Now()
	w.wroteHeader = true
	w.status = statusCode
	w.headerSnapshot = cloneHTTPHeader(w.ResponseWriter.Header())
	if w.observation != nil && w.observation.TTFB == 0 && !w.startedAt.IsZero() {
		w.observation.TTFB = time.Since(w.startedAt)
	}
	w.ResponseWriter.WriteHeader(statusCode)
	w.addResponseWriteDuration(time.Since(writeStarted))
}

func (w *edgeProxyObservationResponseWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.maxBytes > 0 && !w.overflow {
		remaining := w.maxBytes - int64(len(w.body))
		if remaining > 0 {
			chunk := data
			if int64(len(chunk)) > remaining {
				chunk = chunk[:remaining]
				w.overflow = true
			}
			w.body = append(w.body, chunk...)
		} else {
			w.overflow = true
		}
	}
	w.bytesWritten += int64(len(data))
	writeStarted := time.Now()
	n, err := w.ResponseWriter.Write(data)
	w.addResponseWriteDuration(time.Since(writeStarted))
	return n, err
}

func (w *edgeProxyObservationResponseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *edgeProxyObservationResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (w *edgeProxyObservationResponseWriter) statusCode() int {
	if w.status != 0 {
		return w.status
	}
	return http.StatusOK
}

func (w *edgeProxyObservationResponseWriter) addResponseWriteDuration(duration time.Duration) {
	if w == nil || w.observation == nil || duration <= 0 {
		return
	}
	w.observation.ResponseWrite += duration
}

func normalizeRouteHost(host string) string {
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "" {
		return ""
	}
	if splitHost, _, err := net.SplitHostPort(host); err == nil {
		host = splitHost
	} else if idx := strings.Index(host, ":"); idx >= 0 {
		host = host[:idx]
	}
	return strings.Trim(host, "[]")
}

func routePathPrefixMatches(prefix, requestPath string) bool {
	prefix = model.NormalizeAppRoutePathPrefix(prefix)
	requestPath = model.NormalizeAppRoutePathPrefix(requestPath)
	if prefix == "/" {
		return true
	}
	return requestPath == prefix || strings.HasPrefix(requestPath, prefix+"/")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstNonEmptyInt(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func edgeGroupRegion(edgeGroupID string) string {
	edgeGroupID = strings.TrimSpace(edgeGroupID)
	if edgeGroupID == "" {
		return ""
	}
	if strings.HasPrefix(edgeGroupID, "edge-group-country-") {
		return strings.TrimPrefix(edgeGroupID, "edge-group-country-")
	}
	if strings.HasPrefix(edgeGroupID, "edge-group-") {
		return strings.TrimPrefix(edgeGroupID, "edge-group-")
	}
	return edgeGroupID
}

func firstNonEmptyHeader(r *http.Request, header string, fallback string) string {
	if r == nil {
		return fallback
	}
	if value := strings.TrimSpace(r.Header.Get(header)); value != "" {
		return value
	}
	return fallback
}

func firstNonEmptyHeaders(r *http.Request, headers ...string) string {
	if r == nil {
		return ""
	}
	for _, header := range headers {
		if value := strings.TrimSpace(r.Header.Get(header)); value != "" {
			return value
		}
	}
	return ""
}

func edgeRequestIsWebSocket(r *http.Request) bool {
	if r == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket")
}

func edgeRequestWantsSSE(r *http.Request) bool {
	if r == nil {
		return false
	}
	for _, part := range strings.Split(r.Header.Get("Accept"), ",") {
		if strings.EqualFold(strings.TrimSpace(strings.Split(part, ";")[0]), "text/event-stream") {
			return true
		}
	}
	return false
}

func edgeRequestHasUpload(r *http.Request) bool {
	if r == nil {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(r.Method)) {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
	default:
		return false
	}
	return r.ContentLength != 0 || len(r.TransferEncoding) > 0
}

func safeProxyLogPath(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}
	path := r.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	if r.URL.RawQuery != "" {
		return path + "?..."
	}
	return path
}

func cloneRouteCounterMap(in map[routeMetricKey]uint64) map[routeMetricKey]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[routeMetricKey]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneRouteStatusCounterMap(in map[routeStatusMetricKey]uint64) map[routeStatusMetricKey]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[routeStatusMetricKey]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneRouteResultCounterMap(in map[routeResultMetricKey]uint64) map[routeResultMetricKey]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[routeResultMetricKey]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneRouteFloatMap(in map[routeMetricKey]float64) map[routeMetricKey]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[routeMetricKey]float64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneRouteCacheCounterMap(in map[routeCacheMetricKey]uint64) map[routeCacheMetricKey]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[routeCacheMetricKey]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func sortedRouteResultMetricKeys(values map[routeResultMetricKey]uint64) []routeResultMetricKey {
	keys := make([]routeResultMetricKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left := keys[i].RouteMetricKey
		right := keys[j].RouteMetricKey
		if left.Hostname != right.Hostname {
			return left.Hostname < right.Hostname
		}
		if left.PathPrefix != right.PathPrefix {
			return left.PathPrefix < right.PathPrefix
		}
		if left.AppID != right.AppID {
			return left.AppID < right.AppID
		}
		if left.RouteKind != right.RouteKind {
			return left.RouteKind < right.RouteKind
		}
		if left.ClientCountry != right.ClientCountry {
			return left.ClientCountry < right.ClientCountry
		}
		if left.ClientRegion != right.ClientRegion {
			return left.ClientRegion < right.ClientRegion
		}
		if left.ClientASN != right.ClientASN {
			return left.ClientASN < right.ClientASN
		}
		return keys[i].Result < keys[j].Result
	})
	return keys
}

func sortedRouteMetricKeys[V any](values map[routeMetricKey]V) []routeMetricKey {
	keys := make([]routeMetricKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Hostname != keys[j].Hostname {
			return keys[i].Hostname < keys[j].Hostname
		}
		if keys[i].PathPrefix != keys[j].PathPrefix {
			return keys[i].PathPrefix < keys[j].PathPrefix
		}
		if keys[i].Method != keys[j].Method {
			return keys[i].Method < keys[j].Method
		}
		if keys[i].AppID != keys[j].AppID {
			return keys[i].AppID < keys[j].AppID
		}
		if keys[i].RouteKind != keys[j].RouteKind {
			return keys[i].RouteKind < keys[j].RouteKind
		}
		if keys[i].ClientCountry != keys[j].ClientCountry {
			return keys[i].ClientCountry < keys[j].ClientCountry
		}
		if keys[i].ClientRegion != keys[j].ClientRegion {
			return keys[i].ClientRegion < keys[j].ClientRegion
		}
		return keys[i].ClientASN < keys[j].ClientASN
	})
	return keys
}

func sortedRouteStatusMetricKeys(values map[routeStatusMetricKey]uint64) []routeStatusMetricKey {
	keys := make([]routeStatusMetricKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left := keys[i].RouteMetricKey
		right := keys[j].RouteMetricKey
		if left.Hostname != right.Hostname {
			return left.Hostname < right.Hostname
		}
		if left.PathPrefix != right.PathPrefix {
			return left.PathPrefix < right.PathPrefix
		}
		if left.AppID != right.AppID {
			return left.AppID < right.AppID
		}
		if left.RouteKind != right.RouteKind {
			return left.RouteKind < right.RouteKind
		}
		if left.ClientCountry != right.ClientCountry {
			return left.ClientCountry < right.ClientCountry
		}
		if left.ClientRegion != right.ClientRegion {
			return left.ClientRegion < right.ClientRegion
		}
		if left.ClientASN != right.ClientASN {
			return left.ClientASN < right.ClientASN
		}
		return keys[i].StatusCode < keys[j].StatusCode
	})
	return keys
}

func sortedRouteCacheMetricKeys(values map[routeCacheMetricKey]uint64) []routeCacheMetricKey {
	keys := make([]routeCacheMetricKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left := keys[i].RouteMetricKey
		right := keys[j].RouteMetricKey
		if left.Hostname != right.Hostname {
			return left.Hostname < right.Hostname
		}
		if left.PathPrefix != right.PathPrefix {
			return left.PathPrefix < right.PathPrefix
		}
		if left.AppID != right.AppID {
			return left.AppID < right.AppID
		}
		if left.RouteKind != right.RouteKind {
			return left.RouteKind < right.RouteKind
		}
		if left.ClientCountry != right.ClientCountry {
			return left.ClientCountry < right.ClientCountry
		}
		if left.ClientRegion != right.ClientRegion {
			return left.ClientRegion < right.ClientRegion
		}
		if left.ClientASN != right.ClientASN {
			return left.ClientASN < right.ClientASN
		}
		if keys[i].CacheStatus != keys[j].CacheStatus {
			return keys[i].CacheStatus < keys[j].CacheStatus
		}
		if keys[i].CachePolicyID != keys[j].CachePolicyID {
			return keys[i].CachePolicyID < keys[j].CachePolicyID
		}
		return keys[i].AssetClass < keys[j].AssetClass
	})
	return keys
}

func routeMetricLabels(key routeMetricKey) string {
	return fmt.Sprintf(
		`hostname="%s",path_prefix="%s",method="%s",app="%s",route_kind="%s",client_country="%s",client_region="%s",client_asn="%s"`,
		prometheusLabelValue(key.Hostname),
		prometheusLabelValue(model.NormalizeAppRoutePathPrefix(key.PathPrefix)),
		prometheusLabelValue(key.Method),
		prometheusLabelValue(key.AppID),
		prometheusLabelValue(key.RouteKind),
		prometheusLabelValue(key.ClientCountry),
		prometheusLabelValue(key.ClientRegion),
		prometheusLabelValue(key.ClientASN),
	)
}

func edgeRouteMetricLabels(status Status, key routeMetricKey) string {
	base := routeMetricLabels(key)
	return fmt.Sprintf(
		`edge_id="%s",edge_group_id="%s",%s`,
		prometheusLabelValue(status.EdgeID),
		prometheusLabelValue(status.EdgeGroupID),
		base,
	)
}

func prometheusLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
}
