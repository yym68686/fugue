package edge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/httpx"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
)

const cacheFileVersion = 1

const edgePeerFallbackHeader = "X-Fugue-Edge-Peer-Fallback"

type Service struct {
	Config     config.EdgeConfig
	HTTPClient *http.Client
	Logger     *log.Logger

	mu       sync.Mutex
	snapshot Status
	bundle   *model.EdgeRouteBundle
	etag     string
	metrics  telemetry
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

type cacheFile struct {
	Version  int                   `json:"version"`
	ETag     string                `json:"etag,omitempty"`
	CachedAt time.Time             `json:"cached_at"`
	Bundle   model.EdgeRouteBundle `json:"bundle"`
}

type telemetry struct {
	BundleSyncSuccess     uint64
	BundleSyncNotModified uint64
	BundleSyncError       uint64
	LastSyncDuration      time.Duration
	CacheWriteSuccess     uint64
	CacheWriteError       uint64
	CacheLoadSuccess      uint64
	CacheLoadMiss         uint64
	CacheLoadError        uint64
	CaddyConfigSuccess    uint64
	CaddyConfigError      uint64
	CaddyAppliedVersion   string
	CaddyRouteCount       int
	CaddyLastApplyAt      *time.Time
	CaddyLastError        string
	RouteRequests         map[routeMetricKey]uint64
	RouteStatuses         map[routeStatusMetricKey]uint64
	RouteUpstreamErrors   map[routeMetricKey]uint64
	RouteFallbackHits     map[routeMetricKey]uint64
	RouteWebSocketResults map[routeResultMetricKey]uint64
	RouteSSEResults       map[routeResultMetricKey]uint64
	RouteStreamingResults map[routeResultMetricKey]uint64
	RouteUploadRequests   map[routeMetricKey]uint64
	RouteLatencyCount     map[routeMetricKey]uint64
	RouteLatencySum       map[routeMetricKey]float64
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
	Hostname  string
	AppID     string
	RouteKind string
}

type routeStatusMetricKey struct {
	RouteMetricKey routeMetricKey
	StatusCode     int
}

type routeResultMetricKey struct {
	RouteMetricKey routeMetricKey
	Result         string
}

type edgeProxyObservation struct {
	Host          string
	Route         model.EdgeRouteBinding
	Method        string
	Path          string
	StatusCode    int
	Duration      time.Duration
	UpstreamError string
	Proxied       bool
	FallbackHit   bool
	WebSocket     bool
	SSE           bool
	Streaming     bool
	Upload        bool
	PeerFallback  bool
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
		Logger: logger,
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
	if err := s.validateConfig(); err != nil {
		return err
	}
	if s.Logger == nil {
		s.Logger = log.Default()
	}
	if s.HTTPClient == nil {
		s.HTTPClient = &http.Client{Timeout: s.Config.HTTPTimeout}
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
				s.retryCurrentCaddyConfig(ctx, "route sync failed")
			}
		}
	}()

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
		if err := s.applyCaddyConfig(ctx, bundle); err != nil && s.Logger != nil {
			s.Logger.Printf("edge caddy config apply failed; version=%s error=%s", bundle.Version, s.redact(err.Error()))
		}
		result = "success"
		return nil
	case http.StatusNotModified:
		if !s.hasBundle() {
			err := fmt.Errorf("edge routes returned 304 without a cached bundle")
			s.recordSyncError(err)
			return err
		}
		s.recordNotModified(now)
		if err := s.applyCurrentCaddyConfig(ctx); err != nil && s.Logger != nil {
			status := s.Status()
			s.Logger.Printf("edge caddy config apply failed; version=%s error=%s", status.BundleVersion, s.redact(err.Error()))
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
		if s.Logger != nil {
			status := s.Status()
			s.Logger.Printf("edge caddy cached config reapply failed; reason=%s version=%s error=%s", strings.TrimSpace(reason), status.BundleVersion, s.redact(err.Error()))
		}
	}
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
	return nil
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /edge/bundle", s.handleBundle)
	mux.HandleFunc("GET /edge/tls/ask", s.handleTLSAsk)
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
	var fallback model.EdgeRouteBinding
	for _, route := range s.bundle.Routes {
		if normalizeRouteHost(route.Hostname) != host {
			continue
		}
		if !s.routeAllowedForThisEdge(route) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive) {
			return route, true, false
		}
		if fallback.Hostname == "" {
			fallback = route
		}
	}
	if fallback.Hostname != "" {
		return fallback, true, true
	}
	return model.EdgeRouteBinding{}, false, false
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
	if route.Status != model.EdgeRouteStatusActive {
		http.Error(w, "route is not active", http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) handleProxy(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()
	host := normalizeRouteHost(firstNonEmptyHeader(r, "X-Fugue-Edge-Route-Host", r.Host))
	route, ok, fallbackHit := s.routeForHost(host)
	observed := edgeProxyObservation{
		Host:        host,
		Route:       route,
		Method:      r.Method,
		Path:        safeProxyLogPath(r),
		FallbackHit: fallbackHit,
		WebSocket:   edgeRequestIsWebSocket(r),
		SSE:         edgeRequestWantsSSE(r),
		Upload:      edgeRequestHasUpload(r),
	}
	observed.Streaming = observed.WebSocket || observed.SSE
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
	if !strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive) {
		observed.StatusCode = http.StatusServiceUnavailable
		message := strings.TrimSpace(route.StatusReason)
		if message == "" {
			message = "edge route is not active"
		}
		http.Error(w, message, http.StatusServiceUnavailable)
		return
	}
	target, err := url.Parse(strings.TrimSpace(route.UpstreamURL))
	if err != nil || target.Scheme == "" || target.Host == "" {
		observed.StatusCode = http.StatusServiceUnavailable
		http.Error(w, "edge route upstream is unavailable", http.StatusServiceUnavailable)
		return
	}
	observed.Proxied = true
	peerFallbackAllowed := s.peerFallbackAllowed(r, observed)
	proxy := s.newEdgeReverseProxy(host, target, route, &observed, peerFallbackAllowed)
	observedWriter := newEdgeProxyObservationResponseWriter(w)
	proxy.ServeHTTP(observedWriter, r)
	if observed.UpstreamError != "" && !observedWriter.wroteHeader && peerFallbackAllowed {
		if s.proxyPeerFallback(w, r, host, route, &observed) {
			return
		}
	}
	if observed.UpstreamError != "" && !observedWriter.wroteHeader {
		http.Error(w, "upstream app is unavailable", http.StatusBadGateway)
		observed.StatusCode = http.StatusBadGateway
		return
	}
	observed.StatusCode = observedWriter.statusCode()
	if !observedWriter.wroteHeader && observed.WebSocket && observed.UpstreamError == "" {
		observed.StatusCode = http.StatusSwitchingProtocols
	}
}

func (s *Service) newEdgeReverseProxy(host string, target *url.URL, route model.EdgeRouteBinding, observed *edgeProxyObservation, suppressErrorResponse bool) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(req *httputil.ProxyRequest) {
			req.SetURL(target)
			req.SetXForwarded()
			req.Out.Host = target.Host
			req.Out.Header.Set("X-Forwarded-Host", host)
			req.Out.Header.Set("X-Fugue-Edge-Route", strings.TrimSpace(route.Hostname))
			req.Out.Header.Set("X-Fugue-Edge-App-ID", strings.TrimSpace(route.AppID))
		},
		Transport: newDefaultEdgeProxyTransport(),
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
			if observed != nil {
				observed.UpstreamError = strings.TrimSpace(proxyErr.Error())
			}
			if suppressErrorResponse {
				return
			}
			http.Error(rw, "upstream app is unavailable", http.StatusBadGateway)
		},
	}
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
	proxy := s.newEdgeReverseProxy(host, target, peerRoute, observed, false)
	writer := newEdgeProxyObservationResponseWriter(w)
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
	fmt.Fprintln(w, "# HELP fugue_edge_route_upstream_latency_seconds Edge data-plane upstream proxy latency by route.")
	fmt.Fprintln(w, "# TYPE fugue_edge_route_upstream_latency_seconds summary")
	for _, key := range sortedRouteMetricKeys(snapshot.Metrics.RouteLatencyCount) {
		fmt.Fprintf(w, "fugue_edge_route_upstream_latency_seconds_sum{%s} %.6f\n", routeMetricLabels(key), snapshot.Metrics.RouteLatencySum[key])
		fmt.Fprintf(w, "fugue_edge_route_upstream_latency_seconds_count{%s} %d\n", routeMetricLabels(key), snapshot.Metrics.RouteLatencyCount[key])
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
		return nil
	}
	req, err := s.newHeartbeatRequest(ctx)
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
	if s.Logger != nil {
		status := s.Status()
		s.Logger.Printf("edge heartbeat success; edge_id=%s edge_group_id=%s status=%s route_bundle=%s caddy_routes=%d", strings.TrimSpace(s.Config.EdgeID), strings.TrimSpace(s.Config.EdgeGroupID), edgeHealthStatus(status), status.BundleVersion, s.metricSnapshot().Metrics.CaddyRouteCount)
	}
	return nil
}

func (s *Service) newHeartbeatRequest(ctx context.Context) (*http.Request, error) {
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(s.Config.APIURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("invalid FUGUE_API_URL")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/v1/edge/heartbeat"
	query := base.Query()
	query.Set("token", strings.TrimSpace(s.Config.EdgeToken))
	base.RawQuery = query.Encode()

	snapshot := s.metricSnapshot()
	status := snapshot.Status
	cacheStatus := "missing"
	if status.BundleVersion != "" {
		cacheStatus = "ready"
	}
	if status.StaleCache {
		cacheStatus = "stale"
	}
	body := map[string]any{
		"edge_id":                  strings.TrimSpace(s.Config.EdgeID),
		"edge_group_id":            strings.TrimSpace(s.Config.EdgeGroupID),
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
		"max_stale_exceeded":       status.MaxStaleExceeded,
		"status":                   edgeHealthStatus(status),
		"healthy":                  status.Healthy,
		"draining":                 s.Config.Draining,
		"last_error":               firstNonEmpty(strings.TrimSpace(status.LastError), strings.TrimSpace(status.CaddyLastError)),
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal edge heartbeat: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build edge heartbeat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
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

func (s *Service) routeAllowedForThisEdge(route model.EdgeRouteBinding) bool {
	if !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
		return false
	}
	edgeGroupID := strings.TrimSpace(s.Config.EdgeGroupID)
	if edgeGroupID == "" {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(route.EdgeGroupID), edgeGroupID) {
		return false
	}
	return true
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
	defer s.mu.Unlock()
	s.bundle = &bundle
	s.etag = strings.TrimSpace(cached.ETag)
	if s.etag == "" {
		s.etag = quoteETag(bundle.Version)
	}
	s.snapshot = s.statusForBundleLocked(bundle, cached.CachedAt, nil, true)
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
	defer s.mu.Unlock()
	s.bundle = &bundle
	s.etag = strings.TrimSpace(etag)
	s.snapshot = s.statusForBundleLocked(bundle, now, &now, stale)
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

func (s *Service) recordCaddyApply(bundleVersion string, routeCount int, err error) {
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

func (s *Service) recordProxyObservation(observed edgeProxyObservation) {
	if observed.StatusCode == 0 {
		observed.StatusCode = http.StatusOK
	}
	key := routeMetricKey{
		Hostname:  firstNonEmpty(strings.TrimSpace(observed.Route.Hostname), observed.Host),
		AppID:     strings.TrimSpace(observed.Route.AppID),
		RouteKind: strings.TrimSpace(observed.Route.RouteKind),
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
	if strings.TrimSpace(observed.UpstreamError) != "" {
		if s.metrics.RouteUpstreamErrors == nil {
			s.metrics.RouteUpstreamErrors = make(map[routeMetricKey]uint64)
		}
		s.metrics.RouteUpstreamErrors[key]++
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
	if observed.Proxied {
		if s.metrics.RouteLatencyCount == nil {
			s.metrics.RouteLatencyCount = make(map[routeMetricKey]uint64)
		}
		if s.metrics.RouteLatencySum == nil {
			s.metrics.RouteLatencySum = make(map[routeMetricKey]float64)
		}
		s.metrics.RouteLatencyCount[key]++
		s.metrics.RouteLatencySum[key] += durationSeconds(observed.Duration)
	}
}

func edgeProxyObservationResult(observed edgeProxyObservation) string {
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
	out.Metrics.RouteLatencyCount = cloneRouteCounterMap(s.metrics.RouteLatencyCount)
	out.Metrics.RouteLatencySum = cloneRouteFloatMap(s.metrics.RouteLatencySum)
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
	s.Logger.Printf(
		"edge_proxy_request host=%s app=%s tenant=%s runtime=%s route_kind=%s method=%s path=%s status=%d duration_ms=%d upstream=%s upstream_error=%t fallback_hit=%t websocket=%t sse=%t streaming=%t upload=%t",
		observed.Host,
		strings.TrimSpace(observed.Route.AppID),
		strings.TrimSpace(observed.Route.TenantID),
		strings.TrimSpace(observed.Route.RuntimeID),
		strings.TrimSpace(observed.Route.RouteKind),
		observed.Method,
		observed.Path,
		observed.StatusCode,
		observed.Duration.Milliseconds(),
		strings.TrimSpace(observed.Route.UpstreamURL),
		strings.TrimSpace(observed.UpstreamError) != "",
		observed.FallbackHit,
		observed.WebSocket,
		observed.SSE,
		observed.Streaming,
		observed.Upload,
	)
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
	wroteHeader bool
	status      int
}

func newEdgeProxyObservationResponseWriter(w http.ResponseWriter) *edgeProxyObservationResponseWriter {
	return &edgeProxyObservationResponseWriter{ResponseWriter: w}
}

func (w *edgeProxyObservationResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *edgeProxyObservationResponseWriter) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *edgeProxyObservationResponseWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
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
		if left.AppID != right.AppID {
			return left.AppID < right.AppID
		}
		if left.RouteKind != right.RouteKind {
			return left.RouteKind < right.RouteKind
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
		if keys[i].AppID != keys[j].AppID {
			return keys[i].AppID < keys[j].AppID
		}
		return keys[i].RouteKind < keys[j].RouteKind
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
		if left.AppID != right.AppID {
			return left.AppID < right.AppID
		}
		if left.RouteKind != right.RouteKind {
			return left.RouteKind < right.RouteKind
		}
		return keys[i].StatusCode < keys[j].StatusCode
	})
	return keys
}

func routeMetricLabels(key routeMetricKey) string {
	return fmt.Sprintf(
		`hostname="%s",app="%s",route_kind="%s"`,
		prometheusLabelValue(key.Hostname),
		prometheusLabelValue(key.AppID),
		prometheusLabelValue(key.RouteKind),
	)
}

func prometheusLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
}
