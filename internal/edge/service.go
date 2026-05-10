package edge

import (
	"context"
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
	"strings"
	"sync"
	"time"

	"fugue/internal/config"
	"fugue/internal/httpx"
	"fugue/internal/model"
)

const cacheFileVersion = 1

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
	Status            string     `json:"status"`
	Healthy           bool       `json:"healthy"`
	EdgeID            string     `json:"edge_id,omitempty"`
	EdgeGroupID       string     `json:"edge_group_id,omitempty"`
	BundleVersion     string     `json:"bundle_version,omitempty"`
	RouteCount        int        `json:"route_count"`
	TLSAllowlistCount int        `json:"tls_allowlist_count"`
	LastSyncAt        *time.Time `json:"last_sync_at,omitempty"`
	LastSuccessAt     *time.Time `json:"last_success_at,omitempty"`
	LastError         string     `json:"last_error,omitempty"`
	StaleCache        bool       `json:"stale_cache"`
	CachePath         string     `json:"cache_path,omitempty"`
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
	return &Service{
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

	s.Logger.Printf("fugue-edge shadow started; api=%s edge_id=%s edge_group_id=%s cache=%s listen=%s interval=%s", safeBaseURL(s.Config.APIURL), s.Config.EdgeID, s.Config.EdgeGroupID, s.Config.CachePath, s.Config.ListenAddr, s.syncInterval())
	_ = s.SyncOnce(ctx)

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
		result = "success"
		return nil
	case http.StatusNotModified:
		if !s.hasBundle() {
			err := fmt.Errorf("edge routes returned 304 without a cached bundle")
			s.recordSyncError(err)
			return err
		}
		s.recordNotModified(now)
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
		return err
	}
	if cached.Bundle.Version == "" {
		err := fmt.Errorf("edge route cache missing bundle version")
		s.recordCacheLoad("error")
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
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return mux
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

func (s *Service) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot := s.metricSnapshot()
	status := snapshot.Status
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintln(w, "# HELP fugue_edge_health Whether fugue-edge has a usable route bundle.")
	fmt.Fprintln(w, "# TYPE fugue_edge_health gauge")
	fmt.Fprintf(w, "fugue_edge_health %d\n", boolGauge(status.Healthy))
	fmt.Fprintln(w, "# HELP fugue_edge_stale_cache Whether fugue-edge is serving a last-known-good cached bundle after sync failure.")
	fmt.Fprintln(w, "# TYPE fugue_edge_stale_cache gauge")
	fmt.Fprintf(w, "fugue_edge_stale_cache %d\n", boolGauge(status.StaleCache))
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
	return nil
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
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		s.recordCacheWrite("error")
		return fmt.Errorf("create edge route cache directory: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		s.recordCacheWrite("error")
		return fmt.Errorf("write edge route cache temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		s.recordCacheWrite("error")
		return fmt.Errorf("replace edge route cache: %w", err)
	}
	s.recordCacheWrite("success")
	return nil
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
		return
	}
	s.snapshot.Status = "unhealthy"
	s.snapshot.Healthy = false
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

func (s *Service) statusForBundleLocked(bundle model.EdgeRouteBundle, syncAt time.Time, successAt *time.Time, stale bool) Status {
	status := "ok"
	if stale {
		status = "stale"
	}
	out := Status{
		Status:            status,
		Healthy:           true,
		EdgeID:            strings.TrimSpace(s.Config.EdgeID),
		EdgeGroupID:       strings.TrimSpace(s.Config.EdgeGroupID),
		BundleVersion:     bundle.Version,
		RouteCount:        len(bundle.Routes),
		TLSAllowlistCount: len(bundle.TLSAllowlist),
		LastSyncAt:        &syncAt,
		LastSuccessAt:     successAt,
		StaleCache:        stale,
		CachePath:         strings.TrimSpace(s.Config.CachePath),
	}
	return out
}

func (s *Service) metricSnapshot() metricSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := metricSnapshot{
		Status:  s.snapshot,
		Metrics: s.metrics,
	}
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

func prometheusLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
}
