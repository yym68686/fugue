package dnsserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	miekgdns "github.com/miekg/dns"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
	"fugue/internal/httpx"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
)

const cacheFileVersion = 1

const edgeHealthProbeTTL = 15 * time.Second

type edgeDNSLiveHealthFunc func(string) bool

type Service struct {
	Config     config.DNSConfig
	HTTPClient *http.Client
	Logger     *log.Logger

	mu       sync.Mutex
	snapshot Status
	bundle   *model.EdgeDNSBundle
	etag     string
	metrics  telemetry

	edgeHealthMu sync.Mutex
	edgeHealth   map[string]edgeHealthObservation
	edgeProbe    edgeHealthProbeFunc
}

type Status struct {
	Status                 string     `json:"status"`
	Healthy                bool       `json:"healthy"`
	DNSNodeID              string     `json:"dns_node_id,omitempty"`
	EdgeGroupID            string     `json:"edge_group_id,omitempty"`
	Zone                   string     `json:"zone,omitempty"`
	BundleVersion          string     `json:"bundle_version,omitempty"`
	ServingGeneration      string     `json:"serving_generation,omitempty"`
	LKGGeneration          string     `json:"lkg_generation,omitempty"`
	LastGoodGeneration     string     `json:"last_good_generation,omitempty"`
	CacheCorruptGeneration string     `json:"cache_corrupt_generation,omitempty"`
	BundleValidUntil       *time.Time `json:"bundle_valid_until,omitempty"`
	RecordCount            int        `json:"record_count"`
	LastSyncAt             *time.Time `json:"last_sync_at,omitempty"`
	LastSuccessAt          *time.Time `json:"last_success_at,omitempty"`
	LastError              string     `json:"last_error,omitempty"`
	DegradedReason         string     `json:"degraded_reason,omitempty"`
	StaleCache             bool       `json:"stale_cache"`
	MaxStaleExceeded       bool       `json:"max_stale_exceeded,omitempty"`
	CachePath              string     `json:"cache_path,omitempty"`
	ListenAddr             string     `json:"listen_addr,omitempty"`
	UDPAddr                string     `json:"udp_addr,omitempty"`
	TCPAddr                string     `json:"tcp_addr,omitempty"`
}

type cacheFile struct {
	Version  int                 `json:"version"`
	ETag     string              `json:"etag,omitempty"`
	CachedAt time.Time           `json:"cached_at"`
	Bundle   model.EdgeDNSBundle `json:"bundle"`
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
	QueryTotal            map[dnsQueryMetricKey]uint64
}

type edgeHealthProbeFunc func(context.Context, string) bool

type edgeHealthObservation struct {
	Healthy   bool
	CheckedAt time.Time
}

type dnsQueryMetricKey struct {
	Type  string
	RCode string
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
		return fmt.Sprintf("edge dns bundle returned status %d", e.StatusCode)
	}
	return fmt.Sprintf("edge dns bundle returned status %d: %s", e.StatusCode, e.Body)
}

func NewService(cfg config.DNSConfig, logger *log.Logger) *Service {
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
		Logger:     logger,
		edgeHealth: map[string]edgeHealthObservation{},
		snapshot: Status{
			Status:      "unhealthy",
			DNSNodeID:   strings.TrimSpace(cfg.DNSNodeID),
			EdgeGroupID: strings.TrimSpace(cfg.EdgeGroupID),
			Zone:        normalizeName(cfg.Zone),
			CachePath:   strings.TrimSpace(cfg.CachePath),
			ListenAddr:  strings.TrimSpace(cfg.ListenAddr),
			UDPAddr:     strings.TrimSpace(cfg.UDPAddr),
			TCPAddr:     strings.TrimSpace(cfg.TCPAddr),
		},
	}
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
		s.Logger.Printf("dns bundle cache unavailable: %v", err)
	}

	httpShutdown, err := s.startHTTPServer()
	if err != nil {
		return err
	}
	if httpShutdown != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := httpShutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
				s.Logger.Printf("dns health server shutdown failed: %v", err)
			}
		}()
	}

	dnsShutdown, err := s.startDNSServers()
	if err != nil {
		return err
	}
	if dnsShutdown != nil {
		defer dnsShutdown()
	}

	s.Logger.Printf("fugue-dns shadow started; api=%s dns_node_id=%s edge_group_id=%s zone=%s answer_ips=%s cache=%s listen=%s udp=%s tcp=%s interval=%s", safeBaseURL(s.Config.APIURL), s.Config.DNSNodeID, s.Config.EdgeGroupID, normalizeName(s.Config.Zone), strings.Join(s.Config.AnswerIPs, ","), s.Config.CachePath, s.Config.ListenAddr, s.Config.UDPAddr, s.Config.TCPAddr, s.syncInterval())
	_ = s.SyncOnce(ctx)
	s.startEdgeHealthProbeLoop(ctx)
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
			s.Logger.Printf("dns bundle sync success; version=%s records=%d duration_ms=%d", status.BundleVersion, status.RecordCount, duration.Milliseconds())
		case "not_modified":
			status := s.Status()
			s.Logger.Printf("dns bundle sync not_modified; version=%s duration_ms=%d", status.BundleVersion, duration.Milliseconds())
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
	req, err := s.newBundleRequest(ctx)
	if err != nil {
		s.recordSyncError(err)
		return err
	}
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		err = fmt.Errorf("fetch dns bundle: %s", s.redact(err.Error()))
		s.recordSyncError(err)
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		if s.hasBundle() {
			s.recordSyncSuccess(true)
			result = "not_modified"
			return nil
		}
		err := fmt.Errorf("dns bundle returned 304 without a cached bundle")
		s.recordSyncError(err)
		return err
	case http.StatusOK:
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		err := statusError{StatusCode: resp.StatusCode, Body: s.redact(string(body))}
		s.recordSyncError(err)
		return err
	}

	var bundle model.EdgeDNSBundle
	if err := json.NewDecoder(resp.Body).Decode(&bundle); err != nil {
		err := fmt.Errorf("decode dns bundle: %w", err)
		s.recordSyncError(err)
		return err
	}
	if err := s.verifyBundle(bundle, time.Now().UTC()); err != nil {
		if fallbackErr := s.LoadPreviousCache(); fallbackErr != nil && s.Logger != nil {
			s.Logger.Printf("dns bundle previous cache fallback failed: %v", fallbackErr)
		}
		err := fmt.Errorf("verify dns bundle: %w", err)
		s.recordSyncError(err)
		return err
	}
	etag := strings.TrimSpace(resp.Header.Get("ETag"))
	if etag == "" && strings.TrimSpace(bundle.Version) != "" {
		etag = strconvQuote(strings.TrimSpace(bundle.Version))
	}
	cached := cacheFile{
		Version:  cacheFileVersion,
		ETag:     etag,
		CachedAt: time.Now().UTC(),
		Bundle:   bundle,
	}
	if err := s.writeCache(cached); err != nil {
		s.recordCacheWrite(false)
		err = fmt.Errorf("write dns bundle cache: %w", err)
		s.recordSyncError(err)
		return err
	}
	s.recordCacheWrite(true)
	s.setBundle(bundle, etag, false, "")
	s.recordSyncSuccess(false)
	result = "success"
	return nil
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return mux
}

func (s *Service) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	status := s.Status()
	code := http.StatusOK
	if !status.Healthy {
		code = http.StatusServiceUnavailable
	}
	httpx.WriteJSON(w, code, status)
}

func (s *Service) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	snapshot := s.metricSnapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# HELP fugue_dns_health Whether fugue-dns has a usable DNS bundle.")
	fmt.Fprintln(w, "# TYPE fugue_dns_health gauge")
	if snapshot.Status.Healthy {
		fmt.Fprintln(w, "fugue_dns_health 1")
	} else {
		fmt.Fprintln(w, "fugue_dns_health 0")
	}
	fmt.Fprintln(w, "# HELP fugue_dns_stale_cache Whether fugue-dns is serving a last-known-good cached bundle after sync failure.")
	fmt.Fprintln(w, "# TYPE fugue_dns_stale_cache gauge")
	fmt.Fprintf(w, "fugue_dns_stale_cache %d\n", dnsBoolGauge(snapshot.Status.StaleCache))
	fmt.Fprintln(w, "# HELP fugue_dns_last_sync_timestamp_seconds Last DNS bundle sync attempt time.")
	fmt.Fprintln(w, "# TYPE fugue_dns_last_sync_timestamp_seconds gauge")
	fmt.Fprintf(w, "fugue_dns_last_sync_timestamp_seconds %.0f\n", dnsUnixSeconds(snapshot.Status.LastSyncAt))
	fmt.Fprintln(w, "# HELP fugue_dns_last_success_timestamp_seconds Last successful DNS bundle sync time.")
	fmt.Fprintln(w, "# TYPE fugue_dns_last_success_timestamp_seconds gauge")
	fmt.Fprintf(w, "fugue_dns_last_success_timestamp_seconds %.0f\n", dnsUnixSeconds(snapshot.Status.LastSuccessAt))
	fmt.Fprintln(w, "# HELP fugue_dns_stale_age_seconds Seconds since the last successful bundle sync while serving a stale cache.")
	fmt.Fprintln(w, "# TYPE fugue_dns_stale_age_seconds gauge")
	fmt.Fprintf(w, "fugue_dns_stale_age_seconds %.0f\n", dnsStaleAgeSeconds(snapshot.Status.StaleCache, snapshot.Status.LastSuccessAt, time.Now().UTC()))
	if strings.TrimSpace(snapshot.Status.DegradedReason) != "" {
		fmt.Fprintln(w, "# HELP fugue_dns_degraded_reason Current fugue-dns degraded reason.")
		fmt.Fprintln(w, "# TYPE fugue_dns_degraded_reason gauge")
		fmt.Fprintf(w, "fugue_dns_degraded_reason{reason=\"%s\"} 1\n", dnsPrometheusLabelValue(snapshot.Status.DegradedReason))
	}
	fmt.Fprintln(w, "# HELP fugue_dns_records Number of records in the current DNS bundle.")
	fmt.Fprintln(w, "# TYPE fugue_dns_records gauge")
	fmt.Fprintf(w, "fugue_dns_records %d\n", snapshot.Status.RecordCount)
	fmt.Fprintln(w, "# HELP fugue_dns_bundle_sync_total DNS bundle sync attempts by result.")
	fmt.Fprintln(w, "# TYPE fugue_dns_bundle_sync_total counter")
	fmt.Fprintf(w, "fugue_dns_bundle_sync_total{result=\"success\"} %d\n", snapshot.Metrics.BundleSyncSuccess)
	fmt.Fprintf(w, "fugue_dns_bundle_sync_total{result=\"not_modified\"} %d\n", snapshot.Metrics.BundleSyncNotModified)
	fmt.Fprintf(w, "fugue_dns_bundle_sync_total{result=\"error\"} %d\n", snapshot.Metrics.BundleSyncError)
	fmt.Fprintln(w, "# HELP fugue_dns_cache_write_total DNS bundle cache write attempts by result.")
	fmt.Fprintln(w, "# TYPE fugue_dns_cache_write_total counter")
	fmt.Fprintf(w, "fugue_dns_cache_write_total{result=\"success\"} %d\n", snapshot.Metrics.CacheWriteSuccess)
	fmt.Fprintf(w, "fugue_dns_cache_write_total{result=\"error\"} %d\n", snapshot.Metrics.CacheWriteError)
	fmt.Fprintln(w, "# HELP fugue_dns_cache_load_total DNS bundle cache load attempts by result.")
	fmt.Fprintln(w, "# TYPE fugue_dns_cache_load_total counter")
	fmt.Fprintf(w, "fugue_dns_cache_load_total{result=\"success\"} %d\n", snapshot.Metrics.CacheLoadSuccess)
	fmt.Fprintf(w, "fugue_dns_cache_load_total{result=\"miss\"} %d\n", snapshot.Metrics.CacheLoadMiss)
	fmt.Fprintf(w, "fugue_dns_cache_load_total{result=\"error\"} %d\n", snapshot.Metrics.CacheLoadError)
	fmt.Fprintln(w, "# HELP fugue_dns_bundle_sync_duration_seconds Duration of the last DNS bundle sync attempt.")
	fmt.Fprintln(w, "# TYPE fugue_dns_bundle_sync_duration_seconds gauge")
	fmt.Fprintf(w, "fugue_dns_bundle_sync_duration_seconds %.6f\n", snapshot.Metrics.LastSyncDuration.Seconds())
	fmt.Fprintln(w, "# HELP fugue_dns_bundle_age_seconds Age of the current DNS bundle based on generated_at.")
	fmt.Fprintln(w, "# TYPE fugue_dns_bundle_age_seconds gauge")
	if snapshot.BundleGeneratedAt != nil {
		fmt.Fprintf(w, "fugue_dns_bundle_age_seconds %.0f\n", time.Since(*snapshot.BundleGeneratedAt).Seconds())
	} else {
		fmt.Fprintln(w, "fugue_dns_bundle_age_seconds 0")
	}
	fmt.Fprintln(w, "# HELP fugue_dns_query_total Authoritative DNS queries by qtype and rcode.")
	fmt.Fprintln(w, "# TYPE fugue_dns_query_total counter")
	for _, entry := range sortedQueryMetricEntries(snapshot.Metrics.QueryTotal) {
		fmt.Fprintf(w, "fugue_dns_query_total{qtype=%q,rcode=%q} %d\n", entry.Key.Type, entry.Key.RCode, entry.Value)
	}
}

func (s *Service) ServeDNS(w miekgdns.ResponseWriter, r *miekgdns.Msg) {
	resp := new(miekgdns.Msg)
	resp.SetReply(r)
	resp.Authoritative = true
	resp.RecursionAvailable = false

	qtype := "unknown"
	rcode := "NOERROR"
	defer func() {
		s.recordQuery(qtype, rcode)
	}()

	if len(r.Question) == 0 {
		resp.Rcode = miekgdns.RcodeFormatError
		rcode = miekgdns.RcodeToString[resp.Rcode]
		_ = w.WriteMsg(resp)
		return
	}

	question := r.Question[0]
	qtype = miekgdns.TypeToString[question.Qtype]
	if qtype == "" {
		qtype = fmt.Sprintf("TYPE%d", question.Qtype)
	}
	name := normalizeName(question.Name)
	snapshot := s.currentBundle()
	zone := normalizeName(s.Config.Zone)
	if snapshot != nil && normalizeName(snapshot.Zone) != "" {
		zone = normalizeName(snapshot.Zone)
	}

	if !nameWithinZone(name, zone) {
		resp.Rcode = miekgdns.RcodeRefused
		rcode = miekgdns.RcodeToString[resp.Rcode]
		s.applyECSResponseScope(resp, r, w)
		_ = w.WriteMsg(resp)
		return
	}

	switch question.Qtype {
	case miekgdns.TypeSOA:
		if name == zone {
			resp.Answer = append(resp.Answer, s.soaRecord(zone))
		} else {
			resp.Ns = append(resp.Ns, s.soaRecord(zone))
		}
	case miekgdns.TypeNS:
		records, nameExists := s.edgeDNSRecordsForQuestion(context.Background(), snapshot, name, question.Qtype, r, w)
		if len(records) > 0 {
			resp.Answer = append(resp.Answer, records...)
		} else if name == zone {
			resp.Answer = append(resp.Answer, s.nsRecords(zone)...)
		} else if !nameExists {
			resp.Rcode = miekgdns.RcodeNameError
			resp.Ns = append(resp.Ns, s.soaRecord(zone))
		} else {
			resp.Ns = append(resp.Ns, s.soaRecord(zone))
		}
	case miekgdns.TypeA, miekgdns.TypeAAAA, miekgdns.TypeCAA, miekgdns.TypeCNAME, miekgdns.TypeMX, miekgdns.TypeTXT:
		records, nameExists := s.edgeDNSRecordsForQuestion(context.Background(), snapshot, name, question.Qtype, r, w)
		if len(records) > 0 {
			resp.Answer = append(resp.Answer, records...)
		} else if !nameExists {
			resp.Rcode = miekgdns.RcodeNameError
			resp.Ns = append(resp.Ns, s.soaRecord(zone))
		}
	default:
		if !edgeDNSNameExists(snapshot, name) && name != zone {
			resp.Rcode = miekgdns.RcodeNameError
			resp.Ns = append(resp.Ns, s.soaRecord(zone))
		}
	}
	rcode = miekgdns.RcodeToString[resp.Rcode]
	if rcode == "" {
		rcode = fmt.Sprintf("RCODE%d", resp.Rcode)
	}
	s.applyECSResponseScope(resp, r, w)
	_ = w.WriteMsg(resp)
}

func (s *Service) LoadCache() error {
	path := strings.TrimSpace(s.Config.CachePath)
	if path == "" {
		s.recordCacheLoad("miss")
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.recordCacheLoad("miss")
			return nil
		}
		s.recordCacheLoad("error")
		return err
	}
	var cached cacheFile
	if err := json.Unmarshal(data, &cached); err != nil {
		s.recordCacheLoad("error")
		if fallbackErr := s.LoadPreviousCache(); fallbackErr == nil {
			s.recordCacheCorruptGeneration("unknown")
			return nil
		}
		return err
	}
	if cached.Version != cacheFileVersion {
		s.recordCacheLoad("error")
		if fallbackErr := s.LoadPreviousCache(); fallbackErr == nil {
			s.recordCacheCorruptGeneration(edgeDNSCacheGeneration(cached.Bundle))
			return nil
		}
		return fmt.Errorf("unsupported dns cache file version %d", cached.Version)
	}
	if err := s.verifyCachedBundle(cached.Bundle, time.Now().UTC()); err != nil {
		s.recordCacheLoad("error")
		if fallbackErr := s.LoadPreviousCache(); fallbackErr == nil {
			s.recordCacheCorruptGeneration(edgeDNSCacheGeneration(cached.Bundle))
			return nil
		}
		return fmt.Errorf("verify dns cache: %w", err)
	}
	s.setBundle(cached.Bundle, cached.ETag, true, "")
	s.recordCacheLoad("success")
	s.Logger.Printf("dns bundle cache loaded; version=%s etag=%s cached_at=%s records=%d path=%s", cached.Bundle.Version, cached.ETag, cached.CachedAt.Format(time.RFC3339Nano), len(cached.Bundle.Records), path)
	return nil
}

func (s *Service) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

func (s *Service) metricSnapshot() metricSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := s.snapshot
	metrics := s.metrics
	metrics.QueryTotal = cloneQueryMetrics(s.metrics.QueryTotal)
	var generatedAt *time.Time
	if s.bundle != nil && !s.bundle.GeneratedAt.IsZero() {
		value := s.bundle.GeneratedAt
		generatedAt = &value
	}
	return metricSnapshot{Status: status, Metrics: metrics, BundleGeneratedAt: generatedAt}
}

func (s *Service) currentBundle() *model.EdgeDNSBundle {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bundle == nil {
		return nil
	}
	bundle := *s.bundle
	bundle.Records = append([]model.EdgeDNSRecord(nil), s.bundle.Records...)
	return &bundle
}

func (s *Service) hasBundle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bundle != nil
}

func (s *Service) currentETag() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.etag
}

func (s *Service) setBundle(bundle model.EdgeDNSBundle, etag string, stale bool, lastError string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bundle.Records = append([]model.EdgeDNSRecord(nil), bundle.Records...)
	s.bundle = &bundle
	s.etag = strings.TrimSpace(etag)
	now := time.Now().UTC()
	validUntil := bundle.ValidUntil
	status := "ok"
	degradedReason := ""
	healthy := true
	if !validUntil.IsZero() && now.After(validUntil) {
		status = "degraded"
		degradedReason = "dns bundle valid_until expired"
		stale = true
	}
	maxStaleExceeded := s.maxStaleExceeded(validUntil, now)
	if maxStaleExceeded {
		status = "unhealthy"
		healthy = false
		degradedReason = "dns bundle valid_until exceeded max_stale"
		stale = true
	}
	generation := edgeDNSCacheGeneration(bundle)
	s.snapshot = Status{
		Status:             status,
		Healthy:            healthy,
		DNSNodeID:          strings.TrimSpace(bundle.DNSNodeID),
		EdgeGroupID:        strings.TrimSpace(bundle.EdgeGroupID),
		Zone:               normalizeName(bundle.Zone),
		BundleVersion:      strings.TrimSpace(bundle.Version),
		ServingGeneration:  generation,
		LKGGeneration:      generation,
		LastGoodGeneration: generation,
		BundleValidUntil:   &validUntil,
		RecordCount:        len(bundle.Records),
		LastSyncAt:         &now,
		LastSuccessAt:      &now,
		LastError:          strings.TrimSpace(lastError),
		DegradedReason:     degradedReason,
		StaleCache:         stale,
		MaxStaleExceeded:   maxStaleExceeded,
		CachePath:          strings.TrimSpace(s.Config.CachePath),
		ListenAddr:         strings.TrimSpace(s.Config.ListenAddr),
		UDPAddr:            strings.TrimSpace(s.Config.UDPAddr),
		TCPAddr:            strings.TrimSpace(s.Config.TCPAddr),
	}
	if validUntil.IsZero() {
		s.snapshot.BundleValidUntil = nil
	}
}

func (s *Service) recordSyncAttempt(result string, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metrics.QueryTotal == nil {
		s.metrics.QueryTotal = make(map[dnsQueryMetricKey]uint64)
	}
	s.metrics.LastSyncDuration = duration
	switch result {
	case "success":
		s.metrics.BundleSyncSuccess++
	case "not_modified":
		s.metrics.BundleSyncNotModified++
	default:
		s.metrics.BundleSyncError++
	}
	now := time.Now().UTC()
	s.snapshot.LastSyncAt = &now
}

func (s *Service) recordSyncSuccess(notModified bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	s.snapshot.LastSyncAt = &now
	s.snapshot.LastSuccessAt = &now
	s.snapshot.LastError = ""
	s.snapshot.StaleCache = false
	if notModified && s.bundle != nil {
		s.snapshot.Status = "ok"
		s.snapshot.Healthy = true
		s.snapshot.BundleVersion = s.bundle.Version
		s.snapshot.Zone = normalizeName(s.bundle.Zone)
		s.snapshot.RecordCount = len(s.bundle.Records)
	}
}

func (s *Service) recordSyncError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	s.snapshot.LastSyncAt = &now
	if err != nil {
		s.snapshot.LastError = s.redact(err.Error())
	}
	if s.bundle == nil {
		s.snapshot.Status = "unhealthy"
		s.snapshot.Healthy = false
	} else {
		s.snapshot.Status = "ok"
		s.snapshot.Healthy = true
		s.snapshot.StaleCache = true
		if !s.bundle.ValidUntil.IsZero() && now.After(s.bundle.ValidUntil) {
			s.snapshot.Status = "degraded"
			s.snapshot.DegradedReason = "dns bundle valid_until expired"
			if s.maxStaleExceeded(s.bundle.ValidUntil, now) {
				s.snapshot.Status = "unhealthy"
				s.snapshot.Healthy = false
				s.snapshot.MaxStaleExceeded = true
				s.snapshot.DegradedReason = "dns bundle valid_until exceeded max_stale"
			}
		} else if strings.TrimSpace(s.snapshot.DegradedReason) == "" {
			s.snapshot.DegradedReason = "dns bundle sync failed; serving cache"
		}
	}
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

func (s *Service) recordCacheWrite(success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if success {
		s.metrics.CacheWriteSuccess++
	} else {
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

func (s *Service) recordQuery(qtype, rcode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metrics.QueryTotal == nil {
		s.metrics.QueryTotal = make(map[dnsQueryMetricKey]uint64)
	}
	s.metrics.QueryTotal[dnsQueryMetricKey{Type: qtype, RCode: rcode}]++
}

func (s *Service) logSyncFailure(err error) {
	if err == nil {
		return
	}
	status := s.Status()
	if status.BundleVersion != "" {
		s.Logger.Printf("dns bundle sync failed; using stale cache; version=%s error=%s", status.BundleVersion, s.redact(err.Error()))
		return
	}
	s.Logger.Printf("dns bundle sync failed; error=%s", s.redact(err.Error()))
}

func (s *Service) logHeartbeatFailure(err error) {
	if err == nil || s.Logger == nil {
		return
	}
	s.Logger.Printf("dns heartbeat failed; error=%s", s.redact(err.Error()))
}

func (s *Service) newBundleRequest(ctx context.Context) (*http.Request, error) {
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(s.Config.APIURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("invalid FUGUE_API_URL")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/v1/edge/dns"
	query := base.Query()
	query.Set("token", strings.TrimSpace(s.Config.EdgeToken))
	if dnsNodeID := strings.TrimSpace(s.Config.DNSNodeID); dnsNodeID != "" {
		query.Set("dns_node_id", dnsNodeID)
	}
	if edgeGroupID := strings.TrimSpace(s.Config.EdgeGroupID); edgeGroupID != "" {
		query.Set("edge_group_id", edgeGroupID)
	}
	if zone := normalizeName(s.Config.Zone); zone != "" {
		query.Set("zone", zone)
	}
	for _, ip := range s.Config.AnswerIPs {
		query.Add("answer_ip", strings.TrimSpace(ip))
	}
	for _, ip := range s.Config.RouteAAnswerIPs {
		query.Add("route_a_answer_ip", strings.TrimSpace(ip))
	}
	if s.Config.TTL > 0 {
		query.Set("ttl", fmt.Sprintf("%d", s.Config.TTL))
	}
	base.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build edge dns request: %w", err)
	}
	if etag := s.currentETag(); etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	return req, nil
}

func (s *Service) startHeartbeatLoop(ctx context.Context) {
	if !s.heartbeatEnabled() {
		if s.Logger != nil {
			s.Logger.Printf("dns heartbeat disabled; dns_node_id=%t edge_group_id=%t token=%t", strings.TrimSpace(s.Config.DNSNodeID) != "", strings.TrimSpace(s.Config.EdgeGroupID) != "", strings.TrimSpace(s.Config.EdgeToken) != "")
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

func (s *Service) startEdgeHealthProbeLoop(ctx context.Context) {
	if !s.Config.EdgeHealthProbeEnabled {
		return
	}
	go func() {
		s.refreshEdgeHealth(ctx)
		ticker := time.NewTicker(edgeHealthProbeTTL)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.refreshEdgeHealth(ctx)
			}
		}
	}()
}

func (s *Service) refreshEdgeHealth(ctx context.Context) {
	ips := s.edgeHealthProbeIPs()
	if len(ips) == 0 {
		return
	}
	now := time.Now().UTC()
	timeout := s.Config.EdgeHealthProbeTimeout
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}
	for _, ip := range ips {
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		healthy := false
		if s.edgeProbe != nil {
			healthy = s.edgeProbe(probeCtx, ip)
		} else {
			healthy = s.dialEdgeIP(probeCtx, ip)
		}
		cancel()
		s.edgeHealthMu.Lock()
		if s.edgeHealth == nil {
			s.edgeHealth = map[string]edgeHealthObservation{}
		}
		s.edgeHealth[ip] = edgeHealthObservation{Healthy: healthy, CheckedAt: now}
		s.edgeHealthMu.Unlock()
	}
}

func (s *Service) edgeHealthProbeIPs() []string {
	bundle := s.currentBundle()
	if bundle == nil {
		return nil
	}
	seen := map[string]struct{}{}
	ips := make([]string, 0)
	add := func(raw string) {
		ip := net.ParseIP(strings.TrimSpace(raw))
		if ip == nil {
			return
		}
		normalized := ip.String()
		if _, ok := seen[normalized]; ok {
			return
		}
		seen[normalized] = struct{}{}
		ips = append(ips, normalized)
	}
	for _, record := range bundle.Records {
		for _, candidate := range record.Candidates {
			add(candidate.IP)
		}
		if len(record.Candidates) > 0 {
			continue
		}
		switch strings.ToUpper(record.Type) {
		case model.EdgeDNSRecordTypeA, model.EdgeDNSRecordTypeAAAA:
			for _, value := range record.Values {
				add(value)
			}
		}
	}
	sort.Strings(ips)
	return ips
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
		err = fmt.Errorf("send dns heartbeat: %s", s.redact(err.Error()))
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
		s.Logger.Printf("dns heartbeat success; dns_node_id=%s edge_group_id=%s status=%s bundle=%s records=%d", strings.TrimSpace(s.Config.DNSNodeID), strings.TrimSpace(s.Config.EdgeGroupID), dnsHealthStatus(status), status.BundleVersion, status.RecordCount)
	}
	return nil
}

func (s *Service) newHeartbeatRequest(ctx context.Context) (*http.Request, error) {
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(s.Config.APIURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("invalid FUGUE_API_URL")
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/v1/dns/heartbeat"
	query := base.Query()
	query.Set("token", strings.TrimSpace(s.Config.EdgeToken))
	base.RawQuery = query.Encode()

	snapshot := s.metricSnapshot()
	status := snapshot.Status
	queryCount, queryErrorCount, rcodeCounts, qtypeCounts := dnsQueryMetricCounters(snapshot.Metrics.QueryTotal)
	body := map[string]any{
		"dns_node_id":              strings.TrimSpace(s.Config.DNSNodeID),
		"edge_group_id":            strings.TrimSpace(s.Config.EdgeGroupID),
		"public_hostname":          strings.TrimSpace(s.Config.PublicHostname),
		"public_ipv4":              firstNonEmpty(strings.TrimSpace(s.Config.PublicIPv4), firstAnswerIPByFamily(s.Config.AnswerIPs, true)),
		"public_ipv6":              firstNonEmpty(strings.TrimSpace(s.Config.PublicIPv6), firstAnswerIPByFamily(s.Config.AnswerIPs, false)),
		"mesh_ip":                  strings.TrimSpace(s.Config.MeshIP),
		"zone":                     normalizeName(firstNonEmpty(status.Zone, s.Config.Zone)),
		"dns_bundle_version":       strings.TrimSpace(status.BundleVersion),
		"serving_generation":       strings.TrimSpace(status.ServingGeneration),
		"lkg_generation":           strings.TrimSpace(status.LKGGeneration),
		"last_good_generation":     strings.TrimSpace(status.LastGoodGeneration),
		"cache_corrupt_generation": strings.TrimSpace(status.CacheCorruptGeneration),
		"record_count":             status.RecordCount,
		"cache_status":             dnsCacheStatus(status),
		"max_stale_exceeded":       status.MaxStaleExceeded,
		"cache_write_errors":       snapshot.Metrics.CacheWriteError,
		"cache_load_errors":        snapshot.Metrics.CacheLoadError,
		"bundle_sync_errors":       snapshot.Metrics.BundleSyncError,
		"query_count":              queryCount,
		"query_error_count":        queryErrorCount,
		"query_rcode_counts":       rcodeCounts,
		"query_qtype_counts":       qtypeCounts,
		"listen_addr":              strings.TrimSpace(s.Config.ListenAddr),
		"udp_addr":                 strings.TrimSpace(s.Config.UDPAddr),
		"tcp_addr":                 strings.TrimSpace(s.Config.TCPAddr),
		"udp_listen":               strings.TrimSpace(s.Config.UDPAddr) != "",
		"tcp_listen":               strings.TrimSpace(s.Config.TCPAddr) != "",
		"status":                   dnsHealthStatus(status),
		"healthy":                  status.Healthy,
		"last_error":               strings.TrimSpace(status.LastError),
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal dns heartbeat: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build dns heartbeat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func (s *Service) heartbeatEnabled() bool {
	return strings.TrimSpace(s.Config.APIURL) != "" &&
		strings.TrimSpace(s.Config.EdgeToken) != "" &&
		strings.TrimSpace(s.Config.DNSNodeID) != "" &&
		strings.TrimSpace(s.Config.EdgeGroupID) != "" &&
		normalizeName(s.Config.Zone) != "" &&
		s.heartbeatInterval() > 0
}

func (s *Service) heartbeatInterval() time.Duration {
	if s.Config.HeartbeatInterval > 0 {
		return s.Config.HeartbeatInterval
	}
	return 30 * time.Second
}

func dnsHealthStatus(status Status) string {
	switch {
	case status.Healthy && !status.StaleCache && strings.TrimSpace(status.LastError) == "":
		return model.EdgeHealthHealthy
	case status.Healthy:
		return model.EdgeHealthDegraded
	default:
		return model.EdgeHealthUnhealthy
	}
}

func dnsCacheStatus(status Status) string {
	if strings.TrimSpace(status.BundleVersion) == "" {
		return "missing"
	}
	if status.StaleCache {
		return "stale"
	}
	return "ready"
}

func dnsQueryMetricCounters(metrics map[dnsQueryMetricKey]uint64) (uint64, uint64, map[string]uint64, map[string]uint64) {
	rcodeCounts := make(map[string]uint64)
	qtypeCounts := make(map[string]uint64)
	var queryCount uint64
	var queryErrorCount uint64
	for key, count := range metrics {
		qtype := strings.TrimSpace(key.Type)
		if qtype == "" {
			qtype = "unknown"
		}
		rcode := strings.TrimSpace(key.RCode)
		if rcode == "" {
			rcode = "unknown"
		}
		queryCount += count
		qtypeCounts[qtype] += count
		rcodeCounts[rcode] += count
		if !strings.EqualFold(rcode, "NOERROR") {
			queryErrorCount += count
		}
	}
	return queryCount, queryErrorCount, rcodeCounts, qtypeCounts
}

func firstAnswerIPByFamily(values []string, ipv4 bool) string {
	for _, value := range values {
		ip := net.ParseIP(strings.TrimSpace(value))
		if ip == nil {
			continue
		}
		if ipv4 && ip.To4() != nil {
			return ip.String()
		}
		if !ipv4 && ip.To4() == nil {
			return ip.String()
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (s *Service) validateConfig() error {
	if strings.TrimSpace(s.Config.APIURL) == "" {
		return fmt.Errorf("FUGUE_API_URL is required")
	}
	if strings.TrimSpace(s.Config.EdgeToken) == "" {
		return fmt.Errorf("FUGUE_DNS_TOKEN or FUGUE_EDGE_TOKEN is required")
	}
	if normalizeName(s.Config.Zone) == "" {
		return fmt.Errorf("FUGUE_DNS_ZONE is required")
	}
	if len(s.Config.AnswerIPs) == 0 {
		return fmt.Errorf("FUGUE_DNS_ANSWER_IPS is required")
	}
	for _, value := range s.Config.AnswerIPs {
		if net.ParseIP(strings.TrimSpace(value)) == nil {
			return fmt.Errorf("FUGUE_DNS_ANSWER_IPS contains invalid IP %q", value)
		}
	}
	if s.Config.TTL <= 0 || s.Config.TTL > 3600 {
		return fmt.Errorf("FUGUE_DNS_TTL must be between 1 and 3600")
	}
	if strings.TrimSpace(s.Config.ListenAddr) == "" {
		return fmt.Errorf("FUGUE_DNS_LISTEN_ADDR is required")
	}
	if strings.TrimSpace(s.Config.UDPAddr) == "" && strings.TrimSpace(s.Config.TCPAddr) == "" {
		return fmt.Errorf("FUGUE_DNS_UDP_ADDR or FUGUE_DNS_TCP_ADDR is required")
	}
	return nil
}

func (s *Service) startHTTPServer() (func(context.Context) error, error) {
	addr := strings.TrimSpace(s.Config.ListenAddr)
	if addr == "" {
		return nil, nil
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen dns health %s: %w", addr, err)
	}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.Logger.Printf("dns health server failed: %v", err)
		}
	}()
	return server.Shutdown, nil
}

func (s *Service) startDNSServers() (func(), error) {
	servers := make([]*miekgdns.Server, 0, 2)
	if addr := strings.TrimSpace(s.Config.UDPAddr); addr != "" {
		packetConn, err := net.ListenPacket("udp", addr)
		if err != nil {
			return nil, fmt.Errorf("listen dns udp %s: %w", addr, err)
		}
		server := &miekgdns.Server{PacketConn: packetConn, Net: "udp", Handler: s}
		go func() {
			if err := server.ActivateAndServe(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.Logger.Printf("dns udp server failed: %v", err)
			}
		}()
		servers = append(servers, server)
	}
	if addr := strings.TrimSpace(s.Config.TCPAddr); addr != "" {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			for _, started := range servers {
				_ = started.Shutdown()
			}
			return nil, fmt.Errorf("listen dns tcp %s: %w", addr, err)
		}
		server := &miekgdns.Server{Listener: listener, Net: "tcp", Handler: s}
		go func() {
			if err := server.ActivateAndServe(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.Logger.Printf("dns tcp server failed: %v", err)
			}
		}()
		servers = append(servers, server)
	}
	return func() {
		for _, server := range servers {
			_ = server.Shutdown()
		}
	}, nil
}

func (s *Service) writeCache(cached cacheFile) error {
	path := strings.TrimSpace(s.Config.CachePath)
	if path == "" {
		return nil
	}
	data, err := json.MarshalIndent(cached, "", "  ")
	if err != nil {
		return err
	}
	s.preservePreviousCache(path)
	return lkgcache.WriteCurrent(path, edgeDNSCacheGeneration(cached.Bundle), data, s.cacheArchiveLimit())
}

func (s *Service) LoadPreviousCache() error {
	cachePath := strings.TrimSpace(s.Config.CachePath)
	if cachePath == "" {
		return fmt.Errorf("previous dns cache path is not configured")
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
			lastErr = fmt.Errorf("decode previous dns cache %s: %w", candidate.Path, err)
			s.recordCacheLoad("error")
			continue
		}
		if cached.Version != cacheFileVersion {
			lastErr = fmt.Errorf("unsupported dns cache file version %d in %s", cached.Version, candidate.Path)
			s.recordCacheLoad("error")
			continue
		}
		if err := s.verifyCachedBundle(cached.Bundle, time.Now().UTC()); err != nil {
			lastErr = fmt.Errorf("verify previous dns cache %s: %w", candidate.Path, err)
			s.recordCacheLoad("error")
			continue
		}
		s.setBundle(cached.Bundle, cached.ETag, true, "")
		s.recordCacheLoad("success")
		if s.Logger != nil {
			s.Logger.Printf("dns previous cache loaded; version=%s etag=%s path=%s", cached.Bundle.Version, cached.ETag, candidate.Path)
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
		if err := json.Unmarshal(data, &cached); err != nil || cached.Version != cacheFileVersion {
			return false
		}
		return s.verifyCachedBundle(cached.Bundle, time.Now().UTC()) == nil
	})
	if err != nil && !os.IsNotExist(err) && s.Logger != nil {
		s.Logger.Printf("preserve previous dns cache failed: %v", err)
	}
}

func previousCachePath(path string) string {
	return lkgcache.PreviousPath(path)
}

func (s *Service) verifyBundle(bundle model.EdgeDNSBundle, now time.Time) error {
	keyring := bundleauth.NewKeyring(
		s.Config.BundleSigningKey,
		s.Config.BundleSigningKeyID,
		s.Config.BundleSigningPreviousKey,
		s.Config.BundleSigningPreviousKeyID,
		s.Config.BundleRevokedKeyIDs,
	)
	if err := bundleauth.VerifyEdgeDNSBundleWithKeyring(bundle, keyring, now); err != nil {
		return err
	}
	if strings.TrimSpace(bundle.Version) == "" {
		return fmt.Errorf("dns bundle version is required")
	}
	return nil
}

func (s *Service) verifyCachedBundle(bundle model.EdgeDNSBundle, now time.Time) error {
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

func (s *Service) maxStaleExceeded(validUntil, now time.Time) bool {
	maxStale := s.Config.MaxStale
	if maxStale <= 0 || validUntil.IsZero() || now.IsZero() || !now.After(validUntil) {
		return false
	}
	return now.Sub(validUntil) > maxStale
}

func edgeDNSCacheGeneration(bundle model.EdgeDNSBundle) string {
	return firstNonEmpty(bundle.Generation, bundle.Version)
}

func (s *Service) syncInterval() time.Duration {
	if s.Config.SyncInterval <= 0 {
		return 15 * time.Second
	}
	return s.Config.SyncInterval
}

func (s *Service) nsRecords(zone string) []miekgdns.RR {
	nameservers := s.nameservers(zone)
	records := make([]miekgdns.RR, 0, len(nameservers))
	for _, ns := range nameservers {
		records = append(records, &miekgdns.NS{
			Hdr: miekgdns.RR_Header{Name: fqdn(zone), Rrtype: miekgdns.TypeNS, Class: miekgdns.ClassINET, Ttl: uint32(s.ttl())},
			Ns:  fqdn(ns),
		})
	}
	return records
}

func (s *Service) soaRecord(zone string) miekgdns.RR {
	ns := s.nameservers(zone)[0]
	return &miekgdns.SOA{
		Hdr:     miekgdns.RR_Header{Name: fqdn(zone), Rrtype: miekgdns.TypeSOA, Class: miekgdns.ClassINET, Ttl: uint32(s.ttl())},
		Ns:      fqdn(ns),
		Mbox:    fqdn("hostmaster." + zone),
		Serial:  uint32(time.Now().UTC().Unix() / 60),
		Refresh: 300,
		Retry:   60,
		Expire:  3600,
		Minttl:  uint32(s.ttl()),
	}
}

func (s *Service) nameservers(zone string) []string {
	if len(s.Config.Nameservers) == 0 {
		return []string{"ns1." + zone}
	}
	out := make([]string, 0, len(s.Config.Nameservers))
	for _, ns := range s.Config.Nameservers {
		if normalized := normalizeName(ns); normalized != "" {
			out = append(out, normalized)
		}
	}
	if len(out) == 0 {
		return []string{"ns1." + zone}
	}
	return out
}

func (s *Service) ttl() int {
	if s.Config.TTL <= 0 {
		return 60
	}
	return s.Config.TTL
}

func (s *Service) edgeDNSRecordsForQuestion(ctx context.Context, bundle *model.EdgeDNSBundle, name string, qtype uint16, msg *miekgdns.Msg, writer miekgdns.ResponseWriter) ([]miekgdns.RR, bool) {
	var liveHealth edgeDNSLiveHealthFunc
	if s.Config.EdgeHealthProbeEnabled {
		liveHealth = s.edgeIPHealthy
	}
	answers, nameExists := edgeDNSRecordsForQuestion(bundle, name, qtype, s.geoHintForQuery(msg, writer), liveHealth)
	if qtype != miekgdns.TypeA && qtype != miekgdns.TypeAAAA {
		return answers, nameExists
	}
	return s.filterHealthyEdgeAnswers(ctx, answers), nameExists
}

func (s *Service) filterHealthyEdgeAnswers(_ context.Context, answers []miekgdns.RR) []miekgdns.RR {
	if len(answers) == 0 || !s.Config.EdgeHealthProbeEnabled {
		return answers
	}
	filtered := make([]miekgdns.RR, 0, len(answers))
	for _, rr := range answers {
		ip := ""
		switch typed := rr.(type) {
		case *miekgdns.A:
			if typed.A != nil {
				ip = typed.A.String()
			}
		case *miekgdns.AAAA:
			if typed.AAAA != nil {
				ip = typed.AAAA.String()
			}
		default:
			filtered = append(filtered, rr)
			continue
		}
		if ip == "" || s.edgeIPHealthy(ip) {
			filtered = append(filtered, rr)
		}
	}
	return filtered
}

func (s *Service) edgeIPHealthy(ip string) bool {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return false
	}
	s.edgeHealthMu.Lock()
	observation, ok := s.edgeHealth[ip]
	s.edgeHealthMu.Unlock()
	if ok {
		return observation.Healthy
	}
	return true
}

func (s *Service) dialEdgeIP(ctx context.Context, ip string) bool {
	port := s.Config.EdgeHealthProbePort
	if port <= 0 {
		port = 443
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func edgeDNSRecordsForQuestion(bundle *model.EdgeDNSBundle, name string, qtype uint16, hint dnsGeoHint, liveHealth edgeDNSLiveHealthFunc) ([]miekgdns.RR, bool) {
	if bundle == nil {
		return nil, false
	}
	recordTypes := []string{}
	switch qtype {
	case miekgdns.TypeA:
		recordTypes = []string{model.EdgeDNSRecordTypeA}
	case miekgdns.TypeAAAA:
		recordTypes = []string{model.EdgeDNSRecordTypeAAAA}
	case miekgdns.TypeCAA:
		recordTypes = []string{model.EdgeDNSRecordTypeCAA}
	case miekgdns.TypeCNAME:
		recordTypes = []string{model.EdgeDNSRecordTypeCNAME}
	case miekgdns.TypeMX:
		recordTypes = []string{model.EdgeDNSRecordTypeMX}
	case miekgdns.TypeNS:
		recordTypes = []string{model.EdgeDNSRecordTypeNS}
	case miekgdns.TypeTXT:
		recordTypes = []string{model.EdgeDNSRecordTypeTXT}
	default:
		return nil, edgeDNSNameExists(bundle, name)
	}
	matchingRecords, ownerName := edgeDNSMatchingRecords(bundle, name)
	nameExists := len(matchingRecords) > 0
	answers := make([]miekgdns.RR, 0)
	for _, recordType := range recordTypes {
		for _, record := range matchingRecords {
			if strings.EqualFold(record.Type, recordType) {
				answers = append(answers, rrForEdgeDNSRecordWithGeo(record, ownerName, qtype, hint, liveHealth)...)
			}
		}
	}
	if len(answers) == 0 && (qtype == miekgdns.TypeA || qtype == miekgdns.TypeAAAA) {
		for _, record := range matchingRecords {
			if strings.EqualFold(record.Type, model.EdgeDNSRecordTypeCNAME) {
				answers = append(answers, rrForEdgeDNSRecord(record, ownerName)...)
			}
		}
	}
	return answers, nameExists
}

func edgeDNSMatchingRecords(bundle *model.EdgeDNSBundle, name string) ([]model.EdgeDNSRecord, string) {
	if bundle == nil {
		return nil, normalizeName(name)
	}
	name = normalizeName(name)
	exact := make([]model.EdgeDNSRecord, 0)
	for _, record := range bundle.Records {
		if normalizeName(record.Name) == name {
			exact = append(exact, record)
		}
	}
	if len(exact) > 0 {
		return exact, name
	}
	wildcard := edgeDNSWildcardName(name)
	if wildcard == "" {
		return nil, name
	}
	matches := make([]model.EdgeDNSRecord, 0)
	for _, record := range bundle.Records {
		if normalizeName(record.Name) == wildcard {
			matches = append(matches, record)
		}
	}
	return matches, name
}

func edgeDNSWildcardName(name string) string {
	name = normalizeName(name)
	if name == "" {
		return ""
	}
	if index := strings.IndexByte(name, '.'); index > 0 && index+1 < len(name) {
		return "*." + name[index+1:]
	}
	return ""
}

func rrForEdgeDNSRecord(record model.EdgeDNSRecord, ownerName string) []miekgdns.RR {
	ttl := uint32(record.TTL)
	if ttl == 0 {
		ttl = 60
	}
	ownerName = normalizeName(firstNonEmpty(ownerName, record.Name))
	rrs := make([]miekgdns.RR, 0, len(record.Values))
	for _, value := range record.Values {
		switch strings.ToUpper(record.Type) {
		case model.EdgeDNSRecordTypeA:
			ip := net.ParseIP(value)
			if ip == nil {
				continue
			}
			if v4 := ip.To4(); v4 != nil {
				rrs = append(rrs, &miekgdns.A{
					Hdr: miekgdns.RR_Header{Name: fqdn(ownerName), Rrtype: miekgdns.TypeA, Class: miekgdns.ClassINET, Ttl: ttl},
					A:   v4,
				})
			}
		case model.EdgeDNSRecordTypeAAAA:
			ip := net.ParseIP(value)
			if ip == nil {
				continue
			}
			if ip.To4() == nil {
				rrs = append(rrs, &miekgdns.AAAA{
					Hdr:  miekgdns.RR_Header{Name: fqdn(ownerName), Rrtype: miekgdns.TypeAAAA, Class: miekgdns.ClassINET, Ttl: ttl},
					AAAA: ip,
				})
			}
		case model.EdgeDNSRecordTypeCAA:
			if caa, ok := parseEdgeDNSCAA(value); ok {
				caa.Hdr = miekgdns.RR_Header{Name: fqdn(ownerName), Rrtype: miekgdns.TypeCAA, Class: miekgdns.ClassINET, Ttl: ttl}
				rrs = append(rrs, caa)
			}
		case model.EdgeDNSRecordTypeCNAME:
			target := normalizeName(value)
			if target != "" {
				rrs = append(rrs, &miekgdns.CNAME{
					Hdr:    miekgdns.RR_Header{Name: fqdn(ownerName), Rrtype: miekgdns.TypeCNAME, Class: miekgdns.ClassINET, Ttl: ttl},
					Target: fqdn(target),
				})
			}
		case model.EdgeDNSRecordTypeMX:
			if mx, ok := parseEdgeDNSMX(value); ok {
				mx.Hdr = miekgdns.RR_Header{Name: fqdn(ownerName), Rrtype: miekgdns.TypeMX, Class: miekgdns.ClassINET, Ttl: ttl}
				rrs = append(rrs, mx)
			}
		case model.EdgeDNSRecordTypeNS:
			target := normalizeName(value)
			if target != "" {
				rrs = append(rrs, &miekgdns.NS{
					Hdr: miekgdns.RR_Header{Name: fqdn(ownerName), Rrtype: miekgdns.TypeNS, Class: miekgdns.ClassINET, Ttl: ttl},
					Ns:  fqdn(target),
				})
			}
		case model.EdgeDNSRecordTypeTXT:
			if strings.TrimSpace(value) != "" {
				rrs = append(rrs, &miekgdns.TXT{
					Hdr: miekgdns.RR_Header{Name: fqdn(ownerName), Rrtype: miekgdns.TypeTXT, Class: miekgdns.ClassINET, Ttl: ttl},
					Txt: edgeDNSTXTChunks(value),
				})
			}
		}
	}
	return rrs
}

type dnsGeoHint struct {
	IP          string
	Country     string
	Region      string
	ASN         string
	EdgeGroupID string
	Source      string
}

func (s *Service) geoHintForQuery(msg *miekgdns.Msg, writer miekgdns.ResponseWriter) dnsGeoHint {
	if hint := s.geoHintFromEDNS(msg); hint.IP != "" {
		return s.geoHintForIP(hint.IP, "ecs")
	}
	if writer != nil {
		if host, _, err := net.SplitHostPort(writer.RemoteAddr().String()); err == nil && host != "" {
			if hint := s.geoHintForIP(host, "remote_addr"); hint.IP != "" {
				return hint
			}
		}
	}
	return dnsGeoHint{}
}

func (s *Service) applyECSResponseScope(resp, req *miekgdns.Msg, writer miekgdns.ResponseWriter) {
	if resp == nil || req == nil {
		return
	}
	opt := req.IsEdns0()
	if opt == nil {
		return
	}
	var subnet *miekgdns.EDNS0_SUBNET
	for _, extra := range opt.Option {
		if candidate, ok := extra.(*miekgdns.EDNS0_SUBNET); ok && candidate != nil {
			subnet = candidate
			break
		}
	}
	if subnet == nil {
		return
	}
	scopeNetmask := uint8(0)
	if hint := s.geoHintForQuery(req, writer); hint.Country != "" || hint.Region != "" || hint.ASN != "" || hint.EdgeGroupID != "" {
		scopeNetmask = subnet.SourceNetmask
	}
	respOpt := resp.IsEdns0()
	if respOpt == nil {
		respOpt = new(miekgdns.OPT)
		respOpt.Hdr.Name = "."
		respOpt.Hdr.Rrtype = miekgdns.TypeOPT
		respOpt.SetUDPSize(opt.UDPSize())
		resp.Extra = append(resp.Extra, respOpt)
	}
	respOpt.Option = append(respOpt.Option, &miekgdns.EDNS0_SUBNET{
		Code:          miekgdns.EDNS0SUBNET,
		Family:        subnet.Family,
		SourceNetmask: subnet.SourceNetmask,
		SourceScope:   scopeNetmask,
		Address:       subnet.Address,
	})
}

func (s *Service) geoHintFromEDNS(msg *miekgdns.Msg) dnsGeoHint {
	if msg == nil {
		return dnsGeoHint{}
	}
	opt := msg.IsEdns0()
	if opt == nil {
		return dnsGeoHint{}
	}
	for _, extra := range opt.Option {
		subnet, ok := extra.(*miekgdns.EDNS0_SUBNET)
		if !ok || subnet == nil {
			continue
		}
		ip := net.IP(subnet.Address)
		if ip == nil {
			continue
		}
		return dnsGeoHint{IP: ip.String()}
	}
	return dnsGeoHint{}
}

func (s *Service) geoHintForIP(raw, source string) dnsGeoHint {
	ip := net.ParseIP(strings.TrimSpace(raw))
	if ip == nil {
		return dnsGeoHint{}
	}
	if hint, ok := s.geoHintForOverride(ip, source); ok {
		return hint
	}
	return dnsGeoHint{
		IP:     ip.String(),
		Source: source,
	}
}

func (s *Service) geoHintForOverride(ip net.IP, source string) (dnsGeoHint, bool) {
	for _, override := range s.Config.GeoIPOverrides {
		_, cidr, err := net.ParseCIDR(strings.TrimSpace(override.CIDR))
		if err != nil || cidr == nil {
			continue
		}
		if !cidr.Contains(ip) {
			continue
		}
		return dnsGeoHint{
			IP:          ip.String(),
			Country:     strings.ToLower(strings.TrimSpace(override.Country)),
			Region:      strings.TrimSpace(override.Region),
			ASN:         strings.TrimSpace(override.ASN),
			EdgeGroupID: strings.TrimSpace(override.EdgeGroupID),
			Source:      source,
		}, true
	}
	return dnsGeoHint{}, false
}

func edgeGroupCountry(edgeGroupID string) string {
	edgeGroupID = strings.ToLower(strings.TrimSpace(edgeGroupID))
	switch {
	case strings.Contains(edgeGroupID, "-country-"):
		return edgeGroupID[strings.Index(edgeGroupID, "-country-")+len("-country-"):]
	default:
		return ""
	}
}

func rrForEdgeDNSRecordWithGeo(record model.EdgeDNSRecord, ownerName string, qtype uint16, hint dnsGeoHint, liveHealth edgeDNSLiveHealthFunc) []miekgdns.RR {
	if len(record.Candidates) == 0 || (qtype != miekgdns.TypeA && qtype != miekgdns.TypeAAAA) {
		return rrForEdgeDNSRecord(record, ownerName)
	}
	ordered := edgeDNSOrderedCandidates(record, hint, time.Now().UTC())
	if liveHealth != nil && len(ordered) > 0 {
		filtered := ordered[:0]
		for _, candidate := range ordered {
			if candidate.IP == "" || liveHealth(candidate.IP) {
				filtered = append(filtered, candidate)
			}
		}
		ordered = filtered
	}
	if len(ordered) == 0 {
		return nil
	}
	if limit := edgeDNSAnswerCandidateLimit(record.AnswerPolicy, ordered); limit > 0 && len(ordered) > limit {
		ordered = ordered[:limit]
	}
	ttl := uint32(record.TTL)
	if ttl == 0 {
		ttl = 60
	}
	ownerName = normalizeName(firstNonEmpty(ownerName, record.Name))
	rrs := make([]miekgdns.RR, 0, len(ordered))
	for _, candidate := range ordered {
		ip := net.ParseIP(strings.TrimSpace(candidate.IP))
		if ip == nil {
			continue
		}
		switch qtype {
		case miekgdns.TypeA:
			if v4 := ip.To4(); v4 != nil {
				rrs = append(rrs, &miekgdns.A{Hdr: miekgdns.RR_Header{Name: fqdn(ownerName), Rrtype: miekgdns.TypeA, Class: miekgdns.ClassINET, Ttl: ttl}, A: v4})
			}
		case miekgdns.TypeAAAA:
			if ip.To4() == nil {
				rrs = append(rrs, &miekgdns.AAAA{Hdr: miekgdns.RR_Header{Name: fqdn(ownerName), Rrtype: miekgdns.TypeAAAA, Class: miekgdns.ClassINET, Ttl: ttl}, AAAA: ip})
			}
		}
	}
	if len(rrs) == 0 {
		return nil
	}
	return rrs
}

func edgeDNSAnswerCandidateLimit(policy model.DNSAnswerPolicy, ordered []model.EdgeDNSAnswerCandidate) int {
	if len(ordered) <= 1 {
		return len(ordered)
	}
	switch strings.TrimSpace(policy.PolicyKind) {
	case model.DNSAnswerPolicyKindPinned:
		return 1
	case model.DNSAnswerPolicyKindLatencyAware:
		return 1
	case model.DNSAnswerPolicyKindGeo, model.DNSAnswerPolicyKindWeighted, model.DNSAnswerPolicyKindGlobal, "":
		return 1
	case model.DNSAnswerPolicyKindDisabled:
		return len(ordered)
	default:
		return 1
	}
}

func edgeDNSOrderedCandidates(record model.EdgeDNSRecord, hint dnsGeoHint, now time.Time) []model.EdgeDNSAnswerCandidate {
	policy := record.AnswerPolicy
	sourceCandidates := record.Candidates
	if scoped, ok := edgeDNSScopedCandidatesForHint(record.ScopedCandidates, hint); ok {
		sourceCandidates = scoped.Candidates
		if strings.TrimSpace(scoped.PolicyKind) != "" {
			policy.PolicyKind = scoped.PolicyKind
		}
		if strings.TrimSpace(scoped.Reason) != "" {
			policy.Reason = scoped.Reason
		}
	}
	candidates := make([]model.EdgeDNSAnswerCandidate, 0, len(sourceCandidates))
	for _, candidate := range sourceCandidates {
		if !edgeDNSCandidateEligible(candidate, policy) {
			continue
		}
		candidates = append(candidates, candidate)
	}
	hasLatencyScore := strings.TrimSpace(policy.PolicyKind) == model.DNSAnswerPolicyKindLatencyAware && edgeDNSAnyCandidateScore(candidates)
	sort.SliceStable(candidates, func(i, j int) bool {
		ai := edgeDNSCandidateSortScore(candidates[i], policy, hint, hasLatencyScore)
		aj := edgeDNSCandidateSortScore(candidates[j], policy, hint, hasLatencyScore)
		if ai != aj {
			return ai < aj
		}
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority < candidates[j].Priority
		}
		if candidates[i].Weight != candidates[j].Weight {
			return candidates[i].Weight > candidates[j].Weight
		}
		if candidates[i].EdgeGroupID != candidates[j].EdgeGroupID {
			return candidates[i].EdgeGroupID < candidates[j].EdgeGroupID
		}
		return candidates[i].IP < candidates[j].IP
	})
	if promoted, ok := edgeDNSMaybePromoteNodeExploration(record, policy, hint, candidates, now); ok {
		return promoted
	}
	candidates = edgeDNSMaybePromoteExploration(record, policy, hint, candidates, now)
	return candidates
}

func edgeDNSAnyCandidateScore(candidates []model.EdgeDNSAnswerCandidate) bool {
	for _, candidate := range candidates {
		if candidate.Score > 0 {
			return true
		}
	}
	return false
}

func edgeDNSCandidateEligible(candidate model.EdgeDNSAnswerCandidate, policy model.DNSAnswerPolicy) bool {
	if policy.HealthRequired && !candidate.Healthy {
		return false
	}
	if policy.RouteReadyRequired && !candidate.RouteReady {
		return false
	}
	if policy.PolicyKind != model.DNSAnswerPolicyKindDisabled {
		if !candidate.TLSReady {
			return false
		}
	}
	return true
}

func edgeDNSScopedCandidatesForHint(scoped []model.EdgeDNSScopedAnswerCandidates, hint dnsGeoHint) (model.EdgeDNSScopedAnswerCandidates, bool) {
	bestScore := 0
	var best model.EdgeDNSScopedAnswerCandidates
	for _, candidate := range scoped {
		score := edgeDNSScopedCandidateMatchScore(candidate, hint)
		if score == 0 {
			continue
		}
		if score > bestScore || (score == bestScore && candidate.ScopeKey < best.ScopeKey) {
			bestScore = score
			best = candidate
		}
	}
	return best, bestScore > 0
}

func edgeDNSScopedCandidateMatchScore(candidate model.EdgeDNSScopedAnswerCandidates, hint dnsGeoHint) int {
	score := 0
	if hint.ASN != "" && candidate.ASN != "" && strings.EqualFold(candidate.ASN, hint.ASN) {
		score += 8000
	}
	if hint.Country != "" && candidate.Country != "" && strings.EqualFold(candidate.Country, hint.Country) {
		score += 4000
	}
	if hint.Region != "" && candidate.Region != "" && strings.EqualFold(candidate.Region, hint.Region) {
		score += 2000
	}
	if score == 0 {
		return 0
	}
	return score
}

func edgeDNSMaybePromoteExploration(record model.EdgeDNSRecord, policy model.DNSAnswerPolicy, hint dnsGeoHint, candidates []model.EdgeDNSAnswerCandidate, now time.Time) []model.EdgeDNSAnswerCandidate {
	if len(candidates) <= 1 {
		return candidates
	}
	hash, ok := edgeDNSExplorationHash(record, policy, hint, now)
	if !ok {
		return candidates
	}
	percent := edgeDNSExplorationPercent(policy)
	if int(hash%100) >= percent {
		return candidates
	}
	rest := len(candidates) - 1
	if rest <= 0 {
		return candidates
	}
	index := 1 + int((hash/100)%uint64(rest))
	if index <= 0 || index >= len(candidates) {
		return candidates
	}
	out := append([]model.EdgeDNSAnswerCandidate(nil), candidates...)
	explorer := out[index]
	copy(out[1:index+1], out[0:index])
	out[0] = explorer
	return out
}

func edgeDNSMaybePromoteNodeExploration(record model.EdgeDNSRecord, policy model.DNSAnswerPolicy, hint dnsGeoHint, candidates []model.EdgeDNSAnswerCandidate, now time.Time) ([]model.EdgeDNSAnswerCandidate, bool) {
	if len(candidates) <= 1 {
		return candidates, false
	}
	primaryGroupID := strings.TrimSpace(candidates[0].EdgeGroupID)
	if primaryGroupID == "" {
		return candidates, false
	}
	siblingIndexes := make([]int, 0, len(candidates)-1)
	for index := 1; index < len(candidates); index++ {
		if strings.EqualFold(strings.TrimSpace(candidates[index].EdgeGroupID), primaryGroupID) {
			siblingIndexes = append(siblingIndexes, index)
		}
	}
	if len(siblingIndexes) == 0 {
		return candidates, false
	}
	hash, ok := edgeDNSExplorationHash(record, policy, hint, now)
	if !ok {
		return candidates, false
	}
	percent := edgeDNSExplorationPercent(policy)
	if int(hash%100) >= percent {
		return candidates, false
	}
	index := siblingIndexes[int((hash/100)%uint64(len(siblingIndexes)))]
	out := append([]model.EdgeDNSAnswerCandidate(nil), candidates...)
	explorer := out[index]
	copy(out[1:index+1], out[0:index])
	out[0] = explorer
	return out, true
}

func edgeDNSExplorationHash(record model.EdgeDNSRecord, policy model.DNSAnswerPolicy, hint dnsGeoHint, now time.Time) (uint64, bool) {
	switch strings.TrimSpace(policy.PolicyKind) {
	case model.DNSAnswerPolicyKindDisabled, model.DNSAnswerPolicyKindPinned:
		return 0, false
	}
	if edgeDNSExplorationPercent(policy) <= 0 {
		return 0, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	bucket := now.Unix() / int64((10 * time.Minute).Seconds())
	scope := firstNonEmpty(hint.ASN, hint.Region, hint.Country, hint.EdgeGroupID, hint.IP, hint.Source, "global")
	seed := strings.Join([]string{
		normalizeName(record.Name),
		strings.ToUpper(strings.TrimSpace(record.Type)),
		scope,
		strconv.FormatInt(bucket, 10),
	}, "|")
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(seed))
	return hash.Sum64(), true
}

func edgeDNSExplorationPercent(policy model.DNSAnswerPolicy) int {
	percent := policy.ExplorationPercent
	if percent <= 0 {
		return 0
	}
	if percent > 50 {
		return 50
	}
	return percent
}

func edgeDNSCandidateSortScore(candidate model.EdgeDNSAnswerCandidate, policy model.DNSAnswerPolicy, hint dnsGeoHint, latencyScoreMode bool) int {
	score := candidate.Priority * 100
	switch policy.PolicyKind {
	case model.DNSAnswerPolicyKindLatencyAware:
		if latencyScoreMode {
			// Composite quality scores already include latency, error, body-read and cache health.
			// Keep route priority as a preference and weight as a tie-break, not the primary signal.
			score = 500000 + candidate.Priority*10 - candidate.Weight
			if candidate.Score > 0 {
				score = int(candidate.Score) + candidate.Priority*10 - candidate.Weight
			}
		} else {
			score = candidate.Priority*10 - candidate.Weight*20
		}
	case model.DNSAnswerPolicyKindWeighted:
		score -= candidate.Weight * 20
	}
	if policy.PolicyKind != model.DNSAnswerPolicyKindLatencyAware {
		if hint.EdgeGroupID != "" && strings.EqualFold(candidate.EdgeGroupID, hint.EdgeGroupID) {
			score -= 10000
		}
		if hint.Country != "" && strings.EqualFold(candidate.Country, hint.Country) {
			score -= 5000
		}
		if hint.Region != "" && strings.EqualFold(candidate.Region, hint.Region) {
			score -= 2500
		}
		if hint.ASN != "" && strings.Contains(strings.ToLower(candidate.Reason), "asn_"+strings.ToLower(hint.ASN)) {
			score -= 1250
		}
		if strings.EqualFold(candidate.Reason, "same_region") {
			score -= 250
		}
	}
	if candidate.Score > 0 && !(policy.PolicyKind == model.DNSAnswerPolicyKindLatencyAware && latencyScoreMode) {
		score += int(candidate.Score)
	}
	return score
}

func edgeDNSNameExists(bundle *model.EdgeDNSBundle, name string) bool {
	if bundle == nil {
		return false
	}
	records, _ := edgeDNSMatchingRecords(bundle, name)
	return len(records) > 0
}

func parseEdgeDNSMX(value string) (*miekgdns.MX, bool) {
	fields := strings.Fields(strings.TrimSpace(value))
	if len(fields) == 0 {
		return nil, false
	}
	preference := uint16(10)
	exchange := fields[0]
	if len(fields) > 1 {
		if parsed, err := strconv.ParseUint(fields[0], 10, 16); err == nil {
			preference = uint16(parsed)
			exchange = fields[1]
		}
	}
	exchange = normalizeName(exchange)
	if exchange == "" {
		return nil, false
	}
	return &miekgdns.MX{Preference: preference, Mx: fqdn(exchange)}, true
}

func parseEdgeDNSCAA(value string) (*miekgdns.CAA, bool) {
	fields := strings.Fields(strings.TrimSpace(value))
	if len(fields) < 3 {
		return nil, false
	}
	flag, err := strconv.ParseUint(fields[0], 10, 8)
	if err != nil {
		return nil, false
	}
	tag := strings.TrimSpace(fields[1])
	if tag == "" {
		return nil, false
	}
	content := strings.TrimSpace(strings.Join(fields[2:], " "))
	if unquoted, err := strconv.Unquote(content); err == nil {
		content = unquoted
	} else {
		content = strings.Trim(content, `"`)
	}
	if content == "" {
		return nil, false
	}
	return &miekgdns.CAA{Flag: uint8(flag), Tag: tag, Value: content}, true
}

func edgeDNSTXTChunks(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if len(value) <= 255 {
		return []string{value}
	}
	chunks := make([]string, 0, (len(value)/255)+1)
	for len(value) > 255 {
		chunks = append(chunks, value[:255])
		value = value[255:]
	}
	if value != "" {
		chunks = append(chunks, value)
	}
	return chunks
}

func nameWithinZone(name, zone string) bool {
	name = normalizeName(name)
	zone = normalizeName(zone)
	return name != "" && zone != "" && (name == zone || strings.HasSuffix(name, "."+zone))
}

func normalizeName(value string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(value)), ".")
}

func fqdn(value string) string {
	return miekgdns.Fqdn(normalizeName(value))
}

func safeBaseURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "<invalid>"
	}
	return parsed.Scheme + "://" + parsed.Host
}

func (s *Service) redact(value string) string {
	token := strings.TrimSpace(s.Config.EdgeToken)
	if token == "" {
		return value
	}
	return strings.ReplaceAll(value, token, "<redacted>")
}

func strconvQuote(value string) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func dnsBoolGauge(value bool) int {
	if value {
		return 1
	}
	return 0
}

func dnsUnixSeconds(value *time.Time) float64 {
	if value == nil || value.IsZero() {
		return 0
	}
	return float64(value.Unix())
}

func dnsStaleAgeSeconds(stale bool, lastSuccessAt *time.Time, now time.Time) float64 {
	if !stale || lastSuccessAt == nil || lastSuccessAt.IsZero() || now.IsZero() {
		return 0
	}
	age := now.Sub(*lastSuccessAt)
	if age < 0 {
		return 0
	}
	return age.Seconds()
}

func dnsPrometheusLabelValue(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	return strings.ReplaceAll(value, `"`, `\"`)
}

func cloneQueryMetrics(in map[dnsQueryMetricKey]uint64) map[dnsQueryMetricKey]uint64 {
	out := make(map[dnsQueryMetricKey]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type queryMetricEntry struct {
	Key   dnsQueryMetricKey
	Value uint64
}

func sortedQueryMetricEntries(metrics map[dnsQueryMetricKey]uint64) []queryMetricEntry {
	entries := make([]queryMetricEntry, 0, len(metrics))
	for key, value := range metrics {
		entries = append(entries, queryMetricEntry{Key: key, Value: value})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Key.Type != entries[j].Key.Type {
			return entries[i].Key.Type < entries[j].Key.Type
		}
		return entries[i].Key.RCode < entries[j].Key.RCode
	})
	return entries
}
