package dnsserver

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
	"sort"
	"strings"
	"sync"
	"time"

	miekgdns "github.com/miekg/dns"

	"fugue/internal/config"
	"fugue/internal/httpx"
	"fugue/internal/model"
)

const cacheFileVersion = 1

type Service struct {
	Config     config.DNSConfig
	HTTPClient *http.Client
	Logger     *log.Logger

	mu       sync.Mutex
	snapshot Status
	bundle   *model.EdgeDNSBundle
	etag     string
	metrics  telemetry
}

type Status struct {
	Status        string     `json:"status"`
	Healthy       bool       `json:"healthy"`
	DNSNodeID     string     `json:"dns_node_id,omitempty"`
	EdgeGroupID   string     `json:"edge_group_id,omitempty"`
	Zone          string     `json:"zone,omitempty"`
	BundleVersion string     `json:"bundle_version,omitempty"`
	RecordCount   int        `json:"record_count"`
	LastSyncAt    *time.Time `json:"last_sync_at,omitempty"`
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	StaleCache    bool       `json:"stale_cache"`
	CachePath     string     `json:"cache_path,omitempty"`
	ListenAddr    string     `json:"listen_addr,omitempty"`
	UDPAddr       string     `json:"udp_addr,omitempty"`
	TCPAddr       string     `json:"tcp_addr,omitempty"`
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
		Logger: logger,
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
		if name == zone {
			resp.Answer = append(resp.Answer, s.nsRecords(zone)...)
		} else {
			resp.Ns = append(resp.Ns, s.soaRecord(zone))
		}
	case miekgdns.TypeA, miekgdns.TypeAAAA:
		records, nameExists := edgeDNSRecordsForQuestion(snapshot, name, question.Qtype)
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
		return err
	}
	if cached.Version != cacheFileVersion {
		s.recordCacheLoad("error")
		return fmt.Errorf("unsupported dns cache file version %d", cached.Version)
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
	s.snapshot = Status{
		Status:        "ok",
		Healthy:       true,
		DNSNodeID:     strings.TrimSpace(bundle.DNSNodeID),
		EdgeGroupID:   strings.TrimSpace(bundle.EdgeGroupID),
		Zone:          normalizeName(bundle.Zone),
		BundleVersion: strings.TrimSpace(bundle.Version),
		RecordCount:   len(bundle.Records),
		LastSyncAt:    &now,
		LastSuccessAt: &now,
		LastError:     strings.TrimSpace(lastError),
		StaleCache:    stale,
		CachePath:     strings.TrimSpace(s.Config.CachePath),
		ListenAddr:    strings.TrimSpace(s.Config.ListenAddr),
		UDPAddr:       strings.TrimSpace(s.Config.UDPAddr),
		TCPAddr:       strings.TrimSpace(s.Config.TCPAddr),
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
	}
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
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cached, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".dns-cache-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
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

func edgeDNSRecordsForQuestion(bundle *model.EdgeDNSBundle, name string, qtype uint16) ([]miekgdns.RR, bool) {
	if bundle == nil {
		return nil, false
	}
	recordType := ""
	switch qtype {
	case miekgdns.TypeA:
		recordType = model.EdgeDNSRecordTypeA
	case miekgdns.TypeAAAA:
		recordType = model.EdgeDNSRecordTypeAAAA
	default:
		return nil, edgeDNSNameExists(bundle, name)
	}
	answers := make([]miekgdns.RR, 0)
	nameExists := false
	for _, record := range bundle.Records {
		if normalizeName(record.Name) != name {
			continue
		}
		nameExists = true
		if strings.EqualFold(record.Type, recordType) {
			answers = append(answers, rrForEdgeDNSRecord(record)...)
		}
	}
	return answers, nameExists
}

func rrForEdgeDNSRecord(record model.EdgeDNSRecord) []miekgdns.RR {
	ttl := uint32(record.TTL)
	if ttl == 0 {
		ttl = 60
	}
	rrs := make([]miekgdns.RR, 0, len(record.Values))
	for _, value := range record.Values {
		ip := net.ParseIP(value)
		if ip == nil {
			continue
		}
		switch strings.ToUpper(record.Type) {
		case model.EdgeDNSRecordTypeA:
			if v4 := ip.To4(); v4 != nil {
				rrs = append(rrs, &miekgdns.A{
					Hdr: miekgdns.RR_Header{Name: fqdn(record.Name), Rrtype: miekgdns.TypeA, Class: miekgdns.ClassINET, Ttl: ttl},
					A:   v4,
				})
			}
		case model.EdgeDNSRecordTypeAAAA:
			if ip.To4() == nil {
				rrs = append(rrs, &miekgdns.AAAA{
					Hdr:  miekgdns.RR_Header{Name: fqdn(record.Name), Rrtype: miekgdns.TypeAAAA, Class: miekgdns.ClassINET, Ttl: ttl},
					AAAA: ip,
				})
			}
		}
	}
	return rrs
}

func edgeDNSNameExists(bundle *model.EdgeDNSBundle, name string) bool {
	if bundle == nil {
		return false
	}
	for _, record := range bundle.Records {
		if normalizeName(record.Name) == name {
			return true
		}
	}
	return false
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
