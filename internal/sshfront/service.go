package sshfront

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
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/tcpproxy"
)

type Config struct {
	APIURL                              string
	EdgeToken                           string
	EdgeID                              string
	EdgeGroupID                         string
	ListenHost                          string
	HealthAddr                          string
	CachePath                           string
	PublicPortStart                     int
	PublicPortEnd                       int
	SyncInterval                        time.Duration
	HTTPTimeout                         time.Duration
	DialTimeout                         time.Duration
	IdleTimeout                         time.Duration
	ShutdownTimeout                     time.Duration
	MaxConnectionsPerIP                 int
	MaxConnectionAttemptsPerIPPerMinute int
	MaxConnectionsPerApp                int
	MaxConnectionsPerTenant             int
	BundleSigningKey                    string
	BundleSigningKeyID                  string
	BundleSigningPreviousKey            string
	BundleSigningPreviousKeyID          string
	BundleRevokedKeyIDs                 []string
}

type Service struct {
	Config Config
	Logger *log.Logger

	mu             sync.Mutex
	listeners      map[int]*routeListener
	bundle         model.EdgeSSHRouteBundle
	lastSyncAt     time.Time
	lastOKAt       time.Time
	lastError      string
	syncCount      uint64
	errorCount     uint64
	connCount      uint64
	dialErrors     uint64
	limitRejects   uint64
	bytesIn        uint64
	bytesOut       uint64
	activeConns    int
	activeByIP     map[string]int
	activeByApp    map[string]int
	activeByTenant map[string]int
	rateByIP       map[string]connectionRateWindow
}

type routeListener struct {
	route    model.EdgeSSHRoute
	listener net.Listener
	done     chan struct{}
}

type connectionRateWindow struct {
	start    time.Time
	attempts int
}

func NewService(cfg Config, logger *log.Logger) *Service {
	if logger == nil {
		logger = log.Default()
	}
	return &Service{
		Config: cfg,
		Logger: logger,
	}
}

func (s *Service) Run(ctx context.Context) error {
	cfg := s.withDefaults()
	if err := validateConfig(cfg); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var healthServer *http.Server
	if strings.TrimSpace(cfg.HealthAddr) != "" {
		healthServer = s.startHealthServer(cfg, &wg)
	}

	if err := s.loadCachedBundle(cfg); err != nil && strings.TrimSpace(cfg.CachePath) != "" {
		s.Logger.Printf("ssh-front route cache unavailable; path=%s error=%v", cfg.CachePath, err)
	}
	s.syncAndLog(ctx, cfg)
	ticker := time.NewTicker(cfg.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()
			if healthServer != nil {
				_ = healthServer.Shutdown(shutdownCtx)
			}
			s.closeAllListeners()
			wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			s.syncAndLog(ctx, cfg)
		}
	}
}

func (s *Service) withDefaults() Config {
	cfg := s.Config
	if strings.TrimSpace(cfg.ListenHost) == "" {
		cfg.ListenHost = "0.0.0.0"
	}
	if strings.TrimSpace(cfg.HealthAddr) == "" {
		cfg.HealthAddr = ":7836"
	}
	if cfg.PublicPortStart <= 0 {
		cfg.PublicPortStart = model.DefaultAppSSHPublicPortStart
	}
	if cfg.PublicPortEnd <= 0 {
		cfg.PublicPortEnd = model.DefaultAppSSHPublicPortEnd
	}
	if cfg.PublicPortEnd < cfg.PublicPortStart {
		cfg.PublicPortStart, cfg.PublicPortEnd = cfg.PublicPortEnd, cfg.PublicPortStart
	}
	if cfg.SyncInterval <= 0 {
		cfg.SyncInterval = 15 * time.Second
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 10 * time.Second
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if strings.TrimSpace(cfg.BundleSigningKeyID) == "" {
		cfg.BundleSigningKeyID = "control-plane"
	}
	return cfg
}

func validateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.APIURL) == "" {
		return fmt.Errorf("FUGUE_SSH_FRONT_API_URL or FUGUE_API_URL is required")
	}
	if strings.TrimSpace(cfg.EdgeToken) == "" {
		return fmt.Errorf("FUGUE_SSH_FRONT_EDGE_TOKEN or FUGUE_EDGE_TOKEN is required")
	}
	if cfg.PublicPortStart <= 0 || cfg.PublicPortEnd <= 0 || cfg.PublicPortStart > 65535 || cfg.PublicPortEnd > 65535 {
		return fmt.Errorf("ssh public port range must be within 1-65535")
	}
	return nil
}

func (s *Service) syncAndLog(ctx context.Context, cfg Config) {
	started := time.Now()
	bundle, err := s.fetchBundle(ctx, cfg)
	s.mu.Lock()
	s.lastSyncAt = time.Now().UTC()
	s.syncCount++
	if err != nil {
		redactedError := redactToken(err.Error(), cfg.EdgeToken)
		s.errorCount++
		s.lastError = redactedError
		s.mu.Unlock()
		s.Logger.Printf("ssh-front route sync failed; error=%s", redactedError)
		return
	}
	s.lastOKAt = time.Now().UTC()
	s.lastError = ""
	s.bundle = bundle
	s.mu.Unlock()
	if err := s.reconcile(cfg, bundle.Routes); err != nil {
		s.mu.Lock()
		s.errorCount++
		s.lastError = err.Error()
		s.mu.Unlock()
		s.Logger.Printf("ssh-front route reconcile failed; error=%s", err)
		return
	}
	if err := writeCachedBundle(cfg, bundle); err != nil {
		s.Logger.Printf("ssh-front route cache write failed; path=%s error=%v", cfg.CachePath, err)
	}
	s.Logger.Printf("ssh-front route sync success; version=%s routes=%d duration_ms=%d", bundle.Version, len(bundle.Routes), time.Since(started).Milliseconds())
}

func (s *Service) loadCachedBundle(cfg Config) error {
	path := strings.TrimSpace(cfg.CachePath)
	if path == "" {
		return nil
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var bundle model.EdgeSSHRouteBundle
	if err := json.Unmarshal(payload, &bundle); err != nil {
		return fmt.Errorf("decode cached ssh routes: %w", err)
	}
	if err := verifyBundle(cfg, bundle, time.Now().UTC()); err != nil {
		return fmt.Errorf("verify cached ssh routes: %w", err)
	}
	s.mu.Lock()
	s.bundle = bundle
	s.lastOKAt = time.Now().UTC()
	s.mu.Unlock()
	if err := s.reconcile(cfg, bundle.Routes); err != nil {
		return fmt.Errorf("reconcile cached ssh routes: %w", err)
	}
	s.Logger.Printf("ssh-front loaded cached routes; version=%s routes=%d path=%s", bundle.Version, len(bundle.Routes), path)
	return nil
}

func writeCachedBundle(cfg Config, bundle model.EdgeSSHRouteBundle) error {
	path := strings.TrimSpace(cfg.CachePath)
	if path == "" {
		return nil
	}
	payload, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Service) fetchBundle(ctx context.Context, cfg Config) (model.EdgeSSHRouteBundle, error) {
	endpoint, err := sshRoutesURL(cfg)
	if err != nil {
		return model.EdgeSSHRouteBundle{}, err
	}
	reqCtx, cancel := context.WithTimeout(ctx, cfg.HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return model.EdgeSSHRouteBundle{}, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(cfg.EdgeToken))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return model.EdgeSSHRouteBundle{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return model.EdgeSSHRouteBundle{}, fmt.Errorf("fetch ssh routes status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var bundle model.EdgeSSHRouteBundle
	if err := json.NewDecoder(resp.Body).Decode(&bundle); err != nil {
		return model.EdgeSSHRouteBundle{}, fmt.Errorf("decode ssh routes: %w", err)
	}
	if err := verifyBundle(cfg, bundle, time.Now().UTC()); err != nil {
		return model.EdgeSSHRouteBundle{}, fmt.Errorf("verify ssh routes: %w", err)
	}
	return bundle, nil
}

func sshRoutesURL(cfg Config) (string, error) {
	base, err := url.Parse(strings.TrimSpace(cfg.APIURL))
	if err != nil {
		return "", err
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/v1/edge/ssh/routes"
	query := base.Query()
	query.Set("token", strings.TrimSpace(cfg.EdgeToken))
	if edgeID := strings.TrimSpace(cfg.EdgeID); edgeID != "" {
		query.Set("edge_id", edgeID)
	}
	if edgeGroupID := strings.TrimSpace(cfg.EdgeGroupID); edgeGroupID != "" {
		query.Set("edge_group_id", edgeGroupID)
	}
	base.RawQuery = query.Encode()
	return base.String(), nil
}

func verifyBundle(cfg Config, bundle model.EdgeSSHRouteBundle, now time.Time) error {
	keyring := bundleauth.NewKeyring(
		cfg.BundleSigningKey,
		cfg.BundleSigningKeyID,
		cfg.BundleSigningPreviousKey,
		cfg.BundleSigningPreviousKeyID,
		cfg.BundleRevokedKeyIDs,
	)
	if err := bundleauth.VerifyEdgeSSHRouteBundleWithKeyring(bundle, keyring, now); err != nil {
		return err
	}
	if strings.TrimSpace(bundle.Version) == "" {
		return fmt.Errorf("ssh route bundle version is required")
	}
	return nil
}

func (s *Service) reconcile(cfg Config, routes []model.EdgeSSHRoute) error {
	desired := map[int]model.EdgeSSHRoute{}
	for _, route := range routes {
		if !routePublishable(route, cfg) {
			continue
		}
		desired[route.PublicPort] = route
	}

	s.mu.Lock()
	if s.listeners == nil {
		s.listeners = map[int]*routeListener{}
	}
	existing := s.listeners
	toClose := make([]*routeListener, 0)
	toStart := make([]model.EdgeSSHRoute, 0)
	for port, listener := range existing {
		route, ok := desired[port]
		if !ok || routeKey(route) != routeKey(listener.route) {
			toClose = append(toClose, listener)
			delete(existing, port)
		}
	}
	for port, route := range desired {
		if _, ok := existing[port]; !ok {
			toStart = append(toStart, route)
		}
	}
	s.mu.Unlock()

	for _, listener := range toClose {
		_ = listener.listener.Close()
		<-listener.done
	}
	sort.Slice(toStart, func(i, j int) bool {
		return toStart[i].PublicPort < toStart[j].PublicPort
	})
	for _, route := range toStart {
		listener, err := s.startRouteListener(cfg, route)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.listeners[route.PublicPort] = listener
		s.mu.Unlock()
	}
	return nil
}

func routePublishable(route model.EdgeSSHRoute, cfg Config) bool {
	status := model.NormalizeAppSSHEndpointStatus(route.Status)
	if status != model.AppSSHEndpointStatusReady && status != model.AppSSHEndpointStatusPending {
		return false
	}
	if route.PublicPort < cfg.PublicPortStart || route.PublicPort > cfg.PublicPortEnd {
		return false
	}
	return route.TargetPort > 0 && strings.TrimSpace(route.TargetHost) != ""
}

func routeKey(route model.EdgeSSHRoute) string {
	return strings.Join([]string{
		strconv.Itoa(route.PublicPort),
		strings.TrimSpace(route.TargetHost),
		strconv.Itoa(route.TargetPort),
		strings.TrimSpace(route.RouteGeneration),
	}, "|")
}

func (s *Service) startRouteListener(cfg Config, route model.EdgeSSHRoute) (*routeListener, error) {
	addr := net.JoinHostPort(strings.TrimSpace(cfg.ListenHost), strconv.Itoa(route.PublicPort))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen ssh route app_id=%s port=%d: %w", route.AppID, route.PublicPort, err)
	}
	entry := &routeListener{
		route:    route,
		listener: listener,
		done:     make(chan struct{}),
	}
	go func() {
		defer close(entry.done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				s.Logger.Printf("ssh-front accept failed; port=%d error=%v", route.PublicPort, err)
				continue
			}
			go s.handleConnection(cfg, route, conn)
		}
	}()
	s.Logger.Printf("ssh-front listening; app_id=%s public_port=%d target=%s:%d", route.AppID, route.PublicPort, route.TargetHost, route.TargetPort)
	return entry, nil
}

func (s *Service) handleConnection(cfg Config, route model.EdgeSSHRoute, downstream net.Conn) {
	started := time.Now()
	defer downstream.Close()
	release, accepted := s.acquireConnection(cfg, route, downstream.RemoteAddr())
	if !accepted {
		s.Logger.Printf("ssh-front connection rejected by limit; app_id=%s tenant_id=%s public_port=%d remote=%s", route.AppID, route.TenantID, route.PublicPort, downstream.RemoteAddr())
		return
	}
	defer release()
	s.Logger.Printf("ssh-front connection accepted; app_id=%s tenant_id=%s public_port=%d remote=%s", route.AppID, route.TenantID, route.PublicPort, downstream.RemoteAddr())
	if cfg.IdleTimeout > 0 {
		deadline := time.Now().Add(cfg.IdleTimeout)
		_ = downstream.SetDeadline(deadline)
	}

	target := net.JoinHostPort(strings.TrimSpace(route.TargetHost), strconv.Itoa(route.TargetPort))
	upstream, err := net.DialTimeout("tcp", target, cfg.DialTimeout)
	if err != nil {
		s.mu.Lock()
		s.dialErrors++
		s.mu.Unlock()
		s.Logger.Printf("ssh-front dial failed; app_id=%s public_port=%d target=%s error=%v", route.AppID, route.PublicPort, target, err)
		return
	}
	defer upstream.Close()
	if cfg.IdleTimeout > 0 {
		deadline := time.Now().Add(cfg.IdleTimeout)
		_ = upstream.SetDeadline(deadline)
	}
	clientToTarget, targetToClient, _ := tcpproxy.CopyBidirectional(downstream, upstream, "client_to_target", "target_to_client")
	s.mu.Lock()
	s.bytesIn += uint64(clientToTarget.Bytes)
	s.bytesOut += uint64(targetToClient.Bytes)
	s.mu.Unlock()
	s.Logger.Printf("ssh-front connection closed; app_id=%s public_port=%d target=%s duration_ms=%d client_to_target_bytes=%d target_to_client_bytes=%d", route.AppID, route.PublicPort, target, time.Since(started).Milliseconds(), clientToTarget.Bytes, targetToClient.Bytes)
}

func (s *Service) acquireConnection(cfg Config, route model.EdgeSSHRoute, remote net.Addr) (func(), bool) {
	ip := remoteIP(remote)
	appID := strings.TrimSpace(route.AppID)
	tenantID := strings.TrimSpace(route.TenantID)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeByIP == nil {
		s.activeByIP = map[string]int{}
	}
	if s.activeByApp == nil {
		s.activeByApp = map[string]int{}
	}
	if s.activeByTenant == nil {
		s.activeByTenant = map[string]int{}
	}
	if s.rateByIP == nil {
		s.rateByIP = map[string]connectionRateWindow{}
	}
	if cfg.MaxConnectionAttemptsPerIPPerMinute > 0 && ip != "" &&
		!s.allowIPConnectionAttemptLocked(ip, time.Now().UTC(), cfg.MaxConnectionAttemptsPerIPPerMinute) {
		s.limitRejects++
		return nil, false
	}
	if cfg.MaxConnectionsPerIP > 0 && ip != "" && s.activeByIP[ip] >= cfg.MaxConnectionsPerIP {
		s.limitRejects++
		return nil, false
	}
	if cfg.MaxConnectionsPerApp > 0 && appID != "" && s.activeByApp[appID] >= cfg.MaxConnectionsPerApp {
		s.limitRejects++
		return nil, false
	}
	if cfg.MaxConnectionsPerTenant > 0 && tenantID != "" && s.activeByTenant[tenantID] >= cfg.MaxConnectionsPerTenant {
		s.limitRejects++
		return nil, false
	}
	s.connCount++
	s.activeConns++
	if ip != "" {
		s.activeByIP[ip]++
	}
	if appID != "" {
		s.activeByApp[appID]++
	}
	if tenantID != "" {
		s.activeByTenant[tenantID]++
	}
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.activeConns--
		if ip != "" {
			s.activeByIP[ip]--
			if s.activeByIP[ip] <= 0 {
				delete(s.activeByIP, ip)
			}
		}
		if appID != "" {
			s.activeByApp[appID]--
			if s.activeByApp[appID] <= 0 {
				delete(s.activeByApp, appID)
			}
		}
		if tenantID != "" {
			s.activeByTenant[tenantID]--
			if s.activeByTenant[tenantID] <= 0 {
				delete(s.activeByTenant, tenantID)
			}
		}
	}, true
}

func (s *Service) allowIPConnectionAttemptLocked(ip string, now time.Time, limit int) bool {
	window := s.rateByIP[ip]
	if window.start.IsZero() || now.Sub(window.start) >= time.Minute {
		s.rateByIP[ip] = connectionRateWindow{start: now, attempts: 1}
		return true
	}
	if window.attempts >= limit {
		return false
	}
	window.attempts++
	s.rateByIP[ip] = window
	return true
}

func remoteIP(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return strings.TrimSpace(addr.String())
	}
	return host
}

func (s *Service) startHealthServer(cfg Config, wg *sync.WaitGroup) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		s.writeStatus(w, http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		ready := !s.lastOKAt.IsZero()
		s.mu.Unlock()
		if !ready {
			s.writeStatus(w, http.StatusServiceUnavailable)
			return
		}
		s.writeStatus(w, http.StatusOK)
	})
	mux.HandleFunc("/metrics", s.handleMetrics)
	server := &http.Server{Addr: cfg.HealthAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.Logger.Printf("ssh-front health server failed: %v", err)
		}
	}()
	return server
}

func (s *Service) writeStatus(w http.ResponseWriter, code int) {
	s.mu.Lock()
	status := map[string]any{
		"status":         "ok",
		"listeners":      len(s.listeners),
		"active_conns":   s.activeConns,
		"last_sync_at":   s.lastSyncAt,
		"last_ok_at":     s.lastOKAt,
		"bundle_version": s.bundle.Version,
		"last_error":     s.lastError,
	}
	if code >= 400 {
		status["status"] = "unavailable"
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(status)
}

func (s *Service) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	listeners := len(s.listeners)
	activeConns := s.activeConns
	connCount := s.connCount
	dialErrors := s.dialErrors
	limitRejects := s.limitRejects
	bytesIn := s.bytesIn
	bytesOut := s.bytesOut
	syncCount := s.syncCount
	errorCount := s.errorCount
	lastOK := s.lastOKAt
	s.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# HELP fugue_ssh_front_listeners Active SSH route listeners.")
	fmt.Fprintln(w, "# TYPE fugue_ssh_front_listeners gauge")
	fmt.Fprintf(w, "fugue_ssh_front_listeners %d\n", listeners)
	fmt.Fprintln(w, "# HELP fugue_ssh_front_active_connections Active SSH TCP proxy connections.")
	fmt.Fprintln(w, "# TYPE fugue_ssh_front_active_connections gauge")
	fmt.Fprintf(w, "fugue_ssh_front_active_connections %d\n", activeConns)
	fmt.Fprintln(w, "# HELP fugue_ssh_front_connections_total SSH TCP proxy connections.")
	fmt.Fprintln(w, "# TYPE fugue_ssh_front_connections_total counter")
	fmt.Fprintf(w, "fugue_ssh_front_connections_total %d\n", connCount)
	fmt.Fprintln(w, "# HELP fugue_ssh_front_dial_errors_total SSH upstream dial errors.")
	fmt.Fprintln(w, "# TYPE fugue_ssh_front_dial_errors_total counter")
	fmt.Fprintf(w, "fugue_ssh_front_dial_errors_total %d\n", dialErrors)
	fmt.Fprintln(w, "# HELP fugue_ssh_front_limit_rejections_total SSH connections rejected by edge limits.")
	fmt.Fprintln(w, "# TYPE fugue_ssh_front_limit_rejections_total counter")
	fmt.Fprintf(w, "fugue_ssh_front_limit_rejections_total %d\n", limitRejects)
	fmt.Fprintln(w, "# HELP fugue_ssh_front_bytes_total SSH TCP proxy bytes.")
	fmt.Fprintln(w, "# TYPE fugue_ssh_front_bytes_total counter")
	fmt.Fprintf(w, "fugue_ssh_front_bytes_total{direction=\"client_to_target\"} %d\n", bytesIn)
	fmt.Fprintf(w, "fugue_ssh_front_bytes_total{direction=\"target_to_client\"} %d\n", bytesOut)
	fmt.Fprintln(w, "# HELP fugue_ssh_front_sync_total SSH route sync attempts.")
	fmt.Fprintln(w, "# TYPE fugue_ssh_front_sync_total counter")
	fmt.Fprintf(w, "fugue_ssh_front_sync_total %d\n", syncCount)
	fmt.Fprintln(w, "# HELP fugue_ssh_front_sync_errors_total SSH route sync or reconcile errors.")
	fmt.Fprintln(w, "# TYPE fugue_ssh_front_sync_errors_total counter")
	fmt.Fprintf(w, "fugue_ssh_front_sync_errors_total %d\n", errorCount)
	if !lastOK.IsZero() {
		fmt.Fprintln(w, "# HELP fugue_ssh_front_last_success_timestamp_seconds Last successful SSH route sync time.")
		fmt.Fprintln(w, "# TYPE fugue_ssh_front_last_success_timestamp_seconds gauge")
		fmt.Fprintf(w, "fugue_ssh_front_last_success_timestamp_seconds %d\n", lastOK.Unix())
	}
}

func (s *Service) closeAllListeners() {
	s.mu.Lock()
	listeners := make([]*routeListener, 0, len(s.listeners))
	for _, listener := range s.listeners {
		listeners = append(listeners, listener)
	}
	s.listeners = map[int]*routeListener{}
	s.mu.Unlock()
	for _, listener := range listeners {
		_ = listener.listener.Close()
		<-listener.done
	}
}

func redactToken(message, token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return message
	}
	return strings.ReplaceAll(message, token, "<redacted>")
}
