package edgefront

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fugue/internal/proxyproto"
	"fugue/internal/tcpdiag"
)

const (
	ProtocolHTTP  = "http"
	ProtocolHTTPS = "https"

	HTTPModeRedirect = "redirect"
	HTTPModeTCP      = "tcp"
	HTTPModeDisabled = "disabled"
)

type SlotTargets struct {
	HTTPAddress  string
	HTTPSAddress string
}

type Config struct {
	HTTPListenAddr     string
	HTTPSListenAddr    string
	HealthAddr         string
	EdgeID             string
	EdgeGroupID        string
	NodeHost           string
	HTTPMode           string
	ActiveSlotFile     string
	DefaultSlot        string
	DialTimeout        time.Duration
	ShutdownTimeout    time.Duration
	ProxyProtocol      bool
	Slots              map[string]SlotTargets
	ProcNetSNMPPath    string
	ProcNetNetstatPath string
}

type Service struct {
	Config   Config
	Logger   *log.Logger
	mu       sync.Mutex
	active   map[string]edgeFrontActiveTCPConnection
	metrics  edgeFrontMetrics
	sequence uint64
}

type tcpCopyResult struct {
	name     string
	bytes    int64
	duration time.Duration
	err      error
}

type edgeFrontActiveTCPConnection struct {
	ID               string
	Protocol         string
	Slot             string
	Target           string
	DownstreamRemote string
	DownstreamLocal  string
	UpstreamLocal    string
	StartedAt        time.Time
	ProxyProtocol    bool
	Downstream       net.Conn
}

type edgeFrontMetrics struct {
	ConnectionsTotal      map[edgeFrontMetricKey]uint64
	ClientToWorkerBytes   map[edgeFrontMetricKey]uint64
	WorkerToClientBytes   map[edgeFrontMetricKey]uint64
	DurationCount         map[edgeFrontMetricKey]uint64
	DurationSum           map[edgeFrontMetricKey]float64
	ClientTCPRTTCount     map[edgeFrontMetricKey]uint64
	ClientTCPRTTSum       map[edgeFrontMetricKey]float64
	ClientTCPRetransCount map[edgeFrontMetricKey]uint64
	ClientTCPRetransSum   map[edgeFrontMetricKey]float64
}

type edgeFrontMetricKey struct {
	Protocol       string
	Slot           string
	FirstCompleted string
	ProxyProtocol  bool
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
	shutdowns := make([]func(context.Context) error, 0, 3)

	if strings.TrimSpace(cfg.HealthAddr) != "" {
		shutdown, err := s.startHealthServer(cfg, &wg)
		if err != nil {
			return err
		}
		shutdowns = append(shutdowns, shutdown)
	}
	switch cfg.HTTPMode {
	case HTTPModeRedirect:
		if strings.TrimSpace(cfg.HTTPListenAddr) != "" {
			shutdown, err := s.startHTTPRedirectServer(cfg, &wg)
			if err != nil {
				return err
			}
			shutdowns = append(shutdowns, shutdown)
		}
	case HTTPModeTCP:
		if strings.TrimSpace(cfg.HTTPListenAddr) != "" {
			shutdown, err := s.startTCPProxy(cfg, ProtocolHTTP, cfg.HTTPListenAddr, &wg)
			if err != nil {
				return err
			}
			shutdowns = append(shutdowns, shutdown)
		}
	case HTTPModeDisabled:
	default:
		return fmt.Errorf("FUGUE_EDGE_FRONT_HTTP_MODE must be redirect, tcp, or disabled")
	}
	if strings.TrimSpace(cfg.HTTPSListenAddr) != "" {
		shutdown, err := s.startTCPProxy(cfg, ProtocolHTTPS, cfg.HTTPSListenAddr, &wg)
		if err != nil {
			return err
		}
		shutdowns = append(shutdowns, shutdown)
	}

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	var shutdownErr error
	for _, shutdown := range shutdowns {
		if err := shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			shutdownErr = errors.Join(shutdownErr, err)
		}
	}
	wg.Wait()
	if shutdownErr != nil {
		return shutdownErr
	}
	return ctx.Err()
}

func (s *Service) withDefaults() Config {
	cfg := s.Config
	if strings.TrimSpace(cfg.HTTPMode) == "" {
		cfg.HTTPMode = HTTPModeRedirect
	}
	cfg.HTTPMode = strings.ToLower(strings.TrimSpace(cfg.HTTPMode))
	if strings.TrimSpace(cfg.DefaultSlot) == "" {
		cfg.DefaultSlot = "a"
	}
	cfg.DefaultSlot = normalizeSlot(cfg.DefaultSlot)
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if strings.TrimSpace(cfg.ProcNetSNMPPath) == "" {
		cfg.ProcNetSNMPPath = "/proc/net/snmp"
	}
	if strings.TrimSpace(cfg.ProcNetNetstatPath) == "" {
		cfg.ProcNetNetstatPath = "/proc/net/netstat"
	}
	if cfg.Slots == nil {
		cfg.Slots = map[string]SlotTargets{}
	}
	return cfg
}

func validateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.HTTPSListenAddr) == "" && strings.TrimSpace(cfg.HTTPListenAddr) == "" {
		return fmt.Errorf("at least one edge front listen address is required")
	}
	if cfg.DefaultSlot == "" {
		return fmt.Errorf("default edge slot is required")
	}
	for _, slot := range []string{"a", "b"} {
		targets, ok := cfg.Slots[slot]
		if !ok {
			return fmt.Errorf("slot %s targets are required", slot)
		}
		if cfg.HTTPMode == HTTPModeTCP && strings.TrimSpace(targets.HTTPAddress) == "" {
			return fmt.Errorf("slot %s HTTP target is required when FUGUE_EDGE_FRONT_HTTP_MODE=tcp", slot)
		}
		if strings.TrimSpace(cfg.HTTPSListenAddr) != "" && strings.TrimSpace(targets.HTTPSAddress) == "" {
			return fmt.Errorf("slot %s HTTPS target is required", slot)
		}
	}
	if _, ok := cfg.Slots[cfg.DefaultSlot]; !ok {
		return fmt.Errorf("default edge slot %q has no targets", cfg.DefaultSlot)
	}
	return nil
}

func (s *Service) startHealthServer(cfg Config, wg *sync.WaitGroup) (func(context.Context) error, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		slot := s.activeSlot(cfg)
		targets := cfg.Slots[slot]
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","active_slot":%q,"https_target":%q}`+"\n", slot, targets.HTTPSAddress)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		slot := s.activeSlot(cfg)
		target := strings.TrimSpace(cfg.Slots[slot].HTTPSAddress)
		if strings.TrimSpace(cfg.HTTPSListenAddr) == "" {
			target = strings.TrimSpace(cfg.Slots[slot].HTTPAddress)
		}
		if target == "" {
			http.Error(w, "active slot target is missing", http.StatusServiceUnavailable)
			return
		}
		conn, err := net.DialTimeout("tcp", target, cfg.DialTimeout)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		_ = conn.Close()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","active_slot":%q}`+"\n", slot)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s.handleMetrics(w, r, cfg)
	})
	mux.HandleFunc("/edge/tcp-connections", func(w http.ResponseWriter, r *http.Request) {
		s.handleTCPConnections(w, r)
	})
	mux.HandleFunc("/edge/tcp-capture-hints", func(w http.ResponseWriter, r *http.Request) {
		s.handleTCPCaptureHints(w, r)
	})

	server := &http.Server{
		Addr:              cfg.HealthAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.Logger.Printf("edge front health server failed: %v", err)
		}
	}()
	return server.Shutdown, nil
}

func (s *Service) startHTTPRedirectServer(cfg Config, wg *sync.WaitGroup) (func(context.Context) error, error) {
	server := &http.Server{
		Addr: cfg.HTTPListenAddr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			host := r.Host
			if host == "" {
				host = r.URL.Host
			}
			target := "https://" + host + r.URL.RequestURI()
			http.Redirect(w, r, target, http.StatusPermanentRedirect)
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.Logger.Printf("edge front HTTP redirect server failed: %v", err)
		}
	}()
	return server.Shutdown, nil
}

func (s *Service) startTCPProxy(cfg Config, protocol string, listenAddr string, wg *sync.WaitGroup) (func(context.Context) error, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen edge front %s on %s: %w", protocol, listenAddr, err)
	}
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				s.Logger.Printf("edge front %s accept failed: %v", protocol, err)
				continue
			}
			go s.handleTCPConnection(cfg, protocol, conn)
		}
	}()
	return func(context.Context) error {
		err := listener.Close()
		<-done
		return err
	}, nil
}

func (s *Service) handleTCPConnection(cfg Config, protocol string, downstream net.Conn) {
	startedAt := time.Now()
	defer downstream.Close()

	slot := s.activeSlot(cfg)
	target := cfg.Slots[slot].HTTPSAddress
	if protocol == ProtocolHTTP {
		target = cfg.Slots[slot].HTTPAddress
	}
	target = strings.TrimSpace(target)
	if target == "" {
		s.Logger.Printf("edge front %s target missing; slot=%s", protocol, slot)
		return
	}

	upstream, err := net.DialTimeout("tcp", target, cfg.DialTimeout)
	if err != nil {
		s.Logger.Printf("edge front %s dial failed; slot=%s target=%s error=%v", protocol, slot, target, err)
		return
	}
	defer upstream.Close()
	if cfg.ProxyProtocol {
		if _, err := io.WriteString(upstream, proxyproto.HeaderV1(downstream.RemoteAddr(), downstream.LocalAddr())); err != nil {
			s.Logger.Printf("edge front %s proxy protocol write failed; slot=%s target=%s error=%v", protocol, slot, target, err)
			return
		}
	}
	connectionID := s.startTCPConnection(protocol, slot, target, downstream, upstream, startedAt, cfg.ProxyProtocol)
	defer s.finishTCPConnection(connectionID)
	clientToWorker, workerToClient, firstCompleted := proxyConns(downstream, upstream)
	clientTCPInfo := tcpdiag.SnapshotFromConn(downstream)
	s.recordTCPConnection(protocol, slot, firstCompleted, cfg.ProxyProtocol, time.Since(startedAt), clientToWorker, workerToClient, clientTCPInfo)
	s.logTCPConnection(protocol, slot, target, downstream, upstream, startedAt, clientToWorker, workerToClient, firstCompleted, cfg.ProxyProtocol, clientTCPInfo)
}

func proxyConns(a net.Conn, b net.Conn) (tcpCopyResult, tcpCopyResult, string) {
	var wg sync.WaitGroup
	results := make(chan tcpCopyResult, 2)
	copyAndClose := func(name string, dst net.Conn, src net.Conn) {
		defer wg.Done()
		startedAt := time.Now()
		n, err := io.Copy(dst, src)
		closeWrite(dst)
		results <- tcpCopyResult{
			name:     name,
			bytes:    n,
			duration: time.Since(startedAt),
			err:      err,
		}
	}
	wg.Add(2)
	go copyAndClose("worker_to_client", a, b)
	go copyAndClose("client_to_worker", b, a)
	first := <-results
	wg.Wait()
	second := <-results
	close(results)

	clientToWorker := first
	workerToClient := second
	if first.name == "worker_to_client" {
		clientToWorker = second
		workerToClient = first
	}
	return clientToWorker, workerToClient, first.name
}

func closeWrite(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
		return
	}
	_ = conn.Close()
}

func (s *Service) startTCPConnection(protocol, slot, target string, downstream, upstream net.Conn, startedAt time.Time, proxyProtocol bool) string {
	if s == nil || downstream == nil {
		return ""
	}
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	id := fmt.Sprintf("edgefront_%d_%d", startedAt.UnixNano(), atomic.AddUint64(&s.sequence, 1))
	entry := edgeFrontActiveTCPConnection{
		ID:               id,
		Protocol:         strings.TrimSpace(protocol),
		Slot:             strings.TrimSpace(slot),
		Target:           strings.TrimSpace(target),
		DownstreamRemote: connAddr(downstream.RemoteAddr()),
		DownstreamLocal:  connAddr(downstream.LocalAddr()),
		UpstreamLocal:    connAddr(upstream.LocalAddr()),
		StartedAt:        startedAt.UTC(),
		ProxyProtocol:    proxyProtocol,
		Downstream:       downstream,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		s.active = make(map[string]edgeFrontActiveTCPConnection)
	}
	s.active[id] = entry
	return id
}

func (s *Service) finishTCPConnection(id string) {
	id = strings.TrimSpace(id)
	if s == nil || id == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.active, id)
}

func (s *Service) recordTCPConnection(protocol, slot, firstCompleted string, proxyProtocol bool, duration time.Duration, clientToWorker, workerToClient tcpCopyResult, clientTCPInfo tcpdiag.Snapshot) {
	if s == nil {
		return
	}
	key := edgeFrontMetricKey{
		Protocol:       strings.TrimSpace(protocol),
		Slot:           strings.TrimSpace(slot),
		FirstCompleted: strings.TrimSpace(firstCompleted),
		ProxyProtocol:  proxyProtocol,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metrics.ConnectionsTotal == nil {
		s.metrics.ConnectionsTotal = make(map[edgeFrontMetricKey]uint64)
	}
	s.metrics.ConnectionsTotal[key]++
	if s.metrics.ClientToWorkerBytes == nil {
		s.metrics.ClientToWorkerBytes = make(map[edgeFrontMetricKey]uint64)
	}
	if clientToWorker.bytes > 0 {
		s.metrics.ClientToWorkerBytes[key] += uint64(clientToWorker.bytes)
	}
	if s.metrics.WorkerToClientBytes == nil {
		s.metrics.WorkerToClientBytes = make(map[edgeFrontMetricKey]uint64)
	}
	if workerToClient.bytes > 0 {
		s.metrics.WorkerToClientBytes[key] += uint64(workerToClient.bytes)
	}
	if duration > 0 {
		if s.metrics.DurationCount == nil {
			s.metrics.DurationCount = make(map[edgeFrontMetricKey]uint64)
		}
		if s.metrics.DurationSum == nil {
			s.metrics.DurationSum = make(map[edgeFrontMetricKey]float64)
		}
		s.metrics.DurationCount[key]++
		s.metrics.DurationSum[key] += duration.Seconds()
	}
	if clientTCPInfo.Available {
		if s.metrics.ClientTCPRTTCount == nil {
			s.metrics.ClientTCPRTTCount = make(map[edgeFrontMetricKey]uint64)
		}
		if s.metrics.ClientTCPRTTSum == nil {
			s.metrics.ClientTCPRTTSum = make(map[edgeFrontMetricKey]float64)
		}
		s.metrics.ClientTCPRTTCount[key]++
		s.metrics.ClientTCPRTTSum[key] += float64(clientTCPInfo.RTTUsec) / 1_000_000
		if s.metrics.ClientTCPRetransCount == nil {
			s.metrics.ClientTCPRetransCount = make(map[edgeFrontMetricKey]uint64)
		}
		if s.metrics.ClientTCPRetransSum == nil {
			s.metrics.ClientTCPRetransSum = make(map[edgeFrontMetricKey]float64)
		}
		s.metrics.ClientTCPRetransCount[key]++
		s.metrics.ClientTCPRetransSum[key] += float64(clientTCPInfo.TotalRetrans)
	}
}

type edgeFrontTCPConnectionsResponse struct {
	Count  int                           `json:"count"`
	Active []edgeFrontTCPConnectionDebug `json:"active"`
}

type edgeFrontTCPConnectionDebug struct {
	ID               string         `json:"id"`
	Protocol         string         `json:"protocol,omitempty"`
	Slot             string         `json:"slot,omitempty"`
	Target           string         `json:"target,omitempty"`
	DownstreamRemote string         `json:"downstream_remote,omitempty"`
	DownstreamLocal  string         `json:"downstream_local,omitempty"`
	UpstreamLocal    string         `json:"upstream_local,omitempty"`
	StartedAt        string         `json:"started_at"`
	ElapsedMS        int64          `json:"elapsed_ms"`
	ProxyProtocol    bool           `json:"proxy_protocol"`
	ClientTCPInfo    map[string]any `json:"client_tcp_info,omitempty"`
}

func (s *Service) handleTCPConnections(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	connections := s.activeTCPConnections()
	active := make([]edgeFrontTCPConnectionDebug, 0, len(connections))
	for _, conn := range connections {
		elapsed := now.Sub(conn.StartedAt)
		if elapsed < 0 {
			elapsed = 0
		}
		active = append(active, edgeFrontTCPConnectionDebug{
			ID:               conn.ID,
			Protocol:         conn.Protocol,
			Slot:             conn.Slot,
			Target:           conn.Target,
			DownstreamRemote: conn.DownstreamRemote,
			DownstreamLocal:  conn.DownstreamLocal,
			UpstreamLocal:    conn.UpstreamLocal,
			StartedAt:        conn.StartedAt.UTC().Format(time.RFC3339Nano),
			ElapsedMS:        elapsed.Milliseconds(),
			ProxyProtocol:    conn.ProxyProtocol,
			ClientTCPInfo:    tcpdiag.SnapshotFields("", tcpdiag.SnapshotFromConn(conn.Downstream)),
		})
	}
	writeJSON(w, http.StatusOK, edgeFrontTCPConnectionsResponse{Count: len(active), Active: active})
}

func (s *Service) activeTCPConnections() []edgeFrontActiveTCPConnection {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]edgeFrontActiveTCPConnection, 0, len(s.active))
	for _, conn := range s.active {
		out = append(out, conn)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].StartedAt.Before(out[j].StartedAt)
	})
	return out
}

func (s *Service) handleTCPCaptureHints(w http.ResponseWriter, r *http.Request) {
	remote := strings.TrimSpace(r.URL.Query().Get("remote"))
	if remote == "" {
		remote = strings.TrimSpace(r.URL.Query().Get("client_remote_addr"))
	}
	host := remote
	port := ""
	if parsedHost, parsedPort, err := net.SplitHostPort(remote); err == nil {
		host = parsedHost
		port = parsedPort
	}
	filter := "tcp"
	if host != "" {
		filter += " and host " + host
	}
	if port != "" {
		filter += " and port " + port
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":            "ok",
		"remote":            remote,
		"bpf_filter":        filter,
		"edge_id":           strings.TrimSpace(s.Config.EdgeID),
		"edge_group_id":     strings.TrimSpace(s.Config.EdgeGroupID),
		"node_host":         strings.TrimSpace(s.Config.NodeHost),
		"active_debug_path": "/edge/tcp-connections",
		"metrics_path":      "/metrics",
		"note":              "Packet capture is intentionally on-demand; use the BPF filter on the edge node or with an ephemeral debug container when deeper packet evidence is required.",
	})
}

func (s *Service) handleMetrics(w http.ResponseWriter, r *http.Request, cfg Config) {
	metrics, active := s.metricsSnapshot()
	procMetrics, procErr := tcpdiag.ReadProcNetTCPMetrics(cfg.ProcNetSNMPPath, cfg.ProcNetNetstatPath)
	identityLabels := edgeFrontIdentityMetricLabels(cfg)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintln(w, "# HELP fugue_edge_front_info Static edge-front identity labels.")
	fmt.Fprintln(w, "# TYPE fugue_edge_front_info gauge")
	fmt.Fprintf(w, "fugue_edge_front_info{edge_id=\"%s\",edge_group_id=\"%s\",node_host=\"%s\"} 1\n", prometheusTCPLabelValue(cfg.EdgeID), prometheusTCPLabelValue(cfg.EdgeGroupID), prometheusTCPLabelValue(cfg.NodeHost))
	fmt.Fprintln(w, "# HELP fugue_edge_front_tcp_active_connections Active public TCP connections handled by edge-front.")
	fmt.Fprintln(w, "# TYPE fugue_edge_front_tcp_active_connections gauge")
	for _, key := range sortedEdgeFrontActiveKeys(active) {
		fmt.Fprintf(w, "fugue_edge_front_tcp_active_connections{%s,protocol=\"%s\",slot=\"%s\",proxy_protocol=\"%s\"} %d\n", identityLabels, prometheusTCPLabelValue(key.Protocol), prometheusTCPLabelValue(key.Slot), prometheusTCPLabelValue(fmt.Sprintf("%t", key.ProxyProtocol)), active[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_front_tcp_connections_total Public TCP connections completed by edge-front.")
	fmt.Fprintln(w, "# TYPE fugue_edge_front_tcp_connections_total counter")
	for _, key := range sortedEdgeFrontMetricKeys(metrics.ConnectionsTotal) {
		fmt.Fprintf(w, "fugue_edge_front_tcp_connections_total{%s,%s} %d\n", identityLabels, edgeFrontMetricLabels(key), metrics.ConnectionsTotal[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_front_tcp_bytes_total Bytes proxied by edge-front public TCP connections.")
	fmt.Fprintln(w, "# TYPE fugue_edge_front_tcp_bytes_total counter")
	for _, key := range sortedEdgeFrontMetricKeys(metrics.ClientToWorkerBytes) {
		fmt.Fprintf(w, "fugue_edge_front_tcp_bytes_total{%s,%s,direction=\"client_to_worker\"} %d\n", identityLabels, edgeFrontMetricLabels(key), metrics.ClientToWorkerBytes[key])
	}
	for _, key := range sortedEdgeFrontMetricKeys(metrics.WorkerToClientBytes) {
		fmt.Fprintf(w, "fugue_edge_front_tcp_bytes_total{%s,%s,direction=\"worker_to_client\"} %d\n", identityLabels, edgeFrontMetricLabels(key), metrics.WorkerToClientBytes[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_front_tcp_connection_duration_seconds Public TCP connection lifetime at edge-front.")
	fmt.Fprintln(w, "# TYPE fugue_edge_front_tcp_connection_duration_seconds summary")
	for _, key := range sortedEdgeFrontMetricKeys(metrics.DurationCount) {
		fmt.Fprintf(w, "fugue_edge_front_tcp_connection_duration_seconds_sum{%s,%s} %.6f\n", identityLabels, edgeFrontMetricLabels(key), metrics.DurationSum[key])
		fmt.Fprintf(w, "fugue_edge_front_tcp_connection_duration_seconds_count{%s,%s} %d\n", identityLabels, edgeFrontMetricLabels(key), metrics.DurationCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_front_client_tcp_rtt_seconds Client-side TCP RTT sampled from Linux TCP_INFO when public connections close.")
	fmt.Fprintln(w, "# TYPE fugue_edge_front_client_tcp_rtt_seconds summary")
	for _, key := range sortedEdgeFrontMetricKeys(metrics.ClientTCPRTTCount) {
		fmt.Fprintf(w, "fugue_edge_front_client_tcp_rtt_seconds_sum{%s,%s} %.6f\n", identityLabels, edgeFrontMetricLabels(key), metrics.ClientTCPRTTSum[key])
		fmt.Fprintf(w, "fugue_edge_front_client_tcp_rtt_seconds_count{%s,%s} %d\n", identityLabels, edgeFrontMetricLabels(key), metrics.ClientTCPRTTCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_front_client_tcp_total_retrans Client-side total retransmits sampled from Linux TCP_INFO when public connections close.")
	fmt.Fprintln(w, "# TYPE fugue_edge_front_client_tcp_total_retrans summary")
	for _, key := range sortedEdgeFrontMetricKeys(metrics.ClientTCPRetransCount) {
		fmt.Fprintf(w, "fugue_edge_front_client_tcp_total_retrans_sum{%s,%s} %.0f\n", identityLabels, edgeFrontMetricLabels(key), metrics.ClientTCPRetransSum[key])
		fmt.Fprintf(w, "fugue_edge_front_client_tcp_total_retrans_count{%s,%s} %d\n", identityLabels, edgeFrontMetricLabels(key), metrics.ClientTCPRetransCount[key])
	}
	fmt.Fprintln(w, "# HELP fugue_edge_node_tcp_counter TCP counters read from the edge node /proc/net files.")
	fmt.Fprintln(w, "# TYPE fugue_edge_node_tcp_counter counter")
	for _, metric := range procMetrics {
		fmt.Fprintf(w, "fugue_edge_node_tcp_counter{%s,protocol=\"%s\",name=\"%s\"} %d\n", identityLabels, prometheusTCPLabelValue(metric.Protocol), prometheusTCPLabelValue(metric.Name), metric.Value)
	}
	fmt.Fprintln(w, "# HELP fugue_edge_node_tcp_proc_read_error Whether edge-front failed to read node /proc/net TCP counters.")
	fmt.Fprintln(w, "# TYPE fugue_edge_node_tcp_proc_read_error gauge")
	errorValue := 0
	if procErr != nil {
		errorValue = 1
	}
	fmt.Fprintf(w, "fugue_edge_node_tcp_proc_read_error{%s} %d\n", identityLabels, errorValue)
}

func (s *Service) metricsSnapshot() (edgeFrontMetrics, map[edgeFrontActiveMetricKey]int) {
	active := map[edgeFrontActiveMetricKey]int{}
	if s == nil {
		return edgeFrontMetrics{}, active
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := edgeFrontMetrics{
		ConnectionsTotal:      cloneEdgeFrontCounterMap(s.metrics.ConnectionsTotal),
		ClientToWorkerBytes:   cloneEdgeFrontCounterMap(s.metrics.ClientToWorkerBytes),
		WorkerToClientBytes:   cloneEdgeFrontCounterMap(s.metrics.WorkerToClientBytes),
		DurationCount:         cloneEdgeFrontCounterMap(s.metrics.DurationCount),
		DurationSum:           cloneEdgeFrontFloatMap(s.metrics.DurationSum),
		ClientTCPRTTCount:     cloneEdgeFrontCounterMap(s.metrics.ClientTCPRTTCount),
		ClientTCPRTTSum:       cloneEdgeFrontFloatMap(s.metrics.ClientTCPRTTSum),
		ClientTCPRetransCount: cloneEdgeFrontCounterMap(s.metrics.ClientTCPRetransCount),
		ClientTCPRetransSum:   cloneEdgeFrontFloatMap(s.metrics.ClientTCPRetransSum),
	}
	for _, conn := range s.active {
		key := edgeFrontActiveMetricKey{Protocol: conn.Protocol, Slot: conn.Slot, ProxyProtocol: conn.ProxyProtocol}
		active[key]++
	}
	return out, active
}

type edgeFrontActiveMetricKey struct {
	Protocol      string
	Slot          string
	ProxyProtocol bool
}

func cloneEdgeFrontCounterMap(in map[edgeFrontMetricKey]uint64) map[edgeFrontMetricKey]uint64 {
	out := make(map[edgeFrontMetricKey]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneEdgeFrontFloatMap(in map[edgeFrontMetricKey]float64) map[edgeFrontMetricKey]float64 {
	out := make(map[edgeFrontMetricKey]float64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (s *Service) activeSlot(cfg Config) string {
	if path := strings.TrimSpace(cfg.ActiveSlotFile); path != "" {
		if data, err := os.ReadFile(path); err == nil {
			if slot := normalizeSlot(string(data)); slot != "" {
				if _, ok := cfg.Slots[slot]; ok {
					return slot
				}
			}
			s.Logger.Printf("edge front active slot file contains invalid slot; path=%s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			s.Logger.Printf("edge front active slot file read failed; path=%s error=%v", path, err)
		}
	}
	if _, ok := cfg.Slots[cfg.DefaultSlot]; ok {
		return cfg.DefaultSlot
	}
	return "a"
}

func (s *Service) logTCPConnection(protocol, slot, target string, downstream, upstream net.Conn, startedAt time.Time, clientToWorker, workerToClient tcpCopyResult, firstCompleted string, proxyProtocol bool, clientTCPInfo tcpdiag.Snapshot) {
	if s == nil || s.Logger == nil {
		return
	}
	duration := time.Duration(0)
	if !startedAt.IsZero() {
		duration = time.Since(startedAt)
	}
	s.Logger.Printf(
		"edge_front_tcp_connection protocol=%s slot=%s target=%s downstream_remote=%s downstream_local=%s upstream_local=%s duration_ms=%d client_to_worker_bytes=%d client_to_worker_ms=%d client_to_worker_error=%s worker_to_client_bytes=%d worker_to_client_ms=%d worker_to_client_error=%s first_completed=%s proxy_protocol=%t client_tcp_info_available=%t client_tcp_rtt_us=%d client_tcp_total_retrans=%d",
		protocol,
		slot,
		logSafeTCPValue(target),
		connAddr(downstream.RemoteAddr()),
		connAddr(downstream.LocalAddr()),
		connAddr(upstream.LocalAddr()),
		duration.Milliseconds(),
		clientToWorker.bytes,
		clientToWorker.duration.Milliseconds(),
		logSafeTCPError(clientToWorker.err),
		workerToClient.bytes,
		workerToClient.duration.Milliseconds(),
		logSafeTCPError(workerToClient.err),
		firstCompleted,
		proxyProtocol,
		clientTCPInfo.Available,
		clientTCPInfo.RTTUsec,
		clientTCPInfo.TotalRetrans,
	)
	fields := map[string]any{
		"event_type":             "edge_front_tcp_connection",
		"severity":               "info",
		"message":                "edge front TCP connection",
		"edge_id":                strings.TrimSpace(s.Config.EdgeID),
		"edge_group_id":          strings.TrimSpace(s.Config.EdgeGroupID),
		"node_host":              strings.TrimSpace(s.Config.NodeHost),
		"protocol":               strings.TrimSpace(protocol),
		"slot":                   strings.TrimSpace(slot),
		"target":                 logSafeTCPValue(target),
		"downstream_remote":      connAddr(downstream.RemoteAddr()),
		"downstream_local":       connAddr(downstream.LocalAddr()),
		"upstream_local":         connAddr(upstream.LocalAddr()),
		"duration_ms":            duration.Milliseconds(),
		"client_to_worker_bytes": nonNegativeTCPInt64(clientToWorker.bytes),
		"client_to_worker_ms":    durationTCPMilliseconds(clientToWorker.duration),
		"client_to_worker_error": logSafeTCPError(clientToWorker.err),
		"worker_to_client_bytes": nonNegativeTCPInt64(workerToClient.bytes),
		"worker_to_client_ms":    durationTCPMilliseconds(workerToClient.duration),
		"worker_to_client_error": logSafeTCPError(workerToClient.err),
		"first_completed":        strings.TrimSpace(firstCompleted),
		"proxy_protocol":         proxyProtocol,
	}
	for key, value := range tcpdiag.SnapshotFields("client", clientTCPInfo) {
		fields[key] = value
	}
	writeStructuredLog(s.Logger, fields)
}

func connAddr(addr net.Addr) string {
	if addr == nil {
		return "-"
	}
	return logSafeTCPValue(addr.String())
}

func edgeFrontMetricLabels(key edgeFrontMetricKey) string {
	return fmt.Sprintf(
		"protocol=\"%s\",slot=\"%s\",first_completed=\"%s\",proxy_protocol=\"%s\"",
		prometheusTCPLabelValue(key.Protocol),
		prometheusTCPLabelValue(key.Slot),
		prometheusTCPLabelValue(key.FirstCompleted),
		prometheusTCPLabelValue(fmt.Sprintf("%t", key.ProxyProtocol)),
	)
}

func edgeFrontIdentityMetricLabels(cfg Config) string {
	return fmt.Sprintf(
		"edge_id=\"%s\",edge_group_id=\"%s\",node_host=\"%s\"",
		prometheusTCPLabelValue(cfg.EdgeID),
		prometheusTCPLabelValue(cfg.EdgeGroupID),
		prometheusTCPLabelValue(cfg.NodeHost),
	)
}

func sortedEdgeFrontMetricKeys[T any](values map[edgeFrontMetricKey]T) []edgeFrontMetricKey {
	keys := make([]edgeFrontMetricKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Protocol != keys[j].Protocol {
			return keys[i].Protocol < keys[j].Protocol
		}
		if keys[i].Slot != keys[j].Slot {
			return keys[i].Slot < keys[j].Slot
		}
		if keys[i].FirstCompleted != keys[j].FirstCompleted {
			return keys[i].FirstCompleted < keys[j].FirstCompleted
		}
		return !keys[i].ProxyProtocol && keys[j].ProxyProtocol
	})
	return keys
}

func sortedEdgeFrontActiveKeys(values map[edgeFrontActiveMetricKey]int) []edgeFrontActiveMetricKey {
	keys := make([]edgeFrontActiveMetricKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Protocol != keys[j].Protocol {
			return keys[i].Protocol < keys[j].Protocol
		}
		if keys[i].Slot != keys[j].Slot {
			return keys[i].Slot < keys[j].Slot
		}
		return !keys[i].ProxyProtocol && keys[j].ProxyProtocol
	})
	return keys
}

func prometheusTCPLabelValue(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return value
}

func durationTCPMilliseconds(value time.Duration) int64 {
	if value <= 0 {
		return 0
	}
	return value.Milliseconds()
}

func nonNegativeTCPInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

var edgeFrontStructuredLogMu sync.Mutex

func writeStructuredLog(logger *log.Logger, fields map[string]any) {
	if logger == nil || len(fields) == 0 {
		return
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return
	}
	edgeFrontStructuredLogMu.Lock()
	defer edgeFrontStructuredLogMu.Unlock()
	_, _ = logger.Writer().Write(append(data, '\n'))
}

func logSafeTCPError(err error) string {
	if err == nil {
		return "-"
	}
	return logSafeTCPValue(err.Error())
}

func logSafeTCPValue(value string) string {
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

func normalizeSlot(value string) string {
	slot := strings.ToLower(strings.TrimSpace(value))
	switch slot {
	case "a", "slot-a", "worker-a":
		return "a"
	case "b", "slot-b", "worker-b":
		return "b"
	default:
		return ""
	}
}
