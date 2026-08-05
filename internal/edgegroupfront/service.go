package edgegroupfront

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
	"fugue/internal/tcpproxy"
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
	HTTPListenAddr         string
	HTTPSListenAddr        string
	HealthAddr             string
	EdgeID                 string
	EdgeGroupID            string
	NodeHost               string
	HTTPMode               string
	ActiveSlotFile         string
	DefaultSlot            string
	RequireActivationState bool
	DialTimeout            time.Duration
	ShutdownTimeout        time.Duration
	ProxyProtocol          bool
	Slots                  map[string]SlotTargets
	ProcNetSNMPPath        string
	ProcNetNetstatPath     string
}

type Service struct {
	Config         Config
	Logger         *log.Logger
	activationMu   sync.Mutex
	lastActivation *ActivationState
	mu             sync.Mutex
	active         map[string]edgeFrontActiveTCPConnection
	metrics        edgeFrontMetrics
	sequence       uint64
}

type tcpCopyResult = tcpproxy.CopyResult

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
	ConnectionsTotal           map[edgeFrontMetricKey]uint64
	ClientToWorkerBytes        map[edgeFrontMetricKey]uint64
	WorkerToClientBytes        map[edgeFrontMetricKey]uint64
	DurationCount              map[edgeFrontMetricKey]uint64
	DurationSum                map[edgeFrontMetricKey]float64
	ClientTCPRTTCount          map[edgeFrontMetricKey]uint64
	ClientTCPRTTSum            map[edgeFrontMetricKey]float64
	ClientTCPRetransCount      map[edgeFrontMetricKey]uint64
	ClientTCPRetransSum        map[edgeFrontMetricKey]float64
	ClientTCPBytesRetransCount map[edgeFrontMetricKey]uint64
	ClientTCPBytesRetransSum   map[edgeFrontMetricKey]float64
	ClientTCPRTOCount          map[edgeFrontMetricKey]uint64
	ClientTCPRTOSum            map[edgeFrontMetricKey]float64
	ClientTCPDeliveryCount     map[edgeFrontMetricKey]uint64
	ClientTCPDeliverySum       map[edgeFrontMetricKey]float64
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
	if cfg.RequireActivationState {
		if err := s.waitForRequiredActivation(ctx, cfg); err != nil {
			return err
		}
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

func (s *Service) waitForRequiredActivation(ctx context.Context, cfg Config) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		slot, activation, err := readActiveSlot(cfg.ActiveSlotFile, cfg.EdgeGroupID)
		if err == nil && activation != nil {
			if _, exists := cfg.Slots[slot]; !exists {
				return errors.New("required group activation state selects an unknown slot")
			}
			s.activationMu.Lock()
			copy := *activation
			s.lastActivation = &copy
			s.activationMu.Unlock()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
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
	if cfg.RequireActivationState && (strings.TrimSpace(cfg.EdgeGroupID) == "" || strings.TrimSpace(cfg.ActiveSlotFile) == "") {
		return fmt.Errorf("group id and activation state file are required when activation CAS is enforced")
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
		s.writeActivationHealth(w, slot, targets.HTTPSAddress)
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
		s.writeActivationHealth(w, slot, "")
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

func (s *Service) writeActivationHealth(w http.ResponseWriter, slot, httpsTarget string) {
	payload := map[string]any{"status": "ok", "active_slot": slot}
	if httpsTarget != "" {
		payload["https_target"] = httpsTarget
	}
	s.activationMu.Lock()
	if s.lastActivation != nil && s.lastActivation.ActiveSlot == slot {
		payload["activation_generation"] = s.lastActivation.Generation
		payload["bundle_generation"] = s.lastActivation.BundleGeneration
		payload["worker_source_commit"] = s.lastActivation.WorkerSourceCommit
		payload["worker_image_digest"] = s.lastActivation.WorkerImageDigest
		payload["route_authority"] = s.lastActivation.Authority
	}
	s.activationMu.Unlock()
	writeJSON(w, http.StatusOK, payload)
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
	clientToWorker, workerToClient, firstCompleted := tcpproxy.CopyBidirectional(downstream, upstream, "client_to_worker", "worker_to_client")
	clientTCPInfo := tcpdiag.SnapshotFromConn(downstream)
	s.recordTCPConnection(protocol, slot, firstCompleted, cfg.ProxyProtocol, time.Since(startedAt), clientToWorker, workerToClient, clientTCPInfo)
	s.logTCPConnection(protocol, slot, target, downstream, upstream, startedAt, clientToWorker, workerToClient, firstCompleted, cfg.ProxyProtocol, clientTCPInfo)
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
	if clientToWorker.Bytes > 0 {
		s.metrics.ClientToWorkerBytes[key] += uint64(clientToWorker.Bytes)
	}
	if s.metrics.WorkerToClientBytes == nil {
		s.metrics.WorkerToClientBytes = make(map[edgeFrontMetricKey]uint64)
	}
	if workerToClient.Bytes > 0 {
		s.metrics.WorkerToClientBytes[key] += uint64(workerToClient.Bytes)
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
		if s.metrics.ClientTCPBytesRetransCount == nil {
			s.metrics.ClientTCPBytesRetransCount = make(map[edgeFrontMetricKey]uint64)
		}
		if s.metrics.ClientTCPBytesRetransSum == nil {
			s.metrics.ClientTCPBytesRetransSum = make(map[edgeFrontMetricKey]float64)
		}
		s.metrics.ClientTCPBytesRetransCount[key]++
		s.metrics.ClientTCPBytesRetransSum[key] += float64(clientTCPInfo.BytesRetrans)
		if s.metrics.ClientTCPRTOCount == nil {
			s.metrics.ClientTCPRTOCount = make(map[edgeFrontMetricKey]uint64)
		}
		if s.metrics.ClientTCPRTOSum == nil {
			s.metrics.ClientTCPRTOSum = make(map[edgeFrontMetricKey]float64)
		}
		s.metrics.ClientTCPRTOCount[key]++
		s.metrics.ClientTCPRTOSum[key] += float64(clientTCPInfo.TotalRTO)
		if clientTCPInfo.DeliveryRateBPS > 0 {
			if s.metrics.ClientTCPDeliveryCount == nil {
				s.metrics.ClientTCPDeliveryCount = make(map[edgeFrontMetricKey]uint64)
			}
			if s.metrics.ClientTCPDeliverySum == nil {
				s.metrics.ClientTCPDeliverySum = make(map[edgeFrontMetricKey]float64)
			}
			s.metrics.ClientTCPDeliveryCount[key]++
			s.metrics.ClientTCPDeliverySum[key] += float64(clientTCPInfo.DeliveryRateBPS)
		}
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
	labels := fmt.Sprintf(`component="front",group="%s"`, prometheusTCPLabelValue(cfg.EdgeGroupID))
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintln(w, "# HELP fugue_edge_front_info Static low-cardinality edge-front identity labels.")
	fmt.Fprintln(w, "# TYPE fugue_edge_front_info gauge")
	fmt.Fprintf(w, "fugue_edge_front_info{%s} 1\n", labels)
	fmt.Fprintln(w, "# HELP fugue_edge_front_tcp_active_connections Active public TCP connections handled by edge-front.")
	fmt.Fprintln(w, "# TYPE fugue_edge_front_tcp_active_connections gauge")
	for _, slot := range sortedEdgeFrontActiveSlots(active) {
		fmt.Fprintf(w, "fugue_edge_front_tcp_active_connections{%s,slot=\"%s\"} %d\n", labels, prometheusTCPLabelValue(slot), sumEdgeFrontActiveSlot(active, slot))
	}
	fmt.Fprintln(w, "# HELP fugue_edge_front_tcp_connections_total Public TCP connections completed by edge-front.")
	fmt.Fprintln(w, "# TYPE fugue_edge_front_tcp_connections_total counter")
	fmt.Fprintf(w, "fugue_edge_front_tcp_connections_total{%s} %d\n", labels, sumEdgeFrontCounter(metrics.ConnectionsTotal))
	fmt.Fprintln(w, "# HELP fugue_edge_front_tcp_bytes_total Bytes proxied by edge-front public TCP connections.")
	fmt.Fprintln(w, "# TYPE fugue_edge_front_tcp_bytes_total counter")
	fmt.Fprintf(w, "fugue_edge_front_tcp_bytes_total{%s} %d\n", labels, sumEdgeFrontCounter(metrics.ClientToWorkerBytes)+sumEdgeFrontCounter(metrics.WorkerToClientBytes))
	writeEdgeFrontAggregateSummary(w, labels, "fugue_edge_front_tcp_connection_duration_seconds", "Public TCP connection lifetime at edge-front.", metrics.DurationSum, metrics.DurationCount)
	writeEdgeFrontAggregateSummary(w, labels, "fugue_edge_front_client_tcp_rtt_seconds", "Client-side TCP RTT sampled from Linux TCP_INFO.", metrics.ClientTCPRTTSum, metrics.ClientTCPRTTCount)
	writeEdgeFrontAggregateSummary(w, labels, "fugue_edge_front_client_tcp_total_retrans", "Client-side total retransmits sampled from Linux TCP_INFO.", metrics.ClientTCPRetransSum, metrics.ClientTCPRetransCount)
	writeEdgeFrontAggregateSummary(w, labels, "fugue_edge_front_client_tcp_bytes_retrans", "Client-side retransmitted bytes sampled from Linux TCP_INFO.", metrics.ClientTCPBytesRetransSum, metrics.ClientTCPBytesRetransCount)
	writeEdgeFrontAggregateSummary(w, labels, "fugue_edge_front_client_tcp_total_rto", "Client-side RTO count sampled from Linux TCP_INFO.", metrics.ClientTCPRTOSum, metrics.ClientTCPRTOCount)
	writeEdgeFrontAggregateSummary(w, labels, "fugue_edge_front_client_tcp_delivery_rate_bps", "Client-side delivery rate sampled from Linux TCP_INFO.", metrics.ClientTCPDeliverySum, metrics.ClientTCPDeliveryCount)
	var retransSegments uint64
	for _, metric := range procMetrics {
		if metric.Protocol == "Tcp" && metric.Name == "RetransSegs" {
			retransSegments += metric.Value
		}
	}
	fmt.Fprintln(w, "# HELP fugue_edge_node_tcp_retrans_segments_total TCP retransmitted segments read from the edge node procfs counters.")
	fmt.Fprintln(w, "# TYPE fugue_edge_node_tcp_retrans_segments_total counter")
	fmt.Fprintf(w, "fugue_edge_node_tcp_retrans_segments_total{%s} %d\n", labels, retransSegments)
	fmt.Fprintln(w, "# HELP fugue_edge_node_tcp_proc_read_error Whether edge-front failed to read node /proc/net TCP counters.")
	fmt.Fprintln(w, "# TYPE fugue_edge_node_tcp_proc_read_error gauge")
	errorValue := 0
	if procErr != nil {
		errorValue = 1
	}
	fmt.Fprintf(w, "fugue_edge_node_tcp_proc_read_error{%s} %d\n", labels, errorValue)
}

func (s *Service) metricsSnapshot() (edgeFrontMetrics, map[edgeFrontActiveMetricKey]int) {
	active := map[edgeFrontActiveMetricKey]int{}
	if s == nil {
		return edgeFrontMetrics{}, active
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := edgeFrontMetrics{
		ConnectionsTotal:           cloneEdgeFrontCounterMap(s.metrics.ConnectionsTotal),
		ClientToWorkerBytes:        cloneEdgeFrontCounterMap(s.metrics.ClientToWorkerBytes),
		WorkerToClientBytes:        cloneEdgeFrontCounterMap(s.metrics.WorkerToClientBytes),
		DurationCount:              cloneEdgeFrontCounterMap(s.metrics.DurationCount),
		DurationSum:                cloneEdgeFrontFloatMap(s.metrics.DurationSum),
		ClientTCPRTTCount:          cloneEdgeFrontCounterMap(s.metrics.ClientTCPRTTCount),
		ClientTCPRTTSum:            cloneEdgeFrontFloatMap(s.metrics.ClientTCPRTTSum),
		ClientTCPRetransCount:      cloneEdgeFrontCounterMap(s.metrics.ClientTCPRetransCount),
		ClientTCPRetransSum:        cloneEdgeFrontFloatMap(s.metrics.ClientTCPRetransSum),
		ClientTCPBytesRetransCount: cloneEdgeFrontCounterMap(s.metrics.ClientTCPBytesRetransCount),
		ClientTCPBytesRetransSum:   cloneEdgeFrontFloatMap(s.metrics.ClientTCPBytesRetransSum),
		ClientTCPRTOCount:          cloneEdgeFrontCounterMap(s.metrics.ClientTCPRTOCount),
		ClientTCPRTOSum:            cloneEdgeFrontFloatMap(s.metrics.ClientTCPRTOSum),
		ClientTCPDeliveryCount:     cloneEdgeFrontCounterMap(s.metrics.ClientTCPDeliveryCount),
		ClientTCPDeliverySum:       cloneEdgeFrontFloatMap(s.metrics.ClientTCPDeliverySum),
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

func sumEdgeFrontCounter(values map[edgeFrontMetricKey]uint64) uint64 {
	var total uint64
	for _, value := range values {
		total += value
	}
	return total
}

func sumEdgeFrontFloat(values map[edgeFrontMetricKey]float64) float64 {
	var total float64
	for _, value := range values {
		total += value
	}
	return total
}

func writeEdgeFrontAggregateSummary(w io.Writer, labels, name, help string, sums map[edgeFrontMetricKey]float64, counts map[edgeFrontMetricKey]uint64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s summary\n", name)
	fmt.Fprintf(w, "%s_sum{%s} %.6f\n", name, labels, sumEdgeFrontFloat(sums))
	fmt.Fprintf(w, "%s_count{%s} %d\n", name, labels, sumEdgeFrontCounter(counts))
}

func sortedEdgeFrontActiveSlots(values map[edgeFrontActiveMetricKey]int) []string {
	seen := map[string]struct{}{}
	for key := range values {
		seen[key.Slot] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for slot := range seen {
		out = append(out, slot)
	}
	sort.Strings(out)
	return out
}

func sumEdgeFrontActiveSlot(values map[edgeFrontActiveMetricKey]int, slot string) int {
	total := 0
	for key, value := range values {
		if key.Slot == slot {
			total += value
		}
	}
	return total
}

func (s *Service) activeSlot(cfg Config) string {
	if path := strings.TrimSpace(cfg.ActiveSlotFile); path != "" {
		slot, activation, readErr := readActiveSlot(path, cfg.EdgeGroupID)
		if readErr == nil && cfg.RequireActivationState && activation == nil {
			readErr = errors.New("edge front activation CAS state is required")
		}
		if readErr == nil {
			if activation != nil {
				s.activationMu.Lock()
				copy := *activation
				s.lastActivation = &copy
				s.activationMu.Unlock()
			}
			if _, ok := cfg.Slots[slot]; ok {
				return slot
			}
		} else {
			s.activationMu.Lock()
			last := s.lastActivation
			s.activationMu.Unlock()
			if last != nil && last.GroupID == strings.TrimSpace(cfg.EdgeGroupID) {
				if _, ok := cfg.Slots[last.ActiveSlot]; ok {
					s.Logger.Printf("edge front activation read failed; serving activation LKG; path=%s generation=%d error=%v", path, last.Generation, readErr)
					return last.ActiveSlot
				}
			}
			if !errors.Is(readErr, os.ErrNotExist) {
				s.Logger.Printf("edge front active slot file read failed; path=%s error=%v", path, readErr)
			}
		}
	}
	if _, ok := cfg.Slots[cfg.DefaultSlot]; ok {
		if cfg.RequireActivationState {
			return ""
		}
		return cfg.DefaultSlot
	}
	return "a"
}

func readActiveSlot(path, expectedGroupID string) (string, *ActivationState, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", nil, err
	}
	trimmed := strings.TrimSpace(string(raw))
	if !strings.HasPrefix(trimmed, "{") {
		slot := normalizeSlot(trimmed)
		if slot == "" {
			return "", nil, errors.New("edge front active slot file contains invalid slot")
		}
		return slot, nil, nil
	}
	state, exists, err := ReadActivationState(path)
	if err != nil || !exists {
		if err == nil {
			err = errors.New("edge front activation state is missing")
		}
		return "", nil, err
	}
	if strings.TrimSpace(expectedGroupID) == "" || state.GroupID != strings.TrimSpace(expectedGroupID) {
		return "", nil, errors.New("edge front activation state group does not match this front")
	}
	return state.ActiveSlot, &state, nil
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
		clientToWorker.Bytes,
		clientToWorker.Duration.Milliseconds(),
		logSafeTCPError(clientToWorker.Err),
		workerToClient.Bytes,
		workerToClient.Duration.Milliseconds(),
		logSafeTCPError(workerToClient.Err),
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
		"client_to_worker_bytes": nonNegativeTCPInt64(clientToWorker.Bytes),
		"client_to_worker_ms":    durationTCPMilliseconds(clientToWorker.Duration),
		"client_to_worker_error": logSafeTCPError(clientToWorker.Err),
		"worker_to_client_bytes": nonNegativeTCPInt64(workerToClient.Bytes),
		"worker_to_client_ms":    durationTCPMilliseconds(workerToClient.Duration),
		"worker_to_client_error": logSafeTCPError(workerToClient.Err),
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
