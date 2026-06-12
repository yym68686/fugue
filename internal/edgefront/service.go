package edgefront

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"fugue/internal/proxyproto"
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
	HTTPListenAddr  string
	HTTPSListenAddr string
	HealthAddr      string
	HTTPMode        string
	ActiveSlotFile  string
	DefaultSlot     string
	DialTimeout     time.Duration
	ShutdownTimeout time.Duration
	ProxyProtocol   bool
	Slots           map[string]SlotTargets
}

type Service struct {
	Config Config
	Logger *log.Logger
}

type tcpCopyResult struct {
	name     string
	bytes    int64
	duration time.Duration
	err      error
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
	clientToWorker, workerToClient, firstCompleted := proxyConns(downstream, upstream)
	s.logTCPConnection(protocol, slot, target, downstream, upstream, startedAt, clientToWorker, workerToClient, firstCompleted, cfg.ProxyProtocol)
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

func (s *Service) logTCPConnection(protocol, slot, target string, downstream, upstream net.Conn, startedAt time.Time, clientToWorker, workerToClient tcpCopyResult, firstCompleted string, proxyProtocol bool) {
	if s == nil || s.Logger == nil {
		return
	}
	duration := time.Duration(0)
	if !startedAt.IsZero() {
		duration = time.Since(startedAt)
	}
	s.Logger.Printf(
		"edge_front_tcp_connection protocol=%s slot=%s target=%s downstream_remote=%s downstream_local=%s upstream_local=%s duration_ms=%d client_to_worker_bytes=%d client_to_worker_ms=%d client_to_worker_error=%s worker_to_client_bytes=%d worker_to_client_ms=%d worker_to_client_error=%s first_completed=%s proxy_protocol=%t",
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
	)
}

func connAddr(addr net.Addr) string {
	if addr == nil {
		return "-"
	}
	return logSafeTCPValue(addr.String())
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
