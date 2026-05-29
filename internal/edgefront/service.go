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
	Slots           map[string]SlotTargets
}

type Service struct {
	Config Config
	Logger *log.Logger
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
	proxyConns(downstream, upstream)
}

func proxyConns(a net.Conn, b net.Conn) {
	var wg sync.WaitGroup
	copyAndClose := func(dst net.Conn, src net.Conn) {
		defer wg.Done()
		_, _ = io.Copy(dst, src)
		closeWrite(dst)
	}
	wg.Add(2)
	go copyAndClose(a, b)
	go copyAndClose(b, a)
	wg.Wait()
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
