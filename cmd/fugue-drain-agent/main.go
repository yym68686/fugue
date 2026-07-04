package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	componentName = "fugue-drain-agent"

	defaultBindAddr               = ":19090"
	defaultDrainTimeout           = 600 * time.Second
	defaultQuietPeriod            = 2 * time.Second
	defaultPollInterval           = 200 * time.Millisecond
	defaultPreStopSignalWait      = 30 * time.Second
	defaultShutdownGrace          = 5 * time.Second
	defaultProcTCPPath            = "/proc/net/tcp"
	defaultProcTCP6Path           = "/proc/net/tcp6"
	tcpStateEstablished      byte = 0x01
	tcpStateSynRecv          byte = 0x03
	tcpStateFinWait1         byte = 0x04
	tcpStateFinWait2         byte = 0x05
	tcpStateCloseWait        byte = 0x08
	tcpStateClosing          byte = 0x0B
	tcpStateLastAck          byte = 0x09
)

type config struct {
	BindAddr     string
	AppPorts     map[int]struct{}
	Timeout      time.Duration
	QuietPeriod  time.Duration
	PollInterval time.Duration
	ProcTCPPath  string
	ProcTCP6Path string
	FailClosed   bool
	PodName      string
	Namespace    string
}

type tcpEntry struct {
	LocalPort int
	State     byte
}

type observeResult struct {
	Active int
	States map[string]int
}

type drainResult struct {
	OK                bool   `json:"ok"`
	Reason            string `json:"reason"`
	WaitedMS          int64  `json:"waited_ms"`
	ActiveConnections int    `json:"active_connections"`
	MaxActive         int    `json:"max_active_connections"`
	ObserverErrors    int    `json:"observer_errors"`
}

type metrics struct {
	mu              sync.Mutex
	Active          int
	PreStopRequests int64
	EarlyExitTotal  int64
	TimeoutTotal    int64
	ObserverErrors  int64
	LastWait        time.Duration
}

type server struct {
	cfg            config
	logger         *log.Logger
	metrics        *metrics
	now            func() time.Time
	sleep          func(context.Context, time.Duration) error
	drainStarted   chan struct{}
	drainCompleted chan struct{}
	startOnce      sync.Once
	completeOnce   sync.Once
}

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		log.Fatalf("%s config error: %v", componentName, err)
	}
	logger := log.New(os.Stdout, "", log.LstdFlags|log.LUTC)
	srv := newServer(cfg, logger)
	mux := http.NewServeMux()
	srv.register(mux)
	httpServer := &http.Server{
		Addr:    cfg.BindAddr,
		Handler: mux,
	}

	errCh := make(chan error, 1)
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)

	logger.Printf("%s started bind_addr=%s ports=%s timeout_seconds=%d quiet_period_seconds=%d poll_interval_ms=%d fail_closed=%t", componentName, cfg.BindAddr, formatPorts(cfg.AppPorts), int(cfg.Timeout.Seconds()), int(cfg.QuietPeriod.Seconds()), int(cfg.PollInterval/time.Millisecond), cfg.FailClosed)

	go func() {
		errCh <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("%s listen error: %v", componentName, err)
		}
	case sig := <-sigCh:
		logger.Printf("%s termination_signal signal=%s action=wait_for_prestop_or_drain", componentName, sig)
		go func() {
			second := <-sigCh
			logger.Printf("%s termination_signal signal=%s action=force_exit", componentName, second)
			os.Exit(128 + signalExitCode(second))
		}()
		waitCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout+defaultPreStopSignalWait)
		outcome := srv.waitForDrainAfterSignal(waitCtx, defaultPreStopSignalWait)
		cancel()
		logger.Printf("%s termination_signal_done signal=%s outcome=%s", componentName, sig, outcome)
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), defaultShutdownGrace)
		defer shutdownCancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("%s shutdown error: %v", componentName, err)
		}
	}
}

func signalExitCode(sig os.Signal) int {
	if value, ok := sig.(syscall.Signal); ok {
		return int(value)
	}
	return 1
}

func (s *server) waitForDrainAfterSignal(ctx context.Context, preStopWait time.Duration) string {
	timer := time.NewTimer(preStopWait)
	defer timer.Stop()

	select {
	case <-s.drainStarted:
	case <-s.drainCompleted:
		return "drain_complete"
	case <-ctx.Done():
		return "deadline_before_prestop"
	case <-timer.C:
		return "no_prestop_request"
	}

	select {
	case <-s.drainCompleted:
		return "drain_complete"
	case <-ctx.Done():
		return "deadline"
	}
}

func (s *server) markDrainStarted() {
	s.startOnce.Do(func() {
		close(s.drainStarted)
	})
}

func (s *server) markDrainCompleted() {
	s.completeOnce.Do(func() {
		s.markDrainStarted()
		close(s.drainCompleted)
	})
}

func newServer(cfg config, logger *log.Logger) *server {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &server{
		cfg:            cfg,
		logger:         logger,
		metrics:        &metrics{},
		now:            time.Now,
		drainStarted:   make(chan struct{}),
		drainCompleted: make(chan struct{}),
		sleep: func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		},
	}
}

func (s *server) register(mux *http.ServeMux) {
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/drain/prestop", s.handlePreStop)
	mux.HandleFunc("/metrics", s.handleMetrics)
}

func configFromEnv() (config, error) {
	ports, err := parsePorts(strings.TrimSpace(os.Getenv("FUGUE_DRAIN_APP_PORTS")))
	if err != nil {
		return config{}, err
	}
	return config{
		BindAddr:     getenv("FUGUE_DRAIN_AGENT_BIND_ADDR", defaultBindAddr),
		AppPorts:     ports,
		Timeout:      getenvDurationSeconds("FUGUE_DRAIN_TIMEOUT_SECONDS", defaultDrainTimeout),
		QuietPeriod:  getenvDurationSeconds("FUGUE_DRAIN_QUIET_PERIOD_SECONDS", defaultQuietPeriod),
		PollInterval: getenvDurationMillis("FUGUE_DRAIN_POLL_INTERVAL_MS", defaultPollInterval),
		ProcTCPPath:  getenv("FUGUE_DRAIN_PROC_TCP_PATH", defaultProcTCPPath),
		ProcTCP6Path: getenv("FUGUE_DRAIN_PROC_TCP6_PATH", defaultProcTCP6Path),
		FailClosed:   getenvBool("FUGUE_DRAIN_FAIL_CLOSED", true),
		PodName:      strings.TrimSpace(os.Getenv("POD_NAME")),
		Namespace:    strings.TrimSpace(os.Getenv("POD_NAMESPACE")),
	}, nil
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvDurationSeconds(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return time.Duration(parsed) * time.Second
}

func getenvDurationMillis(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return time.Duration(parsed) * time.Millisecond
}

func parsePorts(value string) (map[int]struct{}, error) {
	ports := map[int]struct{}{}
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		port, err := strconv.Atoi(part)
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("invalid FUGUE_DRAIN_APP_PORTS entry %q", part)
		}
		ports[port] = struct{}{}
	}
	if len(ports) == 0 {
		return nil, fmt.Errorf("FUGUE_DRAIN_APP_PORTS must include at least one port")
	}
	return ports, nil
}

func (s *server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "component": componentName})
}

func (s *server) handlePreStop(w http.ResponseWriter, r *http.Request) {
	s.markDrainStarted()
	result := s.drain(r.Context())
	writeJSON(w, http.StatusOK, result)
	s.markDrainCompleted()
}

func (s *server) drain(ctx context.Context) drainResult {
	start := s.now()
	deadline := start.Add(s.cfg.Timeout)
	var idleSince time.Time
	maxActive := 0
	observerErrors := 0

	s.metrics.mu.Lock()
	s.metrics.PreStopRequests++
	s.metrics.mu.Unlock()

	s.logger.Printf("fugue_drain_start pod=%s namespace=%s ports=%s timeout_seconds=%d quiet_period_seconds=%d poll_interval_ms=%d", s.cfg.PodName, s.cfg.Namespace, formatPorts(s.cfg.AppPorts), int(s.cfg.Timeout.Seconds()), int(s.cfg.QuietPeriod.Seconds()), int(s.cfg.PollInterval/time.Millisecond))

	for {
		now := s.now()
		observed, err := s.observe()
		if err != nil {
			observerErrors++
			s.metrics.mu.Lock()
			s.metrics.ObserverErrors++
			s.metrics.mu.Unlock()
			s.logger.Printf("fugue_drain_observer_error error=%q waited_ms=%d", err.Error(), now.Sub(start).Milliseconds())
			if !s.cfg.FailClosed {
				return s.complete("observer_error_open", start, 0, maxActive, observerErrors)
			}
		} else {
			if observed.Active > maxActive {
				maxActive = observed.Active
			}
			s.metrics.mu.Lock()
			s.metrics.Active = observed.Active
			s.metrics.mu.Unlock()
			s.logger.Printf("fugue_drain_sample active_connections=%d states=%s waited_ms=%d", observed.Active, formatStateCounts(observed.States), now.Sub(start).Milliseconds())
			if observed.Active == 0 {
				if idleSince.IsZero() {
					idleSince = now
				}
				if now.Sub(idleSince) >= s.cfg.QuietPeriod {
					return s.complete("idle", start, 0, maxActive, observerErrors)
				}
			} else {
				idleSince = time.Time{}
			}
		}
		if !now.Before(deadline) {
			active := 0
			if observed, err := s.observe(); err == nil {
				active = observed.Active
			}
			return s.complete("timeout", start, active, maxActive, observerErrors)
		}
		if err := s.sleep(ctx, minDuration(s.cfg.PollInterval, time.Until(deadline))); err != nil {
			return s.complete("context_canceled", start, 0, maxActive, observerErrors)
		}
	}
}

func (s *server) complete(reason string, start time.Time, active, maxActive, observerErrors int) drainResult {
	waited := s.now().Sub(start)
	if waited < 0 {
		waited = 0
	}
	s.metrics.mu.Lock()
	s.metrics.LastWait = waited
	if reason == "idle" || reason == "observer_error_open" {
		s.metrics.EarlyExitTotal++
	}
	if reason == "timeout" {
		s.metrics.TimeoutTotal++
	}
	s.metrics.Active = active
	s.metrics.mu.Unlock()
	s.logger.Printf("fugue_drain_complete reason=%s waited_ms=%d active_connections=%d max_active_connections=%d observer_errors=%d", reason, waited.Milliseconds(), active, maxActive, observerErrors)
	return drainResult{
		OK:                true,
		Reason:            reason,
		WaitedMS:          waited.Milliseconds(),
		ActiveConnections: active,
		MaxActive:         maxActive,
		ObserverErrors:    observerErrors,
	}
}

func (s *server) observe() (observeResult, error) {
	entries, err := readTCPEntries(s.cfg.ProcTCPPath, s.cfg.ProcTCP6Path)
	if err != nil {
		return observeResult{}, err
	}
	result := observeResult{States: map[string]int{}}
	for _, entry := range entries {
		if _, ok := s.cfg.AppPorts[entry.LocalPort]; !ok {
			continue
		}
		if !tcpStateActive(entry.State) {
			continue
		}
		result.Active++
		result.States[tcpStateName(entry.State)]++
	}
	return result, nil
}

func readTCPEntries(paths ...string) ([]tcpEntry, error) {
	var entries []tcpEntry
	var errs []string
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		file, err := os.Open(path)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", path, err))
			continue
		}
		parsed, parseErr := parseTCPEntries(file)
		closeErr := file.Close()
		if parseErr != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", path, parseErr))
			continue
		}
		if closeErr != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", path, closeErr))
			continue
		}
		entries = append(entries, parsed...)
	}
	if len(errs) > 0 {
		return entries, errors.New(strings.Join(errs, "; "))
	}
	return entries, nil
}

func parseTCPEntries(r io.Reader) ([]tcpEntry, error) {
	scanner := bufio.NewScanner(r)
	var entries []tcpEntry
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "sl") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			return nil, fmt.Errorf("line %d has %d fields", lineNo, len(fields))
		}
		port, err := parseLocalPort(fields[1])
		if err != nil {
			return nil, fmt.Errorf("line %d local address: %w", lineNo, err)
		}
		state, err := strconv.ParseUint(fields[3], 16, 8)
		if err != nil {
			return nil, fmt.Errorf("line %d tcp state %q: %w", lineNo, fields[3], err)
		}
		entries = append(entries, tcpEntry{LocalPort: port, State: byte(state)})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func parseLocalPort(value string) (int, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("expected address:port, got %q", value)
	}
	parsed, err := strconv.ParseUint(parts[1], 16, 16)
	if err != nil {
		return 0, err
	}
	return int(parsed), nil
}

func tcpStateActive(state byte) bool {
	switch state {
	case tcpStateEstablished,
		tcpStateSynRecv,
		tcpStateFinWait1,
		tcpStateFinWait2,
		tcpStateCloseWait,
		tcpStateLastAck,
		tcpStateClosing:
		return true
	default:
		return false
	}
}

func tcpStateName(state byte) string {
	switch state {
	case 0x01:
		return "ESTABLISHED"
	case 0x02:
		return "SYN_SENT"
	case 0x03:
		return "SYN_RECV"
	case 0x04:
		return "FIN_WAIT1"
	case 0x05:
		return "FIN_WAIT2"
	case 0x06:
		return "TIME_WAIT"
	case 0x07:
		return "CLOSE"
	case 0x08:
		return "CLOSE_WAIT"
	case 0x09:
		return "LAST_ACK"
	case 0x0A:
		return "LISTEN"
	case 0x0B:
		return "CLOSING"
	default:
		return fmt.Sprintf("UNKNOWN_%02X", state)
	}
}

func (s *server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	s.metrics.mu.Lock()
	snap := *s.metrics
	s.metrics.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# HELP fugue_app_drain_active_connections Active TCP connections observed by fugue-drain-agent.")
	fmt.Fprintln(w, "# TYPE fugue_app_drain_active_connections gauge")
	fmt.Fprintf(w, "fugue_app_drain_active_connections %d\n", snap.Active)
	fmt.Fprintln(w, "# HELP fugue_app_drain_prestop_requests_total Total preStop drain requests handled.")
	fmt.Fprintln(w, "# TYPE fugue_app_drain_prestop_requests_total counter")
	fmt.Fprintf(w, "fugue_app_drain_prestop_requests_total %d\n", snap.PreStopRequests)
	fmt.Fprintln(w, "# HELP fugue_app_drain_early_exit_total Total drain requests that completed before timeout.")
	fmt.Fprintln(w, "# TYPE fugue_app_drain_early_exit_total counter")
	fmt.Fprintf(w, "fugue_app_drain_early_exit_total %d\n", snap.EarlyExitTotal)
	fmt.Fprintln(w, "# HELP fugue_app_drain_timeout_total Total drain requests that reached the hard timeout.")
	fmt.Fprintln(w, "# TYPE fugue_app_drain_timeout_total counter")
	fmt.Fprintf(w, "fugue_app_drain_timeout_total %d\n", snap.TimeoutTotal)
	fmt.Fprintln(w, "# HELP fugue_app_drain_observer_errors_total Total connection observer errors.")
	fmt.Fprintln(w, "# TYPE fugue_app_drain_observer_errors_total counter")
	fmt.Fprintf(w, "fugue_app_drain_observer_errors_total %d\n", snap.ObserverErrors)
	fmt.Fprintln(w, "# HELP fugue_app_drain_wait_seconds Last preStop drain wait duration.")
	fmt.Fprintln(w, "# TYPE fugue_app_drain_wait_seconds gauge")
	fmt.Fprintf(w, "fugue_app_drain_wait_seconds %.3f\n", snap.LastWait.Seconds())
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	payload, err := json.Marshal(value)
	if err != nil {
		http.Error(w, `{"ok":false,"error":"json_encode_failed"}`+"\n", http.StatusInternalServerError)
		return
	}
	payload = append(payload, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.Header().Set("Connection", "close")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func formatPorts(ports map[int]struct{}) string {
	items := make([]int, 0, len(ports))
	for port := range ports {
		items = append(items, port)
	}
	sort.Ints(items)
	parts := make([]string, 0, len(items))
	for _, port := range items {
		parts = append(parts, strconv.Itoa(port))
	}
	return strings.Join(parts, ",")
}

func formatStateCounts(states map[string]int) string {
	if len(states) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(states))
	for key := range states {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", key, states[key]))
	}
	return strings.Join(parts, ",")
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
