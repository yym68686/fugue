// Command fugue-backup-observer runs one independently isolated,
// observation-only backup cell. It is disabled by default and has no backup
// execution, database, Kubernetes, object-storage, or process capability.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fugue/internal/backupobserver"
)

const (
	defaultBindAddr       = "127.0.0.1:8092"
	defaultInterval       = 30 * time.Second
	defaultAttemptTimeout = 20 * time.Second
	defaultRequestTimeout = 10 * time.Second
	defaultShutdown       = 10 * time.Second
	defaultResponseBytes  = int64(64 << 10)
	probeTimeout          = 2 * time.Second
	probeResponseBytes    = int64(32 << 10)
)

type observerConfig struct {
	BindAddr        string
	ShutdownTimeout time.Duration
	Service         backupobserver.ServiceConfig
}

func main() {
	if len(os.Args) > 1 {
		if err := runProbe(os.Args[1:], os.Getenv); err != nil {
			fmt.Fprintln(os.Stderr, "fugue-backup-observer probe:", err)
			os.Exit(1)
		}
		return
	}
	cfg, err := observerConfigFromEnv(os.Getenv)
	if err != nil {
		log.Fatalf("backup observer config: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log.Default()); err != nil {
		log.Fatalf("backup observer: %v", err)
	}
}

func run(ctx context.Context, cfg observerConfig, logger *log.Logger) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := validateLoopbackBindAddr(cfg.BindAddr); err != nil {
		return fmt.Errorf("bind address: %w", err)
	}
	if cfg.ShutdownTimeout <= 0 || cfg.ShutdownTimeout > time.Minute {
		return errors.New("shutdown timeout must be between 1ns and 1m")
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	service, err := backupobserver.NewService(cfg.Service, logger)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	server := &http.Server{
		Addr:              cfg.BindAddr,
		Handler:           service.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	serviceDone := make(chan error, 1)
	serverDone := make(chan error, 1)
	go func() { serviceDone <- service.Run(runCtx) }()
	go func() { serverDone <- server.ListenAndServe() }()
	logger.Printf("fugue-backup-observer listening on %s enabled=%t cell=%s", cfg.BindAddr, cfg.Service.Enabled, cfg.Service.ExpectedCellKey)

	var result error
	select {
	case <-ctx.Done():
	case err := <-serviceDone:
		if err != nil {
			result = fmt.Errorf("control loop: %w", err)
		}
	case err := <-serverDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			result = fmt.Errorf("health server: %w", err)
		}
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil && result == nil && !errors.Is(err, context.Canceled) {
		result = fmt.Errorf("health server shutdown: %w", err)
	}
	return result
}

func observerConfigFromEnv(getenv func(string) string) (observerConfig, error) {
	if getenv == nil {
		return observerConfig{}, errors.New("environment reader is nil")
	}
	enabled, err := strictBool(getenv("FUGUE_BACKUP_OBSERVER_ENABLED"))
	if err != nil {
		return observerConfig{}, fmt.Errorf("FUGUE_BACKUP_OBSERVER_ENABLED: %w", err)
	}
	bindAddr := strings.TrimSpace(getenv("FUGUE_BACKUP_OBSERVER_BIND_ADDR"))
	if bindAddr == "" {
		bindAddr = defaultBindAddr
	}
	if err := validateLoopbackBindAddr(bindAddr); err != nil {
		return observerConfig{}, fmt.Errorf("FUGUE_BACKUP_OBSERVER_BIND_ADDR: %w", err)
	}
	interval, err := positiveDuration(getenv("FUGUE_BACKUP_OBSERVER_RECONCILE_INTERVAL"), defaultInterval)
	if err != nil {
		return observerConfig{}, fmt.Errorf("FUGUE_BACKUP_OBSERVER_RECONCILE_INTERVAL: %w", err)
	}
	attemptTimeout, err := positiveDuration(getenv("FUGUE_BACKUP_OBSERVER_ATTEMPT_TIMEOUT"), defaultAttemptTimeout)
	if err != nil {
		return observerConfig{}, fmt.Errorf("FUGUE_BACKUP_OBSERVER_ATTEMPT_TIMEOUT: %w", err)
	}
	requestTimeout, err := positiveDuration(getenv("FUGUE_BACKUP_OBSERVER_REQUEST_TIMEOUT"), defaultRequestTimeout)
	if err != nil {
		return observerConfig{}, fmt.Errorf("FUGUE_BACKUP_OBSERVER_REQUEST_TIMEOUT: %w", err)
	}
	shutdownTimeout, err := positiveDuration(getenv("FUGUE_BACKUP_OBSERVER_SHUTDOWN_TIMEOUT"), defaultShutdown)
	if err != nil {
		return observerConfig{}, fmt.Errorf("FUGUE_BACKUP_OBSERVER_SHUTDOWN_TIMEOUT: %w", err)
	}
	if shutdownTimeout > time.Minute {
		return observerConfig{}, errors.New("FUGUE_BACKUP_OBSERVER_SHUTDOWN_TIMEOUT must not exceed 1m")
	}
	maxResponseBytes, err := positiveInt64(getenv("FUGUE_BACKUP_OBSERVER_MAX_RESPONSE_BYTES"), defaultResponseBytes)
	if err != nil {
		return observerConfig{}, fmt.Errorf("FUGUE_BACKUP_OBSERVER_MAX_RESPONSE_BYTES: %w", err)
	}
	cfg := observerConfig{
		BindAddr:        bindAddr,
		ShutdownTimeout: shutdownTimeout,
		Service: backupobserver.ServiceConfig{
			Enabled:          enabled,
			ExpectedCellKey:  strings.TrimSpace(getenv("FUGUE_BACKUP_OBSERVER_CELL_KEY")),
			SpecPath:         strings.TrimSpace(getenv("FUGUE_BACKUP_OBSERVER_SPEC_FILE")),
			TokenPath:        strings.TrimSpace(getenv("FUGUE_BACKUP_OBSERVER_TOKEN_FILE")),
			APIBaseURL:       strings.TrimSpace(getenv("FUGUE_BACKUP_OBSERVER_API_BASE_URL")),
			Interval:         interval,
			AttemptTimeout:   attemptTimeout,
			RequestTimeout:   requestTimeout,
			MaxResponseBytes: maxResponseBytes,
		},
	}
	if _, err := backupobserver.NewService(cfg.Service, nil); err != nil {
		return observerConfig{}, err
	}
	return cfg, nil
}

func runProbe(args []string, getenv func(string) string) error {
	if getenv == nil {
		return errors.New("environment reader is nil")
	}
	if len(args) != 2 || args[0] != "probe" || (args[1] != "health" && args[1] != "ready") {
		return errors.New("usage: fugue-backup-observer probe health|ready")
	}
	bindAddr := strings.TrimSpace(getenv("FUGUE_BACKUP_OBSERVER_BIND_ADDR"))
	if bindAddr == "" {
		bindAddr = defaultBindAddr
	}
	if err := validateLoopbackBindAddr(bindAddr); err != nil {
		return err
	}
	path := "/healthz"
	if args[1] == "ready" {
		path = "/readyz"
	}
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+bindAddr+path, nil)
	if err != nil {
		return errors.New("construct loopback probe")
	}
	client := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("loopback probe unavailable")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, probeResponseBytes))
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("loopback probe returned HTTP %d", response.StatusCode)
	}
	return nil
}

func strictBool(value string) (bool, error) {
	switch value {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, errors.New("must be exactly true or false")
	}
}

func positiveDuration(value string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, errors.New("must be a positive duration")
	}
	return parsed, nil
}

func positiveInt64(value string, fallback int64) (int64, error) {
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, errors.New("must be a positive base-10 integer")
	}
	return parsed, nil
}

func validateLoopbackBindAddr(value string) error {
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return errors.New("must be an IP host:port address")
	}
	address := net.ParseIP(host)
	if address == nil || !address.IsLoopback() {
		return errors.New("host must be an explicit loopback IP")
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil || parsedPort < 1 || parsedPort > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	return nil
}
