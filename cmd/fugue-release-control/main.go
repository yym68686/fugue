// Command fugue-release-control runs the independently deployable,
// observation-only component plan control loop. It is disabled by default and
// has no database or Kubernetes capability.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fugue/internal/releasecontrol"
)

const (
	defaultReleaseControlBindAddr       = "127.0.0.1:8091"
	defaultReleaseControlInterval       = 30 * time.Second
	defaultReleaseControlAttemptTimeout = 45 * time.Second
	defaultReleaseControlRequestTimeout = 10 * time.Second
	defaultReleaseControlShutdown       = 10 * time.Second
	defaultReleaseControlResponseBytes  = 2 << 20
)

type releaseControlConfig struct {
	BindAddr        string
	ShutdownTimeout time.Duration
	Service         releasecontrol.ComponentPlanServiceConfig
}

func main() {
	cfg, err := releaseControlConfigFromEnv(os.Getenv)
	if err != nil {
		log.Fatalf("release-control config: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log.Default()); err != nil {
		log.Fatalf("release-control: %v", err)
	}
}

func run(ctx context.Context, cfg releaseControlConfig, logger *log.Logger) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := validateBindAddr(cfg.BindAddr); err != nil {
		return fmt.Errorf("bind address: %w", err)
	}
	if cfg.ShutdownTimeout <= 0 {
		return errors.New("shutdown timeout must be positive")
	}
	if logger == nil {
		logger = log.Default()
	}
	service, err := releasecontrol.NewComponentPlanService(cfg.Service, logger)
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
	logger.Printf("fugue-release-control listening on %s enabled=%t", cfg.BindAddr, cfg.Service.Enabled)

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

func releaseControlConfigFromEnv(getenv func(string) string) (releaseControlConfig, error) {
	if getenv == nil {
		return releaseControlConfig{}, errors.New("environment reader is nil")
	}
	enabled, err := strictEnvBool(getenv("FUGUE_RELEASE_CONTROL_ENABLED"))
	if err != nil {
		return releaseControlConfig{}, fmt.Errorf("FUGUE_RELEASE_CONTROL_ENABLED: %w", err)
	}
	bindAddr := strings.TrimSpace(getenv("FUGUE_RELEASE_CONTROL_BIND_ADDR"))
	if bindAddr == "" {
		bindAddr = defaultReleaseControlBindAddr
	}
	if err := validateBindAddr(bindAddr); err != nil {
		return releaseControlConfig{}, fmt.Errorf("FUGUE_RELEASE_CONTROL_BIND_ADDR: %w", err)
	}
	interval, err := positiveEnvDuration(getenv("FUGUE_RELEASE_CONTROL_RECONCILE_INTERVAL"), defaultReleaseControlInterval)
	if err != nil {
		return releaseControlConfig{}, fmt.Errorf("FUGUE_RELEASE_CONTROL_RECONCILE_INTERVAL: %w", err)
	}
	attemptTimeout, err := positiveEnvDuration(getenv("FUGUE_RELEASE_CONTROL_ATTEMPT_TIMEOUT"), defaultReleaseControlAttemptTimeout)
	if err != nil {
		return releaseControlConfig{}, fmt.Errorf("FUGUE_RELEASE_CONTROL_ATTEMPT_TIMEOUT: %w", err)
	}
	requestTimeout, err := positiveEnvDuration(getenv("FUGUE_RELEASE_CONTROL_REQUEST_TIMEOUT"), defaultReleaseControlRequestTimeout)
	if err != nil {
		return releaseControlConfig{}, fmt.Errorf("FUGUE_RELEASE_CONTROL_REQUEST_TIMEOUT: %w", err)
	}
	shutdownTimeout, err := positiveEnvDuration(getenv("FUGUE_RELEASE_CONTROL_SHUTDOWN_TIMEOUT"), defaultReleaseControlShutdown)
	if err != nil {
		return releaseControlConfig{}, fmt.Errorf("FUGUE_RELEASE_CONTROL_SHUTDOWN_TIMEOUT: %w", err)
	}
	maxResponseBytes, err := positiveEnvInt64(getenv("FUGUE_RELEASE_CONTROL_MAX_RESPONSE_BYTES"), defaultReleaseControlResponseBytes)
	if err != nil {
		return releaseControlConfig{}, fmt.Errorf("FUGUE_RELEASE_CONTROL_MAX_RESPONSE_BYTES: %w", err)
	}
	cfg := releaseControlConfig{
		BindAddr:        bindAddr,
		ShutdownTimeout: shutdownTimeout,
		Service: releasecontrol.ComponentPlanServiceConfig{
			Enabled:          enabled,
			SpecPath:         strings.TrimSpace(getenv("FUGUE_RELEASE_CONTROL_SPEC_FILE")),
			TokenPath:        strings.TrimSpace(getenv("FUGUE_RELEASE_CONTROL_TOKEN_FILE")),
			APIBaseURL:       strings.TrimSpace(getenv("FUGUE_RELEASE_CONTROL_API_BASE_URL")),
			Interval:         interval,
			AttemptTimeout:   attemptTimeout,
			RequestTimeout:   requestTimeout,
			MaxResponseBytes: maxResponseBytes,
		},
	}
	if _, err := releasecontrol.NewComponentPlanService(cfg.Service, nil); err != nil {
		return releaseControlConfig{}, err
	}
	return cfg, nil
}

func strictEnvBool(value string) (bool, error) {
	switch value {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, fmt.Errorf("must be exactly true or false")
	}
}

func positiveEnvDuration(value string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, errors.New("must be a positive duration")
	}
	return parsed, nil
}

func positiveEnvInt64(value string, fallback int64) (int64, error) {
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, errors.New("must be a positive base-10 integer")
	}
	return parsed, nil
}

func validateBindAddr(value string) error {
	_, port, err := net.SplitHostPort(value)
	if err != nil {
		return errors.New("must be a host:port address")
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil || parsedPort < 1 || parsedPort > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	return nil
}
