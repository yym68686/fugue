// Command fugue-edge-control runs the independently deployable Edge control
// boundary. This first version is intentionally non-authoritative.
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

	"fugue/internal/edgecontrol"
)

const (
	defaultBindAddr        = "127.0.0.1:8092"
	defaultShutdownTimeout = 10 * time.Second
)

type config struct {
	Enabled         bool
	BindAddr        string
	ShutdownTimeout time.Duration
}

func main() {
	cfg, err := configFromEnv(os.Getenv)
	if err != nil {
		log.Fatalf("edge-control config: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log.Default()); err != nil {
		log.Fatalf("edge-control: %v", err)
	}
}

func run(ctx context.Context, cfg config, logger *log.Logger) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	if logger == nil {
		logger = log.Default()
	}
	server := edgecontrol.Server(cfg.BindAddr, edgecontrol.NewBoundary(cfg.Enabled).Handler())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.ListenAndServe() }()
	logger.Printf("fugue-edge-control listening on %s mode=boundary-only authority=none enabled=%t", cfg.BindAddr, cfg.Enabled)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	case err := <-serverDone:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve: %w", err)
	}
}

func configFromEnv(getenv func(string) string) (config, error) {
	if getenv == nil {
		return config{}, errors.New("environment reader is nil")
	}
	enabled, err := strictBool(getenv("FUGUE_EDGE_CONTROL_ENABLED"))
	if err != nil {
		return config{}, fmt.Errorf("FUGUE_EDGE_CONTROL_ENABLED: %w", err)
	}
	bindAddr := strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_BIND_ADDR"))
	if bindAddr == "" {
		bindAddr = defaultBindAddr
	}
	shutdownTimeout := defaultShutdownTimeout
	if raw := strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_SHUTDOWN_TIMEOUT")); raw != "" {
		shutdownTimeout, err = time.ParseDuration(raw)
		if err != nil || shutdownTimeout <= 0 || shutdownTimeout > 2*time.Minute {
			return config{}, errors.New("FUGUE_EDGE_CONTROL_SHUTDOWN_TIMEOUT must be between 1ns and 2m")
		}
	}
	cfg := config{Enabled: enabled, BindAddr: bindAddr, ShutdownTimeout: shutdownTimeout}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg config) validate() error {
	host, port, err := net.SplitHostPort(cfg.BindAddr)
	if err != nil {
		return errors.New("bind address must be host:port")
	}
	if host == "" {
		return errors.New("bind address host is required")
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil || parsedPort < 1 || parsedPort > 65535 {
		return errors.New("bind address port must be between 1 and 65535")
	}
	if cfg.ShutdownTimeout <= 0 || cfg.ShutdownTimeout > 2*time.Minute {
		return errors.New("shutdown timeout must be positive and no greater than 2m")
	}
	return nil
}

func strictBool(value string) (bool, error) {
	switch strings.TrimSpace(value) {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, errors.New("must be exactly true or false")
	}
}
