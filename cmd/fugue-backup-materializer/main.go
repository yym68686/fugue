// Command fugue-backup-materializer runs one independently isolated,
// observation-only backup Secret materializer cell. It is disabled by default
// and has no Secret writer, datastore, signer, backup execution, or subprocess
// capability.
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

	materializeragent "fugue/internal/backupmaterializer/agent"
	materializerclient "fugue/internal/backupmaterializer/client"
	clientprojected "fugue/internal/backupmaterializer/client/projected"
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/reconciler"
	"fugue/internal/backupmaterializer/secretreader"
	secretprojected "fugue/internal/backupmaterializer/secretreader/projected"
)

const (
	defaultBindAddr               = "127.0.0.1:8093"
	defaultInterval               = 30 * time.Second
	defaultAttemptTimeout         = 20 * time.Second
	defaultInputRequestTimeout    = 5 * time.Second
	defaultInputHandshakeTimeout  = 5 * time.Second
	defaultSecretRequestTimeout   = 5 * time.Second
	defaultSecretHandshakeTimeout = 5 * time.Second
	defaultShutdownTimeout        = 10 * time.Second
	defaultInputResponseBytes     = int64(materializercontract.MaxObserverInputBundleBytes)
	defaultSecretResponseBytes    = secretreader.DefaultMaxResponse
	probeTimeout                  = 2 * time.Second
	probeResponseBytes            = int64(32 << 10)

	minimumOperationDuration = time.Second
	maximumInterval          = 10 * time.Minute
	maximumAttemptTimeout    = time.Minute
	maximumHandshakeTimeout  = 15 * time.Second
	maximumShutdownTimeout   = time.Minute
)

type materializerConfig struct {
	Enabled          bool
	BindAddr         string
	CellKey          string
	RunID            string
	Interval         time.Duration
	AttemptTimeout   time.Duration
	ShutdownTimeout  time.Duration
	InputProjection  clientprojected.Config
	SecretProjection secretprojected.Config
}

func (config materializerConfig) String() string {
	return "backup materializer process configuration [REDACTED]"
}

func (config materializerConfig) GoString() string { return config.String() }

func main() {
	if len(os.Args) > 1 {
		if err := runProbe(os.Args[1:], os.Getenv); err != nil {
			fmt.Fprintln(os.Stderr, "fugue-backup-materializer probe:", err)
			os.Exit(1)
		}
		return
	}
	config, err := materializerConfigFromEnv(os.Getenv)
	if err != nil {
		log.Fatalf("backup materializer config: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, config, log.Default()); err != nil {
		log.Fatalf("backup materializer: %v", err)
	}
}

func run(ctx context.Context, config materializerConfig, logger *log.Logger) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateLoopbackBindAddr(config.BindAddr); err != nil {
		return fmt.Errorf("bind address: %w", err)
	}
	if !boundedDuration(config.ShutdownTimeout, minimumOperationDuration, maximumShutdownTimeout) {
		return errors.New("shutdown timeout must be between 1s and 1m at millisecond precision")
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	service, err := newMaterializerService(config, logger)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", config.BindAddr)
	if err != nil {
		return errors.New("loopback listener unavailable")
	}
	server := &http.Server{
		Addr:              config.BindAddr,
		Handler:           service.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	logger.Printf("fugue-backup-materializer listening on Pod loopback enabled=%t cell=%s", config.Enabled, publicCell(config))
	return serve(ctx, config.ShutdownTimeout, service, server, listener)
}

func newMaterializerService(config materializerConfig, logger *log.Logger) (*materializeragent.Service, error) {
	if !config.Enabled {
		service, err := materializeragent.New(materializeragent.Config{Enabled: false}, logger)
		if err != nil {
			return nil, errors.New("construct disabled materializer agent")
		}
		return service, nil
	}
	if !config.InputProjection.Enabled || !config.SecretProjection.Enabled ||
		config.InputProjection.ExpectedCellKey != config.CellKey ||
		config.SecretProjection.ExpectedCellKey != config.CellKey ||
		config.InputProjection.ExpectedRunID != config.RunID {
		return nil, errors.New("materializer projection identity inconsistent")
	}
	inputClient, err := clientprojected.New(config.InputProjection)
	if err != nil {
		return nil, errors.New("desired-input projection unavailable")
	}
	secretReader, err := secretprojected.New(config.SecretProjection)
	if err != nil {
		return nil, errors.New("current-Secret projection unavailable")
	}
	cycle, err := reconciler.New(reconciler.Config{
		Enabled:       true,
		CellKey:       config.CellKey,
		DesiredSource: inputClient,
		CurrentSource: secretReader,
	})
	if err != nil {
		return nil, errors.New("construct cell-local materializer cycle")
	}
	service, err := materializeragent.New(materializeragent.Config{
		Enabled:        true,
		CellKey:        config.CellKey,
		Cycle:          cycle,
		Interval:       config.Interval,
		AttemptTimeout: config.AttemptTimeout,
	}, logger)
	if err != nil {
		return nil, errors.New("construct cell-local materializer agent")
	}
	return service, nil
}

func serve(
	ctx context.Context,
	shutdownTimeout time.Duration,
	service *materializeragent.Service,
	server *http.Server,
	listener net.Listener,
) error {
	if ctx == nil || service == nil || server == nil || server.Handler == nil || listener == nil ||
		!boundedDuration(shutdownTimeout, minimumOperationDuration, maximumShutdownTimeout) {
		return errors.New("materializer lifecycle configuration invalid")
	}
	if materializeragent.ValidateSnapshot(service.Snapshot()) != nil || validateLoopbackBindAddr(listener.Addr().String()) != nil {
		return errors.New("materializer lifecycle boundary invalid")
	}
	runCtx, cancel := context.WithCancel(ctx)
	serviceDone := make(chan error, 1)
	serverDone := make(chan error, 1)
	go func() { serviceDone <- service.Run(runCtx) }()
	go func() { serverDone <- server.Serve(listener) }()

	var failures []error
	serviceObserved := false
	serverObserved := false
	select {
	case <-ctx.Done():
	case err := <-serviceDone:
		serviceObserved = true
		if ctx.Err() == nil {
			if err == nil {
				failures = append(failures, errors.New("materializer control loop stopped unexpectedly"))
			} else {
				failures = append(failures, errors.New("materializer control loop failed"))
			}
		}
	case err := <-serverDone:
		serverObserved = true
		if ctx.Err() == nil {
			if err == nil {
				failures = append(failures, errors.New("materializer health server stopped unexpectedly"))
			} else {
				failures = append(failures, errors.New("materializer health server failed"))
			}
		}
	}

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
		_ = server.Close()
		failures = append(failures, errors.New("materializer health server shutdown failed"))
	}
	if !serviceObserved {
		select {
		case err := <-serviceDone:
			if err != nil {
				failures = append(failures, errors.New("materializer control loop shutdown failed"))
			}
		case <-shutdownCtx.Done():
			failures = append(failures, errors.New("materializer control loop shutdown timed out"))
		}
	}
	if !serverObserved {
		select {
		case err := <-serverDone:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				failures = append(failures, errors.New("materializer health server shutdown failed"))
			}
		case <-shutdownCtx.Done():
			_ = server.Close()
			failures = append(failures, errors.New("materializer health server shutdown timed out"))
		}
	}
	return errors.Join(failures...)
}

func materializerConfigFromEnv(getenv func(string) string) (materializerConfig, error) {
	if getenv == nil {
		return materializerConfig{}, errors.New("environment reader is nil")
	}
	enabled, err := strictBool(getenv("FUGUE_BACKUP_MATERIALIZER_ENABLED"))
	if err != nil {
		return materializerConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_ENABLED: %w", err)
	}
	bindAddr := strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_BIND_ADDR"))
	if bindAddr == "" {
		bindAddr = defaultBindAddr
	}
	if err := validateLoopbackBindAddr(bindAddr); err != nil {
		return materializerConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_BIND_ADDR: %w", err)
	}
	shutdownTimeout, err := envDuration(
		getenv("FUGUE_BACKUP_MATERIALIZER_SHUTDOWN_TIMEOUT"),
		defaultShutdownTimeout,
		minimumOperationDuration,
		maximumShutdownTimeout,
	)
	if err != nil {
		return materializerConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_SHUTDOWN_TIMEOUT: %w", err)
	}
	config := materializerConfig{
		Enabled:         enabled,
		BindAddr:        bindAddr,
		ShutdownTimeout: shutdownTimeout,
	}
	if !enabled {
		return config, nil
	}

	interval, err := envDuration(
		getenv("FUGUE_BACKUP_MATERIALIZER_RECONCILE_INTERVAL"),
		defaultInterval,
		minimumOperationDuration,
		maximumInterval,
	)
	if err != nil {
		return materializerConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_RECONCILE_INTERVAL: %w", err)
	}
	attemptTimeout, err := envDuration(
		getenv("FUGUE_BACKUP_MATERIALIZER_ATTEMPT_TIMEOUT"),
		defaultAttemptTimeout,
		minimumOperationDuration,
		maximumAttemptTimeout,
	)
	if err != nil {
		return materializerConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_ATTEMPT_TIMEOUT: %w", err)
	}
	inputRequestTimeout, err := envDuration(
		getenv("FUGUE_BACKUP_MATERIALIZER_INPUT_REQUEST_TIMEOUT"),
		defaultInputRequestTimeout,
		minimumOperationDuration,
		materializerclient.MaximumRequestTimeout,
	)
	if err != nil {
		return materializerConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_INPUT_REQUEST_TIMEOUT: %w", err)
	}
	inputHandshakeTimeout, err := envDuration(
		getenv("FUGUE_BACKUP_MATERIALIZER_INPUT_HANDSHAKE_TIMEOUT"),
		defaultInputHandshakeTimeout,
		minimumOperationDuration,
		maximumHandshakeTimeout,
	)
	if err != nil || inputHandshakeTimeout > inputRequestTimeout {
		return materializerConfig{}, errors.New("FUGUE_BACKUP_MATERIALIZER_INPUT_HANDSHAKE_TIMEOUT: must be bounded by the input request timeout")
	}
	secretRequestTimeout, err := envDuration(
		getenv("FUGUE_BACKUP_MATERIALIZER_SECRET_REQUEST_TIMEOUT"),
		defaultSecretRequestTimeout,
		minimumOperationDuration,
		secretreader.MaximumRequestTimeout,
	)
	if err != nil {
		return materializerConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_SECRET_REQUEST_TIMEOUT: %w", err)
	}
	secretHandshakeTimeout, err := envDuration(
		getenv("FUGUE_BACKUP_MATERIALIZER_SECRET_HANDSHAKE_TIMEOUT"),
		defaultSecretHandshakeTimeout,
		minimumOperationDuration,
		maximumHandshakeTimeout,
	)
	if err != nil || secretHandshakeTimeout > secretRequestTimeout {
		return materializerConfig{}, errors.New("FUGUE_BACKUP_MATERIALIZER_SECRET_HANDSHAKE_TIMEOUT: must be bounded by the Secret request timeout")
	}
	if inputRequestTimeout > attemptTimeout || secretRequestTimeout > attemptTimeout {
		return materializerConfig{}, errors.New("materializer request timeouts must not exceed the attempt timeout")
	}
	inputResponseBytes, err := envInt64(
		getenv("FUGUE_BACKUP_MATERIALIZER_INPUT_MAX_RESPONSE_BYTES"),
		defaultInputResponseBytes,
		1024,
		int64(materializercontract.MaxObserverInputBundleBytes),
	)
	if err != nil {
		return materializerConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_INPUT_MAX_RESPONSE_BYTES: %w", err)
	}
	secretResponseBytes, err := envInt64(
		getenv("FUGUE_BACKUP_MATERIALIZER_SECRET_MAX_RESPONSE_BYTES"),
		defaultSecretResponseBytes,
		4<<10,
		secretreader.MaximumResponse,
	)
	if err != nil {
		return materializerConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_SECRET_MAX_RESPONSE_BYTES: %w", err)
	}

	cellKey := strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_CELL_KEY"))
	runID := strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_RUN_ID"))
	config.CellKey = cellKey
	config.RunID = runID
	config.Interval = interval
	config.AttemptTimeout = attemptTimeout
	config.InputProjection = clientprojected.Config{
		Enabled:          true,
		BaseURL:          strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_INPUT_API_BASE_URL")),
		ProjectionRoot:   strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_INPUT_PROJECTION_ROOT")),
		ExpectedCellKey:  cellKey,
		ExpectedRunID:    runID,
		RequestTimeout:   inputRequestTimeout,
		HandshakeTimeout: inputHandshakeTimeout,
		MaxResponseBytes: inputResponseBytes,
	}
	config.SecretProjection = secretprojected.Config{
		Enabled:          true,
		APIServerURL:     strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_KUBERNETES_API_URL")),
		ProjectionRoot:   strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_KUBERNETES_PROJECTION_ROOT")),
		ExpectedCellKey:  cellKey,
		RequestTimeout:   secretRequestTimeout,
		HandshakeTimeout: secretHandshakeTimeout,
		MaxResponseBytes: secretResponseBytes,
	}
	return config, nil
}

func runProbe(args []string, getenv func(string) string) error {
	if getenv == nil {
		return errors.New("environment reader is nil")
	}
	if len(args) != 2 || args[0] != "probe" || (args[1] != "health" && args[1] != "ready") {
		return errors.New("usage: fugue-backup-materializer probe health|ready")
	}
	bindAddr := strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_BIND_ADDR"))
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
		Transport: &http.Transport{
			Proxy:               nil,
			DisableCompression:  true,
			DisableKeepAlives:   true,
			MaxConnsPerHost:     1,
			MaxIdleConnsPerHost: 0,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
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

func envDuration(raw string, fallback, minimum, maximum time.Duration) (time.Duration, error) {
	value := fallback
	if raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return 0, errors.New("must be a duration")
		}
		value = parsed
	}
	if !boundedDuration(value, minimum, maximum) {
		return 0, errors.New("duration is outside the bounded millisecond range")
	}
	return value, nil
}

func envInt64(raw string, fallback, minimum, maximum int64) (int64, error) {
	value := fallback
	if raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, errors.New("must be a base-10 integer")
		}
		value = parsed
	}
	if value < minimum || value > maximum {
		return 0, errors.New("integer is outside the bounded range")
	}
	return value, nil
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
	if err != nil || parsedPort < 1024 || parsedPort > 65535 {
		return errors.New("port must be between 1024 and 65535")
	}
	return nil
}

func boundedDuration(value, minimum, maximum time.Duration) bool {
	return value >= minimum && value <= maximum && value%time.Millisecond == 0
}

func publicCell(config materializerConfig) string {
	if !config.Enabled {
		return "disabled"
	}
	return config.CellKey
}
