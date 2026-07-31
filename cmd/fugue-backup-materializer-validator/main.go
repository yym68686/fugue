// Command fugue-backup-materializer-validator runs one independently isolated
// backup Secret server-side-dry-run validation cell. It is disabled by default
// and cannot persist, delete, execute, publish, or deploy anything.
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

	materializerclient "fugue/internal/backupmaterializer/client"
	clientprojected "fugue/internal/backupmaterializer/client/projected"
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/secretreader"
	readerprojected "fugue/internal/backupmaterializer/secretreader/projected"
	"fugue/internal/backupmaterializer/secretwriter"
	writerprojected "fugue/internal/backupmaterializer/secretwriter/projected"
	"fugue/internal/backupmaterializer/validationagent"
	"fugue/internal/backupmaterializer/validationcomposition"
)

const (
	defaultBindAddr                   = "127.0.0.1:8094"
	defaultInterval                   = 30 * time.Second
	defaultAttemptTimeout             = 20 * time.Second
	defaultInputRequestTimeout        = 5 * time.Second
	defaultInputHandshakeTimeout      = 5 * time.Second
	defaultReaderRequestTimeout       = 5 * time.Second
	defaultReaderHandshakeTimeout     = 5 * time.Second
	defaultValidationRequestTimeout   = 5 * time.Second
	defaultValidationHandshakeTimeout = 5 * time.Second
	defaultShutdownTimeout            = 10 * time.Second
	defaultInputResponseBytes         = int64(materializercontract.MaxObserverInputBundleBytes)
	defaultReaderResponseBytes        = secretreader.DefaultMaxResponse
	defaultValidationResponseBytes    = secretwriter.DefaultMaxResponse
	probeTimeout                      = 2 * time.Second
	probeResponseBytes                = int64(32 << 10)

	minimumOperationDuration = time.Second
	maximumInterval          = 10 * time.Minute
	maximumAttemptTimeout    = time.Minute
	maximumHandshakeTimeout  = 15 * time.Second
	maximumShutdownTimeout   = time.Minute
)

type validatorConfig struct {
	Enabled         bool
	BindAddr        string
	ShutdownTimeout time.Duration
	Composition     validationcomposition.Config
}

func (config validatorConfig) String() string {
	return "backup materializer validator process configuration [REDACTED]"
}

func (config validatorConfig) GoString() string { return config.String() }

func main() {
	if len(os.Args) > 1 {
		if err := runProbe(os.Args[1:], os.Getenv); err != nil {
			fmt.Fprintln(os.Stderr, "fugue-backup-materializer-validator probe:", err)
			os.Exit(1)
		}
		return
	}
	config, err := validatorConfigFromEnv(os.Getenv)
	if err != nil {
		log.Fatalf("backup materializer validator config: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, config, log.Default()); err != nil {
		log.Fatalf("backup materializer validator: %v", err)
	}
}

func run(ctx context.Context, config validatorConfig, logger *log.Logger) error {
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
	service, err := newValidatorService(config, logger)
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
	logger.Printf(
		"fugue-backup-materializer-validator listening on Pod loopback enabled=%t cell=%s",
		config.Enabled, publicCell(config),
	)
	return serve(ctx, config.ShutdownTimeout, service, server, listener)
}

func newValidatorService(config validatorConfig, logger *log.Logger) (*validationagent.Service, error) {
	composition := config.Composition
	composition.Enabled = config.Enabled
	service, err := validationcomposition.New(composition, logger)
	if err != nil || service == nil || service.Enabled() != config.Enabled {
		return nil, errors.New("construct backup materializer validator service")
	}
	return service, nil
}

func serve(
	ctx context.Context,
	shutdownTimeout time.Duration,
	service *validationagent.Service,
	server *http.Server,
	listener net.Listener,
) error {
	if ctx == nil || service == nil || server == nil || server.Handler == nil || listener == nil ||
		!boundedDuration(shutdownTimeout, minimumOperationDuration, maximumShutdownTimeout) {
		return errors.New("validator lifecycle configuration invalid")
	}
	if validationagent.ValidateSnapshot(service.Snapshot()) != nil || validateLoopbackBindAddr(listener.Addr().String()) != nil {
		return errors.New("validator lifecycle boundary invalid")
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
				failures = append(failures, errors.New("validator control loop stopped unexpectedly"))
			} else {
				failures = append(failures, errors.New("validator control loop failed"))
			}
		}
	case err := <-serverDone:
		serverObserved = true
		if ctx.Err() == nil {
			if err == nil {
				failures = append(failures, errors.New("validator health server stopped unexpectedly"))
			} else {
				failures = append(failures, errors.New("validator health server failed"))
			}
		}
	}

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
		_ = server.Close()
		failures = append(failures, errors.New("validator health server shutdown failed"))
	}
	if !serviceObserved {
		select {
		case err := <-serviceDone:
			if err != nil {
				failures = append(failures, errors.New("validator control loop shutdown failed"))
			}
		case <-shutdownCtx.Done():
			failures = append(failures, errors.New("validator control loop shutdown timed out"))
		}
	}
	if !serverObserved {
		select {
		case err := <-serverDone:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				failures = append(failures, errors.New("validator health server shutdown failed"))
			}
		case <-shutdownCtx.Done():
			_ = server.Close()
			failures = append(failures, errors.New("validator health server shutdown timed out"))
		}
	}
	return errors.Join(failures...)
}

func validatorConfigFromEnv(getenv func(string) string) (validatorConfig, error) {
	if getenv == nil {
		return validatorConfig{}, errors.New("environment reader is nil")
	}
	enabled, err := strictBool(getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ENABLED"))
	if err != nil {
		return validatorConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ENABLED: %w", err)
	}
	bindAddr := strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_BIND_ADDR"))
	if bindAddr == "" {
		bindAddr = defaultBindAddr
	}
	if err := validateLoopbackBindAddr(bindAddr); err != nil {
		return validatorConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_BIND_ADDR: %w", err)
	}
	shutdownTimeout, err := envDuration(
		getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_SHUTDOWN_TIMEOUT"),
		defaultShutdownTimeout, minimumOperationDuration, maximumShutdownTimeout,
	)
	if err != nil {
		return validatorConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_SHUTDOWN_TIMEOUT: %w", err)
	}
	config := validatorConfig{
		Enabled: enabled, BindAddr: bindAddr, ShutdownTimeout: shutdownTimeout,
		Composition: validationcomposition.Config{Enabled: enabled},
	}
	if !enabled {
		return config, nil
	}

	interval, err := envDuration(
		getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_RECONCILE_INTERVAL"),
		defaultInterval, minimumOperationDuration, maximumInterval,
	)
	if err != nil {
		return validatorConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_RECONCILE_INTERVAL: %w", err)
	}
	attemptTimeout, err := envDuration(
		getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ATTEMPT_TIMEOUT"),
		defaultAttemptTimeout, minimumOperationDuration, maximumAttemptTimeout,
	)
	if err != nil {
		return validatorConfig{}, fmt.Errorf("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_ATTEMPT_TIMEOUT: %w", err)
	}
	inputRequestTimeout, inputHandshakeTimeout, inputResponseBytes, err := projectionLimits(
		getenv,
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT",
		defaultInputRequestTimeout,
		defaultInputHandshakeTimeout,
		defaultInputResponseBytes,
		1024,
		int64(materializercontract.MaxObserverInputBundleBytes),
		materializerclient.MaximumRequestTimeout,
	)
	if err != nil {
		return validatorConfig{}, err
	}
	readerRequestTimeout, readerHandshakeTimeout, readerResponseBytes, err := projectionLimits(
		getenv,
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER",
		defaultReaderRequestTimeout,
		defaultReaderHandshakeTimeout,
		defaultReaderResponseBytes,
		4<<10,
		secretreader.MaximumResponse,
		secretreader.MaximumRequestTimeout,
	)
	if err != nil {
		return validatorConfig{}, err
	}
	validationRequestTimeout, validationHandshakeTimeout, validationResponseBytes, err := projectionLimits(
		getenv,
		"FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN",
		defaultValidationRequestTimeout,
		defaultValidationHandshakeTimeout,
		defaultValidationResponseBytes,
		4<<10,
		secretwriter.MaximumResponse,
		secretwriter.MaximumRequestTimeout,
	)
	if err != nil {
		return validatorConfig{}, err
	}
	if inputRequestTimeout > attemptTimeout || readerRequestTimeout > attemptTimeout ||
		validationRequestTimeout > attemptTimeout {
		return validatorConfig{}, errors.New("validator request timeouts must not exceed the attempt timeout")
	}

	cellKey := strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_CELL_KEY"))
	runID := strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_RUN_ID"))
	kubernetesAPIURL := strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_KUBERNETES_API_URL"))
	config.Composition = validationcomposition.Config{
		Enabled: true, CellKey: cellKey, RunID: runID, Interval: interval, AttemptTimeout: attemptTimeout,
		InputProjection: clientprojected.Config{
			Enabled:         true,
			BaseURL:         strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_API_BASE_URL")),
			ProjectionRoot:  strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_INPUT_PROJECTION_ROOT")),
			ExpectedCellKey: cellKey, ExpectedRunID: runID,
			RequestTimeout: inputRequestTimeout, HandshakeTimeout: inputHandshakeTimeout,
			MaxResponseBytes: inputResponseBytes,
		},
		CurrentProjection: readerprojected.Config{
			Enabled: true, APIServerURL: kubernetesAPIURL,
			ProjectionRoot:  strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_READER_PROJECTION_ROOT")),
			ExpectedCellKey: cellKey, RequestTimeout: readerRequestTimeout,
			HandshakeTimeout: readerHandshakeTimeout, MaxResponseBytes: readerResponseBytes,
		},
		ValidationProjection: writerprojected.Config{
			Enabled: true, APIServerURL: kubernetesAPIURL,
			ProjectionRoot:  strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_DRY_RUN_PROJECTION_ROOT")),
			ExpectedCellKey: cellKey, RequestTimeout: validationRequestTimeout,
			HandshakeTimeout: validationHandshakeTimeout, MaxResponseBytes: validationResponseBytes,
		},
	}
	return config, nil
}

func projectionLimits(
	getenv func(string) string,
	prefix string,
	defaultRequest time.Duration,
	defaultHandshake time.Duration,
	defaultResponse int64,
	minimumResponse int64,
	maximumResponse int64,
	maximumRequest time.Duration,
) (time.Duration, time.Duration, int64, error) {
	requestTimeout, err := envDuration(
		getenv(prefix+"_REQUEST_TIMEOUT"), defaultRequest, minimumOperationDuration, maximumRequest,
	)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s_REQUEST_TIMEOUT: %w", prefix, err)
	}
	handshakeTimeout, err := envDuration(
		getenv(prefix+"_HANDSHAKE_TIMEOUT"), defaultHandshake, minimumOperationDuration, maximumHandshakeTimeout,
	)
	if err != nil || handshakeTimeout > requestTimeout {
		return 0, 0, 0, fmt.Errorf("%s_HANDSHAKE_TIMEOUT: must be bounded by the request timeout", prefix)
	}
	responseBytes, err := envInt64(
		getenv(prefix+"_MAX_RESPONSE_BYTES"), defaultResponse, minimumResponse, maximumResponse,
	)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%s_MAX_RESPONSE_BYTES: %w", prefix, err)
	}
	return requestTimeout, handshakeTimeout, responseBytes, nil
}

func runProbe(args []string, getenv func(string) string) error {
	if getenv == nil {
		return errors.New("environment reader is nil")
	}
	if len(args) != 2 || args[0] != "probe" || (args[1] != "health" && args[1] != "ready") {
		return errors.New("usage: fugue-backup-materializer-validator probe health|ready")
	}
	bindAddr := strings.TrimSpace(getenv("FUGUE_BACKUP_MATERIALIZER_VALIDATOR_BIND_ADDR"))
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
			Proxy: nil, DisableCompression: true, DisableKeepAlives: true,
			MaxConnsPerHost: 1, MaxIdleConnsPerHost: 0,
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

func publicCell(config validatorConfig) string {
	if !config.Enabled {
		return "disabled"
	}
	return config.Composition.CellKey
}
