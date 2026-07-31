package releasecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
)

const (
	ComponentPlanServiceAPIVersion = "release-control.fugue.dev/v1"
	ComponentPlanServiceKind       = "ComponentPlanServiceStatus"
	ComponentPlanServiceModeOff    = "disabled"
	ComponentPlanServiceModeShadow = "shadow"
	defaultComponentPlanInterval   = 30 * time.Second
	defaultComponentPlanAttemptTTL = 45 * time.Second
	minComponentPlanInterval       = time.Second
	minComponentPlanAttemptTTL     = time.Second
	maxComponentPlanInterval       = 10 * time.Minute
	maxComponentPlanAttemptTTL     = 2 * time.Minute
	maxComponentPlanSpecBytes      = 64 << 10
	maxComponentPlanTokenBytes     = 16 << 10
)

var (
	ErrComponentPlanService       = errors.New("component plan service failed")
	ErrComponentPlanServiceConfig = errors.New("component plan service configuration is invalid")
	ErrComponentPlanToken         = errors.New("component plan credential is invalid")
)

// ComponentPlanServiceConfig is deliberately limited to a file-fed spec and
// the versioned HTTP adapter. It contains no database, Kubernetes, or process
// execution capability. Disabled is the safe default and performs no I/O.
type ComponentPlanServiceConfig struct {
	Enabled          bool
	SpecPath         string
	TokenPath        string
	APIBaseURL       string
	Interval         time.Duration
	AttemptTimeout   time.Duration
	RequestTimeout   time.Duration
	MaxResponseBytes int64
	HTTPClient       *http.Client
}

type componentPlanPrincipalStore interface {
	ComponentPlanStore
	ResolvePrincipal(context.Context) (model.Principal, error)
}

type componentPlanStoreFactory func(HTTPComponentPlanStoreConfig) (componentPlanPrincipalStore, error)

// ComponentPlanServiceSnapshot is safe to expose from the local health
// endpoint. It contains no credential or remote response body; the durable
// shadow status itself remains owned by the platform artifact ledger.
type ComponentPlanServiceSnapshot struct {
	APIVersion                string               `json:"apiVersion"`
	Kind                      string               `json:"kind"`
	Mode                      string               `json:"mode"`
	Ready                     bool                 `json:"ready"`
	Reconciling               bool                 `json:"reconciling"`
	ObservationOnly           bool                 `json:"observationOnly"`
	ProductionMutationAllowed bool                 `json:"productionMutationAllowed"`
	AttemptCount              uint64               `json:"attemptCount"`
	ConsecutiveFailures       uint64               `json:"consecutiveFailures"`
	LastAttemptAt             *time.Time           `json:"lastAttemptAt,omitempty"`
	LastSuccessAt             *time.Time           `json:"lastSuccessAt,omitempty"`
	FailureCode               string               `json:"failureCode,omitempty"`
	DesiredSpec               *ComponentPlanSpec   `json:"desiredSpec,omitempty"`
	CurrentStatus             *ComponentPlanStatus `json:"currentStatus,omitempty"`
	LastKnownGood             *ComponentPlanStatus `json:"lastKnownGood,omitempty"`
}

// ComponentPlanService runs one lane-local, observation-only control loop.
// A failed attempt marks only this lane unready and retains its last good
// status; later attempts reread both files so credential/spec rotation can
// recover without restarting or affecting another component.
type ComponentPlanService struct {
	cfg       ComponentPlanServiceConfig
	logger    *log.Logger
	now       func() time.Time
	newStore  componentPlanStoreFactory
	attemptMu sync.Mutex
	stateMu   sync.RWMutex
	snapshot  ComponentPlanServiceSnapshot
}

// NewComponentPlanService validates configuration without opening a network
// connection. In disabled mode even nonexistent spec and credential paths are
// accepted and never read.
func NewComponentPlanService(cfg ComponentPlanServiceConfig, logger *log.Logger) (*ComponentPlanService, error) {
	if err := validateComponentPlanServiceConfig(cfg); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	interval := cfg.Interval
	if interval == 0 {
		interval = defaultComponentPlanInterval
	}
	attemptTimeout := cfg.AttemptTimeout
	if attemptTimeout == 0 {
		attemptTimeout = defaultComponentPlanAttemptTTL
	}
	cfg.Interval = interval
	cfg.AttemptTimeout = attemptTimeout
	service := &ComponentPlanService{
		cfg:    cfg,
		logger: logger,
		now:    time.Now,
		newStore: func(storeCfg HTTPComponentPlanStoreConfig) (componentPlanPrincipalStore, error) {
			return NewHTTPComponentPlanStore(storeCfg)
		},
	}
	service.snapshot = ComponentPlanServiceSnapshot{
		APIVersion:                ComponentPlanServiceAPIVersion,
		Kind:                      ComponentPlanServiceKind,
		Mode:                      ComponentPlanServiceModeOff,
		ObservationOnly:           true,
		ProductionMutationAllowed: false,
	}
	if cfg.Enabled {
		service.snapshot.Mode = ComponentPlanServiceModeShadow
	}
	return service, nil
}

func validateComponentPlanServiceConfig(cfg ComponentPlanServiceConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if strings.TrimSpace(cfg.APIBaseURL) == "" {
		return fmt.Errorf("%w: API base URL is required when enabled", ErrComponentPlanServiceConfig)
	}
	for name, path := range map[string]string{"spec path": cfg.SpecPath, "token path": cfg.TokenPath} {
		path = strings.TrimSpace(path)
		if path == "" || !filepath.IsAbs(path) {
			return fmt.Errorf("%w: %s must be an absolute path", ErrComponentPlanServiceConfig, name)
		}
	}
	if filepath.Clean(cfg.SpecPath) == filepath.Clean(cfg.TokenPath) {
		return fmt.Errorf("%w: spec and token paths must differ", ErrComponentPlanServiceConfig)
	}
	if cfg.Interval != 0 && (cfg.Interval < minComponentPlanInterval || cfg.Interval > maxComponentPlanInterval) {
		return fmt.Errorf("%w: interval must be between %s and %s", ErrComponentPlanServiceConfig, minComponentPlanInterval, maxComponentPlanInterval)
	}
	if cfg.AttemptTimeout != 0 && (cfg.AttemptTimeout < minComponentPlanAttemptTTL || cfg.AttemptTimeout > maxComponentPlanAttemptTTL) {
		return fmt.Errorf("%w: attempt timeout must be between %s and %s", ErrComponentPlanServiceConfig, minComponentPlanAttemptTTL, maxComponentPlanAttemptTTL)
	}
	if cfg.RequestTimeout < 0 || cfg.RequestTimeout > maxComponentPlanAPIRequestTimeout {
		return fmt.Errorf("%w: request timeout must be between 1ns and %s", ErrComponentPlanServiceConfig, maxComponentPlanAPIRequestTimeout)
	}
	if cfg.MaxResponseBytes < 0 || cfg.MaxResponseBytes > maxComponentPlanAPIResponseBytes {
		return fmt.Errorf("%w: max response size must be between 1 and %d bytes", ErrComponentPlanServiceConfig, maxComponentPlanAPIResponseBytes)
	}
	if _, err := NewHTTPComponentPlanStore(HTTPComponentPlanStoreConfig{
		BaseURL:          cfg.APIBaseURL,
		BearerToken:      "configuration-validation-only",
		Client:           cfg.HTTPClient,
		RequestTimeout:   cfg.RequestTimeout,
		MaxResponseBytes: cfg.MaxResponseBytes,
	}); err != nil {
		return fmt.Errorf("%w: HTTP boundary: %v", ErrComponentPlanServiceConfig, err)
	}
	return nil
}

// ReconcileOnce performs one bounded attempt. It is exported so a future
// scheduler can own timing while retaining exactly the same lane boundary.
func (s *ComponentPlanService) ReconcileOnce(ctx context.Context) error {
	if s == nil {
		return ErrComponentPlanService
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrComponentPlanService)
	}
	if !s.cfg.Enabled {
		return nil
	}
	s.attemptMu.Lock()
	defer s.attemptMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	now := s.now().UTC()
	s.beginAttempt(now)
	attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.AttemptTimeout)
	defer cancel()

	spec, err := readComponentPlanSpec(s.cfg.SpecPath)
	if err != nil {
		return s.failAttempt("spec_invalid", err)
	}
	s.setDesiredSpec(spec)
	token, err := readComponentPlanToken(s.cfg.TokenPath)
	if err != nil {
		return s.failAttempt("credential_unavailable", err)
	}
	client, err := s.newStore(HTTPComponentPlanStoreConfig{
		BaseURL:          s.cfg.APIBaseURL,
		BearerToken:      token,
		Client:           s.cfg.HTTPClient,
		RequestTimeout:   s.cfg.RequestTimeout,
		MaxResponseBytes: s.cfg.MaxResponseBytes,
	})
	if err != nil {
		return s.failAttempt("transport_config_invalid", err)
	}
	principal, err := client.ResolvePrincipal(attemptCtx)
	if err != nil {
		return s.failAttempt(componentPlanFailureCode(err), err)
	}
	status, err := ReconcileComponentPlan(attemptCtx, client, spec, principal)
	if err != nil {
		return s.failAttempt(componentPlanFailureCode(err), err)
	}
	s.succeedAttempt(s.now().UTC(), spec, status)
	return nil
}

// Run keeps retrying one lane until its context is canceled. A failed attempt
// is intentionally not returned: a bad credential, stale spec, or temporary
// API outage must be recoverable in place and must not kill the process.
func (s *ComponentPlanService) Run(ctx context.Context) error {
	if s == nil {
		return ErrComponentPlanService
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrComponentPlanService)
	}
	if !s.cfg.Enabled {
		<-ctx.Done()
		return nil
	}
	for {
		if err := s.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
			s.logger.Printf("release-control attempt failed code=%s", s.Snapshot().FailureCode)
		}
		timer := time.NewTimer(s.cfg.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

// Snapshot returns a deep-enough copy for concurrent JSON encoding.
func (s *ComponentPlanService) Snapshot() ComponentPlanServiceSnapshot {
	if s == nil {
		return ComponentPlanServiceSnapshot{}
	}
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	snapshot := s.snapshot
	if snapshot.LastAttemptAt != nil {
		value := *snapshot.LastAttemptAt
		snapshot.LastAttemptAt = &value
	}
	if snapshot.LastSuccessAt != nil {
		value := *snapshot.LastSuccessAt
		snapshot.LastSuccessAt = &value
	}
	if snapshot.DesiredSpec != nil {
		value := *snapshot.DesiredSpec
		snapshot.DesiredSpec = &value
	}
	if snapshot.CurrentStatus != nil {
		value := *snapshot.CurrentStatus
		snapshot.CurrentStatus = &value
	}
	if snapshot.LastKnownGood != nil {
		value := *snapshot.LastKnownGood
		snapshot.LastKnownGood = &value
	}
	return snapshot
}

// Handler exposes only local operational endpoints. No endpoint accepts a
// spec or a release command, so an accidental network exposure cannot add a
// production mutation path.
func (s *ComponentPlanService) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	return mux
}

func (s *ComponentPlanService) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeComponentPlanJSON(w, http.StatusOK, s.Snapshot())
}

func (s *ComponentPlanService) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	snapshot := s.Snapshot()
	status := http.StatusOK
	if !snapshot.Ready {
		status = http.StatusServiceUnavailable
	}
	writeComponentPlanJSON(w, status, snapshot)
}

func (s *ComponentPlanService) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeComponentPlanJSON(w, http.StatusOK, s.Snapshot())
}

func writeComponentPlanJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (s *ComponentPlanService) beginAttempt(now time.Time) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.snapshot.AttemptCount++
	s.snapshot.Reconciling = true
	s.snapshot.LastAttemptAt = timePtr(now)
	s.snapshot.FailureCode = ""
}

func (s *ComponentPlanService) setDesiredSpec(spec ComponentPlanSpec) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	snapshot := spec
	s.snapshot.DesiredSpec = &snapshot
}

func (s *ComponentPlanService) failAttempt(code string, err error) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.snapshot.Reconciling = false
	s.snapshot.Ready = false
	s.snapshot.ConsecutiveFailures++
	s.snapshot.FailureCode = code
	s.snapshot.CurrentStatus = nil
	if err == nil {
		return fmt.Errorf("%w: %s", ErrComponentPlanService, code)
	}
	return fmt.Errorf("%w: %s: %w", ErrComponentPlanService, code, err)
}

func (s *ComponentPlanService) succeedAttempt(now time.Time, spec ComponentPlanSpec, status ComponentPlanStatus) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.snapshot.Reconciling = false
	s.snapshot.Ready = true
	s.snapshot.ConsecutiveFailures = 0
	s.snapshot.FailureCode = ""
	s.snapshot.LastSuccessAt = timePtr(now)
	specCopy := spec
	statusCopy := status
	s.snapshot.DesiredSpec = &specCopy
	s.snapshot.CurrentStatus = &statusCopy
	s.snapshot.LastKnownGood = &statusCopy
}

func componentPlanFailureCode(err error) string {
	if err == nil {
		return "unknown"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "attempt_timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var statusErr *ComponentPlanAPIStatusError
	if errors.As(err, &statusErr) {
		if statusErr.StatusCode == http.StatusUnauthorized || statusErr.StatusCode == http.StatusForbidden {
			return "authorization_rejected"
		}
		if statusErr.Retryable() {
			return "api_retryable"
		}
		return "api_rejected"
	}
	if errors.Is(err, ErrComponentPlanAPI) {
		return "api_unavailable"
	}
	if errors.Is(err, ErrComponentPlanReconcile) {
		return "reconcile_rejected"
	}
	return "internal_error"
}

func readComponentPlanSpec(path string) (ComponentPlanSpec, error) {
	data, err := readBoundedFile(path, maxComponentPlanSpecBytes)
	if err != nil {
		return ComponentPlanSpec{}, fmt.Errorf("%w: read spec: %v", ErrComponentPlanSpec, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var spec ComponentPlanSpec
	if err := decoder.Decode(&spec); err != nil {
		return ComponentPlanSpec{}, fmt.Errorf("%w: decode spec: %v", ErrComponentPlanSpec, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return ComponentPlanSpec{}, fmt.Errorf("%w: spec contains trailing JSON", ErrComponentPlanSpec)
		}
		return ComponentPlanSpec{}, fmt.Errorf("%w: decode trailing JSON: %v", ErrComponentPlanSpec, err)
	}
	if err := ValidateComponentPlanSpec(spec); err != nil {
		return ComponentPlanSpec{}, err
	}
	return spec, nil
}

func readComponentPlanToken(path string) (string, error) {
	data, err := readBoundedFile(path, maxComponentPlanTokenBytes)
	if err != nil {
		return "", fmt.Errorf("%w: read credential: %v", ErrComponentPlanToken, err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return "", fmt.Errorf("%w: credential is empty or contains a newline", ErrComponentPlanToken)
	}
	return token, nil
}

func readBoundedFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("file exceeds %d bytes", limit)
	}
	return data, nil
}

func timePtr(value time.Time) *time.Time {
	return &value
}

var _ componentPlanPrincipalStore = (*HTTPComponentPlanStore)(nil)
