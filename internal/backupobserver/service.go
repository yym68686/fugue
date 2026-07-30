// Package backupobserver runs the independently isolated, observation-only
// backup control loop used during the backup-storage migration. It imports the
// pure backup contract but has no legacy API, store, model, database,
// Kubernetes, object-storage, or process-execution capability.
package backupobserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"fugue/internal/backupcontrol"
)

const (
	ServiceAPIVersion        = "backup-observer.fugue.dev/v2"
	ServiceKind              = "BackupObserverStatus"
	ServiceModeDisabled      = "disabled"
	ServiceModeShadow        = "shadow"
	defaultReconcileInterval = 30 * time.Second
	defaultAttemptTimeout    = 20 * time.Second
	minReconcileInterval     = time.Second
	maxReconcileInterval     = 10 * time.Minute
	minAttemptTimeout        = time.Second
	maxAttemptTimeout        = time.Minute
	maxSpecBytes             = int64(32 << 10)
	maxTokenBytes            = int64(16 << 10)
	maxObservationFutureSkew = 5 * time.Second
)

var (
	ErrService       = errors.New("backup observer failed")
	ErrServiceConfig = errors.New("backup observer configuration is invalid")
	canonicalCellKey = regexp.MustCompile(`^backup/(control-plane-db|app-database|persistent-storage|data-workspace|registry|platform-component)/[0-9a-f]{16}$`)
)

type ServiceConfig struct {
	Enabled                   bool
	ExpectedCellKey           string
	SpecPath                  string
	TokenPath                 string
	LKGPath                   string
	APIBaseURL                string
	Interval                  time.Duration
	AttemptTimeout            time.Duration
	RequestTimeout            time.Duration
	MaxResponseBytes          int64
	AllowInsecureHTTPForTests bool
	HTTPClient                *http.Client
}

type observationSource interface {
	Observe(context.Context, backupcontrol.BackupRunSpec) (backupcontrol.BackupRunStatus, error)
}

type sourceFactory func(HTTPObservationSourceConfig) (observationSource, error)

// Snapshot is safe for local operational endpoints. It never contains a
// bearer token or a remote response body, and both mutation flags are fixed
// false regardless of remote input.
type Snapshot struct {
	APIVersion                string                         `json:"apiVersion"`
	Kind                      string                         `json:"kind"`
	Mode                      string                         `json:"mode"`
	CellKey                   string                         `json:"cellKey,omitempty"`
	Ready                     bool                           `json:"ready"`
	Reconciling               bool                           `json:"reconciling"`
	ObservationOnly           bool                           `json:"observationOnly"`
	ProductionMutationAllowed bool                           `json:"productionMutationAllowed"`
	AttemptCount              uint64                         `json:"attemptCount"`
	ConsecutiveFailures       uint64                         `json:"consecutiveFailures"`
	LastAttemptAt             *time.Time                     `json:"lastAttemptAt,omitempty"`
	LastSuccessAt             *time.Time                     `json:"lastSuccessAt,omitempty"`
	FailureCode               string                         `json:"failureCode,omitempty"`
	DesiredSpec               *backupcontrol.BackupRunSpec   `json:"desiredSpec,omitempty"`
	CurrentStatus             *backupcontrol.BackupRunStatus `json:"currentStatus,omitempty"`
	LastKnownGood             *backupcontrol.BackupRunStatus `json:"lastKnownGood,omitempty"`
	LKGState                  string                         `json:"lkgState"`
}

// Service retries only its own cell. Invalid input or a remote outage makes
// this lane unready while retaining the last validated status for diagnosis
// and recovery; it cannot affect another backup cell or the legacy worker.
type Service struct {
	cfg       ServiceConfig
	logger    *log.Logger
	now       func() time.Time
	newSource sourceFactory
	attemptMu sync.Mutex
	stateMu   sync.RWMutex
	snapshot  Snapshot
}

func NewService(cfg ServiceConfig, logger *log.Logger) (*Service, error) {
	if err := validateServiceConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.Interval == 0 {
		cfg.Interval = defaultReconcileInterval
	}
	if cfg.AttemptTimeout == 0 {
		cfg.AttemptTimeout = defaultAttemptTimeout
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	service := &Service{
		cfg:    cfg,
		logger: logger,
		now:    time.Now,
		newSource: func(sourceCfg HTTPObservationSourceConfig) (observationSource, error) {
			return NewHTTPObservationSource(sourceCfg)
		},
		snapshot: Snapshot{
			APIVersion:                ServiceAPIVersion,
			Kind:                      ServiceKind,
			Mode:                      ServiceModeDisabled,
			ObservationOnly:           true,
			ProductionMutationAllowed: false,
			LKGState:                  LKGStateDisabled,
		},
	}
	if cfg.Enabled {
		service.snapshot.Mode = ServiceModeShadow
		service.snapshot.CellKey = cfg.ExpectedCellKey
		restored := restoreBackupObserverLKG(cfg.LKGPath, cfg.ExpectedCellKey)
		service.snapshot.LKGState = restored.State
		if restored.State == LKGStateCurrent || restored.State == LKGStatePrevious {
			copySpec := restored.Spec
			copyStatus := restored.Status
			service.snapshot.DesiredSpec = &copySpec
			service.snapshot.LastKnownGood = &copyStatus
		}
		if restored.State == LKGStateInvalid {
			service.snapshot.FailureCode = "lkg_invalid"
		} else if restored.State == LKGStatePrevious {
			service.snapshot.FailureCode = "lkg_current_invalid"
		}
	}
	return service, nil
}

func validateServiceConfig(cfg ServiceConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if !canonicalCellKey.MatchString(cfg.ExpectedCellKey) {
		return fmt.Errorf("%w: expected cell key is not canonical", ErrServiceConfig)
	}
	for label, value := range map[string]string{"spec path": cfg.SpecPath, "token path": cfg.TokenPath} {
		if strings.TrimSpace(value) != value || !filepath.IsAbs(value) || filepath.Clean(value) != value {
			return fmt.Errorf("%w: %s must be a canonical absolute path", ErrServiceConfig, label)
		}
	}
	if filepath.Clean(cfg.SpecPath) == filepath.Clean(cfg.TokenPath) {
		return fmt.Errorf("%w: spec and token paths must differ", ErrServiceConfig)
	}
	if cfg.LKGPath != "" {
		if strings.TrimSpace(cfg.LKGPath) != cfg.LKGPath || !filepath.IsAbs(cfg.LKGPath) || filepath.Clean(cfg.LKGPath) != cfg.LKGPath {
			return fmt.Errorf("%w: LKG path must be a canonical absolute path", ErrServiceConfig)
		}
		for _, inputPath := range []string{cfg.SpecPath, cfg.TokenPath} {
			if pathsOverlap(filepath.Dir(cfg.LKGPath), filepath.Dir(inputPath)) {
				return fmt.Errorf("%w: LKG state must not overlap projected inputs", ErrServiceConfig)
			}
		}
	}
	if cfg.Interval != 0 && (cfg.Interval < minReconcileInterval || cfg.Interval > maxReconcileInterval) {
		return fmt.Errorf("%w: interval must be between %s and %s", ErrServiceConfig, minReconcileInterval, maxReconcileInterval)
	}
	if cfg.AttemptTimeout != 0 && (cfg.AttemptTimeout < minAttemptTimeout || cfg.AttemptTimeout > maxAttemptTimeout) {
		return fmt.Errorf("%w: attempt timeout must be between %s and %s", ErrServiceConfig, minAttemptTimeout, maxAttemptTimeout)
	}
	_, err := NewHTTPObservationSource(HTTPObservationSourceConfig{
		BaseURL:                   cfg.APIBaseURL,
		BearerToken:               "configuration-validation-only",
		RequestTimeout:            cfg.RequestTimeout,
		MaxResponseBytes:          cfg.MaxResponseBytes,
		AllowInsecureHTTPForTests: cfg.AllowInsecureHTTPForTests,
		Client:                    cfg.HTTPClient,
	})
	if err != nil {
		return fmt.Errorf("%w: HTTP boundary: %v", ErrServiceConfig, err)
	}
	return nil
}

func (service *Service) ReconcileOnce(ctx context.Context) error {
	if service == nil || ctx == nil {
		return fmt.Errorf("%w: service or context is nil", ErrService)
	}
	if !service.cfg.Enabled {
		return nil
	}
	service.attemptMu.Lock()
	defer service.attemptMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	attemptStarted := service.now().UTC()
	service.beginAttempt(attemptStarted)
	attemptCtx, cancel := context.WithTimeout(ctx, service.cfg.AttemptTimeout)
	defer cancel()

	spec, err := readSpec(service.cfg.SpecPath)
	if err != nil {
		return service.failAttempt("spec_invalid", err)
	}
	if spec.CellKey != service.cfg.ExpectedCellKey {
		return service.failAttempt("cell_mismatch", errors.New("desired spec does not belong to this cell"))
	}
	service.setDesiredSpec(spec)
	token, err := readToken(service.cfg.TokenPath)
	if err != nil {
		return service.failAttempt("credential_unavailable", err)
	}
	source, err := service.newSource(HTTPObservationSourceConfig{
		BaseURL:                   service.cfg.APIBaseURL,
		BearerToken:               token,
		RequestTimeout:            service.cfg.RequestTimeout,
		MaxResponseBytes:          service.cfg.MaxResponseBytes,
		AllowInsecureHTTPForTests: service.cfg.AllowInsecureHTTPForTests,
		Client:                    service.cfg.HTTPClient,
	})
	if err != nil {
		return service.failAttempt("transport_config_invalid", err)
	}
	status, err := source.Observe(attemptCtx, spec)
	if err != nil {
		return service.failAttempt(observationFailureCode(err), err)
	}
	now := service.now().UTC()
	if status.ObservedAt.After(now.Add(maxObservationFutureSkew)) || !status.ValidUntil.After(now) {
		return service.failAttempt("observation_stale", errors.New("observation is expired or from the future"))
	}
	if err := persistBackupObserverLKG(service.cfg.LKGPath, service.cfg.ExpectedCellKey, spec, status, now); err != nil {
		service.markLKGPersistFailed()
		return service.failAttempt("lkg_persist_failed", err)
	}
	service.succeedAttempt(now, spec, status)
	return nil
}

func (service *Service) Run(ctx context.Context) error {
	if service == nil || ctx == nil {
		return fmt.Errorf("%w: service or context is nil", ErrService)
	}
	if !service.cfg.Enabled {
		<-ctx.Done()
		return nil
	}
	for {
		if err := service.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
			service.logger.Printf("backup observer attempt failed code=%s", service.Snapshot().FailureCode)
		}
		timer := time.NewTimer(service.cfg.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (service *Service) Snapshot() Snapshot {
	if service == nil {
		return Snapshot{}
	}
	service.stateMu.RLock()
	snapshot := cloneSnapshot(service.snapshot)
	service.stateMu.RUnlock()
	if snapshot.Ready && (snapshot.CurrentStatus == nil || !snapshot.CurrentStatus.ValidUntil.After(service.now().UTC())) {
		snapshot.Ready = false
		snapshot.FailureCode = "observation_expired"
	}
	return snapshot
}

// Handler intentionally exposes no endpoint that accepts a spec, command, or
// body. It is safe to bind only to Pod loopback for liveness/readiness probes.
func (service *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeSnapshot(w, http.StatusOK, service.Snapshot())
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		snapshot := service.Snapshot()
		status := http.StatusOK
		if !snapshot.Ready {
			status = http.StatusServiceUnavailable
		}
		writeSnapshot(w, status, snapshot)
	})
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		writeSnapshot(w, http.StatusOK, service.Snapshot())
	})
	return mux
}

func writeSnapshot(w http.ResponseWriter, status int, value Snapshot) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (service *Service) beginAttempt(now time.Time) {
	service.stateMu.Lock()
	defer service.stateMu.Unlock()
	service.snapshot.AttemptCount++
	service.snapshot.Reconciling = true
	service.snapshot.LastAttemptAt = timePointer(now)
	service.snapshot.FailureCode = ""
}

func (service *Service) setDesiredSpec(spec backupcontrol.BackupRunSpec) {
	service.stateMu.Lock()
	defer service.stateMu.Unlock()
	copySpec := spec
	service.snapshot.DesiredSpec = &copySpec
}

func (service *Service) failAttempt(code string, cause error) error {
	service.stateMu.Lock()
	service.snapshot.Reconciling = false
	service.snapshot.Ready = false
	service.snapshot.ConsecutiveFailures++
	service.snapshot.FailureCode = code
	service.snapshot.CurrentStatus = nil
	service.stateMu.Unlock()
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrService, code)
	}
	return fmt.Errorf("%w: %s: %w", ErrService, code, cause)
}

func (service *Service) succeedAttempt(now time.Time, spec backupcontrol.BackupRunSpec, status backupcontrol.BackupRunStatus) {
	service.stateMu.Lock()
	defer service.stateMu.Unlock()
	service.snapshot.Reconciling = false
	service.snapshot.Ready = true
	service.snapshot.ConsecutiveFailures = 0
	service.snapshot.FailureCode = ""
	service.snapshot.LastSuccessAt = timePointer(now)
	copySpec := spec
	copyStatus := status
	service.snapshot.DesiredSpec = &copySpec
	service.snapshot.CurrentStatus = &copyStatus
	service.snapshot.LastKnownGood = &copyStatus
	if service.cfg.LKGPath == "" {
		service.snapshot.LKGState = LKGStateMemoryOnly
	} else {
		service.snapshot.LKGState = LKGStateCurrent
	}
}

func (service *Service) markLKGPersistFailed() {
	service.stateMu.Lock()
	service.snapshot.LKGState = LKGStatePersistFailed
	service.stateMu.Unlock()
}

func pathsOverlap(left, right string) bool {
	within := func(parent, candidate string) bool {
		relative, err := filepath.Rel(parent, candidate)
		return err == nil && (relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
	}
	return within(left, right) || within(right, left)
}

func observationFailureCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "attempt_timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var statusErr *ObservationAPIStatusError
	if errors.As(err, &statusErr) {
		if statusErr.StatusCode == http.StatusUnauthorized || statusErr.StatusCode == http.StatusForbidden {
			return "authorization_rejected"
		}
		if statusErr.Retryable() {
			return "api_retryable"
		}
		return "api_rejected"
	}
	if errors.Is(err, ErrObservationAPI) {
		return "observation_invalid"
	}
	if errors.Is(err, ErrObservationTransport) {
		return "api_unavailable"
	}
	return "internal_error"
}

func readSpec(path string) (backupcontrol.BackupRunSpec, error) {
	document, err := readRegularFile(path, maxSpecBytes, false)
	if err != nil {
		return backupcontrol.BackupRunSpec{}, err
	}
	return backupcontrol.DecodeBackupRunSpec(document)
}

func readToken(path string) (string, error) {
	document, err := readRegularFile(path, maxTokenBytes, true)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(document))
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return "", errors.New("credential is empty or contains a newline")
	}
	return token, nil
}

func readRegularFile(path string, limit int64, credential bool) ([]byte, error) {
	if limit <= 0 || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("path or read limit is not canonical")
	}
	parent := filepath.Dir(path)
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return nil, err
	}
	if !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("input parent is not a non-symlink directory")
	}
	requestedInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	resolved := path
	projected := false
	if requestedInfo.Mode()&os.ModeSymlink != 0 {
		projected = true
		resolved, err = resolveProjectedInput(path)
		if err != nil {
			return nil, err
		}
	} else if !requestedInfo.Mode().IsRegular() {
		return nil, errors.New("input is not a regular file")
	}
	before, err := os.Lstat(resolved)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("resolved input is not a regular file")
	}
	if credential && before.Mode().Perm()&0o137 != 0 {
		return nil, errors.New("credential permissions are too broad")
	}
	file, err := os.Open(resolved)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || !after.Mode().IsRegular() {
		return nil, errors.New("file identity changed while opening")
	}
	document, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(document)) > limit {
		return nil, fmt.Errorf("file exceeds %d bytes", limit)
	}
	currentResolved := path
	if projected {
		currentResolved, err = resolveProjectedInput(path)
		if err != nil || currentResolved != resolved {
			return nil, errors.New("projected input generation changed while reading")
		}
	} else {
		currentRequested, statErr := os.Lstat(path)
		if statErr != nil || !currentRequested.Mode().IsRegular() || currentRequested.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("regular input topology changed while reading")
		}
	}
	current, err := os.Stat(currentResolved)
	if err != nil || !os.SameFile(after, current) {
		return nil, errors.New("input identity changed while reading")
	}
	return document, nil
}

// resolveProjectedInput accepts only Kubernetes' atomic writer layout:
//
//	<name> -> ..data/<name>
//	..data -> ..<generation>
//
// The resolved generation must stay within the same non-symlink volume root.
// Arbitrary symlinks and links escaping that root remain forbidden.
func resolveProjectedInput(path string) (string, error) {
	parent := filepath.Dir(path)
	base := filepath.Base(path)
	linkTarget, err := os.Readlink(path)
	if err != nil || linkTarget != filepath.Join("..data", base) {
		return "", errors.New("input symlink is not a Kubernetes projected-file link")
	}
	dataLink := filepath.Join(parent, "..data")
	dataInfo, err := os.Lstat(dataLink)
	if err != nil || dataInfo.Mode()&os.ModeSymlink == 0 {
		return "", errors.New("projected input has no atomic data link")
	}
	generation, err := os.Readlink(dataLink)
	if err != nil || filepath.IsAbs(generation) || filepath.Clean(generation) != generation ||
		filepath.Base(generation) != generation || generation == ".." || !strings.HasPrefix(generation, "..") {
		return "", errors.New("projected input generation link is not canonical")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", errors.New("resolve projected input")
	}
	resolved = filepath.Clean(resolved)
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return "", errors.New("resolve projected input parent")
	}
	relative, err := filepath.Rel(filepath.Clean(resolvedParent), resolved)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("projected input escapes its volume root")
	}
	return resolved, nil
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
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
		value.LastKnownGood = cloneArtifact(value.LastKnownGood)
		snapshot.CurrentStatus = &value
	}
	if snapshot.LastKnownGood != nil {
		value := *snapshot.LastKnownGood
		value.LastKnownGood = cloneArtifact(value.LastKnownGood)
		snapshot.LastKnownGood = &value
	}
	return snapshot
}

func cloneArtifact(artifact *backupcontrol.BackupArtifactRef) *backupcontrol.BackupArtifactRef {
	if artifact == nil {
		return nil
	}
	value := *artifact
	return &value
}

func timePointer(value time.Time) *time.Time {
	return &value
}
