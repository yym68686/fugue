// Package agent supervises one backup materializer cell as a serial,
// observation-only control loop. It owns scheduling and loopback status but no
// filesystem, Kubernetes, datastore, signer, writer, or process capability.
package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"sync"
	"time"

	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconciler"
)

const (
	APIVersion = "backup-materializer-agent.fugue.dev/v1"
	Kind       = "BackupMaterializerAgentStatus"

	ModeDisabled = "disabled"
	ModeShadow   = "shadow"

	FailureAttemptTimeout = "attempt-timeout"
	FailureCanceled       = "canceled"
	FailureCycleInvalid   = "cycle-invalid"
	FailureCycleError     = "cycle-error"

	defaultInterval       = 30 * time.Second
	defaultAttemptTimeout = 20 * time.Second
	minimumInterval       = time.Second
	maximumInterval       = 10 * time.Minute
	minimumAttemptTimeout = time.Second
	maximumAttemptTimeout = time.Minute
)

var (
	ErrConfig   = errors.New("backup materializer agent configuration invalid")
	ErrDisabled = errors.New("backup materializer agent disabled")
	ErrService  = errors.New("backup materializer agent cycle failed")
)

type Cycle interface {
	ReconcileOnce(context.Context) (reconciler.Status, error)
}

type Config struct {
	Enabled        bool
	CellKey        string
	Cycle          Cycle
	Interval       time.Duration
	AttemptTimeout time.Duration
	Now            func() time.Time
}

func (config Config) String() string {
	return "backup materializer agent configuration [REDACTED]"
}

func (config Config) GoString() string { return config.String() }

// Snapshot is the secret-free state of one cell-local supervisor. A failed
// attempt makes CurrentStatus nil and readiness false but retains the last
// valid result for diagnosis and automatic recovery on the next cycle.
type Snapshot struct {
	APIVersion                string             `json:"apiVersion"`
	Kind                      string             `json:"kind"`
	Mode                      string             `json:"mode"`
	CellKey                   string             `json:"cellKey,omitempty"`
	Ready                     bool               `json:"ready"`
	Reconciling               bool               `json:"reconciling"`
	Converged                 bool               `json:"converged"`
	LastKnownGoodServing      bool               `json:"lastKnownGoodServing"`
	Blocked                   bool               `json:"blocked"`
	Retryable                 bool               `json:"retryable"`
	MutationCandidate         bool               `json:"mutationCandidate"`
	DeleteAllowed             bool               `json:"deleteAllowed"`
	ObservationOnly           bool               `json:"observationOnly"`
	ExecutionAllowed          bool               `json:"executionAllowed"`
	ProductionMutationAllowed bool               `json:"productionMutationAllowed"`
	AttemptCount              uint64             `json:"attemptCount"`
	ConsecutiveFailures       uint64             `json:"consecutiveFailures"`
	LastAttemptAt             *time.Time         `json:"lastAttemptAt,omitempty"`
	LastEvaluationAt          *time.Time         `json:"lastEvaluationAt,omitempty"`
	FailureCode               string             `json:"failureCode,omitempty"`
	CurrentStatus             *reconciler.Status `json:"currentStatus,omitempty"`
	LastValidStatus           *reconciler.Status `json:"lastValidStatus,omitempty"`
	Digest                    string             `json:"digest"`
}

type Service struct {
	enabled        bool
	cellKey        string
	cycle          Cycle
	interval       time.Duration
	attemptTimeout time.Duration
	now            func() time.Time
	logger         *log.Logger

	attemptMu sync.Mutex
	stateMu   sync.RWMutex
	snapshot  Snapshot
}

// New ignores and retains none of the cell, cycle, clock, or logger while
// disabled. Enabled construction performs no cycle I/O and starts no timer or
// goroutine.
func New(config Config, logger *log.Logger) (*Service, error) {
	if !config.Enabled {
		snapshot := baseSnapshot(ModeDisabled, "")
		return &Service{snapshot: snapshot}, nil
	}
	identity, err := materialization.SecretIdentityForCell(config.CellKey)
	if err != nil || nilInterface(config.Cycle) {
		return nil, ErrConfig
	}
	if config.Interval == 0 {
		config.Interval = defaultInterval
	}
	if config.AttemptTimeout == 0 {
		config.AttemptTimeout = defaultAttemptTimeout
	}
	if !boundedDuration(config.Interval, minimumInterval, maximumInterval) ||
		!boundedDuration(config.AttemptTimeout, minimumAttemptTimeout, maximumAttemptTimeout) {
		return nil, ErrConfig
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Service{
		enabled:        true,
		cellKey:        identity.CellKey,
		cycle:          config.Cycle,
		interval:       config.Interval,
		attemptTimeout: config.AttemptTimeout,
		now:            config.Now,
		logger:         logger,
		snapshot:       baseSnapshot(ModeShadow, identity.CellKey),
	}, nil
}

func (service *Service) Enabled() bool {
	return service != nil && service.enabled && service.cellKey != "" && service.cycle != nil &&
		service.interval > 0 && service.attemptTimeout > 0 && service.now != nil && service.logger != nil
}

func (service *Service) String() string {
	if service == nil {
		return "backup materializer agent <nil>"
	}
	return "backup materializer agent [REDACTED]"
}

func (service *Service) GoString() string { return service.String() }

// ReconcileOnce serializes one bounded cycle. Source failures represented by
// a valid reconciler Status are successful evaluations; only cancellation or
// an invalid cycle result becomes a supervisor failure.
func (service *Service) ReconcileOnce(ctx context.Context) error {
	if service == nil || ctx == nil {
		return ErrConfig
	}
	if !service.Enabled() {
		return ErrDisabled
	}
	service.attemptMu.Lock()
	defer service.attemptMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	now := canonicalNow(service.now())
	if now.IsZero() {
		return ErrConfig
	}
	service.beginAttempt(now)
	attemptCtx, cancel := context.WithTimeout(ctx, service.attemptTimeout)
	status, err := service.cycle.ReconcileOnce(attemptCtx)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		service.failAttempt(FailureCanceled)
		return ctx.Err()
	}
	if attemptErr != nil {
		service.failAttempt(FailureAttemptTimeout)
		return ErrService
	}
	if err != nil {
		service.failAttempt(FailureCycleError)
		return ErrService
	}
	if reconciler.ValidateStatus(status) != nil || status.CellKey != service.cellKey {
		service.failAttempt(FailureCycleInvalid)
		return ErrService
	}
	service.succeedAttempt(status)
	return nil
}

// Run immediately evaluates the cell, then waits one full interval after each
// completed attempt. Attempts never overlap, and a cell-local failure cannot
// terminate the supervisor or affect another process.
func (service *Service) Run(ctx context.Context) error {
	if service == nil || ctx == nil {
		return ErrConfig
	}
	if !service.Enabled() {
		<-ctx.Done()
		return nil
	}
	for {
		if err := service.ReconcileOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			snapshot := service.Snapshot()
			service.logger.Printf("backup materializer cycle failed cell=%s code=%s attempts=%d", service.cellKey, snapshot.FailureCode, snapshot.AttemptCount)
		}
		timer := time.NewTimer(service.interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
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
	return snapshot
}

// Handler exposes only credential-free operational reads. The command binds
// it to explicit loopback and provides no input, command, or mutation route.
func (service *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writeSnapshot(writer, http.StatusOK, service.Snapshot())
	})
	mux.HandleFunc("GET /readyz", func(writer http.ResponseWriter, _ *http.Request) {
		snapshot := service.Snapshot()
		statusCode := http.StatusOK
		if !snapshot.Ready {
			statusCode = http.StatusServiceUnavailable
		}
		writeSnapshot(writer, statusCode, snapshot)
	})
	mux.HandleFunc("GET /v1/status", func(writer http.ResponseWriter, _ *http.Request) {
		writeSnapshot(writer, http.StatusOK, service.Snapshot())
	})
	return mux
}

func ValidateSnapshot(snapshot Snapshot) error {
	if snapshot.APIVersion != APIVersion || snapshot.Kind != Kind || snapshot.DeleteAllowed ||
		!snapshot.ObservationOnly || snapshot.ExecutionAllowed || snapshot.ProductionMutationAllowed ||
		snapshot.Digest != DigestSnapshot(snapshot) {
		return ErrService
	}
	if snapshot.Mode == ModeDisabled {
		if snapshot.CellKey != "" || snapshot.Ready || snapshot.Reconciling || snapshot.Converged ||
			snapshot.LastKnownGoodServing || snapshot.Blocked || snapshot.Retryable || snapshot.MutationCandidate ||
			snapshot.AttemptCount != 0 || snapshot.ConsecutiveFailures != 0 || snapshot.LastAttemptAt != nil ||
			snapshot.LastEvaluationAt != nil || snapshot.FailureCode != "" || snapshot.CurrentStatus != nil ||
			snapshot.LastValidStatus != nil {
			return ErrService
		}
		return nil
	}
	if snapshot.Mode != ModeShadow {
		return ErrService
	}
	if _, err := materialization.SecretIdentityForCell(snapshot.CellKey); err != nil ||
		!validOptionalTime(snapshot.LastAttemptAt) || !validOptionalTime(snapshot.LastEvaluationAt) ||
		!validStatusForCell(snapshot.CurrentStatus, snapshot.CellKey) ||
		!validStatusForCell(snapshot.LastValidStatus, snapshot.CellKey) || !validFailureCode(snapshot.FailureCode) {
		return ErrService
	}
	if (snapshot.LastValidStatus == nil) != (snapshot.LastEvaluationAt == nil) ||
		(snapshot.LastValidStatus != nil && !snapshot.LastValidStatus.EvaluatedAt.Equal(*snapshot.LastEvaluationAt)) {
		return ErrService
	}
	if snapshot.AttemptCount == 0 {
		if snapshot.Reconciling || snapshot.ConsecutiveFailures != 0 || snapshot.LastAttemptAt != nil ||
			snapshot.LastEvaluationAt != nil || snapshot.FailureCode != "" || snapshot.CurrentStatus != nil ||
			snapshot.LastValidStatus != nil || snapshot.Ready || snapshot.Converged ||
			snapshot.LastKnownGoodServing || snapshot.Blocked || snapshot.Retryable || snapshot.MutationCandidate {
			return ErrService
		}
		return nil
	}
	if snapshot.LastAttemptAt == nil {
		return ErrService
	}
	if snapshot.FailureCode != "" {
		if snapshot.Reconciling || snapshot.CurrentStatus != nil || snapshot.ConsecutiveFailures == 0 ||
			snapshot.Ready || snapshot.Converged || snapshot.LastKnownGoodServing || snapshot.Blocked ||
			snapshot.Retryable || snapshot.MutationCandidate {
			return ErrService
		}
		return nil
	}
	if snapshot.Reconciling {
		return validSnapshotFlags(snapshot)
	}
	if snapshot.CurrentStatus == nil || snapshot.LastValidStatus == nil || snapshot.LastEvaluationAt == nil ||
		snapshot.ConsecutiveFailures != 0 || snapshot.CurrentStatus.Digest != snapshot.LastValidStatus.Digest {
		return ErrService
	}
	return validSnapshotFlags(snapshot)
}

func DigestSnapshot(snapshot Snapshot) string {
	snapshot.Digest = ""
	document, err := json.Marshal(snapshot)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(document)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func (snapshot Snapshot) String() string {
	return fmt.Sprintf(
		"BackupMaterializerAgentStatus{mode=%q cell=%q ready=%t reconciling=%t failure=%q attempts=%d executionAllowed=false digest=%q}",
		snapshot.Mode, snapshot.CellKey, snapshot.Ready, snapshot.Reconciling, snapshot.FailureCode,
		snapshot.AttemptCount, snapshot.Digest,
	)
}

func (snapshot Snapshot) GoString() string { return snapshot.String() }

func (service *Service) beginAttempt(now time.Time) {
	service.stateMu.Lock()
	defer service.stateMu.Unlock()
	service.snapshot.AttemptCount++
	service.snapshot.Reconciling = true
	service.snapshot.LastAttemptAt = timePointer(now)
	service.snapshot.FailureCode = ""
	service.snapshot.Digest = DigestSnapshot(service.snapshot)
}

func (service *Service) failAttempt(code string) {
	service.stateMu.Lock()
	defer service.stateMu.Unlock()
	service.snapshot.Reconciling = false
	service.snapshot.Ready = false
	service.snapshot.Converged = false
	service.snapshot.LastKnownGoodServing = false
	service.snapshot.Blocked = false
	service.snapshot.Retryable = false
	service.snapshot.MutationCandidate = false
	service.snapshot.ConsecutiveFailures++
	service.snapshot.FailureCode = code
	service.snapshot.CurrentStatus = nil
	service.snapshot.Digest = DigestSnapshot(service.snapshot)
}

func (service *Service) succeedAttempt(status reconciler.Status) {
	service.stateMu.Lock()
	defer service.stateMu.Unlock()
	current := cloneStatus(status)
	lastValid := cloneStatus(status)
	service.snapshot.Reconciling = false
	service.snapshot.Ready = status.Ready
	service.snapshot.Converged = status.Converged
	service.snapshot.LastKnownGoodServing = status.LastKnownGoodServing
	service.snapshot.Blocked = status.Blocked
	service.snapshot.Retryable = status.Retryable
	service.snapshot.MutationCandidate = status.MutationCandidate
	service.snapshot.ConsecutiveFailures = 0
	service.snapshot.LastEvaluationAt = timePointer(status.EvaluatedAt)
	service.snapshot.FailureCode = ""
	service.snapshot.CurrentStatus = &current
	service.snapshot.LastValidStatus = &lastValid
	service.snapshot.Digest = DigestSnapshot(service.snapshot)
}

func baseSnapshot(mode, cellKey string) Snapshot {
	snapshot := Snapshot{
		APIVersion:                APIVersion,
		Kind:                      Kind,
		Mode:                      mode,
		CellKey:                   cellKey,
		ObservationOnly:           true,
		DeleteAllowed:             false,
		ExecutionAllowed:          false,
		ProductionMutationAllowed: false,
	}
	snapshot.Digest = DigestSnapshot(snapshot)
	return snapshot
}

func writeSnapshot(writer http.ResponseWriter, statusCode int, snapshot Snapshot) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "private, no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(statusCode)
	_ = json.NewEncoder(writer).Encode(snapshot)
}

func validSnapshotFlags(snapshot Snapshot) error {
	if snapshot.CurrentStatus == nil {
		if snapshot.Ready || snapshot.Converged || snapshot.LastKnownGoodServing || snapshot.Blocked ||
			snapshot.Retryable || snapshot.MutationCandidate {
			return ErrService
		}
		return nil
	}
	status := snapshot.CurrentStatus
	if snapshot.Ready != status.Ready || snapshot.Converged != status.Converged ||
		snapshot.LastKnownGoodServing != status.LastKnownGoodServing || snapshot.Blocked != status.Blocked ||
		snapshot.Retryable != status.Retryable || snapshot.MutationCandidate != status.MutationCandidate {
		return ErrService
	}
	return nil
}

func validStatusForCell(status *reconciler.Status, cellKey string) bool {
	return status == nil || (reconciler.ValidateStatus(*status) == nil && status.CellKey == cellKey)
}

func validFailureCode(code string) bool {
	switch code {
	case "", FailureAttemptTimeout, FailureCanceled, FailureCycleInvalid, FailureCycleError:
		return true
	default:
		return false
	}
}

func validOptionalTime(value *time.Time) bool {
	return value == nil || !value.IsZero() && value.Location() == time.UTC && value.Nanosecond() == 0
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	if snapshot.LastAttemptAt != nil {
		value := *snapshot.LastAttemptAt
		snapshot.LastAttemptAt = &value
	}
	if snapshot.LastEvaluationAt != nil {
		value := *snapshot.LastEvaluationAt
		snapshot.LastEvaluationAt = &value
	}
	if snapshot.CurrentStatus != nil {
		value := cloneStatus(*snapshot.CurrentStatus)
		snapshot.CurrentStatus = &value
	}
	if snapshot.LastValidStatus != nil {
		value := cloneStatus(*snapshot.LastValidStatus)
		snapshot.LastValidStatus = &value
	}
	return snapshot
}

func cloneStatus(status reconciler.Status) reconciler.Status {
	if status.Decision != nil {
		decision := *status.Decision
		status.Decision = &decision
	}
	return status
}

func timePointer(value time.Time) *time.Time {
	copyValue := value
	return &copyValue
}

func canonicalNow(now time.Time) time.Time {
	return now.UTC().Truncate(time.Second)
}

func boundedDuration(value, minimum, maximum time.Duration) bool {
	return value >= minimum && value <= maximum && value%time.Millisecond == 0
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
