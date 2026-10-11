package controller

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

var errOperationPersistenceInterrupted = errors.New("operation persistence unavailable during observation")

func operationStoreTransient(err error) bool {
	if err == nil || errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidInput) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	var network net.Error
	if errors.As(err, &network) {
		return true
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return strings.HasPrefix(pg.Code, "08") || pg.Code == "57P01" || pg.Code == "57P02" || pg.Code == "57P03" || pg.Code == "40001" || pg.Code == "40P01" || pg.Code == "53300" || pg.Code == "53200"
	}
	return false
}

// Retry persistence, never the operation's side effects. The leader context
// cancels ownership. Each Store call already has a bounded DB timeout; backoff
// is capped so restoration does not require another controller restart.
func retryOperationStore(ctx context.Context, delay time.Duration, pending func(int), call func() error) error {
	if delay <= 0 {
		delay = time.Second
	}
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := call()
		if ctx.Err() != nil {
			if operationStoreTransient(err) {
				return fmt.Errorf("%w: %w", errOperationPersistenceInterrupted, ctx.Err())
			}
			return ctx.Err()
		}
		if !operationStoreTransient(err) {
			return err
		}
		if pending != nil {
			pending(attempt)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%w: %w", errOperationPersistenceInterrupted, ctx.Err())
		case <-timer.C:
		}
		if delay < 15*time.Second {
			delay *= 2
			if delay > 15*time.Second {
				delay = 15 * time.Second
			}
		}
	}
}
func (s *Service) operationPersistencePending(id, stage string) func(int) {
	return func(attempt int) {
		if s.Logger != nil {
			s.Logger.Printf("operation %s persistence_pending stage=%s attempt=%d; retaining execution ownership", id, stage, attempt)
		}
	}
}
func (s *Service) ensureOperationStillActiveContext(ctx context.Context, id string) error {
	return retryOperationStore(ctx, time.Second, s.operationPersistencePending(id, "active_check"), func() error { return s.ensureOperationStillActive(id) })
}
func (s *Service) finalizeManagedOperation(ctx context.Context, expected model.Operation, status string, write func() (model.Operation, error)) (model.Operation, error) {
	var result model.Operation
	err := retryOperationStore(ctx, time.Second, s.operationPersistencePending(expected.ID, status), func() error {
		var err error
		result, err = write()
		if errors.Is(err, store.ErrConflict) {
			// A committed transaction can lose its response during a database outage.
			// A same-attempt terminal receipt is success; cancellation or a new attempt
			// is a fence, never permission to overwrite or replay an operation.
			current, readErr := s.Store.GetOperation(expected.ID)
			if readErr != nil {
				return readErr
			}
			if current.Status == status && operationExecutionKey(current) == operationExecutionKey(expected) {
				result = current
				return nil
			}
			return fmt.Errorf("%w: finalization of %s was superseded", errOperationNoLongerActive, expected.ID)
		}
		if errors.Is(err, store.ErrNotFound) {
			return errOperationNoLongerActive
		}
		return err
	})
	return result, err
}

// An expired observation budget during a Store outage says nothing about the
// workload. Resume observation when persistence returns, without rollback/apply.
func retryRolloutPersistenceObservation(ctx context.Context, observe func() error) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := observe()
		if !errors.Is(err, errOperationPersistenceInterrupted) || ctx.Err() != nil {
			return err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
