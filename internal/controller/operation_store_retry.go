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
	"fugue/internal/operationexecution"
	"fugue/internal/operationqueue"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

var errOperationPersistenceInterrupted = operationexecution.ErrPersistenceInterrupted

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

func retryOperationStore(ctx context.Context, delay time.Duration, pending func(int), call func() error) error {
	return operationexecution.RetryPersistence(ctx, delay, pending, call, operationStoreTransient)
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
			if current.Status == status && operationqueue.ExecutionKey(current) == operationqueue.ExecutionKey(expected) {
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

func retryRolloutPersistenceObservation(ctx context.Context, observe func() error) error {
	return operationexecution.ResumeObservation(ctx, observe)
}
