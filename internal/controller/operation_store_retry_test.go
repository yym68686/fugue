package controller

import (
	"context"
	"errors"
	"fugue/internal/model"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestOperationStoreRetryRetainsFinalizationUntilDatabaseReturns(t *testing.T) {
	calls, pending := 0, 0
	err := retryOperationStore(context.Background(), time.Millisecond, func(int) { pending++ }, func() error {
		calls++
		if calls <= 3 {
			return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		}
		return nil
	})
	if err != nil || calls != 4 || pending != 3 {
		t.Fatalf("calls=%d pending=%d err=%v", calls, pending, err)
	}
}
func TestOperationStoreRetryStopsOnFenceAndLeadershipLoss(t *testing.T) {
	for _, err := range []error{store.ErrConflict, store.ErrNotFound, store.ErrInvalidInput, &pgconn.PgError{Code: "23505"}} {
		calls := 0
		got := retryOperationStore(context.Background(), time.Millisecond, nil, func() error { calls++; return err })
		if !errors.Is(got, err) || calls != 1 {
			t.Fatalf("non-transient write retried: %v", got)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := retryOperationStore(ctx, time.Millisecond, func(int) { cancel() }, func() error { calls++; return &pgconn.PgError{Code: "57P01"} })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal(err, calls)
	}
}
func TestRolloutPersistenceTimeoutIsNotWorkloadFailure(t *testing.T) {
	calls := 0
	err := retryRolloutPersistenceObservation(context.Background(), func() error {
		calls++
		if calls == 1 {
			return errors.Join(errOperationPersistenceInterrupted, context.DeadlineExceeded)
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatal(err, calls)
	}
	calls = 0
	failure := errors.New("pod failed")
	if err = retryRolloutPersistenceObservation(context.Background(), func() error { calls++; return failure }); err != failure || calls != 1 {
		t.Fatal(err, calls)
	}
}
func TestOperationExecutionKeyUsesDurableTimestampPrecision(t *testing.T) {
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)
	stored := stamp.Truncate(time.Microsecond).In(time.FixedZone("offset", 3600))
	if operationExecutionKey(model.Operation{StartedAt: &stamp}) != operationExecutionKey(model.Operation{StartedAt: &stored}) {
		t.Fatal("durable attempt changed")
	}
}

func TestFinalizationRetriesLostCommitResponseWithoutReplayingOperation(t *testing.T) {
	s, _, op := recoveryFixture(t)
	writes, commits := 0, 0
	result, err := s.finalizeManagedOperation(context.Background(), op, model.OperationStatusCompleted, func() (model.Operation, error) {
		writes++
		result, err := s.Store.CompleteManagedOperationGuarded(op, nil, "", "finished", nil, nil, nil)
		if err == nil {
			commits++
			return model.Operation{}, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
		}
		return result, err
	})
	if err != nil || result.Status != model.OperationStatusCompleted || writes != 2 || commits != 1 {
		t.Fatal(result.Status, err, writes, commits)
	}
}
func TestFinalizationCannotResurrectCancelledTaskAfterOutage(t *testing.T) {
	s, _, op := recoveryFixture(t)
	calls := 0
	_, err := s.finalizeManagedOperation(context.Background(), op, model.OperationStatusFailed, func() (model.Operation, error) {
		calls++
		if calls == 1 {
			if _, e := s.Store.CancelOperation(op.ID, "user cancelled"); e != nil {
				t.Fatal(e)
			}
			return model.Operation{}, &pgconn.PgError{Code: "57P01"}
		}
		return s.Store.FailManagedOperationGuarded(op, "late failure")
	})
	current, _ := s.Store.GetOperation(op.ID)
	if !errors.Is(err, errOperationNoLongerActive) || calls != 2 || current.Status != model.OperationStatusCanceled {
		t.Fatal(err, calls, current.Status)
	}
}
