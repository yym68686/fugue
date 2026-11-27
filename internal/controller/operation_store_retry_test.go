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
			if _, e := cancelFixtureOperation(s.Store, op, "user cancelled"); e != nil {
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
