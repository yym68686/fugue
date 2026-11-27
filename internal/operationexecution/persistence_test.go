package operationexecution

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOperationStoreRetryRetainsFinalizationUntilDatabaseReturns(t *testing.T) {
	temporary := errors.New("database unavailable")
	calls, pending := 0, 0
	err := RetryPersistence(context.Background(), time.Millisecond, func(int) { pending++ }, func() error {
		calls++
		if calls <= 3 {
			return temporary
		}
		return nil
	}, func(err error) bool { return errors.Is(err, temporary) })
	if err != nil || calls != 4 || pending != 3 {
		t.Fatalf("calls=%d pending=%d err=%v", calls, pending, err)
	}
}

func TestRolloutPersistenceTimeoutIsNotWorkloadFailure(t *testing.T) {
	calls := 0
	err := ResumeObservation(context.Background(), func() error {
		calls++
		if calls == 1 {
			return errors.Join(ErrPersistenceInterrupted, context.DeadlineExceeded)
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatal(err, calls)
	}
	calls = 0
	failure := errors.New("pod failed")
	if err = ResumeObservation(context.Background(), func() error { calls++; return failure }); err != failure || calls != 1 {
		t.Fatal(err, calls)
	}
}
