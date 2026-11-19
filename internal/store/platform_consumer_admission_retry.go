package store

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// Mark only lock failures before dependency checks or apply. Do not retry a
// general SQLSTATE, a proof conflict, a mutation error or an ambiguous commit.
type consumerAdmissionLockBusy struct{ error }

func (err *consumerAdmissionLockBusy) Unwrap() error { return err.error }
func (err *consumerAdmissionLockBusy) SQLState() string {
	// Both marking sites require the same lock-unavailable classification.
	return "55P03"
}

// The caller owns the usual dependency proof and pin selection. Retry their
// entire admission once in a new transaction, only after the first rollback
// has released every lock and returned its connection. All current witnesses,
// selector/epoch/keyring checks and business checks still run at acceptance.
func retryConsumerAdmissionLock(ctx context.Context, admit func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, compilerInputAdmissionTimeout)
	defer cancel()
	err := admit(ctx)
	var busy *consumerAdmissionLockBusy
	if !errors.As(err, &busy) || ctx.Err() != nil {
		return err
	}
	// Offset concurrent consumers without multiplying database load while a
	// publisher still holds the lock. The normal per-lock limits are unchanged.
	timer := time.NewTimer(time.Duration(35+rand.Intn(51)) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The synchronous observer records the first failure on an attempt.
	// Once a new attempt actually begins, let it report its own outcome.
	if observed, _ := ctx.Value(platformInputReadObservationKey{}).(*PlatformInputReadFailure); observed != nil {
		*observed = PlatformInputReadFailure{}
	}
	return admit(ctx)
}
