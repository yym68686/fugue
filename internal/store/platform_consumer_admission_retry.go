package store

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// Mark only lock failures before dependency checks or apply. Do not retry a
// general SQLSTATE, a proof conflict, a mutation error or an ambiguous commit.
type preApplyAdmissionLockBusy struct{ error }

func (err *preApplyAdmissionLockBusy) Unwrap() error { return err.error }
func (err *preApplyAdmissionLockBusy) SQLState() string {
	// Only pre-apply lock failures have this classification.
	return "55P03"
}

// Call only for explicitly identified pre-apply locks. A lock error from
// business validation, a write, or commit must not be replayed.
func isAdmissionLockTimeout(err error) bool {
	var pgError interface{ SQLState() string }
	return errors.As(err, &pgError) && pgError.SQLState() == "55P03"
}

func markPreApplyAdmissionLockBusy(err error) error {
	if isAdmissionLockTimeout(err) {
		return &preApplyAdmissionLockBusy{err}
	}
	return err
}

// The caller owns the usual dependency proof and pin selection. Retry their
// entire admission once in a new transaction, only after the first rollback
// has released every lock and returned its connection. All current witnesses,
// selector/epoch/keyring checks and business checks still run at acceptance.
func retryConsumerAdmissionLock(ctx context.Context, admit func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, compilerInputAdmissionTimeout)
	defer cancel()
	return retryPreApplyAdmissionLock(ctx, admit, func(err error) bool {
		var busy *preApplyAdmissionLockBusy
		return errors.As(err, &busy)
	}, nil)
}

// The caller supplies the end-to-end deadline. Always reuse it instead of
// granting a new timeout to the second transaction.
func retryPreApplyAdmissionLock(ctx context.Context, admit func(context.Context) error, retryable func(error) bool, onRetry func(error)) error {
	err := admit(ctx)
	if !retryable(err) || ctx.Err() != nil {
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
	if onRetry != nil {
		onRetry(err)
	}
	return admit(ctx)
}
