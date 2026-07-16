package releaseadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"fugue/internal/releasedomain"
)

// ErrCommandPanic is returned instead of propagating a phase panic across the
// release boundary. Panic values are intentionally not copied into evidence.
var ErrCommandPanic = errors.New("release adapter command panicked")

// ErrTracePanic replaces a panic from an evidence sink so a post-Apply trace
// bug still reaches the exactly-once rollback path.
var ErrTracePanic = errors.New("release transaction trace panicked")

// ErrContextPanic replaces a panic from a Context implementation. A
// post-Apply context panic follows the same exactly-once rollback path as a
// cancellation.
var ErrContextPanic = errors.New("release transaction context panicked")

// Run executes one already-authorized single-domain adapter transaction.
//
// All adapter commands and the trace sink are validated before Prepare.
// Prepare failure never rolls back because no write phase has started. The
// apply-started boundary is crossed immediately before the sole Apply call;
// every later Apply, Verify, context, or trace failure invokes Rollback exactly
// once with an independent bounded context. A rollback failure is joined with
// the primary failure and is never retried.
func Run(
	ctx context.Context,
	rollbackTimeout time.Duration,
	authorization releasedomain.TransactionAuthorization,
	adapter DomainAdapter,
	trace Trace,
) error {
	if isNilInterface(ctx) {
		return fmt.Errorf("release transaction context is nil")
	}
	if rollbackTimeout <= 0 {
		return fmt.Errorf("release transaction rollback timeout must be greater than zero")
	}
	if err := authorization.Verify(); err != nil {
		return fmt.Errorf("release transaction authorization: %w", err)
	}
	adapterSnapshot, err := snapshotDomainAdapter(adapter)
	if err != nil {
		return err
	}
	if adapterSnapshot.domain != authorization.Domain() {
		return fmt.Errorf(
			"release transaction adapter domain %q differs from authorized domain %q",
			adapterSnapshot.domain,
			authorization.Domain(),
		)
	}
	if isNilInterface(trace) {
		return fmt.Errorf("release transaction trace is nil")
	}

	sequence := uint64(0)
	record := func(phase TracePhase, state TraceState) error {
		sequence++
		return recordTrace(trace, newTraceEvent(sequence, authorization, phase, state))
	}
	barrier := func() error {
		return barrierTrace(trace)
	}
	recordTransactionFailure := func(primary error) error {
		if traceErr := record(TracePhaseTransaction, TraceStateFailed); traceErr != nil {
			primary = errors.Join(primary, fmt.Errorf("record transaction failure: %w", traceErr))
		}
		if traceErr := barrier(); traceErr != nil {
			primary = errors.Join(primary, fmt.Errorf("persist failed transaction trace: %w", traceErr))
		}
		return primary
	}
	failBeforeApply := func(primary error, phase TracePhase) error {
		if traceErr := record(phase, TraceStateFailed); traceErr != nil {
			primary = errors.Join(primary, fmt.Errorf("record %s failure: %w", phase, traceErr))
		}
		return recordTransactionFailure(primary)
	}
	rollbackAttempted := false
	rollbackOnce := func(primary error) error {
		// This closure has one call site per forward failure and never loops or
		// retries. Marking the attempt precedes trace I/O so a trace failure
		// cannot suppress or duplicate Rollback.
		if rollbackAttempted {
			return errors.Join(primary, fmt.Errorf("release transaction rollback was already attempted"))
		}
		rollbackAttempted = true
		rollbackErrors := make([]error, 0, 4)
		if traceErr := record(TracePhaseRollback, TraceStateStarted); traceErr != nil {
			rollbackErrors = append(rollbackErrors, fmt.Errorf("record rollback start: %w", traceErr))
		}
		rollbackContext, cancelRollback := context.WithTimeout(context.Background(), rollbackTimeout)
		rollbackErr := runCommand(rollbackContext, adapterSnapshot.commands[PhaseRollback], authorization, PhaseRollback)
		if rollbackErr == nil {
			rollbackErr = rollbackContext.Err()
		}
		cancelRollback()
		if rollbackErr != nil {
			rollbackErr = fmt.Errorf("rollback command failed: %w", rollbackErr)
			rollbackErrors = append(rollbackErrors, rollbackErr)
			if traceErr := record(TracePhaseRollback, TraceStateFailed); traceErr != nil {
				rollbackErrors = append(rollbackErrors, fmt.Errorf("record rollback failure: %w", traceErr))
			}
		} else if traceErr := record(TracePhaseRollback, TraceStateSucceeded); traceErr != nil {
			rollbackErrors = append(rollbackErrors, fmt.Errorf("record rollback success: %w", traceErr))
		}
		if traceErr := record(TracePhaseTransaction, TraceStateFailed); traceErr != nil {
			rollbackErrors = append(rollbackErrors, fmt.Errorf("record transaction failure: %w", traceErr))
		}
		if traceErr := barrier(); traceErr != nil {
			rollbackErrors = append(rollbackErrors, fmt.Errorf("persist failed transaction trace: %w", traceErr))
		}
		all := []error{primary}
		all = append(all, rollbackErrors...)
		return errors.Join(all...)
	}

	if err := record(TracePhaseTransaction, TraceStateStarted); err != nil {
		primary := fmt.Errorf("record transaction start: %w", err)
		if barrierErr := barrier(); barrierErr != nil {
			primary = errors.Join(primary, fmt.Errorf("persist rejected transaction trace: %w", barrierErr))
		}
		return primary
	}
	if err := safeContextErr(ctx); err != nil {
		return recordTransactionFailure(fmt.Errorf("release transaction canceled before prepare: %w", err))
	}
	if err := record(TracePhasePrepare, TraceStateStarted); err != nil {
		return recordTransactionFailure(fmt.Errorf("record prepare start: %w", err))
	}
	if err := runCommand(ctx, adapterSnapshot.commands[PhasePrepare], authorization, PhasePrepare); err != nil {
		return failBeforeApply(fmt.Errorf("prepare command failed: %w", err), TracePhasePrepare)
	}
	if err := safeContextErr(ctx); err != nil {
		return failBeforeApply(fmt.Errorf("release transaction canceled after prepare: %w", err), TracePhasePrepare)
	}
	if err := record(TracePhasePrepare, TraceStateSucceeded); err != nil {
		return recordTransactionFailure(fmt.Errorf("record prepare success: %w", err))
	}
	if err := safeContextErr(ctx); err != nil {
		return recordTransactionFailure(fmt.Errorf("release transaction canceled before apply: %w", err))
	}
	if err := record(TracePhaseApply, TraceStateStarted); err != nil {
		return recordTransactionFailure(fmt.Errorf("record apply start: %w", err))
	}

	// The write boundary begins immediately before this call. From here until
	// the final durable success record, every failure goes through rollbackOnce.
	if err := runCommand(ctx, adapterSnapshot.commands[PhaseApply], authorization, PhaseApply); err != nil {
		primary := fmt.Errorf("apply command failed: %w", err)
		if traceErr := record(TracePhaseApply, TraceStateFailed); traceErr != nil {
			primary = errors.Join(primary, fmt.Errorf("record apply failure: %w", traceErr))
		}
		return rollbackOnce(primary)
	}
	if err := safeContextErr(ctx); err != nil {
		primary := fmt.Errorf("release transaction canceled after apply: %w", err)
		if traceErr := record(TracePhaseApply, TraceStateFailed); traceErr != nil {
			primary = errors.Join(primary, fmt.Errorf("record apply cancellation: %w", traceErr))
		}
		return rollbackOnce(primary)
	}
	if err := record(TracePhaseApply, TraceStateSucceeded); err != nil {
		return rollbackOnce(fmt.Errorf("record apply success: %w", err))
	}
	if err := record(TracePhaseVerify, TraceStateStarted); err != nil {
		return rollbackOnce(fmt.Errorf("record verify start: %w", err))
	}
	if err := runCommand(ctx, adapterSnapshot.commands[PhaseVerify], authorization, PhaseVerify); err != nil {
		primary := fmt.Errorf("verify command failed: %w", err)
		if traceErr := record(TracePhaseVerify, TraceStateFailed); traceErr != nil {
			primary = errors.Join(primary, fmt.Errorf("record verify failure: %w", traceErr))
		}
		return rollbackOnce(primary)
	}
	if err := safeContextErr(ctx); err != nil {
		primary := fmt.Errorf("release transaction canceled after verify: %w", err)
		if traceErr := record(TracePhaseVerify, TraceStateFailed); traceErr != nil {
			primary = errors.Join(primary, fmt.Errorf("record verify cancellation: %w", traceErr))
		}
		return rollbackOnce(primary)
	}
	if err := record(TracePhaseVerify, TraceStateSucceeded); err != nil {
		return rollbackOnce(fmt.Errorf("record verify success: %w", err))
	}
	if err := safeContextErr(ctx); err != nil {
		return rollbackOnce(fmt.Errorf("release transaction canceled after recording verify success: %w", err))
	}
	// Barrier is the final pre-commit durability and identity check. It does
	// not close the trace, so a failure can still record the rollback. The
	// following succeeded event is itself fsynced and is the linearization
	// point: a later cancellation loses the commit race and cannot request a
	// contradictory rollback.
	if err := barrier(); err != nil {
		return rollbackOnce(fmt.Errorf("persist pre-commit transaction trace: %w", err))
	}
	if err := safeContextErr(ctx); err != nil {
		return rollbackOnce(fmt.Errorf("release transaction canceled before the success commit: %w", err))
	}
	if err := record(TracePhaseTransaction, TraceStateSucceeded); err != nil {
		return rollbackOnce(fmt.Errorf("record transaction success: %w", err))
	}
	return nil
}

func recordTrace(trace Trace, event TraceEvent) (resultErr error) {
	defer func() {
		if recover() != nil {
			resultErr = ErrTracePanic
		}
	}()
	return trace.Record(event)
}

func barrierTrace(trace Trace) (resultErr error) {
	defer func() {
		if recover() != nil {
			resultErr = ErrTracePanic
		}
	}()
	return trace.Barrier()
}

func safeContextErr(ctx context.Context) (resultErr error) {
	defer func() {
		if recover() != nil {
			resultErr = ErrContextPanic
		}
	}()
	return ctx.Err()
}

func runCommand(
	ctx context.Context,
	command Command,
	authorization releasedomain.TransactionAuthorization,
	phase Phase,
) (resultErr error) {
	defer func() {
		if recover() != nil {
			resultErr = fmt.Errorf("%s: %w", phase, ErrCommandPanic)
		}
	}()
	return command.Run(ctx, authorization)
}
