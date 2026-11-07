package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

// Shared restoration mechanics carry no signing key or activation authority.
// Their caller verifies a signed, purpose-specific ticket before constructing
// this work. The trusted coordinator owns physical/WAL/worker gates and receipts.
type compilerInputPreparationWork struct {
	operation, backend string
	epoch, horizon     int64
	expires            time.Time
	inputs             []taskPreparationManifest
	tuples             []taskPreparationTuple
	limits             CompilerInputPreparationBudget
}

type compilerInputPreparationReceipt struct {
	RawBytes, StoredBytes int64
	Pins                  []CompilerInputLocalPinReport
	Usage                 inputarchive.CopyUsage
}

type CompilerInputPreparationBudget struct {
	MaxInputs      int   `json:"max_inputs"`
	MaxRawBytes    int64 `json:"max_raw_bytes"`
	MaxStoredBytes int64 `json:"max_stored_bytes"`
	MaxGetBytes    int64 `json:"max_get_bytes"`
	MaxGetRequests int64 `json:"max_get_requests"`
	MaxSeconds     int   `json:"max_seconds"`
}

func (b CompilerInputPreparationBudget) validate() error {
	if b.MaxInputs < 1 || b.MaxInputs > 32 || b.MaxRawBytes < 1 || b.MaxRawBytes > 128<<20 ||
		b.MaxStoredBytes < 1 || b.MaxStoredBytes > 128<<20 || b.MaxGetBytes < 1 || b.MaxGetBytes > 448<<20 ||
		b.MaxGetRequests < 1 || b.MaxGetRequests > 32768 || b.MaxSeconds < 1 || b.MaxSeconds > 540 {
		return ErrInvalidInput
	}
	return nil
}

func (w compilerInputPreparationWork) check(ctx context.Context, db *sql.DB, restored bool, pins []CompilerInputLocalPinReport, target func(context.Context, *sql.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	if err := schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return err
	}
	epoch, err := lockCompilerInputAdmissionBarrier(ctx, tx)
	if err != nil || epoch != w.epoch || !time.Now().Before(w.expires) {
		return errors.Join(err, ErrConflict)
	}
	var horizon int64
	if err := tx.QueryRowContext(ctx, `SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint`).Scan(&horizon); err != nil || horizon < w.horizon || horizon-w.horizon >= 1<<31 {
		return errors.Join(err, ErrConflict)
	}
	for _, v := range w.tuples {
		expected := compilerInputActivationTuple{v.Table, v.Key, v.Xmin, v.CTID, v.Relation, v.Filenode}
		current := expected
		if err := readCompilerInputActivationTuple(ctx, tx, &current, true); err != nil || current != expected {
			return errors.Join(err, ErrConflict)
		}
	}
	for _, v := range w.inputs {
		m, err := readInputManifest(ctx, tx, v.Digest)
		expected := v.manifest()
		if restored && expected.Location == "legacy" && (expected.Format == inputFormatV1 || expected.Format == inputFormatCodecV2) {
			expected.Location = "hot"
		}
		if err != nil || m != expected {
			return errors.Join(err, ErrConflict)
		}
	}
	if target != nil {
		if err := target(ctx, tx); err != nil {
			return err
		}
	}
	if restored {
		if len(pins) != len(w.inputs) {
			return ErrConflict
		}
		bindings := make([]compilerInputPinBinding, 0, len(pins))
		for i, pin := range pins {
			if !pin.Verified || pin.Digest != w.inputs[i].Digest || pin.Key != taskPreparationPinKey(w.operation, pin.Digest) {
				return ErrConflict
			}
			bindings = append(bindings, compilerInputPinBinding{Key: pin.Key, Token: pin.Token, Digest: pin.Digest})
		}
		if err := checkCompilerInputVerifiedPins(ctx, tx, bindings, epoch); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (w compilerInputPreparationWork) restore(ctx context.Context, db *sql.DB, objects inputarchive.Objects, report *compilerInputPreparationReceipt, target func(context.Context, *sql.Tx) error) error {
	if enabled, err := inputarchive.MigrationEnabledFromEnv(); err != nil || !enabled {
		return ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(w.limits.MaxSeconds)*time.Second)
	defer cancel()
	ctx, cancelExpiry := context.WithDeadline(ctx, w.expires)
	defer cancelExpiry()
	if err := w.check(ctx, db, false, nil, target); err != nil {
		return err
	}
	budget, err := inputarchive.NewBudgetObjects(objects, taskPreparationNoPut{}, inputarchive.CopyBudget{MaxPutBytes: 1, MaxPutRequests: 1, MaxGetBytes: w.limits.MaxGetBytes, MaxGetRequests: w.limits.MaxGetRequests, BytesPerSecond: 2 << 20})
	if err != nil {
		return err
	}
	defer func() { report.Usage = budget.Usage() }()
	for _, m := range w.inputs {
		report.RawBytes += m.RawBytes
		if m.Location == "legacy" && (m.Format == inputFormatV1 || m.Format == inputFormatCodecV2) {
			if report.StoredBytes >= w.limits.MaxStoredBytes {
				return ErrConflict
			}
			restored, err := restoreCompilerInputArchiveHot(ctx, db, budget, w.backend, CompilerInputArchiveHotOptions{Digest: m.Digest, Apply: true, MaxStoredBytes: w.limits.MaxStoredBytes - report.StoredBytes})
			report.StoredBytes += restored.StoredBytes
			if err != nil {
				return err
			}
		}
		pin, err := runCompilerInputLocalPin(ctx, db, CompilerInputLocalPinOptions{Key: taskPreparationPinKey(w.operation, m.Digest), Digest: m.Digest, MaxRawBytes: m.RawBytes, Apply: true})
		report.Pins = append(report.Pins, pin)
		if err != nil || !pin.Verified {
			return errors.Join(err, ErrConflict)
		}
	}
	// Only fresh trusted signature/closure confirmation may say Prepared.
	return w.check(ctx, db, true, report.Pins, target)
}
