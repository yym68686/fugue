package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"fugue/internal/schemamigrate"
)

// compilerInputPinBinding is an exact durable pin identity, not an operator's
// declaration that bytes were verified. The database record must have been
// promoted after complete independent local readback by the pin protocol.
type compilerInputPinBinding struct {
	Key    string
	Token  int64
	Digest string
}

// lockCompilerInputVerifiedPins is the transaction-side half of admission.
// Call it BEFORE taking artifact, release, consumer, task or input row locks,
// and commit the protected activation in THIS transaction. Restoring and full
// byte/signature verification belong outside this short transaction.
// Any error requires rollback of the whole transaction, never continuing a
// partially checked activation. A successful caller must install the activation
// retention root before commit; this lock is not a lifetime pin ownership API.
//
// This primitive is deliberately not release/rollback/consumer admission by
// itself: the caller still has to derive and bind the COMPLETE dependency
// closure and enforce signature, authorization, lease and retention policy.
// A supplied list, an empty list, or a local pin cannot grant that authority.
// Task claim now supplies those bindings; other activation paths remain work.
func lockCompilerInputVerifiedPins(ctx context.Context, tx *sql.Tx, bindings []compilerInputPinBinding) error {
	if tx == nil || len(bindings) == 0 || len(bindings) > 32 {
		return ErrInvalidInput
	}
	ordered := append([]compilerInputPinBinding(nil), bindings...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Key < ordered[j].Key })
	for i, binding := range ordered {
		if !compilerInputPinKeyPattern.MatchString(binding.Key) || binding.Token < 1 || !inputDigestPattern.MatchString(binding.Digest) ||
			i > 0 && ordered[i-1].Key == binding.Key {
			return ErrInvalidInput
		}
	}
	epoch, err := lockCompilerInputAdmissionBarrier(ctx, tx)
	if err != nil {
		return err
	}
	if err := schemamigrate.VerifyCompilerInputLocalPins(ctx, tx); err != nil {
		return err
	}
	return checkCompilerInputVerifiedPins(ctx, tx, ordered, epoch)
}

// A complete activation may contain more than one transport page of pins.
// Acquire the barriers once, then check every page in the SAME transaction.
// No page is independently committed and no prefix grants admission.
func setCompilerInputAdmissionTimeouts(ctx context.Context, tx *sql.Tx) error {
	// Preserve stricter caller budgets. Zero means unlimited, not zero time.
	// pg_settings.setting is in ms for both of these settings.
	_, err := tx.ExecContext(ctx, `SELECT set_config(name,
 (CASE WHEN setting::bigint=0 THEN limit_ms ELSE LEAST(setting::bigint,limit_ms) END)::text || 'ms',true)
 FROM pg_settings JOIN (VALUES ('lock_timeout',250::bigint),('statement_timeout',5000::bigint)) budget(name,limit_ms) USING(name)`)
	return err
}

func lockCompilerInputAdmissionBarrier(ctx context.Context, tx *sql.Tx) (int64, error) {
	// A repeatable-read snapshot acquired before the barrier could see an old
	// verified label after a completed unpin. Activation uses read-committed,
	// then locks before reading the pin/epoch/representation identity.
	var currentRead bool
	if err := tx.QueryRowContext(ctx, `SELECT current_setting('transaction_isolation')='read committed' AND current_setting('transaction_read_only')='off'`).Scan(&currentRead); err != nil {
		return 0, err
	}
	if !currentRead {
		return 0, fmt.Errorf("pin admission requires a current writable transaction: %w", ErrConflict)
	}
	if err := setCompilerInputAdmissionTimeouts(ctx, tx); err != nil {
		return 0, err
	}
	// Same order as pin registration/maintenance: shared GC, then the archive
	// writer barrier. Shared mode permits other short activations; unpin,
	// re-verification, representation changes and retirement need exclusive.
	// No retries and no network/body I/O occur while these locks are held.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended('fugue.compiler-input-gc.v1',0))`); err != nil {
		return 0, err
	}
	if err := waitCompilerInputAdmissionWriter(ctx, tx); err != nil {
		return 0, err
	}
	return readInputArchiveEpoch(ctx, tx, -1)
}

func checkCompilerInputVerifiedPins(ctx context.Context, tx *sql.Tx, ordered []compilerInputPinBinding, epoch int64) error {
	for _, binding := range ordered {
		var pinned inputManifest
		var token, pinnedEpoch int64
		var state string
		var protected sql.NullString
		var verifiedAt sql.NullTime
		if err := tx.QueryRowContext(ctx, `SELECT token,state,content_hash,source_format,source_location,raw_bytes,chunk_size,chunk_count,source_created_at,source_epoch,protected_content_hash,verified_at
 FROM fugue_platform_input_local_pins WHERE pin_key=$1 FOR SHARE`, binding.Key).
			Scan(&token, &state, &pinned.Digest, &pinned.Format, &pinned.Location, &pinned.Size, &pinned.ChunkSize, &pinned.Count, &pinned.Created, &pinnedEpoch, &protected, &verifiedAt); err != nil {
			return fmt.Errorf("verified local pin unavailable: %w", errors.Join(err, ErrConflict))
		}
		if token != binding.Token || state != "verified" || !verifiedAt.Valid || pinned.Digest != binding.Digest ||
			!protected.Valid || protected.String != binding.Digest || pinnedEpoch != epoch {
			return fmt.Errorf("verified local pin identity or epoch differs: %w", ErrConflict)
		}
		current, err := readInputManifest(ctx, tx, binding.Digest)
		if err != nil {
			return fmt.Errorf("pinned local representation unavailable: %w", errors.Join(err, ErrConflict))
		}
		if current != pinned || validateCompilerInputLocalPinManifest(current, inputMaxBytes) != nil {
			return fmt.Errorf("pinned local representation changed: %w", ErrConflict)
		}
	}
	return ctx.Err()
}
