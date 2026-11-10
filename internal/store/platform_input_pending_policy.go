package store

import (
	"context"
	"database/sql"
	"math"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

// PendingRootPolicySnapshot binds the complete policy, including an empty
// installed table, to its native physical identity. Zero revision means no row,
// never an absent/unverified schema. PolicyJSON is canonical and immutable.
type PendingRootPolicySnapshot struct {
	Revision, Relation, Filenode int64
	Xmin, CTID, PolicyJSON       string
}

type PendingRootPolicyChange struct {
	Before     PendingRootPolicySnapshot
	PolicyJSON string
}

// Only a trusted coordinator source may choose a complete policy. This is not
// a body-decoded operator assertion or a Pod's request to authorize itself.
// The witness must recheck current deployment/trust/revocation authority, and
// bind the same complete desired policy and native parent on every check.
type PendingRootPolicySource interface {
	PreparePendingRootPolicy(context.Context, PendingRootPolicySnapshot) (PendingRootPolicyWitness, error)
}

type PendingRootPolicyWitness interface {
	PendingRootPolicyChange() PendingRootPolicyChange
	CheckPendingRootPolicy(context.Context) error
}

// Reservation includes the native policy transaction's physical/WAL/temp,
// bounded-wait growth and mandatory failure/exit costs. Merely configuring a
// source, a lease/count cap or an observation cannot supply this reservation.
type PendingRootPolicyAdmission interface {
	ReservePendingRootPolicy(context.Context, PendingRootPolicyChange) (PendingRootPolicyReservation, error)
}

type PendingRootPolicyReservation interface {
	CheckPendingRootPolicyReservation(context.Context) error
	RecordPendingRootPolicyOutcome(inputarchive.PendingRootOutcome)
}

type PendingRootPolicyCoordinator struct {
	store     *Store
	source    PendingRootPolicySource
	admission PendingRootPolicyAdmission
}

// NewPendingRootPolicyCoordinator installs no schema, seed or renewer, changes
// no process configuration and enables no producer. Its only policy source is
// independently trusted configuration. There is deliberately no public HTTP
// policy mutation or ordinary-deployment caller.
func NewPendingRootPolicyCoordinator(s *Store, source PendingRootPolicySource, admission PendingRootPolicyAdmission) (*PendingRootPolicyCoordinator, error) {
	if s == nil || s.db == nil || source == nil || admission == nil {
		return nil, ErrConflict
	}
	return &PendingRootPolicyCoordinator{s, source, admission}, nil
}

// ReadPendingRootAuthorityPolicy returns a bounded native parent for the
// coordinator, not permission to change it or evidence of full external H.
func (s *Store) ReadPendingRootAuthorityPolicy(ctx context.Context) (PendingRootPolicySnapshot, error) {
	if s == nil || s.db == nil || ctx == nil {
		return PendingRootPolicySnapshot{}, ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	tx, err := beginPendingRootPolicyTx(ctx, s.db, true)
	if err != nil {
		return PendingRootPolicySnapshot{}, err
	}
	defer tx.Rollback()
	state, err := readPendingRootPolicySnapshot(ctx, tx, false)
	if err != nil {
		return PendingRootPolicySnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return PendingRootPolicySnapshot{}, err
	}
	return state, nil
}

// Reconcile performs a single explicitly admitted CAS, with one outer deadline
// shared by source checks, reservation, SQL waits and commit. No retries or
// lease extension occur here. Exact already-current policies are acknowledged
// without generating a revision; lost ACK can be reconciled by reading current
// state, never by reusing a receipt after a later revocation.
func (c *PendingRootPolicyCoordinator) Reconcile(ctx context.Context) (PendingRootPolicySnapshot, error) {
	if c == nil || c.store == nil || c.source == nil || c.admission == nil || ctx == nil {
		return PendingRootPolicySnapshot{}, ErrConflict
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	before, err := c.store.ReadPendingRootAuthorityPolicy(ctx)
	if err != nil {
		return PendingRootPolicySnapshot{}, err
	}
	witness, err := c.source.PreparePendingRootPolicy(ctx, before)
	if err != nil || witness == nil {
		return PendingRootPolicySnapshot{}, ErrConflict
	}
	change := witness.PendingRootPolicyChange()
	if change.Before != before || before.Revision == math.MaxInt64 {
		return PendingRootPolicySnapshot{}, ErrConflict
	}
	policy, err := inputarchive.DecodePendingRootAuthorityPolicy([]byte(change.PolicyJSON))
	if err != nil || witness.CheckPendingRootPolicy(ctx) != nil || witness.PendingRootPolicyChange() != change {
		return PendingRootPolicySnapshot{}, ErrConflict
	}
	reservation, err := c.admission.ReservePendingRootPolicy(ctx, change)
	if err != nil || reservation == nil {
		return PendingRootPolicySnapshot{}, ErrConflict
	}
	outcome := inputarchive.PendingRootNotAttempted
	defer func() { reservation.RecordPendingRootPolicyOutcome(outcome) }()
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if witness.PendingRootPolicyChange() != change || witness.CheckPendingRootPolicy(ctx) != nil || witness.PendingRootPolicyChange() != change {
			return ErrConflict
		}
		return reservation.CheckPendingRootPolicyReservation(ctx)
	}
	if err := check(); err != nil {
		return PendingRootPolicySnapshot{}, err
	}
	// A transaction failure is conservatively completion-not-confirmed. Only
	// the reservation owner can reconcile/release costs after uncertain exit.
	outcome = inputarchive.PendingRootCompletionNotConfirmed
	tx, err := beginPendingRootPolicyTx(ctx, c.store.db, false)
	if err != nil {
		return PendingRootPolicySnapshot{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fugue.pending-root-policy.v1',0))`); err != nil {
		return PendingRootPolicySnapshot{}, err
	}
	current, err := readPendingRootPolicySnapshot(ctx, tx, true)
	if err != nil || current != before {
		return PendingRootPolicySnapshot{}, ErrConflict
	}
	if change.PolicyJSON != before.PolicyJSON {
		var revision int64
		if before.Revision == 0 {
			err = tx.QueryRowContext(ctx, `INSERT INTO fugue_platform_input_pending_authorities(singleton,revision,policy_json) VALUES(TRUE,1,$1) RETURNING revision`, change.PolicyJSON).Scan(&revision)
		} else {
			err = tx.QueryRowContext(ctx, `UPDATE fugue_platform_input_pending_authorities SET revision=revision+1,policy_json=$1
 WHERE singleton AND revision=$2 AND policy_json=$3 AND xmin::text=$4 AND ctid::text=$5 RETURNING revision`, change.PolicyJSON, before.Revision, before.PolicyJSON, before.Xmin, before.CTID).Scan(&revision)
		}
		if err != nil || revision != before.Revision+1 {
			return PendingRootPolicySnapshot{}, ErrConflict
		}
	}
	// The native row lock is held through commit, serializing with the final
	// SHARE lock of every retention operation, including other API processes.
	after, err := readPendingRootPolicySnapshot(ctx, tx, true)
	if err != nil || after.PolicyJSON != change.PolicyJSON {
		return PendingRootPolicySnapshot{}, ErrConflict
	}
	if err := check(); err != nil {
		return PendingRootPolicySnapshot{}, err
	}
	if len(policy.Leases) != 0 {
		expiry := policy.Leases[0].ExpiresUnix
		for _, lease := range policy.Leases[1:] {
			if lease.ExpiresUnix < expiry {
				expiry = lease.ExpiresUnix
			}
		}
		if err := checkPendingRootLeaseTime(ctx, tx, expiry); err != nil {
			return PendingRootPolicySnapshot{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return PendingRootPolicySnapshot{}, err
	}
	outcome = inputarchive.PendingRootCommitted
	return after, nil
}

func beginPendingRootPolicyTx(ctx context.Context, db *sql.DB, readOnly bool) (*sql.Tx, error) {
	isolation := sql.LevelReadCommitted
	if readOnly {
		isolation = sql.LevelRepeatableRead
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: isolation, ReadOnly: readOnly})
	if err != nil {
		return nil, err
	}
	for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	if err := schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func readPendingRootPolicySnapshot(ctx context.Context, tx *sql.Tx, lock bool) (PendingRootPolicySnapshot, error) {
	var state PendingRootPolicySnapshot
	if err := schemamigrate.VerifyCompilerInputPendingAuthorities(ctx, tx); err != nil {
		return state, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT 'fugue_platform_input_pending_authorities'::regclass::oid::bigint,pg_relation_filenode('fugue_platform_input_pending_authorities'::regclass)`).Scan(&state.Relation, &state.Filenode); err != nil {
		return state, err
	}
	query := `SELECT singleton,revision,left(policy_json,65537),xmin::text,ctid::text FROM fugue_platform_input_pending_authorities LIMIT 2`
	if lock {
		query += ` FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return state, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var singleton bool
		if count > 1 {
			return state, ErrConflict
		}
		if err := rows.Scan(&singleton, &state.Revision, &state.PolicyJSON, &state.Xmin, &state.CTID); err != nil {
			return state, err
		}
		if !singleton || state.Revision <= 0 {
			return state, ErrConflict
		}
		if _, err := inputarchive.DecodePendingRootAuthorityPolicy([]byte(state.PolicyJSON)); err != nil {
			return state, ErrConflict
		}
	}
	return state, rows.Err()
}
