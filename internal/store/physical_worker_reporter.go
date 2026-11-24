package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

// Snapshot BEFORE looking at Kubernetes. Accept the result under the same
// transaction as the consumer mutation. A paused process cannot overwrite an
// accepted successor, even if the public Front has since gone A -> B -> A.
// No cluster calls or waits for remote processes under data/publication locks.
type WorkerReporterPredecessor struct {
	node, binding string
	revision      int64
	store         *Store
	started       time.Time
}
type workerReporterAdmission struct {
	WorkerReporterPredecessor
	observed, credential string
}
type workerReporterKey struct{}

var ErrWorkerReporterChanged = fmt.Errorf("physical Worker admission is unavailable or changed: %w", ErrConflict)

func (s *Store) ReadWorkerReporterPredecessor(ctx context.Context, node string) (WorkerReporterPredecessor, error) {
	p := WorkerReporterPredecessor{node: node, store: s, started: time.Now()}
	if s == nil || s.db == nil || node == "" {
		return p, ErrInvalidInput
	}
	err := s.db.QueryRowContext(ctx, "SELECT revision,binding FROM fugue_worker_reporter_heads WHERE node_id=$1", node).Scan(&p.revision, &p.binding)
	if errors.Is(err, sql.ErrNoRows) {
		return p, nil
	}
	return p, err
}

func (p WorkerReporterPredecessor) Bind(ctx context.Context, digest, credential string) context.Context {
	return context.WithValue(ctx, workerReporterKey{}, workerReporterAdmission{p, digest, credential})
}

func (p WorkerReporterPredecessor) Fenced() bool { return p.revision > 0 }

// A held hosted-only recipient may never use legacy credential-only
// reporting, even if it has not yet created a reporter head.
func (s *Store) acceptHostedWorker(ctx context.Context, tx *sql.Tx, who platformcontrol.PlatformComponentIdentityClaims, selectedRequired bool) error {
	if selectedRequired {
		if _, ok := ctx.Value(workerReporterKey{}).(workerReporterAdmission); !ok {
			return ErrWorkerReporterChanged
		}
	}
	return s.acceptPhysicalWorker(ctx, tx, who, ctx)
}

// observation is the original request context; do not substitute its cancellation
// for store-owned deadlines. The checkpoint and receipt always commit together.
func (s *Store) acceptPhysicalWorker(ctx context.Context, tx *sql.Tx, who platformcontrol.PlatformComponentIdentityClaims, observation ...context.Context) error {
	if who.Component != model.PlatformConsumerComponentEdgeWorker || who.ScopeKey != "global" {
		return nil
	}
	if tx == nil {
		return ErrWorkerReporterChanged
	}
	var a workerReporterAdmission
	var enabled bool
	if len(observation) == 1 && observation[0] != nil {
		a, enabled = observation[0].Value(workerReporterKey{}).(workerReporterAdmission)
	}
	if !enabled {
		if who.WorkerFrontServiceUID != "" {
			return ErrWorkerReporterChanged
		}
		// A missing rollout setting must not return an enrolled node to
		// credential-only reporting. Unenrolled nodes remain unchanged.
		if err := lockWorkerReporterEnrollment(ctx, tx, who.NodeID, false); err != nil {
			return err
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_worker_reporter_heads WHERE node_id=$1)`, who.NodeID).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return ErrWorkerReporterChanged
		}
		return nil
	}
	if a.store != s || a.node != who.NodeID || a.credential != who.CredentialID || !inputDigestPattern.MatchString(a.observed) || time.Since(a.started) > 10*time.Second {
		return ErrWorkerReporterChanged
	}
	var err error
	var r sql.Result
	switch {
	case a.revision == 0:
		if err := lockWorkerReporterEnrollment(ctx, tx, who.NodeID, true); err != nil {
			return err
		}
		r, err = tx.ExecContext(ctx, `INSERT INTO fugue_worker_reporter_heads (node_id,revision,binding) VALUES ($1,1,$2) ON CONFLICT (node_id) DO NOTHING`, a.node, a.observed)
	case a.observed != a.binding:
		r, err = tx.ExecContext(ctx, `UPDATE fugue_worker_reporter_heads SET revision=revision+1,binding=$4 WHERE node_id=$1 AND revision=$2 AND binding=$3`, a.node, a.revision, a.binding, a.observed)
	default:
		// A shared lock permits concurrent stable heartbeats; no row rewrite
		// or head advancement for an unchanged physical selection.
		var one int
		err = tx.QueryRowContext(ctx, `SELECT 1 FROM fugue_worker_reporter_heads WHERE node_id=$1 AND revision=$2 AND binding=$3 FOR SHARE`, a.node, a.revision, a.observed).Scan(&one)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrWorkerReporterChanged
		}
		return err
	}
	if err != nil {
		return err
	}
	rows, err := r.RowsAffected()
	if err == nil && rows != 1 {
		return ErrWorkerReporterChanged
	}
	return err
}

// Until the head exists there is no row to lock. During a rolling activation,
// an unfenced receipt can commit before the first fence, never after it.
// Requests do not wait with compiler/publication locks held; the caller can
// retry against a new observation. Read-committed is required so EXISTS,
// queried separately after the lock, cannot read an older snapshot.
func lockWorkerReporterEnrollment(ctx context.Context, tx *sql.Tx, node string, activation bool) error {
	statement := `SELECT current_setting('transaction_isolation') = 'read committed' AND pg_try_advisory_xact_lock_shared(hashtextextended($1,0))`
	if activation {
		statement = `SELECT current_setting('transaction_isolation') = 'read committed' AND pg_try_advisory_xact_lock(hashtextextended($1,0))`
	}
	var locked bool
	if err := tx.QueryRowContext(ctx, statement, "fugue.worker-reporter-enrollment/v1/"+node).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return ErrWorkerReporterChanged
	}
	return nil
}
