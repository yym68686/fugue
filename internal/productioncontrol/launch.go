package productioncontrol

import (
	"context"
	"database/sql"
	"errors"
)

// ReserveReleaseLaunch authorizes at most one actual Guardian rollout, in
// addition to the distinct reservation for publishing the desired pointer.
// It requires byte-exact approved payload and the current pending fence. A
// caller must launch only when it receives created=true, never on SQL failure
// or an ambiguous commit. This does not grant separate recovery authority.
func (j *Journal) ReserveReleaseLaunch(ctx context.Context, actor string, intent Intent, payload []byte, fence int64) (bool, error) {
	intentDigest, err := intent.Digest()
	if err != nil || intent.Operation != ComponentRelease || fence < 1 ||
		!tokenPattern.MatchString(actor) || intent.VerifyPayload(payload) != nil {
		return false, errors.New("invalid production release launch")
	}
	record, err := j.record(ctx, intentDigest, func(tx *sql.Tx) (bool, error) {
		if err := lockResource(ctx, tx, intent.scope()); err != nil {
			return false, err
		}
		stored, _, err := readIntent(ctx, tx, intentDigest)
		if err != nil || stored != intent {
			return false, ErrStaleFence
		}
		claim, err := readClaim(ctx, tx, intentDigest)
		if err != nil || claim.Scope != intent.scope() || claim.Fence != fence {
			return false, ErrStaleFence
		}
		state, err := readResource(ctx, tx, intent.scope())
		if err != nil {
			return false, err
		}
		if state.LastFence != fence || state.Pending != intentDigest {
			return false, ErrStaleFence
		}
		var launched bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_control.execution_launches WHERE intent_digest=$1)`, intentDigest).Scan(&launched); err != nil || launched {
			return false, err
		}
		if err := verifyRecordedApproval(ctx, tx, j, intent, intentDigest, claim.ApprovalDigest); err != nil {
			return false, err
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO fugue_control.execution_launches (intent_digest, actor) VALUES ($1, $2) ON CONFLICT DO NOTHING`, intentDigest, actor)
		if err != nil {
			return false, err
		}
		rows, err := result.RowsAffected()
		return rows == 1, err
	})
	return record.Created, err
}
