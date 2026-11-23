package store

import (
	"context"
	"database/sql"

	"fugue/internal/routeartifact"
)

// Caller holds the same host/project and producer locks as the offer CAS.
// Populate old offers in place without increasing their sequence or extending
// their expiry. A same-sequence mismatch is never an invitation to repair it.
const putHostedProgramSQL = `INSERT INTO fugue_hosted_program_images AS old(scope_key,sequence,draft_id,draft_digest,program_digest,body)
 SELECT scope_key,sequence,draft_id,draft_digest,$5,$6 FROM fugue_hosted_path_prepare_offers
 WHERE scope_key=$1 AND sequence=$2 AND draft_id=$3 AND draft_digest=$4 AND expires_at>clock_timestamp()
 AND prepared_digest=$7 AND forward_digest=$8
 ON CONFLICT (scope_key) DO UPDATE SET sequence=excluded.sequence,draft_id=excluded.draft_id,draft_digest=excluded.draft_digest,
 program_digest=excluded.program_digest,body=excluded.body WHERE old.sequence<excluded.sequence`

const readHostedProgramSQL = `SELECT i.program_digest,i.body FROM fugue_hosted_program_images i
 JOIN fugue_hosted_path_prepare_offers o USING(scope_key,sequence,draft_id,draft_digest)
 WHERE o.scope_key=$1 AND o.sequence=$2 AND o.draft_id=$3 AND o.draft_digest=$4
 AND o.prepared_digest=$5 AND o.forward_digest=$6 AND o.expires_at>clock_timestamp()`

func putHostedProgramImage(ctx context.Context, tx *sql.Tx, scope string, epoch int64, draft, hash string, p routeartifact.CompiledHostedProgram) error {
	result, e := tx.ExecContext(ctx, putHostedProgramSQL, scope, epoch, draft, hash, p.Digest, p.Wire, p.Prepared, p.Forward)
	if e != nil {
		return e
	}
	changed, e := result.RowsAffected()
	if e != nil {
		return e
	}
	if changed == 1 {
		return nil
	}
	// A mixed-version writer may have advanced just the offer. Never let a
	// stale image satisfy the replay, even if it has the same program bytes.
	var digest string
	var body []byte
	e = tx.QueryRowContext(ctx, readHostedProgramSQL, scope, epoch, draft, hash, p.Prepared, p.Forward).Scan(&digest, &body)
	if e != nil {
		return mapDBErr(e)
	}
	if !p.VerifyStoredBytes(digest, body) {
		return ErrConflict
	}
	return nil
}
