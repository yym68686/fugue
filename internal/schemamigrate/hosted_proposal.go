package schemamigrate

import (
	"context"
	"fmt"
)

// NOT VALID enforces all new writes without scanning or rewriting history.
// The ordinary tables and indexes do not change for old application versions.
const hostedProposalGuardSQL = `DO $m$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='fugue_platform_artifacts'::regclass AND conname='fugue_host_proposal_draft_v1') THEN
  LOCK TABLE fugue_platform_artifacts IN ACCESS EXCLUSIVE MODE NOWAIT;
  ALTER TABLE fugue_platform_artifacts ADD CONSTRAINT fugue_host_proposal_draft_v1
   CHECK (artifact_kind <> 'hosted_path_proposal' OR status = 'draft') NOT VALID;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='fugue_platform_artifact_releases'::regclass AND conname='fugue_host_proposal_no_lane_v1') THEN
  LOCK TABLE fugue_platform_artifact_releases IN ACCESS EXCLUSIVE MODE NOWAIT;
  ALTER TABLE fugue_platform_artifact_releases ADD CONSTRAINT fugue_host_proposal_no_lane_v1
   CHECK (artifact_kind <> 'hosted_path_proposal') NOT VALID;
 END IF;
END $m$`

// Verify the actual check definitions, not just a migration name. Both are
// enforced by PostgreSQL even for old writers that do not know this kind.
const HostedProposalGuardsReadySQL = `SELECT count(*) = 2 FROM pg_constraint
 WHERE contype='c' AND (
 (conrelid=to_regclass('fugue_platform_artifacts') AND conname='fugue_host_proposal_draft_v1'
 AND pg_get_expr(conbin,conrelid) = '((artifact_kind <> ''hosted_path_proposal''::text) OR (status = ''draft''::text))')
 OR (conrelid=to_regclass('fugue_platform_artifact_releases') AND conname='fugue_host_proposal_no_lane_v1'
 AND pg_get_expr(conbin,conrelid) = '(artifact_kind <> ''hosted_path_proposal''::text)'))`

func MigrateHostedProposalGuards(ctx context.Context, databaseURL string) error {
	databaseURL, err := normalizeDatabaseURL(databaseURL)
	if err != nil {
		return err
	}
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	db, err := openDatabase(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{`SET LOCAL lock_timeout='200ms'`, `SET LOCAL statement_timeout='2s'`, hostedProposalGuardSQL} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("install proposal storage guards: %w", err)
		}
	}
	var ok bool
	if err = tx.QueryRowContext(ctx, HostedProposalGuardsReadySQL).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("host proposal storage guard definition differs")
	}
	return tx.Commit()
}
