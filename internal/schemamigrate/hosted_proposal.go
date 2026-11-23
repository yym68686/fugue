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

// A producer offer, deliberately outside the general release/rollback tables.
// Old consumers cannot discover a new serving assignment by listing it. No
// foreign key is added to an existing hot table; claimants must independently
// re-read and authenticate every referenced object, and reject a missing one.
const HostedPrepareOffersSQL = `CREATE TABLE IF NOT EXISTS fugue_hosted_path_prepare_offers (
 scope_key text PRIMARY KEY, draft_id text NOT NULL, draft_digest text NOT NULL,
 project_id text NOT NULL, owner_digest text NOT NULL, path_digest text NOT NULL,
 prepared_digest text NOT NULL, forward_digest text NOT NULL,
 sequence bigint NOT NULL CHECK (sequence > 0),
 offered_at timestamptz NOT NULL, expires_at timestamptz NOT NULL CHECK (expires_at > offered_at),
 CHECK (draft_digest ~ '^sha256:[a-f0-9]{64}$' AND owner_digest ~ '^sha256:[a-f0-9]{64}$'
 AND path_digest ~ '^sha256:[a-f0-9]{64}$' AND prepared_digest ~ '^sha256:[a-f0-9]{64}$'
 AND forward_digest ~ '^sha256:[a-f0-9]{64}$' AND prepared_digest <> forward_digest)
)`

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
	if _, err := tx.ExecContext(ctx, HostedPrepareOffersSQL); err != nil {
		return fmt.Errorf("install isolated host preparation offers: %w", err)
	}
	if _, err := tx.ExecContext(ctx, HostedProgramImagesSQL); err != nil {
		return fmt.Errorf("install isolated host preparation content: %w", err)
	}
	for _, q := range []string{HostedSelectionsSQL, HostedSelectionReceiptsSQL, HostedMemberDirectorySQL} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("install host selection storage: %w", err)
		}
	}
	return tx.Commit()
}

// Exactly one staged image per scope, not an unbounded revision log nor
// an active release. Mixed-version writers may leave no matching image:
// readers must join on the full offer identity, not just the hostname.
const HostedProgramImagesSQL = `CREATE TABLE IF NOT EXISTS fugue_hosted_program_images (
 scope_key text PRIMARY KEY, sequence bigint NOT NULL CHECK(sequence>0),
 draft_id text NOT NULL, draft_digest text NOT NULL CHECK(draft_digest ~ '^sha256:[a-f0-9]{64}$'),
 program_digest text NOT NULL CHECK(program_digest ~ '^sha256:[a-f0-9]{64}$'),
 body bytea NOT NULL CHECK(octet_length(body) BETWEEN 1 AND 4194304),
 CHECK(program_digest = 'sha256:' || encode(sha256(body),'hex'))
)`
