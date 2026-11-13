package schemamigrate

import (
	"context"
	"fmt"
)

// NULL inline content is an explicit CAS reference, never an empty artifact.
// A separate nullable FK protects referenced bodies without scanning/rejecting
// legacy inline rows. No historical bytes are rewritten during schema rollout.
const PlatformArtifactContentSQL = `
ALTER TABLE fugue_platform_artifacts ALTER COLUMN content_json DROP NOT NULL;
ALTER TABLE fugue_platform_artifacts ADD COLUMN IF NOT EXISTS content_ref_hash TEXT;
DO $artifact_content$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='fugue_platform_artifacts'::regclass AND conname='fugue_artifact_content_reference_check') THEN
  ALTER TABLE fugue_platform_artifacts ADD CONSTRAINT fugue_artifact_content_reference_check
   CHECK ((content_ref_hash IS NULL AND content_json IS NOT NULL) OR
    (content_ref_hash IS NOT NULL AND content_ref_hash=content_hash AND content_json IS NULL)) NOT VALID;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='fugue_platform_artifacts'::regclass AND conname='fugue_artifact_content_reference_fk') THEN
  ALTER TABLE fugue_platform_artifacts ADD CONSTRAINT fugue_artifact_content_reference_fk
   FOREIGN KEY (content_ref_hash) REFERENCES fugue_platform_artifact_contents(content_hash) NOT VALID;
 END IF;
END $artifact_content$;
CREATE OR REPLACE FUNCTION fugue_platform_artifact_body(digest TEXT, inline_body JSONB)
 RETURNS JSONB LANGUAGE plpgsql STABLE AS $artifact_body$
DECLARE body JSONB;
BEGIN
 IF inline_body IS NOT NULL THEN RETURN inline_body; END IF;
 SELECT content_json INTO body FROM fugue_platform_artifact_contents
  WHERE content_hash=digest AND content_kind='platform-artifact-v1';
 IF body IS NULL THEN RAISE EXCEPTION 'platform artifact content reference unavailable'; END IF;
 RETURN body;
END $artifact_body$;
`

func MigratePlatformArtifactContent(ctx context.Context, databaseURL string) error {
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
	for _, statement := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SELECT pg_advisory_xact_lock(315609238744297)`, PlatformArtifactContentSQL, CompilerInputLifecycleSQL} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("artifact content reference migration: %w", err)
		}
	}
	return tx.Commit()
}
