package schemamigrate

import (
	"context"
	"fmt"
)

// Additive and inert until a reviewed retirement transition succeeds. This
// fence is deliberately independent of full-history/archive admission: turning
// off an old application's environment flag cannot disarm it.
const CompilerContractSQL = `
CREATE TABLE IF NOT EXISTS fugue_platform_compiler_contract (
 singleton BOOLEAN PRIMARY KEY CHECK(singleton),
 epoch BIGINT NOT NULL DEFAULT 0 CHECK(epoch>=0),
 phase TEXT NOT NULL DEFAULT 'observe' CHECK(phase IN ('observe','enforced')),
 compiler_version TEXT NOT NULL DEFAULT 'platform-config-compiler/v38'
   CHECK(compiler_version='platform-config-compiler/v38'),
 plan_sha256 TEXT NOT NULL DEFAULT '' CHECK(plan_sha256='' OR plan_sha256 ~ '^[0-9a-f]{64}$'),
 enforced_at TIMESTAMPTZ,
 CHECK((phase='observe' AND plan_sha256='' AND enforced_at IS NULL) OR
       (phase='enforced' AND plan_sha256<>'' AND enforced_at IS NOT NULL))
);
INSERT INTO fugue_platform_compiler_contract(singleton) VALUES(true) ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION fugue_guard_compiler_contract_control() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP <> 'UPDATE' THEN
  RAISE EXCEPTION 'compiler contract ledger cannot be removed' USING ERRCODE='40001';
 END IF;
 IF OLD.phase='enforced' AND NEW IS DISTINCT FROM OLD OR
    NEW.singleton IS DISTINCT FROM OLD.singleton OR NEW.epoch<>OLD.epoch+1 OR
    NEW.phase<>'enforced' OR OLD.phase<>'observe' THEN
  RAISE EXCEPTION 'compiler contract transition refused' USING ERRCODE='40001';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS fugue_compiler_contract_control ON fugue_platform_compiler_contract;
CREATE TRIGGER fugue_compiler_contract_control BEFORE UPDATE OR DELETE ON fugue_platform_compiler_contract
 FOR EACH ROW EXECUTE FUNCTION fugue_guard_compiler_contract_control();
DROP TRIGGER IF EXISTS fugue_compiler_contract_truncate ON fugue_platform_compiler_contract;
CREATE TRIGGER fugue_compiler_contract_truncate BEFORE TRUNCATE ON fugue_platform_compiler_contract
 FOR EACH STATEMENT EXECUTE FUNCTION fugue_guard_compiler_contract_control();

-- The database rejects old direct writers as well as the current application.
-- Exact equality is intentional: an unknown v39 is not implicitly authorized.
CREATE OR REPLACE FUNCTION fugue_check_compiler_contract(artifact text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE c record; v text;
BEGIN
 SELECT phase,compiler_version INTO STRICT c FROM fugue_platform_compiler_contract WHERE singleton FOR SHARE;
 IF c.phase='observe' OR artifact='' THEN RETURN; END IF;
 SELECT metadata_json->>'compiler_version' INTO v FROM fugue_platform_artifacts WHERE id=artifact;
 IF v IS NOT NULL AND v<>'' AND v<>c.compiler_version THEN
  RAISE EXCEPTION 'compiler contract retired' USING ERRCODE='40001';
 END IF;
END $$;
CREATE OR REPLACE FUNCTION fugue_guard_compiler_contract_root() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE j jsonb; a text; g text; k text; r record;
BEGIN
 -- Observe mode adds no artifact lookups to high-frequency consumer receipts.
 -- Keep the ledger row locked until commit so enforcement cannot race a writer
 -- that has already passed this gate.
 IF (SELECT phase='observe' FROM fugue_platform_compiler_contract WHERE singleton FOR SHARE) THEN
  RETURN NEW;
 END IF;
 j=to_jsonb(NEW);
 IF TG_TABLE_NAME='fugue_platform_artifacts' THEN
  PERFORM fugue_check_compiler_contract(NEW.id);
  IF (SELECT phase='enforced' FROM fugue_platform_compiler_contract WHERE singleton FOR SHARE)
    AND COALESCE(NEW.metadata_json->>'compiler_version','') NOT IN ('','platform-config-compiler/v38') THEN
   RAISE EXCEPTION 'compiler writer contract retired' USING ERRCODE='40001';
  END IF;
 ELSIF TG_TABLE_NAME='fugue_platform_release_lanes' THEN
  SELECT artifact_id INTO a FROM fugue_platform_artifact_releases WHERE id=NEW.active_release_id;
  PERFORM fugue_check_compiler_contract(COALESCE(a,''));
 ELSIF TG_TABLE_NAME='fugue_platform_artifact_releases' THEN
  IF NEW.status='active' THEN PERFORM fugue_check_compiler_contract(NEW.artifact_id); END IF;
 ELSIF TG_TABLE_NAME IN ('fugue_platform_consumer_instances','fugue_platform_shadow_consumers') THEN
  IF TG_TABLE_NAME='fugue_platform_shadow_consumers' THEN j=j || (j->'body'); END IF;
  PERFORM fugue_check_compiler_contract(COALESCE(j->>'release_set_id',''));
  FOREACH k IN ARRAY ARRAY['desired_generation','actual_generation','candidate_generation','lkg_generation'] LOOP
   g=COALESCE(j->>k,'');
   IF g<>'' THEN
    FOR r IN SELECT id FROM fugue_platform_artifacts
     WHERE artifact_kind=j->>'artifact_kind' AND scope_key=j->>'scope_key' AND generation=g LOOP
     PERFORM fugue_check_compiler_contract(r.id);
    END LOOP;
   END IF;
  END LOOP;
 ELSE
  PERFORM fugue_check_compiler_contract(COALESCE(j->>'artifact_id',''));
 END IF;
 RETURN NEW;
END $$;
DO $$
DECLARE t text;
BEGIN
 FOREACH t IN ARRAY ARRAY['fugue_platform_artifacts','fugue_platform_artifact_releases',
 'fugue_platform_release_lanes','fugue_platform_lkg_snapshots','fugue_platform_lkg_snapshot_history',
 'fugue_platform_consumer_instances','fugue_platform_shadow_consumers'] LOOP
  EXECUTE format('DROP TRIGGER IF EXISTS fugue_compiler_contract_root ON %I',t);
  EXECUTE format('CREATE TRIGGER fugue_compiler_contract_root BEFORE INSERT OR UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION fugue_guard_compiler_contract_root()',t);
  EXECUTE format('ALTER TABLE %I ENABLE ALWAYS TRIGGER fugue_compiler_contract_root',t);
 END LOOP;
END $$;
`

func MigrateCompilerContract(ctx context.Context, databaseURL string) error {
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
	for _, q := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SELECT pg_advisory_xact_lock(315609238744330)`, CompilerContractSQL} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("compiler contract migration: %w", err)
		}
	}
	return tx.Commit()
}
