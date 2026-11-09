package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
)

// Explicitly installed by a gated coordinator. This is not appended to the
// ordinary archive migration: deploying this capability neither allocates a
// production registry nor enables registration or group publication policy.
const CompilerInputExternalRootsSQL = `
CREATE TABLE IF NOT EXISTS fugue_platform_input_external_roots (
 registration_sha256 TEXT PRIMARY KEY CHECK (registration_sha256 ~ '^[0-9a-f]{64}$'),
 registry_revision BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE CHECK (registry_revision>0),
 owner_key TEXT NOT NULL CHECK (length(owner_key) BETWEEN 1 AND 256),
 generation TEXT NOT NULL CHECK (length(generation) BETWEEN 1 AND 512),
 source_digest TEXT NOT NULL CHECK (source_digest ~ '^sha256:[0-9a-f]{64}$'),
 source_epoch BIGINT NOT NULL CHECK (source_epoch>=0),
 registration_json TEXT NOT NULL CHECK (octet_length(registration_json) BETWEEN 1 AND 16384),
 created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
 UNIQUE(owner_key,generation)
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_external_root_refs (
 registration_sha256 TEXT NOT NULL REFERENCES fugue_platform_input_external_roots(registration_sha256) ON DELETE RESTRICT,
 ordinal INTEGER NOT NULL CHECK (ordinal>=0 AND ordinal<32768),
 artifact_id TEXT REFERENCES fugue_platform_artifacts(id) ON DELETE RESTRICT,
 release_id TEXT REFERENCES fugue_platform_artifact_releases(id) ON DELETE RESTRICT,
 content_hash TEXT REFERENCES fugue_platform_input_manifests(content_hash) ON DELETE RESTRICT,
 CHECK (num_nonnulls(artifact_id,release_id,content_hash)=1),
 PRIMARY KEY(registration_sha256,ordinal)
);
CREATE OR REPLACE FUNCTION fugue_platform_input_external_roots_guard() RETURNS trigger LANGUAGE plpgsql AS $guard$` + compilerInputExternalRootsGuardBody + `$guard$;
DROP TRIGGER IF EXISTS fugue_external_roots_guard ON fugue_platform_input_external_roots;
CREATE TRIGGER fugue_external_roots_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON fugue_platform_input_external_roots
 FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_external_roots_guard();
ALTER TABLE fugue_platform_input_external_roots ENABLE ALWAYS TRIGGER fugue_external_roots_guard;
DROP TRIGGER IF EXISTS fugue_external_root_refs_guard ON fugue_platform_input_external_root_refs;
CREATE TRIGGER fugue_external_root_refs_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON fugue_platform_input_external_root_refs
 FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_external_roots_guard();
ALTER TABLE fugue_platform_input_external_root_refs ENABLE ALWAYS TRIGGER fugue_external_root_refs_guard;
DROP TRIGGER IF EXISTS fugue_external_roots_budget ON fugue_platform_input_external_roots;
CREATE TRIGGER fugue_external_roots_budget AFTER INSERT ON fugue_platform_input_external_roots
 FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_external_roots_guard();
ALTER TABLE fugue_platform_input_external_roots ENABLE ALWAYS TRIGGER fugue_external_roots_budget;
DROP TRIGGER IF EXISTS fugue_external_root_refs_budget ON fugue_platform_input_external_root_refs;
CREATE TRIGGER fugue_external_root_refs_budget AFTER INSERT ON fugue_platform_input_external_root_refs
 FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_external_roots_guard();
ALTER TABLE fugue_platform_input_external_root_refs ENABLE ALWAYS TRIGGER fugue_external_root_refs_budget;
DO $barriers$
DECLARE barrier RECORD;
BEGIN
 FOR barrier IN SELECT t.tgrelid::regclass AS relation,t.tgname FROM pg_trigger t
  JOIN pg_constraint c ON c.oid=t.tgconstraint
  WHERE c.conrelid='fugue_platform_input_external_root_refs'::regclass AND c.contype='f'
 LOOP
  EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %I',barrier.relation,barrier.tgname);
 END LOOP;
END $barriers$;
`

// The shared archive barriers precede this private append lock in application
// code. Updates, deletion and TRUNCATE are always refused, including replica
// sessions; no TTL, crash cleanup or successful publication drops a root.
const compilerInputExternalRootsGuardBody = `
BEGIN
 IF TG_OP<>'INSERT' THEN
  RAISE EXCEPTION 'pending compiler input roots cannot be changed or removed' USING ERRCODE='55000';
 END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('fugue.compiler-input-external-roots.v1',0));
 IF TG_WHEN='AFTER' AND ((SELECT count(*) FROM fugue_platform_input_external_roots)>1024
  OR EXISTS(SELECT 1 FROM fugue_platform_input_external_roots GROUP BY owner_key HAVING count(*)>32)
  OR (SELECT count(*) FROM fugue_platform_input_external_root_refs)>32768) THEN
  RAISE EXCEPTION 'pending compiler input root registry full' USING ERRCODE='54000';
 END IF;
 RETURN NULL;
END `

// Check the native guards and reference barriers before accepting an existing
// registry. A lookalike table or disabled trigger is never an empty inventory.
func VerifyCompilerInputExternalRoots(ctx context.Context, tx interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	for _, table := range []struct {
		name, trigger string
		kind          int
	}{
		{"fugue_platform_input_external_roots", "fugue_external_roots_guard", 62},
		{"fugue_platform_input_external_root_refs", "fugue_external_root_refs_guard", 62},
		{"fugue_platform_input_external_roots", "fugue_external_roots_budget", 4},
		{"fugue_platform_input_external_root_refs", "fugue_external_root_refs_budget", 4},
	} {
		var valid bool
		err := tx.QueryRowContext(ctx, `SELECT t.tgenabled='A' AND t.tgtype=$4 AND p.prosrc=$3 AND p.prolang=(SELECT oid FROM pg_language WHERE lanname='plpgsql')
 AND NOT p.prosecdef FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
 WHERE t.tgrelid=$1::regclass AND t.tgname=$2 AND NOT t.tgisinternal`, table.name, table.trigger, compilerInputExternalRootsGuardBody, table.kind).Scan(&valid)
		if err != nil || !valid {
			return fmt.Errorf("pending input root guard unavailable: %s", table.name)
		}
	}
	var valid bool
	if err := tx.QueryRowContext(ctx, `SELECT count(*)=4 AND bool_and(c.convalidated AND c.confdeltype='r' AND NOT EXISTS(SELECT 1 FROM pg_trigger t WHERE t.tgconstraint=c.oid AND t.tgenabled<>'A'))
 FROM pg_constraint c WHERE c.conrelid='fugue_platform_input_external_root_refs'::regclass AND c.contype='f' AND
 (pg_get_constraintdef(c.oid) IN (
 'FOREIGN KEY (registration_sha256) REFERENCES fugue_platform_input_external_roots(registration_sha256) ON DELETE RESTRICT',
 'FOREIGN KEY (artifact_id) REFERENCES fugue_platform_artifacts(id) ON DELETE RESTRICT',
 'FOREIGN KEY (release_id) REFERENCES fugue_platform_artifact_releases(id) ON DELETE RESTRICT',
 'FOREIGN KEY (content_hash) REFERENCES fugue_platform_input_manifests(content_hash) ON DELETE RESTRICT'))`).Scan(&valid); err != nil || !valid {
		return fmt.Errorf("pending input root reference barrier unavailable")
	}
	if err := tx.QueryRowContext(ctx, `SELECT attidentity='a' AND attnotnull FROM pg_attribute WHERE attrelid='fugue_platform_input_external_roots'::regclass AND attname='registry_revision' AND NOT attisdropped`).Scan(&valid); err != nil || !valid {
		return fmt.Errorf("pending input root revision identity unavailable")
	}
	for _, check := range []struct{ table, name, definition string }{
		{"fugue_platform_input_external_roots", "fugue_platform_input_external_roots_pkey", "PRIMARY KEY (registration_sha256)"},
		{"fugue_platform_input_external_roots", "fugue_platform_input_external_roots_registry_revision_key", "UNIQUE (registry_revision)"},
		{"fugue_platform_input_external_roots", "fugue_platform_input_external_roots_owner_key_generation_key", "UNIQUE (owner_key, generation)"},
		{"fugue_platform_input_external_root_refs", "fugue_platform_input_external_root_refs_pkey", "PRIMARY KEY (registration_sha256, ordinal)"},
		{"fugue_platform_input_external_root_refs", "fugue_platform_input_external_root_refs_check", "CHECK ((num_nonnulls(artifact_id, release_id, content_hash) = 1))"},
	} {
		var definition string
		if err := tx.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=$1::regclass AND conname=$2 AND convalidated`, check.table, check.name).Scan(&definition); err != nil || definition != check.definition {
			return fmt.Errorf("pending input root constraint unavailable: %s", check.name)
		}
	}
	return nil
}
