package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
)

// The projection is an acceleration index, never a retention decision. Keep
// JSON null distinct from missing keys and charge the complete source size.
// Changing this expression requires a new projection version and a new table.
const CompilerInputHotProjection = `CASE WHEN artifact_kind='dns_answer_bundle' AND jsonb_typeof(content_json)='object'
 THEN COALESCE((SELECT jsonb_object_agg(fields.key,fields.value)
 FROM jsonb_to_record(content_json) AS dependency(lineage jsonb,policy jsonb,
 cell_route_publications jsonb,previous_traffic_publication jsonb,cell_dns_source jsonb)
 CROSS JOIN LATERAL (SELECT array_agg(key) AS present FROM jsonb_object_keys(content_json) AS object_keys(key)) keys
 CROSS JOIN LATERAL (VALUES
 ('lineage',COALESCE(dependency.lineage,'null'::jsonb)),('policy',COALESCE(dependency.policy,'null'::jsonb)),
 ('cell_route_publications',COALESCE(dependency.cell_route_publications,'null'::jsonb)),
 ('previous_traffic_publication',COALESCE(dependency.previous_traffic_publication,'null'::jsonb)),
 ('cell_dns_source',COALESCE(dependency.cell_dns_source,'null'::jsonb))) fields(key,value)
 WHERE fields.key=ANY(keys.present)),'{}'::jsonb)
 ELSE content_json END`

const compilerInputHotIndexTriggerBody = `
BEGIN
 -- Caller-supplied projections, sizes and version bindings are never trusted.
 -- Capture the source in this statement's snapshot. A concurrent source
 -- update makes this entry stale; readers then use the original source.
 SELECT xmin,ctid,
  left((` + CompilerInputHotProjection + `)::text,8388609),
  left(metadata_json::text,16385),octet_length(content_json::text)
 INTO NEW.source_xmin,NEW.source_ctid,
  NEW.content_text,NEW.metadata_text,NEW.source_bytes
 FROM fugue_platform_artifacts WHERE id=NEW.artifact_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'hot dependency index source unavailable'; END IF;
 NEW.built_xid := pg_current_xact_id()::text::bigint;
 IF NEW.source_bytes>8388608 OR octet_length(NEW.content_text)>8388608
  OR octet_length(NEW.metadata_text)>16384 THEN
  RAISE EXCEPTION 'hot dependency index source exceeds budget';
 END IF;
 RETURN NEW;
END
`

// Inert schema only: no table rewrite, historical body scan or source mutation.
// The explicit bounded maintenance operation fills entries later. Any source
// tuple update invalidates its entry without adding work to business writers.
const CompilerInputHotIndexSQL = `
CREATE TABLE fugue_platform_input_hot_dependency_index_v1 (
 artifact_id TEXT PRIMARY KEY,
 source_xmin XID NOT NULL,
 source_ctid TID NOT NULL,
 built_xid BIGINT NOT NULL,
 content_text TEXT NOT NULL,
 metadata_text TEXT NOT NULL,
 source_bytes BIGINT NOT NULL CHECK (source_bytes>=0 AND source_bytes<=8388608),
 CHECK (octet_length(content_text)<=8388608 AND octet_length(metadata_text)<=16384)
);
CREATE FUNCTION fugue_input_hot_dependency_index_v1_fill() RETURNS trigger
 LANGUAGE plpgsql AS $hot_index$` + compilerInputHotIndexTriggerBody + `$hot_index$;
CREATE TRIGGER fugue_input_hot_dependency_index_v1_fill BEFORE INSERT OR UPDATE
 ON fugue_platform_input_hot_dependency_index_v1 FOR EACH ROW
 EXECUTE FUNCTION fugue_input_hot_dependency_index_v1_fill();
`

func MigrateCompilerInputHotIndex(ctx context.Context, databaseURL string) error {
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
	for _, statement := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SELECT pg_advisory_xact_lock(315609238744293)`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create compiler input dependency index: %w", err)
		}
	}
	var installed bool
	if err = tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_hot_dependency_index_v1') IS NOT NULL`).Scan(&installed); err != nil {
		return err
	}
	if !installed {
		if _, err = tx.ExecContext(ctx, CompilerInputHotIndexSQL); err != nil {
			return fmt.Errorf("create compiler input dependency index: %w", err)
		}
	}
	if err = VerifyCompilerInputHotIndex(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// An enabled, exact server-side derivation must guard every cache write. A
// lookalike or disabled trigger is not a trusted source of dependency metadata.
func VerifyCompilerInputHotIndex(ctx context.Context, tx interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	var body string
	var invoker, enabled, exact bool
	err := tx.QueryRowContext(ctx, `SELECT p.prosrc,NOT p.prosecdef,t.tgenabled IN ('O','A'),
 t.tgtype=23 AND t.tgnargs=0 AND t.tgattr='' AND t.tgqual IS NULL AND t.tgconstraint=0
 AND p.proname='fugue_input_hot_dependency_index_v1_fill' AND p.pronamespace=c.relnamespace
 AND p.prorettype='trigger'::regtype AND l.lanname='plpgsql'
 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_language l ON l.oid=p.prolang
 WHERE t.tgrelid='fugue_platform_input_hot_dependency_index_v1'::regclass
 AND t.tgname='fugue_input_hot_dependency_index_v1_fill' AND NOT t.tgisinternal`).Scan(&body, &invoker, &enabled, &exact)
	if err != nil || body != compilerInputHotIndexTriggerBody || !invoker || !enabled || !exact {
		return fmt.Errorf("compiler input dependency index derivation missing or changed")
	}
	// Each source identity must have at most one derived entry; duplicate rows
	// must not turn a batch read into an ambiguous graph or bypass byte bounds.
	var primaryKey string
	if err = tx.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint
 WHERE conrelid='fugue_platform_input_hot_dependency_index_v1'::regclass AND contype='p' AND convalidated`).Scan(&primaryKey); err != nil || primaryKey != "PRIMARY KEY (artifact_id)" {
		return fmt.Errorf("compiler input dependency index identity key missing or changed")
	}
	var columns bool
	if err = tx.QueryRowContext(ctx, `SELECT count(*)=7 AND bool_and(a.attnotnull AND a.atttypid=w.typ::regtype)
 FROM (VALUES ('artifact_id','text'),('source_xmin','xid'),('source_ctid','tid'),('built_xid','bigint'),
 ('content_text','text'),('metadata_text','text'),('source_bytes','bigint')) w(name,typ)
 JOIN pg_attribute a ON a.attname=w.name
 WHERE a.attrelid='fugue_platform_input_hot_dependency_index_v1'::regclass AND NOT a.attisdropped`).Scan(&columns); err != nil || !columns {
		return fmt.Errorf("compiler input dependency index required columns missing or nullable")
	}
	return nil
}
