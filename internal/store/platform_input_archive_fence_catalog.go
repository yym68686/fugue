package store

import (
	"context"
	"database/sql"
)

const compilerInputArchiveFenceBody = ` BEGIN RAISE EXCEPTION 'compiler input chunks are read-only during archive retirement' USING ERRCODE='55000'; END `

// Content is shared with ordinary artifacts, so reject the old untyped insert
// contract rather than guessing from payload keys. Published artifact writers
// insert immutable typed bytes. Unknown historical rows are never relabeled
// or deleted by this guard.
const compilerInputArchiveInlineFenceBody = ` BEGIN IF TG_OP='INSERT' AND NEW.content_kind='platform-artifact-v1' THEN RETURN NEW; END IF; RAISE EXCEPTION 'untyped or inline compiler input writes are fenced during archive retirement' USING ERRCODE='55000'; END `
const compilerInputArchiveInlineTruncateBody = ` BEGIN RAISE EXCEPTION 'inline compiler input truncation is fenced during archive retirement' USING ERRCODE='55000'; END `

type compilerInputArchiveFenceState struct {
	Present    bool
	Compatible bool
	Always     bool
}

// A name alone proves neither execution nor rejection. Validate the complete
// statement trigger and its unconditional function in the relation's schema.
// Compatible also recognizes the original origin-only trigger so an explicit
// arm can upgrade it, or an explicit pre-retirement disarm can remove it.
func compilerInputArchiveWriteFenceState(ctx context.Context, db inputQueryer) (compilerInputArchiveFenceState, error) {
	return compilerInputArchiveTriggerState(ctx, db, "fugue_platform_input_chunks", "fugue_platform_input_chunks_archive_write_fence", compilerInputArchiveFenceBody, 30)
}

func compilerInputArchiveTriggerState(ctx context.Context, db inputQueryer, relation, name, body string, events int) (compilerInputArchiveFenceState, error) {
	var state compilerInputArchiveFenceState
	err := db.QueryRowContext(ctx, `SELECT count(*)=1,
 COALESCE(bool_and(t.tgenabled IN ('O','A') AND
  t.tgtype=$4 AND NOT t.tgisinternal AND t.tgqual IS NULL AND
  t.tgnargs=0 AND octet_length(t.tgargs)=0 AND t.tgattr=''::int2vector AND
  t.tgconstraint=0 AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL AND
  p.proname=$3 AND
  p.pronamespace=c.relnamespace AND p.pronargs=0 AND p.prokind='f' AND
  p.prorettype='trigger'::regtype AND l.lanname='plpgsql' AND
  NOT p.prosecdef AND p.proconfig IS NULL AND p.prosrc=$1),false),
 COALESCE(bool_and(t.tgenabled='A'),false)
 FROM pg_trigger t
 JOIN pg_class c ON c.oid=t.tgrelid
 JOIN pg_proc p ON p.oid=t.tgfoid
 JOIN pg_language l ON l.oid=p.prolang
 WHERE t.tgrelid=to_regclass($2)
 AND t.tgname=$3`, body, relation, name, events).Scan(&state.Present, &state.Compatible, &state.Always)
	return state, err
}

type compilerInputArchiveInlineFenceState struct {
	compilerInputArchiveFenceState
	Truncate         compilerInputArchiveFenceState
	SchemaCompatible bool
}

func compilerInputArchiveInlineWriteFenceState(ctx context.Context, db inputQueryer) (compilerInputArchiveInlineFenceState, error) {
	var state compilerInputArchiveInlineFenceState
	var err error
	state.compilerInputArchiveFenceState, err = compilerInputArchiveTriggerState(ctx, db, "fugue_platform_artifact_contents", "fugue_platform_input_inline_write_fence", compilerInputArchiveInlineFenceBody, 31)
	if err != nil {
		return state, err
	}
	state.Truncate, err = compilerInputArchiveTriggerState(ctx, db, "fugue_platform_artifact_contents", "fugue_platform_input_inline_truncate_fence", compilerInputArchiveInlineTruncateBody, 34)
	if err != nil {
		return state, err
	}
	// A changed default could falsely mark old inserts as modern artifacts.
	err = db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 WHERE a.attrelid=to_regclass('fugue_platform_artifact_contents') AND a.attname='content_kind'
 AND NOT a.attisdropped AND a.atttypid='text'::regtype AND a.attnotnull AND a.attgenerated='' AND a.attidentity=''
 AND pg_get_expr(d.adbin,d.adrelid)=$1)`, "''::text").Scan(&state.SchemaCompatible)
	state.Compatible = state.Compatible && state.SchemaCompatible
	return state, err
}

func (s compilerInputArchiveInlineFenceState) installed() bool {
	return s.compilerInputArchiveFenceState.installed() && s.Truncate.installed()
}

func (s compilerInputArchiveInlineFenceState) unexpected(fenced bool) bool {
	return s.Present && (!fenced || !s.Compatible) || s.Truncate.Present && (!fenced || !s.Truncate.Compatible)
}

func (s compilerInputArchiveFenceState) installed() bool {
	return s.Present && s.Compatible && s.Always
}

// Called only by the explicit arm transaction, after catalog and admission
// checks. Deployment never installs the active fence.
func installCompilerInputInlineWriteFence(ctx context.Context, tx *sql.Tx, state compilerInputArchiveInlineFenceState) (bool, error) {
	if state.installed() {
		return false, nil
	}
	if !state.SchemaCompatible || state.unexpected(true) {
		return false, ErrConflict
	}
	if !state.Present {
		if _, err := tx.ExecContext(ctx, `CREATE FUNCTION fugue_platform_input_inline_write_fence() RETURNS trigger LANGUAGE plpgsql AS $fence$`+compilerInputArchiveInlineFenceBody+`$fence$;
 CREATE TRIGGER fugue_platform_input_inline_write_fence BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_artifact_contents FOR EACH ROW EXECUTE FUNCTION fugue_platform_input_inline_write_fence()`); err != nil {
			return false, err
		}
	}
	if !state.Truncate.Present {
		if _, err := tx.ExecContext(ctx, `CREATE FUNCTION fugue_platform_input_inline_truncate_fence() RETURNS trigger LANGUAGE plpgsql AS $fence$`+compilerInputArchiveInlineTruncateBody+`$fence$;
 CREATE TRIGGER fugue_platform_input_inline_truncate_fence BEFORE TRUNCATE ON fugue_platform_artifact_contents FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_inline_truncate_fence()`); err != nil {
			return false, err
		}
	}
	_, err := tx.ExecContext(ctx, `ALTER TABLE fugue_platform_artifact_contents ENABLE ALWAYS TRIGGER fugue_platform_input_inline_write_fence; ALTER TABLE fugue_platform_artifact_contents ENABLE ALWAYS TRIGGER fugue_platform_input_inline_truncate_fence`)
	return err == nil, err
}
