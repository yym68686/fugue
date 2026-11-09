package store

import "context"

const compilerInputArchiveFenceBody = ` BEGIN RAISE EXCEPTION 'compiler input chunks are read-only during archive retirement' USING ERRCODE='55000'; END `

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
	var state compilerInputArchiveFenceState
	err := db.QueryRowContext(ctx, `SELECT count(*)=1,
 COALESCE(bool_and(t.tgenabled IN ('O','A') AND
  t.tgtype=30 AND NOT t.tgisinternal AND t.tgqual IS NULL AND
  t.tgnargs=0 AND octet_length(t.tgargs)=0 AND t.tgattr=''::int2vector AND
  t.tgconstraint=0 AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL AND
  p.proname='fugue_platform_input_chunks_archive_write_fence' AND
  p.pronamespace=c.relnamespace AND p.pronargs=0 AND p.prokind='f' AND
  p.prorettype='trigger'::regtype AND l.lanname='plpgsql' AND
  NOT p.prosecdef AND p.proconfig IS NULL AND p.prosrc=$1),false),
 COALESCE(bool_and(t.tgenabled='A'),false)
 FROM pg_trigger t
 JOIN pg_class c ON c.oid=t.tgrelid
 JOIN pg_proc p ON p.oid=t.tgfoid
 JOIN pg_language l ON l.oid=p.prolang
 WHERE t.tgrelid=to_regclass('fugue_platform_input_chunks')
 AND t.tgname='fugue_platform_input_chunks_archive_write_fence'`, compilerInputArchiveFenceBody).Scan(&state.Present, &state.Compatible, &state.Always)
	return state, err
}

func (s compilerInputArchiveFenceState) installed() bool {
	return s.Present && s.Compatible && s.Always
}
