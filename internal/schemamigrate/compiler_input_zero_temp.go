package schemamigrate

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
)

// CompilerInputZeroTempSQL must run only via the existing database admin in
// declarative CI. Ordinary schema migration deliberately cannot install it.
//
//go:embed compiler_input_zero_temp.sql
var CompilerInputZeroTempSQL string

const compilerInputZeroTempBody = "\nBEGIN\n PERFORM pg_catalog.set_config('temp_file_limit','0',true);\nEND\n"

// EnforceCompilerInputZeroTemp tightens only this transaction, never the role,
// database or session default. The no-argument definer has no SQL/body/setting
// input and grants no general SET privilege. A missing or changed installation
// fails closed before any dependency query. Even privileged callers use this
// same path, so superuser-only tests cannot hide ordinary-role incompatibility.
func EnforceCompilerInputZeroTemp(ctx context.Context, tx *sql.Tx) error {
	var exact bool
	err := tx.QueryRowContext(ctx, `SELECT p.prosrc=$1 AND p.prosecdef AND r.rolsuper
 AND p.prorettype='pg_catalog.void'::regtype AND p.provolatile='v' AND p.proparallel='u'
 AND p.proconfig=ARRAY['search_path=pg_catalog']::text[] AND l.lanname='plpgsql'
 AND NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
 LEFT JOIN pg_roles g ON g.oid=a.grantee
 WHERE a.privilege_type='CREATE' AND (a.grantee=0 OR NOT COALESCE(g.rolsuper,false)))
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 JOIN pg_roles r ON r.oid=p.proowner JOIN pg_roles nr ON nr.oid=n.nspowner
 JOIN pg_language l ON l.oid=p.prolang
 WHERE n.nspname='fugue_input_safety' AND nr.rolsuper
 AND p.proname='zero_temp_v1' AND p.pronargs=0 AND p.prokind='f'`, compilerInputZeroTempBody).Scan(&exact)
	if err != nil || !exact {
		return fmt.Errorf("compiler input zero-temp guard unavailable or changed")
	}
	if _, err := tx.ExecContext(ctx, `SELECT fugue_input_safety.zero_temp_v1()`); err != nil {
		return fmt.Errorf("compiler input zero-temp guard failed: %w", err)
	}
	var zero bool
	if err := tx.QueryRowContext(ctx, `SELECT current_setting('temp_file_limit')='0'`).Scan(&zero); err != nil || !zero {
		return fmt.Errorf("compiler input zero-temp bound not effective")
	}
	return nil
}
