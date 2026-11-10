package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
)

// Deliberately NOT part of ordinary migration/deployment. A gated coordinator
// must reserve the operation and install a complete policy explicitly. Empty
// table means disabled. There is no permissive seed, lease renewer or command.
const CompilerInputPendingAuthoritiesSQL = `
CREATE TABLE IF NOT EXISTS fugue_platform_input_pending_authorities (
 singleton BOOLEAN PRIMARY KEY CHECK(singleton),
 revision BIGINT NOT NULL CHECK(revision>0),
 policy_json TEXT NOT NULL CHECK(octet_length(policy_json) BETWEEN 1 AND 65536)
);
CREATE OR REPLACE FUNCTION fugue_platform_input_pending_authorities_guard() RETURNS trigger LANGUAGE plpgsql AS $guard$` + compilerInputPendingAuthoritiesGuard + `$guard$;
DROP TRIGGER IF EXISTS fugue_pending_authorities_guard ON fugue_platform_input_pending_authorities;
CREATE TRIGGER fugue_pending_authorities_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON fugue_platform_input_pending_authorities
 FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_pending_authorities_guard();
ALTER TABLE fugue_platform_input_pending_authorities ENABLE ALWAYS TRIGGER fugue_pending_authorities_guard;
CREATE OR REPLACE FUNCTION fugue_platform_input_pending_authorities_revision() RETURNS trigger LANGUAGE plpgsql AS $guard$` + compilerInputPendingAuthoritiesRevision + `$guard$;
DROP TRIGGER IF EXISTS fugue_pending_authorities_revision ON fugue_platform_input_pending_authorities;
CREATE TRIGGER fugue_pending_authorities_revision BEFORE INSERT OR UPDATE ON fugue_platform_input_pending_authorities
 FOR EACH ROW EXECUTE FUNCTION fugue_platform_input_pending_authorities_revision();
ALTER TABLE fugue_platform_input_pending_authorities ENABLE ALWAYS TRIGGER fugue_pending_authorities_revision;
`

const compilerInputPendingAuthoritiesGuard = `
BEGIN
 IF TG_OP NOT IN ('INSERT','UPDATE') THEN
  RAISE EXCEPTION 'pending authority policy cannot be removed' USING ERRCODE='55000';
 END IF;
 RETURN NULL;
END `

const compilerInputPendingAuthoritiesRevision = `
BEGIN
 IF (TG_OP='INSERT' AND NEW.revision<>1) OR
  (TG_OP='UPDATE' AND (OLD.revision=9223372036854775807 OR NEW.revision<>OLD.revision+1 OR NEW.singleton<>OLD.singleton)) THEN
  RAISE EXCEPTION 'pending authority policy revision must advance exactly once' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END `

func VerifyCompilerInputPendingAuthorities(ctx context.Context, tx interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	for _, check := range []struct {
		name, body string
		kind       int
	}{
		{"fugue_pending_authorities_guard", compilerInputPendingAuthoritiesGuard, 62},
		{"fugue_pending_authorities_revision", compilerInputPendingAuthoritiesRevision, 23},
	} {
		var valid bool
		if err := tx.QueryRowContext(ctx, `SELECT t.tgenabled='A' AND t.tgtype=$3 AND p.prosrc=$2 AND NOT p.prosecdef
 AND p.prolang=(SELECT oid FROM pg_language WHERE lanname='plpgsql')
 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
 WHERE t.tgrelid='fugue_platform_input_pending_authorities'::regclass AND t.tgname=$1 AND NOT t.tgisinternal`, check.name, check.body, check.kind).Scan(&valid); err != nil || !valid {
			return fmt.Errorf("pending authority policy guard unavailable")
		}
	}
	var valid bool
	if err := tx.QueryRowContext(ctx, `SELECT count(*)=1 AND bool_and(convalidated AND pg_get_constraintdef(oid)='PRIMARY KEY (singleton)') FROM pg_constraint WHERE conrelid='fugue_platform_input_pending_authorities'::regclass AND contype='p'`).Scan(&valid); err != nil || !valid {
		return fmt.Errorf("pending authority policy identity unavailable")
	}
	return nil
}
