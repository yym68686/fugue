package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
)

// Explicit installation only, NOT an ordinary deployment migration. This is
// a finite, non-renewing pool: no seed, epoch rollover, cleanup, release or
// replenishment is provided. Even confirmed completion remains charged.
const CompilerInputResourceReservationsSQL = `
CREATE SEQUENCE IF NOT EXISTS fugue_platform_input_resource_tickets AS BIGINT MINVALUE 1 MAXVALUE 1024 START 1 CACHE 1 NO CYCLE;
CREATE TABLE IF NOT EXISTS fugue_platform_input_resource_pool (
 singleton BOOLEAN PRIMARY KEY CHECK(singleton),
 config_json TEXT NOT NULL CHECK(octet_length(config_json) BETWEEN 1 AND 8192)
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_resource_reservations (
 reservation_id TEXT PRIMARY KEY CHECK(reservation_id ~ '^[0-9a-f]{64}$'),
 ticket BIGINT NOT NULL UNIQUE CHECK(ticket BETWEEN 1 AND 1024),
 operation TEXT NOT NULL CHECK(operation IN ('pending-root','pending-policy')),
 request_sha256 TEXT NOT NULL CHECK(request_sha256 ~ '^[0-9a-f]{64}$'),
 created_at TIMESTAMPTZ NOT NULL,
 allocation_json TEXT NOT NULL CHECK(octet_length(allocation_json) BETWEEN 1 AND 4096)
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_resource_outcomes (
 reservation_id TEXT PRIMARY KEY REFERENCES fugue_platform_input_resource_reservations(reservation_id),
 outcome TEXT NOT NULL CHECK(outcome IN ('not-attempted','completion-not-confirmed','committed'))
);
CREATE OR REPLACE FUNCTION fugue_platform_input_resource_append_guard() RETURNS trigger LANGUAGE plpgsql AS $guard$` + compilerInputResourceAppendGuard + `$guard$;
DO $install$
DECLARE target TEXT;
BEGIN
 FOREACH target IN ARRAY ARRAY['fugue_platform_input_resource_pool','fugue_platform_input_resource_reservations','fugue_platform_input_resource_outcomes'] LOOP
  EXECUTE format('DROP TRIGGER IF EXISTS fugue_input_resource_append_guard ON %I',target);
  EXECUTE format('CREATE TRIGGER fugue_input_resource_append_guard BEFORE UPDATE OR DELETE OR TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_resource_append_guard()',target);
  EXECUTE format('ALTER TABLE %I ENABLE ALWAYS TRIGGER fugue_input_resource_append_guard',target);
 END LOOP;
END $install$;
`

const compilerInputResourceAppendGuard = `
BEGIN
 RAISE EXCEPTION 'compiler input resource reservations cannot be removed or rewritten' USING ERRCODE='55000';
END `

func VerifyCompilerInputResourceReservations(ctx context.Context, tx interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	for _, table := range []string{"fugue_platform_input_resource_pool", "fugue_platform_input_resource_reservations", "fugue_platform_input_resource_outcomes"} {
		var valid bool
		if err := tx.QueryRowContext(ctx, `SELECT t.tgenabled='A' AND t.tgtype=58 AND p.prosrc=$2 AND NOT p.prosecdef
 AND p.prolang=(SELECT oid FROM pg_language WHERE lanname='plpgsql')
 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
 WHERE t.tgrelid=to_regclass($1) AND t.tgname='fugue_input_resource_append_guard' AND NOT t.tgisinternal`, table, compilerInputResourceAppendGuard).Scan(&valid); err != nil || !valid {
			return fmt.Errorf("compiler input resource reservation guard unavailable")
		}
		key := "PRIMARY KEY (reservation_id)"
		if table == "fugue_platform_input_resource_pool" {
			key = "PRIMARY KEY (singleton)"
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*)=1 AND bool_and(convalidated AND pg_get_constraintdef(oid)=$2)
 FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype='p'`, table, key).Scan(&valid); err != nil || !valid {
			return fmt.Errorf("compiler input resource reservation identity unavailable")
		}
		if err := tx.QueryRowContext(ctx, `SELECT relkind='r' AND relpersistence='p' AND NOT relrowsecurity AND NOT relforcerowsecurity
 FROM pg_class WHERE oid=to_regclass($1)`, table).Scan(&valid); err != nil || !valid {
			return fmt.Errorf("compiler input resource reservation relation unavailable")
		}
	}
	var sequenceValid bool
	if err := tx.QueryRowContext(ctx, `SELECT s.seqtypid='int8'::regtype AND s.seqstart=1 AND s.seqincrement=1 AND s.seqmin=1 AND s.seqmax=1024 AND s.seqcache=1 AND NOT s.seqcycle AND c.relpersistence='p'
 FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid WHERE s.seqrelid='fugue_platform_input_resource_tickets'::regclass`).Scan(&sequenceValid); err != nil || !sequenceValid {
		return fmt.Errorf("compiler input resource ticket bound unavailable")
	}
	return nil
}
