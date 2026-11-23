package schemamigrate

import (
	"context"
	"fmt"
)

// Empty on installation. Retirement is an explicit, irreversible operator
// decision about an exact (consumer, kind, scope), never an inference from age.
const PlatformConsumerRetirementsSQL = `
CREATE TABLE IF NOT EXISTS fugue_platform_consumer_retirement_epoch (
 singleton BOOLEAN PRIMARY KEY CHECK(singleton),
 epoch BIGINT NOT NULL DEFAULT 0 CHECK(epoch>=0)
);
INSERT INTO fugue_platform_consumer_retirement_epoch(singleton) VALUES(true) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS fugue_platform_consumer_retirements (
 consumer_id TEXT NOT NULL CHECK(length(consumer_id) BETWEEN 1 AND 512),
 artifact_kind TEXT NOT NULL CHECK(length(artifact_kind) BETWEEN 1 AND 128),
 scope_key TEXT NOT NULL CHECK(length(scope_key) BETWEEN 1 AND 512),
 actor TEXT NOT NULL CHECK(length(btrim(actor)) BETWEEN 1 AND 256),
 reason TEXT NOT NULL CHECK(length(btrim(reason)) BETWEEN 1 AND 2048),
 plan_sha256 TEXT NOT NULL CHECK(plan_sha256 ~ '^sha256:[0-9a-f]{64}$'),
 consumer_snapshot JSONB NOT NULL CHECK(jsonb_typeof(consumer_snapshot)='object' AND octet_length(consumer_snapshot::text)<=262144),
 shadow_snapshot JSONB NOT NULL DEFAULT 'null'::jsonb CHECK(shadow_snapshot='null'::jsonb OR (jsonb_typeof(shadow_snapshot)='object' AND octet_length(shadow_snapshot::text)<=65536)),
 recovery_permanently_abandoned BOOLEAN NOT NULL CHECK(recovery_permanently_abandoned),
 retired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(consumer_id,artifact_kind,scope_key)
);
CREATE OR REPLACE FUNCTION fugue_guard_consumer_retirement_control() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TABLE_NAME='fugue_platform_consumer_retirement_epoch' AND TG_OP='UPDATE' THEN
  IF NEW.singleton=OLD.singleton AND NEW.epoch=OLD.epoch+1 THEN RETURN NEW; END IF;
 END IF;
 RAISE EXCEPTION 'consumer retirement ledger is irreversible' USING ERRCODE='40001';
END $$;
CREATE OR REPLACE FUNCTION fugue_guard_consumer_retirement_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE c jsonb; s jsonb;
BEGIN
 -- A changed fence also rejects writers carrying a pre-retirement repeatable
 -- read snapshot. Normal admission takes FOR SHARE before consulting the ledger.
 UPDATE fugue_platform_consumer_retirement_epoch SET epoch=epoch+1 WHERE singleton;
 IF NOT FOUND THEN RAISE EXCEPTION 'consumer retirement fence missing' USING ERRCODE='40001'; END IF;
 SELECT to_jsonb(x) INTO c FROM fugue_platform_consumer_instances x
  WHERE consumer_id=NEW.consumer_id AND artifact_kind=NEW.artifact_kind AND scope_key=NEW.scope_key FOR UPDATE;
 SELECT to_jsonb(x) INTO s FROM fugue_platform_shadow_consumers x
  WHERE consumer_id=NEW.consumer_id AND artifact_kind=NEW.artifact_kind AND scope_key=NEW.scope_key FOR UPDATE;
 IF c IS NULL OR c IS DISTINCT FROM NEW.consumer_snapshot OR COALESCE(s,'null'::jsonb) IS DISTINCT FROM NEW.shadow_snapshot THEN
  RAISE EXCEPTION 'consumer retirement snapshot changed' USING ERRCODE='40001';
 END IF;
 IF EXISTS(SELECT 1 FROM (
   SELECT id FROM fugue_platform_artifact_releases WHERE status='active' AND release_channel<>'shadow'
   UNION SELECT active_release_id FROM fugue_platform_release_lanes WHERE active_release_id<>'' AND release_channel<>'shadow'
  ) active CROSS JOIN LATERAL (
   SELECT 1 FROM fugue_platform_expected_consumer_sets e
   WHERE e.artifact_release_id=active.id AND e.artifact_release_id<>''
    AND e.artifact_kind=NEW.artifact_kind AND e.scope_key=NEW.scope_key
    AND e.consumers_json @> jsonb_build_array(jsonb_build_object('consumer_id',NEW.consumer_id)) OFFSET 0
  ) member) THEN
  RAISE EXCEPTION 'consumer is still enrolled in an active serving expectation' USING ERRCODE='40001';
 END IF;
 RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION fugue_check_retired_consumer_release(release_id text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM fugue_platform_expected_consumer_sets e
  CROSS JOIN LATERAL jsonb_array_elements(e.consumers_json) member
  JOIN fugue_platform_consumer_retirements retired
   ON retired.consumer_id=member->>'consumer_id' AND retired.artifact_kind=e.artifact_kind AND retired.scope_key=e.scope_key
  WHERE e.artifact_release_id=release_id AND e.artifact_release_id<>'') THEN
  RAISE EXCEPTION 'release includes a permanently retired consumer' USING ERRCODE='40001';
 END IF;
END $$;
CREATE OR REPLACE FUNCTION fugue_guard_retired_consumer_root() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE current_epoch bigint;
BEGIN
 SELECT epoch INTO STRICT current_epoch FROM fugue_platform_consumer_retirement_epoch WHERE singleton FOR SHARE;
 IF current_epoch=0 THEN RETURN NEW; END IF;
 IF TG_TABLE_NAME IN ('fugue_platform_consumer_instances','fugue_platform_shadow_consumers') THEN
  IF EXISTS(SELECT 1 FROM fugue_platform_consumer_retirements
   WHERE consumer_id=NEW.consumer_id AND artifact_kind=NEW.artifact_kind AND scope_key=NEW.scope_key) THEN
   RAISE EXCEPTION 'consumer identity permanently retired' USING ERRCODE='40001';
  END IF;
 ELSIF TG_TABLE_NAME='fugue_platform_expected_consumer_sets' THEN
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.consumers_json) member
   JOIN fugue_platform_consumer_retirements retired
    ON retired.consumer_id=member->>'consumer_id' AND retired.artifact_kind=NEW.artifact_kind AND retired.scope_key=NEW.scope_key) THEN
   RAISE EXCEPTION 'cannot re-enroll a permanently retired consumer' USING ERRCODE='40001';
  END IF;
 ELSIF TG_TABLE_NAME='fugue_platform_artifact_releases' THEN
  IF NEW.status='active' THEN PERFORM fugue_check_retired_consumer_release(NEW.id); END IF;
 ELSIF TG_TABLE_NAME='fugue_platform_release_lanes' THEN
  IF NEW.active_release_id<>'' THEN PERFORM fugue_check_retired_consumer_release(NEW.active_release_id); END IF;
 END IF;
 RETURN NEW;
END $$;
` + beforeTriggerReplayStart + `
  SELECT rel,trig,'fugue_guard_consumer_retirement_control' AS fn,events,granularity,bits,'A' AS mode
  FROM unnest(ARRAY['fugue_platform_consumer_retirement_epoch','fugue_platform_consumer_retirements']) AS ledgers(rel)
  CROSS JOIN (VALUES ('fugue_consumer_retirement_control','UPDATE OR DELETE','ROW',27),
                     ('fugue_consumer_retirement_truncate','TRUNCATE','STATEMENT',34)) AS controls(trig,events,granularity,bits)
  UNION ALL SELECT 'fugue_platform_consumer_retirements','fugue_consumer_retirement_insert','fugue_guard_consumer_retirement_insert','INSERT','ROW',7,'A'
  UNION ALL SELECT rel,'fugue_retired_consumer_root','fugue_guard_retired_consumer_root','INSERT OR UPDATE','ROW',23,'A'
  FROM unnest(ARRAY['fugue_platform_consumer_instances','fugue_platform_shadow_consumers',
   'fugue_platform_expected_consumer_sets','fugue_platform_artifact_releases','fugue_platform_release_lanes']) AS roots(rel)
` + beforeTriggerReplayEnd

func MigratePlatformConsumerRetirements(ctx context.Context, databaseURL string) error {
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
	for _, q := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SELECT pg_advisory_xact_lock(hashtextextended('fugue/consumer-retirements/v1',0))`, PlatformConsumerRetirementsSQL} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("consumer retirement migration: %w", err)
		}
	}
	return tx.Commit()
}
