-- Install through the bound database administrator. No caller is granted
-- execution implicitly; the release grants it only to the dedicated worker.
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '250ms';
DO $install$
BEGIN
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = CURRENT_USER) THEN
        RAISE EXCEPTION 'observation maintenance installer requires the database administrator';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner
               WHERE n.nspname = 'fugue_observation_maintenance' AND NOT r.rolsuper) THEN
        RAISE EXCEPTION 'observation maintenance namespace is not administrator owned';
    END IF;
END
$install$;
CREATE SCHEMA IF NOT EXISTS fugue_observation_maintenance AUTHORIZATION CURRENT_USER;
REVOKE ALL ON SCHEMA fugue_observation_maintenance FROM PUBLIC;
DO $acl$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace n,
        LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) a
        LEFT JOIN pg_roles r ON r.oid = a.grantee
        WHERE n.nspname = 'fugue_observation_maintenance' AND a.privilege_type = 'CREATE'
          AND (a.grantee = 0 OR NOT COALESCE(r.rolsuper, false))) THEN
        RAISE EXCEPTION 'observation maintenance namespace has an unsafe CREATE grant';
    END IF;
END
$acl$;
CREATE TABLE IF NOT EXISTS fugue_observation_maintenance.quality_evidence_progress (
    one_row boolean PRIMARY KEY CHECK (one_row),
    completed_at timestamptz NOT NULL,
    completed_passes bigint NOT NULL CHECK (completed_passes > 0),
    total_deleted bigint NOT NULL CHECK (total_deleted >= 0)
);
DO $owned$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_roles r ON r.oid = c.relowner
               WHERE c.oid = 'fugue_observation_maintenance.quality_evidence_progress'::regclass
                 AND (c.relkind <> 'r' OR NOT r.rolsuper)) THEN
        RAISE EXCEPTION 'observation maintenance progress is not administrator owned';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner
               WHERE p.oid IN (to_regprocedure('fugue_observation_maintenance.prune_quality_evidence_v1()'),
                               to_regprocedure('fugue_observation_maintenance.fence_legacy_quality_evidence_delete_v1()'))
                 AND NOT r.rolsuper) THEN
        RAISE EXCEPTION 'observation maintenance function is not administrator owned';
    END IF;
END
$owned$;
REVOKE ALL ON TABLE fugue_observation_maintenance.quality_evidence_progress FROM PUBLIC;
CREATE OR REPLACE FUNCTION fugue_observation_maintenance.prune_quality_evidence_v1()
RETURNS TABLE (state text, deleted bigint, finished_at timestamptz)
LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY DEFINER
SET search_path = pg_catalog
SET lock_timeout = '250ms'
SET work_mem = '4MB'
SET temp_file_limit = '0'
SET max_parallel_workers_per_gather = '0'
AS $prune$
DECLARE
    owner text;
    cutoff timestamptz;
    removed bigint;
    finished timestamptz;
BEGIN
    -- Contend with foreground capacity enforcement and the gated legacy timer.
    IF NOT pg_try_advisory_xact_lock(315609238744329::bigint) THEN
        RETURN QUERY SELECT 'busy'::text, 0::bigint, NULL::timestamptz;
        RETURN;
    END IF;
    SELECT value INTO owner FROM public.fugue_meta
    WHERE key = 'observation/v1/57c22d99570d554e355f0490da0af2384767623d38729660d03f104e8ef638ed';
    IF COALESCE(owner, '') = '' THEN
        RETURN QUERY SELECT 'standby'::text, 0::bigint, NULL::timestamptz;
        RETURN;
    END IF;
    IF owner <> 'worker' THEN
        RAISE EXCEPTION 'unknown periodic quality evidence retention writer';
    END IF;
    cutoff := clock_timestamp();
    DELETE FROM public.fugue_platform_quality_evidence AS evidence
    USING (SELECT content_hash FROM public.fugue_platform_quality_evidence
           WHERE expires_at <= cutoff ORDER BY expires_at, content_hash LIMIT 256) AS expired
    WHERE evidence.content_hash = expired.content_hash AND evidence.expires_at <= cutoff;
    GET DIAGNOSTICS removed = ROW_COUNT;
    finished := clock_timestamp();
    INSERT INTO fugue_observation_maintenance.quality_evidence_progress AS progress
        (one_row, completed_at, completed_passes, total_deleted) VALUES (true, finished, 1, removed)
    ON CONFLICT (one_row) DO UPDATE SET completed_at = EXCLUDED.completed_at,
       completed_passes = progress.completed_passes + 1, total_deleted = progress.total_deleted + removed;
    RETURN QUERY SELECT 'completed'::text, removed, finished;
END
$prune$;
REVOKE ALL ON FUNCTION fugue_observation_maintenance.prune_quality_evidence_v1() FROM PUBLIC;
CREATE OR REPLACE FUNCTION fugue_observation_maintenance.fence_legacy_quality_evidence_delete_v1()
RETURNS trigger
LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY INVOKER
SET search_path = pg_catalog
AS $fence$
BEGIN
    IF current_setting('fugue.quality_evidence_admission', true) = 'v1' THEN
        RETURN NULL;
    END IF;
    -- The pruner runs as its administrator owner, the API does not. This
    -- trigger is installed only at transfer, even a pre-fence timer cannot
    -- perform its former DELETE after transfer.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = CURRENT_USER AND rolsuper) THEN
        RETURN NULL;
    END IF;
    RAISE EXCEPTION 'legacy periodic quality evidence deletion fenced' USING ERRCODE = '42501';
END
$fence$;
REVOKE ALL ON FUNCTION fugue_observation_maintenance.fence_legacy_quality_evidence_delete_v1() FROM PUBLIC;
CREATE OR REPLACE VIEW fugue_observation_maintenance.quality_evidence_worker_progress
WITH (security_barrier = true) AS
SELECT COALESCE(
    (SELECT value FROM public.fugue_meta
     WHERE key = 'observation/v1/57c22d99570d554e355f0490da0af2384767623d38729660d03f104e8ef638ed'),
    'api') AS owner,
    progress.completed_at,
    COALESCE(progress.completed_passes, 0) AS completed_passes,
    COALESCE(progress.total_deleted, 0) AS total_deleted
FROM (VALUES (true)) AS seed(one_row)
LEFT JOIN fugue_observation_maintenance.quality_evidence_progress AS progress USING (one_row);
REVOKE ALL ON TABLE fugue_observation_maintenance.quality_evidence_worker_progress FROM PUBLIC;
COMMIT;
