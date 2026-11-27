-- Install the bounded worker capability in standby. It does not attach the
-- legacy API fence, transfer the writer, or grant execution to any caller.
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '250ms';
DO $ownership$
BEGIN
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = CURRENT_USER)
       OR NOT EXISTS (SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner
                      WHERE n.nspname = 'fugue_observation_maintenance' AND r.rolsuper) THEN
        RAISE EXCEPTION 'sample-retention installer requires the database administrator';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_namespace n,
        LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) a
        LEFT JOIN pg_roles r ON r.oid = a.grantee
        WHERE n.nspname = 'fugue_observation_maintenance' AND a.privilege_type = 'CREATE'
          AND (a.grantee = 0 OR NOT COALESCE(r.rolsuper, false))) THEN
        RAISE EXCEPTION 'observation maintenance namespace has an unsafe CREATE grant';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_roles r ON r.oid = c.relowner
               WHERE c.oid = to_regclass('fugue_observation_maintenance.sample_retention_progress')
                 AND (c.relkind <> 'r' OR NOT r.rolsuper))
       OR EXISTS (SELECT 1 FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner
                  WHERE p.oid = to_regprocedure('fugue_observation_maintenance.prune_samples_v1(text,boolean,integer,integer,integer)')
                    AND NOT r.rolsuper) THEN
        RAISE EXCEPTION 'sample-retention objects are not administrator owned';
    END IF;
END
$ownership$;

CREATE TABLE IF NOT EXISTS fugue_observation_maintenance.sample_retention_progress (
    target text PRIMARY KEY CHECK (target IN ('network', 'performance', 'resource-usage')),
    completed_at timestamptz NOT NULL,
    completed_passes bigint NOT NULL CHECK (completed_passes > 0),
    total_deleted bigint NOT NULL CHECK (total_deleted >= 0)
);
REVOKE ALL ON TABLE fugue_observation_maintenance.sample_retention_progress FROM PUBLIC;

CREATE OR REPLACE FUNCTION fugue_observation_maintenance.prune_samples_v1(
    p_target text, p_periodic boolean, p_probe_days int, p_probe_rows int, p_batch_rows int
)
RETURNS TABLE (state text, deleted bigint, finished_at timestamptz)
LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY DEFINER
SET search_path = pg_catalog
SET lock_timeout = '250ms'
SET work_mem = '4MB'
SET temp_file_limit = '0'
SET max_parallel_workers_per_gather = '0'
AS $prune$
DECLARE
    table_name text;
    time_column text;
    minimum_history interval;
    owner text;
    at timestamptz;
    cutoff timestamptz;
    protected_cutoff timestamptz;
    capacity_boundary timestamptz;
    removed bigint := 0;
    finished timestamptz;
BEGIN
    CASE p_target
    WHEN 'network' THEN
        table_name := 'fugue_edge_network_samples'; time_column := 'observed_at';
        minimum_history := interval '1 hour';
    WHEN 'performance' THEN
        table_name := 'fugue_edge_performance_samples'; time_column := 'sampled_at';
        minimum_history := interval '168 hours';
    WHEN 'resource-usage' THEN
        table_name := 'fugue_resource_usage_samples'; time_column := 'observed_at';
        minimum_history := interval '168 hours';
    ELSE
        RAISE EXCEPTION 'unknown observation sample retention target';
    END CASE;
    IF p_periodic IS NULL OR p_probe_days IS NULL OR p_probe_days NOT BETWEEN 7 AND 90
       OR p_probe_rows IS NULL OR p_probe_rows NOT BETWEEN 10000 AND 10000000
       OR p_batch_rows IS NULL OR p_batch_rows NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION 'observation sample retention policy exceeds bounds';
    END IF;
    IF NOT pg_try_advisory_xact_lock(hashtextextended('observation-sample-retention-batch/v1/' || p_target, 0)) THEN
        RETURN QUERY SELECT 'busy'::text, 0::bigint, NULL::timestamptz;
        RETURN;
    END IF;
    SELECT value INTO owner FROM public.fugue_meta WHERE key =
        'observation/v1/' || encode(sha256(convert_to('observation-sample-retention-writer/v1', 'UTF8')), 'hex');
    IF owner IS NULL THEN
        RETURN QUERY SELECT 'standby'::text, 0::bigint, NULL::timestamptz;
        RETURN;
    END IF;
    IF owner <> 'worker' THEN
        RAISE EXCEPTION 'unrecognized observation sample retention writer';
    END IF;
    -- Do not run when an inconsistent deployment selects the worker
    -- without a database fence. Hold the relation lock until commit so
    -- the guard cannot be replaced or disabled while this pass is active.
    EXECUTE format('LOCK TABLE public.%I IN ROW EXCLUSIVE MODE', table_name);
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = to_regclass(format('public.%I', table_name))
          AND tgname = 'fugue_sample_retention_delete_fence_v1'
          AND tgfoid = to_regprocedure('fugue_observation_maintenance.fence_legacy_sample_delete_v1()')
          AND tgenabled = 'A' AND tgtype = 11 AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'sample deletion fence is missing or not enforced';
    END IF;

    at := clock_timestamp();
    SELECT CASE WHEN value ~ '^-?[0-9]{1,12}$' THEN to_timestamp(value::bigint) END
    INTO cutoff FROM public.fugue_meta WHERE key =
        'observation/v1/' || encode(sha256(convert_to('observation-sample-retention-demand/v1/' || p_target, 'UTF8')), 'hex');
    IF cutoff IS NOT NULL THEN
        cutoff := LEAST(cutoff, at - minimum_history);
    END IF;
    IF p_periodic THEN
        protected_cutoff := at - interval '168 hours';
        cutoff := GREATEST(cutoff, at - make_interval(hours => p_probe_days * 24));
        -- If an age cutoff already covers everything the capacity rule
        -- may select, do not scan the budget offset on a large table.
        IF cutoff < protected_cutoff THEN
            EXECUTE format('SELECT %1$I FROM public.%2$I ORDER BY %1$I DESC OFFSET $1 LIMIT 1',
                           time_column, table_name)
               INTO capacity_boundary USING p_probe_rows - 1;
            IF capacity_boundary IS NOT NULL THEN
                cutoff := GREATEST(cutoff, LEAST(capacity_boundary, protected_cutoff));
            END IF;
        END IF;
    END IF;
    IF cutoff IS NOT NULL THEN
        EXECUTE format('WITH expired AS (SELECT ctid FROM public.%2$I WHERE %1$I < $1
                       ORDER BY %1$I LIMIT $2 FOR UPDATE SKIP LOCKED)
                       DELETE FROM public.%2$I AS s USING expired WHERE s.ctid = expired.ctid',
                       time_column, table_name)
          USING cutoff, p_batch_rows;
        GET DIAGNOSTICS removed = ROW_COUNT;
    END IF;
    finished := clock_timestamp();
    INSERT INTO fugue_observation_maintenance.sample_retention_progress AS progress
        (target, completed_at, completed_passes, total_deleted) VALUES (p_target, finished, 1, removed)
    ON CONFLICT (target) DO UPDATE SET completed_at = EXCLUDED.completed_at,
        completed_passes = progress.completed_passes + 1, total_deleted = progress.total_deleted + removed;
    RETURN QUERY SELECT 'completed'::text, removed, finished;
END
$prune$;
REVOKE ALL ON FUNCTION fugue_observation_maintenance.prune_samples_v1(text, boolean, int, int, int) FROM PUBLIC;

CREATE OR REPLACE VIEW fugue_observation_maintenance.sample_worker_progress WITH (security_barrier = true) AS
SELECT seed.target, COALESCE(
    (SELECT value FROM public.fugue_meta WHERE key = 'observation/v1/' ||
        encode(sha256(convert_to('observation-sample-retention-writer/v1', 'UTF8')), 'hex')), 'api') AS owner,
    progress.completed_at, COALESCE(progress.completed_passes, 0) AS completed_passes,
    COALESCE(progress.total_deleted, 0) AS total_deleted,
    EXISTS (SELECT 1 FROM pg_catalog.pg_trigger
            WHERE tgrelid = pg_catalog.to_regclass(seed.relation)
              AND tgname = 'fugue_sample_retention_delete_fence_v1'
              AND tgfoid = pg_catalog.to_regprocedure('fugue_observation_maintenance.fence_legacy_sample_delete_v1()')
              AND tgenabled = 'A' AND tgtype = 11 AND NOT tgisinternal) AS fence_enforced
FROM (VALUES ('network'::text,'public.fugue_edge_network_samples'::text),
             ('performance'::text,'public.fugue_edge_performance_samples'::text),
             ('resource-usage'::text,'public.fugue_resource_usage_samples'::text)) AS seed(target,relation)
LEFT JOIN fugue_observation_maintenance.sample_retention_progress AS progress USING (target);
REVOKE ALL ON TABLE fugue_observation_maintenance.sample_worker_progress FROM PUBLIC;
COMMIT;
