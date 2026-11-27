-- Administrator-installed audit capability. Installation does not grant a
-- login, set a policy, change the owner, or modify any business relation.
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '250ms';
DO $install$
DECLARE capability pg_roles%ROWTYPE;
BEGIN
    IF NOT (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = CURRENT_USER) THEN
        RAISE EXCEPTION 'storage audit installation requires the database administrator';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_roles r ON r.oid = n.nspowner
        WHERE n.nspname = 'fugue_storage_audit' AND NOT r.rolsuper
    ) THEN
        RAISE EXCEPTION 'storage audit namespace is not administrator owned';
    END IF;
    SELECT * INTO capability FROM pg_roles WHERE rolname = 'fugue_storage_audit_definer';
    IF NOT FOUND THEN
        CREATE ROLE fugue_storage_audit_definer NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    ELSIF capability.rolcanlogin OR capability.rolinherit OR capability.rolsuper OR capability.rolcreatedb
          OR capability.rolcreaterole OR capability.rolreplication OR capability.rolbypassrls
          OR EXISTS (SELECT 1 FROM pg_auth_members WHERE member = capability.oid OR roleid = capability.oid) THEN
        RAISE EXCEPTION 'storage audit capability role is not isolated';
    END IF;
END
$install$;
CREATE SCHEMA IF NOT EXISTS fugue_storage_audit AUTHORIZATION CURRENT_USER;
REVOKE ALL ON SCHEMA fugue_storage_audit FROM PUBLIC;
DO $guard$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_namespace n
        CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) a
        LEFT JOIN pg_roles r ON r.oid = a.grantee
        WHERE n.nspname = 'fugue_storage_audit' AND a.privilege_type = 'CREATE' AND NOT COALESCE(r.rolsuper, false)
    ) OR EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_roles r ON r.oid = c.relowner
        WHERE n.nspname = 'fugue_storage_audit' AND NOT r.rolsuper
    ) OR EXISTS (
        SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        JOIN pg_roles r ON r.oid = p.proowner
        WHERE n.nspname = 'fugue_storage_audit' AND NOT r.rolsuper
          AND (r.rolname <> 'fugue_storage_audit_definer' OR p.proname <> 'sample_lifecycle_v1' OR p.pronargs <> 0)
    ) THEN
        RAISE EXCEPTION 'storage audit namespace contains unsafe object ownership or CREATE privileges';
    END IF;
END
$guard$;

CREATE TABLE IF NOT EXISTS fugue_storage_audit.lifecycle_control (
    singleton boolean PRIMARY KEY CHECK (singleton),
    owner text NOT NULL CHECK (owner IN ('api','worker')),
    mode text NOT NULL CHECK (mode IN ('audit','disabled')),
    policy_sha256 text NOT NULL CHECK (policy_sha256 ~ '^[a-f0-9]{64}$'),
    effective_from timestamptz NOT NULL CHECK (isfinite(effective_from)),
    interval_seconds integer NOT NULL CHECK (interval_seconds BETWEEN 300 AND 3600),
    source_page_rows integer NOT NULL CHECK (source_page_rows BETWEEN 1 AND 128),
    policy_document jsonb NOT NULL CHECK (COALESCE(
        jsonb_typeof(policy_document) = 'object'
        AND (policy_document->>'version')::integer = 1
        AND policy_document->>'mode' = mode
        AND (policy_document->>'effective_from')::timestamptz = effective_from
        AND (policy_document->>'interval_seconds')::integer = interval_seconds
        AND (policy_document->>'source_page_rows')::integer = source_page_rows, false))
);
REVOKE ALL ON TABLE fugue_storage_audit.lifecycle_control FROM PUBLIC;

CREATE OR REPLACE FUNCTION fugue_storage_audit.keep_lifecycle_owner_v1() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER
SET search_path = pg_catalog
AS $fence$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.owner <> 'api' THEN
            RAISE EXCEPTION 'storage audit must start with legacy owner';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'TRUNCATE' THEN
        IF EXISTS (SELECT FROM fugue_storage_audit.lifecycle_control WHERE singleton AND owner = 'worker') THEN
            RAISE EXCEPTION 'active storage audit owner cannot be reset';
        END IF;
        RETURN NULL;
    END IF;
    IF OLD.owner = 'worker' THEN
        IF TG_OP = 'DELETE' OR NEW.owner IS DISTINCT FROM 'worker' THEN
            RAISE EXCEPTION 'active storage audit owner cannot be reset';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    IF OLD.owner = 'api' AND NEW.owner = 'worker' AND NOT
       pg_try_advisory_xact_lock(hashtextextended('storage-lifecycle-audit/owner/v1', 0)) THEN
        RAISE EXCEPTION 'storage audit owner fence is busy' USING ERRCODE = '55P03';
    END IF;
    RETURN NEW;
END
$fence$;
REVOKE ALL ON FUNCTION fugue_storage_audit.keep_lifecycle_owner_v1() FROM PUBLIC;
CREATE OR REPLACE TRIGGER storage_audit_owner_no_downgrade
BEFORE INSERT OR UPDATE OF owner OR DELETE ON fugue_storage_audit.lifecycle_control
FOR EACH ROW EXECUTE FUNCTION fugue_storage_audit.keep_lifecycle_owner_v1();
ALTER TABLE fugue_storage_audit.lifecycle_control ENABLE ALWAYS TRIGGER storage_audit_owner_no_downgrade;
CREATE OR REPLACE TRIGGER storage_audit_owner_no_truncate
BEFORE TRUNCATE ON fugue_storage_audit.lifecycle_control
FOR EACH STATEMENT EXECUTE FUNCTION fugue_storage_audit.keep_lifecycle_owner_v1();
ALTER TABLE fugue_storage_audit.lifecycle_control ENABLE ALWAYS TRIGGER storage_audit_owner_no_truncate;

CREATE TABLE IF NOT EXISTS fugue_storage_audit.lifecycle_progress (
    singleton boolean PRIMARY KEY CHECK (singleton),
    policy_sha256 text NOT NULL,
    source_cursor text NOT NULL DEFAULT '',
    apps_cursor text NOT NULL DEFAULT '',
    ops_cursor text NOT NULL DEFAULT '',
    imports_cursor text NOT NULL DEFAULT '',
    last_attempt_at timestamptz NOT NULL,
    duration_seconds double precision NOT NULL CHECK (duration_seconds >= 0),
    last_run_failed boolean NOT NULL,
    failure_count bigint NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
    completed_passes bigint NOT NULL DEFAULT 0 CHECK (completed_passes >= 0),
    sample jsonb
);
REVOKE ALL ON TABLE fugue_storage_audit.lifecycle_progress FROM PUBLIC;

GRANT USAGE ON SCHEMA public, fugue_storage_audit TO fugue_storage_audit_definer;
-- FOR SHARE must lock the policy row against concurrent revision. The
-- capability can lock it but SQL constraints make its singleton key immutable.
GRANT SELECT, UPDATE (singleton) ON fugue_storage_audit.lifecycle_control TO fugue_storage_audit_definer;
GRANT SELECT, INSERT, UPDATE ON fugue_storage_audit.lifecycle_progress TO fugue_storage_audit_definer;
GRANT SELECT (id, tenant_id, created_at, size_bytes) ON public.fugue_source_uploads TO fugue_storage_audit_definer;
GRANT SELECT (id, tenant_id, source_json) ON public.fugue_apps TO fugue_storage_audit_definer;
GRANT SELECT (id, tenant_id, desired_source_json) ON public.fugue_operations TO fugue_storage_audit_definer;
GRANT SELECT (id, tenant_id, source_upload_id) ON public.fugue_app_database_import_jobs TO fugue_storage_audit_definer;
DO $connect$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO fugue_storage_audit_definer', current_database());
END
$connect$;

CREATE OR REPLACE VIEW fugue_storage_audit.lifecycle_status_v1
WITH (security_barrier = true) AS
SELECT COALESCE(control.owner, 'api') AS owner, COALESCE(control.mode, 'disabled') AS mode,
       COALESCE(control.policy_sha256, '') AS policy_sha256, progress.last_attempt_at,
       COALESCE(progress.duration_seconds, 0) AS duration_seconds,
       COALESCE(progress.last_run_failed, false) AS last_run_failed,
       COALESCE(progress.failure_count, 0) AS failure_count,
       COALESCE(progress.completed_passes, 0) AS completed_passes, progress.sample, control.policy_document
FROM (VALUES (true)) AS seed(singleton)
LEFT JOIN fugue_storage_audit.lifecycle_control AS control USING (singleton)
LEFT JOIN fugue_storage_audit.lifecycle_progress AS progress USING (singleton);
REVOKE ALL ON TABLE fugue_storage_audit.lifecycle_status_v1 FROM PUBLIC;

-- Only the NOLOGIN audit capability may request this transaction-local bound.
-- Neither its caller nor the capability receives SET privileges.
CREATE OR REPLACE FUNCTION fugue_storage_audit.zero_temp_v1() RETURNS void
LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY DEFINER
SET search_path = pg_catalog
AS $bound$
BEGIN
    PERFORM set_config('temp_file_limit', '0', true);
END
$bound$;
REVOKE ALL ON FUNCTION fugue_storage_audit.zero_temp_v1() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION fugue_storage_audit.zero_temp_v1() TO fugue_storage_audit_definer;

CREATE OR REPLACE FUNCTION fugue_storage_audit.sample_lifecycle_v1()
RETURNS text
LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY DEFINER
SET search_path = pg_catalog
SET lock_timeout = '100ms'
SET work_mem = '1MB'
SET max_parallel_workers_per_gather = '0'
SET jit = 'off'
AS $audit$
DECLARE
    policy fugue_storage_audit.lifecycle_control%ROWTYPE;
    prior fugue_storage_audit.lifecycle_progress%ROWTYPE;
    stamp timestamptz;
    new_source text := '';
    new_apps text := '';
    new_ops text := '';
    new_imports text := '';
    page jsonb;
    relations jsonb;
    next_sample jsonb;
    invalid_rows bigint;
BEGIN
    -- Exactly one mutation authority and atomic sample/cursors, even if the
    -- previous worker loses its process or its connection during the pass.
    IF NOT pg_try_advisory_xact_lock(hashtextextended('storage-lifecycle-audit/owner/v1', 0)) THEN
        RETURN 'busy';
    END IF;
    SELECT * INTO policy FROM fugue_storage_audit.lifecycle_control WHERE singleton FOR SHARE;
    IF NOT FOUND OR policy.owner = 'api' THEN
        RETURN 'standby';
    END IF;
    stamp := clock_timestamp();
    IF policy.mode <> 'audit' OR stamp < policy.effective_from THEN
        RETURN 'disabled';
    END IF;
    IF current_setting('transaction_isolation') <> 'repeatable read' THEN
        RAISE EXCEPTION 'storage audit worker requires REPEATABLE READ';
    END IF;
    PERFORM fugue_storage_audit.zero_temp_v1();
    SELECT * INTO prior FROM fugue_storage_audit.lifecycle_progress WHERE singleton;
    IF prior.policy_sha256 = policy.policy_sha256 THEN
        IF stamp < prior.last_attempt_at + make_interval(secs => policy.interval_seconds) THEN
            RETURN 'not_due';
        END IF;
        new_source := prior.source_cursor;
        new_apps := prior.apps_cursor;
        new_ops := prior.ops_cursor;
        new_imports := prior.imports_cursor;
    END IF;

    BEGIN
        -- Fixed relation allowlist: absent is not zero allocation. No
        -- tenant-upload identifiers or bodies cross the capability boundary.
        WITH requested(name) AS (VALUES
            ('fugue_source_uploads'), ('fugue_platform_artifacts'), ('fugue_platform_artifact_contents'),
            ('fugue_audit_events'), ('fugue_platform_input_shared_packs'), ('fugue_platform_input_shared_objects'),
            ('fugue_platform_input_component_refs'), ('fugue_platform_input_component_recipes'),
            ('fugue_platform_input_separated_refs'), ('fugue_platform_input_components'),
            ('fugue_platform_quality_evidence')
        )
        SELECT jsonb_agg(jsonb_build_object(
            'relation', r.name, 'present', c.oid IS NOT NULL,
            'heap_bytes', COALESCE(pg_relation_size(c.oid), 0),
            'table_bytes', COALESCE(pg_table_size(c.oid), 0),
            'index_bytes', COALESCE(pg_indexes_size(c.oid), 0),
            'total_bytes', COALESCE(pg_total_relation_size(c.oid), 0)
        ) ORDER BY r.name) INTO relations
        FROM requested r
        LEFT JOIN pg_namespace n ON n.nspname = 'public'
        LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = r.name AND c.relkind = 'r';

        -- Page each input independently before matching. The extra source
        -- row is a sentinel; it is never counted, classified or skipped.
        WITH source_next AS MATERIALIZED (
            SELECT id, tenant_id, created_at, size_bytes FROM public.fugue_source_uploads
            WHERE id > new_source ORDER BY id LIMIT policy.source_page_rows + 1
        ), source_page AS MATERIALIZED (
            SELECT * FROM source_next ORDER BY id LIMIT policy.source_page_rows
        ), apps AS MATERIALIZED (
            SELECT id, tenant_id, source_json->>'upload_id' AS direct_upload,
                source_json->'origin_source'->>'upload_id' AS origin_upload,
                source_json->'build_source'->>'upload_id' AS build_upload
            FROM public.fugue_apps WHERE id > new_apps ORDER BY id LIMIT policy.source_page_rows + 1
        ), operations AS MATERIALIZED (
            SELECT id, tenant_id, desired_source_json->>'upload_id' AS direct_upload,
                desired_source_json->'desired_source'->>'upload_id' AS desired_upload,
                desired_source_json->'desired_origin_source'->>'upload_id' AS origin_upload
            FROM public.fugue_operations WHERE id > new_ops ORDER BY id LIMIT policy.source_page_rows + 1
        ), imports AS MATERIALIZED (
            SELECT id, tenant_id, source_upload_id FROM public.fugue_app_database_import_jobs
            WHERE id > new_imports ORDER BY id LIMIT policy.source_page_rows + 1
        ), refs AS MATERIALIZED (
            SELECT p.id FROM source_page p JOIN imports i ON i.source_upload_id = p.id AND i.tenant_id = p.tenant_id
            UNION ALL
            SELECT p.id FROM apps a CROSS JOIN LATERAL (VALUES (a.direct_upload),(a.origin_upload),(a.build_upload)) v(id)
            JOIN source_page p ON p.id = v.id AND p.tenant_id = a.tenant_id
            UNION ALL
            SELECT p.id FROM operations o CROSS JOIN LATERAL (VALUES (o.direct_upload),(o.desired_upload),(o.origin_upload)) v(id)
            JOIN source_page p ON p.id = v.id AND p.tenant_id = o.tenant_id
        ), classified AS MATERIALIZED (
            SELECT p.*, EXISTS (SELECT FROM refs r WHERE r.id = p.id) AS referenced FROM source_page p
        ), sentinel AS (
            SELECT count(*) <= policy.source_page_rows AS at_end FROM source_next
        )
        SELECT jsonb_build_object(
            'rows', count(p.id), 'declared_bytes', COALESCE(sum(p.size_bytes), 0)::bigint,
            'observed_referenced_rows', count(*) FILTER (WHERE p.referenced),
            'observed_referenced_declared_bytes', COALESCE(sum(p.size_bytes) FILTER (WHERE p.referenced), 0)::bigint,
            'unknown_rows', count(*) FILTER (WHERE NOT p.referenced),
            'unknown_declared_bytes', COALESCE(sum(p.size_bytes) FILTER (WHERE NOT p.referenced), 0)::bigint,
            'predating_contract_rows', count(*) FILTER (WHERE p.created_at < policy.effective_from),
            'older_than_30_days_rows', count(*) FILTER (WHERE p.created_at < stamp - interval '720 hours'),
            'older_than_30_days_declared_bytes', COALESCE(sum(p.size_bytes)
                FILTER (WHERE p.created_at < stamp - interval '720 hours'), 0)::bigint,
            'page_at_end', s.at_end, 'references_complete', false, 'retirement_authorized', false, 'eligible_rows', 0
        ), count(*) FILTER (WHERE p.id IS NOT NULL AND (p.size_bytes IS NULL OR p.size_bytes < 0 OR p.created_at IS NULL)),
        CASE WHEN s.at_end THEN '' ELSE max(p.id) END,
        (SELECT CASE WHEN count(*) > policy.source_page_rows THEN max(id) ELSE '' END FROM apps),
        (SELECT CASE WHEN count(*) > policy.source_page_rows THEN max(id) ELSE '' END FROM operations),
        (SELECT CASE WHEN count(*) > policy.source_page_rows THEN max(id) ELSE '' END FROM imports)
        INTO page, invalid_rows, new_source, new_apps, new_ops, new_imports
        FROM sentinel s LEFT JOIN classified p ON true GROUP BY s.at_end;
        IF invalid_rows <> 0 THEN
            RAISE EXCEPTION 'invalid bounded audit source metadata';
        END IF;
        next_sample := jsonb_build_object(
            'observed_at', stamp, 'policy_sha256', policy.policy_sha256,
            'database_bytes', pg_database_size(current_database()), 'relations', relations, 'source_page', page
        );
    EXCEPTION WHEN OTHERS THEN
        -- No SQL, IDs or driver errors reach observers. Query cancellation
        -- is not caught and aborts the whole pass. For a failed policy change,
        -- keep the last sample but restart paging on the next eligible pass.
        INSERT INTO fugue_storage_audit.lifecycle_progress AS progress
            (singleton, policy_sha256, last_attempt_at, duration_seconds, last_run_failed, failure_count)
        VALUES (true, policy.policy_sha256, stamp, GREATEST(0, EXTRACT(EPOCH FROM clock_timestamp() - stamp)), true, 1)
        ON CONFLICT (singleton) DO UPDATE SET
            policy_sha256 = EXCLUDED.policy_sha256, last_attempt_at = EXCLUDED.last_attempt_at,
            duration_seconds = EXCLUDED.duration_seconds, last_run_failed = true,
            failure_count = progress.failure_count + 1,
            source_cursor = CASE WHEN progress.policy_sha256 = EXCLUDED.policy_sha256 THEN progress.source_cursor ELSE '' END,
            apps_cursor = CASE WHEN progress.policy_sha256 = EXCLUDED.policy_sha256 THEN progress.apps_cursor ELSE '' END,
            ops_cursor = CASE WHEN progress.policy_sha256 = EXCLUDED.policy_sha256 THEN progress.ops_cursor ELSE '' END,
            imports_cursor = CASE WHEN progress.policy_sha256 = EXCLUDED.policy_sha256 THEN progress.imports_cursor ELSE '' END;
        RETURN 'read_failed';
    END;

    INSERT INTO fugue_storage_audit.lifecycle_progress AS progress
        (singleton, policy_sha256, source_cursor, apps_cursor, ops_cursor, imports_cursor,
         last_attempt_at, duration_seconds, last_run_failed, completed_passes, sample)
    VALUES (true, policy.policy_sha256, new_source, new_apps, new_ops, new_imports,
            stamp, GREATEST(0, EXTRACT(EPOCH FROM clock_timestamp() - stamp)), false, 1, next_sample)
    ON CONFLICT (singleton) DO UPDATE SET
        policy_sha256 = EXCLUDED.policy_sha256, source_cursor = EXCLUDED.source_cursor,
        apps_cursor = EXCLUDED.apps_cursor, ops_cursor = EXCLUDED.ops_cursor, imports_cursor = EXCLUDED.imports_cursor,
        last_attempt_at = EXCLUDED.last_attempt_at, duration_seconds = EXCLUDED.duration_seconds, last_run_failed = false,
        completed_passes = progress.completed_passes + 1, sample = EXCLUDED.sample;
    RETURN 'sampled';
END
$audit$;
ALTER FUNCTION fugue_storage_audit.sample_lifecycle_v1() OWNER TO fugue_storage_audit_definer;
REVOKE ALL ON FUNCTION fugue_storage_audit.sample_lifecycle_v1() FROM PUBLIC;
COMMIT;
