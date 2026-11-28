-- The isolated import worker is provisioned separately. Installing these
-- entrypoints does not set the execution owner or expose standby jobs.
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '250ms';
SET LOCAL search_path = pg_catalog;

DO $roles$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = current_user AND rolsuper) THEN
        RAISE EXCEPTION 'database import capability requires the database administrator';
    END IF;
    IF NOT EXISTS (
        SELECT FROM pg_roles r WHERE r.rolname = 'fugue_app_db_import_definer'
          AND NOT (r.rolcanlogin OR r.rolinherit OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole
                   OR r.rolbypassrls OR r.rolreplication)
          AND NOT EXISTS (SELECT FROM pg_auth_members m WHERE m.roleid = r.oid OR m.member = r.oid)
    ) OR NOT EXISTS (
        SELECT FROM pg_roles r WHERE r.rolname = 'fugue_app_db_import_worker'
          AND r.rolcanlogin AND NOT (r.rolinherit OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole
                                    OR r.rolbypassrls OR r.rolreplication)
          AND NOT EXISTS (SELECT FROM pg_auth_members m WHERE m.roleid = r.oid OR m.member = r.oid)
    ) THEN
        RAISE EXCEPTION 'database import worker roles are not isolated';
    END IF;
END
$roles$;

CREATE SCHEMA fugue_app_db_import_worker;
REVOKE ALL ON SCHEMA fugue_app_db_import_worker FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO fugue_app_db_import_definer;
GRANT USAGE ON SCHEMA fugue_app_db_import_worker TO fugue_app_db_import_definer, fugue_app_db_import_worker;
GRANT SELECT (key, value, updated_at) ON public.fugue_meta TO fugue_app_db_import_definer;
GRANT INSERT (key, value, updated_at) ON public.fugue_meta TO fugue_app_db_import_definer;
GRANT UPDATE (value, updated_at) ON public.fugue_meta TO fugue_app_db_import_definer;
GRANT DELETE ON public.fugue_meta TO fugue_app_db_import_definer;
GRANT SELECT (id, tenant_id, app_id, source_upload_id, status, created_at, started_at, format, clean, logs_json)
    ON public.fugue_app_database_import_jobs TO fugue_app_db_import_definer;
GRANT SELECT (id, tenant_id, name, spec_json, status_json), UPDATE (id)
    ON public.fugue_apps TO fugue_app_db_import_definer;
GRANT SELECT (id, tenant_id, app_id, service_id, created_at) ON public.fugue_service_bindings TO fugue_app_db_import_definer;
GRANT SELECT (id, tenant_id, owner_app_id, type, provisioner, status, spec_json) ON public.fugue_backing_services TO fugue_app_db_import_definer;
GRANT SELECT (app_id, type, status) ON public.fugue_operations TO fugue_app_db_import_definer;
GRANT SELECT (id, status) ON public.fugue_tenants TO fugue_app_db_import_definer;
-- PostgreSQL requires UPDATE on at least one column for a row lock, even
-- when the fixed function only uses FOR SHARE and never edits those rows.
GRANT UPDATE (id) ON public.fugue_tenants TO fugue_app_db_import_definer;
GRANT UPDATE (status, started_at, completed_at, updated_at, error_message, result_message, logs_json)
    ON public.fugue_app_database_import_jobs TO fugue_app_db_import_definer;
GRANT SELECT (id, tenant_id, filename, archive_bytes)
    ON public.fugue_source_uploads TO fugue_app_db_import_definer;

CREATE FUNCTION fugue_app_db_import_worker.owner_v1()
RETURNS text
LANGUAGE sql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog
AS $owner$
    SELECT COALESCE((SELECT value FROM public.fugue_meta WHERE key = 'app-database-import/v1/writer'), 'api')
$owner$;

-- All functions admitting work must take this lock before reading the owner.
-- Do not use a snapshot older than the activation or a lease renewal.
CREATE FUNCTION fugue_app_db_import_worker.can_execute_v1()
RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY INVOKER
SET search_path = pg_catalog
AS $gate$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed'
       OR NOT pg_try_advisory_xact_lock_shared(hashtextextended('app-database-import/v1/writer', 0)) THEN
        RETURN false;
    END IF;
    RETURN fugue_app_db_import_worker.owner_v1() = 'worker';
END
$gate$;

CREATE FUNCTION fugue_app_db_import_worker.pending_v1(p_limit int)
RETURNS SETOF text
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog
AS $pending$
BEGIN
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 5 THEN
        RAISE EXCEPTION 'invalid database import batch' USING ERRCODE = '22023';
    END IF;
    IF NOT fugue_app_db_import_worker.can_execute_v1() THEN RETURN; END IF;
    RETURN QUERY
        SELECT pending.id FROM public.fugue_app_database_import_jobs pending
        JOIN public.fugue_tenants t ON t.id = pending.tenant_id
        WHERE pending.status = 'pending' AND t.status <> 'access-held'
          AND NOT EXISTS (SELECT FROM public.fugue_app_database_import_jobs running
                          WHERE running.app_id = pending.app_id AND running.status = 'running')
        ORDER BY pending.created_at, pending.id LIMIT p_limit;
END
$pending$;

CREATE FUNCTION fugue_app_db_import_worker.claim_v1(p_job text, p_attempt text)
RETURNS TABLE(job_id text, import_format text, clean boolean)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog SET lock_timeout = '250ms'
AS $claim$
DECLARE
    job_app text;
    job_tenant text;
    app_tenant text;
    app_name text;
    app_spec jsonb;
    app_status jsonb;
    at_time timestamptz;
BEGIN
    IF p_job IS NULL OR octet_length(p_job) NOT BETWEEN 1 AND 128
       OR p_attempt IS NULL OR octet_length(p_attempt) NOT BETWEEN 1 AND 128 THEN
        RAISE EXCEPTION 'invalid database import claim' USING ERRCODE = '22023';
    END IF;
    IF NOT fugue_app_db_import_worker.can_execute_v1() THEN RETURN; END IF;
    SELECT j.app_id INTO job_app FROM public.fugue_app_database_import_jobs j WHERE j.id = p_job;
    IF NOT FOUND THEN RETURN; END IF;

    -- Match foreground lifecycle changes and recovery: lock the app first,
    -- then recheck queued state on the current job under its row lock.
    SELECT a.tenant_id, a.name, a.spec_json, a.status_json INTO app_tenant, app_name, app_spec, app_status
    FROM public.fugue_apps a WHERE a.id = job_app FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN RETURN; END IF;
    IF lower(btrim(app_status->>'phase')) = 'deleted'
       OR (btrim(regexp_replace(lower(btrim(app_name)) COLLATE "C", '[^a-z0-9]+', '-', 'g'), '-') ~ '-deleted($|-)'
           AND COALESCE((app_spec->>'replicas')::int, 0) <= 0
           AND COALESCE((app_status->>'current_replicas')::int, 0) <= 0
           AND COALESCE(btrim(app_status->>'current_runtime_id'), '') = '') THEN
        RETURN;
    END IF;
    SELECT j.tenant_id INTO job_tenant FROM public.fugue_app_database_import_jobs j
    WHERE j.id = p_job AND j.app_id = job_app AND j.status = 'pending' FOR UPDATE;
    IF NOT FOUND OR job_tenant IS DISTINCT FROM app_tenant THEN RETURN; END IF;
    PERFORM 1 FROM public.fugue_tenants t WHERE t.id = job_tenant AND t.status <> 'access-held' FOR SHARE;
    IF NOT FOUND OR EXISTS (SELECT FROM public.fugue_app_database_import_jobs j
                         WHERE j.app_id = job_app AND j.status = 'running') THEN
        RETURN;
    END IF;

    at_time := clock_timestamp();
    IF app_spec #>> '{postgres,suspended}' = 'true'
       OR EXISTS (SELECT FROM public.fugue_service_bindings b
                  JOIN public.fugue_backing_services s ON s.id = b.service_id
                  WHERE b.app_id = job_app AND lower(btrim(s.type)) = 'postgres'
                    AND lower(btrim(s.provisioner)) IN ('','managed') AND lower(btrim(s.status)) <> 'deleted'
                    AND s.spec_json #>> '{postgres,suspended}' = 'true')
       OR EXISTS (SELECT FROM public.fugue_operations o WHERE o.app_id = job_app
                  AND o.type IN ('database-suspend','database-resume','database-resize','database-recover',
                                 'database-image-migrate','database-replica-retire','database-source-retire')
                  AND o.status IN ('pending','running','waiting-agent')) THEN
        UPDATE public.fugue_app_database_import_jobs j
        SET status = 'failed', result_message = '', started_at = NULL, completed_at = at_time, updated_at = at_time,
            error_message = 'managed_postgres_unavailable_for_database_import: resume the database or wait for database suspend, resume, or resize to finish before importing'
        WHERE j.id = p_job;
        RETURN;
    END IF;

    INSERT INTO public.fugue_meta(key, value, updated_at) VALUES (
        'app-database-import/v1/attempt/' || p_job,
        jsonb_build_object('attempt', p_attempt, 'phase', 'preparing')::text, at_time);
    RETURN QUERY UPDATE public.fugue_app_database_import_jobs j
        SET status = 'running', started_at = at_time, updated_at = at_time
        WHERE j.id = p_job RETURNING j.id, j.format, j.clean;
END
$claim$;

CREATE FUNCTION fugue_app_db_import_worker.archive_v1(p_job text, p_attempt text, p_max_bytes int)
RETURNS TABLE(filename text, archive bytea)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog
AS $archive$
BEGIN
    IF p_job IS NULL OR octet_length(p_job) NOT BETWEEN 1 AND 128
       OR p_attempt IS NULL OR octet_length(p_attempt) NOT BETWEEN 1 AND 128
       OR p_max_bytes IS NULL OR p_max_bytes NOT BETWEEN 1 AND 134217728 THEN
        RAISE EXCEPTION 'invalid database import archive request' USING ERRCODE = '22023';
    END IF;
    IF NOT fugue_app_db_import_worker.can_execute_v1() THEN RETURN; END IF;
    RETURN QUERY
        SELECT upload.filename, upload.archive_bytes
        FROM public.fugue_app_database_import_jobs job
        JOIN public.fugue_meta attempt ON attempt.key = 'app-database-import/v1/attempt/' || job.id
        JOIN public.fugue_source_uploads upload ON upload.id = job.source_upload_id AND upload.tenant_id = job.tenant_id
        JOIN public.fugue_tenants t ON t.id = job.tenant_id
        WHERE job.id = p_job AND job.status = 'running' AND t.status <> 'access-held'
          AND attempt.updated_at > clock_timestamp() - interval '60 seconds'
          AND attempt.value::jsonb->>'attempt' = p_attempt AND attempt.value::jsonb->>'phase' = 'preparing'
          AND octet_length(upload.archive_bytes) BETWEEN 1 AND p_max_bytes;
END
$archive$;

-- Return connection material for only the database selected by the normal
-- managed binding order. Recheck the current tenant, lifecycle and binding
-- on the same snapshot; never return the app or service environment.
CREATE FUNCTION fugue_app_db_import_worker.connection_v1(p_job text, p_attempt text)
RETURNS TABLE(service_id text, tenant_id text, endpoint text, database_name text, login_name text, login_password text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog
AS $connection$
BEGIN
    IF p_job IS NULL OR octet_length(p_job) NOT BETWEEN 1 AND 128
       OR p_attempt IS NULL OR octet_length(p_attempt) NOT BETWEEN 1 AND 128 THEN
        RAISE EXCEPTION 'invalid database import connection request' USING ERRCODE = '22023';
    END IF;
    IF NOT fugue_app_db_import_worker.can_execute_v1() THEN RETURN; END IF;
    RETURN QUERY
        SELECT target.id, a.tenant_id,
            coalesce(nullif(btrim(target.pg->>'endpoint_service_name'),''), btrim(target.pg->>'service_name')),
            target.pg->>'database', target.pg->>'user', target.pg->>'password'
        FROM public.fugue_app_database_import_jobs j
        JOIN public.fugue_meta m ON m.key = 'app-database-import/v1/attempt/' || j.id
        JOIN public.fugue_apps a ON a.id = j.app_id AND a.tenant_id = j.tenant_id
        JOIN public.fugue_tenants t ON t.id = j.tenant_id
        CROSS JOIN LATERAL (
            SELECT s.id, s.tenant_id, b.tenant_id AS binding_tenant_id, s.spec_json->'postgres' AS pg
            FROM public.fugue_service_bindings b JOIN public.fugue_backing_services s ON s.id = b.service_id
            WHERE b.app_id = a.id AND lower(btrim(s.type)) = 'postgres'
              AND lower(btrim(s.provisioner)) IN ('','managed') AND lower(btrim(s.status)) <> 'deleted'
            ORDER BY coalesce(s.owner_app_id = a.id, false) DESC, b.created_at, b.id
            LIMIT 1
        ) AS target
        WHERE j.id = p_job AND j.status = 'running' AND t.status <> 'access-held'
          AND m.updated_at > clock_timestamp() - interval '60 seconds'
          AND m.value::jsonb->>'attempt' = p_attempt AND m.value::jsonb->>'phase' = 'preparing'
          AND coalesce(lower(btrim(a.status_json->>'phase')),'') <> 'deleted'
          AND NOT (btrim(regexp_replace(lower(btrim(a.name)) COLLATE "C", '[^a-z0-9]+', '-', 'g'), '-') ~ '-deleted($|-)'
               AND coalesce((a.spec_json->>'replicas')::int, 0) <= 0
               AND coalesce((a.status_json->>'current_replicas')::int, 0) <= 0
               AND coalesce(btrim(a.status_json->>'current_runtime_id'), '') = '')
          AND target.tenant_id = a.tenant_id AND target.binding_tenant_id = a.tenant_id
          AND jsonb_typeof(target.pg) = 'object'
          AND coalesce(a.spec_json #>> '{postgres,suspended}','false') <> 'true'
          AND (jsonb_typeof(a.spec_json->'postgres') = 'object' OR EXISTS (
              SELECT FROM public.fugue_service_bindings b JOIN public.fugue_backing_services s ON s.id = b.service_id
              WHERE b.app_id = a.id AND b.tenant_id = a.tenant_id AND s.tenant_id = a.tenant_id
                AND (nullif(btrim(s.owner_app_id),'') IS NULL OR btrim(s.owner_app_id) = a.id)
                AND lower(btrim(s.type)) = 'postgres' AND lower(btrim(s.provisioner)) IN ('','managed')
                AND lower(btrim(s.status)) <> 'deleted' AND jsonb_typeof(s.spec_json->'postgres') = 'object'))
          AND NOT EXISTS (SELECT FROM public.fugue_service_bindings b
              JOIN public.fugue_backing_services s ON s.id = b.service_id
              WHERE b.app_id = a.id AND lower(btrim(s.type)) = 'postgres'
                AND lower(btrim(s.provisioner)) IN ('','managed') AND lower(btrim(s.status)) <> 'deleted'
                AND s.spec_json #>> '{postgres,suspended}' = 'true')
          AND NOT EXISTS (SELECT FROM public.fugue_operations o WHERE o.app_id = a.id
              AND o.type IN ('database-suspend','database-resume','database-resize','database-recover',
                             'database-image-migrate','database-replica-retire','database-source-retire')
              AND o.status IN ('pending','running','waiting-agent'))
          AND coalesce(nullif(btrim(target.pg->>'endpoint_service_name'),''), btrim(target.pg->>'service_name'))
              ~ '^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'
          AND coalesce(target.pg->>'database','') <> '' AND coalesce(target.pg->>'user','') <> ''
          AND coalesce(target.pg->>'password','') <> '';
END
$connection$;

CREATE FUNCTION fugue_app_db_import_worker.touch_v1(p_job text, p_attempt text, p_begin_effect boolean)
RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog SET lock_timeout = '250ms'
AS $touch$
DECLARE
    tenant text;
    phase text;
    attempt_key text := 'app-database-import/v1/attempt/' || p_job;
BEGIN
    IF p_job IS NULL OR octet_length(p_job) NOT BETWEEN 1 AND 128
       OR p_attempt IS NULL OR octet_length(p_attempt) NOT BETWEEN 1 AND 128
       OR p_begin_effect IS NULL THEN
        RAISE EXCEPTION 'invalid database import lease request' USING ERRCODE = '22023';
    END IF;
    IF NOT fugue_app_db_import_worker.can_execute_v1() THEN RETURN false; END IF;

    -- Same lock order as the compatible executor and recovery: job, then
    -- attempt. Recovery additionally takes the app lock before either.
    SELECT j.tenant_id INTO tenant FROM public.fugue_app_database_import_jobs j
    WHERE j.id = p_job AND j.status = 'running' FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;

    SELECT m.value::jsonb->>'phase' INTO phase FROM public.fugue_meta m
    WHERE m.key = attempt_key AND m.value::jsonb->>'attempt' = p_attempt
      AND m.updated_at > clock_timestamp() - interval '60 seconds' FOR UPDATE;
    IF NOT FOUND OR phase IS NULL OR phase NOT IN ('preparing','executing') THEN RETURN false; END IF;
    IF p_begin_effect THEN
        IF phase <> 'preparing' THEN RETURN false; END IF;
        PERFORM 1 FROM public.fugue_tenants t WHERE t.id = tenant AND t.status <> 'access-held' FOR SHARE;
        IF NOT FOUND THEN RETURN false; END IF;
        phase := 'executing';
    END IF;
    UPDATE public.fugue_meta m SET
        value = jsonb_build_object('attempt', p_attempt, 'phase', phase)::text,
        updated_at = clock_timestamp()
    WHERE m.key = attempt_key AND m.updated_at > clock_timestamp() - interval '60 seconds';
    RETURN FOUND;
END
$touch$;

-- Caller holds the app lock and has verified an executing attempt. Keep
-- these messages stable: the compatibility API refuses retries from history
-- using these exact persisted outcomes.
CREATE FUNCTION fugue_app_db_import_worker.fail_uncertain_v1(p_app text, p_job text, p_at timestamptz)
RETURNS void
LANGUAGE sql VOLATILE SECURITY INVOKER
SET search_path = pg_catalog
AS $failure$
    UPDATE public.fugue_app_database_import_jobs j
    SET status = 'failed', error_message = failure.message, completed_at = p_at, updated_at = p_at,
        logs_json = COALESCE(NULLIF(j.logs_json,'null'::jsonb),'[]'::jsonb)
                    || jsonb_build_array(jsonb_build_object('at',p_at,'message',failure.message))
    FROM (VALUES
        (true, 'database import outcome is unknown after SQL execution began; inspect the target database before submitting another import'),
        (false, 'queued database import did not run because an earlier import''s outcome is unknown; inspect the target database before submitting another import')
    ) AS failure(running,message)
    WHERE j.app_id = p_app AND
        ((failure.running AND j.id = p_job AND j.status = 'running')
          OR (NOT failure.running AND j.status = 'pending'))
$failure$;

CREATE FUNCTION fugue_app_db_import_worker.recover_v1(p_limit int)
RETURNS integer
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog SET lock_timeout = '250ms'
AS $recovery$
DECLARE
    job record;
    phase text;
    recovered integer := 0;
    attempt_prefix CONSTANT text := 'app-database-import/v1/attempt/';
BEGIN
    IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 5 THEN
        RAISE EXCEPTION 'invalid database import recovery batch' USING ERRCODE = '22023';
    END IF;
    IF NOT fugue_app_db_import_worker.can_execute_v1() THEN RETURN 0; END IF;
    FOR job IN
        SELECT j.id, j.app_id FROM public.fugue_app_database_import_jobs j
        JOIN public.fugue_apps a ON a.id = j.app_id
        JOIN public.fugue_meta m ON m.key = attempt_prefix || j.id
        WHERE j.status = 'running' AND m.updated_at <= clock_timestamp() - interval '60 seconds'
        ORDER BY j.started_at, j.id LIMIT p_limit FOR UPDATE OF a SKIP LOCKED
    LOOP
        -- Same app -> job -> attempt order as claim/finalize. Recheck
        -- heartbeat after the job lock to avoid expiring a renewed lease.
        PERFORM 1 FROM public.fugue_app_database_import_jobs j
        WHERE j.id = job.id AND j.status = 'running' FOR UPDATE SKIP LOCKED;
        IF NOT FOUND THEN CONTINUE; END IF;
        SELECT m.value::jsonb->>'phase' INTO phase FROM public.fugue_meta m
        WHERE m.key = attempt_prefix || job.id AND m.updated_at <= clock_timestamp() - interval '60 seconds'
        FOR UPDATE;
        IF NOT FOUND THEN CONTINUE; END IF;
        IF phase = 'preparing' THEN
            UPDATE public.fugue_app_database_import_jobs j
            SET status = 'pending', started_at = NULL, updated_at = clock_timestamp()
            WHERE j.id = job.id;
        ELSIF phase = 'executing' THEN
            PERFORM fugue_app_db_import_worker.fail_uncertain_v1(job.app_id, job.id, clock_timestamp());
        ELSE
            RAISE EXCEPTION 'unknown database import attempt phase';
        END IF;
        DELETE FROM public.fugue_meta m WHERE m.key = attempt_prefix || job.id;
        recovered := recovered + 1;
    END LOOP;
    RETURN recovered;
END
$recovery$;

-- Callers hold the job row lock before taking the attempt lock. The helper
-- is not exposed to the worker login.
CREATE FUNCTION fugue_app_db_import_worker.lock_attempt_v1(p_job text, p_attempt text)
RETURNS text
LANGUAGE sql VOLATILE SECURITY INVOKER
SET search_path = pg_catalog
AS $lease$
    SELECT m.value::jsonb->>'phase' FROM public.fugue_meta m
    WHERE m.key = 'app-database-import/v1/attempt/' || p_job AND m.value::jsonb->>'attempt' = p_attempt
      AND m.updated_at > clock_timestamp() - interval '60 seconds' FOR UPDATE
$lease$;

CREATE FUNCTION fugue_app_db_import_worker.append_log_v1(p_job text, p_attempt text, p_message text)
RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog SET lock_timeout = '250ms'
AS $append$
DECLARE
    at_time timestamptz;
BEGIN
    IF p_job IS NULL OR octet_length(p_job) NOT BETWEEN 1 AND 128
       OR p_attempt IS NULL OR octet_length(p_attempt) NOT BETWEEN 1 AND 128
       OR p_message IS NULL OR octet_length(p_message) NOT BETWEEN 1 AND 8192 THEN
        RAISE EXCEPTION 'invalid database import log' USING ERRCODE = '22023';
    END IF;
    IF NOT fugue_app_db_import_worker.can_execute_v1() THEN RETURN false; END IF;
    PERFORM 1 FROM public.fugue_app_database_import_jobs j WHERE j.id = p_job AND j.status = 'running' FOR UPDATE;
    IF NOT FOUND OR coalesce(fugue_app_db_import_worker.lock_attempt_v1(p_job,p_attempt),'') NOT IN ('preparing','executing') THEN
        RETURN false;
    END IF;
    at_time := clock_timestamp();
    UPDATE public.fugue_app_database_import_jobs j
    SET updated_at = at_time, logs_json = coalesce(nullif(j.logs_json,'null'::jsonb),'[]'::jsonb)
        || jsonb_build_array(jsonb_build_object('at',at_time,'message',p_message))
    WHERE j.id = p_job;
    RETURN true;
END
$append$;

CREATE FUNCTION fugue_app_db_import_worker.complete_v1(p_job text, p_attempt text, p_status text, p_result text, p_error text)
RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog SET lock_timeout = '250ms'
AS $complete$
DECLARE
    job_app text;
    phase text;
    at_time timestamptz;
    terminal_log text;
BEGIN
    IF p_job IS NULL OR octet_length(p_job) NOT BETWEEN 1 AND 128
       OR p_attempt IS NULL OR octet_length(p_attempt) NOT BETWEEN 1 AND 128
       OR p_status IS NULL OR p_status NOT IN ('completed','failed')
       OR p_result IS NULL OR octet_length(p_result) > 8192 OR p_error IS NULL OR octet_length(p_error) > 8192
       OR (p_status = 'failed' AND p_result <> '') OR (p_status = 'completed' AND p_error <> '') THEN
        RAISE EXCEPTION 'invalid database import result' USING ERRCODE = '22023';
    END IF;
    IF NOT fugue_app_db_import_worker.can_execute_v1() THEN RETURN false; END IF;

    -- Lock app -> job -> attempt, matching claim, recovery and enqueue.
    SELECT a.id INTO job_app FROM public.fugue_apps a
        JOIN public.fugue_app_database_import_jobs j ON a.id = j.app_id WHERE j.id = p_job FOR UPDATE OF a;
    IF NOT FOUND THEN RETURN false; END IF;
    PERFORM 1 FROM public.fugue_app_database_import_jobs j
        WHERE j.id = p_job AND j.app_id = job_app AND j.status = 'running' FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    phase := fugue_app_db_import_worker.lock_attempt_v1(p_job,p_attempt);
    IF phase IS NULL OR phase NOT IN ('preparing','executing') OR (p_status = 'completed' AND phase <> 'executing') THEN
        RETURN false;
    END IF;

    at_time := clock_timestamp();
    IF p_status = 'failed' AND phase = 'executing' THEN
        IF p_error <> '' THEN
            UPDATE public.fugue_app_database_import_jobs j
            SET logs_json = coalesce(nullif(j.logs_json,'null'::jsonb),'[]'::jsonb)
                || jsonb_build_array(jsonb_build_object('at',at_time,'message',p_error))
            WHERE j.id = p_job;
        END IF;
        PERFORM fugue_app_db_import_worker.fail_uncertain_v1(job_app,p_job,at_time);
    ELSE
        terminal_log := CASE WHEN p_status = 'completed' THEN 'database import completed' ELSE p_error END;
        UPDATE public.fugue_app_database_import_jobs j
        SET status = p_status, result_message = p_result, error_message = p_error, completed_at = at_time, updated_at = at_time,
            logs_json = coalesce(nullif(j.logs_json,'null'::jsonb),'[]'::jsonb)
                        || jsonb_build_array(jsonb_build_object('at',at_time,'message',terminal_log))
        WHERE j.id = p_job;
    END IF;
    DELETE FROM public.fugue_meta m WHERE m.key = 'app-database-import/v1/attempt/' || p_job;
    RETURN true;
END
$complete$;

-- Assign function ownership without giving the definer lasting DDL rights.
GRANT CREATE ON SCHEMA fugue_app_db_import_worker TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.owner_v1() OWNER TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.can_execute_v1() OWNER TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.pending_v1(int) OWNER TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.claim_v1(text,text) OWNER TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.archive_v1(text,text,int) OWNER TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.connection_v1(text,text) OWNER TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.touch_v1(text,text,boolean) OWNER TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.fail_uncertain_v1(text,text,timestamptz) OWNER TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.recover_v1(int) OWNER TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.lock_attempt_v1(text,text) OWNER TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.append_log_v1(text,text,text) OWNER TO fugue_app_db_import_definer;
ALTER FUNCTION fugue_app_db_import_worker.complete_v1(text,text,text,text,text) OWNER TO fugue_app_db_import_definer;
REVOKE CREATE ON SCHEMA fugue_app_db_import_worker FROM fugue_app_db_import_definer;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA fugue_app_db_import_worker FROM PUBLIC;
GRANT EXECUTE ON FUNCTION fugue_app_db_import_worker.owner_v1(), fugue_app_db_import_worker.pending_v1(int),
    fugue_app_db_import_worker.claim_v1(text,text),
    fugue_app_db_import_worker.archive_v1(text,text,int), fugue_app_db_import_worker.connection_v1(text,text),
    fugue_app_db_import_worker.touch_v1(text,text,boolean),
    fugue_app_db_import_worker.recover_v1(int), fugue_app_db_import_worker.append_log_v1(text,text,text),
    fugue_app_db_import_worker.complete_v1(text,text,text,text,text)
    TO fugue_app_db_import_worker;
COMMIT;
