-- Candidate for the separately admitted import-worker handoff. Never run on
-- API/worker startup. The release must first verify the live and rollback
-- artifacts and the independently provisioned import-worker capability.
BEGIN ISOLATION LEVEL READ COMMITTED;
SET LOCAL statement_timeout = '8s';
SET LOCAL lock_timeout = '250ms';
SET LOCAL search_path = pg_catalog;

DO $activation$
DECLARE
    owner text;
    writer_key CONSTANT text := 'app-database-import/v1/writer';
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = current_user AND rolsuper) THEN
        RAISE EXCEPTION 'database import handoff requires the database administrator';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles executor WHERE executor.rolname = 'fugue_app_db_import_definer'
                   AND NOT (executor.rolcanlogin OR executor.rolsuper OR executor.rolcreatedb OR executor.rolcreaterole
                            OR executor.rolbypassrls OR executor.rolreplication)
                   AND NOT EXISTS (SELECT FROM pg_auth_members membership
                                   WHERE membership.roleid = executor.oid OR membership.member = executor.oid)) THEN
        RAISE EXCEPTION 'database import execution owner is not an isolated NOLOGIN role';
    END IF;
    IF NOT pg_try_advisory_xact_lock(hashtextextended(writer_key, 0)) THEN
        RAISE EXCEPTION 'database import executor has not drained';
    END IF;
    -- Old API versions do not take the advisory lock. Stop concurrent queue
    -- writes while checking that no import is running and installing the fence.
    LOCK TABLE public.fugue_app_database_import_jobs IN SHARE ROW EXCLUSIVE MODE NOWAIT;
    LOCK TABLE public.fugue_meta IN SHARE ROW EXCLUSIVE MODE NOWAIT;
    SELECT value INTO owner FROM public.fugue_meta WHERE key = writer_key FOR UPDATE;
    IF owner = 'worker' THEN
        IF NOT EXISTS (SELECT FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid
            JOIN pg_roles r ON r.oid = p.proowner
            WHERE t.tgrelid = 'public.fugue_app_database_import_jobs'::regclass
              AND t.tgname = 'fugue_app_db_import_writer_v1' AND t.tgtype = 23 AND t.tgenabled = 'A'
              AND t.tgfoid = to_regprocedure('fugue_app_db_import_fence.guard_job_writes_v1()')
              AND r.rolsuper AND NOT p.prosecdef) THEN
            RAISE EXCEPTION 'database import worker owner exists without its write fence';
        END IF;
        IF NOT EXISTS (SELECT FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid
            JOIN pg_roles r ON r.oid = p.proowner
            WHERE t.tgrelid = 'public.fugue_meta'::regclass
              AND t.tgname = 'fugue_app_db_import_meta_v1' AND t.tgtype = 31 AND t.tgenabled = 'A'
              AND t.tgfoid = to_regprocedure('fugue_app_db_import_fence.guard_metadata_v1()')
              AND r.rolsuper AND NOT p.prosecdef) THEN
            RAISE EXCEPTION 'database import worker owner exists without its metadata fence';
        END IF;
        RETURN;
    END IF;
    IF owner IS NOT NULL AND owner <> 'api' THEN
        RAISE EXCEPTION 'unexpected database import owner';
    END IF;
    IF EXISTS (SELECT FROM public.fugue_app_database_import_jobs WHERE status = 'running') THEN
        RAISE EXCEPTION 'database import queue still has a running attempt';
    END IF;

    CREATE SCHEMA fugue_app_db_import_fence;
    REVOKE ALL ON SCHEMA fugue_app_db_import_fence FROM PUBLIC;
    CREATE FUNCTION fugue_app_db_import_fence.guard_job_writes_v1()
    RETURNS trigger
    LANGUAGE plpgsql VOLATILE SECURITY INVOKER
    SET search_path = pg_catalog
    AS $fence$
    BEGIN
        IF TG_WHEN <> 'BEFORE' OR TG_LEVEL <> 'ROW'
           OR TG_RELID <> 'public.fugue_app_database_import_jobs'::regclass THEN
            RAISE EXCEPTION 'database import fence attached outside its intended scope';
        END IF;
        IF TG_OP = 'INSERT' AND NEW.status = 'pending'
           AND NEW.started_at IS NULL AND NEW.completed_at IS NULL
           AND NEW.result_message = '' AND NEW.error_message = '' THEN
            RETURN NEW;
        END IF;
        IF TG_OP = 'UPDATE' AND current_user = 'fugue_app_db_import_definer' THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'database import execution belongs to the worker' USING ERRCODE = '42501';
    END
    $fence$;
    REVOKE ALL ON FUNCTION fugue_app_db_import_fence.guard_job_writes_v1() FROM PUBLIC;
    CREATE TRIGGER fugue_app_db_import_writer_v1
    BEFORE INSERT OR UPDATE ON public.fugue_app_database_import_jobs
    FOR EACH ROW EXECUTE FUNCTION fugue_app_db_import_fence.guard_job_writes_v1();
    ALTER TABLE public.fugue_app_database_import_jobs ENABLE ALWAYS TRIGGER fugue_app_db_import_writer_v1;

    INSERT INTO public.fugue_meta(key, value, updated_at)
    VALUES (writer_key, 'worker', clock_timestamp())
    ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
    WHERE fugue_meta.value = 'api';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'database import owner changed concurrently';
    END IF;

    CREATE FUNCTION fugue_app_db_import_fence.guard_metadata_v1()
    RETURNS trigger
    LANGUAGE plpgsql VOLATILE SECURITY INVOKER
    SET search_path = pg_catalog
    AS $metadata$
    DECLARE
        old_key text;
        new_key text;
        old_attempt boolean;
        new_attempt boolean;
        writer_key CONSTANT text := 'app-database-import/v1/writer';
        attempt_prefix CONSTANT text := 'app-database-import/v1/attempt/';
    BEGIN
        IF TG_WHEN <> 'BEFORE' OR TG_LEVEL <> 'ROW' OR TG_RELID <> 'public.fugue_meta'::regclass THEN
            RAISE EXCEPTION 'database import metadata fence attached outside its intended scope';
        END IF;
        IF TG_OP <> 'INSERT' THEN old_key := OLD.key; END IF;
        IF TG_OP <> 'DELETE' THEN new_key := NEW.key; END IF;
        IF old_key = writer_key OR new_key = writer_key THEN
            RAISE EXCEPTION 'database import owner cannot change after handoff' USING ERRCODE = '42501';
        END IF;
        old_attempt := COALESCE(starts_with(old_key, attempt_prefix), false);
        new_attempt := COALESCE(starts_with(new_key, attempt_prefix), false);
        IF (old_attempt OR new_attempt)
           AND (current_user <> 'fugue_app_db_import_definer'
                OR (TG_OP = 'UPDATE' AND old_key IS DISTINCT FROM new_key)) THEN
            RAISE EXCEPTION 'database import attempt belongs to the worker' USING ERRCODE = '42501';
        END IF;
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END
    $metadata$;
    REVOKE ALL ON FUNCTION fugue_app_db_import_fence.guard_metadata_v1() FROM PUBLIC;
    CREATE TRIGGER fugue_app_db_import_meta_v1
    BEFORE INSERT OR UPDATE OR DELETE ON public.fugue_meta
    FOR EACH ROW EXECUTE FUNCTION fugue_app_db_import_fence.guard_metadata_v1();
    ALTER TABLE public.fugue_meta ENABLE ALWAYS TRIGGER fugue_app_db_import_meta_v1;
END
$activation$;
COMMIT;
