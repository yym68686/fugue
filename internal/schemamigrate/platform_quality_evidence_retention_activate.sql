BEGIN;
SET LOCAL statement_timeout = '8s';
SET LOCAL lock_timeout = '250ms';
SELECT 1 / CASE WHEN pg_try_advisory_xact_lock(315609238744329::bigint) THEN 1 ELSE 0 END;
LOCK TABLE public.fugue_platform_quality_evidence IN SHARE ROW EXCLUSIVE MODE NOWAIT;
DO $handoff$
DECLARE
    checkpoint CONSTANT text := 'observation/v1/57c22d99570d554e355f0490da0af2384767623d38729660d03f104e8ef638ed';
    owner text;
BEGIN
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = CURRENT_USER) THEN
        RAISE EXCEPTION 'quality evidence handoff requires the database administrator';
    END IF;
    SELECT value INTO owner FROM public.fugue_meta WHERE key = checkpoint FOR UPDATE;
    IF owner = 'worker' THEN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger
            WHERE tgrelid = 'public.fugue_platform_quality_evidence'::regclass
              AND tgname = 'fugue_quality_evidence_delete_fence_v1'
              AND tgtype = 10 AND tgenabled = 'A'
              AND tgfoid = 'fugue_observation_maintenance.fence_legacy_quality_evidence_delete_v1()'::regprocedure) THEN
            RAISE EXCEPTION 'persisted quality evidence handoff fence missing or drifted';
        END IF;
        RETURN;
    END IF;
    IF FOUND THEN
        RAISE EXCEPTION 'refusing unexpected quality evidence writer checkpoint';
    END IF;
    CREATE TRIGGER fugue_quality_evidence_delete_fence_v1
    BEFORE DELETE ON public.fugue_platform_quality_evidence
    FOR EACH STATEMENT EXECUTE FUNCTION fugue_observation_maintenance.fence_legacy_quality_evidence_delete_v1();
    ALTER TABLE public.fugue_platform_quality_evidence
        ENABLE ALWAYS TRIGGER fugue_quality_evidence_delete_fence_v1;
    INSERT INTO public.fugue_meta (key, value, updated_at) VALUES (checkpoint, 'worker', clock_timestamp());
END
$handoff$;
COMMIT;
