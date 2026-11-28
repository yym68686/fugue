BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '250ms';

-- The release lane must already have proved the running and rollback
-- artifacts. Neither startup, readiness nor a reconciler runs this file.
DO $handoff$
DECLARE
    owner text;
BEGIN
    -- Same order and PostgreSQL keys as the API's periodic path. This also
    -- refuses to race a session lock from a pre-compatibility binary.
    IF NOT pg_try_advisory_xact_lock(hashtextextended('app-mail-reconcile', 0))
       OR NOT pg_try_advisory_xact_lock(hashtextextended('app-mail-reconcile/owner/v1', 0)) THEN
        RAISE EXCEPTION 'application mail API writer has not drained';
    END IF;
    LOCK TABLE public.fugue_app_mail_bindings IN SHARE MODE NOWAIT;
    LOCK TABLE public.fugue_app_mail_reconcile IN SHARE MODE NOWAIT;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'public.fugue_app_mail_bindings'::regclass
          AND tgname = 'fugue_app_mail_reconcile_enqueue' AND tgenabled IN ('O','A')
          AND tgtype = 21
          AND tgfoid = 'public.fugue_enqueue_app_mail_reconcile()'::regprocedure
    ) THEN
        RAISE EXCEPTION 'application mail generation trigger is not enabled';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM public.fugue_meta
        WHERE key = 'app-mail-reconcile/seed/v1' AND value::jsonb->>'complete' = 'true'
          AND coalesce(value::jsonb->>'after','') = ''
    ) OR EXISTS (
        SELECT 1 FROM public.fugue_app_mail_bindings binding
        LEFT JOIN public.fugue_app_mail_reconcile work ON work.app_id = binding.app_id
        WHERE work.generation IS DISTINCT FROM binding.generation
    ) THEN
        RAISE EXCEPTION 'application mail work does not cover all current generations';
    END IF;
    SELECT value INTO owner FROM public.fugue_meta WHERE key = 'app-mail-reconcile/owner/v1';
    IF owner = 'controller' THEN
        RETURN; -- retry observes the original activation timestamp
    END IF;
    IF owner IS NOT NULL AND owner <> 'api' THEN
        RAISE EXCEPTION 'unsupported application mail owner';
    END IF;
    INSERT INTO public.fugue_meta(key,value,updated_at)
    VALUES('app-mail-reconcile/owner/v1','controller',statement_timestamp())
    ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
    WHERE fugue_meta.value = 'api';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'application mail owner changed concurrently';
    END IF;
END
$handoff$;
COMMIT;
