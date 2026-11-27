"""SQL for the forward-only edge quality rollup ownership handoff.

This module deliberately has no executable entrypoint. The release driver must
verify the live API/worker before calling it through the bound CNPG primary.
"""

import re

from scripts.provision_observation_worker_database import BINDING, CHECKPOINT, SCHEMA, UID


WRITER_LOCK = "edge-quality-rollup-builder"
FUNCTION = "fugue_check_edge_quality_writer"
TABLES = ("fugue_edge_quality_rollups", "fugue_edge_quality_rollup_watermarks")
OPERATIONS = {
    "insert": "REFERENCING NEW TABLE AS fugue_rollup_changes",
    "update": "REFERENCING NEW TABLE AS fugue_rollup_changes",
    "delete": "REFERENCING OLD TABLE AS fugue_rollup_changes",
    "truncate": "",
}


def handoff_sql(provision, secret_uid):
    """Atomically fence direct legacy writes and assign the durable owner.

    No-op writes may still run: old API schema startup includes an INSERT
    ... SELECT that inserts nothing once watermarks already exist. Inspect
    the actual changed rows after the statement rather than blocking it.
    """
    if not isinstance(secret_uid, str) or not re.fullmatch(UID, secret_uid):
        raise ValueError(f"invalid credential identity for {BINDING}")
    role = provision.config["role"]  # Already validated by Provision.
    marker = f"{SCHEMA}:{secret_uid}"
    acquire_table_locks = ", ".join("public." + name for name in TABLES)
    install = []
    checks = []
    for table in TABLES:
        for operation, transition in OPERATIONS.items():
            trigger = f"fugue_quality_writer_{operation}"
            install += [
                f"CREATE TRIGGER {trigger} AFTER {operation.upper()} ON public.{table} "
                f"{transition} FOR EACH STATEMENT WHEN (session_user <> '{role}') "
                f"EXECUTE FUNCTION public.{FUNCTION}();",
                f"ALTER TABLE public.{table} ENABLE ALWAYS TRIGGER {trigger};",
            ]
            checks.append(f"(tgrelid = 'public.{table}'::regclass AND tgname = '{trigger}')")
    installed = "\n    ".join(install)
    expected = " OR ".join(checks)
    return f"""
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '8s';
{provision.identity_sql()}
DO $handoff$
DECLARE owner text;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles
        WHERE rolname = '{role}' AND rolcanlogin
          AND shobj_description(oid, 'pg_authid') = '{marker}'
    ) THEN
        RAISE EXCEPTION 'dedicated observation login no longer matches its Secret';
    END IF;
    IF NOT pg_try_advisory_xact_lock(hashtextextended('{WRITER_LOCK}', 0)) THEN
        RAISE EXCEPTION 'rollup writer is busy; retry handoff after it drains';
    END IF;
    LOCK TABLE {acquire_table_locks} IN SHARE ROW EXCLUSIVE MODE NOWAIT;
    SELECT value INTO owner FROM public.fugue_meta WHERE key = '{CHECKPOINT}' FOR UPDATE;
    IF owner = 'worker' THEN
        IF (SELECT count(*) FROM pg_trigger WHERE tgenabled = 'A'
            AND tgfoid = to_regprocedure('public.{FUNCTION}()') AND ({expected})) <> {len(checks)} THEN
            RAISE EXCEPTION 'existing worker owner has no complete database fence';
        END IF;
        RETURN;
    END IF;
    IF COALESCE(owner, '') <> '' THEN
        RAISE EXCEPTION 'unexpected persisted owner';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM public.fugue_edge_quality_rollup_watermarks WHERE window_name = '5m') THEN
        RAISE EXCEPTION 'missing last known five-minute watermark';
    END IF;
    CREATE FUNCTION public.{FUNCTION}() RETURNS trigger LANGUAGE plpgsql
        SET search_path = pg_catalog AS $guard$
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'observation rollup writes belong to the worker' USING ERRCODE = 'insufficient_privilege';
        END IF;
        IF EXISTS (SELECT 1 FROM fugue_rollup_changes) THEN
            RAISE EXCEPTION 'observation rollup writes belong to the worker' USING ERRCODE = 'insufficient_privilege';
        END IF;
        RETURN NULL;
    END
    $guard$;
    {installed}
    INSERT INTO public.fugue_meta (key, value, updated_at) VALUES ('{CHECKPOINT}', 'worker', NOW())
       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;
END
$handoff$;
SELECT json_build_object('watermark', window_ended_at, 'cutoverUnix', EXTRACT(EPOCH FROM clock_timestamp()))
FROM public.fugue_edge_quality_rollup_watermarks WHERE window_name = '5m';
COMMIT;
"""
