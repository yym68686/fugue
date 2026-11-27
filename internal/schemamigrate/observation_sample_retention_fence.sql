-- Prepare the fence only. The declarative handoff must attach it to the
-- three sample tables and change the persisted writer in one transaction.
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '250ms';
DO $ownership$
BEGIN
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = CURRENT_USER) THEN
        RAISE EXCEPTION 'sample-retention fence installer requires the database administrator';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner
                   WHERE n.nspname = 'fugue_observation_maintenance' AND r.rolsuper) THEN
        RAISE EXCEPTION 'observation maintenance namespace is not administrator owned';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_namespace n,
        LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) a
        LEFT JOIN pg_roles r ON r.oid = a.grantee
        WHERE n.nspname = 'fugue_observation_maintenance' AND a.privilege_type = 'CREATE'
          AND (a.grantee = 0 OR NOT COALESCE(r.rolsuper, false))) THEN
        RAISE EXCEPTION 'observation maintenance namespace has an unsafe CREATE grant';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner
               WHERE p.oid = to_regprocedure('fugue_observation_maintenance.fence_legacy_sample_delete_v1()')
                 AND NOT r.rolsuper) THEN
        RAISE EXCEPTION 'sample-retention fence is not administrator owned';
    END IF;
END
$ownership$;

CREATE OR REPLACE FUNCTION fugue_observation_maintenance.fence_legacy_sample_delete_v1()
RETURNS trigger
LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY INVOKER
SET search_path = pg_catalog
AS $fence$
BEGIN
    IF TG_LEVEL <> 'ROW' OR TG_WHEN <> 'BEFORE' OR TG_OP <> 'DELETE'
       OR TG_RELID NOT IN ('public.fugue_edge_network_samples'::regclass,
                          'public.fugue_edge_performance_samples'::regclass,
                          'public.fugue_resource_usage_samples'::regclass) THEN
        RAISE EXCEPTION 'sample-retention fence attached outside its intended scope';
    END IF;
    -- The restricted worker can only invoke its fixed SECURITY DEFINER
    -- pruner. It has no direct DELETE or DDL privileges on these tables.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = CURRENT_USER AND rolsuper) THEN
        RETURN OLD;
    END IF;
    IF TG_RELID = 'public.fugue_resource_usage_samples'::regclass AND pg_trigger_depth() > 1 THEN
        -- A real tenant deletion cascades after its parent row is gone in
        -- this transaction. A direct historical cleanup has depth 1. A
        -- nested delete of a live tenant's sample is not a cascade either.
        IF OLD.tenant_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM public.fugue_tenants WHERE id = OLD.tenant_id
        ) THEN
            RETURN OLD;
        END IF;
    END IF;
    -- An older API may combine deletion and insertion in one transaction.
    -- Skip its row without aborting the rest of the foreground transaction.
    RETURN NULL;
END
$fence$;
REVOKE ALL ON FUNCTION fugue_observation_maintenance.fence_legacy_sample_delete_v1() FROM PUBLIC;
COMMIT;
