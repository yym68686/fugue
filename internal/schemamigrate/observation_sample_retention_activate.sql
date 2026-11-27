-- The declarative production handoff must first verify its exact database
-- identity, compatible running and rollback artifacts and worker readiness.
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '250ms';
DO $admission$
DECLARE
    target text;
    owner text;
    owner_key constant text := 'observation/v1/' ||
        encode(sha256(convert_to('observation-sample-retention-writer/v1', 'UTF8')), 'hex');
BEGIN
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = CURRENT_USER)
       OR to_regprocedure('fugue_observation_maintenance.prune_samples_v1(text,boolean,integer,integer,integer)') IS NULL
       OR to_regprocedure('fugue_observation_maintenance.fence_legacy_sample_delete_v1()') IS NULL
       OR to_regclass('fugue_observation_maintenance.sample_worker_progress') IS NULL THEN
        RAISE EXCEPTION 'sample retention prerequisites are not installed';
    END IF;
    IF NOT pg_try_advisory_xact_lock(315609238744311::bigint)
       OR NOT pg_try_advisory_xact_lock(hashtextextended(owner_key, 0)) THEN
        RAISE EXCEPTION 'legacy sample retention or ingestion transaction is active';
    END IF;
    FOREACH target IN ARRAY ARRAY['network', 'performance', 'resource-usage'] LOOP
        IF NOT pg_try_advisory_xact_lock(hashtextextended('observation-sample-retention-batch/v1/' || target, 0)) THEN
            RAISE EXCEPTION 'sample retention worker batch is active';
        END IF;
    END LOOP;
    SELECT value INTO owner FROM public.fugue_meta WHERE key = owner_key FOR UPDATE;
    IF owner IS NOT NULL AND owner <> 'worker' THEN
        RAISE EXCEPTION 'unexpected observation sample retention writer';
    END IF;
END
$admission$;

LOCK TABLE public.fugue_edge_network_samples, public.fugue_edge_performance_samples,
           public.fugue_resource_usage_samples IN SHARE ROW EXCLUSIVE MODE NOWAIT;
DO $attach$
DECLARE
    sample_table text;
BEGIN
    FOREACH sample_table IN ARRAY ARRAY['fugue_edge_network_samples', 'fugue_edge_performance_samples',
                                       'fugue_resource_usage_samples'] LOOP
        EXECUTE format('CREATE OR REPLACE TRIGGER fugue_sample_retention_delete_fence_v1 BEFORE DELETE ON public.%I
           FOR EACH ROW EXECUTE FUNCTION fugue_observation_maintenance.fence_legacy_sample_delete_v1()', sample_table);
        EXECUTE format('ALTER TABLE public.%I ENABLE ALWAYS TRIGGER fugue_sample_retention_delete_fence_v1', sample_table);
    END LOOP;
END
$attach$;

INSERT INTO public.fugue_meta (key,value,updated_at) VALUES ('observation/v1/' ||
    encode(sha256(convert_to('observation-sample-retention-writer/v1', 'UTF8')), 'hex'), 'worker', clock_timestamp())
ON CONFLICT (key) DO NOTHING;
SELECT clock_timestamp() AS transfer_observed_at;
COMMIT;
