-- The worker gets execution of the fixed functions below. It receives no
-- grants on hosted DNS tables, tenant data, or publication state.
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '250ms';

CREATE SCHEMA IF NOT EXISTS fugue_hosted_dns_maintenance;
REVOKE ALL ON SCHEMA fugue_hosted_dns_maintenance FROM PUBLIC;

DO $ownership$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner
        WHERE n.nspname = 'fugue_hosted_dns_maintenance' AND r.rolname = CURRENT_USER AND r.rolsuper
    ) OR EXISTS (
        SELECT 1 FROM pg_namespace n,
            LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) a
        WHERE n.nspname = 'fugue_hosted_dns_maintenance' AND a.privilege_type = 'CREATE' AND a.grantee <> n.nspowner
    ) THEN
        RAISE EXCEPTION 'hosted DNS maintenance requires an administrator-owned namespace';
    END IF;
END
$ownership$;

CREATE OR REPLACE FUNCTION fugue_hosted_dns_maintenance.writer_v1()
RETURNS text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog
AS $function$
    SELECT COALESCE((
        SELECT value FROM public.fugue_meta WHERE key = 'hosted-dns-flatten/v1/writer'
    ), 'api')
$function$;

CREATE OR REPLACE FUNCTION fugue_hosted_dns_maintenance.records_v1(p_after text)
RETURNS SETOF jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog
AS $function$
    SELECT jsonb_build_object(
        'zone', jsonb_build_object('id', z.id, 'tenant_id', z.tenant_id, 'project_id', z.project_id, 'status', z.status),
        'record', jsonb_build_object('id', r.id, 'zone_id', r.zone_id, 'tenant_id', r.tenant_id, 'type', r.type,
        'values', r.values_json, 'status', r.status, 'flatten_mode', r.flatten_mode,
        'flatten_target', r.flatten_target, 'flatten_ipv4_policy', r.flatten_ipv4_policy,
        'flatten_ipv6_policy', r.flatten_ipv6_policy, 'flatten_fallback_policy', r.flatten_fallback_policy,
        'flatten_status', r.flatten_status, 'flattened_a', r.flattened_a_json,
        'flattened_aaaa', r.flattened_aaaa_json, 'last_resolved_at', r.last_resolved_at,
        'resolve_error', r.resolve_error, 'last_message', r.last_message,
        'created_at', r.created_at, 'updated_at', r.updated_at))
    FROM public.fugue_dns_records r
    JOIN public.fugue_dns_zones z ON z.id = r.zone_id AND z.tenant_id = r.tenant_id
    WHERE fugue_hosted_dns_maintenance.writer_v1() = 'worker' AND r.id > p_after
      AND z.status NOT IN ('deleted', 'suspended', 'pending_ownership')
      AND r.status <> 'disabled'
      AND (r.type IN ('ALIAS', 'ANAME') OR (r.type = 'CNAME' AND r.flatten_mode NOT IN ('', 'none')))
    ORDER BY r.id LIMIT 8
$function$;

-- Keep the denied address ranges in sync with platformconfig.PublicDNSFlattenIP.
CREATE OR REPLACE FUNCTION fugue_hosted_dns_maintenance.public_answers_v1(p_answers jsonb, p_family integer)
RETURNS boolean
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path = pg_catalog
AS $function$
DECLARE
    item jsonb;
    answer text;
    parsed inet;
BEGIN
    FOR item IN SELECT value FROM jsonb_array_elements(p_answers) LOOP
        IF jsonb_typeof(item) <> 'string' THEN
            RETURN false;
        END IF;
        answer := item #>> '{}';
        IF answer !~ '^[0-9A-Fa-f:.]{2,45}$' THEN
            RETURN false;
        END IF;
        parsed := answer::inet;
        IF family(parsed) <> p_family OR masklen(parsed) <> (CASE p_family WHEN 4 THEN 32 ELSE 128 END)
           OR EXISTS (
               SELECT 1 FROM unnest(ARRAY[
                   '0.0.0.0/8', '10.0.0.0/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16',
                   '172.16.0.0/12', '192.0.0.0/24', '192.0.2.0/24', '192.168.0.0/16',
                   '198.18.0.0/15', '198.51.100.0/24', '203.0.113.0/24', '224.0.0.0/4', '240.0.0.0/4',
                   '::/128', '::1/128', '::ffff:0.0.0.0/96', '64:ff9b:1::/48', '100::/64',
                   '2001:db8::/32', 'fc00::/7', 'fe80::/10', 'ff00::/8'
               ]::inet[]) AS denied(prefix)
               WHERE parsed <<= prefix
           ) THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN true;
EXCEPTION WHEN invalid_text_representation THEN
    RETURN false;
END
$function$;

CREATE OR REPLACE FUNCTION fugue_hosted_dns_maintenance.commit_v1(
    p_zone text, p_tenant text, p_project text, p_record text, p_created timestamptz,
    p_updated timestamptz, p_a jsonb, p_aaaa jsonb, p_flatten_status text,
    p_status text, p_resolved timestamptz, p_error text, p_message text
)
RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog SET lock_timeout = '250ms'
AS $function$
BEGIN
    IF p_status IS NULL OR p_status NOT IN ('active', 'degraded')
       OR p_flatten_status IS NULL OR p_flatten_status NOT IN ('resolved', 'stale', 'error')
       OR jsonb_typeof(p_a) IS DISTINCT FROM 'array' OR jsonb_typeof(p_aaaa) IS DISTINCT FROM 'array'
       OR pg_column_size(p_a) + pg_column_size(p_aaaa) > 65536
       OR octet_length(p_error) > 4096 OR octet_length(p_message) > 4096 THEN
        RAISE EXCEPTION 'invalid hosted DNS observation';
    END IF;
    IF NOT fugue_hosted_dns_maintenance.public_answers_v1(p_a, 4)
       OR NOT fugue_hosted_dns_maintenance.public_answers_v1(p_aaaa, 6) THEN
        RAISE EXCEPTION 'hosted DNS observation contains an invalid public address';
    END IF;
    IF NOT pg_try_advisory_xact_lock_shared(hashtextextended('hosted-dns-flatten/v1/writer', 0))
       OR fugue_hosted_dns_maintenance.writer_v1() <> 'worker' THEN
        RETURN false;
    END IF;
    PERFORM 1 FROM public.fugue_dns_zones
    WHERE id = p_zone AND tenant_id = p_tenant AND project_id = p_project
      AND status NOT IN ('deleted', 'suspended', 'pending_ownership')
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    UPDATE public.fugue_dns_records
    SET flattened_a_json = p_a, flattened_aaaa_json = p_aaaa, flatten_status = p_flatten_status,
        status = p_status, last_resolved_at = p_resolved, resolve_error = p_error, last_message = p_message,
        updated_at = GREATEST(clock_timestamp(), updated_at + interval '1 microsecond')
    WHERE id = p_record AND zone_id = p_zone AND tenant_id = p_tenant
      AND created_at = p_created AND updated_at = p_updated AND status <> 'disabled'
      AND (type IN ('ALIAS', 'ANAME') OR (type = 'CNAME' AND flatten_mode NOT IN ('', 'none')));
    RETURN FOUND;
END
$function$;

CREATE OR REPLACE FUNCTION fugue_hosted_dns_maintenance.progress_v1(p_updated integer, p_cursor text)
RETURNS TABLE (owner text, completed_at timestamptz, updated integer, cursor text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog
AS $function$
BEGIN
    IF p_updated IS NOT NULL THEN
        IF p_updated < 0 OR p_updated > 8 OR p_cursor IS NULL OR octet_length(p_cursor) > 256
           OR NOT pg_try_advisory_xact_lock_shared(hashtextextended('hosted-dns-flatten/v1/writer', 0))
           OR fugue_hosted_dns_maintenance.writer_v1() <> 'worker' THEN
            RAISE EXCEPTION 'hosted DNS worker completion was not authorized';
        END IF;
        INSERT INTO public.fugue_meta (key, value, updated_at)
        VALUES ('hosted-dns-flatten/v1/progress', jsonb_build_object('updated', p_updated, 'cursor', p_cursor)::text, clock_timestamp())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;
    END IF;
    RETURN QUERY
        SELECT fugue_hosted_dns_maintenance.writer_v1(), m.updated_at, (m.value::jsonb->>'updated')::integer,
               COALESCE(m.value::jsonb->>'cursor', '')
        FROM (SELECT true) seed LEFT JOIN public.fugue_meta m ON m.key = 'hosted-dns-flatten/v1/progress';
END
$function$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA fugue_hosted_dns_maintenance FROM PUBLIC;
COMMIT;
