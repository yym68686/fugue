-- Installed by the declarative CI database-administration lane, not by the
-- ordinary application/schema-migrator role. No data, role or global GUC changes.
BEGIN;
SET LOCAL statement_timeout='5s';
SET LOCAL lock_timeout='250ms';
SELECT pg_advisory_xact_lock(315609238744294);
DO $install$
BEGIN
 IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname=current_user) THEN
  RAISE EXCEPTION 'zero-temp installer requires the existing database administrator';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner
           WHERE n.nspname='fugue_input_safety' AND NOT r.rolsuper) THEN
  RAISE EXCEPTION 'zero-temp schema is not administrator owned';
 END IF;
END
$install$;
CREATE SCHEMA IF NOT EXISTS fugue_input_safety AUTHORIZATION CURRENT_USER;
REVOKE ALL ON SCHEMA fugue_input_safety FROM PUBLIC;
GRANT USAGE ON SCHEMA fugue_input_safety TO PUBLIC;
-- Refuse a pre-existing CREATE grant instead of installing privileged code in
-- a schema that application roles can replace or shadow.
DO $schema_acl$
BEGIN
 IF EXISTS(SELECT 1 FROM pg_namespace n,
           LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
           LEFT JOIN pg_roles r ON r.oid=a.grantee
           WHERE n.nspname='fugue_input_safety' AND a.privilege_type='CREATE'
             AND (a.grantee=0 OR NOT COALESCE(r.rolsuper,false))) THEN
  RAISE EXCEPTION 'zero-temp schema has an unsafe CREATE grant';
 END IF;
END
$schema_acl$;
CREATE OR REPLACE FUNCTION fugue_input_safety.zero_temp_v1() RETURNS void
LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY DEFINER
SET search_path=pg_catalog
AS $zero_temp$
BEGIN
 PERFORM pg_catalog.set_config('temp_file_limit','0',true);
END
$zero_temp$;
ALTER FUNCTION fugue_input_safety.zero_temp_v1() OWNER TO CURRENT_USER;
REVOKE ALL ON FUNCTION fugue_input_safety.zero_temp_v1() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION fugue_input_safety.zero_temp_v1() TO PUBLIC;
COMMIT;
