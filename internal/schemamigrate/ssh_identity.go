package schemamigrate

import (
	"context"
	"fmt"
)

const SSHIdentitySQL = `
CREATE TABLE IF NOT EXISTS fugue_app_ssh_identity_anchors (
 app_id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 identity_id TEXT UNIQUE NOT NULL,
 public_body JSONB NOT NULL,
 CHECK (public_body->>'app_id' IS NOT NULL AND public_body->>'app_id' = app_id),
 CHECK (public_body->>'tenant_id' IS NOT NULL AND public_body->>'tenant_id' = tenant_id),
 CHECK (public_body->>'id' IS NOT NULL AND public_body->>'id' = identity_id)
);
CREATE TABLE IF NOT EXISTS fugue_app_ssh_identities (
 app_id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 identity_id TEXT UNIQUE NOT NULL,
 body JSONB NOT NULL,
 CHECK (body->'identity'->>'app_id' IS NOT NULL AND body->'identity'->>'app_id' = app_id),
 CHECK (body->'identity'->>'tenant_id' IS NOT NULL AND body->'identity'->>'tenant_id' = tenant_id),
 CHECK (body->'identity'->>'id' IS NOT NULL AND body->'identity'->>'id' = identity_id),
 CHECK (COALESCE(body->>'ciphertext', '') <> ''),
 CHECK (COALESCE(body->>'encryption_key_id', '') <> '')
);
CREATE INDEX IF NOT EXISTS idx_fugue_app_ssh_identities_tenant ON fugue_app_ssh_identities(tenant_id, app_id);
CREATE TABLE IF NOT EXISTS fugue_app_ssh_observations (
 id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 app_id TEXT NOT NULL,
 identity_id TEXT NOT NULL,
 observed_at TIMESTAMPTZ NOT NULL,
 body JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fugue_app_ssh_observations_latest ON fugue_app_ssh_observations(tenant_id, app_id, observed_at DESC, id DESC);
CREATE TABLE IF NOT EXISTS fugue_edge_ssh_acknowledgements (
 edge_id TEXT NOT NULL,
 instance_uid TEXT NOT NULL,
 loaded_at TIMESTAMPTZ NOT NULL,
 expires_at TIMESTAMPTZ NOT NULL,
 body JSONB NOT NULL,
 PRIMARY KEY (edge_id,instance_uid)
);
CREATE TABLE IF NOT EXISTS fugue_managed_ssh_intents (
 app_id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 generation BIGINT NOT NULL CHECK (generation > 0),
 body JSONB NOT NULL,
 CHECK (body->>'app_id' IS NOT NULL AND body->>'app_id' = app_id),
 CHECK (body->>'tenant_id' IS NOT NULL AND body->>'tenant_id' = tenant_id),
 CHECK (body->>'generation' IS NOT NULL AND (body->>'generation')::BIGINT = generation)
);
CREATE INDEX IF NOT EXISTS idx_fugue_managed_ssh_intents_tenant ON fugue_managed_ssh_intents(tenant_id, app_id);
CREATE TABLE IF NOT EXISTS fugue_managed_ssh_control (
 app_id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 revision BIGINT NOT NULL CHECK (revision > 0),
 body JSONB NOT NULL,
 CHECK (body->>'app_id' IS NOT NULL AND body->>'app_id' = app_id),
 CHECK (body->>'tenant_id' IS NOT NULL AND body->>'tenant_id' = tenant_id),
 CHECK (body->>'revision' IS NOT NULL AND (body->>'revision')::BIGINT = revision)
);
CREATE TABLE IF NOT EXISTS fugue_managed_ssh_artifacts (
 app_id TEXT NOT NULL,
 generation BIGINT NOT NULL CHECK (generation > 0),
 body JSONB NOT NULL,
 PRIMARY KEY (app_id, generation),
 CHECK (body->'configuration'->'identity'->>'app_id' IS NOT NULL AND body->'configuration'->'identity'->>'app_id' = app_id),
 CHECK (body->'configuration'->>'generation' IS NOT NULL AND (body->'configuration'->>'generation')::BIGINT = generation)
);
CREATE TABLE IF NOT EXISTS fugue_managed_ssh_bindings (
 app_id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 public_port INTEGER UNIQUE NOT NULL CHECK (public_port BETWEEN 1 AND 65535),
 body JSONB NOT NULL,
 CHECK (body->>'app_id' IS NOT NULL AND body->>'app_id' = app_id),
 CHECK (body->>'tenant_id' IS NOT NULL AND body->>'tenant_id' = tenant_id),
 CHECK (body->>'public_port' IS NOT NULL AND (body->>'public_port')::INTEGER = public_port)
);
CREATE TABLE IF NOT EXISTS fugue_managed_ssh_routes (
 app_id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 generation BIGINT NOT NULL CHECK (generation > 0),
 artifact_digest TEXT NOT NULL,
 body JSONB NOT NULL,
 CHECK (body->>'app_id' IS NOT NULL AND body->>'app_id'=app_id),
 CHECK (body->>'tenant_id' IS NOT NULL AND body->>'tenant_id'=tenant_id)
);
CREATE TABLE IF NOT EXISTS fugue_managed_ssh_route_history (
 app_id TEXT NOT NULL,
 generation BIGINT NOT NULL CHECK (generation > 0),
 artifact_digest TEXT NOT NULL,
 actor TEXT NOT NULL,
 activated_at TIMESTAMPTZ NOT NULL,
 body JSONB NOT NULL,
 PRIMARY KEY (app_id,generation)
);
CREATE OR REPLACE FUNCTION fugue_managed_ssh_projection_guard() RETURNS TRIGGER AS $$
DECLARE
 projected JSONB;
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS (SELECT 1 FROM fugue_managed_ssh_routes WHERE app_id=OLD.app_id)
    AND EXISTS (SELECT 1 FROM fugue_apps WHERE id=OLD.app_id AND COALESCE(status_json->>'phase','') NOT IN ('deleted','deleting')) THEN
   RETURN NULL;
  END IF;
  RETURN OLD;
 END IF;
 SELECT body INTO projected FROM fugue_managed_ssh_routes WHERE app_id=NEW.app_id;
 IF projected IS NOT NULL THEN
  IF NEW.tenant_id<>projected->>'tenant_id' THEN
   RAISE EXCEPTION 'managed SSH route ownership cannot change' USING ERRCODE='23505';
  END IF;
  NEW.id:=projected->>'id';
  NEW.hostname:=projected->>'hostname';
  NEW.public_port:=(projected->>'public_port')::INTEGER;
  NEW.runtime_id:=COALESCE(projected->>'runtime_id','');
  NEW.runtime_type:=COALESCE(projected->>'runtime_type','');
  NEW.edge_group_id:=COALESCE(projected->>'edge_group_id','');
  NEW.target_namespace:=projected->>'target_namespace';
  NEW.target_service:=projected->>'target_service';
  NEW.target_host:=projected->>'target_host';
  NEW.target_port:=(projected->>'target_port')::INTEGER;
  NEW.ssh_user:=projected->>'user';
  NEW.status:=projected->>'status';
  NEW.status_reason:=COALESCE(projected->>'status_reason','');
  NEW.host_key_fingerprint:=projected->>'host_key_fingerprint';
  NEW.released_at:=NULL;
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER fugue_managed_ssh_projection_guard BEFORE INSERT OR UPDATE OR DELETE ON fugue_app_ssh_endpoints FOR EACH ROW EXECUTE FUNCTION fugue_managed_ssh_projection_guard();
CREATE OR REPLACE FUNCTION fugue_managed_ssh_port_guard() RETURNS TRIGGER AS $$
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended('fugue.ssh-public-ports/v1', 0));
 IF TG_TABLE_NAME = 'fugue_app_ssh_endpoints' THEN
  IF EXISTS (SELECT 1 FROM fugue_managed_ssh_bindings WHERE public_port=NEW.public_port AND app_id<>NEW.app_id) THEN
   RAISE EXCEPTION 'SSH public port is reserved by an independent managed binding' USING ERRCODE='23505';
  END IF;
  IF EXISTS (SELECT 1 FROM fugue_managed_ssh_bindings WHERE app_id=NEW.app_id AND public_port<>NEW.public_port) THEN
   RAISE EXCEPTION 'managed SSH public port cannot be rotated through application SSH configuration' USING ERRCODE='23505';
  END IF;
 ELSE
  IF TG_OP = 'UPDATE' AND NEW.body IS DISTINCT FROM OLD.body THEN
   RAISE EXCEPTION 'managed SSH public binding is immutable' USING ERRCODE='23505';
  END IF;
  IF EXISTS (SELECT 1 FROM fugue_app_ssh_endpoints WHERE public_port=NEW.public_port AND app_id<>NEW.app_id) THEN
   RAISE EXCEPTION 'SSH public port is already assigned to another application' USING ERRCODE='23505';
  END IF;
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER fugue_managed_ssh_native_port_guard BEFORE INSERT OR UPDATE OF public_port,app_id ON fugue_app_ssh_endpoints FOR EACH ROW EXECUTE FUNCTION fugue_managed_ssh_port_guard();
CREATE OR REPLACE TRIGGER fugue_managed_ssh_binding_port_guard BEFORE INSERT OR UPDATE ON fugue_managed_ssh_bindings FOR EACH ROW EXECUTE FUNCTION fugue_managed_ssh_port_guard();
`

func MigrateSSHIdentity(ctx context.Context, databaseURL string) error {
	databaseURL, err := normalizeDatabaseURL(databaseURL)
	if err != nil {
		return err
	}
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	database, err := openDatabase(databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	if _, err = transaction.ExecContext(ctx, `SELECT set_config('lock_timeout', $1, true)`, platformStateLockLimit.String()); err != nil {
		return err
	}
	if _, err = transaction.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fugue.ssh-identity.schema/v1', 0))`); err != nil {
		return err
	}
	if _, err = transaction.ExecContext(ctx, SSHIdentitySQL); err != nil {
		return fmt.Errorf("create SSH identity schema: %w", err)
	}
	return transaction.Commit()
}
