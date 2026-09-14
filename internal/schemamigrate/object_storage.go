package schemamigrate

// No cascade from app/project deletion: object ownership and revocation records
// outlive compute. Tenant deletion is blocked until resources are dealt with.
const ObjectStorageSQL = `
CREATE TABLE IF NOT EXISTS fugue_object_storage_config (
 id TEXT PRIMARY KEY CHECK (id = 'platform'), body JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS fugue_object_stores (
 id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id),
 project_id TEXT NOT NULL,
 name TEXT NOT NULL,
 body JSONB NOT NULL,
 UNIQUE (tenant_id, project_id, name)
);
CREATE TABLE IF NOT EXISTS fugue_object_storage_credentials (
 id TEXT PRIMARY KEY,
 store_id TEXT NOT NULL REFERENCES fugue_object_stores(id),
 name TEXT NOT NULL,
 body JSONB NOT NULL,
 UNIQUE (store_id, name)
);
CREATE INDEX IF NOT EXISTS idx_fugue_object_stores_tenant ON fugue_object_stores(tenant_id, project_id);
`
