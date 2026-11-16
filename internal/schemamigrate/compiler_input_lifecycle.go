package schemamigrate

// Register explicit operational/audit roles for NEW pairs only. Legacy inputs
// remain unclassified/retained. First registration never renews on replay.
// This ledger is not a discard permission, nor a replacement for native roots.
const CompilerInputLifecycleSQL = `
CREATE TABLE IF NOT EXISTS fugue_platform_input_audit_bindings (
 original_digest TEXT PRIMARY KEY CHECK (original_digest ~ '^sha256:[0-9a-f]{64}$'),
 runtime_digest TEXT NOT NULL CHECK (runtime_digest ~ '^sha256:[0-9a-f]{64}$'),
 registered_at TIMESTAMPTZ NOT NULL,
 CHECK (original_digest<>runtime_digest)
);
CREATE INDEX IF NOT EXISTS fugue_platform_input_audit_bindings_runtime_idx
 ON fugue_platform_input_audit_bindings(runtime_digest);
CREATE TABLE IF NOT EXISTS fugue_platform_quality_evidence (
 content_hash TEXT PRIMARY KEY CHECK (content_hash ~ '^sha256:[0-9a-f]{64}$'),
 evidence BYTEA NOT NULL,
 size_bytes BIGINT NOT NULL CHECK (size_bytes BETWEEN 1 AND 67108864 AND size_bytes=octet_length(evidence)),
 created_at TIMESTAMPTZ NOT NULL,
 expires_at TIMESTAMPTZ NOT NULL CHECK (expires_at>created_at)
);
CREATE INDEX IF NOT EXISTS fugue_platform_quality_evidence_fifo_idx
 ON fugue_platform_quality_evidence(created_at,content_hash);
CREATE INDEX IF NOT EXISTS fugue_platform_quality_evidence_expiry_idx
 ON fugue_platform_quality_evidence(expires_at);
`
