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
`
