package schemamigrate

// One checkpoint per participating node, not one row per heartbeat. No
// backfill, inferred owner, or foreign key on a live consumer table.
const WorkerReporterHeadsSQL = `CREATE TABLE IF NOT EXISTS fugue_worker_reporter_heads (
 node_id text PRIMARY KEY CHECK (length(node_id) BETWEEN 1 AND 253),
 revision bigint NOT NULL CHECK (revision > 0),
 binding text NOT NULL CHECK (binding ~ '^sha256:[a-f0-9]{64}$')
)`
