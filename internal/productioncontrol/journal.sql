-- Apply with a schema owner. The journal service needs only USAGE on this
-- schema and SELECT/INSERT on its tables; it must not own any of them.
CREATE SCHEMA IF NOT EXISTS fugue_control;

CREATE TABLE IF NOT EXISTS fugue_control.intents (
    digest text PRIMARY KEY,
    cluster_id text NOT NULL,
    resource_id text NOT NULL,
    request_id text NOT NULL,
    envelope bytea NOT NULL,
    payload bytea NOT NULL CHECK (octet_length(payload) BETWEEN 1 AND 2097152),
    UNIQUE (cluster_id, resource_id, request_id)
);

CREATE TABLE IF NOT EXISTS fugue_control.approvals (
    digest text PRIMARY KEY,
    intent_digest text NOT NULL REFERENCES fugue_control.intents(digest),
    envelope bytea NOT NULL
);

CREATE INDEX IF NOT EXISTS approvals_intent_idx ON fugue_control.approvals (intent_digest);
CREATE UNIQUE INDEX IF NOT EXISTS intents_exact_scope_idx ON fugue_control.intents (digest, cluster_id, resource_id);
CREATE UNIQUE INDEX IF NOT EXISTS approvals_exact_intent_idx ON fugue_control.approvals (digest, intent_digest);

CREATE TABLE IF NOT EXISTS fugue_control.audit (
    kind text NOT NULL CHECK (kind IN ('intent.recorded', 'approval.recorded')),
    subject_digest text NOT NULL,
    intent_digest text NOT NULL REFERENCES fugue_control.intents(digest),
    actor text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (kind, subject_digest)
);

CREATE INDEX IF NOT EXISTS audit_intent_idx ON fugue_control.audit (intent_digest, recorded_at);

CREATE TABLE IF NOT EXISTS fugue_control.resource_baselines (
    cluster_id text NOT NULL,
    resource_id text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    verified_digest text NOT NULL,
    evidence_digest text NOT NULL,
    actor text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cluster_id, resource_id)
);

CREATE TABLE IF NOT EXISTS fugue_control.execution_claims (
    cluster_id text NOT NULL,
    resource_id text NOT NULL,
    fence bigint NOT NULL CHECK (fence > 0),
    intent_digest text NOT NULL UNIQUE,
    approval_digest text NOT NULL,
    actor text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (cluster_id, resource_id) REFERENCES fugue_control.resource_baselines(cluster_id, resource_id),
    FOREIGN KEY (intent_digest, cluster_id, resource_id) REFERENCES fugue_control.intents(digest, cluster_id, resource_id),
    FOREIGN KEY (approval_digest, intent_digest) REFERENCES fugue_control.approvals(digest, intent_digest),
    PRIMARY KEY (cluster_id, resource_id, fence)
);

CREATE TABLE IF NOT EXISTS fugue_control.execution_results (
    intent_digest text PRIMARY KEY REFERENCES fugue_control.execution_claims(intent_digest),
    outcome text NOT NULL CHECK (outcome IN ('applied', 'restored', 'not_applied')),
    generation bigint NOT NULL CHECK (generation > 0),
    verified_digest text NOT NULL,
    evidence_digest text NOT NULL,
    actor text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Publication and executing a published candidate are separate write
-- boundaries. Committing this row before launch burns the only permit, even
-- if the worker dies before its first request. It is never a renewable lease.
CREATE TABLE IF NOT EXISTS fugue_control.execution_launches (
    intent_digest text PRIMARY KEY REFERENCES fugue_control.execution_claims(intent_digest),
    actor text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
