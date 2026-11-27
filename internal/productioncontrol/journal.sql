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

CREATE TABLE IF NOT EXISTS fugue_control.audit (
    kind text NOT NULL CHECK (kind IN ('intent.recorded', 'approval.recorded')),
    subject_digest text NOT NULL,
    intent_digest text NOT NULL REFERENCES fugue_control.intents(digest),
    actor text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (kind, subject_digest)
);

CREATE INDEX IF NOT EXISTS audit_intent_idx ON fugue_control.audit (intent_digest, recorded_at);
