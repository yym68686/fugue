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

ALTER TABLE fugue_control.resource_baselines ADD COLUMN IF NOT EXISTS verified_snapshot bytea
    CHECK (octet_length(verified_snapshot) BETWEEN 1 AND 262144);

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

-- The adopted sample retention owner has no reverse authorizer or production
-- dispatch adapter. A durable baseline cannot become an executable claim.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_constraint
        WHERE conrelid='fugue_control.execution_claims'::pg_catalog.regclass AND conname='observation_sample_retention_no_claim') THEN
        ALTER TABLE fugue_control.execution_claims ADD CONSTRAINT observation_sample_retention_no_claim
            CHECK (resource_id <> 'maintenance/observation-sample-retention-writer');
    END IF;
END;
$$;

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

-- The existing claim allocates the one shared Deployment fence. Keep the
-- signed evidence beside it, never in the approval or a Git declaration.
CREATE TABLE IF NOT EXISTS fugue_control.registry_rehome_admissions (
    intent_digest text PRIMARY KEY REFERENCES fugue_control.execution_claims(intent_digest),
    approval_digest text NOT NULL,
    fence bigint NOT NULL CHECK (fence > 0),
    evidence_digest text NOT NULL UNIQUE CHECK (evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
    envelope bytea NOT NULL CHECK (octet_length(envelope) BETWEEN 1 AND 8192),
    FOREIGN KEY (approval_digest, intent_digest) REFERENCES fugue_control.approvals (digest, intent_digest)
);

-- Ordinary runtime roles must not have INSERT on registry_rehome_admissions.
-- The deferred check makes a raw ordinary claim fail without the atomic
-- admission append. It does not substitute for signature/provider checks.
CREATE OR REPLACE FUNCTION fugue_control.require_registry_rehome_admission() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF NOT EXISTS (SELECT FROM fugue_control.intents
        WHERE digest=NEW.intent_digest AND convert_from(envelope, 'UTF8')::jsonb->>'operation'='component_registry_rehome') THEN
        RETURN NULL;
    END IF;
    IF TG_TABLE_NAME='execution_results' THEN
        -- An approval binds the registry projection, not a new verified
        -- serving/rollback record. No ordinary result may advance this head.
        RAISE EXCEPTION 'registry rehome needs a verified recovery baseline for settlement' USING ERRCODE='23514';
    END IF;
    IF NOT EXISTS (SELECT FROM fugue_control.registry_rehome_admissions
            WHERE intent_digest=NEW.intent_digest AND approval_digest=NEW.approval_digest AND fence=NEW.fence) THEN
        RAISE EXCEPTION 'registry rehome claim requires atomic dedicated admission' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$;
REVOKE ALL ON FUNCTION fugue_control.require_registry_rehome_admission() FROM PUBLIC;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_trigger
        WHERE tgrelid='fugue_control.execution_claims'::pg_catalog.regclass AND tgname='registry_rehome_claim_admission') THEN
        CREATE CONSTRAINT TRIGGER registry_rehome_claim_admission
            AFTER INSERT ON fugue_control.execution_claims DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE FUNCTION fugue_control.require_registry_rehome_admission();
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_trigger
        WHERE tgrelid='fugue_control.execution_results'::pg_catalog.regclass AND tgname='registry_rehome_result_guard') THEN
        CREATE TRIGGER registry_rehome_result_guard
            AFTER INSERT ON fugue_control.execution_results
            FOR EACH ROW EXECUTE FUNCTION fugue_control.require_registry_rehome_admission();
    END IF;
END;
$$;

-- Triggers keep execution audit inseparable from the append. In particular,
-- the launch principal does not need a general INSERT grant on audit.
-- This SECURITY DEFINER function is installed by the owner-only migrator.
CREATE OR REPLACE FUNCTION fugue_control.audit_execution_transition() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    INSERT INTO fugue_control.audit (kind, subject_digest, intent_digest, actor)
    VALUES (TG_ARGV[0], NEW.intent_digest, NEW.intent_digest, NEW.actor);
    RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS fugue_control.participation_topologies (
    cluster_id text NOT NULL,
    resource_id text NOT NULL CHECK (resource_id = 'topology/node-participation'),
    generation bigint NOT NULL CHECK (generation > 0),
    policy_digest text NOT NULL,
    intent_digest text NOT NULL UNIQUE,
    approval_digest text NOT NULL,
    actor text NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (cluster_id, generation),
    FOREIGN KEY (intent_digest, cluster_id, resource_id) REFERENCES fugue_control.intents (digest, cluster_id, resource_id),
    FOREIGN KEY (approval_digest, intent_digest) REFERENCES fugue_control.approvals (digest, intent_digest)
);

CREATE TABLE IF NOT EXISTS fugue_control.dns_cutovers (
    cluster_id text NOT NULL,
    resource_id text NOT NULL CHECK (resource_id = 'producer/global-policy-cutover'),
    phase text NOT NULL CHECK (phase IN ('freeze', 'activate')),
    intent_digest text NOT NULL UNIQUE,
    approval_digest text NOT NULL,
    actor text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cluster_id, phase),
    FOREIGN KEY (intent_digest, cluster_id, resource_id) REFERENCES fugue_control.intents (digest, cluster_id, resource_id),
    FOREIGN KEY (approval_digest, intent_digest) REFERENCES fugue_control.approvals (digest, intent_digest)
);

REVOKE ALL ON FUNCTION fugue_control.audit_execution_transition() FROM PUBLIC;

-- CREATE TABLE IF NOT EXISTS does not upgrade the original audit CHECK.
-- Exclude concurrent appends during the one-time backfill so no transition
-- can land between the historical read and trigger installation.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid='fugue_control.audit'::pg_catalog.regclass AND conname IN ('audit_event_kind_v2', 'audit_event_kind_v3')) THEN
        LOCK TABLE fugue_control.execution_claims, fugue_control.execution_results, fugue_control.execution_launches
            IN SHARE ROW EXCLUSIVE MODE;
        ALTER TABLE fugue_control.audit DROP CONSTRAINT audit_kind_check;
        ALTER TABLE fugue_control.audit ADD CONSTRAINT audit_event_kind_v2 CHECK (kind IN (
            'intent.recorded', 'approval.recorded', 'execution.claimed', 'execution.launched', 'execution.completed'
        ));
        INSERT INTO fugue_control.audit (kind, subject_digest, intent_digest, actor, recorded_at)
        SELECT 'execution.claimed', intent_digest, intent_digest, actor, recorded_at FROM fugue_control.execution_claims
        UNION ALL
        SELECT 'execution.completed', intent_digest, intent_digest, actor, recorded_at FROM fugue_control.execution_results
        UNION ALL
        SELECT 'execution.launched', intent_digest, intent_digest, actor, recorded_at FROM fugue_control.execution_launches;

        CREATE TRIGGER execution_claim_audit AFTER INSERT ON fugue_control.execution_claims
            FOR EACH ROW EXECUTE FUNCTION fugue_control.audit_execution_transition('execution.claimed');
        CREATE TRIGGER execution_result_audit AFTER INSERT ON fugue_control.execution_results
            FOR EACH ROW EXECUTE FUNCTION fugue_control.audit_execution_transition('execution.completed');
        CREATE TRIGGER execution_launch_audit AFTER INSERT ON fugue_control.execution_launches
            FOR EACH ROW EXECUTE FUNCTION fugue_control.audit_execution_transition('execution.launched');
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid='fugue_control.audit'::pg_catalog.regclass AND conname='audit_event_kind_v3') THEN
        ALTER TABLE fugue_control.audit DROP CONSTRAINT audit_event_kind_v2;
        ALTER TABLE fugue_control.audit ADD CONSTRAINT audit_event_kind_v3 CHECK (kind IN (
            'intent.recorded', 'approval.recorded', 'execution.claimed', 'execution.launched', 'execution.completed',
            'topology.participation_activated'
        ));
        CREATE TRIGGER participation_topology_audit AFTER INSERT ON fugue_control.participation_topologies
            FOR EACH ROW EXECUTE FUNCTION fugue_control.audit_execution_transition('topology.participation_activated');
    END IF;
END;
$$;
