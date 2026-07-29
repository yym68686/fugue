# Application automation restart

This runbook governs the `app.restart` automatic-action contract. The contract
is app-scoped, starts in shadow mode, and may target at most one application.

## Required evidence

Before proposing a restart, retain all of the following evidence for the same
application and policy generation:

- bounded `app_request_outcomes` observations matching the configured request
  metric window;
- the current desired `app_revision`;
- current `app_readiness`.

Unknown, stale, cross-app, or incomplete evidence fails closed. A policy
definition never authorizes a production restart by itself.

## Safety boundary

Production execution requires the registered action contract, effective gate
policy, global and action kill switches, a current fencing token, a durable
write-ahead record, an idempotency key, an audit sink, and the captured desired
application revision. Repeated evaluations for the same evidence and policy
generation must reuse the idempotency key.

The initial user-facing policy modes are `disabled` and `shadow`. Shadow mode
may emit a `would_action` decision but must not mutate an application.
The effective action mode is the more restrictive of the immutable user-policy
snapshot and the current platform gate. A gate change may tighten an existing
intent, but it cannot elevate that intent beyond the user's authorized mode.

## Shadow control loop

The API starts an observe-only control loop when
`FUGUE_AUTOMATION_SHADOW_LOOP_ENABLED=true`, observability is enabled, and a
ClickHouse analytics exporter is configured. One API replica holds the
PostgreSQL advisory lock named `automation-shadow-control-loop`; the other API
replicas remain followers and retry leadership without evaluating policies.
`FUGUE_AUTOMATION_SHADOW_LOOP_INTERVAL` defaults to `30s`, while a policy with
a smaller valid window lowers the leader's polling interval for that process.

The leader evaluates only completed windows, with a five-second ingestion
settling delay. It reads the typed `request_facts` table with a bounded query,
deduplicates facts by request or trace identity, and prefers application-side
facts over edge-side facts so one request is not counted at both layers. A
disabled, deleting, deleted, or zero-replica application is skipped without a
telemetry query. A query or evaluation failure for one policy does not block
other policies. Large policy inventories are processed in deterministic,
rotating bounded batches so a later-sorted policy cannot be permanently
starved.

A match appends a trusted `control_loop` intent and a system audit event. The
intent remains `observed`, has `production_mutation_allowed=false`, and cannot
create an operation or restart. The loop also appends exactly one durable
action-dispatch/WAL record for that intent. In the current atom the record is
observe-only: it is `held` while the gate is `shadow` or disabled, and no
executor claims it. Replaying a completed window after API failover reuses the
deterministic intent and dispatch records and does not append a second
creation audit.

Inspect the read-only dispatch inventory with
`GET /v1/automation-dispatches` or
`GET /v1/automation-dispatches/{dispatch_id}`. The list supports
`tenant_id` (platform admin only), `project_id`, `policy_id`, `app_id`,
`status`, and `limit` filters. Dispatch records include the immutable intent
identity, source revision, rollback target, safety decision, WAL hash, status,
and monotonic per-application fencing token. The token and record version are
the stale-writer boundary for the future executor; they do not authorize a
mutation by themselves.

Inspect these API metrics before promoting any later execution mode:

- `fugue_automation_shadow_loop_active`
- `fugue_automation_shadow_loop_leader`
- `fugue_automation_shadow_loop_runs_total`
- `fugue_automation_shadow_loop_errors_total`
- `fugue_automation_shadow_loop_evaluations_total`
- `fugue_automation_shadow_loop_matches_total`
- `fugue_automation_shadow_loop_intents_created_total`
- `fugue_automation_shadow_loop_intents_reused_total`
- `fugue_automation_shadow_loop_dispatches_created_total`
- `fugue_automation_shadow_loop_dispatches_reused_total`
- `fugue_automation_shadow_loop_policy_limit_deferred_total`
- `fugue_automation_shadow_loop_last_success_timestamp_seconds`

`enabled=1` with `active=0` means the analytics dependency is unavailable or
not configured; no policy evaluation is attempted in that state.

## Recovery and rollback

After a restart, verify request outcomes and readiness against the same
application revision. If readiness regresses, the captured desired revision
changes unexpectedly, or a restart loop is detected:

1. activate `FUGUE_AUTOMATION_APP_RESTART_KILL_SWITCH`;
2. hold further restart intents for the application;
3. leave dispatch records held; do not manually edit status, fencing tokens,
   or WAL hashes;
4. reconcile the captured desired application revision;
5. preserve the action WAL, evidence, fencing token, and audit trail for
   diagnosis;
6. require an operator to clear the hold after readiness and representative
   request probes pass.
