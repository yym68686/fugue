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

## Recovery and rollback

After a restart, verify request outcomes and readiness against the same
application revision. If readiness regresses, the captured desired revision
changes unexpectedly, or a restart loop is detected:

1. activate `FUGUE_AUTOMATION_APP_RESTART_KILL_SWITCH`;
2. hold further restart intents for the application;
3. reconcile the captured desired application revision;
4. preserve the action WAL, evidence, fencing token, and audit trail for
   diagnosis;
5. require an operator to clear the hold after readiness and representative
   request probes pass.
