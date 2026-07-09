# Automatic Repair Safety Runbook

## Trigger

Use this before enabling or executing any non-dry-run repair task.

## Safety Classes

- `observe_only`: no mutation; can run frequently.
- `provenance_scoped`: mutates only Fugue-owned objects with explicit
  provenance.
- `service_restart`: restarts only allowlisted stateless node-side services.
- `operator_approval_required`: must stop at plan/preflight.

## Mandatory Guards

- Dry-run first.
- Idempotent execution.
- Rate limit or duplicate pending task coalescing.
- Lock/lease through node update task status.
- Audit event with actor, target, safety class, dry-run, and evidence.
- Fresh deep-health reprobe after repair.
- Failed repair keeps quarantine active.

## Safety Contract

Automatic repair and automatic isolation actions must have a registered safety
contract before execution. The contract must declare:

- action id
- scope
- trigger invariant
- evidence source
- max blast radius
- TTL
- recovery condition
- rollback action
- dry-run output
- audit log location
- kill switch
- human approval boundary

Non-dry-run automatic repair is refused unless the action has a contract and the
kill switch allows it.

## Disable Switch

Set the control plane environment variable below when automatic repair must be
disabled globally:

```bash
FUGUE_ROBUSTNESS_REPAIR_DISABLED=true
```

## Operator Flow

- Use `fugue admin robustness repair-plan <incident-id>` first.
- Prefer scoped dry-runs and canary repair over global action.
- Verify release guard, robustness, and public synthetic probes after repair.

## Verification

- `node_repair_dry_run_completed`, `node_repair_completed`, or
  `node_repair_failed` audit event exists.
- The repair target matches the incident subject.
- No unrelated object was mutated.
