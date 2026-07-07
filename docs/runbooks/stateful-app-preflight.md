# Stateful App Preflight Runbook

## Trigger

Use this before moving, restarting, or failing over any stateful app or backing
service.

## Read-Only Diagnosis

```bash
fugue admin robustness status --json
fugue admin robustness check service <hostname> --json
```

Collect lease owner, fencing evidence, backup freshness, restore plan, and
database DNS/TCP evidence.

## Safety Rules

- Stateful failover is preflight-only unless lease, fence, backup, and restore
  evidence are all present.
- Database TCP/DNS failure must be attributed separately from app HTTP failure.
- Do not run automatic destructive repair from this runbook.

## Recovery

1. Verify a valid lease and fence state.
2. Verify backup freshness and restore target.
3. Verify database DNS/TCP reachability from the target runtime.
4. Require operator approval before failover.

## Verification

- Preflight evidence is attached to the incident.
- No route cutover occurs before the target is ready.
- Rollback path and data restore plan are documented.
