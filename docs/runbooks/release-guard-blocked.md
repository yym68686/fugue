# Release Guard Blocked Runbook

## Trigger

Use this when `fugue admin release guard status` reports
`block_rollout=true` or the deploy workflow logs a release guard warning or
failure.

## Read-Only Diagnosis

```bash
fugue admin release guard status --json
fugue admin robustness status --json
fugue admin artifact ls --json
```

Classify the blocker as robustness baseline, platform artifact validation,
consumer generation drift, LKG expiry, request/traffic safety regression, or
gate policy violation.

Check `blocked_reasons`, `gate_policy_violations`, artifact validation failures,
and platform consumer drift. If the block comes from a tenant workload, confirm
whether that workload is an explicit platform release signal.

## Safety Rules

- Do not continue a normal rollout while a `block_publish` incident is active.
- Force publish requires an explicit reason and audit trail.
- Prefer rollback, gate demotion, or scoped exclusion over manual live edits.
- Unsafe new gates must be demoted to `shadow` or `disabled` before release
  expansion continues.

## Recovery

1. Fix invalid artifact source or rollback to the last validated generation.
2. If consumer drift blocks rollout, follow
   `docs/runbooks/consumer-generation-drift.md`.
3. If LKG expired blocks rollout, follow `docs/runbooks/lkg-expired.md`.
4. Demote unsafe new gates to `shadow`:
   `fugue admin gate promote <gate-id> --mode shadow --reason <reason>`.
5. Let the deploy workflow perform Helm rollback when canary/readiness/smoke
   fails.

## Verification

- `release guard status` reports `pass=true`.
- `blocked_reasons` is empty.
- `gate_policy_violations` is empty.
- Robustness status has no platform `block_publish` incident.
- Public synthetic probes do not return Fugue routing 503 classes.
- Deploy logs include the pre-deploy and post-deploy release guard summary.
