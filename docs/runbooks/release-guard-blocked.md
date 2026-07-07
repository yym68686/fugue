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
consumer generation drift, LKG expiry, or request/traffic safety regression.

## Safety Rules

- Do not continue a normal rollout while a `block_publish` incident is active.
- Force publish requires an explicit reason and audit trail.
- Prefer rollback or scoped exclusion over manual live edits.

## Recovery

1. Fix invalid artifact source or rollback to the last validated generation.
2. If consumer drift blocks rollout, follow
   `docs/runbooks/consumer-generation-drift.md`.
3. If LKG expired blocks rollout, follow `docs/runbooks/lkg-expired.md`.
4. Let the deploy workflow perform Helm rollback when canary/readiness/smoke
   fails.

## Verification

- `release guard status` reports `pass=true`.
- `blocked_reasons` is empty.
- Deploy logs include the pre-deploy and post-deploy release guard summary.
