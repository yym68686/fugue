# Consumer Generation Drift Runbook

## Trigger

Use this when `fugue_platform_consumer_generation_drift_total` is non-zero or
release guard reports platform consumer drift.

## Read-Only Diagnosis

```bash
fugue admin artifact consumers <artifact-id> --json
fugue admin release guard status --json
```

Check component, node id, desired generation, actual generation, LKG generation,
apply status, probe status, and last error.

## Safety Rules

- Do not expand gray or full rollout while a required consumer is drifted.
- A release message may be lost; consumers must periodic-pull the active
  artifact and report convergence.

## Recovery

1. Ask the consumer to periodic-pull or refresh desired state.
2. If apply failed, keep serving LKG and inspect local probe output.
3. If local cache is corrupt, follow the LKG expired runbook.

## Verification

- `actual_generation == desired_generation`.
- `apply_status=applied`.
- `probe_status=passed`.
- Release guard clears the drift blocker.
