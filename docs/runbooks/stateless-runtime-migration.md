# Stateless Runtime Migration Runbook

## Trigger

Use this when a stateless app loses ready replicas because its runtime node is
quarantined or unavailable.

## Read-Only Diagnosis

```bash
fugue admin robustness status --json
fugue admin robustness check service <hostname> --json
```

Check desired replicas, ready replicas, runtime id, runtime node, node
quarantine state, and route readiness.

## Safety Rules

- Create replacement capacity before route cutover.
- Only switch route after replacement readiness and service DNS/TCP probes pass.
- Do not mix stateful apps into this runbook.

## Recovery

1. Confirm the app is stateless: no Postgres, no workspace, no persistent
   storage, no backing service binding requiring locality.
2. Let runtime continuity produce a replacement plan.
3. Bring up replacement pod on a non-quarantined runtime node.
4. After probes pass, publish route update through the platform artifact release
   path.

## Verification

- Ready replicas meet desired replicas.
- `runtime_continuity` state is healthy.
- Traffic safety remains pass for the hostname.
