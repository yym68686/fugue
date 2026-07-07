# LKG Expired Runbook

## Trigger

Use this when a consumer reports `lkg_expired=true`, the LKG hash check fails,
or the consumer is serving an expired cached generation.

## Read-Only Diagnosis

```bash
fugue admin artifact lkg <artifact-id> --json
fugue admin artifact consumers <artifact-id> --json
fugue admin robustness status --json
```

Check LKG generation, content hash, expiry, serving state, and local apply/probe
status.

## Safety Rules

- Expired or hash-invalid LKG is degraded and fail-closed for new rollout.
- A consumer may continue serving a still-valid LKG if the control plane is
  temporarily unreadable.
- Do not silently serve corrupt local cache.

## Recovery

1. Re-pull the active artifact.
2. Apply atomically to a temp path and rename only after write succeeds.
3. Verify local hash sidecar.
4. Run the consumer local probe before reporting convergence.

## Verification

- LKG hash matches `content_hash`.
- `lkg_expired=false`.
- Consumer heartbeat reports `probe_status=passed`.
