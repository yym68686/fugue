# Platform Artifact Rollback Runbook

## Trigger

Use this when a released artifact causes route, DNS, TLS, consumer, or request
quality regression.

## Read-Only Diagnosis

```bash
fugue admin artifact ls --json
fugue admin release guard status --json
fugue admin robustness status --json
```

Find the current artifact id, current generation, previous validated
generation, release channel, and affected scope.

## Recovery

```bash
fugue admin artifact rollback <current-artifact-id> \
  --to-generation <previous-generation> \
  --channel <shadow|gray|full> \
  --reason "<why rollback is safer>"
```

Use full-channel rollback only when the previous generation is validated and
compatible. A rollback release also enters `serving_unverified`; it does not
replace verified LKG until rollback verification succeeds. Gray rollback must
not replace full-channel LKG.

```bash
fugue admin artifact verify-lkg <rollback-release-id> \
  --fencing-token <token> \
  --consumer-convergence \
  --local-probe \
  --public-synthetic \
  --watch-window \
  --baseline-monotonic \
  --database-rollback-compatible \
  --evidence-ref <ref> \
  --reason "<rollback verified>"
```

## Verification

- A rollback release record exists.
- Release message type is `rollback`.
- Consumers converge to the rollback generation.
- A later `verified_lkg` message exists.
- LKG generation changes only after rollback verification.
