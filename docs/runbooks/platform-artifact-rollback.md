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
compatible. Gray rollback must not replace full-channel LKG.

## Verification

- A rollback release record exists.
- Release message type is `rollback`.
- Consumers converge to the rollback generation.
- LKG generation matches the active full release.
