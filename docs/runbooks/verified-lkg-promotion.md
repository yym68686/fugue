# Verified LKG Promotion Runbook

## Trigger

Use this after a Platform Artifact release is serving as
`serving_unverified`, or when explicitly bootstrapping the first LKG from a
shadow release.

## Required Evidence

- Required consumers converged to the candidate generation.
- Local apply and serving probes passed.
- Public synthetic probes passed.
- The subsystem-specific watch window completed.
- Baseline comparison found no new or worsened blocker.
- Database state remains rollback compatible.
- The release still owns the current fencing token.

Record durable evidence references. Do not mark a boolean true only because the
signal is unavailable.

## Command

```bash
fugue admin artifact verify-lkg <release-id> \
  --fencing-token <token> \
  --consumer-convergence \
  --local-probe \
  --public-synthetic \
  --watch-window \
  --baseline-monotonic \
  --database-rollback-compatible \
  --expected-consumer-set <set-id> \
  --evidence-ref <ref> \
  --reason "<why this generation is verified>"
```

Add `--allow-initial-lkg` only for the first shadow generation in a scope.

## Verification

```bash
fugue admin artifact lkg <artifact-id>
fugue admin artifact show <artifact-id>
```

The release must show `verification_state=verified`; LKG must reference the same
release and contain a SHA-256 evidence hash. Its schema, generation sequence,
content hash, artifact provenance, and snapshot provenance must match the
referenced artifact, and both signatures must verify against a trusted,
non-revoked key. Repeating the same request is idempotent and must return the
same signed LKG snapshot.

## Failure Handling

- Stale fencing token: stop and inspect the current active release.
- Missing evidence: keep the candidate serving-unverified or abort it.
- Public or local probe failure: rollback using the pinned generation.
- Missing consumer: do not promote. Treat missing evidence as unknown, not pass.
- Expired or signature-invalid current LKG: stop full promotion and follow
  `pinned-rollback-recovery.md`; do not treat the invalid snapshot as an
  existing healthy LKG.
