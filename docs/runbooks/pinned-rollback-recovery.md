# Pinned Rollback Recovery Runbook

## Trigger

Use this when full release is blocked because the verified LKG is expired,
corrupt, missing its artifact, or no longer matches the stored content hash.

## Safety Rule

Do not force a full release without a usable rollback target. The Platform
Safety Kernel intentionally rejects this state.

## Diagnosis

```bash
fugue admin artifact ls --kind <kind> --scope <scope> --json
fugue admin artifact lkg <artifact-id>
fugue admin artifact show <lkg-artifact-id>
fugue admin release guard status --json
```

Confirm:

- LKG has not expired.
- `verified_by_release_id` is present.
- `verification_evidence_hash` is a SHA-256 value.
- The referenced artifact still exists and is validated.
- Artifact kind, scope, schema version, generation, generation sequence, content
  hash, and artifact provenance exactly match LKG.
- Artifact and LKG snapshot signatures verify against the configured current or
  previous signing key, and neither key id is revoked.

## Recovery

1. Stop full releases for the affected artifact lane.
2. Recreate or select a validated artifact with known-good content.
3. Release it to shadow with a unique idempotency key.
4. Run local and public probes and complete the required watch window.
5. Explicitly verify the shadow release with `--allow-initial-lkg`.
6. Confirm the new verified LKG is readable, non-expired, and passes artifact
   and snapshot signature verification before resuming full releases.

## Prohibited Actions

- Do not edit LKG rows manually during normal recovery.
- Do not reuse a stale fencing token.
- Do not claim missing evidence as passed.
- Do not delete the last readable artifact while it is pinned as rollback.
- Do not edit a signature, content hash, generation sequence, provenance, or
  expiry field to make an invalid LKG appear healthy.
