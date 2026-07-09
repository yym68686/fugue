# Platform Safety Kernel Runbook

## Purpose

The Platform Safety Kernel is the non-configurable minimum protection applied
to Platform Artifact release and verified LKG promotion. Ordinary artifact
content, gate policy, environment overrides, and `force_publish` cannot disable
these checks.

## Hard Failures

- Artifact is not validated.
- Canonical artifact content does not match its SHA-256 content hash.
- Full release has no readable, non-expired verified rollback generation.
- Verification fencing token is stale.
- Initial LKG bootstrap is not an explicitly approved shadow release.
- Required verification evidence is incomplete.

These failures return a conflict and must not be bypassed by retrying with
`force_publish`.

## Diagnosis

```bash
fugue admin artifact show <artifact-id>
fugue admin artifact lkg <artifact-id>
fugue admin artifact consumers <artifact-id>
fugue admin release guard status --json
fugue admin robustness status --json
```

Check the artifact status/hash, release channel, fencing token,
`pinned_rollback_generation`, `verification_state`, LKG expiry, verified release
id, and evidence hash.

## Recovery

1. Correct and recreate an invalid artifact instead of mutating stored content.
2. If no verified LKG exists, release a validated generation to shadow.
3. Collect the required evidence and explicitly seed the initial LKG with
   `--allow-initial-lkg`.
4. If the pinned rollback artifact is missing or expired, stop full releases and
   follow `pinned-rollback-recovery.md`.
5. If the fencing token is stale, inspect the current active release. Do not
   reuse an older release id.

## Invariants

- Safety checks fail closed for full release.
- Shadow recovery remains available when no usable verified LKG exists.
- A bad serving-unverified generation cannot overwrite the previous verified
  LKG.
- Database uniqueness prevents two active releases in one Platform Artifact
  lane.
