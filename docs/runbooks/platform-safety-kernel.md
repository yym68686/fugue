# Platform Safety Kernel Runbook

## Purpose

The Platform Safety Kernel is the non-configurable minimum protection applied
to Platform Artifact release and verified LKG promotion. Ordinary artifact
content, gate policy, environment overrides, and `force_publish` cannot disable
these checks.

## Hard Failures

- Artifact is not validated.
- Artifact does not use the supported Platform Artifact schema version or has
  no positive generation sequence.
- Canonical artifact content does not match its SHA-256 content hash.
- Artifact provenance is missing, signed by an unknown or revoked key, or does
  not match the immutable artifact identity, scope, generation, hash, creator,
  compatibility floor, and metadata.
- An ordinary release tries to publish a generation sequence that is not newer
  than the active generation in the same artifact kind, normalized scope, and
  release-channel lane.
- Gray release has no single bounded canary selector, or uses a global,
  wildcard, or multi-target selector.
- Full release has no readable verified rollback generation, or the LKG is
  expired, corrupt, signature-invalid, signed by an unknown or revoked key, or
  no longer matches its signed artifact and evidence hash.
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

Check the artifact schema version, generation sequence, provenance key id and
signature, status/hash, release channel, fencing token,
`pinned_rollback_generation`, `verification_state`, LKG expiry, verified release
id, artifact and snapshot provenance, evidence hash, and `canary_rule_ref`.

For gate-policy issues, compare the effective policy with its compiled default:

```bash
fugue admin gate ls --json
fugue admin gate show <gate-id> --json
```

Ordinary policy artifacts may increase soak time, samples, failure-domain
requirements, rollback signals, and enforcement. They cannot lower compiled
minimums, change an existing scope/kill-switch/runbook binding, enlarge the
compiled blast-radius cap, or disable an enforced default. A gate kill switch
may only move a gate toward `shadow` or `disabled`; it cannot promote a gate.

## Recovery

1. Correct and recreate an invalid artifact instead of mutating stored content.
2. If the active signing key is unavailable, restore the configured signing
   key and key id before creating another artifact. Never publish unsigned
   fallback content.
3. If a signing key was rotated, keep the previous key configured during the
   overlap window. Revoked key ids are rejected even when their key material is
   still present.
4. If no verified LKG exists, release a validated generation to shadow.
5. Collect the required evidence and explicitly seed the initial LKG with
   `--allow-initial-lkg`.
6. If the pinned rollback artifact is missing, expired, corrupt, or
   signature-invalid, stop full releases and
   follow `pinned-rollback-recovery.md`.
7. If the fencing token is stale, inspect the current active release. Do not
   reuse an older release id.
8. If an older known-good generation must be restored, use the explicit
   rollback operation. Retrying it as an ordinary release is intentionally
   rejected by generation monotonicity.
9. If a canary selector is rejected, use one explicit selector such as
   `node:<id>`, `edge=<id>`, or `failure_domain:<id>`. Do not use `global`,
   `*`, or a comma-separated target list.
10. If a gate must be disabled during incident recovery, use its compiled kill
   switch. Do not publish a weaker gate-policy artifact.

## Invariants

- Safety checks fail closed for full release.
- Artifact creation fails closed when no trusted signing key is configured.
- Artifact schema, canonical content hash, provenance signature, and positive
  generation sequence are checked again at publication time.
- Ordinary releases are monotonic within their release lane. Only the explicit
  rollback operation has the narrow exemption required to publish an older
  signed generation.
- Shadow and gray ledger entries never claim a production
  `serving_unverified_generation`.
- Shadow recovery remains available when no usable verified LKG exists; making
  that initial generation a production LKG still requires a separate explicit
  verification operation.
- Gray release is confined to one bounded canary selector.
- Compiled blast-radius, evidence, scope, and enforced-mode floors cannot be
  loosened by ordinary gate-policy artifacts.
- Kill-switch state takes precedence over concurrent ordinary policy
  evaluation and can only reduce production impact.
- A bad serving-unverified generation cannot overwrite the previous verified
  LKG.
- Expired, corrupt, signature-invalid, unknown-key, or revoked-key LKG data is
  treated as unavailable, never as a healthy rollback target.
- Database uniqueness prevents two active releases in one Platform Artifact
  lane.
