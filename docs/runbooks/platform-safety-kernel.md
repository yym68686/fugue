# Platform Safety Kernel Runbook

## Purpose

The Platform Safety Kernel is the non-configurable minimum protection applied
to Platform Artifact release and verified LKG promotion. Ordinary artifact
content, gate policy, environment overrides, and `force_publish` cannot disable
these checks.

## Hard Failures

- Artifact is not validated.
- Canonical artifact content does not match its SHA-256 content hash.
- Gray release has no single bounded canary selector, or uses a global,
  wildcard, or multi-target selector.
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
id, evidence hash, and `canary_rule_ref`.

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
2. If no verified LKG exists, release a validated generation to shadow.
3. Collect the required evidence and explicitly seed the initial LKG with
   `--allow-initial-lkg`.
4. If the pinned rollback artifact is missing or expired, stop full releases and
   follow `pinned-rollback-recovery.md`.
5. If the fencing token is stale, inspect the current active release. Do not
   reuse an older release id.
6. If a canary selector is rejected, use one explicit selector such as
   `node:<id>`, `edge=<id>`, or `failure_domain:<id>`. Do not use `global`,
   `*`, or a comma-separated target list.
7. If a gate must be disabled during incident recovery, use its compiled kill
   switch. Do not publish a weaker gate-policy artifact.

## Invariants

- Safety checks fail closed for full release.
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
- Database uniqueness prevents two active releases in one Platform Artifact
  lane.
