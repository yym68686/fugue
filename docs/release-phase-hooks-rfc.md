# RFC: Evidence-Gated Generic Release Phases and Promote Command

Status: draft, evidence-gated
Last updated: 2026-07-03

## Purpose

This RFC defines how Fugue should decide whether to add first-class release
phases, generic hooks, or a `fugue app release promote` command after the
operation-evidence rollout has produced enough durable data.

This RFC intentionally does not change deploy, image tracking, env patch, or
migration behavior. It defines the evidence required before behavior changes are
allowed.

## Non-goals

- No project-, repository-, app-, hostname-, or environment-variable-specific
  logic.
- No special case for any migration variable name.
- No automatic env mutation after deploy.
- No release behavior change until retained evidence proves the pattern is
  frequent enough and the operational benefit is clear.

## Evidence Inputs

The first implementation exposes these low-cardinality research summaries in
operation/release debug bundle `metrics_summary.release_research`:

```json
{
  "env_patch_tracking_sync": {
    "env_patch_attempts": 0,
    "tracking_sync_attempts": 0,
    "env_patch_then_tracking_sync_attempts": 0,
    "window_seconds": 3600
  },
  "migration_log_evidence": [
    {"signal": "schema_or_migration_log", "confidence": "confirmed", "count": 0},
    {"signal": "sqlstate_log", "confidence": "confirmed", "count": 0},
    {"signal": "deadlock_log", "confidence": "confirmed", "count": 0}
  ]
}
```

The same counts are produced from retained `fugue_release_attempts` and
`fugue_operation_evidence`, not from ad-hoc logs or operator memory.

## Decision Gates

A behavior proposal may move beyond draft only when all of these are true:

1. The retained evidence window contains enough release attempts to avoid a
   single-incident conclusion.
2. `env_patch_then_tracking_sync_attempts / env_patch_attempts` is high enough to
   show that users repeatedly combine env patch and image promote manually.
3. Migration-related log evidence is frequent enough to justify first-class
   phase support, and at least some samples have `confidence=confirmed`.
4. Debug bundles for the relevant failures include operation timeline, release
   timeline, pod/container evidence, logs/events, and metrics summary.
5. The proposed behavior can be expressed with generic release phases/hooks, not
   with named env keys or app-specific heuristics.

## Generic Release Phase Schema

A future app-level release spec should describe phases, not business-specific
variables:

```yaml
release:
  phases:
    - name: migrate
      kind: job
      command: ./migrate
      timeout: 5m
      retries: 0
      evidence:
        collectLogs: true
        collectEvents: true
    - name: deploy
      kind: rollout
      healthCheck: default
      timeout: 10m
    - name: verify
      kind: check
      command: ./verify
      timeout: 2m
```

Constraints:

- `name` is user-facing and generic.
- `kind` is a platform-understood execution primitive.
- `command`, `timeout`, `retries`, and evidence options are explicit config.
- Fugue must not infer phase meaning from env key names.
- Each phase must create release steps and operation evidence.

## Alignment With Safe Zero Downtime Rollout

The first concrete stable/candidate state machine uses release steps instead of
generic user hooks. Future hook support must reuse this timeline shape rather
than introducing a parallel phase recorder.

Current safe rollout phases:

- `candidate_create`
- `candidate_ready`
- `gate_check`
- `canary_shift`
- `canary_gate`
- `final_gate`
- `promote`
- `abort`
- `restore_previous`

Gate failures use `app_release_gate_failure` operation evidence and should carry
the evidence id in the release step payload. Future generic phases should follow
the same rule: user-visible phase failure must produce a durable evidence id,
and the CLI/debug bundle should link to that evidence.

## Promote Command Shape

A future command may combine explicit env patch and image promote into one
release attempt:

```sh
fugue app release promote <app> \
  --image <ref-or-digest> \
  --set-env KEY=VALUE \
  --unset-env OLD_KEY \
  --wait
```

Expected semantics:

1. Create one `release_attempt` with a stable ID before mutating state.
2. Record env patch keys only; values remain redacted in evidence.
3. Import/promote the image digest.
4. Queue deploy from the explicitly chosen digest and env patch.
5. Record deploy apply, rollout wait, health check, and finalize steps.
6. On failure, attach primary evidence and confidence-aware diagnosis.

Backward compatibility:

- Existing `app env set`, `app release tracking sync`, and manual deploy commands
  continue to work.
- JSON output gains optional fields only.
- Scripts that read existing operation fields are not broken.

## Required Follow-up Before Implementation

Before implementing behavior changes, produce an evidence report with:

- Retained evidence window start/end.
- Number of env patch attempts.
- Number and ratio of env patch attempts followed by tracking sync within the
  configured window.
- Number of migration/schema/SQLSTATE/deadlock log evidence rows by confidence.
- At least one redacted debug bundle for a representative success and failure.
- A rollback plan that leaves current commands unchanged.
