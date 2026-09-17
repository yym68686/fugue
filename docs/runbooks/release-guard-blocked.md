# Release Guard Blocked Runbook

## Trigger

Use this when `fugue admin release guard status` reports
`block_rollout=true` or the deploy workflow logs a release guard warning or
failure.

## Read-Only Diagnosis

```bash
fugue admin release guard status --json
fugue admin robustness status --json
fugue admin artifact ls --json
```

Classify the blocker as robustness baseline, platform artifact validation,
consumer generation drift, LKG expiry, request/traffic safety regression, or
gate policy violation.

Check `blocked_reasons`, `gate_policy_violations`, artifact validation failures,
and platform consumer drift. If the block comes from a tenant workload, confirm
whether that workload is an explicit platform release signal.

## Safety Rules

- Do not continue a normal rollout while a `block_publish` incident is active.
- Force publish requires an explicit reason and audit trail.
- Prefer rollback, gate demotion, or scoped exclusion over manual live edits.
- Unsafe new gates must be demoted to `shadow` or `disabled` before release
  expansion continues.

## Recovery

1. Fix invalid artifact source or rollback to the last validated generation.
2. If consumer drift blocks rollout, follow
   `docs/runbooks/consumer-generation-drift.md`.
3. If LKG expired blocks rollout, follow `docs/runbooks/lkg-expired.md`.
4. Demote unsafe new gates to `shadow`:
   `fugue admin gate promote <gate-id> --mode shadow --reason <reason>`.
5. Use the declarative GitHub Actions and Guardian recovery path. Bind any
   recovery intent to the exact failed target and verified successful LKG.

## Configuration changes during an Edge code rollout

A new configuration generation may retire an inactive Worker candidate while
the code rollout is still waiting for it. The executor can restage at most four
times when Edge Control reports a newer healthy publication, an empty candidate
slot, and the same recovery epoch. The Front activation and active Worker code
identity must remain unchanged, with fresh inventory evidence. Each new stage
gets a new signed candidate and must pass the normal independent canary.

An already committed CurrentAuthority is a separate outcome. The executor
requires the exact staged record, Worker source/image, slot and original bundle
generation, plus a complete ready Worker cohort with fresh active inventory.
A later verified configuration publication is allowed without requiring the
old candidate headers to remain present. Front and public-route verification
still run before the release is complete.

After Front CAS, a missing candidate header requires a fresh, independent
code/image/Front-generation witness for each successful public-route sample.
The expected response body must still match. Contradictory or partial headers
cannot use this fallback, and the observed headers are never synthesized.

An absent candidate, failed status request, changed recovery epoch, changed
traffic activation or unknown error alone does not authorize restaging. These
conditions remain blocked; preserve their failed record for explicit recovery.

## Verification

- `release guard status` reports `pass=true`.
- `blocked_reasons` is empty.
- `gate_policy_violations` is empty.
- Robustness status has no platform `block_publish` incident.
- Public synthetic probes do not return Fugue routing 503 classes.
- Deploy logs include the pre-deploy and post-deploy release guard summary.
