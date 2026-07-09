# Quarantine blast-radius exceeded

Use this runbook when an automatic quarantine, exclusion, scheduler gate, or route gate exceeds its maximum impact.

## Expected behavior

- The system preserves LKG route/DNS eligibility.
- The gate cannot be promoted while the cap is exceeded.
- Operators receive a block incident and can explain the before/after eligible set.

## Recovery

- Inspect the gate with `fugue admin gate show <gate-id>`.
- Inspect affected nodes with `fugue admin quarantine explain <node-or-edge>`.
- Demote the gate to `shadow` or `disabled`.
- Keep node-updater rollout paused if the signal came from deep health.
- Re-run release guard and public synthetic probes before resuming rollout.
