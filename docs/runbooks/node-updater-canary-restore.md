# Node-updater canary restore

Use this runbook when a node-updater generation produces deep-health, quarantine, or desired-state drift signals.

## Safe defaults

- New node-updater gates must remain `shadow` or `canary`.
- Do not enable every `fugue-node-updater.timer` at once after a failure.
- Canary must cover one full timer cycle before expansion.

## Commands

- Inspect rollout: `fugue admin node-updater rollout status`.
- Pause rollout: `fugue admin node-updater rollout pause --reason <reason>`.
- Resume canary: `fugue admin node-updater rollout resume --canary-scope node:<node> --reason <reason>`.
- Inspect health: `fugue admin node-updater health ls`.
- Explain one node: `fugue admin quarantine explain <node-or-edge>`.

## Recovery

- Keep desired generation frozen while the failing generation is investigated.
- Re-enable one timer or one failure domain only after the gate remains warning/degraded without quarantine.
- Promote beyond canary only after release guard and public synthetic probes remain clean for the watch window.
