# Queued build activation and configuration isolation

A build's configuration merge must happen twice: when the build queues its deploy,
and when the controller starts that deploy. A scale or environment operation can
finish between those two points. Previously the second merge was absent. A queued
image update with an old replica count could be rejected as an unplanned restart;
without the zero-downtime guard it could restore the old configuration.

All newly accepted deploys now persist their enqueue-time configuration baseline in the existing
private operation source envelope. At execution, the store locks the operation and
app, merges intervening configuration (including nested edits and deletions), and
persists the snapshot actually used by rendering and completion. An explicit image change retains its artifact; a configuration-only edit preserves
a newer serving image and its source metadata. Competing image changes return a
conflict. Only an explicitly changed restart token is carried forward. Repeating the merge
is idempotent; terminal operations return a conflict. Failed activation leaves the
serving app unchanged. Existing operations without a recorded baseline retain
legacy behavior; the system never invents a baseline for historical requests.

For operations with a proven baseline, completion time alone no longer causes
an independent edit to be silently marked superseded. No-op merged operations
still complete without a rollout.

This does not weaken the zero-downtime guard, change CLI account selection, or
change the public deploy request contract. It addresses the queued-operation window; it cannot infer client edits made
before an arbitrary stale full-spec API request reaches the server. In field
conflicts after acceptance, the latest committed configuration is retained.

Validation: controller regression `TestQueuedBuildExecutionPreservesScaleAfterEnqueue`
reproduces `zero-downtime deploy refused: the requested restart has no validated
online rollout plan` with the execution merge removed and passes with the fix.
Store regressions cover scale, environment edits/deletions, idempotency, terminal
conflicts, and actual PostgreSQL persistence.
