# Queued build activation and configuration isolation

A build's configuration merge must happen twice: when the build queues its deploy,
and when the controller starts that deploy. A scale or environment operation can
finish between those two points. Previously the second merge was absent. A queued
image update with an old replica count could be rejected as an unplanned restart;
without the zero-downtime guard it could restore the old configuration.

Build deploys now persist their enqueue-time configuration baseline in the existing
private operation source envelope. At execution, the store locks the operation and
app, merges intervening configuration (including nested edits and deletions), and
persists the snapshot actually used by rendering and completion. The build keeps
ownership of its immutable image and explicit restart token. Repeating the merge
is idempotent; terminal operations return a conflict. Failed activation leaves the
serving app unchanged. Existing operations without a recorded baseline retain
legacy behavior; the system never invents a baseline for historical requests.

This does not weaken the zero-downtime guard, change CLI account selection, or
change the public deploy request contract. It addresses the post-build queue
window; it is not a general three-way merge for arbitrary stale full-spec API
requests.

Validation: controller regression `TestQueuedBuildExecutionPreservesScaleAfterEnqueue`
reproduces `zero-downtime deploy refused: the requested restart has no validated
online rollout plan` with the execution merge removed and passes with the fix.
Store regressions cover scale, environment edits/deletions, idempotency, terminal
conflicts, and actual PostgreSQL persistence.
