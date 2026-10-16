# Independent build permission on shared nodes

Application sharing and build permission are separate node-policy decisions. An
explicit `allow_builds=false` survives normalization, runtime registration, pool
refresh, and storage reload. It leaves application sharing and ownership intact.

On a shared node with builds disabled, reconciliation writes `fugue.io/build=false`
and `fugue.io/role.builder=false`. Both the inventory view and builder scheduler
honor this explicit exclusion before legacy shared-node fallback. A shared node
with no declared build role retains the legacy fallback. New builds can use other
eligible nodes or queue when capacity is unavailable; running workloads are not
evicted and serving configuration is not rewritten.

Use the normal node-policy API/CLI to change the intent and verify its readback.
The CLI rejects an explicit build request if the response does not confirm both
saved and effective permission. `node_reconciled=false` alone may mean an already
converged no-op, so it must not be confused with a mismatching desired role.

Regression coverage exercises persisted JSON and PostgreSQL policy, repeated
runtime refresh, API reconciliation, scheduler fallback with an alternate node,
and CLI responses for ignored, unapplied, missing, and already-converged policy.
