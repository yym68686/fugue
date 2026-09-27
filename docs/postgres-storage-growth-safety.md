# Capacity-only PostgreSQL growth

Increasing storage on the same database runtime and StorageClass uses a narrow
in-place operation. A retained primary node pin is not a placement change.
Database topology, failover configuration, affinity, image, resources and the app
Deployment are preserved. The operation does not render/apply the general app or
CNPG desired state and has no restart, switchover or shrink fallback.

Before writing, the existing PVC/StorageClass/LocalPV capacity and reserve gates
must pass. The live Cluster must explicitly allow in-use expansion. Fugue then
persists a continuity baseline containing Cluster UID, primary Pod UID, node,
container restart count/start time, SQL postmaster start time, and digests of the
Pod spec and the Cluster spec excluding capacity. Credentials and raw specs are
not included in that evidence.

The only Kubernetes writes are existing data PVC storage requests and a Cluster
JSON Patch to `/spec/storage/size`, conditional on its UID, resourceVersion and
previous size. A conflict stops the operation; a retry re-observes state and
retains the original baseline. No rollback tries to shrink a PVC.

Completion requires matching declared capacity, PVC and filesystem convergence,
an unchanged continuity baseline, and successful SQL through the read-write
Service. Polling does not constitute proof of every individual request's
availability; an observed interruption or identity change prevents a success
claim. External node/storage failure remains possible, and a failed operation
may already have enlarged some volumes. Inspect the operation evidence before
resuming.

## Bounded maintenance diagnostics

`fugue admin node-updater task ls` requests the newest 100 task summaries by
default. Use `--limit` (1–1000) and `--details` explicitly, or use
`fugue admin node-updater task show TASK_ID` for one exact task with its evidence.
The API adds optional `task_id`, `limit`, and `details` query parameters while
preserving the legacy response when omitted. Tenant authorization, node/status
filters and newest-first ordering apply before the limit. SQL summary reads do
not load payload/log JSON columns.

Image-cache inventory, prune-plan and prune task creation use compact CLI
receipts by default; `--details` requests manifest/blob arrays. Existing API
consumers retain full responses unless they request `summary=true`. Compact
responses keep plan/task IDs, totals and reason summaries. Mutation requests are
not blindly retried when their outcome is uncertain.

Delete-mode image-cache planning uses the executor's candidate-reason policy
before graph and alias protection. Unknown control-plane images remain
explainable in observe/dry-run mode but cannot poison an executable deletion
batch. The executor still rechecks fresh state and refuses stale or newly
protected targets. Those refusals are safety outcomes, not permission to bypass
the guard.

## Verification

Regression tests exercise legacy affinity, unchanged node pins, HA preservation,
partial growth, changed Pod UIDs, postmaster restarts, late replacement, affinity
drift, SQL failure and compare-and-swap conflict. HTTP fixtures reject any app
deployment, Pod deletion, or non-capacity Cluster patch. Optional disposable
loopback PostgreSQL tests verify the actual SQL witness and task projections.

The API contract and generated frontend types must be synchronized. Production
components are released only through the repository's declarative CI pipeline.
Do not validate a repair by forcing a production database restart or another
capacity change.
