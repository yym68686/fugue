# PostgreSQL bootstrap CPU request

CNPG 1.29 supports `cnpg.io/podPatch` on each Cluster. Fugue uses that existing
contract to lower only `bootstrap-controller.resources.requests.cpu`. The
controller must first include `bootstrap-cpu-isolation.patch`: upstream shares
the init/main resource maps and otherwise overwrites an init-only override
while unmarshaling the patched Pod. The Pod patch first tests the init container's
name, so an incompatible generated Pod fails admission rather than modifying
another container. Database CPU recommendations and runtime resources remain
independent. Memory and all limits retain their previous values until startup
measurements justify a separate change.

The production declaration is
`deploy/environments/production/managed-postgres/bootstrap-cpu.json`.
Only explicitly enrolled database UIDs and system IDs are eligible. No default
policy is applied to other databases. An existing different `cnpg.io/podPatch`
is rejected rather than replaced. Do not add production names to the executor.

Push changes to `main`. The `postgres_bootstrap_cpu` lane in `ci.yml` runs
the same unit tests and prepares server-side dry runs, SQL identity probes,
bound-claim evidence and the other databases' Pod/configuration inventory.
The node must have a full replacement's spare CPU and memory before any restart;
the old Pod's eventual release is not counted. DELETE previews put `dryRun: [All]`
in the request body alongside preconditions, since Kubernetes ignores query
options when a DELETE request body is present.
The prepared witness is uploaded before execution. The job checks that the
declaration and executor still match current `main` before any write.

Changing the annotation alone does not replace existing init containers. The
executor gracefully deletes exactly the enrolled original Pod using API-side
UID and resourceVersion preconditions, retaining its termination grace period.
This is an explicitly authorized maintenance operation on a single-instance
database: connections can briefly fail. It is not an eviction and does not
weaken or modify its PodDisruptionBudget. No PVC is deleted or resized.

Each replacement must stay Ready for 30 seconds, pass a read-only SQL system ID
probe, retain its claims, node, database resources and all other init resources,
and complete bootstrap without a restart before the next database is touched.
The job also verifies that the non-enrolled database inventory is unchanged.
Both preflight and result evidence are retained as GitHub Actions artifacts.

The run is resumable: an optimized replacement Pod is verified without another
restart. An unrelated replacement with the old init resources is rejected.
Failure stops before the next database; the durable evidence identifies the
last action. Do not automatically delete a second Pod to recover from a failed
first startup. Diagnose and submit any corrective declaration through the same
CI entrypoint. A rollback needs the previous annotation and a separately
authorized replacement if the new Pod already exists.

`1m` is a CPU reservation, not a CPU limit. Retaining the previous limit allows
short CPU bursts during copying, but a busy node may still make startup slower.
The first production enrollment is a limited canary, not evidence that every
database can adopt the same policy without observation. Existing main-container
requests remain until the regular resize policy adjusts them. Lowering only
the init request does not immediately release CPU if the main request is larger.
