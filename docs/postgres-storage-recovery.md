# Managed PostgreSQL storage recovery

Platform administrators can run `fugue app db recover <app> --apply`. The CLI
waits for the operation by default; `--wait=false` returns the operation ID.
Without `--apply`, the response describes recovery stages and explicitly marks
live preflight as pending. It is not a capacity approval.

The controller preserves the largest persisted, CNPG-declared, or requested
PVC size. A stopped disk-full source with an already declared migration to a
different storage class is rescued before replication. Its two-GiB rescue
target is recorded durably. The source claim is retained, automatic expansion
is frozen on that cluster with a resource-version precondition, and actual
filesystem capacity plus writable SQL must converge before migration starts.
The source check ignores unrelated destination claims that have not started.
For Longhorn, recovery selects a ready attachment node within the target
runtime's scheduling constraints; CSI registration alone is insufficient.
An unstarted join Job pinned to an incompatible node is removed after applying
the corrected placement. An abandoned initializing replica PVC with no Pod or
Job references is retained with a recovery annotation, outside CNPG discovery
and garbage collection, allowing CNPG to bootstrap a fresh replica. No PVC is
deleted and an uninitialized claim is never marked ready. Whole-spec writes
preserve the explicitly observed CNPG in-use resize policy.

File-backed OpenEBS LVM pools can grow through the authenticated node updater.
The optional task requires updater protocol v43; only participating nodes need
the upgrade. A separate read-only task checks capacity before allocation is queued. The
executor repeats the same checks under its lock when applying. Its complete
bounded output is stored in task logs, and the exact failure reason is propagated
to the operation and CLI. A failed preflight never queues an expansion.
The task verifies the exact backing file, loop device and sole PV,
allocates at most 64 GiB, preserves at least 10%/5 GiB of host filesystem space,
and verifies loop/PV capacity. It reserves only the newly appended file interval;
existing sparse holes created by filesystem discard remain unchanged. The controller also preserves the LocalPV pool's
10%/5 GiB free reserve after the requested PVC growth. No LVs are deleted.

Existing localization checks still gate replica readiness, replication catch-up,
promotion and SQL service identity. An interrupted recovery does not perform a
blind rollback. Repeating the same active request returns its operation; a
conflicting target or another app mutation returns 409. Failed tasks and
operation progress identify the precise failed prerequisite.

Unknown primary identity, missing or stale capacity evidence, insufficient host
space, and unsupported storage layouts fail before the corresponding mutation.
This command cannot manufacture disk space or recover data from a lost volume.

## Project move with an offline database

Use `fugue project move <project> --to <runtime> --recover-offline --storage-class <class>`
for an independent managed PostgreSQL service bound to one app, together with
stopped dedicated application PVCs. `--dry-run` validates intent without queuing
operations; the response does not certify live storage capacity. The controller
validates source identity, storage capacity, and destination placement before
rescue. The command requires waiting and rejects `--skip-blocked`.

The CLI submits database recovery first, waits for verified localization, then
refreshes each application preflight before submitting its move. Stopped apps
remain stopped. Dedicated PVCs are copied to a new operation-specific movable
RWO claim in the explicitly selected storage class. Actual live consumers of the
source volume must be absent, both transfer pipelines must succeed, and their
SHA-256 digests must match before the application references the new claim.
Source claims are retained. A rerun skips resources already on the destination
and resumes an identical active offline operation; conflicting operations fail.
A later failure may leave earlier resources already migrated. Repeating the same
command continues from that durable state; it does not reverse completed moves.

`fugue service postgres recover <service> --to <runtime> --storage-class <class>`
exposes the same independent service recovery separately. Add `--apply` to queue
it. Platform administrator authority is required because source rescue may
require a guarded host storage task. Normal healthy moves remain available via
`fugue project move` without `--recover-offline`.

## Known limitation and cold recovery prerequisite

`--recover-offline` currently rescues a disk-full source and then uses normal
replication. It is not a direct physical cold-copy restore. When host headroom
cannot satisfy both pool growth and the existing reserves, recovery must stop;
this failure does not permit reserve reduction, deleting unrelated volumes, or
editing CNPG runtime status to make a copied claim appear ready.

The deployed CNPG source supports `bootstrap.recovery.volumeSnapshots.storage`
with `kind: PersistentVolumeClaim` for a fenced cold copy. Implementing this safely
requires a durable migration resource independent of code releases:

1. Record source cluster, primary, PVC/PV UIDs, PostgreSQL image digest, system
   identifier, placement, target storage intent and source resource versions.
   Reject separate WAL/tablespaces unless every volume is included atomically.
2. Acquire an exclusive service-and-binding mutation lease. Fence all source
   instances through declared CNPG hibernation and wait until no live or
   terminating Pod references any source volume. Keep fencing persistent across
   reconciler restarts and releases.
3. Mount source claims read-only. Copy to operation-owned destination claims,
   verify filesystem manifests and ownership, and record immutable digests.
   Never mark an uninitialized CNPG claim ready or overwrite the source.
4. Bootstrap an isolated replacement cluster using the supported recovery API,
   preserving the original credentials. Verify actual SQL recovery, system ID,
   schema/row invariants, WAL/checkpoint state and destination placement. A copy
   digest alone is not proof of a recoverable database.
5. Atomically persist the selected replacement cluster and all affected bindings
   only after validation. Reconciliation must preserve this selection, stable
   service identity, the recovery bootstrap, and the fenced source. Any write
   after cutover prohibits automatic rollback to the old copy.
6. Persist every phase and resource UID before acting. Retry must adopt the same
   resources, resume interruption safely, and never recopy over an activated
   destination. Retain source claims for explicit later cleanup.

Required validation before enabling execution includes a real operator/CSI test
with a disk-full, unclean PostgreSQL shutdown, a cross-class cold copy, an
interruption at each phase, concurrent configuration changes, missing WAL,
corrupted copy, failed bootstrap, and writes immediately after cutover. The
current implementation deliberately does not enable this unverified path.

Non-deployment CLI waits return a terminal `outcome: failed`, operation identity,
status, redacted server-reported reason, and commands to inspect operation facts.
Missing evidence remains explicitly missing; it is never converted into a
successful or hypothetical migration result.
