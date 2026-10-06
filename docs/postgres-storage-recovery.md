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
The optional task requires updater protocol v39; only participating nodes need
the upgrade. The task verifies the exact backing file, loop device and sole PV,
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
