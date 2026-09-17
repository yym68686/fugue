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
