# Managed PostgreSQL cold migration

`fugue project move <project> --to <runtime> --recover-offline --storage-class <class>`
coordinates an independent managed PostgreSQL service with exactly one bound app,
then moves the project apps. `--dry-run` checks intent; `live_preflight_pending`
means it does not certify live storage, fencing, or recovery readiness. Execution
requires platform administrator authority, `--wait`, and no `--skip-blocked`.

For a disk-full, stopped source the controller does not expand source storage:

1. Record source cluster, Pod and PVC/PV UIDs, immutable PostgreSQL image, target
   node/class/capacity and original service configuration fingerprint. A separate
   ConfigMap persists these facts across operations and code releases.
2. Fence the source via CNPG. Verify the instance-manager fencing metric and
   `pg_ctl` stopped status. Read `pg_control` identity and checkpoint WAL directly;
   a desired annotation or empty CNPG status field is not sufficient evidence.
   If the recorded primary never started, suspend reconciliation, gracefully
   terminate the exact source instances and verify their continued absence.
   A nonroot read-only reader then mounts the original primary volume; it does
   not start PostgreSQL or require the source primary's CPU request.
3. Stream PGDATA read-only through authenticated Kubernetes exec, using the
   Kubernetes CA, into an isolated destination seed PVC. The existing source Pod
   is used when fenced in place because some RWO drivers cannot mount the same
   PVC in another Pod; an unstarted source uses its verified read-only reader.
   Allocate destination recovery headroom only. Source capacity is unchanged.
4. Compare stream SHA-256 and a per-file manifest of data, WAL, modes, ownership,
   and checksums. Recheck the source after copying. Only continuously maintained
   instance configuration and process markers are excluded from the manifest.
5. Copy and physically verify a separate destination primary claim while the
   destination Cluster does not exist. Persist the receipt and claim UID, then
   hand it to CNPG's orphan-volume restoration using metadata-only UID/version
   compare-and-swap. This supports local storage without CSI cloning. Preserve
   database, owner, credentials and the exact PostgreSQL image.
   Require matching PostgreSQL system ID, target placement, ready primary Pod,
   writable SQL and matching service endpoints before selecting the target.
   Restore the requested HA instance/synchronous counts and require distinct
   storage-compatible nodes and caught-up synchronous replicas before cutover.
6. Commit the exact service configuration under the running operation's lease
   and a configuration compare-and-swap. Preserve the original application-facing
   Service hostname and reconcile its selector to the validated target. Verify
   SQL through that stable hostname before completing the operation.

A copied seed is never overwritten after target bootstrap begins. A resumed
operation must match persisted UIDs and target intent. After target selection,
retries finish endpoint verification; they never revert to the old source. The
source remains fenced and its PVC is retained for separately authorized cleanup.
Source recovery uses one PGDATA volume. External WAL, tablespaces, standby or
backup recovery markers are rejected. Prepared or activated primary claims are
never recopied. Older already-started clone recoveries retain their original
bootstrap method. A failed bootstrap leaves serving selection unchanged.

Live preflight checks primary and replica storage compatibility and actual
destination size, including recovery headroom, before stopping a source. A CSI
plugin alone does not prove the storage engine is available on that node.
If an offline attempt has copied its seed but never switched the stable service,
an administrator may retry with only the storage class changed. The controller
revalidates the unchanged stopped source and original endpoint, gracefully stops
the exact old target, and retains all old data claims and attempt identities.
It then copies a fresh seed and primary for the new class. Changed credentials,
HA counts or placement, an activated target and reuse of an old attempt class
are rejected. This does not authorize infrastructure changes or source cleanup.

Healthy sources use the existing replication/localization flow. Ordinary moves
remain available through `fugue project move` without `--recover-offline`.
`fugue service postgres recover <service> --to <runtime> --storage-class <class>`
exposes the same service recovery flow; add `--apply` to queue it.

The CLI waits for database validation before refreshing application preflights.
Stopped apps retain zero replicas. Dedicated application volumes are copied to
new movable RWO claims only after live source consumers are absent and matching
stream digests are recorded. Source claims are retained. Repeating the same
command continues from durable resource state, including an interrupted database
endpoint cutover; already completed app moves are skipped.

After migration, `fugue service localize <service> --to <runtime> --storage-size
10Gi --wait` expands the target PVC in place. It verifies filesystem convergence,
SQL readiness, Pod UID/restarts and PostgreSQL process identity. It must not be
used as a prerequisite to expand a stopped migration source.

Tests include guarded target bootstrap, source reconciliation fencing, exact
service CAS/lease and retry behavior, stable endpoint rendering, and an opt-in
isolated PostgreSQL crash/copy/replay rehearsal in
`scripts/tests/cold_postgres_rehearsal.py`. That rehearsal also detects a deliberate
WAL bit flip and verifies committed rows after replay; it does not replace live
operator/CSI readiness checks during execution.
