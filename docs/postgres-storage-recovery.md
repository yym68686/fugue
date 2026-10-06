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
3. Stream PGDATA read-only through authenticated Kubernetes exec, using the
   Kubernetes CA, into an isolated destination seed PVC. The existing source Pod
   is used because some RWO drivers cannot mount the same PVC in another Pod.
   Allocate destination recovery headroom only. Source capacity is unchanged.
4. Compare stream SHA-256 and a per-file manifest of data, WAL, modes, ownership,
   and checksums. Recheck the source after copying. Only continuously maintained
   instance configuration and process markers are excluded from the manifest.
5. Bootstrap an isolated CNPG cluster through supported PVC clone recovery,
   preserving database, owner, credentials and the exact PostgreSQL image.
   Require matching PostgreSQL system ID, target placement, ready primary Pod,
   writable SQL and matching service endpoints before selecting the target.
6. Commit the exact service configuration under the running operation's lease
   and a configuration compare-and-swap. Preserve the original application-facing
   Service hostname and reconcile its selector to the validated target. Verify
   SQL through that stable hostname before completing the operation.

A copied seed is never overwritten after target bootstrap begins. A resumed
operation must match persisted UIDs and target intent. After target selection,
retries finish endpoint verification; they never revert to the old source. The
source remains fenced and its PVC is retained for separately authorized cleanup.
Source recovery uses one PGDATA volume. External WAL, tablespaces, standby or
backup recovery markers are rejected. The target storage driver must support
PVC cloning. A failed bootstrap leaves serving selection unchanged.

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
