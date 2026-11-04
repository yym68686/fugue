# Local database snapshot review

`python3 -m scripts.plan_longhorn_snapshot_recovery <declaration> --evidence <path>`
is a read-only review, including when called by the snapshot review step in the
existing `postgres_volume_recovery` job in the main CI workflow. It has no apply
mode. It does not acquire a
Lease, delete a Snapshot/Backup CR, initiate a purge, change retention or write SQL.

An exact declaration binds the current database, primary Pod, PVC, volume,
Snapshot UID and original backup artifact/run. The review requires every ledger
alias to be terminal, unprotected, physically retired remotely, and free of live
restore references. It also checks complete bounded CSI/clone/remote-backup
inventories, an empty shared backup/release Lease, a healthy single RW replica,
and the native v1.12.1 engine implementation. Truncated or unsupported observations
fail closed. Unknown system snapshots are included in the native purge scope;
only an additional head-parent prune is accepted for review.

The existing replica instance manager runs a bounded metadata-only FIEMAP probe.
No data content is read, no SYNC flag is set, and the live head is never opened.
Snapshot file identity and allocation are checked before and after the probe.
The native engine folds **child into parent**, then replaces the child with the
merged inode. Its full child-data write traffic and newly allocated logical
ranges are reported separately. The planner preserves at least 25 percent of
physical disk capacity, an additional 16 GiB, and a declared working reserve.
Mapped overlap is an estimate of potential release; exact reclaimable bytes are
deliberately unknown until native completion and actual filesystem measurement.
The physical free counter uses `f_bfree`, matching the root-run storage engine;
filesystem `df Available` can be smaller because of reserved root blocks.

`ready_for_review` is **not authorization or permission to delete**. The separate
executor still needs the shared backup/release coordination Lease held through
native purge quiescence, fresh object/ledger/reference checks, UID and resource
version deletion preconditions, failure recovery, and database/API latency and
physical-space acceptance. It must never delete replica files directly, remove
finalizers, modify remote backups, unprotect restore points, or batch-delete
snapshots based on size estimates. Already-retired remote artifacts alone do not
prove that their local Longhorn snapshot layers were cleaned up.

## Explicit single-snapshot execution

`scripts.recover_longhorn_snapshot` is the separate execution entrypoint. It
defaults to a read-only preview. Apply requires the main production Actions
environment, an explicit approval binding the exact declaration SHA256 and
Snapshot UID, and all fresh planner gates. The existing serialized
`postgres_volume_recovery` job is the only release path. No SQL write is used.

Before DELETE, a durable operation ConfigMap records the exact target, fencing
token and review. The existing backup/release Lease uses a `release/` owner:
ordinary backup workers cannot take it over merely because its time expires.
Exact-volume admission policies independently fence new Longhorn snapshots,
backup references, source-volume changes/clones and CSI/PVC clone references.
Native status and finalizer reconciliation remain allowed. Every independent
reference fence must actually deny a server-side dry-run; policy type checking
must also finish without warnings. Existing pending references, incomplete
inventories, another native operation, changed ancestry or insufficient working
space stop execution. These guards do not claim to fence a privileged operator
issuing out-of-band engine RPCs or deleting the guards themselves.

Only the approved Snapshot gets a UID+resourceVersion DELETE. Longhorn performs
the fold/purge. The executor never sends an engine purge or removes a finalizer
or file. Any possible DELETE commit is journaled *before* the request. Unknown
outcomes, native errors, health loss and timeout retain the durable fences.
Another invocation cannot issue another DELETE or release protection while the
original target remains intact. If the exact UID is deleting, its native layer
is already gone, purge is idle and every retained point remains, it can repair
the known nullable SnapshotSpec compatibility rule under the same Lease. The
complete old/new admission specs and all durable UIDs must match; unknown edits
stop recovery. The complete structural SnapshotSpec schema is pinned before
explicit typed field comparisons; new fields or unknown-field preservation stop
execution. Absent and null `labels` are equivalent, and an absent boolean uses
the native false default; actual field values and non-null labels remain
immutable. A retained-child server dry-run must
accept typed normalization and deny real spec changes. A metadata annotation
can requeue Longhorn's native finalizer reconciliation; the executor never
changes a finalizer or sends another delete/engine operation. A known
pre-delete abort can be retried after its guards have been removed and all gates
are re-evaluated. A completed operation is inert on replay.

Success requires the CR and engine layer to disappear, all other user recovery
points to remain, purge quiescence on two polls, unchanged primary identity and
restart count, public readiness and the original physical reserve. Only then are
admission guards and the shared Lease released. The durable receipt contains
actual bottom-disk free counters and PG filesystem available bytes. The free
counter change includes concurrent business writes: it is not an exact causal
reclaim measurement. Local snapshot cleanup does not itself enlarge PostgreSQL's
filesystem. A separate expansion still needs its own current capacity gate.
