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

`ready_for_review` is **not authorization or permission to delete**. A future
executor still needs the shared backup/release coordination Lease held through
native purge quiescence, fresh object/ledger/reference checks, UID and resource
version deletion preconditions, failure recovery, and database/API latency and
physical-space acceptance. It must never delete replica files directly, remove
finalizers, modify remote backups, unprotect restore points, or batch-delete
snapshots based on size estimates. Already-retired remote artifacts alone do not
prove that their local Longhorn snapshot layers were cleaned up.
