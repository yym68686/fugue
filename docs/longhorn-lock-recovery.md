# Longhorn backup lock recovery

Longhorn remote deletion and backup creation use incompatible backup-store
locks. Deletion can outlive the API request, so a short retry chain may finish
while Longhorn is still collecting shared blocks.

Automatic Longhorn backup runs whose error reports `failed to acquire lock`
during `backup create/restore` retain the existing 5, 15 and 30 minute retries,
then retry hourly, up to 24 retries. Other failures retain the three-retry limit;
manual runs remain explicit user operations. Each attempt remains in run history.
The scheduler reconciles an active policy's latest failed run on startup and each
minute, including failures exhausted by older versions. Retry creation compares
the policy's last-run ID under its database lock, so concurrent or delayed workers
cannot create duplicates or revive a superseded failure. Due times are anchored
to failure completion and survive worker restarts.

The background Error collector processes only Fugue-labelled Longhorn objects
whose run failed with that same lock error at least 24 hours ago, with no artifact
row, no remote URL, no remote volume identity, no completed/uploaded data and no
remaining source Snapshot CR. Both the object's creation and run completion must
be outside the grace period. Collection waits while the policy's latest run is
pending or running. Inventories follow Kubernetes pagination; each sweep requests
at most 50 deletions and runs under a global advisory lock and a 45 second budget.

Immediately before requesting deletion, the collector reads the Backup CR again,
checks ownership and the original UID, and sends UID/resourceVersion deletion
preconditions. The Longhorn engine finalizer must remain present. Fugue never
removes finalizers, storage locks or R2 shared blocks. Per-object requests and
deferrals are logged; the failed Fugue run remains as the audit history. Completed
backups and ambiguous Error records continue to require artifact reconciliation.
