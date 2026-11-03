# Storage maintenance

Platform administrators can inspect durable, restart-safe cleanup jobs:

```sh
fugue admin storage jobs ls
fugue admin storage jobs run longhorn-reconcile --wait
fugue admin storage jobs run longhorn-reconcile --confirm --wait
fugue admin storage jobs run retention --scope POLICY_ID --wait
fugue admin storage jobs run retention --scope POLICY_ID --confirm --wait
fugue admin storage jobs run transfer-expiry --wait
fugue admin storage jobs run data-gc --scope WORKSPACE_ID --wait
fugue admin storage jobs get JOB_ID --wait
```

Without `--confirm`, `run` creates a persisted preview. Execution re-reads
current references and object identity; a preview is never deletion authority.
The platform administrator storage page exposes the same jobs.

The worker periodically reconciles enabled-policy retention, completed Longhorn
backups and abandoned transfers. It sweeps each data namespace daily with a
seven-day object age floor. An upload/download retires only after 30 days
without activity AND expired authorization. Authorization expiry alone does
not retire a transfer. Compare-and-swap prevents a retired task from being
resurrected by a stale worker. Unfinished multipart uploads are aborted before
the retired task's plan is discarded.

Data GC considers references from every tenant and workspace sharing a bucket,
including active transfers and completed migration source copies. Final
reference checks and deletion share a fence with reference writers. Missing
reference backends or incomplete inventories stop collection.

Retention runs independently of successful new backups. Protected and restoring
artifacts are excluded. Disabled policies keep history by default; only an
explicit policy-scoped retention job reconciles their existing retention rule.
Manual backups have no implicit expiry. Expiry is a logical lifecycle change;
the existing grace period and physical cleanup workflow still apply.

Failed Fugue runs whose Longhorn backup completed are reconciled from paged
engine inventory. Config identity, block map and referenced object presence
must validate before registration. Original failure evidence remains intact;
historical runs older than seven days are protected when recovered. Object
presence does not prove a successful database restore (`restore_verification`
remains `not_run`). Unknown ownership or missing blocks is reported, never
treated as evidence to delete.

Longhorn deletion is serialized per repository volume. Its finalizer owns
remote backup removal and shared-block GC. A finalized deletion receipt does
not measure the reclaimed R2 bytes; consult repository measurements separately.
Pending deletion jobs retry with bounded backoff. Ordinary failures exhaust
after five attempts and preserve their error/result for inspection.
