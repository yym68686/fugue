# Durable local Longhorn snapshot cleanup

Local Snapshot CRs and remote Backup CRs are separate lifecycles. Remote
artifact retirement is not evidence of local space reclamation. Backup workers
therefore do not issue unjournaled DELETEs from process-local defers anymore.
They reuse run-labelled sources (including pre-Backup snapshots), use stable
names across ambiguous creates, and coordinate every Longhorn backup with the
shared backup/release Lease, including managed tenant databases.

The production `ci.yml` publishes the executor on the declared control-plane
host as `fugue-longhorn-local-cleanup.service`. The service watches managed
Longhorn Backup deletions, then enqueues the matching volume, snapshot and
backup run. SQLite stores the work and watch position together. A 15-minute
inventory scan backfills missed events and already-retired sources. A watch
event is a wake-up signal. The ledger's physical retirement receipt and fresh
reference checks determine eligibility. The host service uses the existing
K3s administrator kubeconfig for native admission fencing.

`executor.json` selects `observe` or `active`. Start with `observe` and check
the exact release, live watch, queue and inventory receipt before enabling
writes with `active`. The policy in `policy.json` defines native admission.
The executor retries deferred targets with backoff, allowing other volumes
to proceed after a target-level failure. One native operation at a time
keeps the existing platform-wide coordination fence.

## In-cluster observation canary

A separately deployed Kubernetes CronJob observes the control-plane database's
Longhorn metadata every five minutes. It reports the policy digest, shared Lease
occupancy and the count of labelled local snapshots on the bound volume. It
does not access the backup ledger, inspect physical capacity, classify candidates
or authorize deletions. Its service account has no exec, secret, Lease-write or
snapshot-write permissions. It does not accept write-mode configuration.

The resident executor has its own watch and ledger health receipt. The
metadata observer keeps its separate read-only identity.

Every alias of a source must be unprotected, terminal, remotely physically
retired, free of active restores, and owned by a terminal run with no live lease.
Alternatively, a failed/canceled source without any artifact must have a distinct
terminal-run proof, no future wake-up/live lease, and a 24-hour terminal grace
period. Backup retries have new run identities and cannot reuse this source;
pending/running sources are never eligible.
Missing/ambiguous ownership and any remaining remote Backup CR block deletion.
Zero-upload lock-conflict Error CRs can be retired by the existing remote GC
while the exact owned local source is retained; unknown source ownership or any
evidence of remote upload still blocks that GC. This avoids a source/remote
cleanup dependency cycle without touching the source or shared remote blocks.
No protected point is automatically unprotected, no remote retention policy is
changed, and no unknown/system snapshot is treated as an owned candidate.

The reconciler reuses [the native recovery executor](local-snapshot-review.md):
exact fresh database/PVC/volume/snapshot identities, full bounded restore/clone/
CSI inventories, healthy supported single-replica topology, immutable child,
metadata-only FIEMAP fold budget, versioned physical reserve,
non-expiring-owner coordination and admission fences. Unsupported
topologies or insufficient working space remain blocked rather than bypassing
these checks. Retaining active local recovery points until remote retirement is
intentional; local count need not equal remote `retain_count` (protected aliases
also remain). A removed live-head parent can remain physically present until a
later immutable child makes native reclamation possible.

Durable native operation ConfigMaps include the exact declaration, approval,
policy digest, delete intent, fencing token and progress receipt. Online
reconciliation resumes an unfinished operation before discovering any new work,
even if its Snapshot CR has already gone. Unknown DELETE outcomes never cause a
second DELETE or automatic unlock. Known pre-delete crashes can retry only when
durable false intent, the exact complete fence inventory and Lease ownership
prove no DELETE was issued. Policy drift, unjournaled fences or ambiguous
ownership require explicit review while protection remains intact. New records
also retain the canonical policy bytes: a later policy revision resumes an
existing operation using its original immutable approval and budget, not the
new admission rules.

## Fixed-capacity recovery (v2)

V1 retains its historical new-replica scheduling threshold. V2 deliberately
does not change Longhorn's global scheduling setting. Its admission uses
`f_bavail` (not root-reserved blocks) on verified writable ext4, with regular,
single-link, non-shared immutable snapshot mappings. It supports only the
reviewed official native image/build (exact Git commit and
EngineImage UID/node deployment are rechecked, not a matching version suffix).
It requires available space for the **whole child's mapped writes**, plus working reserve, the
declared hard extra reserve and a foreground-growth reserve. The latter is the
larger of the configured floor and twice the observed physical allocation rate
over the configured operation window. The overlap-derived smaller allocation
estimate and any future reclaim are never credited to admission. The full native
scope allows only the selected fold and a zero-copy removed live-head-parent
prune; unknown additional folds remain blocked.

The shared platform Lease serializes all Fugue-managed Longhorn backups and
cleanup across volumes, which is stricter than a per-disk cleanup mutex. External
filesystem writers are covered by the growth reserve, not implicitly paused.
The executor publishing lane queues alongside explicit database maintenance
changes. Runtime reclamation uses the Kubernetes Lease and admission fences.
There is no guarantee against arbitrary unbounded concurrent growth. Admission
and native monitoring also enforce explicit database `SELECT 1` and public API
readiness round-trip latency limits (including exec/network overhead).

Due work is ordered by retry time and identity. The byte cap bounds whole
native operations. Each operation's estimated
duration must fit its foreground-growth window. This assumption is an admission
estimate, not an engine throughput guarantee or cancellation deadline. This
implementation does not modify the online engine or claim native throttling.
The pinned sparse-tools Linux test exercises the real upstream fold and retained
data view; it does not replace production health/space admission.

An observation window ending records `native-running`, progress, stagnation and
physical/health observations, and yields without unlocking or replaying DELETE.
The service resumes it first. Errors and unknown outcomes retain protection.
The durable completion floor uses the same available-space budget as admission.

`--evidence` reports `idle`, `blocked`, `ready` (preview), native progress or
`completed`, including candidate blockers and the actual post-native filesystem
counter change. On the executor host, inspect
`/var/lib/fugue-longhorn-local-cleanup/status.json` and the service journal.
The journal records the latest decision and retry reason. Native receipts
remain in Kubernetes. Snapshot size is an inventory metric. The native
completion receipt measures actual filesystem reclaim.
If the executor's fresh capacity sample denies admission after discovery, the
pass records `blocked` with that exact budget and sampling stage, not the stale
successful preflight. A fenced denial first unwinds only its known pre-delete
protection; native errors still fail, preserve partial-pass evidence and never
automatically unlock an issued/ambiguous DELETE.

Read-only preview (SSH, when used, remains observation-only):

```sh
python3 -m scripts.reconcile_longhorn_snapshot_cleanup \
  deploy/environments/production/local-snapshot-cleanup/policy.json \
  --evidence /tmp/local-snapshot-cleanup.json
```

Production automatic writes belong to the online executor. Use the separately
declared recovery process for an explicitly approved emergency operation.
