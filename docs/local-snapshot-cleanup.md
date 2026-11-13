# Durable local Longhorn snapshot cleanup

Local Snapshot CRs and remote Backup CRs are separate lifecycles. Remote
artifact retirement is not evidence of local space reclamation. Backup workers
therefore do not issue unjournaled DELETEs from process-local defers anymore.
They reuse run-labelled sources (including pre-Backup snapshots), use stable
names across ambiguous creates, and coordinate every Longhorn backup with the
shared backup/release Lease, including managed tenant databases.

The committed `local-snapshot-cleanup/policy.json` is standing authorization
for the bounded reconciler in the single production `ci.yml` entrypoint. Deploy
the coordinating API before enabling the policy. Each push and the existing
five-minute schedule re-discover the labelled local inventory independently of
worker memory, artifact list display windows, and previous job success. Native
operations remain serialized with database recovery. V2 executes a bounded
sequence, re-discovering and measuring after every single native completion.
No extra capacity, replicas or paid resources are provisioned.

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
policy digest, delete intent, fencing token and progress receipt. Scheduled
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
single-link, non-shared immutable snapshot mappings. It requires available
space for the **whole child's mapped writes**, plus working reserve, the
declared hard extra reserve and a foreground-growth reserve. The latter is the
larger of the configured floor and twice the observed physical allocation rate
over the configured operation window. The overlap-derived smaller allocation
estimate and any future reclaim are never credited to admission. The full native
scope allows only the selected fold and a zero-copy removed live-head-parent
prune; unknown additional folds remain blocked.

The shared platform Lease serializes all Fugue-managed Longhorn backups and
cleanup across volumes, which is stricter than a per-disk cleanup mutex. External
filesystem writers are covered by the growth reserve, not implicitly paused.
There is no guarantee against arbitrary unbounded concurrent growth. Admission
and native monitoring also enforce explicit database `SELECT 1` and public API
readiness round-trip latency limits (including exec/network overhead).

Feasible work is ranked by fold writes with an age weight. Byte caps authorize
whole native operations; they are **not bandwidth limits**. Roll out one small
canary, inspect actual duration/latency/space, then authorize larger folds. This
implementation does not modify the online engine or claim native throttling.
The pinned sparse-tools Linux test exercises the real upstream fold and retained
data view; it does not replace production health/space admission.

An observation window ending records `native-running`, progress, stagnation and
physical/health observations, and yields without unlocking or replaying DELETE.
The next scheduled reconciliation resumes it first. This is periodic monitoring,
not a continuously resident capacity controller. Errors and unknown outcomes
retain protection. The durable completion floor uses the same available-space
budget as admission, not the unrelated scheduling threshold. The pass deadline
leaves headroom for discovery/native observation within CI's job timeout.

`--evidence` reports `idle`, `blocked`, `ready` (preview), native progress or
`completed`, including candidate blockers and the actual post-native filesystem
counter change. A blocked pass is not reported as reclaimed space. CI uploads
the summary and per-operation evidence even on failure; an incomplete native
operation with an error fails the job; a healthy `native-running` operation is
not falsely marked failed. Blocked candidates retry on later passes. Snapshot
size sums are never reported as guaranteed reclaimable bytes.

Read-only preview (SSH, when used, remains observation-only):

```sh
python3 -m scripts.reconcile_longhorn_snapshot_cleanup \
  deploy/environments/production/local-snapshot-cleanup/policy.json \
  --evidence /tmp/local-snapshot-cleanup.json
```

`--apply` is restricted to the standard main CI lane. Never manually delete
replica files, clear native finalizers or lower capacity gates to make a pass run.
