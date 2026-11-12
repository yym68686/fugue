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
operations remain serialized with database recovery and at most one snapshot is
executed per pass. No extra capacity, replicas or paid resources are provisioned.

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
metadata-only FIEMAP fold budget, at least the physical minimum plus 16 GiB and
working reserve, non-expiring-owner coordination and admission fences. Unsupported
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
ownership require explicit review while protection remains intact.

`--evidence` reports `idle`, `blocked`, `ready` (preview), native progress or
`completed`, including candidate blockers and the actual post-native filesystem
counter change. A blocked pass is not reported as reclaimed space. CI uploads
the summary and per-operation evidence even on failure; an incomplete native
operation fails the job. Blocked candidates retry on later passes. Snapshot
size sums are never reported as guaranteed reclaimable bytes.

Read-only preview (SSH, when used, remains observation-only):

```sh
python3 -m scripts.reconcile_longhorn_snapshot_cleanup \
  deploy/environments/production/local-snapshot-cleanup/policy.json \
  --evidence /tmp/local-snapshot-cleanup.json
```

`--apply` is restricted to the standard main CI lane. Never manually delete
replica files, clear native finalizers or lower capacity gates to make a pass run.
