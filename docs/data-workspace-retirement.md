# Safe data workspace retirement

`fugue data workspace delete` retains its metadata-only semantics. It does not
delete referenced snapshots or imply that object-storage bytes were reclaimed.

Use the durable retirement workflow to retire all data belonging to a workspace:

```sh
fugue data workspace retire example-dataset
fugue data workspace retire example-dataset --cancel-planned-transfers --revoke-grants --evict-caches
fugue data workspace retire example-dataset --cancel-planned-transfers --revoke-grants --evict-caches --confirm
fugue data workspace retirement <job-id> --json
```

Both commands without `--confirm` are previews, even when cleanup options are
selected. Inspect the resolved workspace ID and blockers before confirming.
Manifest-derived unique/shared byte estimates are not R2 measurements and are
not a guarantee of the eventual reclaimed amount.

Execution requires `data.delete` and workspace administrator access. Applications
and active operations must be detached explicitly. Running uploads, downloads,
and migrations remain blockers; `--cancel-planned-transfers` is not a force flag.
Enabled backup policies, active backup runs, and retained snapshot-reference
backup artifacts also block retirement; retire those through their own lifecycle
instead of leaving a registered backup whose underlying data has disappeared.
Cache eviction follows the existing runtime cleanup lifecycle and waits for an
observed `removed` state, including when a runtime is temporarily unavailable.

Confirmation atomically freezes new snapshots, transfers, grants, app references,
and backend changes. The task survives API restarts. After caches are removed it
soft-deletes snapshots and persists a reference-release clock. Reclamation waits
at least seven days after release and any outstanding transfer authorization
expiry; old blob modification times cannot bypass that clock. A recently modified
remote blob extends the grace period. There is no API option to shorten it.

The normal namespace GC protects retirement tombstones. Retirement itself only
examines immutable keys from that workspace's snapshot/transfer manifests and
migration history. It rechecks all tenants' live references in the physical
namespace under the writer fence. Shared blobs survive. It does not delete a
workspace prefix or touch Longhorn backup blocks.

Work is checkpointed in bounded batches. Counters represent confirmed delete
acknowledgements, not pre-existing missing objects; a crash between a remote
delete and checkpoint may undercount but must never overclaim reclamation.
Only after all candidates are deleted, absent, or retained by other references
does the worker remove workspace metadata. The task's audit identity and status
remain queryable after metadata deletion, even for the last workspace.
The original project boundary remains enforced for task access. The durable
task also retains backend identities so normal namespace GC continues after
the last workspace is removed, including for late orphaned uploads.

`--wait --timeout 5m` polls until completion or the bounded timeout. A CLI timeout
does not cancel the server task; normal seven-day grace is not an error. Failed
tasks retain their freeze and can be requeued with the same retirement command
and `--confirm` after the reported cause is resolved. Repeated confirmation
returns the existing task instead of duplicating work.
