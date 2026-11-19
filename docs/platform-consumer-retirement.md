# Explicit platform consumer retirement

This is a permanent decision about one exact `(consumer_id, artifact_kind,
scope_key)`, not node decommissioning, heartbeat expiry or a proof that an
unreachable process has stopped. It does not change topology, replicas, routes,
current LKG, rollback retention or compiler enforcement. Other scopes and artifact
kinds on the same physical node remain independent identities.

The schema component installs an empty `fugue_platform_consumer_retirements`
ledger and a monotonic epoch. Installation alone retires nothing. The API image
contains the operator-only maintenance command; there is no new HTTP endpoint.

Prepare a strict JSON contract (at most 32 distinct exact targets):

```json
{
  "version": 1,
  "actor": "authorized-operator",
  "reason": "Explicit authorization to permanently abandon old identity recovery",
  "permanently_abandon_recovery": true,
  "targets": [
    {"consumer_id": "edge-worker:retired-host", "artifact_kind": "edge_route_bundle", "scope_key": "global"}
  ]
}
```

Read-only coherent preview using the API-role database:

```
fugue-input-storage --mode consumer-retire --consumer-retirement-contract contract.json
```

Review the exact identities, instance IDs and `plan_sha256`. Under the existing
maintenance opt-in (`FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED=true`), apply
the same contract and exact preview hash:

```
fugue-input-storage --mode consumer-retire --consumer-retirement-contract contract.json \
  --consumer-retirement-plan-sha256 sha256:EXACT_PREVIEW_HASH --apply
```

`-` reads a bounded strict JSON contract from stdin. Unknown fields, duplicate
targets, missing consent, stale facts, missing schema/ALWAYS triggers and changed
plan hashes fail closed. There is no force, age-based expiry or wildcard match.
Actor is an operator-supplied audit attribution, not a new authentication mode;
execution requires the existing privileged maintenance access. Do not include
credentials in a contract.

Apply is one read-committed transaction, with 250ms lock and 5s statement limits,
a 15s overall budget, 1MiB work memory and the installed admin-owned zero-temp
guard. It exclusively locks the retirement epoch before reading facts. Ordinary
serving/shadow/expectation/activation writers hold its shared side until commit;
repeatable-read writers carrying an older fence snapshot cannot pass afterward.
The command performs no network, body verification or retry under that barrier.

A target still enrolled in an active or selected **non-shadow** release expectation
is refused. First change its authorized serving topology through the existing
publication workflow; retirement cannot silently shrink serving membership.
Non-serving shadow observations may be abandoned, but the shadow publication,
lane, expectation and their independent history obligations are **not** retired.
The preview reports these `retained_shadow_expectations` explicitly; they may
continue to block whole-history collection. The preview binds database/schema,
target set, actor, reason, retained shadow expectation IDs and complete serving/shadow snapshots. Apply stores
those facts without rewriting actual/LKG, then removes only those exact live
consumer slots in the same transaction. All targets commit together or none do.
An exact retry is idempotent. Unknown commit outcomes must be inspected/retried
with that same contract/hash; never invent a new identity or broaden the batch.

The durable tombstones reject serving or shadow reinsertion, re-enrollment in
new expectations and reactivation of old enrolled releases/lanes, including
direct old-binary writers and replica-session writes. Ledger UPDATE/DELETE/
TRUNCATE and epoch downgrade are refused. It is a database backstop, not a
promise against a database owner deliberately disabling schema protections.

For global releases with a signed offline-edge recovery policy, a successful
probe cannot lift the hold if either required global worker route enrollment
has been permanently retired. The producer retains the original hold in newly
compiled releases and still requires the remaining consumers to converge. It
does not alter historical releases or enroll a replacement identity. If the
retirement ledger cannot be read, candidate capture stops rather than guessing.

### A new global Worker enrollment on the same physical Node

To deliberately return a physical Edge without reviving an abandoned consumer,
deploy replacement enrollment support to the API, producer and Worker. Then add
`worker_enrollment` to its existing recovery hold in the signed global
projection policy, using the fenced `edge_offline` configuration lane. Supply
a fresh 32-character lowercase hex `id`, the exact Kubernetes `node_uid`,
the Worker `service_account`, sorted and unique `daemonset_uids` for the
declared rollout slots, and a UTC `pod_created_after` timestamp. The approved
Worker must be running on the original Node and created strictly after that
cutoff; an old Pod, foreign workload, changed Node UID, or Cell credential
cannot use the new identity. Only a global Worker with both route artifact
capabilities, both original route slots retired, and neither successor slot
retired can receive the new identity.

Both route and TLS expectations use the new consumer key while DNS, endpoint
probes and routing keep the original physical Edge ID and addresses. Recovery
checkpoints are bound to the updated declaration: old proof, convergence,
cursors and LKG are not transferred. Normal signed candidate readiness,
authenticated Worker convergence and DNS publication gates still apply.
Keep this signed enrollment declaration with the hold after readmission; it
is the authority for future Worker restarts. Do not use an old declaration or
remove a tombstone as rollback.

Archived audit snapshots are deliberately not restoration/retention roots:
their prior recovery obligations have explicitly been abandoned. Consequently
the native root inventory stops retaining those old consumer generations without
needing to hide an unresolved reference or falsify an actual/LKG heartbeat.
Physical history, input identities, shared packs/components and other owners are
untouched. `history_deleted=false` and `node_execution_fenced=false` remain true
statements after success. A disconnected process may still serve cached data;
this mechanism fences its control-plane identity, not its network or CPU.

Independent local input pins require their separate existing exact-key/token/
digest release protocol (`archive-local-unpin`). Releasing a pin retains its
release tombstone and does not delete its bytes. Neither consumer retirement nor
unpin authorizes whole-history deletion or compiler-contract enforcement; rerun
bounded readiness and honor every remaining owner and rollback obligation.
