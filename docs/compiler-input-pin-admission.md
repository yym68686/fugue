# Transaction-side verified pin admission

`lockCompilerInputVerifiedPins` is an internal, bounded building block used by
the dependency preparation and admission paths. The persisted history phase
controls ordinary requests: `capability` preserves their original business
checks, while `enforced` also requires the signed dependency closure and exact
durable readback pins. Explicit preparation confirmation is strict in either
phase. Fenced or retired storage cannot use the capability compatibility path.
The migration-only full-history enabling coordinator and physical retirement
entry points have been removed. Existing persisted enforcement is still read
and enforced; removing the coordinator does not downgrade an old database.

The existing local-pin protocol first commits a protective pending record,
performs complete independent local SHA readback outside the writer lock, then
promotes the unchanged pin/manifest/epoch by CAS. The new transaction-side check
consumes that durable record using an exact key/token/digest, not a caller's
`verified` boolean or in-memory cache.

Before taking artifact/release/consumer/task row locks, a read-committed writable
activation transaction takes shared GC then shared archive-writer barriers.
Within at most the caller's existing timeout or 250ms lock / 5s statement limits,
it checks the installed pin schema and each exact verified pin, current manifest
representation and epoch. Pending, released, missing, stale-epoch or changed
representations fail closed. Shared barriers permit concurrent short admission
transactions, but exclude protocol unpin, re-verification, representation
maintenance and retirement until commit. There is no retry, network call or
body read in this transaction. Repeatable-read snapshots and read-only
transactions are refused to prevent consuming an obsolete verified label.

The caller must roll back on **any** error. On success it must install the actual
activation's durable retention root and commit in the same transaction. The
check protects through commit only; it neither transfers pin ownership nor
provides lifetime protection after the activation root is retired.

## Preparation and consumption

One batch of up to 32 pins is not evidence of a complete closure. Preparation
binds the server-derived complete closure, artifact signatures, physical tuple
witnesses, representation, keyring revision, epoch and short expiry. Consumption
rechecks every batch and witness in one transaction and installs the real
retention root atomically. Restore and full-SHA work remain outside publication
and heartbeat locks. Operator booleans cannot replace the proof; a checked
prefix cannot commit as a successful activation.

Integrated entry points retain their original request-specific authority:

| Activation | Transaction boundary | Dependency admission |
| --- | --- | --- |
| Publication / rollback | `pgReleasePlatformArtifact`, `pgRollbackPlatformArtifact` | Prepare outside the short publication lock; consume the exact closure witness inside the transaction |
| Expected consumer set | `pgCreatePlatformExpectedConsumerSet` / insert helper | Bind parent, selected release, child generation and scope |
| Consumer registration / heartbeat | both trusted and ordinary consumer paths | Protect new identities or generation transitions without rereading all bytes on every unchanged heartbeat |
| Shadow consumer | `pgAcceptPlatformShadowConsumerHeartbeat` | Same scope/parent identity and durable admission rules |
| Task creation / claim | `pgCreateNodeUpdateTask`, `pgClaimNodeUpdateTask` | Bind server-derived payload dependencies; pending protection precedes creation and verified pins precede claim |

Publication and rollback retain their existing 30-second total work budget through
input admission, including signed DNS source authority validation. Other activation
transactions retain the 10-second admission limit. A shorter caller deadline always
wins; the 5-second SQL statement and 250ms lock-wait limits remain unchanged. No
timeout grants admission, retries a mutation, or bypasses the final epoch, signature,
pin, selector, keyring-revision or proof-expiry checks.

## Historical admission-state compatibility

There is no full-history enabling operation in the current binary. The unused
retained-census preparation, enabling CAS and all-session/prepared-transaction
drain observer have been removed, rather than exposed as another migration
coordinator. Ordinary proof preparation can only start from selected runtime
roots; it has no switch to expand into the complete retained census.

The existing phase/epoch reader and transaction-side locks remain until the
current runtime dependencies have a complete replacement contract. An existing
enforced phase cannot be downgraded, erased or replaced by a different plan.
Missing or invalid state fails closed, and a stale capability proof cannot cross
a persisted state change. This is compatibility with stored obligations, not
permission to resume full-history migration or retire relations.

Signatures, authorization, lease/TTL, rollback retention and missing-history
isolation remain mandatory. Complete H/C/D/U, exhaustive reader/writer coverage,
same-version isolated production restore/restart and real physical retirement
are still unproved.

Tests use disposable loopback PostgreSQL with the real schema and real full
local readback. They cover fresh-connection persistence, concurrent admissions,
unpin exclusion through commit, exact token/digest/key, pending/epoch/schema/
representation failures, timeout tightening, old snapshots, cancellation,
writer contention and multi-pin rollback. Isolated PostgreSQL 18 additionally
checks persisted phase compatibility, fresh-pool enforcement, concurrent state
changes, missing state and durable downgrade refusal.
Synthetic local tests are not a production restore or activation exercise.

The registered binary/current/LKG checker keeps executable capability JSON at
16KiB. Immutable release manifests and monitor records use their separate
2MiB metadata boundary, with duplicate-key rejection and exact manifest/image
bindings intact. A normal API manifest can exceed the executable protocol's
limit without implying an incompatible binary. This separation does not
certify old schema rollback binaries or unregistered/offline clients.
