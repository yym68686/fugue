# Transaction-side verified pin admission

`lockCompilerInputVerifiedPins` is an internal, bounded building block, not a
completed release or reactivation admission system. No production activation
entry point invokes it yet. It does not change the retirement safety rejection,
reader routing, retention policy, writer fallback or current service behavior.

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

## Remaining integration contract

The list of up to 32 pins is not evidence of a complete closure. A future
preparation/consumption protocol must bind the server-derived complete closure,
all artifact identities/signatures and representations, pending protection,
restore/readback results and an immutable activation intent. It must recheck
the closure/witness at consumption and install all retention roots atomically,
without doing full-SHA work under publication or heartbeat locks. No new
operator assertion may replace that proof, and batching cannot return a partial
activation as success.

Required entry points identified so far:

| Activation | Current transaction boundary | Required integration |
| --- | --- | --- |
| Publication / rollback | `pgReleasePlatformArtifact`, `pgRollbackPlatformArtifact` | Prepare outside short publication lock; consume exact closure witness inside the transaction |
| Expected consumer set | `pgCreatePlatformExpectedConsumerSet` / insert helper | Bind parent, selected release, child generation and scope |
| Consumer registration / heartbeat | both trusted and ordinary consumer paths | Protect new identities or generation transitions without rereading all bytes on every unchanged heartbeat |
| Shadow consumer | `pgAcceptPlatformShadowConsumerHeartbeat` | Same scope/parent identity and durable admission rules |
| Task creation / claim | `pgCreateNodeUpdateTask`, `pgClaimNodeUpdateTask` | Transactional server-derived payload dependencies; claim must not bypass pending protection |

Signatures, authorization, lease/TTL, rollback retention and missing-history
isolation remain mandatory. Full H/C/D/U, all entry-point coverage, same-version
isolated restore/restart and real physical retirement are still unproved.

Tests use disposable loopback PostgreSQL with the real schema and real full
local readback. They cover fresh-connection persistence, concurrent admissions,
unpin exclusion through commit, exact token/digest/key, pending/epoch/schema/
representation failures, timeout tightening, old snapshots, cancellation,
writer contention and multi-pin rollback. Local PostgreSQL 14 is not evidence
of a production PostgreSQL 18 restore or a production activation exercise.
