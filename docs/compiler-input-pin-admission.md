# Transaction-side verified pin admission

`lockCompilerInputVerifiedPins` is an internal, bounded building block used by
the dependency preparation and admission paths. The persisted history phase
controls ordinary requests: `capability` preserves their original business
checks, while `enforced` also requires the signed dependency closure and exact
durable readback pins. Explicit preparation confirmation is strict in either
phase. Fenced or retired storage cannot use the capability compatibility path.
The complete production enforcement coordinator and physical retirement are
still unfinished; public retirement remains refused.

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

## One-way enforcement transition

The process-owned preparation for the private enforcement transition covers the
complete native retained census, signed closure and existing verified pins. Its
short commit takes exclusive GC/archive barriers and root-table SHARE locks,
then rechecks roots, tuple identities, pin tokens/representations, epoch, keyring
and expiry before the one-way phase CAS. The persisted enforced phase cannot be
downgraded, erased or replaced by a different plan.

Both before witness checks and after the CAS, fresh PostgreSQL statistics must
show no outstanding old snapshot transaction or prepared transaction. Effective
full statistics visibility is required; hidden NULL fields cannot prove an idle
session. Statistics are cleared before each check. Failure rolls back the CAS;
the transition never cancels sessions or resolves another prepared transaction.
This catches a reader that has not touched an input table or application lease.

Transaction drainage is separate from complete reader inventory. It cannot
certify an unregistered or offline binary, all promised rollback versions,
external retention authorities, resource headroom or a confidential restore.
The private switch has no production HTTP/CLI activation path: the formal
coordinator must verify those remaining real prerequisites before exposing one.

Signatures, authorization, lease/TTL, rollback retention and missing-history
isolation remain mandatory. Complete H/C/D/U, exhaustive reader/writer coverage,
same-version isolated production restore/restart and real physical retirement
are still unproved.

Tests use disposable loopback PostgreSQL with the real schema and real full
local readback. They cover fresh-connection persistence, concurrent admissions,
unpin exclusion through commit, exact token/digest/key, pending/epoch/schema/
representation failures, timeout tightening, old snapshots, cancellation,
writer contention and multi-pin rollback. Isolated PostgreSQL 18 additionally
checks native phase enforcement, unregistered old snapshots, prepared
transactions, effective statistics privilege and durable downgrade refusal.
Synthetic local tests are not a production restore or activation exercise.

The registered binary/current/LKG checker keeps executable capability JSON at
16KiB. Immutable release manifests and monitor records use their separate
2MiB metadata boundary, with duplicate-key rejection and exact manifest/image
bindings intact. A normal API manifest can exceed the executable protocol's
limit without implying an incompatible binary. This separation does not
certify old schema rollback binaries or unregistered/offline clients.
