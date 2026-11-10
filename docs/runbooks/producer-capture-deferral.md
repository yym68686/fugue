# Bounded producer capture deferral

Status: local implementation and isolated validation only. The local process
registration/join adapter exists; there is no production installation, complete
rollout enrollment proof, prepare/enable endpoint, configuration switch or enabled
deferral. An authenticated observe/release endpoint exists for retained controls;
the normal server leaves the admission hook nil.

The private Unix `producer-runtime` observation is registered in the normal API
process independently of that hook. It makes no database/network call, installs
no schema and enables no deferral. It supplies one fresh Server boot identity,
PID and bounded process-local manager/worker/new-input function lifetimes.
Worker cancellation remains visible until the function actually returns;
overlapping workers in one scope remain distinct. The new-input boundary covers
normal direct and scheduled reconciliation even when admission is nil, and
includes admission, capture, compilation, persistence and publication until
return or panic. Returns count function exits, never success or commit.

At most 32 scope rows and 128 active worker identities are retained per boot.
Overflow or counter inconsistency makes `tracking_complete=false` sticky; it
neither stops business work nor later becomes a complete zero. Snapshots clone
their data, report local participant attempts separately from durable tickets,
and never certify fleet coverage. The participant, when privately installed
before startup, uses this same boot ID. A new boot's zero does not resolve old
work. Administrator evidence must independently bind the socket PID/boot to
exact Pod/container/owner/source identities, the full current runtime roster and
old-process exit. These observations do not themselves implement that protocol.

`internal/platformproducer/capture_deferral.go` implements a separate one-shot
control record for new automatic capture, compilation and input persistence.
It never changes a signed producer policy's mode or creates authority to run
work. The request names explicit producer scopes and each policy's exact
artifact ID, content hash, release ID and fencing token. It has an absolute
expiry at most 30 minutes after the original request; drain time consumes that
same window. It contains no customer, project or production scope fixtures.

Preparation refuses missing authority or missing per-scope in-flight evidence.
The draining phase blocks further new-input admission while already admitted
work finishes; the state machine neither cancels nor classifies that work.
Activation requires zero in-flight work for every requested scope. Restart
serialization preserves the original deadline and terminal tombstone. Expiry,
explicit release, authority drift and clock regression cannot reactivate the
record. The admission predicate also checks absolute expiry, so an old active
record cannot extend the deferral while awaiting a terminal CAS.

Removing this gate does not approve capture, publication, migration, replay,
resource allocation, signature renewal or evidence extension. All ordinary
fresh authority, signature, TTL, resource and publication checks remain required.

## Durable adapter and producer boundary

The PostgreSQL adapter serializes record CAS, exclusive scope claims, new-work
admission and completion with one short transaction barrier. It shares normal
policy publication locks, checks the exact active unfrozen lane and release,
and validates the real signed policy body. Administrative controls retain their
requesting actor; admission/completion additionally require the native producer
identity. The optional four-table schema is absent from startup migrations
and has no production installer here.

Each enrolled capture gets a durable ticket before capture. Its completion
handle covers compilation, persistence and publication, including errors. A
failed or ambiguous admission commit never starts work. Completion failure
leaves an occupied ticket. Process or PG restart never turns missing work into
zero: abandoned tickets have no lease timeout and are not automatically declared
complete. A drain can expire without reaching active. Already admitted work is
neither canceled nor retried. Activation requires zero tickets for every scope.

Operation tombstones survive termination and restart; overlapping scope claims
and replay refuse. Unchanged-phase polling does not rewrite the record or renew
its persisted observation floor. A clock behind that floor invalidates control.
CAS and claim removal commit together.

Terminal observation replays the retained record without reading historical
policy or work rows. A live control checks the database clock before that
observation: absolute expiry or clock regression can end this optional gate
even when the historical policy relation is unavailable. The terminal CAS and
scope-claim removal still commit atomically. This does not complete or erase
already admitted tickets. New capture still requires its separate fresh signed
policy and resource checks; removing an expired gate cannot bypass unavailable
ordinary authority storage.

Transactions have a five-second context,
SQL at most five seconds, lock at most 250ms, work_mem 1MB and temp-file limit
zero. Limits are 64KiB per record, 32 scopes, 1MiB per policy body, 4096 retained
operations and 4096 occupied tickets per scope. Exhaustion refuses; no automatic
tombstone/orphan cleanup exists. These bounds are not full disk/WAL budgets or
independently reserved exit capacity.

Native observe/release requests name the immutable original request digest as
well as its operation ID. The original request can be digested after a lost
prepare response. Unsupported actions (including extension), malformed targets,
unprivileged callers and digest mismatches refuse before any transition. A
successful transaction returns its exact target/action, persisted record and
version, whether it changed phase, and its authenticated administrative caller.
An uncertain commit returns no receipt. Replaying release reads the retained
terminal record without another write or renewed deadline, including after a
PostgreSQL restart. These transaction receipts do not attest fleet coverage or
business success and are not standalone cryptographic authorization. They add
no production installer, enrollment or enable path.

### Authenticated observe/release

`POST /v1/admin/platform/producer-capture-deferrals/control` uses the ordinary API
Bearer authentication middleware. It requires both `platform.admin` and explicit
`producer_capture.control`; an ordinary admin key does not inherit this power.
The bootstrap recovery identity retains its existing root permission. The
OpenAPI contract is authoritative. No existing frontend flow consumes this new
operator endpoint.

Send exactly `schema_version`, `operation_id`, `request_digest`, and `action`
as strings in one `application/json` object, at most 2048 bytes. Only `observe`
and `release` are supported. Duplicate keys, alternate field casing, unknown
fields (including caller identities), nulls, trailing JSON and query parameters
refuse before storage access. The authenticated principal, never request data,
supplies the receipt's actor. The endpoint cannot prepare a control, renew its
deadline, install schema or change the local admission hook.

HTTP 200 contains only the complete committed native receipt and is marked
`Cache-Control: private, no-store`. Missing operation is 404; an exact-target
conflict is 409. Missing optional storage, failed transaction or uncertain commit
is 503 without a partial receipt or raw database details. A caller that loses
the response must retain the original ID/digest and may explicitly observe or
release that same retained operation; it must not prepare a replacement, infer
business completion or assume the release happened. There is no automatic retry
or schema installation. Removing the gate still grants no ordinary work or
resource authority. Missing all-replica coverage and full-path budgets continue
to prohibit production enablement.

## Process registration and old work

Each opted-in process must register one fresh boot ID with its exact scopes
before any producer worker starts. Registration uses the same transaction
barrier as tickets and controls. Admission requires an open registration for
that exact process/scope and tickets carry the boot ID. Completion cannot use
another participant's identity. Retained participant/scope rows have a global
4096-row bound; closed boot IDs cannot be reused, even after PostgreSQL restart.
No heartbeat timeout, connection disappearance or leader-lock turnover closes a
participant or deletes work. The legacy session leader lock alone is not a drain
proof: its connection may die while the old function continues on other pooled
connections. This is why older unenrolled work remains a separate rollout gate.

Dynamic discovery may introduce another policy scope after startup. Before the
first capture in that scope, the enrolled process appends it under the same
admission barrier. Only an already registered open boot can extend; the per-boot
32-scope and global 4096-row limits include all retained tombstones. Replaying
an acknowledged or uncertain extension adds no duplicate row, renews no original
enrollment, and never reopens a closed boot. Captures wait for the extension's
commit; an ambiguous reply starts no work. Already registered scopes remain
subject to the usual exact signed-policy and ticket checks.

Exit joins pending extensions as well as admissions and completions without
holding a local mutex across I/O. It reloads the complete durable roster before
the exact close CAS, including scopes whose commit reply was lost. A stale
subset cannot certify exit. Concurrent extension and close serialize; a failed
lookup or close leaves the participant unclosed. This is still an optional
adapter, not a production installer, fleet attestation or enable operation.

The private local Server configuration refuses installation after any producer
reconciliation could have started. Its process object serializes new admission
against closing, joins all returned completion handles, then asks PostgreSQL to
close the exact registration only when no durable tickets remain. Shutdown
joins scope workers before that bounded close. A canceled close neither cancels
work nor writes zero. Completion failure may join locally, but durable occupancy
still refuses close. Closed registrations stay as tombstones. There is no
production caller that installs this private configuration, no automatic stale
cleanup and no inference that the roster covers all runtime replicas.

The hook follows pending safety recovery/timeout, no-new-input quality observation
and interrupted shadow expectation preparation. Deferred new capture still
allows quality refresh. Existing reader/service/heartbeat/pending/exit paths do
not acquire it. A bounded completion context detaches from a canceled work
context; errors surface rather than declaring a ticket complete. The existing
`paused` policy is not used as a substitute.

## Required before production enablement

The explicit availability decision, normal release, verified enrollment of all
replicas and capture entry points (including draining older unenrolled work),
fresh exact authority CAS, bounded installation/exit budgets and finite enable,
drain and exit receipts remain required. None is inferred from these methods
or local tests. Evidence TTLs are unchanged; expiration can cause normal
publication/recovery refusal. The observe/release transport does not supply the
remaining production prepare/enable path.

## Local validation

`go test -race ./internal/platformproducer -count=1` covers restart during drain
and after termination, activation without deadline renewal, exact-deadline exit
even with a stale active record, replay with a retained tombstone, scope isolation,
all policy binding fields, missing drain evidence, clock regression, request
mutation, admission-barrier violations and explicit release. These tests prove
only the local transition logic.

`TestCaptureDeferralPrivatePostgresAtomicDrainCrashAndExpiry` creates its own
synthetic loopback fsync/full-page-write/synchronous-commit cluster. It covers
concurrent clients, missing-schema refusal, exact authority/signature checks,
occupied work across real immediate stop/restart, zero-ticket activation,
unchanged deadlines, scope conflicts, terminal replay after another restart and
absolute expiry. It also makes the policy relation unavailable in the private
fixture: terminal observation and expiry still return committed receipts,
ordinary capture refuses, and a pre-expiry admitted ticket remains occupied
until its exact completion. Set `FUGUE_PRODUCER_DEFERRAL_POSTGRES_BIN` to PostgreSQL 17+ tools;
it accepts no external DSN, existing data directory or production clone.

`TestCaptureDeferralPreservesRealPendingRecoveryAndObservation` exercises the
real file-store producer, dynamic observation queue, gray/full promotion,
failed-candidate timeout recovery and server restarts with a denying new-input
gate. It also checks refusal at the idle capture boundary without changing
publication identities. This proves those local business paths and hook
placement, not production fleet enrollment, complete cold business recovery or
capacity retirement.

`TestCaptureParticipantJoinPreservesOccupiedWorkAndNoCancellation` exercises
concurrent admissions, shutdown refusal while work is outstanding, completion
handle replay and uncertain durable completion. The real PostgreSQL test also
checks unregistered/scope-mismatched admission refusal, occupied-member close
refusal before and after a crash, cross-member completion refusal and closed
boot replay after another real PostgreSQL restart. These tests do not supply
the external rollout roster, old-process termination/exit proof or resource
budget needed for production enablement.

`TestCaptureControlHTTP*` covers the real Bearer authentication middleware,
separate explicit permission, strict request boundaries, authenticated caller
binding and refusal to serialize partial, uncertain or unbound receipts. The
generated route refuses on a real ordinary file store without installing the
optional schema or enabling admission. Successful HTTP responses in these tests
use a synthetic native callback; real PostgreSQL transaction/restart behavior is
tested separately above. This is not a full fleet or production HTTP/PG rehearsal.
