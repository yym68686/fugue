# Bounded producer capture deferral

Status: local implementation and isolated validation only. The local process
registration/join adapter exists; there is no production installation, complete
rollout enrollment proof, control endpoint, configuration switch or enabled
deferral. The normal server leaves the admission hook nil.

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
CAS and claim removal commit together. Transactions have a five-second context,
SQL at most five seconds, lock at most 250ms, work_mem 1MB and temp-file limit
zero. Limits are 64KiB per record, 32 scopes, 1MiB per policy body, 4096 retained
operations and 4096 occupied tickets per scope. Exhaustion refuses; no automatic
tombstone/orphan cleanup exists. These bounds are not full disk/WAL budgets or
independently reserved exit capacity.

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
publication/recovery refusal. No production enable path is supplied here.

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
absolute expiry. Set `FUGUE_PRODUCER_DEFERRAL_POSTGRES_BIN` to PostgreSQL 17+ tools;
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
