# Composite release contracts

MD0 adds three dormant, side-effect-free contracts. Build and activation
evidence remain under `internal/releasedomain`. The strict composite contract
and shared domain vocabulary live under `internal/releasecontract`, with
source-compatible aliases in `internal/releasedomain`. No production workflow
reads the strict composite plan and it does not authorize or execute a release.

## Build and activation are different facts

`BuildArtifactPlan` binds a source range, changed-file evidence, produced OCI
digests, and provenance. An artifact in this plan is only a build output.

`ImageActivationPlan` is relative to one attested live-state digest. It lists
only workload/container image references that would actually change. Each
entry binds the exact rendered workload, fixed production adapter, release
domain, artifact digest, and forward/reverse rendered evidence. A built image
that is absent from `activations` must retain its live production reference.

## Composite plan

`CompositeReleasePlan` seals an ordered dependency graph over at least two
release domains. It includes:

- the exact image-activation plan digest;
- base and target domain-version vectors;
- global generation and fencing epoch;
- a fixed adapter and forward/reverse rendered digest for every step;
- step dependencies and activation IDs;
- health evidence, minimum samples, observation window, and rollback budget.

Dependencies may reference only earlier steps. An activation ID may belong to
only one step. Every domain in the version vector must have a step, and the
fixed adapter must match the domain.

## Dormant boundary

MD0 intentionally does not add:

- a producer or report-only consumer;
- `TransactionEnvelope` authorization;
- a durable coordinator or saga state;
- production mutation or rollback execution;
- workflow or runtime behavior.

Existing single-domain plans remain the only executable release path. Later
checkpoints must independently bind these contracts to exact production
evidence before enabling any composite mutation.

The neutral contract package is intentionally independent of the Planner,
store, controller, and runtime packages. A dormant durable coordinator can
therefore validate and persist the exact `CompositeReleasePlan` without making
the Planner package a production-binary dependency. The aliases preserve the
existing evidence producer API and exact JSON/digest behavior; this package
boundary does not change authorization.

## MD1 report-only production evidence

MD1 adds one read-only producer to the formal release prepare/apply boundary.
Prepare derives `BuildArtifactPlan` from the exact changed-file digest and
verified build provenance. It derives `ImageActivationPlan` independently from
the canonical live and target manifests, matching changed container image
digests to fixed object ownership and adapters. A shared image is assigned per
rendered workload, not by a static image-to-domain shortcut; extra build
outputs remain build-only.

Three canonical activation files are uploaded as a separate artifact: the build plan, the
resolved activation plan, and `ImageActivationEvidence`. Apply receives the
pinned artifact identity and rederives them from the same sealed
inputs, and requires byte equality. Missing ownership and other unresolved
rendered image changes remain explicit gaps with `complete=false`; they are
never relabeled as built-only or admitted to the resolved plan. Malformed or
unrepresentable evidence still fails production closed.

This report is not an authorization input. It is not passed to plan activation,
transaction envelopes, adapters, Helm dispatch, rollback, or runtime baseline
advancement. Multi-domain decomposition remains a later checkpoint.

## MD2A dormant decomposition evidence

`CompositeDecompositionEvidence` is an additive, unused report contract for
the case where production evidence is not yet sufficient to construct a
strict `CompositeReleasePlan`. It binds the exact activation plan and
activation-evidence digests, groups resolved activation IDs by their actual
domain and fixed adapter, seals aggregate forward/reverse rendered evidence,
and represents the groups as a canonical serial dependency chain.

The report keeps unresolved activation IDs explicit. `complete` is derived and
is false while an activation remains unresolved or fewer than two domains are
present. The contract cannot authorize a transaction and has no producer,
workflow artifact, envelope, adapter dispatch, coordinator, or runtime
consumer in this checkpoint.

## MD2 report-only decomposition producer

MD2 derives `CompositeDecompositionEvidence` from the exact verified
`ImageActivationPlan` and `ImageActivationEvidence`. Resolved activations are
grouped by their actual fixed domain and adapter, ordered by the canonical
domain order, and sealed as a strictly serial dependency chain. Each step
contains deterministic aggregate forward and reverse rendered-evidence
digests. Every unresolved activation gap ID is preserved, so incomplete input
remains incomplete instead of being promoted into a composite plan.

The decomposition is the fourth file in the build-activation evidence
artifact. Prepare uploads it; apply independently rederives the complete
four-file directory and requires exact inventory, permissions, and byte
equality. It remains report-only and is not passed to authorization, adapter
dispatch, a transaction envelope, runtime state, or rollback logic.

## MD3 dormant durable coordinator

MD3 adds an unwired durable journal for a future Fugue-owned composite saga.
The journal imports only the neutral `internal/releasecontract` package and
does not reconnect `internal/releasedomain` or the Planner to runtime binaries.
Each record embeds and re-verifies the complete `CompositeReleasePlan` before
it can be stored, so the ordered steps, fixed adapters, forward/reverse
rendered evidence, observation policy, rollback budget, generation, and
fencing epoch are durable before a future first write.

The state machine permits only one serial step at a time:
`prepared -> applying -> observing`; a successful observation either starts
the next step or commits the whole record. Failure changes direction at the
current step, and recovery can only advance through the current and completed
steps in reverse order. Missing reverse proof can only freeze the record.
Every update requires the exact plan digest, fencing epoch, and record
revision, with the same CAS enforced by both the file-backed store and
PostgreSQL.

This checkpoint does not add a coordinator worker, scheduler, API, workflow,
transaction-envelope authorization, adapter call, production mutation, or
rollback execution. The journal and additive table remain dormant until later
authorization and activation checkpoints.

## MD4 enforced no-op authorization

MD4 adds the strict v2 `CompositeReleaseTransactionEnvelope` boundary. The
envelope embeds the complete verified plan and independently binds its digest,
image-activation digest, generation, fencing epoch, exact coordinator record
ID and digest, and expected record revision. Its only valid mode is `noop`.

The coordinator derives every trusted binding from one exact valid `prepared`
record, strictly decodes the envelope, and returns an opaque authorization
whose seal can be reverified only against that same record. The authorization
does not expose the embedded plan, a domain step, adapter, transition, or an
execution method. It therefore proves authorization agreement without making
a production write possible.

This checkpoint does not connect the authorization to the store, state
transitions, an API, worker, workflow, adapter, canary, or rollback execution.
Existing single-domain transaction authorization remains unchanged.

## MD5 controlled no-op recovery drill

MD5 adds one deterministic in-memory canary for an exact prepared two-domain
record and its opaque MD4 no-op authorization. The first ordered domain
completes a no-op observation; the second begins its no-op observation and
then receives a controlled failure. Fugue's coordinator state machine performs
the recovery itself, reversing the current second step and then the completed
first step. The final state must be `reverted`.

Every apply, observation, induced-failure, and reverse event is digest-bound to
the exact record, plan, authorization, action, and step. The result is sealed,
requires `productionWrite=false`, and can be reverified against both the
initial prepared record and final reverted record.

The drill calls no adapter and writes no store, API, workflow, Kubernetes
object, node configuration, DNS state, or network rule. Production activation
and a real mutation/recovery canary remain separate later checkpoints.

## MD6p6 dormant strict-plan materializer

MD6p6 adds the fail-closed bridge from one complete, verified
`CompositeDecompositionEvidence` report to the strict
`CompositeReleasePlan`. Domains, fixed adapters, serial dependencies,
activation IDs, forward/reverse rendered digests, and the two domain-version
vectors are copied or derived only from the decomposition. Callers supply the
coordinator-controlled generation and fencing epoch plus one health/observation
requirement for each already-derived domain; an observation cannot introduce
another domain or adapter.

Incomplete or tampered decomposition, missing/extra/duplicate observations,
and invalid generation, fencing, digest, sample, window, or rollback budget
values all fail closed. The materializer is dormant: it does not allocate a
generation, create a durable record, authorize an envelope, call an adapter,
advance the coordinator, or write production state. Those activation
boundaries remain later checkpoints.

## MD6p7 durable enforced no-op worker

MD6p7 adds a dormant worker that accepts only one exact prepared durable record
and its opaque MD4 no-op authorization. It uses the existing store CAS boundary
to advance the coordinator serially through no-op apply and observation. A
controlled observation failure makes the same worker persist `reverting` and
reverse the current and completed steps in strict reverse order. Every event is
bound to the exact record, plan, authorization, action, and step, and the sealed
result requires `productionWrite=false`.

The worker receives no adapter and is not connected to a scheduler, API,
workflow, or release lane. It cannot materialize a plan, allocate a generation,
change a workload, or advance a runtime baseline. A real two-domain canary,
adapter mutation, automatic health failure detection, freeze/watchdog behavior,
and composite activation remain separate checkpoints.

## MD6p8 admin-only prepare boundary

MD6p8 exposes one platform-admin-only API boundary that accepts an externally
digest-bound, strictly decoded `CompositeReleasePlan` and persists it as one
exact `prepared` coordinator record. Unknown, duplicate, non-canonical, or
digest-mismatched plans fail before creation; a repeated plan digest conflicts
instead of creating another record.

The response is the inert durable record at revision 1. This boundary does not
construct or accept a transaction envelope, return a no-op authorization,
advance the worker, call an adapter, observe health, revert a step, or change a
runtime baseline. Preparing and activating the controlled two-domain canary
remain separate transactions.

## MD6 controlled two-domain no-op execution

The first MD6 execution checkpoint connects one exact `prepared` record to the
existing durable no-op worker through a platform-admin-only API. The caller
must provide the strict MD4 envelope bound to the record digest, revision,
plan digest, generation, and fencing epoch. This controlled boundary accepts
exactly two already ordered plan steps and runs only the success path.

The worker persists serial no-op apply and observation transitions and returns
sealed evidence with `productionWrite=false` and final state `committed`.
It receives no adapter, cannot inject a failure, cannot update a workload or
runtime baseline, and rejects stale or replayed envelopes before another run.
Real adapter apply/observe, controlled failure and reverse recovery, freeze,
and guarded composite activation remain separate later checkpoints.
