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
