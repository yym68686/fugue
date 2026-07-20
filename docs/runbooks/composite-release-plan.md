# Composite release contracts

MD0 adds three dormant, side-effect-free contracts under
`internal/releasedomain`. No production workflow reads them and they do not
authorize or execute a release.

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
