# Dormant image-activation evidence contract

`ImageActivationEvidence` is an additive, side-effect-free v2 contract. It
exists so a report-only producer can remain truthful when an actual rendered
workload image change cannot yet enter a strict `ImageActivationPlan`.

The document binds the exact build-plan and resolved activation-plan digests.
It keeps three facts distinct:

- `builtOnlyArtifacts` were built but are absent from the rendered image diff;
- the resolved activation plan contains only fully bound workload, domain,
  adapter, artifact, and forward/reverse evidence;
- `unresolved` records every actual rendered image change that is missing an
  immutable target, build provenance, live object, unique ownership, or fixed
  adapter.

Every gap has a closed reason set and seals the workload/container, observed
image references, matching build identities, ownership candidates, and
available forward/reverse rendered digests. `complete` is derived from the gap
inventory and cannot be asserted while any gap remains.

This checkpoint does not add a producer, workflow artifact, consumer,
authorization rule, transaction envelope, adapter dispatch, or production
mutation. In particular, the contract does not assign telemetry-agent to a
domain; it only makes a missing assignment representable without pretending
that the image is build-only.
