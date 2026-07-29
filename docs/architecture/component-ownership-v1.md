# Fugue component ownership manifest

`component-ownership-v1.yaml` is the first machine-readable boundary contract
for the microservice migration. It is intentionally a **foundation** document:
all components currently use `transitional-shared` ownership because the live
Fugue chart and PostgreSQL schema have not yet been split.

The manifest separates four concepts that the current release path conflates:

1. component source and artifact consumers;
2. data/spec/status ownership;
3. release lanes and their coordinator;
4. shared resources that require an explicit conflict policy.

The first production migration must not claim a component is independent until
its state owner, image/Helm artifact, last-known-good record, and rollback
boundary are independently verifiable. A shared resource remains safe only when
its owner and conflict mode are explicit.

Target lanes are:

- `release-control`: durable release intents, fencing, evidence, and recovery;
- `control-plane`: API, controller, and telemetry;
- `image-plane`: OCI graph, inventory, replication, and cache agents;
- `backup`: R2 lifecycle, backup runs, usage reconciliation, and restore;
- `node-platform`: node updater and host-level safety;
- `edge-dns`: edge, front, route, and authoritative DNS;
- `cli`: operator client artifacts.

This step has no runtime behavior and does not change the legacy `fugue` Helm
release. `internal/componentmanifest` strictly validates the document and
rejects ambiguous source, artifact, state, dependency, or shared-resource
ownership. Future steps will wire that validator into release planning, then
move one lane at a time from `transitional-shared` to `independent` after
contract, LKG, rollback, and production health evidence are present. The
required evidence is defined in
[`microservices-migration-acceptance-v1.md`](microservices-migration-acceptance-v1.md).

`componentmanifest.PlanChanges` is the shadow planner for that future release
integration. It emits a digest-bound plan with direct impacted components,
downstream validation-only consumers, and every affected shared-resource edge.
It returns `legacy-shared` for shared source paths and `shadow-only` for a
component that is still transitional; neither result authorizes a production
mutation. Only a later plan with `independent` (or an explicitly coordinated)
mode may be handed to a release coordinator.
