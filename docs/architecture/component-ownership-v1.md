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
- `edge-control`: default-off, non-authoritative boundary for the future
  group-scoped inventory, epoch, fencing, bundle and recovery authority;
- `cli`: operator client artifacts.

The initial `edge-control` boundary is deliberately not a serving migration.
It has its own process, immutable image, chart, resource budget and release
lane, but `authority=none`: it has no database, Kubernetes token, bundle
signer, Core API client, or customer data-plane dependency. The existing
`edge-dns` and Core API paths remain authoritative until later shadow and
per-group cutover atoms produce independent production evidence. Building or
publishing the boundary image is not evidence that Edge authority has moved.

The boundary-shadow atom installs that exact image as a production shadow
through `deploy-edge-control-shadow.yml`. The release remains `authority=none`,
has no credentials or outbound network access, and is not on a customer request
path. Its independent Helm history and release receipt prove only that the
process/release failure-domain boundary is deployable; they are not evidence of
inventory, epoch, bundle, signing, or recovery authority migration.

The initial default-off image atom had no runtime behavior. The boundary-shadow
atom adds only the separate `edge-control` Helm release and does not change the
legacy `fugue` Helm release. `internal/componentmanifest` strictly validates
the document and rejects ambiguous source, artifact, state, dependency, or
shared-resource ownership. Future steps will wire that validator into release
planning, then move one lane at a time from `transitional-shared` to
`independent` after contract, LKG, rollback, and production health evidence are
present. The required evidence is defined in
[`microservices-migration-acceptance-v1.md`](microservices-migration-acceptance-v1.md).

`componentmanifest.PlanChanges` is the shadow planner for that future release
integration. It emits a digest-bound plan with direct impacted components,
downstream validation-only consumers, and every affected shared-resource edge.
It returns `legacy-shared` for shared source paths and `shadow-only` for a
component that is still transitional; neither result authorizes a production
mutation. Only a later plan with `independent` (or an explicitly coordinated)
mode may be handed to a release coordinator.

The input-boundary CLI is deliberately read-only. For example:

```console
go run ./cmd/fugue-component-plan \
  --coordination \
  --path cmd/fugue-image-cache/main.go
```

To produce the exact body for the existing shadow artifact endpoint, provide
trusted revision evidence explicitly:

```console
go run ./cmd/fugue-component-plan \
  --artifact-request \
  --base-commit <40-hex-base> \
  --target-commit <40-hex-target> \
  --path cmd/fugue-image-cache/main.go
```

This only prints JSON. It does not submit the request or acquire a release
fence.

`internal/releasecontrol` is the first idempotent spec/status control-loop
boundary. It accepts only a validated `component_release_plan` artifact whose
ID, generation, and content hash match the supplied spec. Reconciliation uses
the envelope idempotency key and the existing atomic lane fence to persist one
shadow release status; replay returns the same release ID, fencing token, lane
version, and status digest. It rejects any store result that claims a gray/full
or bypassed release.

The first cross-component adapter is `component-plan-api.fugue.dev/v1`.
`internal/releasecontrol.HTTPComponentPlanStore` reads the authenticated
principal and immutable artifact, then writes the shadow observation through
the existing `/v1` HTTP API. It has no PostgreSQL or Kubernetes capability.
Every request is context-cancellable and bounded, redirects are disabled,
credentials are redacted from transport/remote errors, and the adapter rejects
gray/full, canary, override, force-publish, break-glass, reason drift, and
idempotency drift before opening a network connection. Required response
semantics are strict while unknown additive response fields remain compatible
within v1.

The API now supports a dedicated least-privilege observer identity. A
release-control credential must explicitly hold only `artifact.read`,
`artifact.release_shadow`, and `component_plan.observe`; the server binds that
exception to one validated `component_release_plan`, its envelope-derived
idempotency key, the fixed observation reason, and the shadow channel. The
credential cannot release other artifact kinds or request gray/full, canary,
override, force-publish, or break-glass behavior. A future release-control Pod
must use this scoped identity and must not mount a platform administrator
credential.

The caller must obtain the paths from trusted revision evidence; the command
does not run `git diff`, read live state, or dispatch a workflow. Coordination
output includes the globally sorted lane/resource lock order and recovery lanes,
but is hard-coded as observation-only and cannot authorize production mutation.

Validated observations are persisted as `component_release_plan` entries in the
existing platform artifact ledger. Their scope key is derived from the exact
base and target Git commits, so unrelated comparisons have independent shadow
release lanes. The envelope, manifest, change plan, coordination plan, scope,
and generation are verified together. This artifact kind is hard-limited to the
`shadow` channel by the platform safety kernel; neither a soft override nor
kernel break-glass can promote it to gray or full while the migration remains at
Gate A.
