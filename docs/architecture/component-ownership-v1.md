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
override, force-publish, or break-glass behavior. Any release-control Pod must
use this scoped identity and must not mount a platform administrator
credential.

`cmd/fugue-release-control` is the first independently buildable process
boundary. It is excluded from the legacy aggregate build and is disabled by
default. Disabled mode performs no file or network I/O and remains unready so
an accidentally deployed no-op process cannot look operational. When explicitly
enabled, one lane-local loop rereads an absolute-path v1 `ComponentPlanSpec`
and a file-mounted scoped token on every bounded attempt, talks only to the
versioned HTTP adapter, and serializes retries. A bad spec, rotated credential,
or temporary API failure makes only this process unready; the last successful
status remains visible as local last-known-good evidence and the next attempt
can recover without a restart. `/healthz`, `/readyz`, and `/v1/status` expose
credential-free state, and no inbound endpoint accepts a plan or release
command.

`Dockerfile.release-control` copies only the process's declared local package
dependency closure into its build stage and produces a CA-enabled `scratch`
image running as numeric user/group `65532:65532`. The path-scoped main-push/PR
`build release-control image` workflow has only `contents: read`, uses a
release-control-specific concurrency group, builds with `--load`, and probes
the local disabled image. It has no registry login, package write permission,
push, promotion, Helm install/upgrade, or deploy step. Thus Gate A has an
independently tested container artifact boundary but still no published image,
installed Kubernetes object, service account, or production enablement.
Feature-branch pushes are covered by the PR event only, so the same revision is
not built twice; direct `main` updates retain the push check.

`deploy/helm/fugue-release-control` is a separate chart rather than another
template in the legacy `fugue` release. It renders no Kubernetes resources by
default. Explicit enablement renders exactly one digest-pinned, non-root
Deployment with no Service, ServiceAccount, RBAC, or Kubernetes API token. The
desired spec and least-privilege observer token remain externally owned,
separate Secrets mounted read-only with non-root-readable group permissions.
The chart rejects multiple replicas until release-control has an independent
durable leader lease, and its liveness remains separate from observation
readiness so an API outage affects only this lane. CI only lints and renders
the chart; there is still no chart packaging, installation, image publication,
release dispatch, or production mutation.

The `image-plane` now also has a build-only artifact lane. Its image Dockerfile
copies only `cmd/fugue-image-cache` and the currently proven local dependency
`internal/imagecacheusage`, with both multi-architecture base images pinned by
digest. A component-specific main-push/PR workflow compiles and probes that
image under `image-plane-image-${ref}` concurrency with read-only repository
permission and no registry login, package write, push, Helm, kubectl, or
production environment. This is an independent compilation boundary only: the
live image-cache DaemonSet remains owned by the legacy `fugue` Helm release, so
the new lane cannot publish or replace it yet.
Feature-branch pushes are intentionally covered by the PR event only, avoiding
duplicate push and PR builds for the same revision; direct `main` changes retain
the push check.
The image-plane process now also owns a bounded lifecycle boundary: SIGTERM or
SIGINT stops discovery of new background hydrate/report work, drains active HTTP
requests and cache-owned jobs under one 25-second deadline, and exits non-zero
if that handoff cannot complete. Its probe runs with a read-only root and an
external writable store and verifies the same graceful-stop contract. Request-
scoped image-graph checks inherit cancellation instead of creating an
uncancelable process-wide operation.

The first image-plane spec/status contract is now registered as
`image_replication_plan` with `image-plane.fugue.dev/v1` as its versioned
payload namespace. Expected consumers are derived server-side as one
`image-cache:<node>` identity per matching node, never accepted from a caller's
self-reported component or node. Each credential is restricted to the
image-cache component, one node scope, and the image replication artifact kind.
The declared LKG location is lane-local
`/var/lib/fugue/image-cache/replication-plan.json`; an expired control-plane
plan must preserve already cached images while holding new replication. During
this migration phase the safety kernel permits this artifact only in `shadow`;
gray/full publication remains non-bypassable until the independent consumer,
chart, and rollback evidence are installed. The control plane now exposes the
fixed-purpose `POST /v1/node-updater/image-cache/identity` bridge: it accepts
only an authenticated active node updater, derives the node and scope from
server-owned enrollment state, and issues a fifteen-minute credential with exactly
the `image-cache:<node>` identity and `image_replication_plan` capability. The
response is versioned, marked `no-store`, locally self-verified before return,
and contains a renewal boundary; signer absence fails closed with retry guidance.
No caller-provided node, scope, component, or artifact capability is accepted,
and legacy updaters that do not advertise
`image-cache-platform-identity-v1` cannot mint the credential.
The node-platform script's rotator is deliberately default-disabled in this
phase; when enabled it writes only a mode-0640, node-bound response under
`/run/fugue/image-cache`, atomically replaces it, and never persists the bearer
token under `/var/lib`. It rejects symlinked paths, cross-node responses,
unexpected artifact capabilities, malformed timestamps, and short/expired TTLs.
During a control-plane outage it retains a still-valid credential and removes
an expired or corrupt one, so image-cache can stop new control-plane work while
continuing to serve its local LKG. The five-minute node-updater cadence renews
after the credential's five-minute renewal boundary and can tolerate two failed
refresh cycles without extending the fixed fifteen-minute revocation window.
The component-authenticated `GET /v1/image-plane/replication-plan` boundary is
also fixed-purpose: node, scope, artifact kind, and `shadow` channel come only
from the verified identity. It returns a versioned desired/LKG response after
rechecking artifact, release, signature, hash, expiry, and node binding. Its
heartbeat contract carries the exact active artifact-release fence and the
release-set binding plus the last server-accepted sequence/time floor, allowing
a restarted component to construct canonical evidence and resume without
trusting a lost local cursor. The trusted heartbeat transaction
locks that referenced release and rejects caller-selected fencing tokens or a
superseded expected set. A generation sequence may decrease only for a new
expected set behind a strictly newer release fence, which makes an intentional
rollback possible without weakening replay protection.

The image-cache binary now contains a separately gated shadow consumer behind
`FUGUE_IMAGE_CACHE_PLATFORM_PLAN_SHADOW_ENABLED=true`. It reloads the rotated
credential file for every reconciliation, refuses redirects, bounds response
sizes and long polls, and cross-checks the version, node, scope, content hash,
release, expected set, and fence before accepting state. HTTPS is mandatory by
default; plaintext HTTP requires a separate explicit test-only exception. The
complete response is written atomically as mode 0600 to
`FUGUE_IMAGE_CACHE_REPLICATION_PLAN_PATH`, with a previous generation and a
bounded generation archive. Bearer material is rejected from persistent state.
The consumer reports `observed`/`passed` status with canonical evidence and a
server-owned monotonic cursor; it deliberately does not report `applied`, so
this shadow reader cannot become production convergence evidence or mutate the
serving registry. API, credential, persistence, or heartbeat failure degrades
only this loop: the current node-local registry and last observation remain in
place. No legacy chart sets the enable flag in this phase.

`deploy/helm/fugue-image-plane` is the first separate image-plane workload
boundary. The chart renders zero objects by default and is validated in the
image-plane-only build lane; that lane still has read-only repository
permission and cannot publish an image, package a chart, dispatch a release, or
touch a cluster. Explicit rendering requires an immutable image digest, HTTPS
API root, `OnDelete` replacement, and the exact opt-in node selector
`fugue.io/image-plane-shadow=true`. It produces one observation-only DaemonSet
with no Service, ports, host network, service-account token, RBAC, init
container, or broad credential. Health and platform-plan readiness are probed
only over Pod loopback from inside the container.

The workload mounts a dedicated host state directory at
`/var/lib/fugue/image-cache` inside the container but never mounts the legacy
serving cache directory. The host paths must be separate canonical directories;
the chart uses `hostPath.type=Directory`, never `DirectoryOrCreate`, so the
node-platform owner must first create the shadow state directory as uid/gid
65532 and provide the mode-0640 component identity directory. This is the first
explicit image-plane dependency on node-platform, while the observation LKG
remains owned by image-plane. Missing prerequisites make only this shadow Pod
unschedulable or unready and cannot alter the legacy image-cache DaemonSet.
Production installation and enablement remain prohibited while the release
freeze is active.

Node-updater script v37 implements that host-state side of the
`image-plane-host-state@v1` contract without enabling it. When the existing
default-off image-cache platform identity refresh is explicitly enabled, the
node-platform lane first converges exactly
`/var/lib/fugue/image-plane-shadow` to uid/gid 65532 and mode 0750. It walks
every existing ancestor with `O_NOFOLLOW`, creates and opens the final directory
relative to a pinned parent descriptor, rejects legacy-cache or credential-path
overlap, and never recursively changes the observation files inside. A
symlink, missing/unsafe parent, ownership error, or mode error stops before the
credential HTTP request, leaving both the legacy cache and any existing shadow
LKG untouched. The updater version bump is repository evidence only until an
authorized node-platform rollout occurs.

The repository-wide Go CI baseline likewise runs feature branches through the
PR event only and direct `main` updates through the push event. PR runs share a
PR-number concurrency key so obsolete revisions are canceled locally, while
`main` runs use their unique commit SHA and therefore cannot cancel one
another. This removes duplicate validation without creating a new global
component or production-release mutex.

The explicit enablement contract is file-based and has no token environment
variable: `FUGUE_RELEASE_CONTROL_ENABLED=true`,
`FUGUE_RELEASE_CONTROL_SPEC_FILE=/run/fugue/component-plan.json`,
`FUGUE_RELEASE_CONTROL_TOKEN_FILE=/run/secrets/release-control/token`, and
`FUGUE_RELEASE_CONTROL_API_BASE_URL=https://<api-root>`. The adapter appends
the versioned `/v1` paths. The process rejects relative paths, ambiguous
booleans, unbounded timeouts, and oversized responses before starting the
loop.

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
