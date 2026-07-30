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

The shadow observer now has a second, narrower compilation boundary rather
than merely another runtime argument. `Dockerfile.image-plane-agent` copies
only five explicitly named agent/contract/lifecycle source files and builds
with the `imageplaneagent` tag, which excludes the legacy `main.go`. Its Linux
amd64/arm64 dependency closure contains no Fugue internal package, OCI registry
library, Kubernetes client, database driver, or PostgreSQL package. The final
non-root image declares no port and starts
`/usr/local/bin/fugue-image-plane-agent` directly. A distinct path-scoped
`image-plane-agent-${ref}` workflow builds and probes this artifact and owns
the default-disabled chart checks; the legacy image-cache workflow no longer
owns that chart. Both workflows remain read-only/build-only and can run
independently from release-control and every other subsystem lane.
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
API root, `OnDelete` replacement, an explicit lowercase `cell.id`, and the
two-part node selector `fugue.io/image-plane-shadow=true` plus
`fugue.io/image-plane-cell=<cell.id>`. The cell ID is also part of the immutable
DaemonSet selector and Pod ownership labels, so one Helm release cannot widen
from its selected canary/recovery cell without creating a different workload.
It produces one observation-only DaemonSet
with no Service, ports, host network, service-account token, RBAC, init
container, or broad credential. Its explicit `platform-plan-shadow` process
mode remains as a compatibility path, but the chart rejects the legacy
registry-capable repository and requires the dedicated
`fugue-image-plane-agent` artifact. That binary cannot initialize the legacy
OCI registry, store, image-location API, management handlers, or registry
background jobs because those sources and dependencies are absent from its
compilation. It exposes only credential-free health and observation readiness.
Both are probed only over Pod loopback from inside the container.

`image-plane-release.fugue.dev/v1` is the non-executable handoff from artifact
validation to a future live preflight. The read-only
`fugue-image-plane-release-plan` command consumes an exact component-plan
envelope, the versioned `ComponentPlanStatus` observation, a source commit,
digest-pinned agent, deterministic chart digest, explicit cell/release
identity, and one Helm-rendered manifest. It independently verifies the v1
status JSON and digest instead of importing release-control's implementation.
The render must contain exactly one namespace-bound, `OnDelete` DaemonSet with
the expected immutable cell selector, loopback probes, non-root security,
fixed isolated host paths, no service account, no ports, and no broad
credential. The resulting candidate binds all inputs, the observed fence, a
cell-local lock key, rollback policy, and idempotency key under one digest.
`executionAllowed` and `productionMutationAllowed` are permanently false; the
active production freeze is an explicit blocker. The command has no concrete
store, release-control, Kubernetes client, registry, Helm execution, or process
execution dependency and only reads bounded, non-symlink inputs. Its
deterministic chart digest covers every relative file path and byte in the
chart directory.
Candidate validation has its own read-only
`image-plane-candidate-${ref}` workflow rather than running inside the agent
image job. It builds no container and has no package write, registry login,
artifact upload, workflow dispatch, production environment, self-hosted
runner, Helm mutation, or Kubernetes command. The agent, candidate, legacy
cache, and release-control checks therefore have independent cancellation and
queue boundaries even when a shared chart edit correctly asks more than one
consumer to validate compatibility.

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

The `backup-storage` extraction starts with the pure
`backup-control.fugue.dev/v1` spec/status boundary. A shadow
`BackupRunSpec` derives its cell and artifact kind from one canonical target,
binds the request idempotency key and opaque backend-configuration generation,
and bounds attempts, lease duration, and operation time. Its corresponding
status is an expiring observation of the legacy run, bound to the exact spec
digest and cell. A successful observation must carry a current-run LKG artifact
whose kind and backend generation match that spec; an in-progress observation
may retain an older same-kind LKG so a backend rotation does not erase known
recoverable state. Artifact references contain content and manifest digests but
no credential, endpoint, bucket, object key, DSN, or physical storage address.
Only structured error codes and error digests cross this boundary.

`internal/backupcontrol` has no dependency on the legacy API, store, model,
database, Kubernetes, network, or object-storage implementation. A dependency-
closure test enforces that property before a standalone observer or adapter is
introduced. Bounded v1 decoders reject unknown fields, trailing JSON, invalid
digests, and oversized documents at that future adapter boundary. The contract
is permanently observation-only in this atom: it
cannot claim a production write, acquire a backup lease, execute a backup,
delete an object, restore data, or alter the existing scheduler. The live
backup subsystem therefore remains `transitional-shared` and continues to run
unchanged under the legacy release while its independent data and failure
boundary are built behind shadow contracts.

`internal/backupobserver` is the next isolated control-loop boundary. It reads
one exact spec and a separately mounted credential on every bounded attempt,
then performs only a fixed-purpose HTTPS `GET` for that run and spec digest.
The adapter refuses redirects, URL credentials, noncanonical URL paths,
plaintext transport outside an explicit test-only option, unbounded responses,
unsafe content/cache headers, and any status that fails the strict v1 digest
and spec binding. A deployment must preconfigure one exact backup cell; a spec
for another target cell is rejected before the credential is opened or a
network request is made.

The observer is still only a library in this atom and is not started by any
legacy binary, chart, or workflow. Disabled mode performs no file or remote I/O.
When driven by a later standalone process, an invalid, expired, future-dated,
unauthorized, or unavailable observation makes only that cell unready while
retaining the last validated observation in memory. Local operational
endpoints expose no input or command surface and never include credentials or
remote error bodies. Its production dependency closure contains only
`backupcontrol`, `backupobserver`, and the Go standard library; in particular it
has no API/store/model, database, Kubernetes, object-storage, or process-
execution dependency. The server-side least-privilege observation endpoint,
standalone artifact, and default-off chart are added by later atoms, while
production enablement remains separately gated.

`cmd/fugue-backup-observer` turns that core into a separately compiled process
without importing any additional Fugue package. Configuration is
default-disabled and exact: enabled mode requires one cell key, absolute spec
and token files, HTTPS API root, and bounded reconcile/request/attempt/shutdown
values. Its local health server accepts only an explicit loopback IP, exposes
no command endpoint, and includes an internal loopback-only probe mode so a
scratch container never needs a shell or HTTP utility. SIGTERM cancels the
cell loop and completes a bounded graceful HTTP shutdown.

`Dockerfile.backup-observer` copies only the command, `backupcontrol`, and
`backupobserver` source closures into a digest-pinned Go builder. The resulting
CA-enabled `scratch` image runs as `65532:65532`, declares no port, and is
probed with a read-only root to prove disabled liveness, unready observation
state, no published port, no credential-shaped logs, and clean SIGTERM exit.
The path-scoped `backup-observer-image-${ref}` workflow has only
`contents: read`, builds with `--load`, and owns no registry login, artifact
upload, package write, dispatch, Helm install/upgrade/package/push, Kubernetes,
production environment, or deploy step. It is therefore an independent
compilation and CI cancellation boundary, not a published or installed backup
service. The legacy API remains the only running backup owner.

The observer input reader supports credential/spec rotation without weakening
its path boundary. It accepts either a stable regular file or exactly the
Kubernetes atomic-writer topology `<name> -> ..data/<name>` with a relative
in-volume generation link. It resolves the volume root, rejects arbitrary or
escaping links, validates credential mode on the resolved file, and rechecks
both generation and inode after the bounded read. A rotation racing an attempt
therefore fails that attempt and is reread cleanly on the next loop; it cannot
mix a spec or token across generations. This allows ordinary projected
Secret/ConfigMap updates and avoids `subPath`, whose file would otherwise stay
pinned to the old generation.

The observer core also supports optional durable cell-local recovery through
`FUGUE_BACKUP_OBSERVER_LKG_FILE`. The state path must be canonical, absolute,
and outside both projected-input trees. Every successful observation is bound
to its exact validated spec in a strict `backup-observer.fugue.dev/v1`
envelope, self-digested, bounded, and atomically published as a private `0600`
regular file only after fsync. A valid current generation is preserved as one
previous generation before replacement; startup can recover that previous
generation when current content is corrupt, but reports the fallback and
remains unready until a fresh observation succeeds. Unknown fields, semantic
or digest drift, cross-cell state, broad modes, world-writable parents,
symlinks, and topology races fail closed. A remote success is not reported
Ready when durable publication fails; v2 status reports `persist-failed`
until a later atomic publication succeeds. The file contains no token, physical
backend configuration, object location, or raw remote error. The recovery core
landed before its workload wiring; the current chart/candidate revision below
now binds an external cell-local volume, while default-off rendering and the
production freeze still keep it unreachable.

Because local status now reports the explicit `lkgState` generation source,
its wire version advances to `backup-observer.fugue.dev/v2`; the sealed backup
candidate references that exact version. The independent on-disk LKG envelope
remains v1. No deployed v1 status consumer exists, so the version transition
does not require a compatibility overlap or production rollout.

The observer artifact now also has an independent Helm lane at
`deploy/helm/fugue-backup-observer`. It is default-off and renders exactly one
cell-scoped Deployment only when explicitly enabled with a dedicated immutable
image digest, canonical cell key, externally owned ConfigMap/Secret, and an
externally owned cell-dedicated LKG PVC. The PVC name must be exactly
`fugue-backup-observer-<cell-id>-lkg`; the chart neither creates nor deletes
it, so a workload rollback or uninstall cannot erase recovery state or select
another cell's claim. The observer mounts only that directory read-write while
spec and token remain separate read-only projections. Chart/app version 0.2.0
and exact Deployment/Pod annotations make the breaking storage contract and
claim identity visible to later live preflight. The
chart creates no Service, ServiceAccount, RBAC, hostPath, or container port;
the binary remains loopback-only and probes run through its fixed executable.
The ConfigMap and Secret are mounted as whole read-only volumes (never
`subPath`) so the atomic-writer rotation contract above remains effective.
The lane is build/lint/render-only; it does not publish an image, install a
release, or authorize production mutation.

`internal/backuprelease` seals one enabled cell render into the versioned
`backup-release.fugue.dev/v2` candidate contract. v2 is a deliberate breaking
revision because the new cell-local claim is a required digest-bound input;
no v1 candidate has been deployed. The candidate binds the
source commit, dedicated image digest, future deterministic chart digest,
canonical cell and workload names, external ConfigMap/Secret/PVC references,
bounded runtime values, backup spec/status contract versions, and the exact
observed release-control fence. It independently revalidates that the source
plan impacts only `backup-storage`, retains the transitional `lane/backup`,
legacy-release, legacy Helm resource, and control-plane Postgres coordination
scopes, and requires cell-local LKG rollback. Exact rendered Deployment,
VolumeSource union, probe, scheduling, resource, and security structures are
verified before producing a digest. The resulting record permanently carries
`observationOnly=true`, `executionAllowed=false`, and
`productionMutationAllowed=false`; recalculating its digest cannot turn it
into a deploy authorization. The candidate also keeps an explicit blocker
until a later live preflight proves the claim is Bound, single-writer,
cell-owned, and unused by another workload.

`cmd/fugue-backup-release-plan` is the candidate lane's only compiler-facing
entrypoint. It accepts three canonical local inputs (request JSON, rendered
manifest, and chart directory), rejects symlinks, hardlink aliases, path
containment, duplicate/unknown/trailing JSON, unstable files, and unbounded
chart topology, then checks the chart digest before invoking the pure
candidate builder. The path-scoped
`validate-backup-release-candidate.yml` workflow runs this contract, Helm lint,
deterministic digest, and disabled-chart render with `contents: read`; it has
no registry, artifact, Kubernetes, Helm mutation, dispatch, or production
capability.

`internal/backupadapter` is the pure compatibility seam between legacy backup
records and the v1 backup-control contract. The future API bridge and spec
materializer must both use its one mapping for stable target scope, cell key,
backend generation, retry/lease bounds, observed state, and artifact LKG. It
accepts no store handle, credential, object location, network client, or
execution adapter. A successful legacy run becomes observable only when
exactly one active artifact matches the run, backend, target, kind, and both
content digests; ambiguous or drifting records fail closed. Legacy worker and
error detail are canonicalized or irreversibly digested before crossing the
boundary, while volatile app placement and backend health probes cannot churn
the ownership cell or backend generation.

`internal/store/backup_observation.go` is the legacy data-owner read boundary
for that seam. Its one snapshot query/locked read computes the final backend
generation inside the data owner and returns only backend/tenant identity plus
that irreversible digest. It does not expose bucket/endpoint configuration,
access-key identifiers, ciphertext, encryption key IDs, secret record IDs,
health-test timestamps, or display metadata, and it cannot mutate the store.
`validate-backup-observation-store.yml` runs JSON-store and SQL-mock
coverage, repeated race tests, and vet under a separate `contents: read`
concurrency lane with no publication or deployment capability.

`internal/backupidentity` establishes a separate
`backup-observer-identity@v1` authority domain before any observation route is
made reachable. A `fugue_bo_v1` token is short-lived, rotation/revocation aware,
and bound to one exact run, tenant, backup cell, spec digest, and fixed read
permission. Its signing material is domain-separated and is never accepted as
a tenant, workload, runtime, node-updater, or platform-component credential.
The authentication middleware is permanently GET-only and applies private
no-store headers even to rejected requests. `fugue-api` reads only dedicated
`FUGUE_BACKUP_OBSERVER_IDENTITY_*` configuration; an absent, weak, partial, or
revoked keyring disables issuance and verification. The legacy Helm chart
still neither creates nor mounts these keys, so the new private route remains
unreachable in production until a later identity-materializer and shadow
rollout atom.
`validate-backup-observer-identity.yml` validates the package, middleware,
OpenAPI auth generator, environment wiring, rotation, revocation, expiry, race,
and dependency boundaries with read-only permissions and no publish/deploy
step.

`internal/backupmaterializer` now defines the private
`backup-materializer.fugue.dev/v1/BackupObserverInputBundle` seam between the
legacy data/signer owner and a future fixed-purpose materializer. The pure
builder accepts one already validated `BackupRunSpec`, mints a dedicated
15-minute observer identity, immediately verifies that identity with the same
rotation-aware keyring, and seals the exact spec/token pair under one digest.
Redundant cell, run, spec-digest, credential, token-id, issued, renew-after,
and expiry bindings are all strict; unknown/trailing/oversized JSON, signature
or digest drift, expiry, revocation, non-canonical time, and either production
mutation flag fail closed. Ordinary `String` and `GoString` formatting always
redact the token, while the private JSON handoff intentionally contains it.
The envelope carries no endpoint, bucket, object key, backend credential,
database handle, or Kubernetes capability.

This atom adds only the contract and its independent read-only
`backup-input-bundle-${ref}` validation lane. There is still no bundle HTTP
route, materializer process, Secret writer, service account, RBAC grant, image,
or workload. The current default-off chart continues to consume its existing
external ConfigMap/Secret until a later reviewed atom moves both keys into one
bundle-derived Secret projection; production behavior is therefore unchanged.

`internal/backupmaterializeridentity` defines the corresponding caller side as
`backup-materializer-identity@v1` without creating another Fugue bearer-key
domain. A future materializer must present a short-lived Kubernetes projected
ServiceAccount JWT for the one fixed audience
`fugue-backup-materializer.fugue.dev`. The pure policy asks a pluggable reviewer
for exactly that audience and accepts only the exact
`fugue-system/fugue-backup-materializer-<cell-id>` username, the three normal
ServiceAccount groups, canonical ServiceAccount UID, credential-document JTI,
and bound Pod name/UID extras. Missing Pod binding rejects legacy
Secret-backed ServiceAccount tokens; another cell's account or another
workload's Pod cannot cross the boundary.
Malformed/oversized/non-JWT credentials are rejected before external review,
while reviewer outages remain distinct from invalid credentials for bounded
fail-closed retry handling. The resulting request-context claims contain no
bearer token and round-trip all six canonical backup cell kinds within the DNS
label limit.

The identity policy is standard-library-only. Its reviewed-cell entry point
derives a canonical cell only from the exact ServiceAccount username and then
reapplies the complete audience, groups, UID, JTI, and Pod-binding policy; it
does not trust the derived string by itself.

`internal/backupmaterializeridentity/httpauth` exposes that policy as the
`backup-materializer-http-auth@v1` claims boundary. It rejects every non-GET
method before parsing or review, accepts one exact Bearer header, returns the
same private `401` for malformed and foreign identities, and converts reviewer
unavailability into a detail-free retryable `503`. On success it clones the
request, removes `Authorization` and `Proxy-Authorization`, and places only
validated cell claims in context. A response-writer guard restores
`private, no-store` and `Vary: Authorization` at every explicit write and after
an implicit success, so a downstream handler cannot accidentally make the
private bundle cacheable. The package has no Kubernetes, filesystem, store,
signer, server configuration, or mutation dependency.

The identity policy and HTTP boundary share the recursive read-only
`backup-materializer-identity-${ref}` validation lane.

`internal/backupmaterializerreview` now supplies the separately owned
`backup-materializer-token-review@v1` network adapter. It performs exactly one
`POST` to the Kubernetes `authentication.k8s.io/v1/tokenreviews` endpoint with
the fixed materializer audience. The API caller credential comes from a
pluggable source on every request so projected-token rotation is observed; it
must be a different credential from the workload token being reviewed. The
transport requires HTTPS, refuses redirects, uses a short timeout and bounded
response, and accepts only a strict `201 application/json` TokenReview that
echoes the exact request spec. It translates the response into the minimal
token-free policy result, reduces remote status errors to a fixed marker,
rejects credential echoes in translated fields, and never returns response
bodies or either bearer credential in errors.

This adapter depends on Kubernetes API structs only, not client-go, informers,
filesystem, datastore, signer, or mutation capability. Its independent
`backup-materializer-review-${ref}` lane is read-only and cannot publish or
deploy.

`internal/backupmaterializerreview/projected` adds the capability-separated
in-cluster bootstrap. It accepts only safe regular files or Kubernetes' exact
atomic-writer topology (`<name> -> ..data/<name>`), with canonical in-volume
generation links, bounded reads, restricted token modes, strict CA-only PEM,
and file/root identity checks across every read. It rereads the caller token
for each review, builds a fresh CA pool and TLS connection for each request,
disables proxy and connection reuse, and therefore observes both kubelet token
rotation and projected CA rotation without retaining an old credential or
trust bundle. A malformed or racing generation makes only that review
unavailable; a later valid generation recovers without process restart.

The same read-only lane recursively validates this bootstrap and its dependency
closure. The HTTP middleware is not attached to a server or route, and there is
still no environment wiring, RBAC, ServiceAccount, projected-token volume,
materializer process, or chart wiring; those remain separately reviewed
default-off atoms, so production behavior is unchanged.

`GET /v1/backup-control/runs/{run}/observation` is the first private bridge
over that identity boundary. It accepts only the dedicated observer bearer
credential and one canonically encoded `spec_digest` query value, then binds
the token, path, tenant, cell, and digest to the current legacy run and current
redacted backend generation. It reads at most two active artifact records to
detect ambiguity and delegates all spec/status construction to the pure
adapter. The handler acquires no audit writer, lease, worker, or object-storage
client; its success schema exposes no physical backend configuration, secret,
object location, or raw legacy error. Backup backend credential or
configuration rotation changes the spec digest so an old token/query pair
fails closed instead of observing the new generation. Tests prove
byte-for-byte store immutability, exercise the real
observer HTTP client against the route, and lock the strict OpenAPI schema
against additional fields. `validate-backup-observation-api.yml` validates
this boundary in its own read-only concurrency lane and has no build,
publication, dispatch, Helm, or Kubernetes capability.

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
