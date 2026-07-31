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

`internal/backupmaterializer/contract` now defines the private
`backup-materializer.fugue.dev/v1/BackupObserverInputBundle` seam between the
legacy data/signer owner and a future fixed-purpose materializer without
importing the signing implementation. The compatible root
`internal/backupmaterializer` issuer accepts one already validated
`BackupRunSpec`, mints a dedicated 15-minute observer identity, immediately
verifies that identity with the same rotation-aware keyring, and seals the
exact spec/token pair under one digest.
Redundant cell, run, spec-digest, credential, token-id, issued, renew-after,
and expiry bindings are all strict; unknown/trailing/oversized JSON, signature
or digest drift, expiry, revocation, non-canonical time, and either production
mutation flag fail closed. Ordinary `String` and `GoString` formatting always
redact the token, while the private JSON handoff intentionally contains it.
The envelope carries no endpoint, bucket, object key, backend credential,
database handle, or Kubernetes capability.

A capability-separated envelope decoder also verifies the complete public
token payload, redundant bindings, digest, and current lifetime without
receiving the HMAC key. It is valid only after the authenticated materializer
transport has succeeded and deliberately cannot authenticate the signature;
the full keyring validator remains mandatory anywhere that already owns
verification capability. This prevents the future input client from gaining
the ability to mint observer credentials merely to consume one generation.

The pure-contract atom initially added only the bundle and its independent
read-only `backup-input-bundle-${ref}` validation lane. The default-off HTTP
registration described below now exposes the versioned shape but has no
process-root composition. There is still no materializer process, Secret
writer, service account, RBAC grant, image, or workload. The current
default-off chart continues to consume its existing external ConfigMap/Secret
until a later reviewed atom moves both keys into one bundle-derived Secret
projection; production behavior is therefore unchanged.

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

`internal/backupmaterializer/httpapi` defines the
`backup-materializer-input-api@v1` handler core for the exact
`GET /v1/backup-control/runs/{run}/observer-input-bundle` route. It accepts no
query string or request body and never guesses a desired run from creation
time or list ordering. A capability-separated source is queried with the exact
reviewed cell and path run, then supplies one current, already redacted
`BackupRunSpec`; the handler validates the complete contract and exact path
run, compares the returned server-owned spec cell with the reviewed
materializer claims, and only then invokes a separately injected issuer. A
foreign-cell run is indistinguishable from an absent run, source/signer details
never enter responses, and a drifted source or issuer fails closed. Successful
responses are bounded strict private JSON and cannot contain the caller's
projected ServiceAccount token.

The handler imports no legacy API, store, model, database, Kubernetes, object
storage, filesystem, or process capability. It owns no route registration; a
separate generated default-off gate now names it, but the process root attaches
no concrete composition. The existing `backup-input-bundle-${ref}` lane
recursively validates this handler and its identity dependency rather than
creating another release queue.

`internal/backupmaterializer/legacysource` implements the transitional
`backup-materializer-legacy-source@v1` compatibility edge around one injected,
bounded snapshot callback. The callback returns only a legacy run plus the
data owner's irreversible backend-generation digest. The adapter reuses the
single `backupadapter.BuildShadowSpec` mapping, requires the exact reviewed
cell/run precondition, and reduces not-found, inconsistent, and unavailable
snapshot outcomes to fixed errors with no backend detail. Its production
dependency closure contains no store, API, database, HTTP, Kubernetes,
filesystem, signer, or mutation capability.

`internal/backupmaterializer/localissuer` supplies the separate
`backup-materializer-local-issuer@v1` signing edge. Construction validates and
copies the dedicated observer keyring into an immutable closure, preventing
later map mutation or ordinary struct formatting from exposing or changing
signing state. Issuance is context-bounded, delegates to the self-verifying
bundle builder, and collapses every signing/configuration failure to one fixed
detail-free unavailable result. It has no source/store/model, network,
filesystem, Kubernetes, route, or mutation dependency.

`internal/backupmaterializer/storesource` is the sole transitional
`backup-materializer-store-source@v1` data-owner bridge. Its stored interface
contains exactly `GetBackupRun` and `GetBackupBackendObservation`; no mutation
method on the monolithic store is reachable through it. Each read uses
platform scope only for one exact run and its referenced backend, validates
run/backend/tenant identity, and emits only the legacy run plus the already
irreversible generation digest. Missing, inconsistent, and unavailable store
outcomes are reduced to fixed snapshot errors, and formatting cannot expose
the underlying store. The package has no API, HTTP, signer, Kubernetes,
filesystem, or direct SQL dependency. A real JSON-store test proves the
adapter and legacy mapper leave the store byte-for-byte unchanged.

`internal/backupmaterializer/composition` is the default-disabled
`backup-materializer-composition@v1` composition root. A disabled construction
does not validate or retain the supplied store, keyring, projected credential,
CA, API origin, or clock and returns a private fail-closed handler. An enabled
construction joins only the read-only store bridge, legacy mapper, immutable
local issuer, rotating projected TokenReview adapter, claims-only middleware,
and input handler core. Constructor failures collapse to one fixed error, and
ordinary configuration and handler formatting is redacted.

The composition root owns no mux, generated path, server, environment reader,
ServiceAccount, RBAC, Secret writer, Kubernetes mutation, or datastore write.
A black-box test uses the real JSON store and a TLS TokenReview endpoint, then
crosses every composed boundary. The returned private bundle is
cryptographically valid while the store remains byte-for-byte unchanged;
bucket, endpoint, backend credentials, API caller credential, and presented
materializer credential do not cross the response. The existing input-bundle
lane covers the composition and its reviewer dependency plus the exact legacy
mapping and two relevant backup store files, without adding a release queue.

The authoritative OpenAPI source now registers the exact GET-only
`backup-materializer-input-route@v1` path and a dedicated
`BackupMaterializerBearerAuth` scheme for a Pod-bound projected ServiceAccount
JWT. Its strict response schemas mirror every JSON field in
`BackupObserverInputBundle`, `BackupRunSpec`, and `BackupTarget`; the bearer
field is explicitly sensitive, bounded, and absent from error responses. The
generated route passes through `internal/api/backup_materializer.go`, which
contains only a private availability gate and an injected v1 endpoint
interface. It cannot construct TokenReview, store, source, or signer
capabilities.

Every existing `ServerConfig{}` leaves that interface nil, producing a private
404 before authentication or data access. An injected endpoint must implement
the explicit v1 marker and report enabled at both admission and dispatch; a
typed nil, disabled endpoint, missing handler, or enablement change remains a
private 404. `cmd/fugue-api` still supplies no endpoint, environment input,
keyring, projected volume, or API origin, so requests cannot proceed past the
private availability gate in the running topology.

`internal/backupmaterializer/client` defines the default-off
`backup-materializer-client@v1` consumer boundary for that route. Disabled
construction ignores and retains none of the URL, cell, run, credential
source, HTTP client, or clock. Enabled construction binds one canonical cell
and run to one HTTPS origin, rereads an injected audience-bound workload JWT
for every request, issues only the exact GET, refuses redirects, and bounds
time and response bytes.

The client accepts only `application/json` with `private`, `no-store`,
`Pragma: no-cache`, `Vary: Authorization`, `nosniff`, and no content encoding.
It strictly decodes the bundle envelope, public token claims, cell/run/spec
bindings, digest, issue/expiry window, and requires the generation still to be
within its bounded delivery-age window and before its renewal boundary. Remote
bodies and both credentials are excluded
from errors. Its production dependency closure contains no filesystem,
Kubernetes API, store, model, signer, Secret, process, or mutation capability;
credential projection and TLS-root rotation remain in the narrower adapter
below, while a durable Secret writer and process wiring remain separate later
atoms.

`internal/backupmaterializer/client/projected` defines the default-off
`backup-materializer-client-projection@v1` capability adapter. Disabled
construction does not inspect or retain any URL, path, cell, run, timeout, or
clock. Enabled construction accepts only one absolute non-symlink projection
root containing `token` and `ca.crt` as restricted regular files or through
Kubernetes' exact `<name> -> ..data/<name>` topology. The in-root generation
must remain a real non-writable directory across each bounded read; kubelet's
canonical `0755` generation mode is accepted, while the workload token is
limited to `0400`, `0440`, `0600`, or `0640` and is never world-readable.

Every fetch rereads the JWT and a strict bounded CA-only PEM bundle, builds a
new TLS 1.2+ transport pinned to the configured HTTPS authority, disables
environment proxies, compression, redirects, keep-alive, and idle connection
reuse, and bounds dialing, handshaking, response headers, the whole request,
and the response body. Invalid or racing token/CA generations fail that fetch
with fixed secret-free outcomes; restoring a complete valid generation
recovers without reconstructing the client. Construction validates the
initial projection but performs no network request.

The adapter's local production closure is only `backupcontrol`, the keyless
bundle contract, the isolated client, and this projected package. It has no
TokenReview/reviewer, Kubernetes types or client, signer, datastore, API
server, Secret writer, process execution, RBAC, chart, or deployment
capability. Its rotating transport deliberately never caches a connection or
CA pool across fetches.

`internal/backupmaterializer/materialization` defines the non-executable
`backup-materializer-secret-plan@v1` desired-data boundary. It deterministically
maps one freshly validated bundle to the exact cell-local
`fugue-backup-observer-<kind>-<cell-id>-input` Secret identity in
`fugue-system`, with only `spec.json` and `token` data keys. The plan binds the
run/spec/bundle/credential generation, exact desired spec, content digests,
issue/renew/expiry window, and a cell/generation idempotency key.

Raw spec bytes and the observer bearer remain unexported in the plan and are
absent from its JSON form; an explicitly private accessor returns fresh copies
and diagnostic formatting redacts both. The v1 policy requires
resource-version CAS, retention of the existing generation on failure, and a
last-known-good generation, while permanently setting `executionAllowed` and
`productionMutationAllowed` false. Its production dependency closure is only
`backupcontrol` plus the keyless bundle contract: no signer, filesystem,
network, Kubernetes type/client, datastore, Secret writer, or process exists.

The plan has separate lifecycle validation for new application and retained
LKG state. A new apply candidate must still be inside the one-minute delivery
window and strictly before `renewAfter`; its private data accessor uses that
same gate, so an old plan cannot be replayed. Once a generation has already
been materialized, `ValidateLastKnownGood` permits retention after those
one-time boundaries only while the complete plan/data binding remains intact
and the observer token is still unexpired. At `expiresAt` it fails closed. This
keeps transient renewal failure from deleting a still-working generation
without allowing the materializer to write that stale generation again.

`internal/backupmaterializer/reconcile` defines the pure
`backup-materializer-secret-reconcile@v1` object-evidence and decision
boundary. It assigns each materializer loop exactly one canonical cell-local
Secret identity and publishes a data-free Opaque Secret manifest containing
only owned labels, recovery annotations, the two expected data keys and their
digests. The manifest requires a mutable, independently owned object: a Secret
that is immutable, already deleting, or has any owner reference cannot become
a managed snapshot and therefore cannot later disappear through unrelated
garbage collection or make renewal permanently unreplaceable.

A managed snapshot is sealed only when UID and opaque resourceVersion are
present, all owned metadata bindings match the source plan, and the object has
exactly `spec.json` and `token` with the expected content digests. Unknown
labels and annotations are tolerated so admission metadata can be preserved;
unknown data keys are rejected. Raw values are neither retained independently
nor rendered in JSON or diagnostics. Structural validation is separate from
current lifetime validation, allowing an expired generation to be recognized
without ever making it applyable again.

Restart recovery does not depend on an in-memory copy of the prior plan.
`RecoverCurrent` strictly decodes the stored desired spec, canonical issue,
renewal, and expiry annotations, bundle identity, observer-token claims, and
all manifest/content digests, then reconstructs the same sealed private plan.
It validates structure at the original issue instant only; the reconcile
decision still applies the trusted current clock before retaining an LKG.
`ObserveExisting` turns a valid recovered object into managed state, an object
without this materializer's ownership claim into foreign state, and a claimed
but unrecoverable object into malformed state. Invalid current objects are
therefore stable cell-local blocking evidence rather than process-wide errors.

The cell-local policy emits exactly five shadow outcomes: create-if-absent,
no-op for an identical generation, UID plus resourceVersion-fenced replace,
retain an unexpired last-known-good generation when the source is unavailable,
or block. Creation is never an upsert, replacement retains the current
generation on conflict/failure, and source loss or token expiry is never a
delete instruction. Foreign and malformed objects are blocked rather than
adopted or overwritten. Every decision is digest-bound and permanently sets
`deleteAllowed`, `executionAllowed`, and `productionMutationAllowed` false.
This package has no Kubernetes types/client, filesystem, network, datastore,
signer, process, RBAC, chart, or deployment capability; observation recovery
and a fixed-purpose writer remain later, separately reviewed atoms.

`internal/backupmaterializer/secretreader` adds the default-off, GET-only
`backup-materializer-secret-reader@v1` Kubernetes observation boundary. An
enabled reader is pinned to one HTTPS API origin and the deterministic Secret
name for one exact backup cell. Construction performs no I/O. Each observation
rereads an injected Kubernetes-API audience credential, uses one bounded
deadline and response limit, refuses redirects and content encoding, sends no
body or query, and requests only
`/api/v1/namespaces/fugue-system/secrets/<cell-secret>`.

A 404 becomes absent only when a bounded JSON `v1/Status` proves `NotFound`
for the exact Secret resource and name. A 200 response canonically decodes
base64 data and passes Kubernetes-neutral UID, opaque resourceVersion,
metadata, lifecycle, ownership, and private data evidence into the pure
restart-recovery classifier. Forward-compatible unknown read-only fields are
ignored, while ambiguous `stringData`, noncanonical data, wrong type/kind/API,
or lifecycle drift can produce only foreign/malformed blocking observations.
Remote bodies and the Kubernetes bearer never enter returned errors or
diagnostics.

The reader has an HTTP GET capability but no Kubernetes library/client-go,
discovery, watch, list, filesystem, datastore, signer, process, or mutation
surface. It defines no POST, PUT, PATCH, DELETE, create-if-absent, CAS writer,
ServiceAccount, RBAC, workload, chart, or environment wiring. Credential and
CA projection plus any writer remain separate later atoms, so the production
topology and current Secret state are unchanged.

`internal/backupmaterializer/secretreader/projected` adds the default-off
`backup-materializer-secret-reader-projection@v1` in-cluster bootstrap for
that read boundary. An enabled instance accepts only one absolute,
non-writable projection root containing `token` and `ca.crt` as restricted
regular files or through Kubernetes' exact `<name> -> ..data/<name>` atomic
writer topology. The in-root generation must remain a real directory across
each bounded read. The JWT is never world-readable; CA input is a strict,
bounded CA-only PEM bundle.

Every Secret observation rereads both inputs, builds a fresh TLS 1.2+ direct
transport pinned to the configured HTTPS authority, and closes the connection
after the response. Environment proxies, compression, redirects, cookies,
keep-alive, cached CA pools, and cached credentials are excluded. The
transport admits only the exact GET-only Secret path already sealed for its
cell, five fixed request headers, no body, query, trailer, transfer encoding,
host override, or alternate path. Invalid or racing rotations fail only that
observation; restoring one complete valid generation recovers without
reconstructing the reader.

The projection package adds filesystem and TLS capability only around the
already isolated reader. It imports no Kubernetes SDK/client-go, discovery,
watch, list, datastore, API server, signer, desired-input client, Secret
writer, process, RBAC, ServiceAccount, workload, chart, or deployment code.
No production composition, token projection, or permission grant constructs
it yet, so the live Kubernetes API and Secret remain untouched.

`internal/backupmaterializer/reconciler` composes those two read boundaries
with the pure CAS/LKG policy as the default-off
`backup-materializer-reconciler@v1` single-cell, single-cycle shadow control
loop. It samples one injected trusted clock, observes the exact current Secret
first, and does not request private desired input when the current object is
foreign or malformed. Current-source failure, invalid evidence, and
cross-cell evidence become one retryable `current-observation-unavailable`
status local to that cell; they do not fail another cell or authorize an
overwrite. Only a valid absent or managed observation can reach the desired
source.

A desired-source failure becomes unavailable, while an invalid, stale, or
cross-cell bundle becomes invalid. Both flow through the same nil-desired
policy: retain a structurally valid unexpired LKG, otherwise block, and never
delete. A valid generation can produce only the already-defined
create-if-absent, no-op, or UID plus resourceVersion CAS replacement
candidate. Runtime source failures are represented as digest-bound cell-local
status rather than process-wide errors; caller cancellation and impossible
internal contract drift remain errors for the future supervisor.

The public status distinguishes not-read, available, unavailable, and invalid
desired state. `ready` is true only for no-op or LKG retention, `converged`
only for no-op, and `lastKnownGoodServing` only for LKG retention. Its stable
idempotency key excludes evaluation time but binds cell, current observation,
desired state and generation, action, and reason; the status digest includes
the exact evaluation time and nested decision. Every path keeps
`deleteAllowed`, `executionAllowed`, and `productionMutationAllowed` false.
The package owns no HTTP, filesystem, Kubernetes, store, signer, writer,
timer, goroutine, process, RBAC, workload, chart, or deployment capability.
The concrete client and Secret reader satisfy its injected interfaces.

The reconciler also owns the default-off
`backup-materializer-candidate-handoff@v1` in-process `PreparedCycle`
contract. `PrepareOnce` performs the same single current/desired source reads
and pure decision as `ReconcileOnce`; it does not perform a second fetch. Only
an unblocked create or CAS-replace candidate retains its exact private plan.
No-op, LKG, blocked, malformed, foreign, invalid-source, and unavailable-source
results retain the public status only.

The serialized and formatted handoff contains the validated reconcile status,
candidate plan digest, idempotency key, evaluation time, and permanently false
delete/execution/production flags. Raw spec and observer token bytes stay in an
unexported field and can be obtained only through `CandidatePlan`, which first
revalidates the complete status/plan binding. `PreparedCycleEvidence` has the
same canonical public digest but physically contains no plan, so downstream
status can revalidate the source handoff without retaining its capability. A
JSON round trip deliberately cannot recreate the private plan. The existing
agent continues to call `ReconcileOnce`, so this atom neither connects the
handoff to a writer nor changes the running process, image dependency closure,
chart, RBAC, or production state.

`internal/backupmaterializer/agent` and
`cmd/fugue-backup-materializer` add the default-off
`backup-materializer-agent@v1` process boundary around exactly one such cell.
The supervisor runs an immediate evaluation and then one serial, bounded
evaluation per interval. It exposes only loopback `GET /healthz`,
`GET /readyz`, and `GET /v1/status`; its digest-bound snapshot retains the
last valid result after a local attempt failure but fails readiness closed
until the cell evaluates successfully again. A create or CAS candidate is
reported as non-ready and non-executable, while no-op or valid LKG retention
is ready. No status permits delete, execution, or production mutation.

The process acquires two deliberately separate projected identities only when
explicitly enabled: one audience-bound projection for the desired-input API
and one Kubernetes-API projection for observing the exact cell Secret. It
constructs the GET-only projected adapters and pure reconciler directly; its
compiled dependency closure contains no legacy API, store/model, database,
Kubernetes SDK, TokenReview adapter, signer, Secret writer, backup executor,
object-store client, registry client, or subprocess capability. Disabled mode
ignores and retains none of the cell, run, endpoint, projection, or timing
inputs and performs no projection or network access.

Loopback serving and the cell loop share one cancellation boundary. Shutdown
stops new health traffic, drains the server, and waits for the loop under one
bounded deadline. The binary also supplies direct loopback-only health and
readiness probes without shell or external HTTP tooling. This atom creates an
independently compiled shadow process, not a production workload; at that
boundary there is still no image, ServiceAccount, RBAC, token projection, Helm
object, Secret write path, release dispatch, or production enablement.

`Dockerfile.backup-materializer` and the dedicated
`backup-materializer-image-${ref}` workflow add the next independent artifact
boundary. The scratch image contains only the static non-root materializer
binary. Its build context copies the exact production dependency closure; it
does not copy legacy API/store/model, identity signing/review code, desired
input serving adapters, Kubernetes SDKs, a Secret writer, backup execution,
object-store/registry clients, a shell, or global CA roots. The two outbound
read adapters trust only their separately projected CA bundles.

The path-scoped workflow has only `contents: read`, pins every action by full
commit, tests the exact local dependency set, compiles Linux amd64 and arm64,
loads the image only into the ephemeral runner, and probes it without a
network. The black-box probe runs the default-disabled image with a read-only
root, no Linux capabilities, no-new-privileges, no published or declared port,
and deliberately invalid private projection settings. It must become live but
remain unready, omit capability-shaped data from logs, lack `/bin/sh`, and
exit cleanly on SIGTERM. At that image-only atom, the workflow had no login,
push, artifact upload, dispatch, Helm, Kubernetes, environment, or production
capability, and there was still no ServiceAccount, RBAC, projected volume,
Pod, chart, Secret write path, published image, or production enablement.

`deploy/helm/fugue-backup-materializer` adds the default-off
`backup-materializer-workload@v1` shadow boundary for exactly one backup cell
and run. Disabled values render no objects. Enabling is valid only in
`fugue-system` and renders one deterministic ServiceAccount, one Role, one
RoleBinding, and one singleton `Recreate` Deployment; the resource identity is
derived from the canonical cell and does not depend on the Helm release name.

The dedicated ServiceAccount and Pod both disable the default token mount. Two
separate projected volumes each rotate their own ten-minute, Pod-bound token
and CA bundle. The desired-input token has the exact
`fugue-backup-materializer.fugue.dev` audience and trusts only an externally
owned input-API CA ConfigMap. The Kubernetes reader token deliberately omits an
audience so the TokenRequest receives the API Server's configured default
audience rather than a guessed cluster identifier; it trusts only the
namespace's `kube-root-ca.crt` projection. Neither identity projection is
available at the other adapter's path.

The Role grants exactly `get` on the deterministic cell-local observer-input
Secret through `resourceNames`; it has no list, watch, create, update, patch,
delete, status, lease, ConfigMap, cluster, or non-resource permission. The
scratch container remains non-root, read-only, capability-free, loopback-only,
resource-bounded, and exposes no Service or port. Its status can report create
or CAS candidates, but the binary still has no writer and every object is
marked `production-mutation=forbidden`.

The materializer's existing build-only lane now validates this chart and its
rejection tests along with the binary and image. It still only loads an image
inside the ephemeral runner and has no registry login, package upload,
workflow dispatch, Kubernetes command, Helm install/upgrade/package/push,
environment, release, or production capability. No chart was installed and no
image was published by this atom.

`internal/backupmaterializer/secretwriter` adds the separately validated,
default-off `backup-materializer-secret-dry-run@v1` network boundary. It
accepts only a fresh, valid create-if-absent or UID/resourceVersion-CAS
replacement decision bound to the exact configured cell and sealed desired
plan. It serializes exactly one Opaque Secret with the two owned data keys,
owned metadata, no owner, no finalizer, and `immutable=false`.

The adapter can issue only `POST` to that cell's Secret collection or `PUT` to
that exact Secret. Every URL contains the fixed `dryRun=All`, strict field
validation, and fixed field manager query. Redirects, retries, cookies, patch,
delete, list, watch, arbitrary paths, and live-write mode are absent. It
validates the API response's exact cell, metadata bindings and data bytes, but
does not treat UID, resourceVersion or other generated dry-run fields as
durable evidence. The returned digest-bound receipt contains no Secret data or
bearer and permanently reports `persisted=false`, `executionAllowed=false`,
and `productionMutationAllowed=false`.

This follows [Kubernetes server-side dry-run semantics](https://kubernetes.io/docs/reference/using-api/api-concepts/#dry-run):
admission, validation and conflict checks run, but the storage stage is
skipped. Kubernetes deliberately authorizes dry-run exactly like a real
mutation, so this atom does **not** add create/update RBAC to the cell chart and
does not wire the adapter into the process, image, projected credential, or
control loop. Its dedicated read-only validation lane has no build, upload,
dispatch, Helm, Kubernetes, release, or production capability.

`internal/backupmaterializer/secretwriter/projected` adds the default-off
`backup-materializer-secret-dry-run-projection@v1` bootstrap without wiring it
into the materializer binary. It accepts only a safe regular-file projection
or Kubernetes' exact atomic-writer link topology, rereads the token and CA for
each request, rejects group/world-writable credentials or trust roots, and
creates a fresh TLS 1.2+ direct connection with no proxy, keepalive, cookie,
redirect, or connection reuse.

The transport independently reads the bounded request body before opening a
socket and validates the fixed credential headers, HTTPS authority,
method/path pair, exact dry-run query, target cell, create/CAS metadata, the
canonical writer-generated JSON bytes, sealed spec/token generation, and
absence of owners, finalizers or generated request fields. The Kubernetes API
remains responsible for authenticating its configured token audience. The
transport then removes the replayable body factory before sending. A partial
atomic-writer rotation fails only that validation attempt; a later complete
token/CA generation recovers without restart. This package remains outside the
process dependency closure and image workflow, so it has no active credential,
RBAC, Pod, Secret mutation, release, or production capability.

`internal/backupmaterializer/dryrunreconciler` adds the default-off
`backup-materializer-secret-dry-run-cycle@v1` status owner around the pure
reconcile result and injected dry-run writer. It accepts only an
`available` desired state whose validated reconcile status is an unblocked
create-if-absent or UID/resourceVersion-CAS mutation candidate, plus the exact
private plan whose digest that status names. No source is reread, so one
validation attempt remains bound to the current observation, desired plan and
decision that produced it.

The component makes at most one validator call. An authenticated dry-run
receipt becomes `accepted`; conflicts, admission rejection, credential or API
unavailability, invalid responses, and stale intents become fixed cell-local
outcomes without retaining error text. Retryable outcomes require a later
fresh cycle, while rejection is fail-closed until policy or input changes. A
replacement status always records that the existing object was preserved,
because dry-run never reaches storage.

Every result nests the secret-free reconcile status and, only on success, the
secret-free dry-run receipt. Its idempotency key binds the upstream cycle and
decision, while the status digest additionally binds the cell, action,
outcome, attempt time and optional receipt. `persisted`, `deleteAllowed`,
`executionAllowed`, and `productionMutationAllowed` remain false for every
outcome. The package imports neither the projected writer bootstrap nor any
reader, source, process, image, chart, RBAC, Kubernetes SDK, datastore or
legacy control-plane code. It is not wired into the binary or workload, so no
credential or production capability is active.

`internal/backupmaterializer/validationcycle` adds the default-off
`backup-materializer-validation-cycle@v1` zero-or-one composition. It invokes
one injected `PrepareOnce`. Non-candidates require zero validator calls and
preserve the source cycle's no-op, LKG, blocked, and retryable semantics. A
candidate extracts its private plan exactly once, invokes one injected dry-run
controller, and then drops the plan; the returned status contains only
`PreparedCycleEvidence` and the secret-free validation status.

The composite status independently revalidates both nested contracts and
binds source status, candidate digest, action, outcome, attempt time, retry
semantics, and existing-object preservation. Even an accepted admission dry
run remains non-ready, non-converged, non-persisted, and unable to delete or
execute. Source and validator errors are reduced to fixed invariant errors,
while expected API failures remain the cell-local outcomes owned by the
dry-run controller. The package has no projected credential, TLS transport,
reader, source implementation, process, image, chart, RBAC, Kubernetes SDK,
datastore, release, or production wiring.

`internal/backupmaterializer/validationagent` adds the separately default-off
`backup-materializer-validation-agent@v1` supervisor around exactly one such
cell. It immediately runs one bounded validation cycle and then waits one full
interval after each completed attempt. A mutex prevents overlapping attempts,
including concurrent callers, so a rotating source generation or one dry-run
candidate can never be consumed twice by this agent.

Its digest-bound snapshot mirrors no-op, LKG, blocked, retryable, candidate,
validation-attempted, accepted, and existing-object-preserved state. An
expected Kubernetes conflict, rejection, credential failure, or API failure
remains the validated cell-local outcome supplied by the inner controller. An
agent timeout, cancellation, contained cycle panic, arbitrary error, invalid status, or cross-cell
status instead clears current readiness and evidence, increments only this
cell's failure counter, and retains an independent deep copy of the last valid
status for diagnosis and automatic recovery on the next cycle.

The package supplies only read-only handlers for `GET /healthz`,
`GET /readyz`, and `GET /v1/status`; it owns no listener or process. Disabled
construction retains none of its inputs. Every snapshot remains
`persisted=false`, `deleteAllowed=false`, `executionAllowed=false`, and
`productionMutationAllowed=false`, including after an accepted server-side
dry run. The validation agent imports neither the existing observation agent
nor projected credentials, readers, source implementations, Kubernetes SDK,
datastore, process, image, chart, RBAC, release, or production code. It is
covered by the dry-run validation lane, while the existing materializer image
lane is also triggered to prove the new package remains outside that image's
exact dependency closure.

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
closure. Neither the HTTP middleware nor the input handler is attached to a
server or generated route, and there is still no environment wiring, RBAC,
ServiceAccount, projected-token volume, materializer process, or chart wiring;
those remain separately reviewed default-off atoms, so production behavior is
unchanged.

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
