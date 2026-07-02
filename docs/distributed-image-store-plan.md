# Fugue distributed image store plan

This document describes the path from the current bundled central registry plus
node-local image-cache model to a registryless mode where the control plane owns
image indexing and replication scheduling, and `fugue-image-cache` becomes a
replicated distributed image store.

This document now tracks the implemented registryless image-store cutover. It
does not prescribe a manual production hotfix path. Any implementation that
affects the control plane, registry, image-cache, cluster bootstrap, runtime
image routing, Helm chart, or platform traffic rules must be released through
the normal Fugue repository workflow and the
`.github/workflows/deploy-control-plane.yml` control-plane deployment path.

For the production disk-pressure follow-up around image-cache orphan cleanup
and LVM LocalPV backing-file recovery, see
`docs/image-cache-localpv-storage-recovery-plan.md`. That recovery plan is the
authoritative scope for manifest-level cache cleanup and explicit LocalPV
decommission work; this document remains the broader distributed image-store
architecture plan.

## 1. Goal

Reach this target architecture:

```text
Build / import / external image source
  -> node-local image-cache write or pull
  -> control-plane image index records canonical digest and manifests
  -> control-plane replication scheduler keeps N healthy replicas
  -> node-local image-cache stores, verifies, serves, replicates, and prunes
  -> containerd pulls through the local image-cache mirror
```

The control plane should be the authority for image identity, desired replica
policy, placement, health, and deletion safety. It should not store image blob
bytes. Each `fugue-image-cache` instance should store image bytes and expose a
registry-compatible pull surface plus a controlled internal management surface.

## 2. Non-goals

- Do not remove the bundled registry before image-cache has durable replica
  semantics.
- Do not make the control plane store OCI layer bytes.
- Do not rely on a single agent node as the only copy of an app image.
- Do not treat the existing `image_locations` table as sufficient durability.
- Do not make app-specific or project-specific image retention exceptions.
- Do not expose every node-local cache as an unauthenticated registry endpoint.
- Do not replace all external registry behavior. User-supplied Docker image
  references can still be mirrored into the Fugue managed image namespace.

## 3. Current baseline

The current system already contains pieces of a distributed image path, but the
durability model still depends on a central origin registry.

### Bundled central registry

- `deploy/helm/fugue/templates/registry-deployment.yaml` creates one
  `Deployment` with `replicas: 1`, `strategy: Recreate`, and labels that mark it
  as an isolated singleton.
- The registry stores Docker Distribution data at `/var/lib/registry` in the
  container. With hostPath persistence, that maps to `/var/lib/fugue/registry`;
  the default chart value is PVC mode.
- `deploy/helm/fugue/templates/_helpers.tpl` defines `registryPushBase` as the
  bundled service address unless `api.registryPushBase` is configured, and
  `registryPullBase` defaults to `registryPushBase`.
- API and controller pods receive `FUGUE_REGISTRY_PUSH_BASE` and
  `FUGUE_REGISTRY_PULL_BASE`.

Current role: central registry is the default managed image origin, inspect
target, deletion target, GC target, and upstream fallback for image-cache.

### Node-local image-cache

- `deploy/helm/fugue/templates/image-cache-daemonset.yaml` creates a DaemonSet
  with `hostNetwork: true` and a host port, defaulting to port `5000`.
- Each pod uses `FUGUE_IMAGE_CACHE_STORE_DIR=/var/lib/fugue/image-cache/registry`
  and mounts the host path `/var/lib/fugue/image-cache`.
- `cmd/fugue-image-cache/main.go` starts a local registry using
  `registry.New(registry.WithBlobHandler(registry.NewDiskBlobHandler(storeDir)))`.
- Pull requests first hit local storage. If local storage misses, image-cache:
  1. looks up present `image_locations` in the control plane,
  2. tries to copy from peer cache endpoints,
  3. falls back to `FUGUE_IMAGE_CACHE_UPSTREAM_BASE`, which defaults to
     `registryPushBase`.
- Non-pull registry writes go into the image-cache local registry and report a
  `present` image location.

Current role: image-cache is a local pull-through registry and sometimes a
node-local build destination. It is not yet a durable image store because it has
no replica policy, no authoritative pin model, and no repair loop.

### Runtime pull routing

- `internal/api/join_cluster.go` writes `/etc/rancher/k3s/registries.yaml` so
  containerd pulls `registryPullBase` through `FUGUE_JOIN_REGISTRY_ENDPOINT`.
- With image-cache enabled, `clusterJoinRegistryEndpoint` defaults to
  `http://127.0.0.1:<imageCache.port>`.
- `internal/appimages/usage.go` maps managed image refs from `registryPushBase`
  to runtime refs under `registryPullBase`.

Current role: workloads already pull through node-local image-cache while still
using the logical managed registry namespace.

### Builder and import paths

- `builderRegistryPushBase` defaults to `127.0.0.1:<imageCache.port>` when
  image-cache is enabled.
- `internal/sourceimport/registry.go` rewrites builder destinations from
  `registryPushBase` to `FUGUE_BUILDER_REGISTRY_PUSH_BASE`.
- `internal/sourceimport/image_source.go`, `buildpacks.go`, `nixpacks.go`, and
  `importer.go` still model the logical image as a managed ref under
  `registryPushBase`.
- `internal/controller/import_operation.go` can accept node-local builder
  registry evidence in some cases, but the normal confirmation path still
  inspects managed refs with the remote inspector.

Current role: builds can land in node-local image-cache, but the controller
still treats the central registry namespace as the managed image identity.

### Control-plane image location records

- `internal/model/model.go` defines `ImageLocation` with image ref, digest,
  node, runtime, cluster node, cache endpoint, status, last seen time, size, and
  last error.
- `internal/store/postgres.go` creates `fugue_image_locations` and lookup
  indexes.
- `internal/api/image_locations.go` provides API, node-updater, and runtime
  endpoints for listing and reporting locations.
- `cmd/fugue-image-cache/main.go` reports `pulling`, `present`, `missing`, and
  `failed`.
- `internal/api/node_updater.go` reports `present` after `prepull-app-images`
  succeeds and the local mirror serves the manifest.
- `internal/controller/deploy_image_guard.go` falls back to present
  image-location evidence when direct managed-image inspection fails under
  node-local builder mode.

Current role: `image_locations` is useful evidence, but it is not a storage
contract. It does not express desired replicas, pin reason, lease validity,
manifest graph, blob availability, failure domain, capacity, or deletion safety.

### Existing cleanup and registry maintenance

- `internal/appimages/remote_delete.go` deletes managed images through the
  remote registry API.
- `internal/controller/prune_images.go`, `delete_images.go`, and
  `image_retention_sweep.go` reason about managed registry refs and remote
  deletes.
- `deploy/helm/fugue/templates/registry-gc-cronjob.yaml` scans
  `/var/lib/registry/docker/registry/v2`, pauses around active builders/imports,
  and runs bundled registry GC.

Current role: retention and GC are registry-centric, not distributed-cache
centric.

## 4. Gaps to close before registryless mode

Deleting the central registry without closing these gaps would turn cache miss
from a recoverable event into possible permanent image loss.

1. Replica policy is missing. Fugue does not currently require N present copies
   of each managed image.
2. Location health is weak. A stale `present` row can survive node loss or disk
   cleanup until a pull fails.
3. Manifest identity is not authoritative in the control plane. Tags, digest
   refs, manifest lists, platform manifests, configs, and layers are not indexed
   as a graph.
4. Cache storage is not pinned. A node has no control-plane lease saying which
   images must be preserved and why.
5. Replication is demand-driven. Hydration happens on pull or deploy guard
   scheduling, not from a background reconciliation loop.
6. Distributed GC does not exist. Existing delete and GC logic target the
   central registry.
7. Peer registry access needs stronger auth and tenant boundaries before it is
   treated as a storage substrate.
8. Build/import success criteria still depend on remote registry inspection in
   several paths.
9. Billing/storage accounting is coupled to remote registry inspection and blob
   sizes, not distributed replica inventory.
10. Rollback is missing. There is no feature-gated path to re-enable the central
    registry as origin if distributed replication falls behind.

## 5. Target architecture

### Logical image model

Every managed app image should have a canonical control-plane record:

```text
managed image
  tenant_id
  app_id
  source_operation_id
  logical_ref                  registryPullBase or stable Fugue-managed ref
  canonical_digest             sha256:...
  manifest_media_type
  manifest_size_bytes
  manifest_json or compact descriptor graph
  platform descriptors         for OCI index / Docker manifest list
  total_unique_blob_bytes
  lifecycle_state              importing | available | deleting | deleted | lost
  required_replica_count
  min_available_replica_count
```

Tags should not be the durability key. Tags are aliases. The durable identity
should be digest-first, with tag aliases recorded separately.

### Replica model

Each cache-held copy should be represented as a replica record:

```text
image replica
  image_id or canonical_digest
  tenant_id
  app_id
  node_id
  runtime_id
  cluster_node_name
  cache_endpoint
  status                        planned | copying | verifying | present |
                                stale | draining | deleting | missing | failed
  source_replica_id
  last_verified_at
  lease_expires_at
  size_bytes
  failure_domain                region / provider / runtime cell / node
  last_error
```

`fugue_image_locations` can either evolve into this model or remain as a
compatibility view over a new `fugue_image_replicas` table. A compatibility
view is safer for the transition because existing handlers and CLI consumers
can keep reading `image_locations` while the scheduler uses richer fields.

### Pin model

The control plane should create explicit pins before relying on any cache copy:

```text
image pin
  image_id
  reason                        current_deploy | rollback_window |
                                import_result | user_pin | retention |
                                replication_seed
  tenant_id
  app_id
  operation_id
  expires_at nullable
  min_replicas
```

GC must not delete an image that has an active pin unless the delete operation
also removes or changes the pin in the same control-plane transaction.

### Replication task model

The scheduler should create explicit tasks instead of relying only on pull-time
hydration:

```text
image replication task
  image_id
  source_cache_endpoint or source_node_id
  target_node_id / target_runtime_id / cluster_node_name
  status                        pending | running | completed | failed |
                                canceled
  priority                      deploy_blocking | repair | warmup | rebalance
  attempts
  last_error
```

This can initially reuse node-updater tasks by adding a new task type, for
example `replicate-app-image`, or by extending `prepull-app-images` with
explicit source and verification metadata. A dedicated task type is cleaner
because replication is not just a containerd pull; it must also verify cache
storage and report replica metadata.

### Image-cache management surface

`fugue-image-cache` should keep its registry-compatible `/v2/` surface and add
an internal authenticated management API, reachable only from the control plane,
node updater, or trusted peers:

```text
GET  /fugue/cache/v1/health
GET  /fugue/cache/v1/inventory
HEAD /fugue/cache/v1/images/{digest}
POST /fugue/cache/v1/verify
POST /fugue/cache/v1/replicate
POST /fugue/cache/v1/pin
POST /fugue/cache/v1/unpin
POST /fugue/cache/v1/prune
```

The registry `/v2/` API remains the data-plane pull interface. The management
API is the storage-control interface.

### Control-plane scheduler

The scheduler should run as a controller loop:

1. Read managed images, active pins, node health, cache capacity, and replicas.
2. Mark stale replicas when node-updater heartbeat, cache inventory, or
   `last_verified_at` exceeds the allowed age.
3. For each pinned image, compare healthy replicas with required replicas.
4. Pick target nodes using failure domain, runtime placement, existing app
   scheduling, free capacity, and anti-affinity.
5. Pick source replicas by health, proximity, and load.
6. Create replication tasks.
7. Verify completion by asking target cache for the digest and by requiring the
   target to report `present`.
8. Create prune tasks only after replica counts exceed policy and no pins
   require the candidate copy.

### Runtime pull path

Containerd continues to pull `registryPullBase` through local image-cache. On
local miss, image-cache can still ask the control plane for source replicas and
copy from peer caches. The difference is that this becomes an emergency/demand
path; normal deploys should already have a present target replica before the
workload starts.

### Registryless mode

Registryless mode is a feature-gated chart setting:

```text
imageStore:
  mode: bundled-registry | distributed | distributed-with-registry-fallback
  minReplicas: 2
  targetReplicas: 2
  requireCrossNodeReplicas: true
  requireCrossRegionReplicas: false
  verifyInterval: 10m
  replicaLeaseTTL: 30m
  pruneEnabled: false
```

The production HA values now use `imageStore.mode=distributed` with
`registry.enabled=false`. In that mode the bundled central registry is not
rendered, image-cache has no central upstream fallback, registry GC/janitor
control names are empty, and controller strict paths do not inspect, delete,
garbage-collect, or bill through the central registry. The
`distributed-with-registry-fallback` and `bundled-registry` modes remain as
rollback settings for future releases that explicitly re-enable a registry
origin.

## 6. Data model changes

Add model structs in `internal/model` and store methods in `internal/store`.
Use Postgres DDL in `internal/store/postgres.go`, following the current
schema-bootstrap style.

Suggested tables:

```sql
fugue_images
  id
  tenant_id
  app_id
  image_ref
  canonical_digest
  media_type
  manifest_json
  manifest_size_bytes
  blob_bytes
  source_operation_id
  lifecycle_state
  required_replicas
  min_available_replicas
  created_at
  updated_at

fugue_image_aliases
  id
  image_id
  tenant_id
  alias_ref
  digest
  created_at
  updated_at

fugue_image_replicas
  id
  image_id
  tenant_id
  app_id
  digest
  node_id
  runtime_id
  cluster_node_name
  cache_endpoint
  failure_domain
  status
  source_replica_id
  last_verified_at
  lease_expires_at
  size_bytes
  last_error
  created_at
  updated_at

fugue_image_pins
  id
  image_id
  tenant_id
  app_id
  operation_id
  reason
  min_replicas
  expires_at
  created_at
  updated_at

fugue_image_replication_tasks
  id
  image_id
  tenant_id
  app_id
  source_replica_id
  source_cache_endpoint
  target_node_id
  target_runtime_id
  target_cluster_node_name
  priority
  status
  attempts
  last_error
  created_at
  updated_at
```

Index requirements:

- lookup replicas by `(tenant_id, image_id, status, last_verified_at DESC)`
- lookup replicas by `(digest, status)`
- unique active replica identity by `(image_id, node_id, runtime_id,
  cluster_node_name)` where status is not terminal
- lookup pins by `(tenant_id, app_id, reason, expires_at)`
- lookup replication tasks by `(status, priority, updated_at)`

Compatibility:

- Keep `/v1/image-locations` working during migration.
- `image_locations` can be populated from replica reports or maintained as a
  compatibility table until all callers move to image replicas.
- Existing `presentImageLocations` deploy guard logic can first read new
  replicas, then fall back to old locations.

## 7. API and OpenAPI changes

Because Fugue is OpenAPI-first, all HTTP changes must start in
`openapi/openapi.yaml`, then regenerate with `make generate-openapi`.

Add platform-admin and node-updater APIs:

```text
GET  /v1/images
GET  /v1/images/{id}
GET  /v1/images/{id}/replicas
POST /v1/images/{id}/replicas/report
POST /v1/images/{id}/verify
POST /v1/images/{id}/pins
DELETE /v1/images/{id}/pins/{pin_id}
GET  /v1/image-replication-tasks
POST /v1/image-replication-tasks
POST /v1/node-updater/image-replicas/report
GET  /v1/node-updater/image-replication-tasks
```

Security:

- Platform admins can list and manage all image state.
- Tenant-scoped users can list only their images and safe replica summaries.
- Node updaters can report only their own node/runtime replica state.
- Runtime actors should not receive cross-tenant cache endpoints unless they
  already have platform-level scope.

`/v1/image-locations` should remain during the transition, but new scheduler
logic should use image replicas and pins.

## 8. Controller changes

### Import completion

After build/import writes an image to a node-local cache:

1. Resolve the canonical digest.
2. Create or update `fugue_images`.
3. Create aliases for managed tag refs and digest refs.
4. Create pins for current deploy and rollback retention.
5. Report or verify the builder node as the first replica.
6. Block import completion until minimum write quorum is reached, or mark the
   app as pending image replication if the user-facing operation may finish
   before warmup.

The existing `resolveImportedManagedImageRef` path should stop treating remote
registry inspection as the only authoritative confirmation when distributed
mode is enabled.

### Deploy guard

`ensureDeployableImage` should become digest/replica aware:

- Resolve app runtime image to the managed image record.
- Confirm at least one healthy source replica exists.
- Confirm the target node has a healthy replica or create a deploy-blocking
  replication task.
- If no healthy replica exists but the source is rebuildable, keep the current
  rebuild behavior.
- If no healthy replica exists and no source is rebuildable, mark the image
  `lost` and fail with a clear recovery message.

### Replication scheduler

Add a controller loop, for example `image_replication_controller.go`:

- `reconcileImageReplicas(ctx)`
- `markStaleImageReplicas(ctx)`
- `scheduleMissingReplicas(ctx)`
- `scheduleDeployTargetReplica(ctx, app, target, imageID)`
- `verifyReplicationTaskCompletion(ctx, task)`
- `scheduleReplicaPrune(ctx)`

The scheduler should be idempotent and safe to run repeatedly.

### Retention and delete

Replace remote-registry deletion with distributed delete intent:

- Deleting an app removes current pins for that app.
- Retention sweep removes expired rollback pins.
- Prune tasks delete cache replicas only when the remaining healthy replicas
  satisfy policy.
- Mark image `deleted` only when all replicas are gone or intentionally left as
  cold retained copies.

Registry GC remains only for bundled-registry fallback mode.

## 9. Image-cache changes

### Inventory and verification

Image-cache should periodically inspect its local registry store and report:

- cache endpoint
- node/runtime identity
- image digest
- aliases served
- size bytes
- last verified time
- disk capacity and free bytes
- verification errors

Verification must prove the local `/v2/<repo>/manifests/<target>` path serves
the requested digest or alias and that required child manifests for multi-arch
images are available.

### Auth

The management API and peer copy path should require signed node credentials or
short-lived tokens minted by the control plane. Do not rely on open hostPort
reachability.

Minimum requirement:

- management endpoints reject unauthenticated callers
- peer-copy tokens are scoped to one image digest and expire quickly
- audit logs include source node, target node, image digest, and task id

### Replicate

Add a task handler that can copy a digest from a selected source cache endpoint
to local storage:

```text
POST /fugue/cache/v1/replicate
  image_ref
  digest
  source_cache_endpoint
  task_id
  expected_media_type
```

It should use `crane.Copy` or lower-level go-containerregistry APIs, then
persist/replay the manifest tree exactly as current hydration does.

### Pin and prune

Image-cache needs local durable pin metadata so a local prune cannot remove a
control-plane-pinned image while the control plane is temporarily unreachable.
The local pin store can start as an atomic JSON or SQLite file under
`/var/lib/fugue/image-cache`, but it should be abstracted for later migration.

Prune must be conservative:

- never delete an image with an active local pin
- never delete while a replication or pull operation is active
- report candidate bytes before deleting
- support dry run
- report final state back to the control plane

## 10. Node-updater changes

The current node-updater can run `prepull-app-images`. Keep that path for deploy
warmup, but add distributed-store task types:

```text
replicate-app-image
verify-image-cache
prune-image-cache
report-image-cache-inventory
```

`replicate-app-image` should:

1. claim the task,
2. ask the local image-cache management API to replicate from the selected
   source,
3. verify the local cache serves the manifest/digest,
4. report a replica with `present` status and size,
5. complete or fail the task with detailed logs.

`verify-image-cache` should refresh leases for images already present. If an
image is missing, it should report `missing`, not silently drop the row.

## 11. Helm and configuration

Add chart values without changing the default behavior first:

```yaml
imageStore:
  enabled: false
  mode: bundled-registry
  minReplicas: 2
  targetReplicas: 2
  requireCrossNodeReplicas: true
  verifyInterval: 10m
  replicaLeaseTTL: 30m
  schedulerInterval: 30s
  prune:
    enabled: true
    maxDeleteBytesPerRun: 10Gi

registry:
  enabled: true
```

`fugue-image-cache` enforces conservative built-in disk pressure defaults:
high watermark 55%, low watermark 45%, minimum free space 50GiB, and a 10GiB
per-run delete budget. On small disks, the effective minimum free space is
clamped to the free space reachable at the low watermark, so a node never keeps
trying to satisfy an impossible reserve that is larger than the disk can
provide. Those defaults avoid changing the node-local image-cache DaemonSet
rendered pod spec during ordinary control-plane releases; override environment
variables should only be introduced through an explicit node-local build-plane
release.

Rollout modes:

1. `bundled-registry`: current behavior.
2. `distributed-with-registry-fallback`: new index, replicas, and scheduler are
   active, but image-cache can still hydrate from the central registry.
3. `distributed`: central registry is not used as image origin.
4. `external-registry`: central origin is externally configured, but
   distributed replicas still serve runtime pulls.

The chart refuses `registry.enabled=false` unless `imageStore.mode` is
`distributed`, and refuses strict distributed mode unless image-cache is
enabled. Production HA values set image-cache as the runtime pull and storage
surface, with central upstream fallback disabled.

## 12. Migration and rollback

### Migration path for rollback-capable releases

1. Deploy schema and API additions while keeping old behavior.
2. Backfill or lazily derive distributed image evidence from existing
   `fugue_image_locations`.
3. Run image-cache inventory reporting in observe-only mode.
4. Enable replica scheduler and compare desired vs actual.
5. Keep `distributed-with-registry-fallback` available as a rollback mode.
6. Require min replicas for new images.
7. Switch strict production values to `distributed` with
   `registry.enabled=false`.
8. Verify rendered output and controller tests show no central registry
   Deployment, upstream fallback, inspect, delete, GC, maintenance, or
   registry-based billing role.

### Rollback

Rollback must be simple:

- switch `imageStore.mode` back to `distributed-with-registry-fallback` or
  `bundled-registry`
- keep `registryPushBase` available during the transition
- keep old `/v1/image-locations` behavior until the new model has been stable
  across multiple releases
- never delete central registry data in the same release that first enables
  distributed mode

## 13. Failure scenarios

| Scenario | Required behavior |
| --- | --- |
| Target node cache miss | Pull from local cache triggers peer lookup; deploy path should already have scheduled target replication. |
| Source peer disappears mid-copy | Task fails, source replica is marked stale/failed, scheduler chooses another source. |
| Only one replica remains | Scheduler raises priority and recreates replicas before pruning anything. |
| All replicas lost, source rebuildable | Queue rebuild using existing rebuild flow. |
| All replicas lost, source not rebuildable | Mark image `lost`; fail deploy with explicit recovery instructions. |
| Control plane unavailable | Existing workloads keep running; local pins prevent local prune; no new distributed GC. |
| Cache disk pressure | Cache reports pressure; scheduler avoids it; prune removes unpinned excess replicas only. |
| Tenant tries to access another tenant image | Management and report APIs reject cross-tenant operations. |
| Strict registryless mode misconfigured | Helm validation blocks invalid `registry.enabled=false` and missing image-cache combinations. |

## 14. Observability

Add metrics and logs for:

- image count by lifecycle state
- replica count by status, node, region, tenant
- under-replicated images
- lost images
- replication task latency and failure reason
- cache disk used/free bytes
- bytes copied by source and target
- cache verify failures
- prune dry-run bytes and deleted bytes
- registry fallback reads in rollback/fallback modes

Useful CLI surfaces:

```text
fugue image ls
fugue image show <image>
fugue image replicas <image>
fugue image repair <image>
fugue image prune --dry-run
fugue node cache status
```

## 15. Detailed TODO list

### P0: Baseline and safety

- [x] Add this design to the architecture docs and link it from any future
      registry/image-cache implementation PR.
- [x] Add tests that document current central registry plus image-cache
      behavior before changing it.
- [x] Add an image-cache inventory dry-run command or endpoint that does not
      mutate state.
- [x] Add metrics for current `image_locations` count by status and age.
- [x] Add a stale-location detector that reports, but does not delete, old
      `present` locations.

### P1: Store schema and model

- [x] Add `Image`, `ImageAlias`, `ImageReplica`, `ImagePin`, and
      `ImageReplicationTask` model structs.
- [x] Add Postgres bootstrap DDL in `internal/store/postgres.go`.
- [x] Add file-store support for tests and development mode.
- [x] Add store methods for upsert/list/get images, aliases, replicas, pins,
      and replication tasks.
- [x] Add compatibility methods so current image-location handlers can read
      from replica state.
- [x] Add store tests for replica uniqueness, stale lease queries, pin
      expiration, and task dedupe.

### P2: OpenAPI and handlers

- [x] Update `openapi/openapi.yaml` with image, replica, pin, and replication
      task endpoints.
- [x] Run `make generate-openapi`.
- [x] Implement platform-admin image inventory handlers.
- [x] Implement node-updater replica report handlers.
- [x] Preserve `/v1/image-locations` and add tests that old clients still work.
- [x] Update `fugue-web` OpenAPI snapshot and generated client after backend
      contract changes.

### P3: Image-cache management API

- [x] Add an authenticated management router separate from `/v2/`.
- [x] Implement `/health`, `/inventory`, and `/verify`.
- [x] Implement digest-first manifest verification, including manifest lists.
- [x] Implement `/replicate` using the existing hydrate/copy code paths.
- [x] Implement local pin storage under the image-cache host path.
- [x] Implement conservative prune dry run and delete paths.
- [x] Add tests for peer copy, manifest persistence, auth rejection, and local
      pin protection.

### P4: Node-updater task support

- [x] Add task types for `replicate-app-image`, `verify-image-cache`,
      `prune-image-cache`, and `report-image-cache-inventory`.
- [x] Extend capability reporting so old node updaters are not assigned new
      task types.
- [x] Implement task shell functions or move task execution into a maintainable
      helper binary.
- [x] Report structured replica status, size bytes, digest, and error reason.
- [x] Add tests for task delivery, capability gating, duplicate task handling,
      and failure reporting.

### P5: Replication scheduler

- [x] Add controller config for min replicas, target replicas, verify interval,
      and lease TTL.
- [x] Implement stale replica detection.
- [x] Implement source replica selection.
- [x] Implement target node selection using node health, failure domain, target
      runtime, and cache capacity.
- [x] Create replication tasks idempotently.
- [x] Verify completed tasks before counting replicas healthy.
- [x] Add metrics and logs for under-replication and task outcomes.

### P6: Import/build integration

- [x] On import/build success, create the canonical image record and aliases.
- [x] Record the builder node cache as the first verified replica.
- [x] Pin current deploy and rollback-window images.
- [x] Change import success criteria in distributed mode from remote registry
      inspect to replica quorum.
- [x] Keep central registry fallback available in rollback/fallback modes.
- [x] Add tests for Docker image import, GitHub build, upload import, and
      node-local builder outputs.

### P7: Deploy integration

- [x] Resolve app runtime image refs to canonical image records.
- [x] Require a healthy source replica before deploy.
- [x] Require or schedule a target-node replica before workload start.
- [x] Keep rebuild behavior for rebuildable sources when all replicas are lost.
- [x] Add explicit lost-image errors for non-rebuildable sources.
- [x] Add tests for target miss, peer repair, all-replica-loss, and rollback
      image availability.

### P8: Retention, GC, and billing

- [x] Replace remote delete as the primary distributed-mode cleanup mechanism.
- [x] Add distributed image prune tasks gated by pins and min replicas.
- [x] Keep registry GC only for bundled-registry and fallback modes.
- [x] Add storage accounting based on unique image bytes and replica bytes.
- [x] Add dry-run output for prune decisions.
- [x] Add tests that current deploy, rollback window, and shared images are not
      pruned.
- [x] Add node-local image-cache disk watermarks and per-run delete budgets so
      excess-replica prune only becomes destructive under cache disk pressure.

### P9: Helm and rollout gates

- [x] Add `imageStore` chart values with default `bundled-registry`.
- [x] Add environment variables for scheduler and image-cache management auth.
- [x] Add Helm validation that blocks `registry.enabled=false` outside strict
      distributed mode and blocks strict distributed mode without image-cache.
- [x] Add a rollback-friendly mode switch back to registry fallback.
- [x] Document operator rollout and rollback steps.

### P10: Registryless cutover

- [x] Render production HA in strict distributed mode with
      `registry.enabled=false`.
- [x] Verify the production HA chart renders no bundled registry Deployment,
      Service, GC CronJob, janitor CronJob, or registry GC Lease name.
- [x] Disable image-cache central upstream fallback in strict distributed mode.
- [x] Disable controller registry inspect, remote delete, registry GC, registry
      maintenance status, and registry-based billing paths in strict
      distributed mode.
- [x] Keep legacy `image_locations` as node-local compatibility evidence without
      falling back to central registry inspection.
- [x] Add tests that fail if strict distributed deploy/import paths call central
      registry inspection or digest resolution.
- [x] Keep rollback modes available by setting `imageStore.mode` back to
      `distributed-with-registry-fallback` or `bundled-registry` in a future
      release.

## 16. Recommended implementation order

The safest sequence is:

1. Strengthen evidence: inventory, verification, metrics, stale detection.
2. Add durable control-plane image objects, replicas, pins, and tasks.
3. Make image-cache manageable and authenticated.
4. Add the replication scheduler while fallback remains available for rollback
   modes.
5. Move import/deploy success gates to replica quorum.
6. Replace remote registry GC with distributed pin-aware prune.
7. Disable the central registry in strict distributed production values and
   verify rendered output plus controller tests show it is not used.

The important boundary is that image-cache must act as a storage system with
explicit durability guarantees in strict distributed mode. Central registry
fallback remains available only when a future rollback release explicitly
switches back to a fallback mode.
