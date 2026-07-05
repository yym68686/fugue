# Fugue image-cache and LocalPV storage recovery plan

This document captures the production disk-pressure investigation and the
implementation plan for two separate recovery tracks:

1. node-local image-cache orphan cleanup, which can become regular automated
   control-plane behavior after observe-only and dry-run rollout;
2. LVM LocalPV backing-file recovery, which must remain an explicit
   per-node maintenance task and must not be folded into automatic GC.

The plan is based on read-only production investigation performed on
2026-07-02. The 2026-07-06 follow-up for distributed image-store retention,
replica-policy, prune-plan explainability, and unreferenced-blob cleanup is
tracked in `docs/distributed-image-store-retention-prune-fix-plan.md`. It does
not prescribe manual production hotfixes. Any change that affects the control
plane, node-updater, image-cache, cluster bootstrap, Helm values, registry
behavior, runtime routing, or platform traffic rules must be released through
the normal Fugue repository path: commit to `main`, push, then let
`.github/workflows/deploy-control-plane.yml` update the remote control plane.

## 1. Scope

This plan addresses the disk pressure seen on Fugue production nodes during
the previous 24 hour investigation window.

In scope:

- image-cache inventory reporting and control-plane retention decisions;
- image-cache manifest-level orphan pruning with dry-run and deletion budgets;
- node-updater task support for cache inventory and prune execution;
- explicit LVM LocalPV inventory and decommission tasks;
- CLI surfaces for operator review, dry-run, and approval;
- tests, metrics, audit events, and rollout gates.

Out of scope:

- manual SSH cleanup as the durable fix;
- deleting LVM backing files directly from a shell;
- automatic LVM LocalPV GC;
- shrinking active LocalPV volume groups in-place;
- weakening image replica, pin, or rollback protection;
- app-specific cleanup exceptions keyed to production app names.

## 2. Investigation facts

Kubelet reported `ImageGCFailed` and `FreeDiskSpaceFailed` on several nodes.
The direct cause was not a large CRI image set. The CRI image totals were small
relative to root filesystem pressure:

| Node | Role | CRI image bytes observed |
| --- | --- | ---: |
| `fortedrape8` | Hong Kong agent | about 1.9 GiB |
| `ns101351` | US agent | about 6.8 GiB |
| `vps-d6d20fa1` | control plane | about 1.5 GiB |

The root cause is that the largest consumers are outside kubelet image GC:

- `/var/lib/fugue/image-cache/registry`
- `/var/lib/fugue/lvm-localpv/fugue-vg.img`
- local-path PV data under the K3s data path
- containerd writable snapshots
- journald

Kubelet image GC can only remove eligible CRI images. It cannot free
image-cache manifests, local registry blobs, LVM backing files, local-path PV
contents, or logs. Therefore kubelet repeatedly attempted to reclaim disk and
found too few eligible image bytes.

### 2.1 Node disk facts

| Host | Cluster node | Root usage | Main observed consumers |
| --- | --- | ---: | --- |
| `alicehk2` | `fortedrape8` | 35 GiB / 40 GiB, about 93% | `/var/lib/fugue` about 15 GiB: image-cache about 6.8 GiB, LVM LocalPV about 8.1 GiB; K3s storage about 6.5 GiB; containerd about 7.5 GiB; journal about 3.9 GiB |
| `ovhuseast` | `ns101351` | 329 GiB / 410 GiB, about 85% | `/var/lib/fugue` about 260 GiB: image-cache about 183 GiB, LVM LocalPV about 76 GiB to 96 GiB apparent with 66 GiB active LVs and 30 GiB VG free; K3s storage about 33 GiB; containerd about 33 GiB; journal about 1.8 GiB |
| `ovhvpsuswest` | `vps-d6d20fa1` | 54 GiB / 74 GiB, about 77% | `/var/lib/fugue` about 22 GiB: LVM LocalPV about 17 GiB, image-cache about 3.4 GiB, Postgres about 1.2 GiB, old registry import backup about 726 MiB; K3s storage about 8.8 GiB; containerd about 5.1 GiB; journal about 2.3 GiB |
| `ovhvps` | edge and DNS | 56 GiB / 74 GiB, about 79% | `/var/lib/fugue` about 37 GiB: LVM LocalPV about 33 GiB, edge data about 4.7 GiB, image-cache tiny; containerd about 16 GiB; journal about 1.5 GiB |
| `ovhvpseu` | edge and DNS | 55 GiB / 74 GiB, about 78% | `/var/lib/fugue` about 37 GiB: LVM LocalPV about 33 GiB, edge data about 4.3 GiB, image-cache tiny; containerd about 16 GiB; journal about 1.3 GiB |
| `netcup` | Germany agent | 70 GiB / 251 GiB, about 30% | image-cache about 17 GiB, LVM LocalPV about 24 GiB active, K3s storage about 14 GiB, journal about 3.9 GiB |
| `bwg` | edge only | 5.5 GiB / 19 GiB, about 32% | no current pressure |

### 2.2 LVM LocalPV facts

Empty LVM LocalPV volume groups were observed on these nodes:

| Host | Cluster node | Backing size | LV count | Production conclusion |
| --- | --- | ---: | ---: | --- |
| `alicehk2` | `fortedrape8` | about 8 GiB | 0 | Theoretical reclaim candidate only through explicit decommission |
| `ovhvpsuswest` | `vps-d6d20fa1` | about 16 GiB | 0 | Theoretical reclaim candidate only through explicit decommission |
| `ovhvps` | edge and DNS | about 32 GiB | 0 | Theoretical reclaim candidate only through explicit decommission |
| `ovhvpseu` | edge and DNS | about 32 GiB | 0 | Theoretical reclaim candidate only through explicit decommission |

Active LVM LocalPV volume groups were observed on these nodes:

| Host | Cluster node | VG state | Production conclusion |
| --- | --- | --- | --- |
| `ovhuseast` | `ns101351` | about 96 GiB PV, 66 GiB active LVs, about 30 GiB VG free | Not eligible for decommission. Requires future migration or shrink strategy. |
| `netcup` | Germany agent | about 32 GiB VG, one 30 GiB active LV, about 2 GiB VG free | Not eligible for decommission. Requires future migration or shrink strategy. |

The active `ns101351` LVs map to current database PVCs:

| PVC LV size | Workload reference |
| ---: | --- |
| 20 GiB | `uni-api-web-api-db-postgres-3` |
| 20 GiB | `morlane-river-postgres-postgres-1` |
| 1 GiB | `oaix-solar-postgres-postgres-1` |
| 5 GiB | `gaokao-db-postgres-1` |
| 20 GiB | `review00-db-postgres-2` |

The active `netcup` LV maps to:

| PVC LV size | Workload reference |
| ---: | --- |
| 30 GiB | `fg-tenant-1774495361-ceff207a44cc/app-1782466554-f459616ec861-workspace-mv-43e5b9446` |

These active LVs must not be removed by any automatic cleanup path.

### 2.3 Image-cache facts

Image-cache inventory at investigation time:

| Node | Manifests | Approx cache bytes | Existing local image-cache pins | Existing prune dry-run unreferenced blob bytes |
| --- | ---: | ---: | ---: | ---: |
| `fortedrape8` | 157 | about 7.30 GB | 0 | about 534,686,407 |
| `ns101351` | 2,091 | about 195.46 GB | 0 | about 6,970,777,976 |
| `netcup` | 200 | about 17.86 GB | 0 | 0 |
| `vps-d6d20fa1` | 18 | about 3.61 GB | 0 | about 696,189,334 |
| `ovhvps` | 33 | about 101 KiB | 0 | 0 |
| `ovhvpseu` | 31 | about 101 KiB | 0 | 0 |
| `bwg` | 0 | 0 | 0 | 0 |

The existing image-cache prune endpoint can identify unreferenced blobs, but
it does not solve the main production pressure because most bytes are still
referenced by local manifests. Fugue needs manifest-level orphan pruning:
select stale local manifests first, then remove only blobs no longer referenced
by any remaining manifest or protected local pin.

### 2.4 Simulated protection result

The proposed new image-cache orphan strategy was simulated against production
state without deleting anything.

Control-plane facts:

- `/v1/images` returned 186 images.
- `fugue_image_pins` had 416 rows:
  - 208 `current_deploy`
  - 208 `rollback_window`
- Kubernetes extraction found 347 workload objects and 80 normalized workload
  image references.
- 76 images were in `available` lifecycle state.
- 33 image references were tied to active pending or running tasks.
- The combined protected set contained 248 image references.
- 110 API image references were in `lost` state.

The simulated orphan-prune protected set included:

- current Kubernetes workload image references;
- control-plane images with `lifecycle_state=available`;
- active image pins such as `current_deploy`, `rollback_window`,
  `import_result`, `user_pin`, `retention`, and `replication_seed`;
- image references attached to pending or running build, deploy,
  image-replication, and node-updater image tasks;
- local manifests younger than 24 hours;
- healthy present replicas that are at or below minimum replica count.

Candidate criteria:

- local manifest is not in the control-plane API, or the image is `lost`, or
  the local replica is stale/failed and has no active pin;
- candidate is not in the protected set;
- candidate local manifest age is greater than the configured grace period,
  initially 24 hours.

Simulation result:

| Node | Candidate manifests | Protected candidate hits |
| --- | ---: | ---: |
| `fortedrape8` | 103 | 0 |
| `ns101351` | 1,876 | 0 |
| `netcup` | 155 | 0 |
| `vps-d6d20fa1` | 17 | 0 |
| `ovhvps` | 0 | 0 |
| `ovhvpseu` | 0 | 0 |
| `bwg` | 0 | 0 |

Conclusion: under this protection model, the proposed image-cache cleanup would
not delete currently protected business images, current workload images,
available images, active pins, or recent manifests. Exact freed bytes must be
computed by the implemented image-cache batch prune dry-run because shared
layers must only count once after selected manifests are removed.

## 3. Safety model

The plan intentionally separates image-cache orphan cleanup from LVM LocalPV
recovery because they have different risk profiles.

### 3.1 Image-cache cleanup safety

Image-cache cleanup can become automated because it deletes only local cache
entries after the control plane proves they are unprotected. A deleted cache
entry can be rebuilt or re-replicated when the image still exists in the
durable image model and has healthy source replicas.

Required safety gates:

- dry-run must be available and must return selected manifests and planned
  unique blob bytes before deletion;
- `allow_delete=false` must never delete bytes;
- destructive runs require `allow_delete=true`;
- destructive runs must have per-node delete budgets and per-run target
  limits;
- current workload refs, available images, active pins, active tasks, recent
  manifests, and minimum replicas must be protected;
- shared blobs must only be deleted when no remaining manifest or local pin
  references them;
- every delete must produce an audit event and node-level task log;
- observe-only and dry-run modes must run in production before delete mode.

### 3.2 LVM LocalPV safety

LVM LocalPV recovery must not be automated GC. A backing file can contain
stateful PVC data. Deleting the wrong file can destroy app data. Even an empty
VG should be removed only through a controlled decommission path because the
loop device, service, Kubernetes node role, and bound PV state must agree.

Required decommission gates:

- node inventory says `lv_count == 0`;
- no Kubernetes PV is bound to the target node and the Fugue VG;
- node role policy says this node should not host LocalPV;
- backing file path exactly matches Fugue's configured localpv image path;
- loop device is bound only to that backing file;
- dry-run succeeds immediately before apply;
- the task request includes `allow_delete=true`;
- the task records audit evidence and final freed bytes.

Nodes with active LVs, such as `ns101351` and `netcup` at investigation time,
are not eligible for this task. They need a separate migration, backup, restore,
or resize plan.

## 4. Track A: image-cache orphan cleanup

### 4.1 Control-plane inventory model

Add authoritative node-local cache inventory storage. These names are
descriptive; final migration names may follow existing store conventions.

```text
fugue_image_cache_nodes
  node_id
  cluster_node_name
  runtime_id
  cache_endpoint
  store_path
  filesystem_total_bytes
  filesystem_free_bytes
  filesystem_used_percent
  cache_bytes
  manifest_count
  blob_count
  pin_count
  observed_at
  reported_by_updater_id
  status
  last_error

fugue_image_cache_manifests
  node_id
  image_ref
  repo
  target
  digest
  media_type
  manifest_size_bytes
  total_blob_bytes
  unique_blob_bytes_observed
  created_at_observed
  last_seen_at
  pinned_locally
  present

fugue_image_cache_prune_plans
  id
  node_id
  mode
  candidate_manifest_count
  protected_manifest_count
  planned_delete_bytes
  max_delete_bytes
  min_manifest_age
  protection_summary_json
  candidate_summary_json
  created_at
  executed_at
  status
  error
```

Inventory rows should be upserted by `(node_id, repo, target, digest)` and
stale rows should be ignored by the planner when `last_seen_at` is older than
the configured inventory TTL.

### 4.2 OpenAPI endpoints

All API additions must start in `openapi/openapi.yaml`, followed by
`make generate-openapi`.

Add node-updater endpoint:

```text
POST /v1/node-updater/image-cache/inventory
```

Purpose: node-updater reports local image-cache inventory to the control plane.

Minimum request fields:

```text
node_id
cluster_node_name
runtime_id
cache_endpoint
store_path
filesystem_total_bytes
filesystem_free_bytes
cache_bytes
manifests[]
observed_at
```

Add admin endpoints:

```text
GET  /v1/admin/image-cache/inventory
GET  /v1/admin/image-cache/prune-plan
POST /v1/admin/image-cache/prune-plan
```

The `GET` plan endpoint returns current computed candidates without scheduling
work. The `POST` endpoint can create dry-run or delete-mode node-updater tasks,
subject to mode and permission gates.

### 4.3 Node-updater inventory reporting

Change `report-image-cache-inventory` from endpoint reachability logging to
real inventory submission:

1. call the local image-cache management API at
   `/fugue/cache/v1/inventory`;
2. normalize node identity and cache endpoint;
3. POST inventory to `/v1/node-updater/image-cache/inventory`;
4. chunk large manifest lists, initially 500 manifests per request;
5. retry bounded failures and complete the node-updater task with structured
   evidence.

The task must remain safe when image-cache is unavailable: it should report
failure and should not block unrelated node-updater tasks.

### 4.4 Planner

Add controller jobs:

```text
scheduleImageCacheInventoryReports
scheduleOrphanImageCachePrune
```

The inventory scheduler creates node-updater tasks at a configurable interval.

The prune scheduler computes candidates from:

- latest image-cache inventory;
- `fugue_images`;
- image aliases and refs;
- active image pins;
- current workload image refs;
- pending and running image-affecting tasks;
- current replica health and minimum replica policy;
- local manifest age.

The scheduler must have these modes:

```text
observe
dry-run
delete
```

`observe` persists plans and metrics only. `dry-run` creates prune tasks with
`allow_delete=false`. `delete` creates destructive prune tasks only after
configuration and task-level gates allow it.

### 4.5 Protection rules

Do not select a manifest if any of these conditions are true:

- it is referenced by a current Kubernetes workload;
- the corresponding control-plane image is `available`;
- it has any active pin;
- it is referenced by a pending or running build, deploy, image-replication, or
  image-cache/node-updater task;
- its observed local manifest age is less than the grace period;
- pruning it would reduce healthy present replicas below minimum replica count;
- its local image-cache reports an active local pin;
- the node inventory is stale or incomplete.

The planner must return skip reasons, not only candidates. Operators need to
see why a large manifest was or was not selected.

### 4.6 Candidate rules

A manifest may be selected only when all protection rules pass and at least one
candidate reason is true:

- no matching image or alias exists in the control plane;
- the matching image has `lifecycle_state=lost`;
- the matching replica is stale, failed, or missing and there is no active pin;
- the manifest belongs to a deleted app/image generation and no current alias
  points to it.

Candidates should be sorted conservatively:

1. oldest orphan manifests;
2. lost images without pins;
3. stale or failed replicas without pins;
4. largest unique-byte estimates within the per-node budget.

### 4.7 Image-cache batch prune API

Extend `cmd/fugue-image-cache` prune support from single target to batch
targets:

```json
{
  "dry_run": true,
  "allow_delete": false,
  "targets": [
    {
      "repo": "fugue-apps/specforge",
      "target": "upload-...",
      "digest": "sha256:..."
    }
  ],
  "max_delete_bytes": 104857600,
  "min_manifest_age": "24h"
}
```

Response fields:

```text
dry_run
selected_manifests[]
skipped_manifests[]
planned_delete_bytes
planned_delete_blobs[]
deleted_manifests[]
deleted_blobs[]
remaining_references[]
budget_exhausted
started_at
completed_at
```

Delete behavior:

- first delete selected manifest references;
- recompute all remaining blob references from local manifests and pins;
- delete only blobs that have zero remaining references;
- stop at `max_delete_bytes`;
- never delete if `dry_run=true` or `allow_delete=false`;
- return detailed skip reasons for age, local pin, missing target, shared blob,
  budget, and active operation.

### 4.8 Configuration

Add configuration with production automation defaults:

```text
FUGUE_IMAGE_CACHE_INVENTORY_ENABLED=true
FUGUE_IMAGE_CACHE_INVENTORY_INTERVAL=30m
FUGUE_IMAGE_CACHE_INVENTORY_TTL=2h
FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MODE=delete
FUGUE_IMAGE_STORE_ORPHAN_PRUNE_GRACE_PERIOD=24h
FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MAX_TARGETS_PER_NODE=50
FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MAX_DELETE_BYTES_PER_NODE=104857600
FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MIN_REPLICA_COUNT=1
```

Production canary and rollout coverage has passed on `v2202605354515455529`,
`ns101351`, and the full-node observe pass. The formal automation default is
low-speed `delete`: each node gets a small per-run delete budget, every delete
task must be followed by a post-prune inventory report, and automation halts
before issuing delete tasks if a previous controller orphan-prune task failed,
if a prune task is already pending/running on that node, or if any plan contains
candidate reasons outside the default orphan set (`missing_control_plane_image`,
`lost_image`).

### 4.9 CLI surface

Add admin CLI commands:

```text
fugue admin image-cache inventory
fugue admin image-cache inventory --node <node>
fugue admin image-cache prune-plan
fugue admin image-cache prune-plan --node <node>
fugue admin image-cache prune --node <node> --dry-run
fugue admin image-cache prune --node <node> --allow-delete
```

Default output should show candidate counts, planned unique bytes, protection
summary, skipped reasons, inventory age, and task id. JSON output should expose
the full plan.

## 5. Track B: LVM LocalPV recovery

### 5.1 Inventory

Add a node-updater task:

```text
report-lvm-localpv-inventory
```

Inventory fields:

```text
node_id
cluster_node_name
node_roles[]
vg_name
image_path
image_size_bytes
loop_device
loop_backing_file
pv_size_bytes
pv_free_bytes
lv_count
lv_names[]
active_lv_count
bound_pv_count
bound_pvc_refs[]
safe_to_decommission
unsafe_reasons[]
observed_at
```

The task may use read-only commands such as:

```text
lvs --reportformat json
vgs --reportformat json
pvs --reportformat json
losetup --json
findmnt --json
kubectl get pv -o json
kubectl get pvc -A -o json
```

The implementation must not rely on production node names. It must derive
state from VG metadata, loop device backing files, Kubernetes PV/PVC metadata,
and node role labels.

### 5.2 Control-plane policy

Add a policy layer that decides whether LocalPV should exist on a node:

- storage-enabled agents may have LocalPV;
- nodes with bound LocalPV PVCs must keep LocalPV;
- control-plane-only nodes should not get new preallocated LocalPV unless
  explicitly configured;
- edge-only and DNS-only nodes should not get new preallocated LocalPV unless
  explicitly configured.

This policy should influence new node preparation. It must not retroactively
delete existing backing files without explicit maintenance approval.

### 5.3 Decommission task

Add a node-updater maintenance task:

```text
decommission-lvm-localpv
```

Request fields:

```text
node_id
vg_name
image_path
dry_run
allow_delete
expected_image_size_bytes
expected_lv_count
expected_bound_pv_count
reason
```

Dry-run behavior:

1. read current VG, PV, LV, loop, mount, PV, and PVC state;
2. evaluate all safety gates;
3. report commands that would be run;
4. report expected freed bytes;
5. do not stop services, detach loop devices, or delete files.

Apply behavior with `allow_delete=true`:

1. re-run the same preflight checks immediately before mutation;
2. stop only `fugue-lvm-localpv-loop.service`;
3. detach only the loop device whose backing file exactly matches `image_path`;
4. disable only the Fugue LocalPV loop service for that image path;
5. delete only the verified backing file;
6. report final freed bytes and post-state inventory;
7. emit an audit event.

The task must fail closed if any preflight value differs from the request's
expected values.

### 5.4 CLI surface

Add admin CLI commands:

```text
fugue admin cluster node localpv ls
fugue admin cluster node localpv show <node>
fugue admin node-updater task create \
  --type decommission-lvm-localpv \
  --node <node> \
  --dry-run
fugue admin node-updater task create \
  --type decommission-lvm-localpv \
  --node <node> \
  --allow-delete \
  --expected-lv-count 0 \
  --expected-bound-pv-count 0
```

Default CLI output must make active LVs and bound PVCs impossible to miss.
Nodes like `ns101351` and `netcup`, when they have active LVs, must display as
`not eligible`.

### 5.5 Node preparation policy

Update `scripts/prepare_fugue_lvm_localpv_node.sh` and the node onboarding path
so LocalPV backing files are not preallocated on nodes that do not need them.

Required behavior:

- default edge-only and DNS-only nodes to no LocalPV preallocation;
- default control-plane-only nodes to no LocalPV preallocation unless
  explicitly configured;
- storage-capable agents may opt into LocalPV with an explicit size;
- existing nodes are not mutated by this policy change;
- rendered install/update manifests should show whether LocalPV will be
  prepared and why.

Operator opt-in for a new storage-capable node is explicit:

```bash
scripts/prepare_fugue_lvm_localpv_node.sh \
  --node-role storage-agent \
  --size-gib 80
```

Edge-only, DNS-only, and control-plane-only nodes must not run this preparation
unless an operator intentionally passes `--allow-localpv` for a documented
maintenance exception.

## 6. Observability and audit

Add metrics:

```text
fugue_image_cache_inventory_age_seconds
fugue_image_cache_manifest_count
fugue_image_cache_candidate_manifest_count
fugue_image_cache_prune_planned_bytes
fugue_image_cache_prune_deleted_bytes
fugue_image_cache_prune_skipped_count
fugue_localpv_inventory_age_seconds
fugue_localpv_backing_file_bytes
fugue_localpv_active_lv_count
fugue_localpv_bound_pv_count
fugue_localpv_decommission_eligible
```

Add diagnosis events:

- `image_cache_inventory_stale`
- `image_cache_orphan_prune_plan_created`
- `image_cache_orphan_prune_dry_run_completed`
- `image_cache_orphan_prune_delete_completed`
- `localpv_inventory_reported`
- `localpv_decommission_refused`
- `localpv_decommission_dry_run_completed`
- `localpv_decommission_completed`

Audit logs must include actor, node, task id, dry-run flag, allow-delete flag,
planned bytes, deleted bytes, skipped reasons, and preflight evidence.

## 7. Rollout plan

### PR 1: read-only image-cache inventory

- Add OpenAPI schema and generated artifacts.
- Add store tables and store methods.
- Implement node-updater inventory POST handler.
- Implement admin inventory read endpoint.
- Change node-updater `report-image-cache-inventory` to submit real inventory.
- Add tests.
- Deploy in production with no pruning behavior change.

### PR 2: image-cache prune planner in observe mode

- Add control-plane planner.
- Persist prune plans.
- Add admin `prune-plan` endpoint and CLI read commands.
- Add metrics and diagnosis events.
- Run production in observe mode until plans match expected candidates.

### PR 3: image-cache batch prune dry-run

- Extend image-cache prune API for batch targets.
- Add node-updater dry-run task execution.
- Add CLI dry-run command.
- Add shared-layer and local-pin tests.
- Run production dry-run on selected nodes and compare planned bytes with
  filesystem deltas of zero.

### PR 4: limited image-cache delete mode

- Enable delete mode only behind explicit config.
- Limit to one canary node first.
- Use small per-node budgets.
- Verify no protected image refs are selected.
- Expand gradually after dry-run and delete metrics are stable.

### PR 5: read-only LVM LocalPV inventory

- Add LocalPV inventory task and control-plane storage.
- Add admin CLI list/show commands.
- Add policy classification for eligible and not eligible nodes.
- Deploy without any decommission task.

### PR 6: LVM LocalPV dry-run decommission

- Add `decommission-lvm-localpv` task in dry-run only.
- Require zero active LVs and zero bound PVs.
- Add tests for active-LV refusal and empty-VG eligibility.
- Run dry-run on candidate nodes only.

### PR 7: explicit LVM LocalPV apply

- Add apply mode with `allow_delete=true`.
- Keep it unavailable from automatic controllers.
- Require operator-created task and expected preflight values.
- Use only for nodes confirmed by immediately preceding dry-run.

### PR 8: node preparation policy

- Stop preallocating LocalPV on nodes that do not need it.
- Preserve existing nodes.
- Add install/update manifest tests and documentation.

## 8. Test plan

### Image-cache unit tests

- batch prune dry-run never deletes bytes;
- `allow_delete=false` never deletes bytes;
- current workload refs are protected;
- active pins are protected;
- pending and running image tasks are protected;
- manifests younger than the grace period are protected;
- shared blobs are retained while any remaining manifest references them;
- delete budget stops deletion before exceeding the configured maximum;
- stale inventory is ignored by the planner;
- local pin metadata blocks local deletion even if the control plane misses it.

### Controller and store tests

- inventory upsert by node, repo, target, and digest;
- stale inventory TTL behavior;
- orphan candidate planning;
- lost image planning;
- available image protection;
- minimum replica protection;
- task deduplication;
- observe mode creates no node-updater prune task;
- dry-run mode creates only `allow_delete=false` tasks;
- delete mode requires explicit configuration.

### Node-updater tests

- image-cache inventory report success;
- inventory report chunking;
- image-cache unavailable failure handling;
- batch prune task dry-run;
- batch prune task delete mode capability gating;
- LocalPV inventory parsing from `lvs`, `vgs`, `pvs`, and `losetup`;
- LocalPV decommission refuses active LVs;
- LocalPV decommission refuses bound PVs;
- LocalPV decommission refuses path mismatch;
- LocalPV dry-run performs no mutation.

### OpenAPI and CLI tests

- `make generate-openapi` has no drift;
- `make test` passes;
- CLI JSON output is stable;
- CLI table output highlights `not eligible` LocalPV nodes;
- generated frontend contract is synchronized if any frontend-consumed endpoint
  changes.

### Release safety tests

- Helm values render image-cache inventory and prune config correctly;
- control-plane release safety tests classify image-cache changes as
  node-local build-plane changes when needed;
- strict distributed image-store tests still protect pins and replicas;
- node preparation tests prove edge-only and DNS-only nodes do not receive
  LocalPV preallocation by default.

## 9. TODO list

Checked items are implemented and verified in this code change. Production
dry-run/delete rollout items remain unchecked until they are deliberately run as
operator maintenance steps; this code change keeps production defaults in
`observe` mode.

### P0: lock scope and evidence

- [ ] Link this document from implementation PR descriptions that touch
      image-cache cleanup, LocalPV decommission, node-updater maintenance, or
      node preparation.
- [x] Preserve the production investigation facts in an internal incident note
      without secrets.
- [x] Add a short README or docs pointer from the distributed image-store plan
      to this recovery plan.
- [x] Confirm current production protected-set simulation immediately before
      enabling dry-run tasks.

### P1: image-cache inventory API and store

- [x] Add `POST /v1/node-updater/image-cache/inventory` to
      `openapi/openapi.yaml`.
- [x] Add admin image-cache inventory and prune-plan endpoints to
      `openapi/openapi.yaml`.
- [x] Run `make generate-openapi`.
- [x] Add image-cache inventory tables and indexes.
- [x] Add store methods for upsert, list, stale filtering, and plan
      persistence.
- [x] Add handler tests for auth, validation, chunking, stale inventory, and
      malformed reports.

### P2: node-updater image-cache inventory

- [x] Change `report-image-cache-inventory` to fetch local inventory and submit
      it to the control plane.
- [x] Add chunking for large manifest lists.
- [x] Add task logs that include manifest count, cache bytes, filesystem free
      bytes, and inventory age.
- [x] Add capability gating so old updaters are not assigned unsupported
      inventory or prune tasks.
- [x] Prioritize inventory report tasks ahead of replication backlog while
      keeping updater self-upgrade as the highest-priority task.
- [x] Deduplicate scheduled inventory report tasks while an equivalent pending
      or running report already exists.
- [x] Fail `report-image-cache-inventory` tasks when no inventory chunks were
      posted or the posted chunk count differs from the generated chunk count.
- [x] Return empty JSON arrays, not `null`, for empty image-cache and LocalPV
      admin inventory responses.

### P3: image-cache orphan planner

- [x] Implement protected-set construction from workloads, available images,
      pins, active tasks, recent manifests, local pins, and min replicas.
- [x] Implement candidate selection for missing API refs, lost images, stale
      replicas, and deleted generations.
- [x] Persist observe-only plans.
- [x] Add skip reasons and protection summaries.
- [x] Add metrics and audit events.
- [x] Add admin CLI plan output.

### P4: image-cache batch prune dry-run

- [x] Extend `/fugue/cache/v1/prune` to accept batch targets.
- [x] Return selected manifests, skipped manifests, planned unique bytes,
      planned blobs, and budget status.
- [x] Prove dry-run performs no filesystem mutation.
- [x] Add node-updater dry-run task execution.
- [x] Run production dry-run on canary nodes before any delete mode.

### P5: image-cache delete rollout

- [x] Add `FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MODE=delete` support behind explicit
      config.
- [x] Require task-level `allow_delete=true`.
- [x] Start with one canary node and a small per-node byte budget.
- [x] Compare planned bytes, deleted bytes, and post-run inventory.
- [x] Confirm no current workload, available image, active pin, or active task
      image was selected.
- [x] Gradually raise budgets only after multiple successful runs.
- [x] Promote orphan cleanup into formal low-speed automation with post-delete
      inventory and halt gates for unsafe candidate reasons, active prune tasks,
      and failed controller prune tasks.

### P6: LVM LocalPV inventory

- [x] Add `report-lvm-localpv-inventory` task type.
- [x] Add control-plane storage for LocalPV inventory.
- [x] Add CLI `fugue admin cluster node localpv ls`.
- [x] Add CLI `fugue admin cluster node localpv show <node>`.
- [x] Mark nodes with active LVs or bound PVs as `not eligible`.
- [x] Add tests for empty VG, active VG, missing loop device, path mismatch,
      and stale inventory.

### P7: LVM LocalPV dry-run decommission

- [x] Add `decommission-lvm-localpv` task with dry-run mode.
- [x] Require `lv_count == 0`.
- [x] Require `bound_pv_count == 0`.
- [x] Require exact image path and loop-device match.
- [x] Require node role policy to allow LocalPV removal.
- [x] Return expected freed bytes and commands that would be run.
- [x] Refuse all active-LV nodes, including cases like `ns101351` and
      `netcup`.

### P8: LVM LocalPV apply mode

- [x] Add apply mode only with explicit `allow_delete=true`.
- [x] Require expected preflight values in the task request.
- [x] Re-run preflight immediately before mutation.
- [x] Stop and disable only the Fugue LocalPV loop service for the verified
      image path.
- [x] Detach only the verified loop device.
- [x] Delete only the verified backing file.
- [x] Report final inventory and freed bytes.
- [x] Emit an audit event.

### P9: node preparation policy

- [x] Update node onboarding so edge-only and DNS-only nodes do not get LocalPV
      backing files by default.
- [x] Update control-plane-only defaults to avoid LocalPV unless explicitly
      requested.
- [x] Keep storage-capable agent LocalPV opt-in with an explicit size.
- [x] Add dry-run render tests for role-based LocalPV preparation.
- [x] Document how to opt into LocalPV for a new storage-capable node.

### P10: release and cross-repo follow-up

- [x] Run `make test` in the Fugue repository before merge.
- [x] Push through the normal `main` branch control-plane deployment workflow.
- [x] If frontend-consumed APIs are added, sync
      `/Users/yanyuming/Downloads/GitHub/fugue-web/openapi/fugue.yaml`.
- [x] Regenerate
      `/Users/yanyuming/Downloads/GitHub/fugue-web/lib/fugue/openapi.generated.ts`.
- [x] Run `npm run contract:check` in `fugue-web` after API sync.

## 10. Operational rule

The operational rule for this work is:

```text
Image-cache orphan cleanup may graduate from observe to dry-run to limited
delete automation because the protected-set simulation showed zero protected
candidate hits.

LVM LocalPV recovery must remain explicit, per-node, dry-run-first maintenance.
It must never run as automatic GC.
```
