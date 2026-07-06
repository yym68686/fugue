# Distributed image-store retention and prune fix plan

本文档固化 2026-07-06 只读生产调查后的完整修改方案，目标是让
Fugue distributed image-store 真正兑现产品层 `image_mirror_limit` / saved
image limit 语义，并解决 image-cache 在磁盘压力下无法解释、无法释放、无法
调度清理的问题。

本文档不是线上手工修复步骤。任何影响 control plane、image-cache、
node-updater、registry、runtime pull 路由、Helm values、cluster bootstrap 或
平台流量规则的改动，都必须通过本仓库正式发布链路：提交到
`main`，然后由 `.github/workflows/deploy-control-plane.yml` 的 self-hosted
runner 发布控制平面。默认禁止通过 SSH 手工删除线上文件、手工 patch
Deployment、手工重启服务或手工同步镜像作为持久修复。

## 1. 背景与问题陈述

产品层语义是：每个 app 的 saved/mirrored image limit 决定 Fugue 为该 app
保留多少个历史镜像版本。默认值是 1。用户如果没有显式提高历史镜像数量，
旧镜像应在安全窗口后自动清理；如果用户设置为 2/5，则按设置保留更多历史
版本。

当前线上控制平面已经运行在 distributed image-store 模式：

```text
FUGUE_IMAGE_STORE_MODE=distributed
```

但 2026-07-06 的只读调查发现：distributed image-store 切换后，旧的
central/bundled registry 路径中按 `app.Spec.ImageMirrorLimit` 清理历史镜像的
逻辑没有被等价迁移。结果是：

- `fugue-web` 和 OpenAPI/API 层仍然正确暴露并保存 `image_mirror_limit`；
- 后端模型默认值仍然是 1；
- 但 distributed 模式下，历史镜像 metadata、pins、replicas、local manifests
  和 node-local blobs 可能长期保留；
- prune-plan 因过度保护和候选类型不完整而给出 `candidate_manifest_count=0`，
  无法解释每个大 manifest 为什么不能删，也不会调度真正能释放空间的节点
  GC。

## 2. 只读生产事实

调查只读取 Fugue CLI、控制面数据库、Kubernetes 对象、节点本地 image-cache
inventory/dry-run 和 blob 文件大小，没有执行 delete/prune/patch/restart。

### 2.1 App retention 设置已经保存

线上 app 的 `image_mirror_limit` 分布：

| `image_mirror_limit` | App 数量 |
| ---: | ---: |
| 1 | 289 |
| 2 | 3 |
| 5 | 4 |

后端默认值也确认为 1：

- `internal/model/model.go` 中 `DefaultAppImageMirrorLimit = 1`；
- `EffectiveAppImageMirrorLimit(value)` 对 `<=0` 使用默认 1；
- `fugue-web` 读取时默认 `imageMirrorLimit ?? 1`，PATCH 时写入
  `image_mirror_limit`。

### 2.2 多个 app 明显超过 retention limit

| App | limit | `fugue_images` | available | lost | active pins | current pins | rollback pins |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `yesir` | 1 | 80 | 44 | 36 | 139 | 81 | 58 |
| `uni-api-web` | 2 | 60 | 60 | 0 | 133 | 67 | 66 |
| `uni-api-web-api` | 2 | 60 | 60 | 0 | 152 | 77 | 75 |
| `gaokao` | 1 | 31 | 2 | 29 | 38 | 32 | 6 |
| `oaix` | 2 | 25 | 17 | 8 | 61 | 37 | 24 |
| `medical-insurance-audit-agent` | 1 | 18 | 17 | 1 | 34 | 18 | 16 |
| `specforge` | 1 | 17 | 17 | 0 | 36 | 18 | 18 |

这说明产品层 limit 已经不是实际清理边界。

### 2.3 当前 image-cache 占用与可释放估计

估算口径：

- `manifest 策略可释放`：在 exact ref/digest、node-aware current workload、
  target/min replica=1、stale replica cleanable 的策略下，删除不再需要的 local
  manifest 后可释放的 unique blob bytes；
- `unreferenced blobs 可释放`：节点本地 image-cache daemon dry-run 看到的
  manifest graph 已不引用的 blob；
- 这是策略修复后的理论释放，不代表当前 prune-plan 已经会执行。

| Node | Role | Current image-cache | Manifest policy reclaim | Unreferenced blob reclaim | Total reclaim estimate | Estimated remaining |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `ns101351` | US agent, filesystem pressure | 191.65 GiB | 19.20 GiB | 165.62 GiB | 184.82 GiB | 6.83 GiB |
| `fortedrape8` | HK agent, filesystem pressure | 7.49 GiB | 1.16 GiB | 5.38 GiB | 6.54 GiB | 0.94 GiB |
| `v2202605354515455529` | DE agent | 16.53 GiB | 0.36 GiB | 15.83 GiB | 16.19 GiB | 0.34 GiB |
| `vps-d6d20fa1` | control plane, filesystem pressure | 3.36 GiB | 0 GiB | 0.65 GiB | 0.65 GiB | 2.71 GiB |
| `vps-591f4447` | edge/DNS | 0 GiB | 0 GiB | 0 GiB | 0 GiB | 0 GiB |
| `vps-84c8f0a9` | edge/DNS | 0 GiB | 0 GiB | 0 GiB | 0 GiB | 0 GiB |
| `bwg` | offline edge | about 0.22 GiB stale inventory | about 0.22 GiB | unknown | about 0.22 GiB | 0 GiB |

### 2.4 当前 prune-plan 为什么没有清掉

当前 control-plane prune-plan：

```text
candidate_manifest_count: 0
protected_manifest_count: 2429
protection_summary:
  current_workload: 2371
  active_pin: 57
  active_task: 1
```

而节点本地 `ns101351` image-cache dry-run 同时看到：

```text
cache: 191.65 GiB
unreferenced_blobs: 2023
unreferenced_blob_bytes: 165.62 GiB
candidate manifests: 1609
planned_delete: 10.00 GiB  # local daemon per-run budget limited
```

因此当前问题不是“没有 prune 功能”，而是：

1. control-plane prune-plan 过度保护 manifest；
2. control-plane prune-plan 没有把 unreferenced blobs 作为一等候选纳入摘要、解释
   和调度决策；
3. controller 以 `CandidateManifestCount == 0` 为跳过条件，导致即使节点本地有
   大量 unreferenced blobs，也不会下发清理任务；
4. historical `current_deploy` pins 和 `rollback_window` pins 长期积累，阻止
   distributed image prune；
5. pending repair/replication backlog 仍然指向 edge、offline、filesystem pressure
   节点，形成 `active_task` 保护和后续空间回填。

## 3. 根因分析

### 3.1 Central registry retention 逻辑被 distributed mode 绕开

非 distributed 模式下，controller 的旧路径会调用：

```go
appimages.ExcessManagedImageRefs(..., app.Spec.ImageMirrorLimit)
```

这个函数真正实现了“按 app limit 只保留最新 N 个镜像”的语义。

但 distributed 模式下，`internal/controller/prune_images.go` 提前返回：

```go
if s.imageStoreDistributedMode() {
    return s.scheduleDistributedImagePruneForApp(ctx, app)
}
```

因此 distributed 模式不会调用 `ExcessManagedImageRefs`，不会使用
`app.Spec.ImageMirrorLimit` 来决定哪些历史 image generation 应删除。

### 3.2 Distributed image prune 只按 pin/replica 数处理，不按 app generation limit

当前 distributed path 主要逻辑：

1. 列出 app 下所有 `fugue_images`；
2. 对每个 image 调 `scheduleDistributedImagePrune`；
3. 如果 image 存在未过期 pin，则直接返回不 prune；
4. 如果 present replicas 数量大于 `imageMinReplicaCount()`，只 prune 多余 replicas。

这会导致：

- image generation 本身不会因 `image_mirror_limit` 超限而过期；
- `current_deploy` pin 没有过期时间，历史 deploy 产生的 current pins 会永久挡住
  prune；
- `rollback_window` pin 虽然会过期，但在窗口内也会保护超出用户 limit 的历史
  generations；
- replica prune 只解决“一个 image 有太多副本”，不解决“一个 app 有太多 image
  generations”。

### 3.3 `current_deploy` pin 语义漂移

`current_deploy` 应表示 app 当前实际运行的 image generation，理论上每个 app 的
每个运行角色最多只有当前 generation。线上却出现单 app 数十个 active
`current_deploy` pins。

根因是 pin identity 包含 `image_id/app_id/operation_id/reason`，新 deploy 会创建新
`current_deploy` pin，但完成后没有把旧 current pins 失效或转成有限 rollback
保护。

### 3.4 Retention limit、replica count、pin min replicas 被混淆

需要明确三层不同语义：

| 概念 | 控制什么 | 默认建议 |
| --- | --- | --- |
| `app.Spec.ImageMirrorLimit` | 每个 app 保留多少 image generations | 1 |
| `ImageStoreTargetReplicas` / `ImageStoreMinReplicas` | 每个被保留 image generation 保留多少健康节点副本 | 1 |
| `ImagePin.MinReplicas` | 某个 pin 原因要求的最小副本数 | current/rollback 默认 1 |

不能用 replica count 替代 generation retention。即使每个 image 只有 1 个副本，
如果 app 下有 80 个 image generations，仍然违反 saved image limit。

### 3.5 Protection key 过粗、缺少 node-aware 解释

当前 prune protection 使用的 reference key 包含 repo 级 key。某个当前 workload
使用 `fugue-apps/foo:<current>` 时，可能把同 repo 下的多个历史 tags 一起判定为
`current_workload`。此外 current workload 保护没有足够 node-aware：某节点当前
并没有运行该 image，也可能因全局 workload ref 被保护。

### 3.6 Unreferenced blob GC 不受 control-plane plan 驱动

`fugue-image-cache` 本地 prune API 能计算 `unreferenced_blobs`，但 control-plane
`ImageCachePrunePlan` 目前以 manifest candidate 为主，缺少：

- unreferenced blob count/bytes；
- blob-level candidate reasons；
- 即使 manifest candidate 为 0 也调度 blob GC 的路径；
- below watermark 时仍清除纯 garbage blobs 的策略。

## 4. 目标状态

### 4.1 产品语义

- 默认每个 app 只保留 1 个 image generation；
- 用户设置 `image_mirror_limit=N` 后，distributed image-store 必须保留最多 N 个
  可回滚/可复用的 image generations；
- 当前 workload 使用的 image 永远不能被清理；
- 正在 deploy/import/replicate 的 candidate image 在操作完成或失败前必须被保护；
- 超出 retention limit 的历史 image generation 应被标记为 retention excess，并
  允许 pins、replicas、local manifests 和 blobs 进入清理路径；
- rollback 只在 retention keeper set 内提供保护；如果用户 limit=1，默认不承诺
  长期保留上一版 image generation。

### 4.2 Replica 语义

- distributed image-store 默认：`targetReplicas=1`、`minReplicas=1`；
- `current_deploy` pin 和 `rollback_window` pin 的 `MinReplicas` 默认降为 1；
- historical 默认 2/2 不应继续扩大副本数；
- filesystem pressure 节点不接收新的 repair/warmup/rebalance replication；
- edge-only、DNS-only、offline 节点不接收 app image replication；
- stale/failed/missing replicas 可以作为 prune candidates。

### 4.3 Prune-plan 语义

- prune-plan 必须能解释每个 manifest 的 keep/delete 决策；
- prune-plan 必须输出 skip reason 明细，不只给 aggregate summary；
- prune-plan 必须输出 unreferenced blob count/bytes/candidates；
- prune-plan 调度条件应是：manifest candidates > 0 **或** unreferenced blob bytes > 0
  **或** node filesystem pressure；
- control-plane 和 node-local daemon 的 dry-run 口径应能对齐。

## 5. 修改方案

### 5.1 新增 distributed image retention planner

新增一个纯函数式 planner，建议放在：

```text
internal/controller/distributed_image_retention.go
```

输入：

- app metadata/spec/status；
- app 相关 `fugue_images`；
- app operations / release metadata；
- active workload refs；
- active import/deploy operation refs；
- current time；
- `app.Spec.ImageMirrorLimit`。

输出：

```text
DistributedImageRetentionPlan
  AppID
  EffectiveLimit
  KeepImageIDs []
  DropImageIDs []
  ImageDecisions []ImageRetentionDecision
```

每个 image decision 至少包含：

```text
image_id
image_ref
source_operation_id
lifecycle_state
last_deployed_at
current_workload bool
active_operation bool
rank
keep bool
reason
```

建议 reason vocabulary：

```text
current_workload
active_operation
retention_keep_latest_n
retention_excess
deleted_app
lost_image
missing_source_operation
```

排序规则：

1. current workload exact ref/digest 永远 keep；
2. active deploy/import operation 的 image 在 operation 完成前 keep；
3. 按 deploy completed time / source operation completed time / image created time 从新到旧
   排序；
4. keep 到 `EffectiveAppImageMirrorLimit`；
5. 其余标记为 `retention_excess`。

注意：这里的 limit 是 image generation 数，不是 replica 数。

### 5.2 修正 current_deploy pin 生命周期

新增或扩展 controller post-deploy maintenance：

```text
reconcileDistributedCurrentDeployPinsForApp(ctx, app)
```

行为：

- 根据 app 当前 desired/runtime image 和 live workload exact refs 找出真实 current image；
- 对真实 current image 保留或创建 `current_deploy` pin，`MinReplicas=1`；
- 对同 app 旧 `current_deploy` pins：
  - 如果 image 在 retention keeper set 内且需要 rollback，则转为/保留有限期
    `rollback_window`；
  - 如果 image 超出 retention limit，则删除 active current pin；
- 禁止单 app 长期累积多个永久 `current_deploy` pins。

`recordImportedDistributedImage` 可以继续为正在导入/部署的 image 建立临时保护，
但部署完成后必须由 reconciler 收敛到真实 current set。

### 5.3 让 distributed retention sweep 消费 `image_mirror_limit`

修改：

```text
internal/controller/image_retention_sweep.go
internal/controller/prune_images.go
internal/controller/distributed_image_cleanup.go
```

当前 distributed sweep 只做：

```text
expired pin sweep -> scheduleDistributedImagePruneForApp
```

应改为：

```text
expired pin sweep
-> build distributed retention plan per app
-> reconcile pins according to plan
-> cancel invalid pending replication tasks for drop set
-> mark/drop image generations beyond limit
-> schedule prune for drop set replicas/manifests/blobs
-> sync billing image storage
```

超出 limit 的 image generation 处理建议：

1. 删除该 image 的 active `current_deploy` / `rollback_window` / default retention pins；
2. 取消该 image 的 pending repair/warmup/rebalance replication tasks；
3. 如果不在 current workload、active operation、user explicit pin 中，设置 lifecycle 为
   `deleting` 或 `deleted`；
4. 将对应 replicas 标记为 stale/missing/delete-candidate，允许 image-cache prune-plan
   生成 candidates；
5. 清理后保留审计事件，不直接删除 blob 文件。

### 5.4 明确 user pin 与 retention pin 优先级

`user_pin` 应该可以覆盖 `image_mirror_limit`，但必须是显式用户/管理员行为。

建议规则：

| Pin reason | 是否可覆盖 imageMirrorLimit | 默认 MinReplicas |
| --- | --- | ---: |
| `user_pin` | 是 | 1 |
| `retention` | 是，但应有创建来源和可见设置 | 1 |
| `current_deploy` | 只保护真实当前 workload | 1 |
| `rollback_window` | 只保护 retention keeper set 内的历史 generation | 1 |
| `import_result` / operation temp | 只在 operation active/grace 内保护 | 1 |
| `replication_seed` | 短期保护，不能永久阻止 retention | 1 |

### 5.5 修正 replica policy 默认与历史值

配置默认：

```text
FUGUE_IMAGE_STORE_TARGET_REPLICAS=1
FUGUE_IMAGE_STORE_MIN_REPLICAS=1
```

代码修正点：

- `internal/config/config.go` 默认值从 2 改为 1；
- Helm `deploy/helm/fugue/values*.yaml` 中 `imageStore.targetReplicas` 与
  `imageStore.minReplicas` 改为 1；
- `recordImportedDistributedImage` 新建 `Image.RequiredReplicaCount` 与
  `MinAvailableReplicaCount` 使用 1；
- `current_deploy` / `rollback_window` pins 新建时 `MinReplicas=1`；
- 对历史默认写入的 `(required_replica_count=2, min_available_replica_count=2)` 做
  兼容收敛：
  - 可通过 migration 将历史默认 2/2 改为 1/1；或
  - planner/scheduler 识别旧默认 2/2，在没有显式 user policy 时按当前默认 1/1
    解释。

推荐使用 migration + 代码兼容双保险，但 migration 必须只针对系统默认产生的
旧值，不引入 app-specific 例外。

### 5.6 加入 node eligibility 过滤

修改 image replication scheduler：

```text
internal/controller/image_replication_controller.go
```

新建统一函数：

```text
nodeEligibleForAppImageReplication(nodePolicy, runtime, updater, reason) bool
```

规则：

- node-updater 必须 active；
- runtime 必须 active/ready；
- node 必须允许 app runtime 或 shared pool app workload；
- filesystem pressure 节点不接收新的 repair/warmup/rebalance replication；
- edge-only / DNS-only / offline 节点不接收 app image replication；
- deploy-blocking replication 只允许目标 workload 所在节点，且需要明确解释；
- control-plane-only 节点不接收普通 app image replication。

同时增加 pending task cleanup：

- cancel target=edge/offline/filesystem-pressure 的 repair/warmup/rebalance tasks；
- cancel retention-excess image 的 pending replication tasks；
- 在 prune-plan skip reason 中不要让这些 obsolete tasks 继续形成 `active_task` 保护。

### 5.7 修正 protection key 与 node-aware current workload

修改：

```text
internal/imagecachekeys/keys.go
internal/api/image_cache_localpv_admin.go
internal/controller/image_cache_orphan_cleanup.go
```

原则：

- current workload protection 使用 exact image ref、tag、digest ref、manifest digest；
- 不允许 bare repo key 单独保护整个 repo 的所有历史 tags；
- current workload 必须 node-aware：只保护当前 node 正在运行的 image manifest；
- active pins 保护对应 image generation，不保护同 repo 全部 history；
- active tasks 只保护仍有效、目标仍 eligible、未被 retention 取消的任务；
- available image 不再全局保护所有 local manifests，而是按 retention keeper set 和
  min replica count 选择需要保留的具体 local replicas。

建议把 key 类型拆成两层：

```text
ImageExactKey     # ref/tag/digest exact match, 用于 current workload / pin / task
ImageRepoKey      # repo-level grouping, 只用于分组和展示，不能作为删除保护命中
```

### 5.8 扩展 prune-plan 输出与 API contract

因为这是 API 返回结构变化，必须先改：

```text
openapi/openapi.yaml
```

再运行：

```text
make generate-openapi
make test
```

建议扩展 `ImageCachePrunePlan`：

```text
candidate_manifest_count
protected_manifest_count
candidate_blob_count
candidate_blob_bytes
protected_blob_count
planned_delete_bytes
max_delete_bytes
min_manifest_age
protection_summary
candidate_summary
candidates[]
protected_manifests[] or skipped_manifests[]
unreferenced_blobs[]
node_pressure
budget_exhausted
```

每个 manifest candidate/skip item 增加：

```text
protected bool
reason
skip_reason
skip_details[]
matched_image_ids[]
matched_pin_ids[]
matched_task_ids[]
matched_workload_refs[]
matched_replica_ids[]
node_name
repo
target
digest
planned_delete_bytes
referenced_blob_count
referenced_blob_bytes
created_at_observed
last_seen_at
```

这样 operator 能回答：

- 这个大 manifest 为什么不能删？
- 是 current workload、active pin、active task、minimum replica，还是 recent grace？
- 如果是 active pin，是哪个 pin？
- 如果是 current workload，是哪个 node 上哪个 workload？
- 如果是 active task，是哪个 task，目标节点是否仍 eligible？

### 5.9 让 control-plane 调度 unreferenced blob GC

修改：

```text
cmd/fugue-image-cache/main.go
internal/api/image_cache_localpv_admin.go
internal/controller/image_cache_orphan_cleanup.go
internal/api/node_updater.go
```

目标：

- node-local prune API 支持显式 `include_unreferenced_blobs=true` 或等价字段；
- unreferenced blob cleanup 不应依赖 manifest candidates；
- below-watermark 节点也可以清理纯 garbage blobs，但需要预算和速率限制；
- filesystem pressure 节点提高优先级和预算；
- control-plane plan 将 unreferenced blob bytes 纳入 candidate summary；
- controller 调度条件改为：

```go
if plan.CandidateManifestCount == 0 && plan.CandidateBlobCount == 0 {
    continue
}
```

当前 `ns101351` 的 165.62 GiB 主要依赖这一项释放。

### 5.10 调整 prune budget 和节流策略

当前相关预算：

```text
FUGUE_IMAGE_STORE_PRUNE_MAX_DELETE_BYTES_PER_RUN=10Gi
FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MAX_DELETE_BYTES_PER_NODE=104857600  # 100 MiB
```

建议：

- orphan prune per-node 默认提高到至少 `10Gi`；
- filesystem pressure 节点可临时使用更高预算，例如 `25Gi` 或按 free target 计算；
- 单轮删除后立即上报 inventory；
- controller 通过 inventory 验证释放效果后再调度下一轮；
- 保留全局并发限制，避免所有节点同时做大规模删除。

### 5.11 Billing/storage accounting 同步

distributed retention 改动后，billing 不能继续按 central registry inspect 估算。

需要确认并修正：

- retained image generations 的 storage accounting；
- unreferenced blobs 不应继续计入用户 saved image storage；
- user-pinned image generations 应计入；
- deleted/retention-excess/lost images 的 replicas 清理完成后应从计费中移除。

## 6. 数据迁移与兼容策略

### 6.1 Historical 2/2 replicas

线上所有 `fugue_images` 当前都是：

```text
required_replica_count=2
min_available_replica_count=2
```

迁移策略：

- 将系统默认生成的 2/2 收敛为 1/1；
- 如果未来支持用户显式指定 image replica policy，则必须用单独字段或 policy source
  区分，不能再把默认值写死为不可区分的 2/2；
- migration 应可重复执行且幂等。

### 6.2 Historical current pins

历史 `current_deploy` pins 需要由 per-app reconciler 重新计算。

迁移策略：

1. dry-run 输出每个 app 当前 active current pins 数量；
2. 按 actual current workload / latest successful deploy 选择真实 current image；
3. 对其他 current pins：
   - 若在 retention keeper set 内，转为 rollback/retention pin 并设过期；
   - 若超出 limit，删除 pin；
4. 生成 audit event 和 operator summary。

### 6.3 Pending task backlog

迁移策略：

- dry-run 列出将取消的 pending replication tasks；
- 取消目标为 edge/offline/filesystem-pressure 的 repair/warmup/rebalance tasks；
- 取消 retention-excess image 的 pending replication tasks；
- 不取消 active deploy-blocking tasks，除非目标 image 已被证明不再属于 active operation。

### 6.4 Lifecycle 收敛

对超出 retention limit 且不被 current workload/user pin/active operation 保护的 images：

- 标记为 `deleting` 或 `deleted`；
- 或新增更明确状态 `retention_excess`，再由 cleanup 转为 `deleted`。

如果新增 lifecycle 状态，必须同步：

- OpenAPI schema；
- store normalization；
- CLI/API output；
- tests；
- fugue-web generated OpenAPI consumer。

如果不新增状态，可以用 existing `deleting/deleted`，但 prune-plan reason 必须保留
`retention_excess` 作为删除原因，避免 operator 无法区分用户删除和 retention 清理。

## 7. Rollout plan

### Phase 0: Observe only

- 发布 planner 和 prune-plan explainability；
- 不删除 pins、tasks、manifests、blobs；
- CLI/API 输出：每个 app keep/drop decisions，每个 node reclaim estimate；
- 对比 production facts：`yesir`、`uni-api-web*`、`specforge` 等是否符合预期。

### Phase 1: Safe metadata reconciliation

- 将新 image/pin 默认写成 1/1；
- current_deploy pin per-app 收敛，但先只对新 deploy 生效；
- 取消明显无效的新 replication target：edge/offline/filesystem-pressure；
- pending old task cancellation 先 dry-run。

### Phase 2: Historical metadata migration

- historical 2/2 -> 1/1；
- historical current pins 按 per-app retention plan 收敛；
- cancel obsolete pending repair tasks；
- still no blob deletion beyond dry-run unless all explainability checks pass。

### Phase 3: Limited deletion

- 开启 retention-excess manifest prune；
- 开启 unreferenced blob GC；
- 先对 pressure 节点小并发、高可观测执行；
- 每轮删除后强制 inventory report；
- 预算从 10Gi/轮开始，按节点压力调高。

### Phase 4: Full automation

- 定期 distributed retention sweep；
- 定期 unreferenced blob GC；
- pressure-aware priority；
- CLI dashboard 和 metrics 常态化。

## 8. Verification plan

### Unit tests

新增/更新测试：

- distributed retention planner：limit=1/2/5；
- current workload exact match，不保护同 repo 历史 tags；
- user_pin override；
- active operation protection；
- rollback pin 在 limit=1 下不长期保留上一版；
- historical current_deploy pins 收敛；
- target/min replicas 默认 1；
- node eligibility：edge/offline/pressure 不接收 app image replication；
- stale/failed/missing replicas 进入 prune candidates；
- unreferenced blob candidates 即使 manifest candidate=0 也输出并调度。

### Integration tests

- deploy app 3 次，limit=1，最终只保留当前 generation；
- deploy app 3 次，limit=2，最终保留当前 + 最近 1 个；
- distributed mode 下 PATCH `image_mirror_limit` 从 5 降到 1 后触发 retention plan；
- prune-plan 输出每个 protected manifest 的 skip details；
- orphan blob only case：manifest candidate=0、unreferenced blob >0，controller 仍下发
  dry-run/delete task；
- pressure node replication scheduler skip。

### Production verification commands

只读验证命令应包括：

```text
fugue admin image-cache inventory --json
fugue admin image-cache prune-plan --mode observe --json
fugue admin image-cache prune-plan --mode delete --json
fugue admin cluster node-policy status --json
fugue runtime ls --json
```

需要新增或扩展 CLI：

```text
fugue admin image-retention plan --app <app> --json
fugue admin image-retention plan --all --json
fugue admin image-retention reconcile --dry-run
fugue admin image-cache prune-plan --include-protected --include-blobs --json
```

## 9. Operator safety gates

上线前必须满足：

- prune-plan 能解释所有 >1GiB manifest 的 skip/delete reason；
- retention planner dry-run 中没有 current workload 被 drop；
- no app image generation with active pod is marked retention_excess；
- no deploy-blocking task is canceled；
- no edge/offline/pressure node receives new repair replication after release；
- deletion budget、并发、inventory refresh 都受控；
- rollback path 是通过 GitHub Actions 发布旧版本，不是 SSH 手工修线上。

## 10. Implementation TODO

### A. Contract and docs

- 实现说明：本轮没有新增 image lifecycle enum，沿用 `deleting/deleted/lost`，并通过
  prune-plan `reason=retention_excess` 保留操作语义。
- [x] 在 `openapi/openapi.yaml` 扩展 `ImageCachePrunePlan`，加入 protected/skipped manifest details 与 unreferenced blob summary。
- [x] 如新增 image lifecycle/reason 字段，先更新 OpenAPI schema。
- [x] 运行 `make generate-openapi` 更新 generated artifacts。
- [x] 更新 CLI help / admin docs，说明 observe/dry-run/delete 的差异。
- [x] 在 `docs/distributed-image-store-plan.md` 和 storage recovery 文档中链接本计划。

### B. Distributed retention planner

- [x] 新增 `internal/controller/distributed_image_retention.go`。
- [x] 实现 per-app generation keeper/drop planner。
- [x] 使用 `model.EffectiveAppImageMirrorLimit(app.Spec.ImageMirrorLimit)`。
- [x] 将 current workload、active operation、user_pin 纳入保护。
- [x] 输出 deterministic decision reasons。
- [x] 添加 planner unit tests。

### C. Pin lifecycle

- [x] 新增 current deploy pin reconciler。
- [x] 新建 `current_deploy` pin 默认 `MinReplicas=1`。
- [x] 新建 `rollback_window` pin 默认 `MinReplicas=1`。
- [x] 部署完成后按真实 current image 收敛旧 current pins。
- [x] 超出 retention limit 的旧 pins 删除或过期。
- [x] 添加历史 current pins 收敛 dry-run 输出。

### D. Replica policy

- [x] 修改 config 默认 `ImageStoreMinReplicas=1`。
- [x] 修改 config 默认 `ImageStoreTargetReplicas=1`。
- [x] 修改 Helm values 中 imageStore 默认 min/target replicas 为 1。
- [x] 修改 import/deploy 记录 image 时写入 1/1。
- [x] 增加历史 2/2 兼容或 migration。
- [x] 添加 scheduler 和 store tests。

### E. Node eligibility and task cleanup

- [x] 新增统一 node eligibility 判断函数。
- [x] filesystem pressure 节点不接收新的 repair/warmup/rebalance replication。
- [x] edge-only / DNS-only / offline 节点不接收 app image replication。
- [x] control-plane-only 节点不接收普通 app image replication。
- [x] deploy-blocking task 只允许目标 workload 所在节点。
- [x] 新增 obsolete pending replication task dry-run/cancel 逻辑。
- [x] prune-plan 中 obsolete tasks 不再形成永久 `active_task` 保护。

### F. Protection key and prune-plan explainability

- [x] 将 current workload 保护改为 node-aware。
- [x] 将 current workload 保护改为 exact ref/digest，不使用 bare repo key 保护同 repo 历史 tags。
- [x] active pin/task protection 改为 image generation 精确匹配。
- [x] available image 保护改为 min replica keeper set，而不是全局保护所有 manifests。
- [x] protected manifest details 输出 matched workload/pin/task/replica IDs。
- [x] prune-plan 输出每个大 manifest 的 skip reason 明细。
- [x] 添加 regression tests：同 repo 历史 tag 不应因当前 tag 被保护。

### G. Unreferenced blob GC

- [x] 扩展 node-local prune API，支持显式 include unreferenced blobs。
- [x] control-plane inventory/prune-plan 纳入 unreferenced blob count/bytes。
- [x] controller 调度条件改为 manifest candidates 或 blob candidates 任一存在即可。
- [x] below watermark 节点也允许按预算删除纯 garbage blobs。
- [x] deletion 后强制 report image-cache inventory。
- [x] 添加 blob-only prune integration test。

### H. Budget and rollout controls

- [x] 将 orphan prune per-node 默认预算从 100MiB 调整到合理值，例如 10GiB。
- [x] filesystem pressure 节点支持更高但受控的预算。
- [x] 增加全局并发限制。
- [x] 增加 per-node cool-down，避免连续删除影响 IO。
- [x] 增加 deletion audit events。
- [x] 增加 metrics：planned bytes、deleted bytes、skip reasons、blob GC bytes。

### I. Data migration / reconciliation

- [x] dry-run 统计所有 app 的 image generation over-limit 情况。
- [x] dry-run 统计将删除/过期的 pins。
- [x] dry-run 统计将取消的 replication tasks。
- [x] historical 2/2 收敛到 1/1。
- [x] historical current pins per-app 收敛。
- [x] retention-excess images 标记并进入 prune-plan。
- [x] billing image storage refresh。

### J. Verification and release

- [x] `make generate-openapi`。
- [x] `make test`。
- [x] 如 API 变更影响 fugue-web，同步运行：
  - [x] `npm run openapi:sync`
  - [x] `npm run openapi:generate`
  - [x] `npm run contract:check`
- [x] 发布前跑 production observe-only plan。
- [x] 发布后确认 edge/offline/pressure 节点没有新 app replication tasks。
- [x] 发布后确认 `candidate_manifest_count`、`candidate_blob_bytes` 与节点 dry-run 对齐。
- [x] 发布后分轮释放 `ns101351`，每轮检查 filesystem usage 与 app rollout 状态。

## 10.1 Release verification log

2026-07-06 发布验证结论：

- 已通过正式发布链路推送并部署 `b08307a`、`190b21f`、`e1ce8b9`、`6a6016e`、`01c62b1`、`bc862c8`、`30a11a1`、`d5d6fa4`、`9323caf`、`ab3b5f9`、`de4221e`、`c5e3da8` 等修复；对应 `ci`、`build-cli`、`deploy-control-plane` 均完成成功。
- `fugue-web` OpenAPI 派生产物已同步并通过 `npm run openapi:sync`、`npm run openapi:generate`、`npm run contract:check`。
- 发布后 pending app image replication 未再调度到 edge-only/offline 节点；`bwg` 仍因流量耗尽保持离线/旧 updater，只有预期的旧 pending updater/inventory 任务，没有新的 app image replication 次生灾害。
- 发布后 app image replication backlog 从 30 逐步降到 4；剩余任务均是修复前创建的旧 app 节点任务，目标集中在 `ns101351` 和 `v2202605354515455529`。
- `candidate_blob_bytes` 与节点 inventory dry-run 已对齐：`ns101351` 为 `167043117003` bytes，`v2202605354515455529` 为 `6265313349` bytes；此前 inventory double-count 已由 `de4221e` 修复。
- 在线 node-updater 已升级到 `v14`：`ns101351`、`fortedrape8`、`v2202605354515455529`、`vps-591f4447`、`vps-84c8f0a9` 均心跳正常；`bwg` 离线保持 `v12` 属预期。
- `ns101351` 已完成两轮自动释放：
  - 第一轮：约 10.74 GiB cache/free-space 改善，后续未发现 rollout gate 阻塞；
  - 第二轮：`planned_delete_bytes=10737418077`、`deleted_bytes=10737418077`，filesystem used 从约 `78.89%` 降至约 `76.45%`，free bytes 提升到约 `81143353344`。
- 其他自动释放：
  - `fortedrape8`：`deleted_bytes=879124489`；
  - `v2202605354515455529`：`deleted_bytes=6265482685`。
- 发布后 `robustness.block_rollout=false`，`platform-autonomy.pass=true` 且 `block_rollout=false`；`fugue-system` pods 全部 Running/Ready。
- 发布后发现的 build pod `ephemeral-storage` 8Gi 驱逐问题已由 `c5e3da8` 修复：heavy builder 对 ephemeral-storage eviction 进行分档扩容重试；修复部署后新 import 与 deploy operation 均 `completed`。

## 11. Expected production effect

修复全部落地后，按 2026-07-06 快照估计：

- `ns101351` 可释放约 184.82 GiB，其中最大头是 165.62 GiB unreferenced blobs；
- `fortedrape8` 可释放约 6.54 GiB；
- `v2202605354515455529` 可释放约 16.19 GiB；
- `vps-d6d20fa1` 可释放约 0.65 GiB；
- edge nodes 当前 image-cache 基本无大头，主要收益是停止接收 app image replication；
- `bwg` offline 属预期状态，但修复后不应继续有 app image replication backlog 或二次灾害。

这些释放必须通过正式控制面和 node-updater 清理任务完成，不能通过手工 SSH 删除
线上文件完成。
