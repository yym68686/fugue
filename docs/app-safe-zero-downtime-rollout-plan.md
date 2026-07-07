# Fugue 用户服务 Safe Zero Downtime Rollout 方案

最后更新：2026-07-07

本文把“用户服务开启 zero downtime 后，发布新版本时必须先验证新 Pod / candidate release 正常；旧 Pod 只有在没有连接且新版本正常时才允许关闭；新版本异常时自动 abort/rollback 并保留未关闭旧 Pod”的完整设计固定下来。

本文只记录方案和 TODO，不包含 secret，不要求手工 patch 线上集群。正式实现仍必须走 `fugue` 仓库的 OpenAPI-first 流程、测试和 GitHub Actions 控制平面发布链路。

## 背景

Fugue 当前已经有以下基础能力：

- Runtime 对可在线滚动的服务渲染 `RollingUpdate`，并使用 `maxUnavailable=0`、`maxSurge=1`。
- Strict zero downtime app 会注入 connection-aware `fugue-drain-agent`，旧 Pod terminating 时等待业务端口 active TCP connections 归零。
- Controller 会在 deploy/migrate 时做 zero downtime surge capacity preflight。
- Controller 会等待 ManagedApp / Deployment / Pod template 收敛，并在失败时记录 operation 和 release attempt failure。
- 平台已有 `AppRelease`、`AppTrafficPolicy`、`AppReleaseGatePolicy`、probe、gate evaluate、promote、abort 等构件。

但这些能力还没有形成用户服务级的自动安全发布协议：

- 普通 app deploy 仍是直接 apply 新 desired state，再等待 rollout 完成。
- `AppRelease` / `TrafficPolicy` / `Gate` 当前更多是手动 API 能力，没有被 zero downtime deploy 自动编排。
- 当前 readiness 默认是 TCP probe，不能代表业务健康、数据库可用、关键 API 可用、流式响应可用。
- `fugue-drain-agent` 只负责旧 Pod 的连接 drain，不判断新 Pod / candidate release 是否健康。
- 如果新 Pod 完全起不来，`maxUnavailable=0` 通常能让旧 Pod 继续服务，但 Fugue 不会自动把 desired spec 回滚到旧版本。
- 如果新 Pod 短暂 Ready 后业务异常，现有 TCP readiness 可能放行 rollout，旧 Pod 仍可能进入 drain。

因此当前能力更准确地说是“滚动更新下线保护”，不是完整的“用户服务自动健康门禁 + 自动回滚”。

## 目标

当用户对某个服务显式开启 safe zero downtime rollout 后，Fugue 的用户服务发布语义变为：

```text
stable release 继续承载生产流量
new candidate release 独立创建和验证

candidate 未通过健康门禁前：
  不允许 retire stable
  不允许把全部生产流量切到 candidate
  不允许删除仍可能作为 rollback target 的旧 Pod / 旧 release

candidate 通过健康门禁后：
  可以按策略逐步接入流量
  只有 active connections = 0 且 candidate 持续健康时，旧 Pod 才能完成关闭

candidate 异常：
  自动 abort candidate
  生产流量回到 stable
  未关闭的旧 Pod / stable release 保留
  release attempt 失败并附带证据
```

最终用户可理解的语义：

```text
zero downtime = 不切断旧连接
safe zero downtime = 不切断旧连接 + 新版本异常不接管生产 + 可自动回退到旧版本
```

## 非目标

第一阶段不做以下事情：

- 不默认替用户服务开启 safe rollout；必须由用户或管理员显式开启。
- 不把 Fugue 平台发布硬门禁和用户应用 continuity 混在一起。
- 不为了某个 app、项目名、域名、镜像名、端口、环境变量写特判。
- 不要求所有用户应用接入 Fugue SDK。
- 不强制所有应用改成 sidecar request proxy。
- 不保证业务层数据迁移可无损回滚；数据迁移必须由用户显式声明 phase / hook / rollback policy。
- 不在 candidate 失败时删除或覆盖用户数据。
- 不绕过 Kubernetes 原生 Deployment 安全语义。

## 当前代码边界

### Runtime rollout 和 drain

相关代码：

- `internal/runtime/objects.go`
- `internal/runtime/strict_drain.go`
- `cmd/fugue-drain-agent/main.go`

当前行为：

- `deploymentStrategy(app)` 对可在线滚动的 app 返回 `RollingUpdate`。
- `rollingUpdateDeploymentStrategy()` 使用 `maxUnavailable=0`、`maxSurge=1`。
- `buildAppTCPReadinessProbe()` 默认只做 TCP readiness。
- `buildStrictZeroDowntimeDrainLifecycle()` 给业务容器注入 `preStop`。
- `fugue-drain-agent` 在 `/drain/prestop` 中观察 active TCP connections，归零并持续 quiet period 后返回。
- drain-agent metrics 包括 active connections、preStop count、early exit、timeout、observer errors、last wait。

缺口：

- drain-agent 不知道 candidate release 是否健康。
- Deployment controller 一旦给旧 Pod 设置 deletionTimestamp，无法可靠撤销删除。
- 只靠一个 Deployment 的 rolling update，难以保证“旧 Pod 在所有业务 gate 通过前完全不进入 termination”。

### Controller deploy path

相关代码：

- `internal/controller/controller.go`
- `internal/controller/managed_app_rollout.go`
- `internal/controller/app_rollout_capacity.go`
- `internal/controller/release_attempts.go`

当前行为：

```text
render bundle
preflight surge capacity
apply ManagedApp desired state
mark release attempt rolling_out
wait managed app rollout
complete operation / complete release attempt
```

失败时：

```text
FailOperation
failReleaseAttemptForOperation
collect evidence
```

缺口：

- deploy path 没有自动创建 candidate release。
- deploy path 没有在切流前自动 evaluate release gate。
- deploy path 没有在 gate fail 时自动调用 abort release。
- deploy path 没有自动把 desired spec 恢复到 previous stable snapshot。
- release attempt 状态没有明确区分 `candidate_creating`、`gate_checking`、`canarying`、`abort_rollback`。

### AppRelease / TrafficPolicy / Gate

相关代码：

- `internal/model/model.go`
- `internal/api/app_releases.go`
- `internal/api/edge_routes.go`
- `internal/store/app_release_sync.go`

当前行为：

- `AppRelease` 支持 `stable`、`candidate`、`previous`、`retired`。
- `AppTrafficPolicy` 支持 stable/candidate release、权重、sticky header/cookie。
- `AppReleaseGatePolicy` 支持窗口、candidate request count、5xx rate、edge upstream error rate、P95 TTFB、P99 duration、probe。
- `handlePromoteAppRelease` 可以设置 canary weight 或全量 promote。
- `handleAbortAppRelease` 可以把 traffic 回到 stable，并可把 candidate 标记 failed。
- `applyAppReleaseTraffic` 会在 candidate 不 ready 或缺少 upstream URL 时把 candidate weight 降到 0。
- completed deploy 后，如果已有 traffic policy 且 candidate weight 为 0，会同步当前版本为 stable release。

缺口：

- 这些 API 不是 app deploy 的自动路径。
- current stable sync 发生在 deploy 完成之后，不能在 deploy 前保护旧 stable。
- candidate release 目前没有对应的独立 runtime revision 生命周期。
- gate evaluate 是一次 API 调用，不是持续 watcher / controller。

## 推荐架构

采用 **Fugue-managed stable/candidate 两阶段发布**。

核心原则：

1. 发布前先固化 stable release snapshot。
2. 新版本先作为 candidate release 创建，不直接覆盖 stable。
3. candidate 有独立可寻址 upstream，便于 probe、canary 和 edge request attribution。
4. candidate 通过 gate 后才逐步接入生产流量。
5. old stable 的 drain / retire 必须同时满足：
   - 旧 Pod / stable release active connections 为 0。
   - candidate release 当前健康。
   - traffic policy 已经停止把新连接发给 stable。
6. candidate 异常时自动 abort，并保留 stable。

推荐数据流：

```text
user deploy new version
  -> create release_attempt
  -> snapshot current stable
  -> create candidate runtime revision
  -> wait candidate pod ready
  -> run active probes
  -> optional small canary traffic
  -> evaluate passive metrics gate
  -> promote candidate or abort
  -> drain stable only after candidate healthy
  -> retire previous stable after grace / rollback window
```

## Rollout 模式

新增 app-level policy：

```yaml
continuity:
  zero_downtime:
    enabled: true
    mode: safe
    strategy: stable_candidate
    canary:
      enabled: true
      initial_weight: 1
      max_weight: 100
      step_weights: [1, 5, 25, 50, 100]
      min_observation_seconds: 60
    gate:
      probes:
        - name: health
          kind: http
          method: GET
          path: /v1/health
          expected_status: 200
          timeout_ms: 3000
          max_duration_ms: 3000
      min_candidate_requests: 20
      max_5xx_rate: 0.01
      max_edge_upstream_error_rate: 0.005
      max_p95_ttfb_ms: 2000
      max_p99_duration_ms: 30000
    stable_retention:
      rollback_window_seconds: 1800
      require_no_active_connections: true
      require_candidate_healthy: true
```

兼容层：

- `mode: off`：保持当前行为。
- `mode: drain_only`：当前 strict drain 语义，只保护旧连接。
- `mode: safe`：启用本文 stable/candidate 两阶段发布。

默认策略：

- 新 app 默认不自动开启 `safe`，避免改变用户预期。
- 对平台官方关键服务可以由管理员显式开启。
- 对开启 failover / continuity 付费能力的用户服务，可以通过 CLI/UI 显式开启。

## Runtime 设计

### 第一阶段：复用现有单 Deployment，但加自动回滚

这是较小改动，但不是最终最强语义。

流程：

1. 发布前保存 previous stable spec snapshot。
2. 继续用现有 ManagedApp / Deployment rolling update。
3. rollout wait 失败时，controller 自动 apply previous spec。
4. previous spec 回滚也走 `RollingUpdate maxUnavailable=0 maxSurge=1`。
5. release attempt 记录 rollback start / complete / fail。

优点：

- 改动小。
- 可以快速补上“新 Pod 完全起不来时自动恢复 desired spec”。

缺点：

- candidate 与 stable 不是独立 runtime revision。
- 如果新 Pod 已经 Ready 并触发旧 Pod termination，无法保证旧 Pod deletion 可以撤销。
- 不能严格满足“gate 全部通过前旧 Pod 绝不关闭”。

结论：

- 可以作为过渡方案，但不能作为最终 safe rollout 的完整实现。

### 第二阶段：独立 stable/candidate runtime revision

这是推荐最终架构。

新增 runtime revision 概念：

```text
ManagedApp
  stable Deployment / Service
  candidate Deployment / Service
  traffic policy points edge route to stable/candidate upstreams
```

候选实现方式：

1. `app-<id>-stable` 和 `app-<id>-candidate` 两个 Deployment。
2. 两个 Deployment 使用同一 app spec 的不同 release snapshot。
3. stable Service 指向 stable Deployment。
4. candidate Service 指向 candidate Deployment。
5. Edge route bundle 根据 AppTrafficPolicy 生成 stable/candidate upstream weight。
6. Promote 后 candidate 变成 stable；旧 stable 进入 previous / draining / retired。

对 Kubernetes Service 的要求：

- stable/candidate 必须有可区分 labels。
- candidate service URL 必须写入 `AppRelease.UpstreamURL`。
- request facts 必须记录 `release_id` / `release_role`。
- old stable draining 阶段不接收新连接，只等待已有连接结束。

对有持久化存储的 app：

- 如果存储不支持双实例并发挂载，不能直接运行 stable + candidate 两个 Deployment。
- 对 `movable_rwo` / direct RWO，需要走同节点 online rollout 或明确降级为 `drain_only` / downtime-required。
- 对 `shared_project_rwx` 或明确支持并发访问的存储，可以使用 stable/candidate 双 revision。
- 对需要 migration 的 app，必须引入 release phase / migration policy，不能默认假设数据层可回滚。

## Controller 编排

新增 SafeRolloutCoordinator，放在 controller 层，负责把 release attempt、runtime apply、gate、traffic、rollback 串成一个状态机。

状态机：

```text
pending
  -> snapshot_stable
  -> create_candidate
  -> candidate_rollout_wait
  -> candidate_probe
  -> canary_shift
  -> passive_gate_wait
  -> promote
  -> stable_drain_wait
  -> retire_previous
  -> completed

failure from any pre-promote state
  -> abort_candidate
  -> restore_stable_traffic
  -> cleanup_candidate
  -> failed

failure after partial canary
  -> set candidate weight 0
  -> restore stable 100
  -> abort_candidate
  -> failed
```

关键不变量：

- 任何时候 traffic policy 至少有一个 healthy stable upstream，除非用户显式接受 downtime-required 发布。
- candidate 不 ready 时 candidate weight 必须为 0。
- gate fail 时不能 promote。
- stable 未 drain 完之前不能 retired。
- stable drain 期间如果 candidate 变 unhealthy，应暂停 retire 并恢复 stable traffic。
- release attempt 必须能解释当前卡在哪个 phase。

## Gate 设计

Gate 分成三类：

### 1. Runtime readiness gate

必须通过：

- candidate Deployment observed generation 匹配。
- candidate ready replicas 达到期望。
- candidate ready pod template 匹配本次 release key。
- candidate minReadySeconds 满足。
- candidate service endpoint 存在。
- candidate scheduling constraints 满足。

### 2. Active probe gate

默认 probe：

```text
GET /v1/health -> 200
timeout 3s
max duration 3s
```

用户可配置：

- HTTP method/path/header/body。
- expected status。
- expected content type。
- expected body contains。
- TTFB 上限。
- 总耗时上限。
- streaming first event 上限。

约束：

- probe 不能默认带 secret。
- 需要带认证的 probe 必须由用户显式配置 header secret 引用。
- probe 失败必须记录 response status、TTFB、duration、错误类别，但不记录敏感 body。

### 3. Passive metrics gate

基于 edge request facts：

- candidate request count。
- edge upstream error rate。
- 5xx rate。
- P95 TTFB。
- P99 duration。
- body read error rate。
- request body read throughput / slow read rate。
- TLS handshake error rate。
- cache correctness metrics 只在服务声明 cache 行为时启用。

默认第一版可复用现有 gate 指标：

- request count。
- 5xx rate。
- edge upstream error rate。
- P95 TTFB。
- P99 duration。
- active probes。

后续把 slow body read、body_read_error_rate 等 edge 质量指标接入 AppReleaseGatePolicy。

## 旧 Pod 关闭条件

旧 stable Pod / release 的最终关闭条件必须从单条件：

```text
active_connections == 0
```

升级为双条件：

```text
active_connections == 0
AND candidate_currently_healthy == true
```

实现注意：

- 不建议让 drain-agent 直接调用控制平面判断 candidate 健康。否则 preStop hook 会依赖控制平面可用性，风险过高。
- 推荐由 controller 决定何时进入 stable retire / scale down，而 drain-agent 只负责已经进入 termination 的 Pod 内连接观察。
- 因为 deletionTimestamp 不能可靠撤销，必须在 scale down / retire stable 之前判断 candidate healthy，而不是在 preStop 里才判断。
- drain-agent metrics 仍用于判断旧 Pod 是否可安全退出。

推荐判断流程：

```text
controller sees candidate gate pass
controller sets traffic policy candidate=100 stable=0
controller waits edge route bundle applied
controller waits stable no new requests window
controller waits stable drain-agent active_connections=0
controller re-checks candidate active probe and passive fast gate
controller retires previous stable
```

## 自动 abort / rollback

### Pre-promote 失败

如果 candidate 在 promote 前失败：

- candidate traffic weight 置 0。
- stable traffic 保持 100。
- candidate release 标记 `failed`。
- candidate runtime revision 保留一段 TTL 供排障，然后清理。
- release attempt 标记 failed。
- 不改 stable desired spec。

### Partial canary 失败

如果 candidate 已经拿到小比例流量后失败：

- 立即把 candidate weight 置 0。
- stable weight 恢复 100。
- edge bundle 发布并等待确认。
- candidate release 标记 `failed`。
- release attempt 标记 failed。
- candidate Pod 可保留 TTL 供排障。

### Post-promote 失败

如果 candidate 已经 promote 成 stable 后才发现异常：

- 创建 rollback release，指向 previous stable snapshot。
- rollback 不是手工覆盖 active state，而是一次新的可审计 release attempt。
- 如果 previous stable runtime revision 仍在 rollback window 内存在，可以直接恢复 traffic。
- 如果 previous stable runtime revision 已清理，则重新创建 previous spec 的 candidate，然后 promote。

### Rollback 失败

如果 rollback 本身失败：

- 创建 high severity operation evidence。
- 保持当前最健康 upstream 承载流量。
- 不自动做连续多次盲目回滚。
- 暴露 CLI/API 让管理员选择 force traffic、force rollback 或 downtime-required repair。

## API 方案

OpenAPI-first，先改 `openapi/openapi.yaml`。

新增或扩展：

### App continuity policy

```text
GET /v1/apps/{id}/continuity
PATCH /v1/apps/{id}/continuity
```

扩展 schema：

```yaml
safe_rollout:
  enabled: boolean
  mode: off | drain_only | safe
  strategy: rolling_update | stable_candidate
  canary:
    enabled: boolean
    initial_weight: integer
    step_weights: integer[]
    min_observation_seconds: integer
  gate:
    window_seconds: integer
    min_candidate_requests: integer
    max_5xx_rate: number
    max_edge_upstream_error_rate: number
    max_p95_ttfb_ms: integer
    max_p99_duration_ms: integer
    probes: AppReleaseProbe[]
  stable_retention:
    rollback_window_seconds: integer
    require_no_active_connections: boolean
    require_candidate_healthy: boolean
```

### Release attempt 状态

扩展 release attempt / step type：

- `snapshot_stable`
- `create_candidate`
- `candidate_rollout_wait`
- `candidate_probe`
- `canary_shift`
- `passive_gate_wait`
- `promote_candidate`
- `stable_drain_wait`
- `retire_previous`
- `abort_candidate`
- `restore_stable_traffic`
- `rollback_release`

### App release 状态

考虑扩展：

- `creating`
- `ready`
- `serving`
- `draining`
- `failed`
- `retired`

现有 `ready/serving/failed/retired` 可兼容；`draining` 是新语义。

### Gate 自动执行

新增内部 controller 能力，不一定第一版暴露 API：

```text
POST /v1/apps/{id}/releases/{release_id}/gate/evaluate
```

保留现有手动 API，同时 controller 可复用同一 evaluator。

## CLI 方案

新增命令：

```sh
fugue app continuity enable <app> --zero-downtime safe
fugue app continuity disable <app> --zero-downtime
fugue app continuity show <app>
fugue app continuity audit <app>
```

发布命令增强：

```sh
fugue app deploy <app> --wait
```

当 safe rollout enabled 时，输出 phase：

```text
snapshot stable release ... ok
create candidate release ... ok
candidate rollout ... ready
probe health ... pass
canary 1% ... pass
canary 5% ... pass
promote candidate ... ok
drain previous stable ... active_connections=0
retire previous stable ... ok
```

失败输出必须说明：

- 失败 phase。
- gate failure reason。
- candidate release id。
- stable release id。
- traffic 是否已恢复 stable。
- rollback / abort 状态。
- debug bundle / evidence id。

## Observability

新增 metrics：

```text
fugue_app_safe_rollout_attempts_total{status,phase}
fugue_app_safe_rollout_gate_failures_total{reason}
fugue_app_safe_rollout_abort_total{reason}
fugue_app_safe_rollout_rollback_total{status}
fugue_app_safe_rollout_candidate_weight{app_id,release_id}
fugue_app_safe_rollout_phase_duration_seconds{phase}
fugue_app_stable_drain_active_connections{app_id,release_id,pod}
fugue_app_stable_drain_wait_seconds{app_id,release_id,pod}
```

新增 audit event：

- `app.safe_rollout.enabled`
- `app.safe_rollout.disabled`
- `app.release.candidate.created`
- `app.release.gate.pass`
- `app.release.gate.fail`
- `app.release.canary.shifted`
- `app.release.abort.auto`
- `app.release.rollback.started`
- `app.release.rollback.completed`
- `app.release.previous.retired`

Debug bundle 必须包含：

- release attempt timeline。
- stable/candidate release records。
- traffic policy snapshots。
- gate policy 和 gate result。
- probe results。
- request facts summary。
- Kubernetes Deployment / Pod / Event evidence。
- drain-agent termination log / metrics 摘要。

## Backward compatibility

- 未开启 safe rollout 的 app 行为不变。
- 现有 `AppRelease` API 保持兼容。
- 现有 traffic policy 手动 canary 继续可用。
- 现有 zero downtime drain 继续作为 `drain_only` 模式存在。
- 对不支持 stable/candidate 双 revision 的存储类型，系统必须明确降级或拒绝 safe mode，而不是假装支持。
- 所有 API 新字段必须可选。
- 所有 CLI 输出新增字段不能破坏 `--json` 现有字段。

## 风险和应对

### 风险：stable/candidate 双 revision 导致资源不足

应对：

- safe rollout 前做 candidate capacity preflight。
- 资源不足时拒绝 safe rollout，提示用户扩容或选择 drain_only。

### 风险：持久化存储不支持并发运行

应对：

- 根据 storage spec 做 hard gate。
- 不支持并发的 app 先使用 rolling_update + auto rollback 过渡方案。
- 对需要真正双 revision 的 app，要求 RWX 或用户显式声明可并发。

### 风险：probe 不代表真实业务健康

应对：

- 默认 probe 只做基础健康。
- 用户可配置关键路径 probe。
- passive metrics gate 观察真实 candidate traffic。

### 风险：candidate 小流量伤害真实用户

应对：

- 初始权重默认很小。
- 可使用 sticky header / internal probe 模式先验证。
- canary 失败立即恢复 stable。

### 风险：controller / control plane 短暂不可用

应对：

- 已发布的 traffic policy 必须是数据面自洽的。
- stable 默认保留，不能因为 controller 不可用而被清理。
- retire previous stable 必须是可重试、幂等的异步阶段。

## 分阶段实施计划

### Phase 0：文档和语义冻结

目标：明确 safe zero downtime rollout 的用户语义、边界和不变量。

验收：

- 文档合并。
- 当前代码能力与缺口明确。
- 不改变线上行为。

### Phase 1：Policy / API / Store

目标：把 safe rollout policy 持久化到 app continuity 配置。

工作：

- 修改 OpenAPI。
- 增加 model。
- 增加 store 字段 / migration。
- 增加 API handler。
- 增加 CLI show/enable/disable。

验收：

- 用户可以查看和开启 safe rollout。
- 未开启的 app 行为不变。
- `make generate-openapi` 和 `make test` 通过。

### Phase 2：单 Deployment 自动 rollback 过渡保护

目标：先补上 rollout 失败自动恢复 previous spec。

工作：

- deploy 前 snapshot stable spec。
- rollout wait 失败时自动 apply previous spec。
- 记录 release attempt rollback step。
- 防止连续盲目 rollback。

验收：

- 新镜像拉取失败 / crashloop 时 operation fail，但 desired spec 自动恢复旧版本。
- 旧 Pod 在 `maxUnavailable=0` 下继续服务。

### Phase 3：Candidate release 自动创建和 gate

目标：复用 AppRelease / Gate，把 deploy 串到 candidate validation。

工作：

- deploy 创建 candidate release record。
- candidate rollout ready 后写 UpstreamURL。
- controller 自动运行 active probe gate。
- gate fail 自动 abort。

验收：

- candidate probe 失败时 stable traffic 不变。
- release attempt 展示 gate failure。

### Phase 4：Traffic canary 自动编排

目标：candidate 通过 active probe 后接入小流量真实请求。

工作：

- controller 设置 candidate initial weight。
- 等待 edge bundle 生效。
- 按 policy 观察 request facts。
- 通过后升权，失败后 abort。

验收：

- request facts 可按 release_id / release_role 区分。
- canary 失败恢复 stable 100。

### Phase 5：Stable/candidate 独立 runtime revision

目标：实现最终强语义，不在 gate 通过前终止 stable。

工作：

- runtime 支持 stable/candidate Deployment / Service。
- AppRelease.UpstreamURL 指向具体 revision service。
- traffic policy 控制 edge upstream weight。
- promote 后 candidate 接管 stable role。
- previous stable 进入 draining。

验收：

- candidate 未 gate pass 时旧 stable Pod 不进入 termination。
- candidate gate fail 时 stable Pod 原样保留。

### Phase 6：Drain 和 retire previous stable

目标：旧 Pod 关闭条件升级为“无连接 + candidate 健康”。

工作：

- controller 在 retire previous 前读取 drain-agent metrics / pod status。
- controller re-check candidate health。
- 满足双条件后 scale down / cleanup previous。
- previous 保留 rollback window。

验收：

- 长请求期间 previous stable 不被强制关闭。
- candidate 变 unhealthy 时 previous retire 暂停或 abort。

### Phase 7：Observability / CLI / Runbook

目标：让用户和管理员能一眼看到 safe rollout 卡在哪里。

工作：

- 增加 metrics。
- 增加 audit events。
- 增强 debug bundle。
- 增强 `fugue app continuity audit`。
- 增加 runbook。

验收：

- 每个失败都能定位到 phase、gate reason 和证据。
- CLI 不需要 SSH 就能解释大多数 rollout 卡顿。

## TODO

### 设计冻结

- [x] 确认 safe zero downtime rollout 的产品语义：`drain_only` 与 `safe` 分离。
- [x] 确认默认值：新 app 默认不开启 safe mode。
- [ ] 确认哪些用户套餐 / app 类型允许开启 safe mode。
- [x] 确认持久化存储 app 的支持矩阵：stateless、RWX、movable RWO、direct RWO、managed Postgres。
- [x] 确认 migration / data rollback 不属于默认 safe rollout 自动保证。

### OpenAPI / Model / Store

- [x] 在 `openapi/openapi.yaml` 增加 safe rollout continuity schema。
- [x] 扩展 `GET /v1/apps/{id}/continuity` 响应。
- [x] 扩展 `PATCH /v1/apps/{id}/continuity` 请求。
- [x] 生成 OpenAPI artifacts。
- [x] 扩展 `model.AppContinuityPolicy` 或等价模型。
- [x] 增加 store 持久化字段。
- [x] 增加 Postgres migration。
- [x] 增加 memory store 兼容。
- [x] 增加 policy normalization 和 validation。

### Controller 过渡保护

- [x] deploy 前 snapshot current stable spec。
- [x] rollout wait 失败时触发 previous spec restore。
- [x] rollback restore 必须创建 release step。
- [x] rollback restore 必须幂等。
- [x] rollback restore 失败必须附带 operation evidence。
- [x] 防止同一 release attempt 无限 rollback。
- [ ] 增加 crashloop / image pull failure 回归测试。

### Candidate Release

- [x] safe mode deploy 自动创建 candidate release。
- [x] candidate release 记录 source ref、resolved image、spec snapshot。
- [x] candidate release 记录 runtime id、deployment name、service name、upstream url。
- [x] candidate rollout ready 后更新 release status 为 `ready`。
- [x] candidate rollout 失败后更新 release status 为 `failed`。
- [x] release attempt timeline 纳入 candidate phases。

### Gate

- [x] 抽出 app release gate evaluator，供 API 和 controller 共用。
- [x] controller 自动运行 active probe gate。
- [x] gate fail 自动 abort candidate。
- [x] gate pass 记录 audit event 和 release step。
- [ ] gate failure 记录 evidence id。
- [x] 增加 default probe 可配置。
- [x] 增加 probe timeout / TTFB / duration 测试。

### Traffic Canary

- [ ] controller 可自动设置 candidate initial weight。
- [ ] controller 等待 edge route bundle 应用完成。
- [x] request facts 必须记录 release id / release role。
- [ ] controller 查询 candidate passive metrics。
- [ ] canary pass 后按 step_weights 升权。
- [ ] canary fail 后把 candidate weight 置 0。
- [x] candidate unavailable 时 edge route 必须 fail closed 到 stable。
- [x] 增加 edge route bundle 回归测试。

### Stable/Candidate Runtime Revision

- [x] 设计 revision resource name 规则。
- [x] runtime 渲染 stable Deployment / Service。
- [x] runtime 渲染 candidate Deployment / Service。
- [x] candidate revision labels 与 selector 独立。
- [x] AppRelease.UpstreamURL 指向 revision service。
- [ ] promote 后 role swap 不破坏现有 traffic policy。
- [ ] previous stable 进入 draining 状态。
- [ ] 清理 retired revision 时保留 rollback window。
- [x] 对不支持双 revision 的存储类型 hard gate。

### Stable Drain / Retire

- [ ] controller 在 retire previous 前读取 drain-agent metrics。
- [ ] controller 确认 previous stable 不再接收新请求。
- [ ] controller 等待 active connections 归零。
- [ ] controller 在 retire 前重新检查 candidate active probe。
- [ ] controller 在 retire 前重新检查短窗口 passive metrics。
- [ ] candidate unhealthy 时暂停 previous retire。
- [ ] previous retire 完成后记录 audit event。
- [ ] 增加长连接 / SSE / streaming 请求回归测试。

### CLI

- [x] 实现 `fugue app continuity enable <app> --zero-downtime safe`。
- [x] 实现 `fugue app continuity disable <app> --zero-downtime`。
- [x] 实现 `fugue app continuity show <app>` safe rollout 字段。
- [ ] 增强 `fugue app continuity audit <app>`。
- [ ] `fugue app deploy --wait` 展示 safe rollout phases。
- [x] JSON 输出保持稳定字段并增加可选字段。
- [ ] CLI failure 输出包含 gate reason、release id、evidence id。

### Observability / Debug Bundle

- [ ] 增加 safe rollout metrics。
- [ ] 增加 app release gate failure metrics。
- [ ] 增加 app release abort / rollback metrics。
- [ ] 增加 audit events。
- [x] debug bundle 包含 traffic policy snapshots。
- [x] debug bundle 包含 stable/candidate release records。
- [x] debug bundle 包含 probe results。
- [x] debug bundle 包含 drain-agent metrics summary。

### Tests

- [x] Unit test：policy validation。
- [x] Unit test：safe mode 默认不改变未开启 app。
- [ ] Unit test：candidate probe fail 自动 abort。
- [ ] Unit test：canary metrics fail 自动 abort。
- [x] Unit test：candidate unavailable 时 edge route fallback stable。
- [ ] Unit test：rollout wait failure 自动 restore previous spec。
- [ ] Integration test：stateless app safe rollout success。
- [ ] Integration test：long request during safe rollout 不被切断。
- [ ] Integration test：candidate crashloop stable 保留。
- [ ] Integration test：candidate Ready 后业务 probe fail stable 保留。
- [ ] Integration test：previous stable rollback window 内可恢复。

### Docs / Runbook

- [ ] 更新 `docs/deploy.md` continuity 章节。
- [ ] 新增 safe zero downtime rollout runbook。
- [ ] 更新 zero downtime incident 文档，说明新语义覆盖哪些历史缺口。
- [ ] 更新 release-phase hooks RFC，与 safe rollout phase 对齐。
- [ ] 更新 CLI help 示例。

### 发布

- [x] 第一阶段以 feature flag / shadow mode 发布。
- [ ] 在测试 app 上开启 safe mode。
- [ ] 验证 candidate gate pass / fail 都符合预期。
- [ ] 在低风险生产 app 上小范围开启。
- [ ] 观察 safe rollout metrics 至少 24 小时。
- [ ] 再开放给普通用户手动开启。
