# Fugue Release / Safe Rollout 架构收敛重构方案

最后更新：2026-07-07

本文把 Fugue 在引入用户服务 safe zero downtime stable/candidate 两阶段发布前，应该优先收敛和重构的代码面固定下来。目标是降低发布、流量、gate、rollback、debug/evidence 相关代码冗余，避免同一功能出现手动 API 一套实现、自动 controller 一套实现、平台 artifact 又一套实现。

本文只记录设计和 TODO，不改线上行为，不包含 secret。正式代码变更必须遵守 `fugue` 仓库 OpenAPI-first 流程和 GitHub Actions 控制平面发布链路。

## 背景

新增的 safe zero downtime rollout 方案要求 Fugue 把用户服务发布改成显式 stable/candidate 两阶段发布：

- 发布前保留 stable。
- 新版本先作为 candidate。
- candidate 通过 runtime readiness、active probe、passive metrics gate 后才逐步接流。
- candidate 异常时自动 abort。
- previous stable 只有在无连接且 candidate 持续健康时才 retire。

当前代码里已经存在很多相关构件：

- `AppRelease` / `AppTrafficPolicy` / `AppReleaseGatePolicy`
- edge route bundle 的 stable/candidate upstream 展开
- edge proxy 的 weighted release upstream 选择
- request facts 中的 `release_id` / `release_role`
- release attempt / release step / operation evidence
- ManagedApp rollout wait 和 Kubernetes evidence capture
- app continuity / app failover / database failover
- platform artifact 的 shadow/gray/full/LKG/rollback 账本
- DNS / edge quality 的 exploration 和 deterministic candidate selection

这些构件有价值，不能重写。但如果直接在 controller 中新增 `SafeRolloutCoordinator` 并重新实现 promote、abort、gate、traffic、debug bundle，很快会出现三套发布状态机：

```text
手动 AppRelease API
自动 SafeRolloutCoordinator
PlatformArtifact release / rollback
```

因此实现 safe rollout 前，应先做一轮架构收敛，把可复用的 release domain logic 抽出来。

## 总体目标

1. 手动 release API 和自动 safe rollout controller 使用同一套 release domain service。
2. AppRelease traffic plan 只有一个生成入口。
3. Release gate 只有一个 evaluator，API 和 controller 共用。
4. Release evidence / debug bundle / rollout timeline 使用统一 view 组装。
5. Runtime stable/candidate revision 复用现有 app renderer，不复制 Deployment 渲染逻辑。
6. Deterministic weighted selection 抽成公共工具，edge release canary 和 DNS exploration 共用基础算法。
7. PlatformArtifact release 的通用账本经验被复用，但不和 AppRelease 直接合表。

## 非目标

- 不把平台 artifact 和用户 app release 合成同一张表。
- 不把 edge DNS quality ranking 与 app release canary 合成同一个状态对象。
- 不让 `fugue-drain-agent` 直接调用控制平面判断 candidate health。
- 不把 app failover 直接改造成普通 app release。
- 不在这轮重构里改变线上默认发布行为。
- 不为了某个 app、hostname、project、region 或 node 写特判。

## 当前实现概览

### AppRelease API

主要文件：

- `internal/api/app_releases.go`
- `internal/store/app_releases.go`
- `internal/store/app_release_sync.go`
- `internal/api/edge_routes.go`
- `internal/edge/service.go`

当前行为：

- API handler 直接实现 `create release`、`patch traffic`、`probe`、`evaluate gate`、`promote`、`abort`。
- Store 提供 CRUD 和 policy upsert。
- Completed deploy 后，store 层会自动同步 current app 为 stable release。
- Edge route bundle 从 AppTrafficPolicy 和 AppRelease 展开 upstreams。
- Edge proxy 在请求时根据 upstream weight 和 sticky key 选择 stable/candidate。

问题：

- Promote / abort / gate 逻辑在 HTTP handler 中，不适合 controller 复用。
- Store 层自动 stable sync 会和 safe rollout 的 candidate/promote 流程冲突。
- Traffic plan 生成逻辑在 `internal/api/edge_routes.go`，不适合作为通用 domain logic。

### ManagedApp rollout

主要文件：

- `internal/controller/controller.go`
- `internal/controller/managed_app_rollout.go`
- `internal/controller/app_rollout_intent.go`
- `internal/controller/app_online_rollout.go`
- `internal/runtime/objects.go`

当前行为：

- Controller 对 deploy/migrate 进行 surge capacity preflight。
- Apply ManagedApp desired state。
- 等待 Deployment / ManagedApp / pod template / backing services ready。
- 失败时记录 evidence，并将 release attempt 标记 failed。
- Online rollout intent 通过 app spec diff 推断，用于决定是否可以 RollingUpdate、same-node pin、strict drain。

问题：

- Rollout wait 返回的是 `error` / message，缺少结构化结果。
- Online rollout intent 是发布能力判定的一部分，但还不是 release coordinator 的输入。
- Safe rollout stable/candidate revision 如果复制 renderer，会增加 Deployment/Service 渲染冗余。

### Operation evidence / release debug bundle

主要文件：

- `internal/api/operation_evidence.go`
- `internal/api/app_rollout_timeline.go`
- `internal/controller/operation_evidence_collector.go`
- `internal/controller/release_attempts.go`

当前行为：

- Operation debug bundle 组装 operation、app、tracking、metrics summary、diagnosis、timeline、evidence。
- Release debug bundle 组装 release attempt、app、tracking、metrics summary、release timeline、evidence。
- Rollout timeline 另行查询 operations、app events、request facts、drain logs、Kubernetes state。

问题：

- Debug bundle / timeline 有重复组装逻辑。
- Safe rollout 会新增 candidate/gate/canary/drain/rollback phases，如果继续分散拼装，排障视图会越来越不一致。

### PlatformArtifact release

主要文件：

- `internal/model/platform_state.go`
- `internal/store/platform_state.go`
- `internal/api/platform_artifacts.go`
- `internal/cli/admin_artifact.go`

当前行为：

- PlatformArtifact 支持 draft/validated/rejected。
- Release 支持 shadow/gray/full。
- Full release 更新 LKG。
- Rollback 通过发布旧 generation 完成。
- Release message 记录 release / rollback。

可借鉴：

- Active release supersede。
- LKG snapshot。
- Rollback target generation。
- Release message。
- Channel separation。

不应直接合并：

- PlatformArtifact 面向平台配置 / artifact。
- AppRelease 面向用户服务 runtime 和 traffic。
- 两者应共享 ledger pattern，而不是共享同一张实体表。

### App continuity / failover

主要文件：

- `internal/api/app_continuity.go`
- `internal/controller/app_failover.go`
- `internal/store/failover_spec.go`
- `internal/failover/app_assessment.go`

当前行为：

- App continuity API 主要管理 app failover 和 database failover。
- App failover controller 自己实现 fence lease、workspace final sync、source scale down、target apply、rollout wait、complete operation。
- FailoverDesiredSpec 会移动 app runtime，并消费 failover 配置。

问题：

- Failover 和 safe rollout 都需要 phase / evidence / rollback / traffic safety 视图。
- 但 failover 涉及 runtime 切换、volume replication、database primary，不能简单并入普通 release。

建议：

- 共享 phase recorder / evidence view / rollout wait result。
- 不合并状态机。

### DNS exploration / edge canary

主要文件：

- `internal/dnsserver/service.go`
- `internal/edge/service.go`
- `internal/api/edge_quality_rank.go`

当前行为：

- DNS answer 有 latency-aware ranking、node exploration、candidate promotion。
- Edge proxy 有 stable/candidate weighted upstream selection。
- 两者都使用稳定 hash / bucket 做小比例候选选择。

问题：

- Hash bucket / deterministic weighted selection 分散实现。
- 但 DNS exploration 与 app release canary 控制目标不同，不能共享业务状态。

建议：

- 抽通用 weighted selector utility。
- 保持 DNS policy 和 AppTrafficPolicy 独立。

## 推荐目标架构

新增几个内部包或服务层：

```text
internal/releaseflow/
  app_release_service.go
  traffic_plan.go
  gate_evaluator.go
  rollout_result.go
  evidence_view.go
  weighted_selector.go

internal/runtime/
  app_revision_render.go

internal/controller/
  safe_rollout_coordinator.go
```

包职责：

### AppReleaseService

统一处理 AppRelease domain logic。

职责：

- Create stable release snapshot。
- Create candidate release。
- Update candidate readiness。
- Promote candidate to stable。
- Abort candidate。
- Restore stable traffic。
- Mark previous draining / retired。
- Legacy sync current app to stable release。

调用方：

- HTTP API handler。
- SafeRolloutCoordinator。
- Store completed deploy legacy hook。
- CLI admin / automation。

### ReleaseTrafficPlanner

统一把 AppTrafficPolicy + AppRelease 转换为 EdgeRouteUpstream plan。

职责：

- 计算 stable/candidate 权重。
- candidate 不 ready 时 fail closed 到 stable。
- paused / single / canary 模式归一化。
- 生成 status reason。
- 生成 route generation 输入。

调用方：

- Edge route bundle。
- SafeRolloutCoordinator dry-run。
- CLI `fugue app traffic show/explain`。
- Debug bundle。

### ReleaseGateEvaluator

统一执行 active probe 和 passive metrics gate。

职责：

- Normalize AppReleaseGatePolicy。
- Run probes。
- Query request facts / rollups。
- 生成 gate result。
- 生成 failure reason。
- 生成 evidence payload。

调用方：

- `POST /v1/apps/{id}/releases/{release_id}/gate/evaluate`
- SafeRolloutCoordinator。
- Future scheduled canary watcher。

### RolloutReadinessResult

把 ManagedApp rollout wait 从纯 error 升级为结构化结果。

字段建议：

```go
type RolloutReadinessResult struct {
    Ready bool
    Phase string
    Message string
    Namespace string
    ManagedAppName string
    DeploymentName string
    ExpectedReleaseKey string
    CurrentReleaseKey string
    ReadyReplicas int
    DesiredReplicas int
    EvidenceID string
    FailureReason string
    SchedulingBlockReason string
}
```

用途：

- Release step summary。
- Debug bundle。
- CLI `--wait` 输出。
- SafeRolloutCoordinator decision。

### ReleaseEvidenceView

统一组装 release / operation 排障视图。

包含：

- operation。
- release attempt。
- release records。
- traffic policy snapshots。
- release timeline。
- operation evidence。
- gate results。
- app events。
- request facts summary。
- Kubernetes snapshots。
- drain-agent metrics summary。

### AppRevisionRenderer

在 runtime 层支持 stable/candidate/previous revision 渲染，但复用现有 Deployment/Service 生成逻辑。

职责：

- 生成 revision-specific resource name。
- 注入 revision labels / selectors。
- 生成 revision service。
- 复用 existing container/env/storage/drain/scheduling helpers。

避免：

- 复制 `buildAppDeploymentObjectWithOptions`。
- 复制 persistent storage / workspace / drain-agent 渲染。

### DeterministicWeightedSelector

通用稳定加权选择工具。

输入：

- candidates。
- weight。
- status。
- sticky key。
- fallback key。

输出：

- selected candidate。
- bucket。
- reason。

调用方：

- Edge release upstream selection。
- DNS exploration candidate promotion。
- Future canary analysis dry-run。

## 重构顺序

### 阶段 1：抽 AppReleaseService，不改变行为

目标：

- 先把 API handler 中的 release domain logic 下沉。
- API 行为不变。

迁移内容：

- `handlePromoteAppRelease` 里的 promote 逻辑。
- `handleAbortAppRelease` 里的 abort 逻辑。
- `ensureAppStableTrafficPolicy` / `ensureAppStableRelease` 相关 helper。
- `validateAppTrafficPolicyReferences`。
- `syncStableReleaseForCompletedDeploy` 的 legacy sync 逻辑。

验收：

- 所有 app release API 测试通过。
- Edge bundle tests 通过。
- 行为无 diff。

### 阶段 2：抽 ReleaseTrafficPlanner

目标：

- `applyAppReleaseTraffic` 从 `internal/api/edge_routes.go` 移出。
- Edge route bundle 和 safe rollout future dry-run 共用。

迁移内容：

- `applyAppReleaseTraffic`。
- `appReleaseCanReceiveEdgeTraffic`。
- release map / traffic policy map helper 可保留在 API，也可转移到 planner input adapter。

验收：

- candidate unavailable 仍 fail closed 到 stable。
- stable/candidate upstreams 与现有输出兼容。
- route generation 不出现非预期变化。

### 阶段 3：抽 ReleaseGateEvaluator

目标：

- API gate/probe 和 controller future gate 共用。

迁移内容：

- `normalizeAppReleaseGatePolicy`。
- `evaluateAppReleaseGate`。
- `queryAppReleaseGateMetrics`。
- `runAppReleaseProbes`。
- `runAppReleaseProbe`。

注意：

- evaluator 需要接口化 ClickHouse query 依赖。
- evaluator 不能直接依赖 HTTP handler。
- probe 默认值必须保持兼容。

验收：

- `probeAppRelease` 输出不变。
- `evaluateAppReleaseGate` 输出不变。
- 新增 evaluator unit tests。

### 阶段 4：结构化 rollout wait result

目标：

- 保留现有 wait 行为，但产出结构化 readiness result。

迁移内容：

- `waitForManagedAppRolloutWithScheduling` 内部收集 phase/message/evidence。
- 新增 `waitForManagedAppRolloutResultWithScheduling`。
- 旧函数包装新函数，保持调用兼容。

验收：

- 原有 rollout wait 测试通过。
- 新测试覆盖 pod failure / scheduling block / policy mismatch 的 result 字段。

### 阶段 5：统一 ReleaseEvidenceView

目标：

- operation debug bundle、release debug bundle、app rollout timeline 共享底层 view builder。

迁移内容：

- operation debug bundle 中的 app/tracking/metrics/evidence/timeline 拼装。
- release debug bundle 中的 app/tracking/metrics/evidence/timeline 拼装。
- rollout timeline 中的 operations/app_events/request_facts/Kubernetes/drain logs 查询适配成 view sections。

验收：

- API response schema 不破坏。
- Debug bundle 仍 redacted。
- Release attempt 可以看到 gate/canary/drain 预留 section。

### 阶段 6：Runtime revision renderer

目标：

- 为 stable/candidate 双 revision 做准备，避免复制 renderer。

迁移内容：

- 新增 revision render options。
- Deployment/Service resource name 可参数化。
- labels/selectors 可注入 `fugue.io/release-role`、`fugue.io/release-id`。
- current single Deployment path 使用默认 revision。

验收：

- 默认 render manifest 完全兼容。
- candidate revision render 只在新测试中启用。

### 阶段 7：SafeRolloutCoordinator 接入

目标：

- 基于已抽出的服务实现 safe rollout，不再新增第三套逻辑。

调用：

- AppReleaseService。
- ReleaseGateEvaluator。
- ReleaseTrafficPlanner。
- RolloutReadinessResult。
- AppRevisionRenderer。
- ReleaseEvidenceView。

验收：

- candidate 创建 / gate pass / promote / previous drain / retire 全流程有统一 release steps。
- candidate fail 自动 abort 并 restore stable traffic。
- old stable retire 前检查 no active connections + candidate healthy。

### 阶段 8：复用 PlatformArtifact ledger pattern

目标：

- 在 AppRelease 中借鉴 LKG / rollback target / release message，而不合并实体。

迁移内容：

- AppRelease 增加 rollback target / previous snapshot 语义。
- AppRelease promotion / rollback audit event 标准化。
- AppRelease 可以生成 release message 类似 artifact message。

验收：

- App rollback 是新的 release attempt，不是直接覆盖 active state。
- previous stable snapshot 可解释、可审计。

## 性能优化方案

### Edge route bundle 生成

当前风险：

- 每次 derive edge route bundle 都读取全量 apps/domains/runtimes/policies/releases/traffic policies。
- Safe rollout 增加 releases 后，全量 join 会更重。

优化方向：

- 建立 active app release index。
- 只把 active stable/candidate/previous draining release 纳入 bundle。
- route bundle 可以从预计算 PlatformArtifact / route plan artifact 读取。
- AppReleaseService 在 traffic policy 变化时生成 route plan invalidation signal。

### Gate metrics 查询

当前风险：

- Gate evaluate 查询 raw `request_facts`。
- 自动 canary 会高频查询。

优化方向：

- 优先使用 rollups。
- 没有 rollup 时 fallback raw facts。
- 针对 release_id / release_role 增加低基数聚合。
- Gate evaluator 支持 cache 窗口，避免同一 release 短时间重复查询。

### Debug bundle

当前风险：

- Debug bundle metrics summary 做全局 counts。
- Release 数量增加后，默认 debug bundle 可能变重。

优化方向：

- 默认按 app/release scoped summary。
- global summary 仅 admin 或 `include_global_summary=true`。
- 大 payload section 支持 lazy / link。

### Weighted selection

当前风险：

- DNS exploration 和 edge release canary 各自实现 hash/bucket。
- 行为差异会让排障困难。

优化方向：

- 抽通用 selector。
- 输出 bucket/reason，便于 trace。
- 保持业务状态和 policy 独立。

## 重构边界和保留项

### 不合并 app failover 状态机

原因：

- failover 涉及 runtime 切换。
- failover 涉及 volume replication final sync。
- failover 涉及 fence lease。
- failover 可能消费 database failover topology。

可共享：

- phase recorder。
- rollout wait result。
- evidence view。
- rollback/audit naming。

### 不让 drain-agent 感知 candidate health

原因：

- preStop hook 不应依赖控制平面。
- 控制平面短暂不可用时，drain-agent 仍应完成本地连接 drain。
- candidate health 应在 controller 决定 scale down / retire 前检查。

可共享：

- drain-agent metrics 被 ReleaseEvidenceView 和 SafeRolloutCoordinator 读取。

### 不合并 PlatformArtifact 和 AppRelease 表

原因：

- PlatformArtifact 是平台配置发布。
- AppRelease 是用户 runtime/traffic 发布。
- 生命周期相似但实体边界不同。

可共享：

- release ledger helper。
- LKG / rollback pattern。
- audit event naming。

### 不合并 edge quality ranking 和 app release gate

原因：

- edge ranking 选择“哪个 edge 处理请求”。
- app release gate 选择“哪个应用版本处理请求”。
- 指标可能重叠，但控制目标不同。

可共享：

- metrics extraction helpers。
- weighted selector。
- fallback / confidence pattern。

## 代码结构建议

### 新增 `internal/releaseflow`

建议文件：

```text
internal/releaseflow/app_release_service.go
internal/releaseflow/app_release_service_test.go
internal/releaseflow/traffic_plan.go
internal/releaseflow/traffic_plan_test.go
internal/releaseflow/gate_evaluator.go
internal/releaseflow/gate_evaluator_test.go
internal/releaseflow/rollout_result.go
internal/releaseflow/evidence_view.go
internal/releaseflow/weighted_selector.go
internal/releaseflow/weighted_selector_test.go
```

### API 层保留职责

API handler 只负责：

- auth / scope check。
- request decode。
- path/query 参数解析。
- 调用 service。
- audit append。
- response encode。

不再直接负责：

- release role/status mutation。
- traffic policy mutation。
- gate evaluation internals。
- rollback target decision。

### Controller 层保留职责

Controller 负责：

- operation loop。
- Kubernetes apply / wait。
- safe rollout state machine。
- background reconcile。

但 release domain mutation 通过 AppReleaseService 完成。

### Store 层保留职责

Store 负责：

- 持久化。
- normalization。
- optimistic conflict。
- list/get/upsert primitives。

不再负责复杂业务编排，例如 safe mode 下的 stable sync。

## 迁移风险

### 风险：抽 service 时破坏现有 API 行为

控制：

- 第一阶段只移动代码，不改 OpenAPI。
- 增加 golden tests。
- 对比 promote/abort/traffic patch 响应。

### 风险：store auto stable sync 与 safe rollout 冲突

控制：

- 先加 mode guard。
- Legacy 模式保持现状。
- Safe mode 下由 coordinator 显式调用 sync/promote。

### 风险：edge route generation 改变导致全量 bundle churn

控制：

- Traffic planner 输出保持字段顺序和默认值兼容。
- 增加 route generation regression tests。

### 风险：gate evaluator 抽离后无法访问 Server 方法

控制：

- 定义窄接口：
  - request facts query
  - now
  - HTTP client
  - logger
- API Server 只作为 adapter。

### 风险：runtime revision render 引入命名冲突

控制：

- revision resource name 必须 DNS-safe。
- 默认 revision name 与现有 resource name 完全一致。
- candidate/previous 只在 safe mode 开启。

## TODO

### 文档和设计冻结

- [x] 确认本重构方案与 `docs/app-safe-zero-downtime-rollout-plan.md` 的边界一致。
- [x] 确认 release domain service 的包名和依赖方向。
- [x] 确认第一阶段只做行为等价重构，不改变线上发布行为。
- [x] 确认 safe rollout 前必须先完成 AppReleaseService / GateEvaluator / TrafficPlanner 抽离。
- [x] 确认不把 PlatformArtifact 和 AppRelease 合表。

### AppReleaseService

- [x] 新增 `internal/releaseflow` 包。
- [x] 定义 `AppReleaseStore` 窄接口。
- [x] 定义 `AppReleaseService`。
- [x] 搬迁 stable release ensure 逻辑。
- [x] 搬迁 candidate create helper。
- [x] 搬迁 promote 逻辑。
- [x] 搬迁 abort 逻辑。
- [x] 搬迁 restore stable traffic 逻辑。
- [x] 搬迁 traffic policy reference validation。
- [x] 搬迁 legacy stable sync 逻辑。
- [x] API handler 改为调用 AppReleaseService。
- [x] Store completed deploy hook 改为调用 legacy sync adapter 或受 mode guard 控制。
- [x] 添加 AppReleaseService unit tests。

### ReleaseTrafficPlanner

- [x] 抽出 `ReleaseTrafficPlanner`。
- [x] 从 `internal/api/edge_routes.go` 移出 `applyAppReleaseTraffic`。
- [x] 抽出 candidate availability 判断。
- [x] 保持 candidate unavailable fail closed 到 stable。
- [x] 保持 single / paused / canary 兼容。
- [x] 输出 stable/candidate upstream plan。
- [x] 输出 status reason。
- [x] Edge route bundle 改为调用 planner。
- [x] 添加 route generation regression tests。
- [x] 添加 candidate missing upstream tests。

### ReleaseGateEvaluator

- [x] 定义 `ReleaseGateEvaluator`。
- [x] 定义 ClickHouse/query 窄接口。
- [x] 搬迁 gate policy normalization。
- [x] 搬迁 active probe runner。
- [x] 搬迁 passive metrics query。
- [x] 搬迁 failure reason builder。
- [x] API `probe` 改为调用 evaluator。
- [x] API `gate/evaluate` 改为调用 evaluator。
- [x] 添加 probe timeout tests。
- [x] 添加 5xx / upstream error / TTFB / duration failure tests。
- [x] 添加 metrics unavailable warning tests。

### RolloutReadinessResult

- [x] 定义 `RolloutReadinessResult`。
- [x] 新增 `waitForManagedAppRolloutResultWithScheduling`。
- [x] 旧 `waitForManagedAppRolloutWithScheduling` 包装 result API。
- [x] result 包含 ready phase。
- [x] result 包含 expected/current release key。
- [x] result 包含 evidence id。
- [x] result 包含 scheduling block reason。
- [x] result 包含 pod failure reason。
- [x] 更新 release step summary 使用结构化结果。
- [x] 添加 result unit tests。

### ReleaseEvidenceView

- [x] 定义 `ReleaseEvidenceViewBuilder`。
- [x] 抽出 operation debug bundle 公共组装逻辑。
- [x] 抽出 release debug bundle 公共组装逻辑。
- [x] 抽出 rollout timeline section builder。
- [x] 支持 release records section。
- [x] 支持 traffic policy snapshots section。
- [x] 支持 gate result section。
- [x] 支持 drain metrics summary section。
- [x] 默认保持 redaction。
- [x] 添加 API response compatibility tests。

### Runtime Revision Renderer

- [x] 定义 app revision role：default/stable/candidate/previous。
- [x] 定义 revision render options。
- [x] Resource name 支持 revision suffix。
- [x] Labels 支持 release role / release id。
- [x] Selectors 支持 release role / release id。
- [x] Service name 支持 revision suffix。
- [x] 默认 render path 输出与当前一致。
- [x] Candidate render path 复用现有 container/env/storage/drain helpers。
- [x] 添加 stateless candidate render tests。
- [x] 添加 persistent storage support matrix tests。

### Deterministic Weighted Selector

- [x] 抽出 weighted selector utility。
- [x] 支持 active candidate filter。
- [x] 支持 weight sum。
- [x] 支持 sticky key。
- [x] 支持 deterministic bucket。
- [x] Edge release upstream selection 改用 utility。
- [x] DNS exploration 逐步改用 utility。
- [x] 保持现有分布测试通过。
- [x] 添加 bucket explain tests。

### PlatformArtifact ledger pattern

- [x] 梳理 PlatformArtifact release/LKG/rollback 可复用 helper。
- [x] 设计 AppRelease rollback target 字段。
- [x] 设计 AppRelease release message / audit event。
- [x] 保持 AppRelease 和 PlatformArtifact store 独立。
- [x] 添加 rollback target tests。

### App failover 边界

- [x] 标记 app failover 不并入 AppRelease state machine。
- [x] 复用 RolloutReadinessResult。
- [x] 复用 ReleaseEvidenceView 的 operation section。
- [x] failover phase 记录改为标准 release/operation step 格式。
- [x] 保持 fence lease / final sync 逻辑独立。

### SafeRolloutCoordinator 接入前置检查

- [x] 确认 AppReleaseService 已被 API 使用。
- [x] 确认 ReleaseGateEvaluator 已被 API 使用。
- [x] 确认 ReleaseTrafficPlanner 已被 edge bundle 使用。
- [x] 确认 rollout wait result 已可用于 controller。
- [x] 确认 runtime revision renderer 默认兼容。
- [x] 确认 debug bundle 可展示 release/gate/traffic sections。

### 性能优化

- [x] 为 active app releases 增加 store 查询或 index。
- [x] Edge route bundle 只加载 active stable/candidate/previous draining releases。
- [x] Gate evaluator 优先读取 rollups。
- [x] Gate evaluator raw facts fallback 有窗口限制。
- [x] Debug bundle global summary 变成可选。
- [x] Release scoped metrics summary 替代默认 global count。
- [x] 增加 bundle generation benchmark。
- [x] 增加 gate evaluation benchmark。

### 测试

- [x] AppRelease API 行为等价测试。
- [x] AppReleaseService promote tests。
- [x] AppReleaseService abort tests。
- [x] Legacy stable sync tests。
- [x] Safe mode 禁用 auto stable sync tests。
- [x] Traffic planner tests。
- [x] Gate evaluator tests。
- [x] Rollout result tests。
- [x] Evidence view compatibility tests。
- [x] Runtime revision render tests。
- [x] Weighted selector distribution tests。
- [x] Edge bundle generation regression tests。

### 发布

- [x] 第一阶段只发布内部重构，不开启 safe rollout。
- [x] 跑 `make generate-openapi`，如没有契约变化应无 diff。
- [x] 跑 `make test`。
- [x] 推送 main 后观察 control plane deploy。
- [x] 对现有 AppRelease API 做 smoke test。
- [x] 对 edge route bundle 做 smoke test。
- [x] 对 release debug bundle 做 smoke test。
- [x] 确认线上 traffic policy 不发生非预期变化。
