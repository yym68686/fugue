# 自动 right-sizing 重复 deploy 导致 strict zero downtime 失效修复方案

本文固定 `2026-07-05 22:16:20–22:19:52 CST` 期间 `0-0 -> uni-api-ember` 503 事件的精确根因、修复方案、验证方案和可打勾 TODO。本文不包含 secret，不包含一次性线上热修步骤，不引入任何项目名 / app 名特判。所有代码修复必须回到 `fugue` 仓库，通过 `main` 分支和 GitHub Actions 控制平面发布链路上线。

## 事故结论

本次不是 `0-0` 前端 / 后端业务代码问题，也不是 `uni-api` 业务代码主动返回 503；本次是 Fugue 控制面自动 right-sizing 与 ManagedApp rollout 策略组合出的平台 bug。

精确原因：

1. Fugue API 当前有多个 replicas，每个 replica 都启动了自动 right-sizing loop。
2. 自动 right-sizing 在创建 deploy operation 前的 `appHasActiveDeployOperation` 是“先查再创建”，没有和 `CreateOperation` 放在同一个数据库事务 / 临界区中。
3. `Store.CreateOperation` / `pgCreateOperation` 对普通 deploy 没有同 app active deploy 排他约束。
4. 因此两个 API replicas 在约 1 秒内为同一个 app 创建了两个相同的 `system / fugue-api/right-sizing/downscale` deploy operation。
5. 第一个 operation 被正确识别为 `online_resource_update`，渲染为 `RollingUpdate / online-required`。
6. 第一个 operation 完成后，第二个 operation 的 desired spec 已经等于 current app spec；控制器重新计算 rollout intent 时得到空 intent。
7. 对带 `movable_rwo` 持久化存储的 app，空 rollout intent 会被渲染为 `Recreate / downtime-required / single-writer-storage`。
8. 第二个重复 no-op deploy 因此把刚创建的新 ReplicaSet 也缩到 0；旧 Pod drain 600 秒到达 hard deadline 后，直到新 Pod 再次创建并 ready 前，service 没有可用业务 Pod，`0-0` 调用下游 `uni-api-ember` 出现 503。

## 线上证据摘要

时间统一使用 UTC，括号内为北京时间 CST。

| 证据 | 时间 | 内容 |
| --- | --- | --- |
| 第一个 right-sizing deploy 创建 | `2026-07-05T14:05:58.405Z` (`22:05:58`) | `op_1783260358_4c4dcf07c531`, `requested_by_type=system`, `requested_by_id=fugue-api/right-sizing/downscale` |
| 第二个 right-sizing deploy 创建 | `2026-07-05T14:05:59.504Z` (`22:05:59`) | `op_1783260359_3f76afaf334f`, 同 app、同 requested_by、同 desired spec |
| 第一个 deploy rollout decision | `2026-07-05T14:06:02.368Z` (`22:06:02`) | `strategy=RollingUpdate`, `downtime_class=online-required`, `reason=resource-only`, `rollout_intent=online_resource_update` |
| 第一个 deploy 完成 | `2026-07-05T14:06:19.502Z` (`22:06:19`) | `managed app reconciled` |
| 第二个 deploy rollout decision | `2026-07-05T14:06:20.558Z` (`22:06:20`) | `strategy=Recreate`, `downtime_class=downtime-required`, `reason=single-writer-storage`, `rollout_intent=""` |
| 新 RS 被缩掉 | `2026-07-05T14:06:20Z` (`22:06:20`) | `Scaled down replica set ...-5c99cc45cd from 1 to 0` |
| 旧 Pod drain 结果 | `2026-07-05T14:16:19Z` (`22:16:19`) | `reason=timeout waited_ms=600314 active_connections=14 max_active_connections=16 observer_errors=0` |
| 新 Pod 再创建 | `2026-07-05T14:19:42Z` (`22:19:42`) | `Scaled up replica set ...-66fdb9b9c7 from 0 to 1` |
| 新 Pod 启动完成 | `2026-07-05T14:19:48Z` (`22:19:48`) | app logs: `Application startup complete`, `Uvicorn running on 0.0.0.0:8000` |
| 0-0 侧恢复 | `2026-07-05T14:19:50–14:19:52Z` (`22:19:50–22:19:52`) | `uni-api-ember` 开始重新返回 `200 OK` |

drain-agent 本身没有“误判无连接”。旧 Pod 的 drain-agent 明确看到连接仍然存在并等满 600 秒：

```text
reason=timeout waited_ms=600314 active_connections=14 max_active_connections=16 observer_errors=0
```

所以本次 strict zero downtime 失效的直接触发点不是 drain-agent，而是第二个重复 no-op right-sizing deploy 被错误应用成 `Recreate`。

## 当前代码缺陷

### 缺陷 1：自动 right-sizing loop 多副本并发，没有全局 single-flight

当前每个 `fugue-api` 进程都会执行：

```go
startRightSizingAutoApplyLoop(ctx)
```

这意味着 API Deployment 有 2 个 replicas 时，两个进程都可能在同一轮扫描同一个 app，并都调用 `applyAutoAppRightSizingRecommendation`。

### 缺陷 2：active deploy 检查不是原子的

自动 right-sizing 现在依赖：

```go
hasActive, err := s.appHasActiveDeployOperation(app)
```

该函数只是读取 operation 列表并在内存里判断 pending/running/waiting。随后才调用 `CreateOperation`。两个 API replicas 可以同时读到“没有 active deploy”，然后分别创建 operation。

### 缺陷 3：普通 deploy operation 创建没有同 app active 排他

`pgCreateOperation` 对 `failover` / `database_switchover` / `database_localize` 这类操作有 in-flight count 检查，但 `OperationTypeDeploy` 没有同 app active deploy 排他检查。

历史上这允许用户连续 push / import 后由 stale deploy guard 跳过旧 deploy；这个行为可以保留。但系统自动 right-sizing 这种 background deploy 不应该并发排队。

### 缺陷 4：deploy desired state 已经等于 current app 时仍继续 apply

`completeStaleDeployOperationIfNeeded` 只处理“有更新的 completed deploy 已经把 app 更新到另一个 desired state”的情况。它遇到：

```go
deployOperationDesiredStateMatchesApp(op, currentApp) == true
```

会直接返回 `false`，让控制器继续渲染和 apply。

这对普通无状态 app 通常只是无意义 apply；但对带 `movable_rwo` 的 app，如果这次执行没有被识别为 online durable rollout，就可能渲染为 `Recreate`。

### 缺陷 5：缺少“background autoscaler 不得制造 downtime”的 fail-safe

对系统自动 right-sizing 来说，即使有其他 bug 导致 rollout intent 为空，也应该 fail closed：跳过 / 失败，而不是把一个正在服务的持久化 app 用 `Recreate` 方式重滚。

## 修复目标

1. 自动 right-sizing 不能为同一个 app 并发创建多个 deploy operation。
2. 已经排队的重复 / no-op deploy 不能触发 Kubernetes apply，更不能触发 `Recreate`。
3. 对 `movable_rwo` 持久化 app，right-sizing 这种资源类变更必须保持 `RollingUpdate maxUnavailable=0 maxSurge=1`，除非是明确需要 downtime 的存储拓扑变化。
4. 不影响用户显式 deploy / import 的正常队列语义；用户连续 push 仍可由已有 stale deploy guard 收敛。
5. 所有跳过、冲突、no-op、fail-safe 都必须被记录到 operation / logs / rollout timeline，便于下次精确调查。

## 非目标

- 不把所有 deploy 全局改成“有 active deploy 就拒绝”。这可能改变用户连续 deploy / import 的既有语义。
- 不对 `uni-api-ember`、`0-0`、任何具体 app 名称写特判。
- 不关闭所有 right-sizing 能力作为永久修复。
- 不把本应 downtime-required 的真实存储迁移伪装成 zero downtime。

## 分层修复方案

### 第一层：自动 right-sizing 全局 single-flight

在 API 多副本部署下，自动 right-sizing scan 应该只有一个进程执行。

推荐实现：

1. 在 `applyAutoRightSizingOnce` 外层增加一个数据库 lease / advisory lock，例如逻辑名：

   ```text
   background:right-sizing:auto-apply
   ```

2. 获取 lease 成功的 API replica 才执行本轮扫描；获取失败的 replica 直接跳过并输出低噪声日志。
3. lease TTL 应小于 `rightSizingAutoApplyInterval`，例如 10–20 分钟；进程崩溃后下一轮可恢复。
4. 即使实现了全局 single-flight，仍必须保留第二层 per-app 原子保护，因为：
   - 手动 API 请求可能和 loop 并发。
   - 未来可能还有其他 background producer。
   - single-flight 本身可能因为数据库/网络短暂异常不可用。

验收标准：同一轮自动 right-sizing 中，同一 app 只会被一个 Fugue API replica 处理。

### 第二层：right-sizing 创建 operation 时做 per-app 原子 guard

新增一个明确的 store/API 创建路径，用于 background autoscaler，而不是继续直接调用通用 `CreateOperation`。

推荐接口方向：

```go
type OperationCreatePolicy struct {
    RejectActiveDeployForApp bool
    RejectNoopDeploy         bool
    IdempotencyScope         string
}

CreateOperationWithPolicy(op model.Operation, policy OperationCreatePolicy) (model.Operation, OperationCreateDecision, error)
```

或者更窄地新增：

```go
CreateAutoscalingDeployOperation(op model.Operation) (model.Operation, AutoscalingOperationDecision, error)
```

实现要求：

1. 对 Postgres store：
   - 在同一个事务里 `SELECT ... FROM fugue_apps WHERE id=$1 FOR UPDATE` 锁住 app 行。
   - 重新 hydrate app 和 backing services。
   - 在事务里查询该 app 是否已有 pending/running/waiting deploy。
   - 如果有 active deploy，返回 benign decision：`active_deploy_exists`，不创建新 operation。
   - 在事务里比较 desired spec/source 与当前 app spec/source；如果已经一致，返回 benign decision：`already_current`，不创建新 operation。
   - 只有确认没有 active deploy 且 desired state 不等于 current app，才插入 operation。
2. 对 file store：
   - 复用 `withLockedState(true, ...)`，在同一个锁内执行 active/no-op 检查和 append operation。
3. 对自动 right-sizing 调用方：
   - `active_deploy_exists` 和 `already_current` 都视为 `alreadyCurrent=true`，不报错，不制造 operation。
   - 记录 structured log：`right_sizing_auto_apply_skipped app_id=... reason=active_deploy_exists|already_current existing_operation_id=...`。

为什么必须在 store 层做：只有 store 层能把“查 active operation”和“创建 operation”放在同一个事务 / 锁里；API 层先查再创建永远有 race。

### 第三层：controller 执行时增加 generic no-op deploy skip

即使 queue-time guard 漏掉，controller 执行 deploy 时也必须安全。

在 `executeManagedOperation` 的 deploy 分支里，在计算 rollout intent、render、apply 之前增加：

```text
如果 op.DesiredSpec + DesiredSource + DesiredOriginSource 已经等于 currentApp：
  CompleteManagedOperationWithSourceState(...)
  result_message = "deploy skipped because desired state is already current"
  log operation_event action=completed/noop
  不调用 render
  不调用 applyManagedAppDesiredState
  不等待 rollout
```

注意事项：

- 这是 generic no-op 保护，不限于 right-sizing。
- restart-only 不会被误跳过，因为 restart token 不同则 spec 不相等。
- config-only / image-only / resource-only 只要真的有差异，就不会被跳过。
- 如果 desired spec 已经与 current app 完全一致，继续 apply 没有业务价值，跳过是最安全行为。

这层直接防止“第二个重复 deploy 当前已相等但仍 apply 成 Recreate”。

### 第四层：background autoscaler downtime fail-safe

对 `RequestedByType=system` 且 `RequestedByID` 属于 right-sizing 的 deploy，增加额外安全约束：

1. 如果 app 有 cluster service、replicas > 0、当前有 ready pod，且变更属于资源类 / no-op / 可在线类变更，则不能应用 `Recreate / downtime-required`。
2. 如果 runtime renderer 得到 `Recreate / downtime-required`，controller 必须：
   - 优先 no-op skip（如果 desired 已当前）。
   - 否则 fail operation，并写明：

     ```text
     right-sizing deploy refused because it would require downtime for a serving app
     ```

3. 真实 storage migration / PVC 迁移仍必须保持 downtime-required，但不应由 auto right-sizing producer 发起。

这层是 fail-closed 保护：宁愿跳过一次自动 right-sizing，也不能由后台调参导致线上服务断流。

### 第五层：rollout intent 和 ManagedApp 渲染不变量测试

补充测试确保：

1. resource-only change + movable_rwo app -> `online_resource_update`。
2. online resource update + movable_rwo app -> Deployment `RollingUpdate maxUnavailable=0 maxSurge=1`。
3. no-op deploy + movable_rwo app -> controller skip，不渲染 `Recreate`。
4. auto right-sizing duplicate queued -> 第二个不会 apply。

### 第六层：观测性补强

每次自动 right-sizing 做出以下决策都应可查：

- `queued_operation`
- `active_deploy_exists`
- `already_current`
- `recommendation_not_ready`
- `change_below_threshold`
- `billing_cap_blocked`
- `downtime_refused`

推荐落点：

1. API 日志：结构化输出 app_id、tenant_id、requested_by_id、decision、target resources 摘要。
2. operation event：如果已经有 operation，则记录 operation event；如果没有 operation，可记录 app-level telemetry event。
3. `app rollout timeline`：展示 right-sizing decision，尤其是 skipped/blocked/no-op。
4. `app diagnose`：如果最近一次 right-sizing 被跳过，输出简短证据，避免误以为系统没工作。

## 最小代码落点

预计涉及文件：

- `internal/api/right_sizing.go`
  - 使用新的原子创建路径。
  - 对 benign decision 做 skip 处理。
  - 增加 structured log / telemetry。
- `internal/store/store.go`
  - file store 版原子 create-with-policy。
- `internal/store/postgres_store.go`
  - Postgres 版事务内 app row lock + active deploy check + no-op check。
- `internal/controller/controller.go` 或 `internal/controller/stale_deploy_guard.go`
  - deploy desired state already current 时直接 complete skip。
- `internal/controller/*_test.go`
  - no-op deploy skip 测试。
- `internal/api/right_sizing_test.go`
  - 自动 right-sizing active/no-op skip 测试。
- `internal/store/*_test.go`
  - create-with-policy active deploy guard 测试。
- 可选：`internal/model/operation_evidence.go`、rollout timeline 相关文件
  - 如果新增 right-sizing decision 事件类型。

## 发布与验证计划

1. 本地实现后先跑窄测试：

   ```sh
   go test ./internal/store ./internal/api ./internal/controller
   ```

2. 再跑全量测试：

   ```sh
   make test
   ```

3. 提交并 push 到 `main`。
4. 等 GitHub Actions 控制平面发布完成。
5. 线上验证 Fugue 控制面版本更新。
6. 线上观察：
   - `fugue-api` pods 正常 ready。
   - 自动 right-sizing 日志没有为同一 app 同一轮创建重复 operation。
   - 对 `uni-api-ember` 再次触发 right-sizing / redeploy 时，timeline 不能出现由 right-sizing 触发的 `Recreate / downtime-required`。
   - `0-0` 请求不再出现同类 503 窗口。

## 回滚策略

如果发布后发现自动 right-sizing 误跳过必要调整：

1. 不回滚 strict drain。
2. 优先仅回滚 / 修正 right-sizing create policy。
3. 可以临时将受影响 app 的 `right_sizing.mode` 设置为 `disabled`，避免后台自动 deploy；这只是临时止血，不是长期修复。
4. 不允许通过手工 patch 线上 Deployment 来作为长期修复。

## TODO

### A. 复现与测试

- [x] 增加测试：两个相同自动 right-sizing deploy 不能在 active deploy 存在时同时创建。
- [x] 增加测试：desired state 已经等于 current app 的 deploy 会被 controller no-op skip。
- [x] 增加测试：no-op deploy skip 不会调用 renderer / Kubernetes apply。
- [x] 增加测试：restart-only deploy 不会被 no-op skip 误伤。
- [x] 增加测试：movable_rwo resource-only change 仍渲染为 `RollingUpdate maxUnavailable=0 maxSurge=1`。
- [x] 增加测试：right-sizing producer 遇到 active deploy 返回 benign skip，不记录 error。
- [x] 增加测试：right-sizing producer 遇到 already-current 返回 benign skip，不创建 operation。

### B. Queue-time 原子保护

- [x] 设计并实现 `CreateOperationWithPolicy` 或 `CreateAutoscalingDeployOperation`。
- [x] Postgres store 在同一事务中锁 app 行、检查 active deploy、检查 no-op、创建 operation。
- [x] file store 在同一 `withLockedState` 中完成相同语义。
- [x] 自动 right-sizing 改用新的原子创建路径。
- [x] active/no-op skip 返回明确 decision，而不是模糊 error。

### C. Controller 执行时 fail-safe

- [x] 在 deploy 执行路径增加 “desired state already current” skip。
- [x] skip 时调用 operation complete，写入明确 result message。
- [x] skip 时保留 current app spec/source，不触发 render/apply/wait。
- [x] 对 right-sizing system deploy 增加 downtime-refused fail-safe。
- [x] 确保真实需要 downtime 的手动存储迁移不被误拒绝。

### D. Single-flight 与观测性

- [x] 为 `applyAutoRightSizingOnce` 增加全局 lease / advisory lock，避免多个 API replicas 同轮扫描。
- [x] right-sizing 每个 app 决策输出 structured log。
- [x] right-sizing skipped / blocked / queued 进入 rollout timeline 或 app-level telemetry。
- [x] `app diagnose` 能展示最近一次 right-sizing skip / blocked / queued 证据。

### E. 验证与发布

- [x] 运行 `go test ./internal/store ./internal/api ./internal/controller`。
- [x] 运行 `make test`。
- [x] 提交代码。
- [x] push 到 `main`，由 GitHub Actions 发布控制面。
- [x] 监控 `fugue-api` / `fugue-controller` 新版本 ready。
- [x] 监控 `uni-api-ember` 后续 right-sizing / deploy timeline，确认不再出现同类重复 deploy + Recreate。
- [x] 监控 `0-0` 503 请求，确认没有复现同类窗口。
