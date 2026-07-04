# Fugue Zero Downtime Rollout 修复记录

最后更新：2026-06-12

本文记录本轮围绕 `uni-api` 在 Fugue 上实现 zero downtime rollout 过程中发现的问题、线上现象、调查过程、证据和修复思路。本文不记录任何 secret、API key 或私有环境变量值。

## 范围

- 目标业务：`uni-api-web` / `uni-api-ember`
- 重点路径：`fugue app deploy`、`fugue app restart --wait`、`fugue app fs put --source live` 之后的 ManagedApp/Deployment rollout
- 重点风险：带持久化存储的 app 在重启、镜像更新、资源更新、生命周期配置更新时是否会被降级为 `Recreate`，以及 `--wait` 是否会在真正可服务之前提前返回

## 总结

本轮问题不是 `uni-api-web` 业务代码的 503/504 bug。主要原因在 Fugue 的 ManagedApp rollout 控制面：

1. 带 `movable_rwo` 持久化存储的 app 默认会走 `Recreate`，但部分变更类型本应可以在同一节点使用 `RollingUpdate maxUnavailable=0 maxSurge=1` 完成。
2. 操作驱动的 online rollout 意图曾经只存在于内存对象，后台 reconciler 或 stale snapshot 可能把它覆盖掉，导致线上 Deployment 被重新渲染为 `Recreate`。
3. `--wait` 曾经只看 Deployment release key 和 readiness，不足以证明 Kubernetes 已经应用了本次期望的 ManagedApp spec、Deployment strategy 和 pod template。
4. 对长连接、慢镜像拉取、stale deploy operation、默认值归一化等场景的处理不完整，会让“看起来已完成”的 rollout 在边界条件下仍然出现短暂不可用。

## 关键线上事件：2026-06-11 23:31:29

### 现象

用户执行：

```sh
fugue app fs put uni-api-ember /home/api.yaml \
  --source live \
  --from-file /Users/yanyuming/Downloads/GitHub/uni-api/api-fugue.yaml \
&& fugue app restart uni-api-ember --wait
```

随后 `uni-api-web` 在 2026-06-11 23:31:29 Asia/Shanghai 附近出现 503/504。业务侧错误形态包括：

```text
unexpected status 503 Service Unavailable: {"detail":"upstream unavailable"}
```

### 调查过程

1. 查询 Fugue operation，确认该次 restart operation 的创建、开始、完成时间。
2. 查询控制平面记录的 app desired spec，确认镜像、replica、runtime、持久化存储和 restart token。
3. SSH 到控制平面节点查看 Kubernetes ManagedApp、Deployment、Pod 的 live 状态。
4. 对齐 `uni-api-web-api` 日志中的 request id、HTTP status 和 downstream app。
5. 比对 operation 完成时间与实际新 pod Ready 时间，确认 `--wait` 是否提前返回。

### 证据

- Operation：`op_1781191757_9ad985c3279b`
- Operation 时间：
  - created：`2026-06-11T15:29:17.035831Z`
  - started：`2026-06-11T15:29:20.121648Z`
  - completed：`2026-06-11T15:29:34.652904Z`
  - controller `rollout_wait`：约 `6059ms`
- 线上 ManagedApp / Deployment：
  - ManagedApp generation：`94`
  - `spec.rolloutIntent: null`
  - Deployment generation：`124`
  - Deployment strategy：`Recreate`
  - annotation `fugue.io/rollout-mode=isolated-singleton`
  - annotation `fugue.io/downtime-class=downtime-required`
  - annotation `fugue.io/rollout-reason=single-writer-storage`
- 新 Pod：
  - pod：`app-1780014428-4ccd706744ad-857f8d944c-fn9xx`
  - created / started：`2026-06-11T15:31:26Z`
  - Ready：`2026-06-11T15:31:31Z`
- `uni-api-web-api` 日志：
  - request id：`fd5e54f2-8032-4cb1-b817-d960dfc4b228`
  - 503 出现在 `2026-06-11T15:31:27.825Z` 到 `2026-06-11T15:31:29.901Z` 附近
  - downstream：`uni-api-ember`
  - 15:30:00Z 到 15:33:30Z 窗口内，503 约 284 条，504 约 196 条，未见 502/500 为主因

### 精确原因

该次 `restart --wait` 的 operation 在 `15:29:34Z` 已经完成，但真正替换出来的新 pod 在 `15:31:31Z` 才 Ready。中间窗口里 Deployment 使用了 `Recreate`，因此旧 pod 已经不可用、新 pod 尚未 Ready，`uni-api-web` 调用 downstream `uni-api-ember` 时得到 upstream unavailable，表现为 503/504。

这不是 `uni-api-web` 业务 bug，而是 Fugue rollout 控制面的 bug：

- `RolloutIntent` 在 `model.AppSpec` 中是 transient 字段，JSON 不持久化。
- operation 期间的 online restart 意图没有被可靠保存到 ManagedApp CR。
- reconciler 后续按存储态 app 重算时，看到持久化存储后重新渲染出 `Recreate`。
- release key 主要覆盖 pod template，不覆盖 Deployment strategy 和部分 rollout policy。
- `--wait` 没有验证 ManagedApp `observedGeneration`、`lastAppliedSpecHash`、期望 Deployment strategy 和实际 ready pod 是否完全匹配本次期望。

### 修复思路

1. `--wait` 不能只等 Deployment ready，还必须确认 ManagedApp 已经观测并应用了本次 desired spec。
2. 对 online durable rollout，必须验证 Deployment strategy 是 `RollingUpdate maxUnavailable=0 maxSurge=1`，不能接受 `Recreate`。
3. ready pod 必须与本次 Deployment pod template 匹配，不能把旧 template 的 ready pod 当成本次 rollout 成功。
4. reconciler 不能在 active operation 期间用旧的 stored app snapshot 覆盖 operation 写入的 online rollout 意图。

对应修复提交：

- `2b8d6b0636801681689c8e6eb830c88339d39c1d` - `Harden online managed app rollout waits`
- `efaa4af` - `Fix managed app online durable rollouts`

验证：

- clean 环境 `make test` 通过。
- GitHub Actions control-plane deploy run `27360010579` 成功。
- 线上 control plane `api` 和 `controller` 已运行镜像 tag `2b8d6b0636801681689c8e6eb830c88339d39c1d`。
- 新版本部署后抽查 `uni-api-web-api` 日志，503/504 在观测窗口内为 0。

## 修复批次 1：支持资源类 zero downtime rollout

提交：

- `4244ba6` - `Support zero-downtime resource rollouts`

### 问题

带持久化存储的 app 默认被视为 single-writer storage。历史逻辑为了避免两个 pod 同时跨节点挂载同一块 RWO 卷，会把 Deployment 渲染为 `Recreate`。但资源类变更，例如 CPU、memory、replica 约束内的 pod spec resource 更新，本身不一定需要停机。

### 现象

对使用持久化存储的 app 做资源更新时，旧 pod 可能先被删除，新 pod 后创建，导致短暂无 ready endpoint。对请求量高的 app，这会直接表现为 upstream 503/504。

### 调查过程

1. 检查 runtime 渲染出的 Deployment strategy。
2. 比对 app spec diff，确认部分变更只是 resource-only。
3. 检查 ready pod 所在节点，确认可以将新 pod 约束到同一节点，避免 RWO 卷跨节点并发挂载风险。

### 证据

代码和测试证据来自该提交：

- 新增 online resource rollout intent。
- runtime 测试覆盖持久化存储 app 的资源更新应使用 `RollingUpdate`。
- pod 调度约束覆盖同节点放置，避免跨节点 RWO 风险。

### 修复思路

为 resource-only 变更新增明确的 online rollout intent。在持久化存储 app 已有 ready pod 的情况下，将新 pod pin 到当前 ready pod 所在节点，并使用：

```text
RollingUpdate
maxUnavailable=0
maxSurge=1
```

这样 Kubernetes 会先拉起新 pod，等新 pod Ready 后再移除旧 pod。

## 修复批次 2：长请求 drain

提交：

- `4e241e5` - `Drain long requests during online rollouts`

### 问题

即使 Deployment strategy 是 rolling update，如果 termination grace 太短，旧 pod 被终止时仍然可能切断长请求或流式请求。对 `/v1/responses` 这类可能有较长生命周期的请求，这会让客户端在 rollout 期间看到失败。

### 现象

rollout 期间 service endpoint 不是完全空，但正在旧 pod 上执行的长请求可能被终止，客户端侧表现为请求中断、503 或 504。

### 调查过程

1. 检查 app lifecycle / termination grace 配置是否能通过 Fugue API 更新。
2. 检查 runtime 是否把 termination grace 渲染到 pod spec。
3. 检查 online rollout intent 是否覆盖 lifecycle-only 变更。

### 证据

代码和测试证据来自该提交：

- API patch 和 OpenAPI schema 覆盖 lifecycle / termination grace。
- runtime 和 controller 测试覆盖 lifecycle 变更下的 online rollout。

### 修复思路

把 termination grace 作为正式 app lifecycle 配置进入模型和 API，并在符合条件时走 online rollout。旧 pod 收到 termination 后保留足够 drain 时间，让已有请求自然完成。

## 修复批次 3：reconciler 保留 online rollout 意图

提交：

- `88ece1f` - `Preserve online managed app rollouts during reconcile`

### 问题

operation 写入的 online rollout intent 如果只存在于内存模型，后台 reconciler 再次根据 stored app 渲染 ManagedApp 时，会丢失该 intent。对带 RWO 持久化存储的 app，丢失 intent 后就会回到默认 `Recreate`。

### 现象

operation 启动时看起来准备做 online rollout，但中途或后续 reconcile 可能把 Deployment 改回 `Recreate`。最终表现为旧 pod 先停、新 pod 后起。

### 调查过程

1. 查看 `model.AppSpec.RolloutIntent` 的序列化行为。
2. 查看 ManagedApp CR spec 是否包含 rollout intent。
3. 查看 reconciler 在 active operation 期间选择 desired app snapshot 的逻辑。

### 证据

代码和测试证据来自该提交：

- ManagedApp CRD 增加 rollout intent 字段。
- ManagedApp runtime object 与 app model 之间支持 intent 回读。
- reconciler 测试覆盖 active operation 期间保留 online rollout snapshot。

### 修复思路

将 operation 的 online rollout intent 写入 ManagedApp CR，使它成为 Kubernetes 侧可观测、可恢复的 rollout 状态。reconciler 在 active operation 期间优先保留当前 ManagedApp 的 desired snapshot，而不是无条件使用 stored app baseline 覆盖。

## 修复批次 4：source drift 下保留 rollout snapshot

提交：

- `c9ba317` - `Keep online rollout snapshots across source drift`

### 问题

Fugue 会注入一些运行时环境变量和 source/build 相关字段。stored app 与 ManagedApp snapshot 在这些字段上可能存在非用户意图的 drift。如果直接做深度比较，reconciler 会误判 snapshot 不一致，从而放弃保留正在进行的 online rollout。

### 现象

用户没有做会影响运行时兼容性的变更，但 active online rollout snapshot 仍可能被判定为与 stored app 不一致，随后被覆盖。

### 调查过程

1. 比对 stored app、ManagedApp desired snapshot 和 Fugue 注入字段。
2. 区分用户可控 spec 差异与 Fugue 运行时注入差异。
3. 检查 active operation 下 snapshot preserve 的判定条件。

### 证据

代码和测试证据来自该提交：

- 测试覆盖 source drift 场景下仍保留当前 online rollout snapshot。
- 比较逻辑排除 Fugue 注入环境变量，必要时回填 source 以进行等价判断。

### 修复思路

比较 snapshot 时只比较用户意图相关字段。对 Fugue 注入的环境变量和 source/default 字段做归一化，避免把平台注入差异误判为用户变更。

## 修复批次 5：归一化 online rollout snapshot

提交：

- `3714e51` - `Normalize online rollout snapshots for reconcile`

### 问题

stored app 和 ManagedApp snapshot 中一些字段可能一个是省略值，一个是默认值。语义相同，但普通 deep equal 会判断为不同，导致 online rollout snapshot 无法被保留。

### 现象

没有真实用户意图变化，reconciler 仍认为 snapshot 漂移，可能覆盖 online rollout 状态。

### 调查过程

1. 比对 defaulted spec 与 raw spec。
2. 检查 app defaults 应用顺序。
3. 检查 snapshot 比较是否在同一归一化层级上进行。

### 证据

代码和测试证据来自该提交：

- 引入可比较 snapshot 的 normalization。
- 测试覆盖默认值差异不应破坏 online rollout preserve。

### 修复思路

在比较前统一应用 app spec defaults 和 normalization，让“省略默认值”和“显式默认值”被视为同一用户意图。

## 修复批次 6：跳过 stale deploy snapshot

提交：

- `bf50456` - `Skip stale deploy snapshots`

### 问题

队列中较旧的 deploy operation 可能在较新的 operation 完成之后才继续执行。如果不识别 stale operation，旧 deploy 会把过期 desired snapshot 重新应用到线上，造成回滚、重复 rollout 或短暂不可用。

### 现象

用户看到较新的配置已经生效，但随后又被旧 operation 写回。对高流量服务，这类意外重复 rollout 可能造成 503/504。

### 调查过程

1. 检查 operation queue 和 completed deploy operation 的时间顺序。
2. 比对 stale operation 的 desired snapshot 与当前 stored app。
3. 判断是否存在更新的 completed deploy 已经代表当前真实用户意图。

### 证据

代码和测试证据来自该提交：

- 新增 stale deploy guard。
- operation controller 在执行 deploy 前检查是否已有更新 deploy 完成。

### 修复思路

在 deploy operation 真正应用前做 stale 检查。如果当前 app 状态已经由更新的 operation 产生，旧 operation 直接标记为 skipped/completed，不能再把旧 snapshot 应用到 runtime。

## 修复批次 7：加强 online rollout readiness

提交：

- `c27dbc9` - `Harden managed app online rollout readiness`

### 问题

只看 Deployment status ready 不足以证明本次 rollout 成功。Deployment 可能仍然有旧 template 的 ready pod，或者 status 是上一轮留下的稳定状态。此时 `--wait` 返回会误导用户认为新配置已经无损生效。

### 现象

CLI 显示 operation 完成，但随后 Kubernetes 才真正开始替换 pod，业务流量在替换窗口出现 503/504。

### 调查过程

1. 比对 ready pod 与 Deployment pod template。
2. 检查 pod label、annotation、image、resource、termination grace 是否与本次 template 匹配。
3. 检查 wait 逻辑是否会接受 stale ready pod。

### 证据

代码和测试证据来自该提交：

- 引入 ready pod 与 Deployment template 的匹配校验。
- 检查 pod container image、resource、label、annotation、termination grace。
- rollout wait 不再把旧 template 的 ready pod 当作本次成功。

### 修复思路

`--wait` 的成功条件必须绑定本次目标 template。只有存在与本次 Deployment template 完全匹配的 ready pod，且 Deployment 自身状态也稳定，才能认为 rollout 完成。

## 修复批次 8：延长 rollout deadline

提交：

- `4ded22a` - `Extend managed app rollout deadlines`

### 问题

跨区域节点、首次拉镜像或镜像缓存不热时，pod 创建可能很慢。过短的 rollout timeout / progress deadline 会把正常的慢启动判定为失败，进而触发错误恢复或重复操作。

### 现象

镜像拉取仍在进行，但 operation 已经超时失败。用户可能再次触发 restart/deploy，放大 rollout 抖动。

### 调查过程

1. 检查 ManagedApp rollout timeout。
2. 检查 Kubernetes Deployment `progressDeadlineSeconds`。
3. 对照跨区域镜像拉取耗时和 operation 超时时间。

### 证据

代码和测试证据来自该提交：

- `DefaultManagedAppRolloutTimeout` 延长到 1 小时。
- Deployment `progressDeadlineSeconds` 调整到 3600 秒。

### 修复思路

把 Fugue operation timeout 与 Kubernetes Deployment progress deadline 对齐到更适合跨区域部署的上限，避免慢镜像拉取被误判为业务失败。

## 修复批次 9：严格校验 ManagedApp 应用状态和 Deployment 策略

提交：

- `2b8d6b0636801681689c8e6eb830c88339d39c1d` - `Harden online managed app rollout waits`

### 问题

此前 wait 逻辑不足以证明 Kubernetes 已经应用了本次 operation 期望的 ManagedApp spec，也不足以证明 Deployment strategy 符合 online durable rollout 要求。尤其在 release key 不覆盖 strategy 的情况下，`Recreate` Deployment 可能被错误接受。

### 现象

`fugue app restart --wait` 返回成功，但 live Deployment 后续仍以 `Recreate` 执行，业务出现断流。

### 调查过程

1. 对比 operation 完成时间和 live pod Ready 时间。
2. 对比 ManagedApp `observedGeneration`、`lastAppliedSpecHash` 与 operation desired spec。
3. 对比期望 Deployment strategy 与 live Deployment strategy。
4. 对比 ready pod 与本次 template。

### 证据

见上文“关键线上事件：2026-06-11 23:31:29”。

代码和测试证据来自该提交：

- 新增 Deployment strategy 解析和 online strategy 验证。
- wait 逻辑校验 ManagedApp observed/apply hash。
- wait 逻辑校验 expected child Deployment。
- 测试覆盖 online rollout 期间不能接受 `Recreate`。

### 修复思路

将 wait 成功条件提升为完整不变量：

1. ManagedApp 已观测到本次 generation。
2. ManagedApp last applied spec hash 等于本次 desired spec hash。
3. 子 Deployment 已存在且归属本次 ManagedApp。
4. 对 online durable rollout，Deployment 必须是 `RollingUpdate maxUnavailable=0 maxSurge=1`。
5. ready pod 必须匹配本次 Deployment template。

## 修复批次 10：修复 image-only durable app online rollout

提交：

- `efaa4af` - `Fix managed app online durable rollouts`

### 问题

image-only deploy 是最常见的生产发布路径之一。带持久化存储的 app 如果 image-only 变更没有被纳入 online durable rollout intent，仍然可能走默认 `Recreate`。此外，reconciler 如果使用 stale ManagedApp 对象继续 apply，也可能覆盖刚写入的 online rollout intent。

### 现象

镜像发布或重新构建后，即使配置上应该允许 online replacement，也可能出现旧 pod 先下线、新 pod 后 Ready 的窗口。

### 调查过程

1. 检查 deploy operation 对 image-only diff 的识别。
2. 检查 online durable rollout reason 是否覆盖 image-only。
3. 检查 reconciler apply 前是否重新读取最新 ManagedApp，避免用 stale informer 对象覆盖新 intent。
4. 检查 active operation 期间 stored app baseline 是否还能覆盖 live ManagedApp snapshot。

### 证据

代码和测试证据来自该提交：

- 新增 `AppRolloutIntentOnlineImageUpdate`。
- runtime 测试覆盖带持久化存储 app 的 image-only update 使用 `RollingUpdate`。
- reconciler 在 active operation 期间跳过会覆盖 ManagedApp 的 reconcile。
- apply 前 refresh 最新 ManagedApp，再选择 desired snapshot。

### 修复思路

把 image-only deploy 纳入 online durable rollout 的正式 intent，与 restart/resource/lifecycle 变更一样使用同节点 rolling replacement。reconciler 在 active operation 期间不能覆盖 operation 正在驱动的 ManagedApp 状态，并且 apply 前必须基于最新 Kubernetes 对象做决策。

## 当前 zero downtime 不变量

对符合 online durable rollout 条件的 ManagedApp，Fugue 必须同时满足以下不变量：

1. Rollout intent 必须可持久化到 ManagedApp CR，不能只存在于 operation 内存对象。
2. active operation 期间，reconciler 不能用 stale stored app baseline 覆盖 operation 写入的 live snapshot。
3. 对 restart-only、resource-only、lifecycle-only、image-only 这类可在线替换的变更，带 RWO 持久化存储的 app 应使用同节点 `RollingUpdate maxUnavailable=0 maxSurge=1`。
4. 对真正需要独占迁移的变更，例如存储迁移、PVC 绑定变化、runtime cluster 迁移，仍应明确标记为 downtime-required，不能假装 zero downtime。
5. `--wait` 必须验证 ManagedApp 应用状态、Deployment strategy、Deployment template 和 ready pod，而不是只看 Deployment ready。
6. 对高流量长请求服务，termination grace 必须足够让旧请求 drain，否则即使 endpoint 不为空，也可能中断存量请求。
7. strict drain 的 600 秒语义是 hard deadline，不是每次 rollout 的固定等待时间；connection-aware 模式下旧 Pod 没有 active connection 且持续 quiet period 后应快速退出。
8. connection-aware drain 必须通过 Pod 内 `fugue-drain-agent` native sidecar 观测业务端口 TCP connection；观测失败时 fail-closed，不能把“无法观测”当作“无连接”。
9. `fixed-sleep` 必须保留为生产回滚开关；如果 native sidecar capability 未启用或 drain-agent image 配置不可用，runtime renderer 必须回退到固定 `preStop.sleep`。
10. 控制平面普通升级不能仅因为 drain-agent image tag 跟随 Fugue commit 变化而重滚全部 app；发布脚本默认保留 live controller 中的 drain-agent image，只有 drain-agent 源码/镜像变化时才允许更新该 image。

## Connection-aware drain runbook

本节用于 rollout 期间精确判断旧 Pod 为什么退出，不允许用猜测替代证据。

### 判断当前 drain mode

优先用 CLI：

```sh
fugue app diagnose <app>
```

输出中的 evidence 应包含类似：

```text
evidence=strict drain mode=connection-aware timeout_seconds=600 quiet_period_seconds=2 agent_port=19090 termination_grace_min_seconds=630
```

如果 CLI 不可用，可只读查看 live Deployment：

```sh
kubectl -n <tenant-namespace> get deploy <app-deployment> \
  -o jsonpath='{.spec.template.metadata.annotations.fugue\.io/drain-mode}{"\n"}'
```

关键 annotation：

- `fugue.io/drain-mode`
- `fugue.io/drain-timeout-seconds`
- `fugue.io/drain-quiet-period-seconds`
- `fugue.io/drain-agent-port`
- `fugue.io/termination-grace-min-seconds`

### 判断最近一次旧 Pod 退出原因

优先用 CLI：

```sh
fugue app diagnose <app>
```

如果该 app 当前或最近的 Pod 仍可查询到 drain-agent 日志，输出中的 evidence 应包含类似：

```text
evidence=strict drain recent result pod=<pod> ... fugue_drain_complete reason=idle waited_ms=3200 active_connections=0 max_active_connections=2 observer_errors=0
```

`reason` 判读：

- `idle`：旧 Pod 的业务端口 active connection 已归零，并且持续超过 quiet period；这是无连接快速退出的正常路径。
- `timeout`：active connection 或观测失败持续到 hard deadline；Pod 最多等待 600 秒后进入应用 SIGTERM/shutdown。
- `observer_error_open`：仅在明确配置 fail-open 时出现；生产默认不应使用。
- `context_canceled`：preStop HTTP 请求上下文被取消，需要结合 kubelet / Pod event 判断是否达到 termination grace 或外部强制删除。

如果 CLI 没取到最近日志，可只读查看 Pod 日志：

```sh
kubectl -n <tenant-namespace> logs <pod> -c fugue-drain-agent --tail=100
```

关键日志：

```text
fugue_drain_start ...
fugue_drain_sample active_connections=<n> states=<state-counts> waited_ms=<ms>
fugue_drain_complete reason=<idle|timeout|context_canceled> waited_ms=<ms> active_connections=<n> max_active_connections=<n> observer_errors=<n>
fugue_drain_observer_error ...
```

### 期望 Kubernetes 形态

connection-aware strict drain app 的 Deployment 应保持：

```text
strategy=RollingUpdate
maxUnavailable=0
maxSurge=1
terminationGracePeriodSeconds>=timeout+buffer
minReadySeconds=10（或生产配置值）
```

Pod template 应包含：

- `initContainers[].name=fugue-drain-agent`
- `initContainers[].restartPolicy=Always`
- 业务容器 `lifecycle.preStop.httpGet.path=/drain/prestop`

如果出现 drain-agent image pull failure，新 Pod 不会 Ready，但 `maxUnavailable=0` 应保护旧 Pod 不被删除；此时应先修复 image 可拉取性或将 `runtime.strictDrain.mode` 回滚为 `fixed-sleep`，不要手工删除旧 Pod。

## 已执行验证

- 本地 clean 环境 `make test` 通过。
- 首次 `make test` 失败原因是本机 shell 中存在生产 R2 环境变量，导致 data backend 测试尝试访问真实 R2；清理相关环境变量后测试通过。该失败不是代码回归。
- control plane 标准发布链路已跑通，GitHub Actions control-plane deploy 成功。
- 线上 control plane `api` / `controller` 更新到修复镜像后 Ready。
- 修复后抽查 `uni-api-web-api` 503/504，在观测窗口内为 0。

## 后续回归建议

每次修改 ManagedApp rollout 相关逻辑时至少覆盖以下回归：

1. 持久化存储 app 的 restart-only rollout。
2. 持久化存储 app 的 image-only rollout。
3. 持久化存储 app 的 resource-only rollout。
4. lifecycle / termination grace 更新。
5. active operation 期间 reconciler 不覆盖 live ManagedApp snapshot。
6. `--wait` 不接受 `Recreate` 作为 online durable rollout 成功条件。
7. ready pod 必须匹配本次 Deployment template。
8. stale deploy operation 不得重新应用旧 snapshot。
