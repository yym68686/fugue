# Connection-aware strict zero-downtime drain 方案

本文把 Fugue strict zero-downtime rollout 从“固定等待 600 秒”升级为“旧 Pod 没有实际连接就尽快退出；有连接则等待连接自然结束；600 秒只作为硬上限”的完整设计、实施步骤、验证计划和可打勾 TODO 固定下来。

本文只记录方案，不包含 secret、不包含线上私有环境变量、不要求手工 patch 线上集群。控制平面和 runtime 相关变更仍必须通过 `fugue` 仓库 `main` 分支和 GitHub Actions 控制平面发布链路上线。

## 背景

当前 Fugue strict zero-downtime app rollout 的核心语义是：

- Deployment 使用 `RollingUpdate`。
- `maxUnavailable=0`，保证新 Pod 未就绪前旧 Pod 不被 Kubernetes 主动降容量。
- `maxSurge=1`，允许新旧 Pod 短暂共存。
- strict drain app 会给业务容器加 `preStop sleep 600`。
- strict drain app 会把 `terminationGracePeriodSeconds` 提升到至少 `630`。
- strict drain app 当前还会把 `minReadySeconds` 设置为 `600`。

当前线上 0-0 API app 的实际 manifest 也符合以上语义：

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxUnavailable: 0
    maxSurge: 1
minReadySeconds: 600
template:
  spec:
    terminationGracePeriodSeconds: 630
    containers:
      - name: uni-api-web-api
        lifecycle:
          preStop:
            sleep:
              seconds: 600
```

这能避免长请求在 rollout 时被立即切断，但代价是：即使旧 Pod 已经没有任何实际连接，也仍然会固定等 600 秒才退出。对频繁上线、1 replica app、长流式业务来说，这会显著拉长 rollout 时间，也会让用户误以为 Fugue “每次升级都卡 10 分钟”。

## 目标

将 strict zero-downtime drain 改为动态连接感知：

```text
新 Pod Ready 并稳定后：
  旧 Pod 进入 Terminating
  旧 Pod 从 Service endpoint 中移除，不再接收新流量
  drain hook 检查旧 Pod 业务端口上是否还有实际连接

  如果 active connection = 0 且持续 quiet period：
      hook 立即返回
      app 收到 SIGTERM
      old Pod 快速退出

  如果 active connection > 0：
      hook 等连接自然结束
      active connection 归零并持续 quiet period 后返回

  如果连接一直存在：
      最多等待 600 秒
      600 秒到达后 hook 返回
      app 收到 SIGTERM
      termination grace 剩余 buffer 用于 app 自身 shutdown
```

最终目标语义：

```text
600 秒 = 强制退出上限 / drain hard deadline
不是每次 rollout 的固定等待时间
```

## 非目标

第一阶段不做以下事情：

- 不把所有业务流量强制改走 Fugue sidecar proxy。
- 不要求业务应用集成 Fugue SDK。
- 不要求业务应用提供 `/metrics` 或 in-flight request counter。
- 不改变应用监听端口和用户配置端口的含义。
- 不为了单个 app 或单个 incident 写项目名特判。
- 不在没有验证的情况下全量开启到所有生产 app。

## 当前实现问题

当前代码在 `internal/runtime/objects.go`：

```go
const (
    appStrictDrainSeconds    = int64(600)
    appStrictDrainStopBuffer = int64(30)
)
```

当前 strict drain lifecycle：

```go
func buildStrictZeroDowntimeDrainLifecycle() map[string]any {
    return map[string]any{
        "preStop": map[string]any{
            "sleep": map[string]any{
                "seconds": appStrictDrainSeconds,
            },
        },
    }
}
```

当前 termination grace：

```go
func appTerminationGracePeriodSeconds(app model.App) int64 {
    grace := app.Spec.TerminationGracePeriodSeconds
    if appUsesStrictZeroDowntimeDrain(app) {
        minGrace := appStrictDrainSeconds + appStrictDrainStopBuffer
        if grace < minGrace {
            return minGrace
        }
    }
    return grace
}
```

当前 `minReadySeconds`：

```go
func deploymentMinReadySeconds(app model.App) int {
    if app.Spec.Replicas <= 0 || !model.AppHasClusterService(app.Spec) {
        return 0
    }
    if appUsesStrictZeroDowntimeDrain(app) {
        return int(appStrictDrainSeconds)
    }
    return appServiceMinReadySeconds
}
```

这导致两个固定 600 秒：

1. 新 Pod 必须 Ready 满 600 秒才算 Available。
2. 旧 Pod terminating 时固定 `preStop sleep 600`。

如果只把旧 Pod `preStop` 改成动态，但保留 `minReadySeconds=600`，rollout 仍会最少等 600 秒。因此本方案必须同时处理这两个地方。

## 推荐架构

采用 **connection-aware drain-agent native sidecar**。

### 核心思路

为 strict drain app 注入一个 Fugue 官方 sidecar：`fugue-drain-agent`。

业务容器的 `preStop` 不再固定 sleep，而是调用本 Pod 内 drain-agent：

```yaml
lifecycle:
  preStop:
    httpGet:
      path: /drain/prestop
      port: 19090
      scheme: HTTP
```

`fugue-drain-agent` 在 `/drain/prestop` 请求期间阻塞，并统计本 Pod 网络命名空间中业务端口上的 active TCP connections。

- 没有连接：快速返回。
- 有连接：等待连接结束。
- 超过 600 秒：返回，让 app 进入 SIGTERM/shutdown。

### 为什么不用普通 sidecar

普通 sidecar container 与 app container 的终止顺序不稳定。业务容器 `preStop` 需要调用 drain-agent，如果 drain-agent 提前退出，就会出现 race。

推荐使用 Kubernetes native sidecar 语义：

```yaml
initContainers:
  - name: fugue-drain-agent
    restartPolicy: Always
```

这样 drain-agent 作为 sidecar 先于业务容器启动，并在 Pod termination 流程中更适合服务业务容器的 `preStop` hook。

线上当前 k3s/kubelet 已观测到 `v1.35.4+k3s1`，支持 native sidecar 语义。为了兼容未来可能存在的旧节点，代码仍应保留 fallback：如果 runtime / cluster capability 未确认支持 native sidecar，则回退到旧的固定 `preStop sleep 600`，不要冒险。

### 为什么第一版不做 request-aware proxy

第一版只看 TCP connection，不改业务流量路径。优点：

- 不改变请求数据面。
- 不引入额外 proxy hop。
- 不要求应用改代码。
- HTTP、SSE、WebSocket、raw TCP 长连接都能以连接维度被保护。

缺点：

- TCP keep-alive idle connection 也可能被视为 active connection。
- 无法区分“正在处理 HTTP request”和“空闲 keep-alive socket”。

这是有意选择的安全保守边界。后续如果发现 idle keep-alive 导致经常等满 600 秒，再单独评估 edge upstream idle timeout 或 request-aware proxy sidecar。

## drain-agent 设计

### 新增二进制

新增：

```text
cmd/fugue-drain-agent/main.go
Dockerfile.drain-agent
```

二进制职责：

1. 提供 HTTP server。
2. 解析 `/proc/net/tcp` 和 `/proc/net/tcp6`。
3. 根据配置的 app ports 统计 active TCP connections。
4. 在 preStop 时动态等待连接 drain。
5. 输出结构化日志和 metrics。

### HTTP endpoints

#### `GET /readyz`

用于 kubelet/调试确认 agent 可用。

返回示例：

```json
{"ok":true,"component":"fugue-drain-agent"}
```

#### `GET /drain/prestop`

业务容器 `preStop.httpGet` 调用的阻塞 endpoint。

行为：

- 读取 app ports。
- 循环统计 active connections。
- active connections 为 0 且持续 quiet period 后返回 `200`。
- 如果达到 drain timeout，也返回 `200`。
- 如果观测失败，fail-closed，不提前放行；持续重试直到 timeout。

返回 body 可为简短 JSON：

```json
{"ok":true,"reason":"idle","waited_ms":1420,"active_connections":0}
```

或：

```json
{"ok":true,"reason":"timeout","waited_ms":600000,"active_connections":1}
```

注意：timeout 也返回 200，因为 timeout 表示“drain hard deadline 到达，可以进入 SIGTERM”，不是 hook 执行失败。

#### `GET /metrics`

Prometheus 文本格式 metrics。

建议指标：

```text
fugue_app_drain_active_connections
fugue_app_drain_wait_seconds
fugue_app_drain_early_exit_total
fugue_app_drain_timeout_total
fugue_app_drain_observer_errors_total
fugue_app_drain_prestop_requests_total
```

### 配置项

通过环境变量注入：

```text
FUGUE_DRAIN_AGENT_BIND_ADDR=:19090
FUGUE_DRAIN_APP_PORTS=8080,8000
FUGUE_DRAIN_TIMEOUT_SECONDS=600
FUGUE_DRAIN_QUIET_PERIOD_SECONDS=2
FUGUE_DRAIN_POLL_INTERVAL_MS=200
FUGUE_DRAIN_PROC_TCP_PATH=/proc/net/tcp
FUGUE_DRAIN_PROC_TCP6_PATH=/proc/net/tcp6
FUGUE_DRAIN_FAIL_CLOSED=true
```

推荐默认：

```text
bind addr: :19090
timeout: 600s
quiet period: 2s
poll interval: 200ms
fail closed: true
```

### TCP 状态统计规则

统计来源：

```text
/proc/net/tcp
/proc/net/tcp6
```

只统计 local port 在 `FUGUE_DRAIN_APP_PORTS` 中的 socket。

建议计入 active 的 TCP states：

```text
ESTABLISHED
SYN_RECV
FIN_WAIT1
FIN_WAIT2
CLOSE_WAIT
LAST_ACK
CLOSING
```

建议排除：

```text
LISTEN
TIME_WAIT
CLOSED
```

`LISTEN` 是监听 socket，不代表实际客户端连接。`TIME_WAIT` 已无应用层请求，不应阻塞 drain。

### fail-closed 策略

如果 `/proc/net/tcp` 或 `/proc/net/tcp6` 读取失败：

- 不应认为 active=0。
- 应记录 observer error。
- 应继续重试。
- 如果持续失败，等到 600s timeout 后返回。

这样可以避免观测失败导致提前杀旧 Pod。

### 日志格式

drain-agent 必须输出结构化日志，方便后续精确排障。

建议日志：

```text
fugue_drain_start pod=<pod> namespace=<ns> ports=8080 timeout_seconds=600 quiet_period_seconds=2 poll_interval_ms=200
fugue_drain_sample active_connections=2 states=ESTABLISHED:2 waited_ms=1200
fugue_drain_complete reason=idle waited_ms=3420 active_connections=0 max_active_connections=2 observer_errors=0
fugue_drain_complete reason=timeout waited_ms=600000 active_connections=1 max_active_connections=4 observer_errors=0
fugue_drain_observer_error error="read /proc/net/tcp: ..." waited_ms=2000
```

日志不要包含请求内容、headers、tokens、env secret。

## Runtime manifest 设计

### strict drain app 注入 native sidecar

目标 manifest：

```yaml
spec:
  minReadySeconds: 10
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
  template:
    metadata:
      annotations:
        fugue.io/drain-mode: connection-aware
        fugue.io/drain-timeout-seconds: "600"
        fugue.io/drain-quiet-period-seconds: "2"
        fugue.io/drain-agent-port: "19090"
        fugue.io/termination-grace-min-seconds: "630"
    spec:
      terminationGracePeriodSeconds: 630
      initContainers:
        - name: fugue-drain-agent
          image: ghcr.io/yym68686/fugue-drain-agent:<tag>
          imagePullPolicy: IfNotPresent
          restartPolicy: Always
          ports:
            - name: drain-agent
              containerPort: 19090
              protocol: TCP
          env:
            - name: FUGUE_DRAIN_AGENT_BIND_ADDR
              value: ":19090"
            - name: FUGUE_DRAIN_APP_PORTS
              value: "8080"
            - name: FUGUE_DRAIN_TIMEOUT_SECONDS
              value: "600"
            - name: FUGUE_DRAIN_QUIET_PERIOD_SECONDS
              value: "2"
            - name: FUGUE_DRAIN_POLL_INTERVAL_MS
              value: "200"
          resources:
            requests:
              cpu: 5m
              memory: 16Mi
            limits:
              cpu: 50m
              memory: 64Mi
      containers:
        - name: app
          lifecycle:
            preStop:
              httpGet:
                path: /drain/prestop
                port: 19090
                scheme: HTTP
```

### fallback manifest

如果 connection-aware drain 未启用或 capability 不满足，则保持旧行为：

```yaml
lifecycle:
  preStop:
    sleep:
      seconds: 600
terminationGracePeriodSeconds: 630
```

fallback 必须继续可用，确保生产可回滚。

### minReadySeconds

把 strict drain 下的 `minReadySeconds` 从 600 改为独立配置：

```text
strictDrain.minReadySeconds = 10
```

推荐默认：

```text
10s
```

如果需要更保守，可生产配置为：

```text
30s
```

不要再使用 `appStrictDrainSeconds` 作为 `minReadySeconds`。

### release key / runtime key 注意事项

当前 `ManagedAppReleaseKey` 基于 Deployment runtime key 计算。引入 drain-agent sidecar、preStop httpGet、minReadySeconds 变化会改变 runtime key，从而触发 app rollout。

这符合预期，但必须避免每次控制平面普通升级都因为 drain-agent image tag 变化导致所有 app 重滚。

建议：

- drain-agent image ref 作为 runtime renderer 配置注入。
- 控制平面发布脚本默认 preserve live drain-agent image。
- 只有 drain-agent 代码/镜像或 strict drain 配置显式变化时，才允许改变 app rendered manifest。
- 文档和发布脚本中明确这个 rollout blast radius。

## 配置设计

### Helm values

新增：

```yaml
runtime:
  strictDrain:
    mode: connection-aware
    timeoutSeconds: 600
    terminationGraceBufferSeconds: 30
    minReadySeconds: 10
    quietPeriodSeconds: 2
    pollIntervalMilliseconds: 200
    agent:
      enabled: true
      port: 19090
      image:
        repository: ghcr.io/yym68686/fugue-drain-agent
        tag: ""
        digest: ""
        pullPolicy: IfNotPresent
      resources:
        requests:
          cpu: 5m
          memory: 16Mi
        limits:
          cpu: 50m
          memory: 64Mi
```

### Controller env

新增：

```text
FUGUE_STRICT_DRAIN_MODE=connection-aware
FUGUE_STRICT_DRAIN_TIMEOUT_SECONDS=600
FUGUE_STRICT_DRAIN_TERMINATION_GRACE_BUFFER_SECONDS=30
FUGUE_STRICT_DRAIN_MIN_READY_SECONDS=10
FUGUE_STRICT_DRAIN_QUIET_PERIOD_SECONDS=2
FUGUE_STRICT_DRAIN_POLL_INTERVAL_MS=200
FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY=ghcr.io/yym68686/fugue-drain-agent
FUGUE_DRAIN_AGENT_IMAGE_TAG=<tag>
FUGUE_DRAIN_AGENT_IMAGE_DIGEST=
FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY=IfNotPresent
```

### Go config model

新增 runtime config 类型，例如：

```go
type StrictDrainConfig struct {
    Mode                          string
    TimeoutSeconds                int
    TerminationGraceBufferSeconds int
    MinReadySeconds               int
    QuietPeriodSeconds            int
    PollIntervalMilliseconds      int
    AgentImageRepository          string
    AgentImageTag                 string
    AgentImageDigest              string
    AgentImagePullPolicy          string
    AgentPort                     int
    AgentResources                ResourceSpecLike
}
```

Renderer：

```go
type Renderer struct {
    BaseDir          string
    WorkloadIdentity WorkloadIdentityConfig
    AppObservability AppObservabilityConfig
    StrictDrain      StrictDrainConfig
}
```

## 代码修改范围

预计需要修改 / 新增：

```text
cmd/fugue-drain-agent/main.go
Dockerfile.drain-agent
.github/workflows/deploy-control-plane.yml

internal/config/config.go
internal/config/config_test.go
internal/controller/controller.go
internal/controller/managed_app_rollout.go
internal/controller/managed_app_reconciler.go
internal/runtime/render.go
internal/runtime/objects.go
internal/runtime/objects_test.go
internal/runtime/managed_app.go
internal/runtime/managed_app_test.go

deploy/helm/fugue/values.yaml
deploy/helm/fugue/values-production-ha.yaml
deploy/helm/fugue/templates/controller-deployment.yaml
deploy/helm/fugue/chart_test.go

scripts/upgrade_fugue_control_plane.sh
scripts/test_release_domain_safety.sh
docs/zero-downtime-rollout-incidents.md
```

## 详细实施计划

### 阶段 0：保持当前行为，补充基线测试

先固定当前行为测试，避免后续重构误伤：

- 当前 fixed sleep mode 应仍渲染 `preStop.sleep.seconds=600`。
- 当前 strict drain 应保证 `terminationGracePeriodSeconds>=630`。
- 当前 online durable rollout 应保证 `RollingUpdate maxUnavailable=0 maxSurge=1`。

### 阶段 1：实现 drain-agent

实现独立二进制，不接入 app manifest。

验收标准：

- 单元测试覆盖 `/proc/net/tcp` / `/proc/net/tcp6` 解析。
- 单元测试覆盖 idle fast-exit。
- 单元测试覆盖 active-to-idle drain。
- 单元测试覆盖 timeout。
- 单元测试覆盖 proc read error fail-closed。
- `go test ./...` 通过。

### 阶段 2：渲染 connection-aware manifest，但默认不开启

新增 renderer 配置和 Helm 配置，但默认可以先保持：

```text
runtime.strictDrain.mode=fixed-sleep
```

或通过 feature flag 控制 connection-aware。

验收标准：

- 配置为 `connection-aware` 时生成 sidecar + `preStop.httpGet`。
- 配置为 `fixed-sleep` 时生成旧 `preStop.sleep`。
- 未配置 drain-agent image 时 fail-safe 回退 fixed sleep，或者拒绝启用 connection-aware 并给出明确错误。

### 阶段 3：控制平面构建和发布链路支持 drain-agent image

GitHub Actions 增加 build/push：

```text
fugue-drain-agent
```

Helm upgrade 脚本传入：

```text
FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY
FUGUE_DRAIN_AGENT_IMAGE_TAG
```

验收标准：

- CI 构建并 push drain-agent image。
- 控制平面 Helm values 可配置 drain-agent image。
- 发布脚本不会无意滚动所有 app。
- release safety 测试覆盖 drain-agent 变更对 app rollout blast radius 的处理。

### 阶段 4：测试 app 灰度验证

建立测试 app：

1. 无连接测试：rollout 后旧 Pod 应在数秒内退出。
2. 长连接测试：rollout 中保持 90s SSE/WebSocket/HTTP stream，旧 Pod 不应中断连接，连接结束后退出。
3. timeout 测试：保持超过 600s 的连接，旧 Pod 应最多等待 600s，然后进入 SIGTERM。

验收标准：

- 无连接不再等待 600s。
- 长连接不被强制中断。
- timeout 行为可观测。
- 无 5xx 尖刺。

### 阶段 5：0-0 灰度

对 0-0 开启 connection-aware strict drain。

验收标准：

- 触发一次 0-0 API image-only rollout。
- 新 Pod Ready 后旧 Pod 进入 Terminating。
- 如果无 active connection，旧 Pod 在 quiet period 后快速退出。
- 如果存在 `/v1/responses` stream，旧 Pod 等 stream 结束。
- `gateway_request_logs` 无 rollout 相关 503 尖刺。
- `context.Canceled` 仍按 499 记录，不回归。
- drain-agent 日志能解释每次旧 Pod 退出原因。

### 阶段 6：扩大默认启用

0-0 稳定后，把 strict drain 默认模式切换为：

```text
connection-aware
```

保留 fallback：

```text
fixed-sleep
```

验收标准：

- app 不配置时使用 connection-aware。
- capability 不满足时自动 fallback 或明确拒绝。
- 回滚只需改 Helm/env 为 `fixed-sleep`。

## 测试清单

### drain-agent 单元测试

- 解析 IPv4 `/proc/net/tcp`。
- 解析 IPv6 `/proc/net/tcp6`。
- 正确解码 hex local address / port。
- 只统计配置中的 app ports。
- `ESTABLISHED` 计入 active。
- `SYN_RECV` 计入 active。
- `FIN_WAIT1` / `FIN_WAIT2` 计入 active。
- `CLOSE_WAIT` / `LAST_ACK` / `CLOSING` 计入 active。
- `LISTEN` 不计入 active。
- `TIME_WAIT` 不计入 active。
- idle 时 `/drain/prestop` 在 quiet period 后返回。
- active 后归零时 `/drain/prestop` 返回。
- active 持续存在时 timeout 返回。
- proc 文件读取失败时不提前返回。
- invalid env 时启动失败并有明确错误。
- metrics 输出符合 Prometheus text format。

### runtime manifest 测试

- stateless service app strict drain 默认支持 connection-aware。
- online image update app 支持 connection-aware。
- online lifecycle update app 支持 connection-aware。
- online resource update app 支持 connection-aware。
- downtime-required durable steady-state app 不注入 connection-aware drain。
- 无 service port app 不注入 connection-aware drain。
- connection-aware manifest 包含 drain-agent native sidecar。
- connection-aware manifest 包含 app container `preStop.httpGet`。
- fixed-sleep manifest 仍包含 `preStop.sleep`。
- `terminationGracePeriodSeconds` 至少为 `timeout + buffer`。
- `minReadySeconds` 使用独立配置，不再等于 600。
- rollout annotations 包含 drain mode / timeout / quiet period / termination grace min。
- release key 对 drain 模式变化有可预期变化。

### controller 测试

- rollout policy readiness 接受 connection-aware annotations。
- `--wait` 验证 Deployment strategy / template / ready pod。
- SIGTERM during rolling update 仍被忽略，不误判失败。
- new Pod drain-agent image pull failure 时 rollout 卡住但旧 Pod 不被删除。
- fallback fixed-sleep 可以正常 rollout。

### Helm / 发布测试

- Helm values 渲染 controller env。
- chart test 覆盖 drain-agent 默认值。
- GitHub Actions build output 包含 drain-agent image repo/tag。
- upgrade script 传递 drain-agent image repo/tag。
- release safety 测试覆盖 drain-agent image-only 变更。
- 不相关控制平面变更不会改变 app runtime manifest。

### 线上验证测试

- 测试 app 无连接 rollout：旧 Pod 小于 10s 退出。
- 测试 app 长连接 rollout：旧 Pod 等连接结束退出。
- 测试 app timeout：旧 Pod 最多等 600s。
- 0-0 rollout 无 503 尖刺。
- 0-0 `context.Canceled` 仍归类 499。
- drain-agent 日志和 metrics 可解释每次 drain。

## 风险与缓解

### 风险 1：idle keep-alive 被统计为 active connection

现象：旧 Pod 仍可能等很久，甚至等到 600s。

缓解：

- 第一阶段接受这个安全保守行为。
- 后续可调 edge upstream idle timeout。
- 如果仍不够，再设计 request-aware proxy sidecar。

### 风险 2：drain-agent image 拉取失败

现象：新 Pod 无法 Ready，rollout 卡住。

缓解：

- `maxUnavailable=0` 保护旧 Pod 不被删。
- 发布前确认 drain-agent image 可拉取。
- 控制面保留 fixed-sleep fallback。

### 风险 3：preStop HTTP hook 无法调用 drain-agent

现象：hook 失败可能导致 app 直接进入 SIGTERM。

缓解：

- 使用 native sidecar 保证 drain-agent 生命周期。
- drain-agent readiness 先于 app 容器。
- 在 manifest 测试和 e2e 测试覆盖。
- 如 capability 不满足，fallback fixed-sleep。

### 风险 4：proc 观测不准确

现象：误以为无连接，提前退出。

缓解：

- fail-closed。
- quiet period。
- 只在稳定验证后扩大默认启用。
- 日志记录每次采样和最终 reason。

### 风险 5：control plane 升级引发全量 app rollout

现象：drain-agent image tag 随 Fugue commit 变化，所有 app manifest 变化。

缓解：

- 发布脚本默认 preserve live drain-agent image。
- 单独判断 drain-agent 相关文件变化。
- release safety 测试防止非预期 blast radius。

## 回滚方案

任何阶段发现异常时：

1. 将 `runtime.strictDrain.mode` 改回 `fixed-sleep`。
2. 控制平面通过正常 GitHub Actions 发布。
3. 确认新渲染 manifest 回到：

```yaml
preStop:
  sleep:
    seconds: 600
```

4. 保留 `terminationGracePeriodSeconds=630`。
5. 继续用已有 0-0 observability 验证无 503 / 499 异常回归。

## 可打勾 TODO list

### 设计与基线

- [x] 确认所有当前 strict drain 代码路径：stateless service、online durable image update、online lifecycle update、online resource update、online restart。
- [x] 固定当前 fixed-sleep 行为测试，确保 fallback 不丢。
- [x] 明确 production 默认灰度策略：先测试 app，再 0-0，再全局默认。
- [x] 确认线上 Kubernetes native sidecar capability，记录最低支持版本和 fallback 条件。

### drain-agent 实现

- [x] 新增 `cmd/fugue-drain-agent/main.go`。
- [x] 新增 `/readyz` endpoint。
- [x] 新增 `/drain/prestop` endpoint。
- [x] 新增 `/metrics` endpoint。
- [x] 实现 `/proc/net/tcp` parser。
- [x] 实现 `/proc/net/tcp6` parser。
- [x] 实现 TCP state active / inactive 分类。
- [x] 实现 app port filter。
- [x] 实现 quiet period。
- [x] 实现 600s hard timeout。
- [x] 实现 fail-closed observer error 策略。
- [x] 实现结构化日志。
- [x] 实现 Prometheus metrics。
- [x] 新增 `Dockerfile.drain-agent`。

### drain-agent 测试

- [x] 测试 IPv4 proc 解析。
- [x] 测试 IPv6 proc 解析。
- [x] 测试 port filter。
- [x] 测试 active states。
- [x] 测试 ignored states。
- [x] 测试 idle fast-exit。
- [x] 测试 active-to-idle drain。
- [x] 测试 timeout。
- [x] 测试 proc read error fail-closed。
- [x] 测试 metrics 输出。

### runtime renderer

- [x] 新增 `StrictDrainConfig`。
- [x] 将 strict drain constants 拆为 timeout / buffer / minReady / quiet period / poll interval。
- [x] 给 `Renderer` 注入 strict drain 配置。
- [x] 实现 `fixed-sleep` mode。
- [x] 实现 `connection-aware` mode。
- [x] strict drain app 注入 drain-agent native sidecar。
- [x] app container `preStop` 改为 `httpGet /drain/prestop`。
- [x] `terminationGracePeriodSeconds` 保持至少 `timeout + buffer`。
- [x] `minReadySeconds` 改用独立配置，默认 10 或 30，不再用 600。
- [x] annotations 增加 drain mode / quiet period / agent port。
- [x] capability 不满足时 fallback fixed-sleep。

### runtime / controller 测试

- [x] 测试 stateless app connection-aware manifest。
- [x] 测试 online image update connection-aware manifest。
- [x] 测试 online lifecycle update connection-aware manifest。
- [x] 测试 durable downtime-required steady-state 不注入。
- [x] 测试无 port app 不注入。
- [x] 测试 fixed-sleep fallback manifest。
- [x] 测试 rollout policy readiness。
- [x] 测试 SIGTERM during rolling update 不误判。
- [x] 测试 image pull failure 时旧 Pod 不被删除。

### Helm / 配置 / 发布链路

- [x] Helm values 增加 `runtime.strictDrain`。
- [x] controller Deployment 注入 strict drain env。
- [x] GitHub Actions build/push `fugue-drain-agent`。
- [x] upgrade script 传入 drain-agent image repo/tag。
- [x] 发布脚本默认 preserve live drain-agent image，避免全量 app rollout。
- [x] release safety 测试覆盖 drain-agent 相关变更。
- [x] chart tests 覆盖新 values 和 env。

### 灰度验证

- [ ] 在测试 app 开启 connection-aware。
- [ ] 无连接 rollout 验证旧 Pod 小于 10s 退出。
- [ ] 长连接 rollout 验证不中断 stream。
- [ ] 超长连接验证最多等待 600s。
- [ ] 检查 drain-agent 日志 reason=idle / timeout。
- [ ] 检查 metrics 正常上报。
- [ ] 对 0-0 开启 connection-aware。
- [ ] 0-0 image-only rollout 验证无 503 尖刺。
- [ ] 0-0 验证 `context.Canceled` 仍记录 499。
- [ ] 0-0 验证无连接时旧 Pod 快速退出。

### 默认启用与收尾

- [ ] connection-aware 在 0-0 稳定运行后设为 strict drain 默认模式。
- [x] 保留 `fixed-sleep` 回滚开关。
- [x] 更新 `docs/zero-downtime-rollout-incidents.md` 的当前不变量。
- [x] 更新运维 runbook，说明如何判断 drain reason。
- [x] 更新 CLI/diagnosis 输出，展示 drain mode 和最近 drain 结果。
- [ ] 记录最终线上验证数据和 commit / workflow / rollout evidence。

## 最终完成定义

本方案完成的判定标准：

- 无连接旧 Pod 不再固定等待 600 秒。
- 有连接旧 Pod 不提前中断连接。
- 600 秒仍是强制退出上限。
- strict zero-downtime rollout 仍保持 `maxUnavailable=0`。
- 控制平面普通升级不会意外触发所有 app rollout。
- 0-0 线上 rollout 无新增 503 尖刺。
- drain 行为可通过日志 / metrics / CLI 证据精确解释。
