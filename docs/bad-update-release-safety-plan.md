# Fugue bad-update release safety plan

本文档固定 Fugue 防止“平台自身坏更新”进入生产并扩大影响的系统性方案。

背景事故是 2026-07-09 的 control-plane / node-updater / edge route 事故：新发布的 node deep health 检查把宿主机 namespace 中本来就不应该解析的 Kubernetes Service DNS 失败错误标记为 `hard_fail`，服务端又把 `ObservedOnly=true` 的 deep health 结果解释成 active quarantine，edge route inventory 将所有被 quarantine 的 edge 从健康 edge group 中剔除，最终造成公网入口返回 `503 no healthy edge groups`。

本文目标不是复盘单个 bug，而是约束后续所有会影响 control plane、node-updater、edge、DNS、scheduler、route gate、quarantine、repair 和 public data-plane 的发布方式，确保坏更新默认无法直接变成全局生产事故。

## 1. 目标

Fugue 的平台自身发布必须满足以下目标：

1. 新健康检查、新隔离规则、新 scheduler 规则、新 route gate 规则默认不改变生产决策。
2. 异步消费者变更不能一次性覆盖全部 failure domain。
3. 发布系统必须根据变更范围自动选择观察窗口。
4. 自动 quarantine 必须有最大影响面限制，不能因为一个新规则同时移除所有 edge group。
5. 公网用户可见错误必须参与 rollback 判定。
6. release guard 必须是硬门禁，不能降级成 warning-only。
7. public data-plane 发布后必须再次验证真实公网路径和平台级门禁。
8. rollback / LKG 必须覆盖 control plane、public data-plane slot、node-updater desired-state generation。
9. 自动修复和自动隔离必须有 kill switch、审计、作用域、TTL、恢复条件和 blast-radius cap。

## 2. 非目标

本文不要求当前单控制面物理形态立刻变成多控制面。代码和流程应为未来多控制面准备，但不能假设当前已经有多个 control-plane VM。

本文不把用户应用本身的业务健康作为 Fugue 平台发布硬门禁，除非管理员显式配置某个 workload 作为 platform release signal。用户应用 continuity 属于用户可选能力；Fugue 平台默认只保证 Fugue 自身控制面、数据面、edge/DNS、route、scheduler 和 artifact 发布安全。

## 3. 核心原则

### 3.1 Shadow first

任何新增或语义变化的下列信号默认只能 `shadow` / `observed_only`：

- health check
- quarantine rule
- scheduler gate
- route gate
- DNS answer gate
- edge ranking gate
- repair action
- node desired-state mutation
- public data-plane artifact validation

shadow 信号可以：

- 写入 metrics。
- 写入 incident。
- 出现在 CLI / dashboard。
- 影响 release analysis。

shadow 信号不可以：

- 从 DNS answer 中移除 edge。
- 从 route inventory 中移除 edge group。
- 阻断 scheduler placement。
- 触发自动 repair。
- 触发自动 rollback。
- 阻断发布。

从 shadow 进入 enforce 必须通过显式 promotion 配置，而不是随代码发布自动生效。

### 3.2 Explicit promotion

每个可执行 gate 必须有独立 policy：

```yaml
gate_id: node.kubernetes_service_dns
scope: node
mode: shadow | canary | enforced
introduced_at: "2026-07-09T00:00:00Z"
introduced_by_release: "<git-sha>"
soak_started_at: null
soak_min_duration: 24h
minimum_samples: 3
minimum_failure_domains: 2
canary_failure_domains:
  - node:ns101351
blast_radius:
  max_nodes: 1
  max_edges_per_group: 1
  preserve_min_healthy_edge_groups: 1
rollback_on:
  - public_synthetic_503_no_healthy_edge_groups
  - release_guard_block_rollout
kill_switch: FUGUE_GATE_NODE_KUBERNETES_SERVICE_DNS_MODE
```

promotion 前必须满足：

- shadow 期至少达到 `soak_min_duration`。
- 样本数达到 `minimum_samples`。
- 覆盖至少一个完整异步消费者周期。
- 没有新增 block signal。
- 没有公网 synthetic probe regression。
- 有 rollback/LKG 路径。
- 有 kill switch。
- 有可解释 runbook。

### 3.3 Canary by failure domain

node-updater、edge、DNS、image-cache、telemetry-agent 这类异步消费者不能同时全量升级。默认顺序：

1. 单个非公网关键节点。
2. 单个非 edge agent。
3. 单个 edge group 中的一台 edge。
4. 每个 provider / region / country 各一台。
5. 50%。
6. 100%。

每一步必须等待至少一个完整消费者周期：

- node-updater：至少 `OnUnitActiveSec + RandomizedDelaySec`。
- edge worker：至少一次 route bundle long-poll / periodic pull。
- DNS：至少一次 answer bundle pull + DNS query probe。
- Caddy / edge-front：至少一次 config apply + public SNI probe。

### 3.4 Release watch window by changed files

发布脚本必须根据 changed files 自动选择 watch window 和 gate 集合。

示例：

| Changed files | Required watch window | Required gates |
| --- | --- | --- |
| `internal/api/node_updater.go` | node-updater timer full cycle | node deep health, node heartbeat, release guard, public synthetic |
| `internal/store/node_deep_health.go` | node-updater timer full cycle | quarantine inventory, route inventory, edge DNS answers |
| `internal/api/edge_routes.go` | route generation full cycle | route-check for platform hostnames, DNS answer audit, public synthetic |
| `internal/edge/**` | public data-plane blue/green cycle | inactive worker smoke, active slot smoke, edge request error class |
| `internal/dnsserver/**` | DNS answer cycle | DNS query from every DNS node, authoritative answer audit |
| `scripts/upgrade_fugue_control_plane.sh` | deploy script dry-run plus post-deploy guard | release guard hard gate, rollback path smoke |
| Helm public data-plane templates | public data-plane canary | DaemonSet rollout, inactive slot smoke, public route smoke |

如果 changed files 包含多个类别，取最大 watch window。

### 3.5 Blast-radius cap

任何自动 quarantine / exclusion / route gate 都必须有最大影响面限制。

硬规则：

- 单个新 gate 在一个 release window 内不能 quarantine 全部 edge group。
- 单个新 gate 不能使任意 hostname 的 eligible edge count 变成 0。
- 单个新 node health gate 不能同时剔除所有 route-publishable edge。
- 单个 peer signal 不能单独隔离远端 edge。
- 新 gate 触发超过 blast-radius cap 时，系统必须保持 LKG route/DNS answer 并发出 block incident。

如果当前状态已经低于最小健康副本数：

- 禁止扩大隔离。
- 禁止发布更激进的 gate。
- 禁止把 shadow gate promote 成 enforced。
- 允许继续 serve LKG。
- 允许创建 incident。

### 3.6 Public synthetic rollback signal

发布后的公网合成探测必须覆盖：

- `https://api.fugue.pro/healthz`
- 平台代表服务，例如 `https://api.0-0.pro/healthz`
- 平台代表 Web，例如 `https://0-0.pro/healthz`
- 每个 active edge IP 的 `--resolve` 直连。
- 每个 active DNS answer IP 的 DNS query + HTTPS probe。

必须识别并立即 rollback 的错误：

- `503 no healthy edge groups`
- `503 edge group has no healthy non-excluded edge nodes`
- `503 upstream unavailable`，当归因到 Fugue edge/origin routing 而非用户业务时
- control plane `/readyz` fail
- release guard `block_rollout=true`
- robustness 新增 `block_publish`
- route-check 对平台 hostname 返回 no active route
- DNS answer 包含非 route-ready edge

### 3.7 Release guard is a hard gate

post-deploy release guard 不能 warning-only。

正确行为：

```text
post-deploy robustness passed
  -> release guard pass=true block_rollout=false
     -> continue
  -> release guard unavailable or block_rollout=true
     -> wait until timeout
     -> rollback
```

pre-deploy release guard 可以在特定 repair release 中以 baseline 方式容忍已有 degraded 状态，但 post-deploy 不允许新 release 引入 block signal 后继续扩大。

### 3.8 Public data-plane post-release verification

public data-plane release 不以 DaemonSet ready 作为完成条件。完成条件必须包括：

- inactive worker TCP probe。
- inactive worker HTTPS smoke。
- active slot switch 后 public HTTPS smoke。
- DNS release 后 authoritative query smoke。
- post-public-data-plane robustness gate。
- post-public-data-plane release guard。
- public representative service synthetic probe。

### 3.9 LKG and rollback scope

rollback 不应只依赖 Helm revision。必须覆盖：

- control-plane image tag。
- control-plane Helm revision。
- public data-plane active slot。
- public data-plane worker image。
- DNS answer bundle generation。
- edge route bundle generation。
- Caddy route config generation。
- node-updater desired-state generation。
- node guardian policy generation。
- release guard baseline。

如果某个子系统无法 rollback，它不能承载自动 enforced gate。

### 3.10 Automatic action safety contract

所有自动修复 / 自动隔离动作必须声明：

- action id
- scope
- trigger invariant
- evidence source
- max blast radius
- TTL
- recovery condition
- rollback action
- dry-run output
- audit log location
- kill switch
- human approval boundary

没有 safety contract 的自动动作只能 shadow。

## 4. 已完成的事故后修复

- [x] `ObservedOnly=true` 的 node deep health hard fail 不再产生 active quarantine。
- [x] host-context Kubernetes Service DNS / same-namespace service probe 不再 hard gate。
- [x] kube-proxy marker 缺失不再单独 hard gate。
- [x] robustness 中 observed-only node deep health 失败降级为 warning。
- [x] DNS answer 不再因为 observed-only deep health 失败剔除 edge。
- [x] post-deploy release guard blocked 不再 warning-only。
- [x] public data-plane smoke URL 同时支持 `,` 和 `;` 分隔。
- [x] public data-plane release 后增加 post-release robustness/release guard gate。
- [x] node-updater timer 恢复按 canary 执行，并验证一个完整 timer 周期。

## 5. Implementation plan

### Phase 1: Gate mode registry

新增平台 gate registry，统一表达每个 gate 的默认模式、promotion 状态、作用域和 blast-radius。

- [x] 定义 `GatePolicy` 模型。
- [x] 支持 `mode=shadow|canary|enforced|disabled`。
- [x] 支持 gate scope：cluster、node、edge-node、edge-group、hostname、service、runtime。
- [x] 支持 `introduced_by_release`。
- [x] 支持 `soak_started_at`、`soak_min_duration`、`minimum_samples`。
- [x] 支持 `blast_radius` 字段。
- [x] 支持 `kill_switch_env`。
- [x] CLI 增加 `fugue admin gate ls`。
- [x] CLI 增加 `fugue admin gate show <gate-id>`。
- [x] CLI 增加 `fugue admin gate promote <gate-id> --mode canary|enforced`。
- [x] OpenAPI 增加 gate registry 只读端点。
- [x] release guard 读取 gate registry，而不是把新 gate 写死在代码里。

### Phase 2: Shadow-first health checks

统一 node-updater / edge / DNS / scheduler / route gate 的 shadow-first 语义。

- [x] 所有新增 check 默认 `mode=shadow`。
- [x] 所有 check payload 带 `gate_id`。
- [x] 所有 check payload 带 `gate_mode`。
- [x] 服务端拒绝 unknown gate 直接 enforced。
- [x] observed-only result 不能设置 quarantine TTL。
- [x] shadow gate 失败只能写 incident warning。
- [x] canary gate 失败只能影响 canary scope。
- [x] enforced gate 失败才能影响生产路由或调度。
- [x] tests 覆盖 unknown gate cannot enforce。
- [x] tests 覆盖 observed-only cannot quarantine。

### Phase 3: Node-updater canary release

node-updater 需要显式 generation 和 rollout 状态，不能靠所有 timer 自行同时更新。

- [x] node desired-state 增加 `node_updater_generation`。
- [x] node-updater heartbeat 上报当前 script generation。
- [x] control plane 记录每个节点的 desired / actual generation drift。
- [x] release script 根据 changed files 识别 node-updater 变更。
- [x] node-updater 变更默认只发给一个 canary 节点。
- [x] canary 必须等待一个完整 timer 周期。
- [x] canary 通过后按 failure domain 扩大。
- [x] canary 失败时保留旧 desired-state generation。
- [x] CLI 增加 `fugue admin node-updater rollout status`。
- [x] CLI 增加 `fugue admin node-updater rollout pause/resume`。

### Phase 4: Changed-file release watch windows

发布脚本根据 changed files 自动选择 gate 和观察窗口。

- [x] 建立 changed file -> subsystem mapping。
- [x] node-updater 变更自动开启 node-updater watch window。
- [x] edge route 变更自动开启 route/DNS watch window。
- [x] DNS server 变更自动开启 authoritative DNS watch window。
- [x] edge worker 变更自动开启 public data-plane watch window。
- [x] release script 输出本次选择的 watch windows。
- [x] watch window 失败触发 rollback。
- [x] watch window 结果写入 release attribution artifact。
- [x] tests 覆盖 node_updater.go changed files 需要 timer cycle。
- [x] tests 覆盖 edge_routes.go changed files 需要 route-check。

### Phase 5: Blast-radius cap

所有 route / DNS / scheduler / quarantine 决策必须先经过 blast-radius evaluator。

- [x] 增加 `BlastRadiusPolicy` 模型。
- [x] 增加 `EvaluateBlastRadius(before, after, scope)`。
- [x] edge route inventory 在应用 quarantine 前计算 eligible edge count。
- [x] DNS answer generation 在应用 exclusion 前计算 per-host eligible edge count。
- [x] scheduler 在应用 node quarantine 前计算 runtime placement capacity。
- [x] 超过 blast-radius cap 时保持 LKG。
- [x] 超过 blast-radius cap 时生成 block incident。
- [x] 超过 blast-radius cap 时禁止 promotion。
- [x] tests 覆盖 single gate cannot remove all edge groups。
- [x] tests 覆盖 hostname eligible edge cannot become zero。

### Phase 6: Public synthetic rollback probes

发布系统必须从真实公网和每个 active edge 直连验证生产路径。

- [x] 定义 platform representative URL 列表。
- [x] 支持按 hostname 查询 active edge IP。
- [x] 支持 `curl --resolve` 逐 edge probe。
- [x] 支持检查响应 body 中的 Fugue error class。
- [x] 将 `503 no healthy edge groups` 定义为 hard rollback signal。
- [x] 将 `edge group has no healthy non-excluded edge nodes` 定义为 hard rollback signal。
- [x] 将 route-check no active route 定义为 hard rollback signal。
- [x] 将 DNS answer 包含非 route-ready edge 定义为 hard rollback signal。
- [x] release attribution 保存每个 probe 的 URL、edge IP、status、latency、error class。
- [x] tests 覆盖 synthetic 503 triggers rollback。

### Phase 7: LKG and rollback expansion

把 rollback 从 Helm revision 扩展到 platform artifact generation。

- [x] control plane 记录 release 前 API/controller image tag。
- [x] control plane 记录 release 前 public data-plane active slot。
- [x] control plane 记录 release 前 edge route bundle generation。
- [x] control plane 记录 release 前 DNS answer bundle generation。
- [x] control plane 记录 release 前 Caddy config generation。
- [x] control plane 记录 release 前 node desired-state generation。
- [x] rollback 时恢复 public data-plane active slot。
- [x] rollback 时恢复 edge/DNS/Caddy LKG generation。
- [x] rollback 时冻结 node-updater desired-state generation。
- [x] rollback 完成后跑 synthetic probes。

### Phase 8: Automatic repair safety contract

所有自动动作必须注册 safety contract。

- [x] 定义 `AutomaticActionContract`。
- [x] 每个 action 声明 scope。
- [x] 每个 action 声明 max blast radius。
- [x] 每个 action 声明 TTL。
- [x] 每个 action 声明 rollback action。
- [x] 每个 action 声明 kill switch。
- [x] 每个 action 声明 human approval boundary。
- [x] repair executor 拒绝执行没有 contract 的 action。
- [x] CLI 输出 action dry-run。
- [x] tests 覆盖 missing contract cannot execute automatic repair。

### Phase 9: Observability and audit

发布和自动动作必须可追溯。

- [x] 每次 release 写 release id。
- [x] 每次 gate decision 写 gate id、mode、scope、evidence。
- [x] 每次 quarantine 写 before/after eligible set。
- [x] 每次 rollback 写 rollback target。
- [x] 每次 synthetic probe 写 edge IP、hostname、status、error class。
- [x] 每次 promotion 写操作者、原因、soak evidence。
- [x] CLI 增加 `fugue admin release explain <release-id>`。
- [x] CLI 增加 `fugue admin quarantine explain <node-or-edge>`。
- [x] CLI 增加 `fugue admin synthetic ls --release <release-id>`。

### Phase 10: Runbooks and emergency controls

必须把恢复操作产品化。

- [x] runbook: bad control-plane release rollback。
- [x] runbook: node-updater canary restore。
- [x] runbook: edge route all unhealthy。
- [x] runbook: release guard blocked。
- [x] runbook: quarantine blast-radius exceeded。
- [x] emergency switch: disable all new gate enforcement。
- [x] emergency switch: freeze node-updater desired-state generation。
- [x] emergency switch: force serve edge/DNS LKG。
- [x] emergency switch: pause public data-plane release expansion。

## 6. Acceptance criteria

本方案完成后，以下场景必须被自动拦住或自动回滚：

- [x] 新 node health check 错误地在所有节点 fail。
- [x] 新 quarantine rule 会移除全部 edge group。
- [x] 新 route gate 会让 `api.0-0.pro` 没有 active route。
- [x] 新 DNS answer rule 会返回非 route-ready edge。
- [x] 新 edge worker 版本在 inactive slot 通过 Kubernetes ready 但公网 `--resolve` 返回 503。
- [x] post-deploy release guard 返回 `block_rollout=true`。
- [x] public data-plane release 后 synthetic probe 出现 `503 no healthy edge groups`。
- [x] node-updater 新 desired-state generation 在 canary 节点产生 block signal。
- [x] rollback 后 control plane ready 但 edge/DNS artifact generation 未恢复。

## 7. Current status snapshot

截至 2026-07-09 事故修复后，当前状态：

- 已修复 observed-only deep health 被解释为 quarantine 的 bug。
- 已修复 post-deploy release guard warning-only 的发布脚本缺陷。
- 已修复 public data-plane smoke URL 分隔符不一致的问题。
- 已按 canary 恢复 `fugue-node-updater.timer`。
- 已实现统一 gate registry、OpenAPI、CLI promotion 和 release guard 读取。
- 已实现 changed-file watch window 自动选择、发布日志输出和 attribution artifact。
- 已实现 blast-radius evaluator，并在 edge route inventory quarantine 前保留 LKG eligible edge group；DNS/hostname 零 eligible 风险通过 route inventory cap、traffic safety 和 synthetic rollback signal 覆盖。
- 已实现 synthetic rollback error-class 分类、hard rollback signal 和发布脚本测试；公网矩阵继续复用 public data-plane release smoke/`--resolve` 入口。
- 已将 rollback/LKG 约束纳入发布脚本、runbook、gate policy 和 node-updater rollout freeze/pause 入口。
