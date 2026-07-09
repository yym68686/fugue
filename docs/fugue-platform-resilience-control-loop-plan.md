# Fugue 平台自组织、自恢复、鲁棒性控制闭环改造方案

更新时间：2026-07-10

## 1. 范围

本文只讨论 Fugue 平台自身的自组织、自恢复、鲁棒性改造，不讨论用户服务的 continuity / failover / safe rollout，也不把“多买服务器、多控制面、多公网入口”作为前提。

本文默认接受当前物理约束：

- 当前如果只有一个控制面 VM，就不能假装具备真正的控制面物理高可用。
- 当前如果所有公网 edge / DNS 入口都不可达，就不能凭软件凭空接住用户流量。
- 本文目标是在不新增服务器的情况下，让 Fugue 平台做到：
  - 坏更新不扩散。
  - 数据面尽可能继续服务已验证的 LKG。
  - 局部故障局部隔离。
  - 自动修复有边界、有证据、有回滚。
  - 发布失败自动阻断或回滚。
  - 事故能快速归因，不需要靠人工跨机器拼日志。

## 2. 当前基线

Fugue 当前已经具备不少基础能力，但这些能力仍然分散在多个模块里，尚未形成一个统一的、可证明的控制闭环。

### 2.1 已有能力

1. 平台状态已经 artifact 化。
   - `edge_route_bundle`
   - `dns_answer_bundle`
   - `caddy_route_config`
   - `discovery_bundle`
   - `node_desired_state`
   - `node_guardian_policy`
   - `release_guard_policy`
   - `edge_ranking_policy`
   - `traffic_safety_policy`
   - `gate_policy_registry`
   - `automatic_action_contracts`

2. Artifact 已经有发布通道。
   - `shadow`
   - `gray`
   - `full`

3. Artifact 已经有 LKG snapshot 和 consumer heartbeat。
   - consumer 可上报 desired generation。
   - consumer 可上报 actual generation。
   - consumer 可上报 LKG generation。
   - consumer 可上报 apply/probe 状态。
   - consumer 可上报是否正在 serving LKG。

4. Gate policy 已经支持基本安全字段。
   - `shadow`
   - `canary`
   - `enforced`
   - `disabled`
   - `soak_min_duration`
   - `minimum_samples`
   - `minimum_failure_domains`
   - `canary_failure_domains`
   - `blast_radius`
   - `rollback_on`
   - `kill_switch_env`
   - `runbook_ref`

5. 高风险 gate 默认已经偏保守。
   - node DNS / kube-proxy / CNI / conntrack 相关 gate 默认 shadow。
   - edge route inventory quarantine 默认 shadow。
   - DNS answer route-ready filtering 默认 shadow。
   - public synthetic 503 和 release guard block rollout 默认 enforced。

6. 发布脚本已经按 changed files 选择 watch window 和 gate。
   - node-updater 变更要求 node deep health、heartbeat、release guard、public synthetic。
   - edge route 变更要求 route-check、DNS answer audit、release guard、public synthetic。
   - DNS server 变更要求 authoritative DNS、DNS answer audit、release guard。
   - edge worker 变更要求 inactive worker smoke、active slot smoke、edge request error class。

7. DNS server 已经有签名 bundle、缓存、LKG、answer-time filtering 基础。
   - DNS bundle 需要签名校验。
   - cached bundle 可在允许窗口内作为 stale/LKG 使用。
   - A/AAAA answer 可按 edge live health 和 peer health 过滤。
   - DNS answer audit 已有记录路径。

8. edge worker 已经有保守请求级 peer fallback。
   - 仅覆盖 `GET` / `HEAD` / `OPTIONS`。
   - 不覆盖 streaming。
   - 不覆盖 upload。
   - 不覆盖默认不可安全重放的 POST。

9. endpoint LKG fallback 已有基础模型。
   - 有 route generation 校验。
   - 有 service identity 校验。
   - 有 TTL。
   - 有 stateless / stateful policy 边界。
   - 有 local WAL 记录。

10. node-updater 已有 deep health、repair guard、allowlist、cooldown、local WAL。
    - 可执行 node deep health。
    - 可执行 `reload-lkg-bundle`。
    - 可执行 allowlist 内的 stateless service restart。
    - 可执行 managed iptables repair dry-run / guarded delete。
    - repair task 有年龄限制和部分安全拒绝逻辑。

11. autonomy 有全局 kill switch 和分项开关。
    - `FUGUE_AUTONOMY_KILL_SWITCH`
    - `FUGUE_AUTONOMY_REPAIR_ENABLED`
    - `FUGUE_AUTONOMY_QUARANTINE_ENABLED`
    - `FUGUE_AUTONOMY_DNS_FILTERING_ENABLED`
    - `FUGUE_AUTONOMY_PEER_OVERLAY_ENABLED`
    - `FUGUE_AUTONOMY_ENDPOINT_FALLBACK_ENABLED`

### 2.2 核心问题

当前问题不是完全没有机制，而是多个机制没有被统一成一个“可执行控制系统”。

目前更像是：

```text
release guard     -> 自己判断发布是否安全
gate policy       -> 自己表达 shadow/canary/enforced
node-updater      -> 自己做 deep health 和 repair
edge route        -> 自己判断 eligible edge
DNS server        -> 自己做 answer-time filtering
artifact/LKG      -> 自己做 generation / LKG / consumer heartbeat
public synthetic  -> 自己做公网探测
```

目标架构应该收束成：

```text
Invariant Registry
  -> Action Safety Evaluator
  -> Platform Release Set
  -> Consumer Convergence
  -> Synthetic Verification
  -> Automatic Rollback / LKG / Quarantine / Repair
  -> Incident DAG / Explain
```

### 2.3 本次代码对照确认的关键偏差

以下不是假设，而是本次对当前实现逐项核对后确认的设计偏差。后续实现不能继续沿用这些语义。

1. Full release 和 LKG 语义不一致。
   - 正常 artifact release 的 `rollback_target_generation` 可以为空。
   - full release 会立即把新 generation 写成 LKG。
   - 当前 LKG 更接近“最新 full”，不是“完成验证的最后良好版本”。
   - 新 generation 尚未完成 consumer convergence/public probe 时，旧 LKG 已可能被覆盖。

2. Consumer absence 被错误折叠为健康。
   - robustness consumer check 只遍历已经存在的 heartbeat。
   - 完全没有 consumer、desired/actual 为空时不会计入 drift。
   - 当前 edge/DNS/node 组件没有完整接入统一 platform consumer heartbeat。

3. Consumer evidence 身份没有绑定。
   - heartbeat handler 只要求普通 Bearer 认证。
   - handler 没有要求 platform component 专用 scope。
   - 客户端可以提交任意 consumer/component/node/scope 字段。

4. Gate promotion 元数据没有形成硬门禁。
   - `SoakMinDuration`、`MinimumSamples`、`MinimumFailureDomains` 已存在于模型。
   - promotion handler 没有读取真实样本验证这些条件。
   - promotion 可以在设置 mode 后直接生成 full artifact release。

5. Artifact compatibility floor 只有格式检查。
   - 当前主要检查字符串是否以 `v` 开头。
   - consumer heartbeat 没有可靠的 protocol/schema capability。
   - 混合版本不兼容无法在 release 前被证明。

6. Release lane 缺少并发唯一性。
   - 单次 release 有数据库事务，但事务锁的是目标 artifact。
   - 同一 kind/scope/channel 没有唯一 active release 数据库约束。
   - 不同 artifact 的并发 release 可能绕过彼此的行锁。

7. DNS 最终 live-health filter 可以隐式清空 answer。
   - 最后一层 filter 会删除所有判定 unhealthy 的 A/AAAA。
   - 当前路径没有显式 preserve-LKG 或 explicit fail-closed 分支。
   - 现有测试主要覆盖“仍有一个健康 fallback”，没有覆盖 all-filtered。

8. Peer fallback 没有确定选择 alternate edge。
   - fallback 仍请求原 hostname。
   - 普通 DNS 可能再次返回当前 edge。
   - loop-prevention header 只能防递归，不能证明请求切换到了其他 edge。

9. Release baseline 可以容忍同名问题恶化。
   - blocker identity 主要由 name/subject/severity 组成。
   - 同名 blocker 的 observed value、影响节点数或范围扩大可能仍被视为旧问题。

10. 数据库 migration 不在 Helm rollback 能力范围内。
    - API 启动阶段会执行 schema bootstrap/DDL。
    - Helm rollback 不会撤销已经提交的 schema。
    - 当前发布模型没有完整表达 mixed-version 和 rollback schema compatibility。

因此，本文后续所有 Phase 必须以前置 `Safety Semantics` 为基础，不能直接在当前 LKG、consumer heartbeat 和 release transaction 语义上继续叠加功能。

## 3. 目标控制闭环

完整控制闭环如下：

```text
observe actual state
  -> authenticate and normalize evidence
  -> classify evidence as pass / fail / unknown / stale
  -> evaluate invariant
  -> produce evidence
  -> decide action through contract
  -> enforce blast-radius cap
  -> prepare release set and acquire fencing token
  -> publish artifact in shadow/canary/full
  -> wait consumer convergence
  -> run local and public probes
  -> compare baseline and post metrics
  -> promote verified LKG / hold / rollback
  -> write incident DAG and audit
```

任何会影响 Fugue 平台生产路径的动作，都必须进入这个闭环。

## 4. 架构改造方案

### 4.1 统一 Invariant Registry

#### 问题

当前 invariant 分散在：

- `robustness/status`
- release guard
- gate policy registry
- node-updater deep health
- edge route inventory
- DNS answer audit
- traffic safety
- platform artifact validation
- consumer generation drift

这会导致同一类风险在多个地方有不同解释，也容易出现一个模块认为 safe、另一个模块认为 unsafe 的情况。

#### 方案

新增统一的 `InvariantRegistry`，把平台自身的关键不变量定义成一等对象。

每个 invariant 至少包含：

- `id`
- `category`
- `scope`
- `subject`
- `owner`
- `description`
- `evidence_source`
- `severity`
- `default_mode`
- `gate_policy_id`
- `automatic_action_contract_id`
- `blast_radius_policy`
- `rollback_signal`
- `kill_switch_env`
- `runbook_ref`
- `evidence_state`
- `evidence_freshness_policy`
- `unknown_behavior`
- `stale_behavior`
- `non_bypassable`
- `expected_consumer_set_ref`
- `compatibility_policy_ref`
- `clock_uncertainty_budget`

#### 平台自身必须覆盖的不变量

1. 发布安全类
   - 新发布不能引入新的 `block_publish`。
   - 新发布不能让 public synthetic 进入 hard rollback class。
   - 新发布不能让 platform consumer generation drift。
   - 新发布不能让 LKG expired。

2. Artifact 类
   - artifact 必须 schema valid。
   - artifact 必须 content-addressed。
   - artifact 必须可验证 hash。
   - artifact release 必须有 rollback target。
   - full release 前必须经过 shadow 或明确 force reason。

3. Consumer 类
   - edge worker 必须上报 route bundle desired/actual/LKG。
   - DNS server 必须上报 DNS bundle desired/actual/LKG。
   - node-updater 必须上报 desired state generation。
   - Caddy edge front 必须上报 Caddy config generation。
   - consumer drift 不能在发布期间扩大。

4. Edge 类
   - edge 必须 online。
   - edge 必须 route-ready。
   - edge 必须 TLS-ready。
   - edge 必须未 draining。
   - edge 必须未 quarantine。
   - edge route bundle 不能为空。
   - edge Caddy reload 失败时必须继续服务上一代配置。

5. DNS 类
   - DNS answer 不能包含已知不 route-ready 的 edge。
   - DNS answer filtering 不能把 hostname eligible edge 清零，除非明确 fail-closed。
   - DNS bundle 过期后必须进入 degraded 或 fail-closed，不能静默继续当健康。
   - DNS answer audit 必须记录 selected / filtered edge 和原因。

6. Node 类
   - node-updater generation 必须可见。
   - node deep health 不能直接全量 enforcement。
   - node repair 必须有 cooldown、attempt limit、WAL、audit。
   - node quarantine 必须有 TTL、scope、recovery condition、blast-radius cap。

7. Fugue 平台 API 类
   - API readiness 必须可探测。
   - API DB readiness 必须可探测。
   - CLI 核心路径必须可探测。
   - 平台 API 的 5xx 必须可归因到 control-plane / edge / DNS / registry / auth / DB / origin connect 等类别。

8. 控制闭环自身类
   - 必需 consumer 完全没有 heartbeat 时不能判定健康。
   - evidence 来源身份无法验证时不能进入 promotion 决策。
   - evidence 过期或缺失时不能被折叠为 pass。
   - 同一 release lane 不能同时存在两个 active release。
   - stale coordinator 不能覆盖更新 generation。
   - 新 full generation 不能在验证完成前覆盖 verified LKG。
   - 数据库 schema 不兼容时不能只依赖 Helm rollback。
   - shadow gate 和 report-only action 永远不能影响生产路径。

### 4.2 统一 Automatic Action Contract

#### 问题

当前 `AutomaticActionContract` 已有模型，但还没有成为所有自动动作的强制执行入口。

高风险动作包括：

- quarantine node
- quarantine edge
- DNS answer-time filtering
- edge route filtering
- restart edge/dns/node-updater
- reload LKG bundle
- repair managed iptables
- endpoint fallback
- release rollback
- freeze node-updater generation

这些动作不能由各模块自行决定。

#### 方案

新增统一 `ActionSafetyEvaluator`。

任何自动动作执行前必须提交：

- `action_type`
- `contract_id`
- `trigger_invariant`
- `scope`
- `subject`
- `evidence`
- `current_mode`
- `candidate_blast_radius`
- `ttl`
- `rollback_target`
- `requested_by`

Evaluator 统一检查：

- contract 是否存在。
- gate mode 是否允许影响生产。
- kill switch 是否关闭。
- 是否在 canary scope 内。
- 是否超过 blast-radius cap。
- 是否满足 minimum samples。
- 是否满足 minimum failure domains。
- 是否满足 soak。
- 是否有 rollback action。
- 是否有 runbook。
- 是否需要人工 approval。

不满足条件时：

- `shadow`：只记录 would_action。
- `canary`：只影响 canary scope。
- `enforced`：允许执行，但必须写 audit/WAL。
- `disabled`：拒绝执行。

### 4.3 Platform Release Set

#### 问题

现在可以回滚 Helm，也可以回滚 platform artifact，但平台真实发布往往同时影响多个对象。

例如一次控制面发布可能同时改变：

- API image。
- controller image。
- edge worker image。
- DNS server image。
- edge route bundle 生成逻辑。
- DNS answer bundle 生成逻辑。
- Caddy config 生成逻辑。
- node-updater desired generation。
- gate policy registry。
- node guardian policy。
- traffic safety policy。

如果这些对象分开发布、分开回滚，就可能出现“API 回滚了，但 node-updater/gate/DNS 仍停留在新 generation”的不一致状态。

#### 方案

新增 `PlatformReleaseSet` 概念。

一次平台发布必须记录：

- `release_set_id`
- `release_lane`
- `state_version`
- `coordinator_id`
- `coordinator_lease_expires_at`
- `fencing_token`
- `idempotency_key`
- `git_sha`
- `started_at`
- `completed_at`
- `changed_files`
- `risk_classes`
- `artifact_generations`
- `image_tags`
- `helm_revision`
- `public_data_plane_slot`
- `node_updater_desired_generation`
- `gate_policy_generation`
- `guardian_policy_generation`
- `traffic_safety_policy_generation`
- `database_schema_generation`
- `generation_vector`
- `rollback_vector`
- `expected_consumer_set_revision`
- `rollback_targets`
- `required_gates`
- `watch_windows`
- `baseline_status`
- `post_status`
- `decision`

Release set 里的对象必须整体进入以下状态之一：

- `planned`
- `preparing`
- `prepared`
- `shadow_published`
- `canary_published`
- `full_published`
- `verifying`
- `verified_good`
- `held`
- `aborting`
- `aborted`
- `rolling_back`
- `rolled_back`
- `failed`

### 4.4 Consumer Convergence

#### 问题

已有 consumer heartbeat，但发布成功往往还容易被 Kubernetes rollout ready 替代。对于 Fugue 平台自身，这不够。

Kubernetes rollout ready 只能说明 Pod running/readiness 过了，不代表：

- edge 已实际应用 route bundle。
- DNS 已实际应用 DNS bundle。
- Caddy 已 reload 并保留上一代。
- node-updater 已拿到 desired generation。
- LKG 已写入且 hash 可验证。
- consumer 没有 generation drift。

#### 方案

每个平台 consumer 必须声明 contract：

- 支持哪些 artifact kind。
- scope 是 global/node/edge/hostname/zone。
- component identity 和 node identity 如何由凭证绑定。
- consumer protocol/schema version。
- 支持的 compatibility floor/capabilities。
- 是否是 required consumer。
- 是否 load LKG first。
- 是否 atomic apply。
- 是否 local probe。
- 是否 heartbeat desired/actual/LKG。
- stale heartbeat 多久算 drift。

发布扩大前必须满足：

- desired generation 已被目标 consumer 看见。
- actual generation 等于 desired generation。
- ExpectedConsumerSet 中没有 required consumer 缺失。
- heartbeat 身份、sequence、issued-at 和 evidence hash 验证通过。
- consumer protocol/schema version 满足 compatibility floor。
- local probe pass。
- LKG generation 可见。
- LKG 未过期。
- apply error 为空。
- probe error 为空。

### 4.5 Node Guardian

#### 问题

node-updater 当前偏定时任务和 task runner。它已经有 deep health、repair guard 和 WAL，但更完整的自恢复应该是 long-running guardian。

#### 方案

把 node-updater 演进为 Node Guardian。

Node Guardian 职责：

1. 观测实际状态。
   - k3s agent process。
   - kubelet process。
   - local apiserver / remotedialer。
   - node lease freshness。
   - CRI。
   - pod netns DNS。
   - CoreDNS pod path。
   - kube-dns Service path。
   - same namespace service DNS/TCP。
   - CNI bridge。
   - PodCIDR route。
   - kube-proxy / iptables。
   - conntrack。
   - disk / inode。
   - time sync。
   - edge/dns systemd。
   - LKG bundle files。

2. 写本地 WAL。
   - observe。
   - degraded。
   - would_repair。
   - repair_action。
   - repair_success。
   - repair_failure。
   - local_quarantine。
   - recovery。

3. 执行低风险本地修复。
   - refresh desired state。
   - reload LKG bundle。
   - restart allowlist 内 stateless Fugue service。
   - managed iptables dry-run。
   - guarded managed iptables delete。

4. 拒绝高风险动作。
   - 不自动做 stateful repair。
   - 不自动删除未知 iptables 规则。
   - 不自动重启非 allowlist 服务。
   - 不自动全局 quarantine。

5. 支持控制面不可用模式。
   - 控制面不可用时继续本地观测。
   - 控制面不可用时只允许本地 LKG reload / stateless guarded restart。
   - 控制面恢复后 replay WAL。

### 4.6 DNS / Edge 自治

#### 问题

DNS/edge 已有 LKG 和局部 filtering，但应该明确升级为平台数据面自治协议。

#### 方案

1. edge worker 启动时：
   - 先加载本地已验证 LKG。
   - 再拉取控制面新 bundle。
   - 新 bundle 校验失败时继续服务 LKG。
   - Caddy reload 失败时继续服务上一代 config。
   - 所有 fallback 行为写 WAL。

2. DNS server 启动时：
   - 先加载本地已验证 DNS LKG。
   - 新 bundle 校验失败时继续服务 previous LKG。
   - LKG 超过 max stale 后进入 degraded/fail-closed。
   - answer-time filter 只能短 TTL 生效。
   - filtering 不能让 hostname eligible edge 清零，除非 contract 明确 fail-closed。

3. peer overlay：
   - peer health 信号必须带 generation。
   - peer health 信号必须带 TTL。
   - peer health 信号必须带 evidence hash。
   - 单 peer 证据只能 shadow 或降权。
   - 多 failure domain 证据一致才允许短 TTL filter。
   - peer 信号过期必须自动清除。

### 4.7 Fugue 平台 API 请求级 Failover

#### 问题

edge peer fallback 当前只覆盖安全可重放方法。Fugue 平台自身 API 可以更强，因为 Fugue 可以控制 API 幂等协议。

#### 方案

仅针对 Fugue 平台 API 增强请求级 failover。

规则：

- GET/HEAD/OPTIONS 继续走现有 safe replay。
- POST/PUT/PATCH/DELETE 默认不 replay。
- 平台 API 如果声明 idempotency contract，可允许 replay。
- replay 前必须确认响应 header 未写出。
- replay 前必须确认请求体已完整 buffer。
- replay 必须带同一个 idempotency key。
- replay 必须带 attempt trace。
- replay budget 必须有限。
- replay 必须从可信 peer inventory 选择明确的 alternate edge。
- replay 必须排除当前 edge 和已尝试 edge。
- replay 必须直连 alternate peer IP，并保留原 Host/TLS SNI。
- peer inventory unknown/stale 时不得假装已完成 failover。
- streaming 不 replay。
- upload 不 replay，除非未来有 resumable upload protocol。

需要新增：

- platform API idempotency middleware。
- operation idempotency store。
- edge attempt trace。
- request explain 展示 fallback attempts。

### 4.8 Release Ledger

#### 问题

发布脚本已经有很多 gate，但 GitHub Actions 日志不是平台自身的长期状态真源。

#### 方案

每次控制面发布必须写入 `ReleaseAttempt` / `ReleaseLedger`。

记录：

- release id。
- git sha。
- actor。
- workflow run id。
- changed files。
- subsystem risk classes。
- required gates。
- required watch windows。
- pre-deploy robustness baseline。
- post-deploy robustness result。
- public synthetic result。
- release guard result。
- platform autonomy result。
- consumer convergence result。
- rollback target。
- final decision。

CLI 能查询：

```bash
fugue admin release attempts ls
fugue admin release attempts show <release-id>
fugue admin release attempts explain <release-id>
```

### 4.9 Incident DAG / Explain

#### 问题

事故排查现在虽然有更多指标，但仍然容易跨 CLI、Kubernetes、edge 日志、DNS 日志、node-updater 日志拼证据。

#### 方案

建立 Fugue 平台 incident DAG。

节点类型：

- public synthetic failure。
- DNS answer decision。
- edge request error。
- edge route bundle generation。
- Caddy apply generation。
- platform artifact release。
- platform consumer heartbeat。
- node deep health result。
- node repair action。
- release attempt。
- rollback action。

边类型：

- caused_by。
- observed_by。
- blocked_by。
- recovered_by。
- superseded_by。
- same_generation。
- same_request。
- same_release_set。

CLI：

```bash
fugue admin incident ls
fugue admin incident show <incident-id>
fugue admin incident explain <incident-id>
fugue admin request explain <request-id>
fugue admin release explain <release-id>
```

### 4.10 Emergency Status

#### 问题

Fugue 已经有很多安全开关和模式，但运维时需要一条命令看到当前安全姿态。

#### 方案

新增：

```bash
fugue admin emergency status
```

输出：

- global autonomy mode。
- global kill switch。
- repair/quarantine/DNS filtering/peer overlay/endpoint fallback 是否启用。
- gate policy mode 摘要。
- enforced gate 列表。
- canary gate 列表。
- active quarantine 列表。
- active temporary DNS filters。
- active LKG serving consumer。
- expired LKG consumer。
- node-updater desired generation。
- node-updater canary rollout 状态。
- edge route bundle generation。
- DNS bundle generation。
- Caddy config generation。
- public data-plane slot。
- rollback targets。
- 最近 public synthetic hard rollback class。

### 4.11 统一服务诊断和 Debug Bundle

#### 问题

当前 Fugue 已经有大量诊断入口，但它们分布在不同命令、不同 API、不同日志系统和不同节点上。

排查一个服务是否可用时，操作员通常需要手动组合：

- 本地 `curl` / `dig` / TLS 探测。
- `fugue app status`。
- `fugue app diagnose`。
- `fugue admin edge route-check`。
- `fugue admin dns answer-check`。
- `fugue admin edge quality-rank`。
- `fugue admin request explain`。
- `fugue admin robustness status`。
- `fugue admin release guard status`。
- Kubernetes pod / event / node 状态。
- edge access log。
- app/runtime log。
- node-updater deep health。
- quarantine / exclusion / LKG / artifact consumer 状态。

这种模式有几个问题：

1. 排障慢。
2. 容易漏证据。
3. 不同操作者会查不同路径，结论不稳定。
4. 本地网络、CLI 版本和权限会影响排障结果。
5. 无法稳定区分 `confirmed`、`probable`、`insufficient_evidence`。
6. 每次事故结束后，很难把证据原样打包用于复盘。

#### 目标

新增一个统一的“服务级诊断面”，让操作员收到“检查某个服务”的需求时，优先执行一条命令：

```bash
fugue diagnose service <app|hostname|url> \
  --since 30m \
  --request-id <request-id> \
  --deep
```

平台管理员可以执行：

```bash
fugue admin diagnose service <app|hostname|url> \
  --since 30m \
  --request-id <request-id> \
  --deep
```

需要归档时执行：

```bash
fugue debug bundle service <app|hostname|url> \
  --since 30m \
  --request-id <request-id> \
  --archive ./fugue-service-debug.zip
```

该能力不是简单在 CLI 里串行调用多个命令，而应该由控制面提供 server-side diagnosis aggregator。CLI 负责目标解析、发起诊断、展示结果和导出 bundle。

#### 分层设计

统一诊断必须分层，不能完全依赖控制面。否则控制面不可用时，诊断本身也会不可用。

1. L0 本地公网探测。
   - 不依赖 Fugue API。
   - 从操作者本机执行 DNS、TLS、HTTP 探测。
   - 支持对每个 active edge IP 执行 `--resolve` 直连。
   - 支持 recursive DNS 和 authoritative DNS 对比。
   - 控制面不可用时仍能判断公网入口是否坏。

2. L1 控制面聚合诊断。
   - 查询 app / project / runtime / route / DNS / edge / release / robustness / metrics。
   - 由 server-side aggregator 并发 fanout。
   - 使用统一 timeout budget。
   - 使用统一 evidence schema。
   - 正常情况下这是主路径。

3. L2 节点和数据面远程探针。
   - 通过 node guardian / node-updater 触发受控只读探针。
   - 从 edge 节点 probe origin。
   - 从 DNS 节点 probe authoritative answer。
   - 从 runtime node probe service DNS / ClusterIP。
   - 所有 probe 必须有 TTL、audit、rate limit 和权限边界。
   - 默认只读，不执行修复。

4. L3 Debug Bundle。
   - 打包完整结构化证据。
   - 默认 redacted。
   - 管理员显式传 `--redact=false --confirm-raw-output` 才允许原始证据。
   - bundle 必须可离线复盘，不要求再次访问线上系统。

#### 输出结构

统一诊断输出必须固定分区，避免每次人工重新判断查什么。

1. Verdict。
   - 服务当前是否可用。
   - 影响范围。
   - 最可能原因。
   - 置信度：`confirmed` / `probable` / `insufficient_evidence`。
   - 是否是 Fugue 平台问题。
   - 是否是用户服务问题。
   - 是否需要人工处理。
   - 推荐下一步。

2. Public Path。
   - 本机 DNS 解析结果。
   - authoritative DNS 节点答案。
   - recursive DNS 答案。
   - 每个 edge IP 的 `--resolve` 直连 HTTP/TLS 结果。
   - 公网域名真实请求结果。
   - TCP connect、TLS handshake、TTFB、total、status、body read/write。

3. Fugue Routing。
   - hostname 绑定的 app/runtime。
   - route policy。
   - service edge exclusion。
   - edge route bundle generation。
   - DNS answer bundle generation。
   - Caddy config generation。
   - 当前 DNS answer 选择的 edge。
   - edge 是否 online / route-ready / TLS-ready / draining / quarantined。
   - edge ranking 结果和实际 answer 是否一致。

4. Origin Path。
   - edge 到 origin 的 DNS / connect / request write / TTFB / total。
   - service DNS 是否可解析。
   - ClusterIP 是否可达。
   - endpoint fallback 是否可用。
   - runtime node 状态。
   - pod readiness / restart / event。
   - kube-dns / CoreDNS / CNI 相关证据。

5. Recent Changes。
   - 最近 app deploy / rebuild / release。
   - 最近 control-plane release。
   - 最近 public data-plane release。
   - 最近 node-updater desired generation 变化。
   - 最近 edge route / DNS bundle generation 变化。
   - 最近 quarantine / exclusion / drain / gate policy 变化。
   - 最近 rollback / LKG serving。

6. Logs and Metrics。
   - request-id 相关 edge log。
   - request-id 相关 app/runtime log。
   - 最近 5xx / 4xx / timeout / upstream unavailable。
   - body read speed。
   - body read error rate。
   - origin DNS error。
   - origin connect error。
   - TLS error。
   - cache hit/miss。
   - per-edge quality ranking。

7. Platform Health。
   - release guard status。
   - robustness status。
   - platform autonomy status。
   - artifact consumer drift。
   - LKG expired。
   - node deep health。
   - active incident。

8. Missing Evidence。
   - 哪些证据拿不到。
   - 是权限不足、控制面不可用、日志过期、指标缺失、edge 不上报，还是节点探针不可用。
   - 证据不足时必须输出 `insufficient_evidence`，禁止猜测根因。

#### Evidence Schema

统一诊断里的每个 check 都必须是结构化对象。

```json
{
  "check_id": "dns.authoritative_answer",
  "scope": "hostname:api.0-0.pro",
  "pass": true,
  "severity": "info",
  "source": "dns-node:ovhvps",
  "expected": "answer contains at least one route-ready edge",
  "observed": "A 15.204.94.71, A 154.0.0.1",
  "confidence": "confirmed",
  "evidence": {
    "dns_bundle_generation": "dnsgen_...",
    "edge_ids": "edge-us,edge-jp"
  },
  "checked_at": "2026-07-10T00:00:00Z"
}
```

最终 verdict 必须是结构化对象。

```json
{
  "status": "degraded",
  "root_cause": "edge_origin_connect_failure",
  "confidence": "confirmed",
  "impact": "api.0-0.pro requests through edge-jp returned 503",
  "recommended_action": "drain edge-jp for api.0-0.pro or restart fugue-edge.service on dmit",
  "missing_evidence": []
}
```

#### Root Cause Classifier

诊断引擎必须只在证据充分时给出 confirmed 根因。

推荐根因分类：

- `public_dns_resolution_failure`
- `authoritative_dns_failure`
- `dns_answer_contains_unhealthy_edge`
- `edge_unreachable`
- `edge_tls_failure`
- `edge_no_active_route`
- `edge_no_healthy_edge_group`
- `edge_origin_dns_failure`
- `edge_origin_connect_failure`
- `edge_origin_tls_failure`
- `edge_origin_request_write_failure`
- `edge_origin_ttfb_timeout`
- `edge_body_read_slow`
- `edge_body_read_error`
- `cluster_service_dns_failure`
- `cluster_clusterip_connect_failure`
- `runtime_pod_not_ready`
- `runtime_node_not_ready`
- `control_plane_api_unavailable`
- `release_regression`
- `platform_artifact_consumer_drift`
- `lkg_expired`
- `insufficient_evidence`

#### API 设计

新增控制面 API：

```text
POST /v1/admin/diagnostics/service
GET  /v1/admin/diagnostics/service/{diagnosis_id}
GET  /v1/admin/diagnostics/service/{diagnosis_id}/bundle
```

请求字段：

- `target`
- `target_type`
- `since`
- `request_id`
- `deep`
- `include_node_probes`
- `include_logs`
- `include_metrics`
- `include_public_probe`
- `redact`
- `timeout_seconds`

响应字段：

- `diagnosis_id`
- `target`
- `resolved_target`
- `generated_at`
- `status`
- `verdict`
- `sections`
- `checks`
- `root_cause_candidates`
- `missing_evidence`
- `recommended_actions`
- `related_incidents`
- `related_release_attempts`
- `debug_bundle_ref`

#### CLI 设计

面向普通用户：

```bash
fugue diagnose service <app|hostname|url>
fugue diagnose service <app|hostname|url> --since 1h
fugue diagnose service <app|hostname|url> --request-id <request-id>
fugue diagnose service <app|hostname|url> --deep
fugue diagnose service <app|hostname|url> --json
```

面向平台管理员：

```bash
fugue admin diagnose service <app|hostname|url>
fugue admin diagnose service <app|hostname|url> --deep --include-node-probes
fugue admin diagnose service <app|hostname|url> --request-id <request-id>
```

Bundle：

```bash
fugue debug bundle service <app|hostname|url> --since 30m --archive ./bundle.zip
fugue debug bundle service <app|hostname|url> --request-id <request-id> --archive ./bundle.zip
```

#### 与现有能力的关系

统一诊断不替代现有命令，而是聚合和标准化它们。

- `fugue app status` 继续用于 app 快速状态。
- `fugue app diagnose` 继续用于 app/runtime 层诊断。
- `fugue admin edge route-check` 继续用于 edge route 细查。
- `fugue admin dns answer-check` 继续用于 DNS answer 细查。
- `fugue admin edge quality-rank` 继续用于 edge ranking 细查。
- `fugue admin request explain` 继续用于单 request 细查。
- `fugue admin robustness status` 继续用于平台整体鲁棒性。
- `fugue admin release guard status` 继续用于发布门禁。

统一诊断负责把这些结果转成同一套 evidence schema，并产出服务级 verdict。

#### 权限和脱敏

- 普通用户只能看到自己 app/hostname 的诊断。
- 平台管理员可以看到 edge/DNS/node/release/platform evidence。
- 默认 redacted。
- debug bundle 默认不包含 secret。
- 原始日志和环境变量需要 `--redact=false --confirm-raw-output`。
- bundle 必须包含 redaction report。

### 4.12 不可绕过安全内核

#### 问题

Fugue 的长期目标是把 invariant、gate、automatic action、watch window 和 traffic safety 等行为数据化，但“全面配置化”不能等价于“所有安全规则都可以被配置关闭”。

如果 artifact 自己能够关闭 artifact validation，或者 gate policy 能够取消 shadow 隔离、扩大 blast radius、跳过 rollback target，那么一份坏配置就可以同时关闭检测和保护机制。

当前 `force_publish` 可以在有权限和 reason 的情况下绕过普通 artifact validation。该能力可以保留为 break-glass，但不能绕过最小安全内核。

#### 方案

建立编译期 `PlatformSafetyKernel`。安全内核只包含少量、稳定、可形式化验证的硬规则：

1. Artifact 基本完整性。
   - artifact kind、scope、schema、content hash 必须合法。
   - 生产 artifact 必须有可验证签名或受信任发布身份。
   - generation 必须单调，禁止 generation rollback 伪装成新发布。
   - compatibility metadata 必须可以被目标 consumer 解释。

2. 发布基本安全。
   - full release 必须有已验证的 rollback target。
   - shadow release 不得改变生产流量或生产 desired state。
   - canary release 不得越过 canary scope。
   - 同一 release lane 只能有一个 active coordinator 和一个 active release。
   - release set 不能在部分对象未 prepared 时进入 commit。

3. 自动动作基本安全。
   - 没有 action contract 的动作不能改变生产状态。
   - kill switch 永远优先于普通策略。
   - blast-radius 硬上限不能通过普通 artifact 放宽。
   - 自动动作必须有 TTL、恢复条件、幂等键、audit 和 rollback/compensation。

4. LKG 基本安全。
   - 未验证 generation 不能成为 verified LKG。
   - expired、corrupt、signature-invalid LKG 不能被报告为 healthy。
   - GC 不能删除 active release、verified LKG 和 pinned rollback target。

5. Evidence 基本安全。
   - 缺失、过期、来源不可信的 evidence 不能视为 pass。
   - consumer 不能自由声明不属于自己的 node/component/scope。
   - stale coordinator 或 stale WAL 不能覆盖更新 generation。

普通配置只能收紧安全内核，不能放宽。需要 break-glass 时，必须区分：

- `soft_override`：可跳过非关键策略，但不能跳过安全内核。
- `kernel_break_glass`：只允许极少数人工恢复场景，需要独立权限、双重确认、短 TTL、完整 audit，并在 TTL 到期后自动恢复默认保护。

### 4.13 Verified LKG 生命周期

#### 问题

LKG 不能简单等于“最新 full release”。新 generation 刚进入 full 时还没有证明它能被所有目标 consumer 正确读取、应用和服务。如果此时立即覆盖旧 LKG，坏发布会同时破坏当前版本和回滚指针。

#### 状态模型

每个 `(artifact_kind, scope_key, release_lane)` 必须维护：

- `candidate_generation`
- `serving_generation`
- `verified_lkg_generation`
- `previous_verified_lkg_generations`
- `pinned_rollback_generation`
- `verification_state`
- `verification_evidence_ref`
- `verified_at`
- `expires_at`

Generation 状态机：

```text
draft
  -> validated
  -> candidate
  -> serving_unverified
  -> verified_good
  -> verified_lkg
  -> superseded_but_retained
  -> retention_expired
```

失败分支：

```text
candidate / serving_unverified
  -> held
  -> rejected
  -> rolled_back
```

#### LKG 晋升条件

新 generation 只有同时满足以下条件，才能替换 verified LKG：

1. Artifact validation 全部通过。
2. 所有 required consumer 已出现且 heartbeat 新鲜。
3. required consumer desired/actual generation 一致。
4. required consumer local apply/probe 通过。
5. compatibility floor 对所有 required consumer 成立。
6. canary/full scope 的 public synthetic 通过。
7. 变更相关 watch window 已完成。
8. 相比 baseline 没有新增 blocker，也没有既有 blocker 定量恶化。
9. 数据库迁移处于 rollback-compatible 阶段。
10. Release Set coordinator 仍持有有效 fencing token。

#### 保留和回滚

- 至少保留当前 verified LKG 和前两代 verified LKG。
- rollback target 在 `prepare` 阶段固定，不能在故障发生后临时猜测。
- rollback target 的 artifact、镜像、Caddy config、DNS bundle、node desired state 和 schema compatibility 必须在发布前验证可用。
- verified LKG 不能由单个 consumer heartbeat 自动晋升。
- rollback 后的旧 generation 需要重新验证，验证通过后才能重新成为 active verified LKG。
- LKG 过期策略按 artifact kind 定义，不能使用一个全局 TTL。

### 4.14 Expected Consumer Set 和可信 Evidence

#### 问题

只检查“已经上报的 consumer”会把完全缺失的 consumer 当成健康。另一方面，如果 heartbeat 端点只要求普通 Bearer token，又允许客户端自由提交 `consumer_id`、component、node 和 generation，控制闭环证据可以被伪造或污染。

#### Expected Consumer Set

每个 Release Set 在 prepare 阶段必须根据实际拓扑生成 `ExpectedConsumerSet`：

- `release_set_id`
- `artifact_kind`
- `scope_key`
- `consumer_id`
- `component`
- `node_id`
- `failure_domain`
- `cohort`
- `required`
- `expected_protocol_version`
- `expected_generation`
- `heartbeat_deadline`
- `convergence_deadline`

Expected set 必须来自服务端可信拓扑，而不是由 consumer 自己声明。拓扑变化时要记录 set revision，避免发布期间节点增删导致验收口径漂移。

以下情况不能判定 convergence：

- required consumer 没有记录。
- heartbeat 超过 freshness deadline。
- desired/actual/LKG generation 为空。
- desired 与 actual 不一致。
- apply/probe 状态是 empty、unknown、failed 或 stale。
- component/node/scope 与服务端身份绑定不一致。
- protocol/schema version 不兼容。
- 同一个 consumer 出现 generation 倒退。
- 实际 heartbeat 数少于 expected cardinality。

#### Evidence 四态模型

核心控制闭环不能只使用 boolean `pass`。统一使用：

- `pass`：证据新鲜、可信并满足 invariant。
- `fail`：证据新鲜、可信并明确违反 invariant。
- `unknown`：没有足够证据得出 pass/fail。
- `stale`：曾有证据，但已经超过 freshness。

每个 invariant 必须定义：

- unknown 是否 hold release。
- stale 是否 hold release。
- evidence 最大年龄。
- 最少独立来源数。
- 是否要求跨 failure domain。
- 是否允许使用 LKG evidence。

#### Consumer 身份和防重放

- edge worker、DNS server、Caddy front、node guardian 和 runtime agent 使用独立 workload/node identity。
- heartbeat endpoint 使用专用认证模式和最小 scope，不接受普通 tenant API key。
- 服务端从凭证推导 component、node、tenant/platform scope。
- heartbeat 包含 sequence、issued_at、generation、nonce 和 evidence hash。
- 服务端拒绝重复 sequence、过期请求、未来时间、generation 倒退和跨节点冒充。
- 组件密钥、证书或 token 必须支持轮换、撤销和短期有效期。
- heartbeat 的原始证据和验证结果写入 tamper-evident audit。

### 4.15 Release Set 事务、并发和 Fencing

#### 问题

数据库单个事务并不等于平台发布原子性。API image、controller、edge/DNS bundle、Caddy config、node desired state 和 schema migration 分布在不同消费者和不同故障域中，无法使用一个数据库事务同时提交。

还必须考虑：

- 两个 GitHub Actions run 并发发布。
- 人工 rollback 与自动 promotion 同时发生。
- coordinator 在一半对象 commit 后崩溃。
- 旧 coordinator 恢复后继续写入。
- Node Guardian WAL replay 与新的 desired state 冲突。

#### Release Lane

定义稳定 release lane：

```text
release_lane = platform + environment + subsystem_scope
```

每个 lane 必须具备：

- 唯一 active release 约束。
- coordinator lease。
- monotonic fencing token。
- expected release-set version。
- CAS 状态更新。
- idempotency key。
- lease expiry 和 takeover 规则。

#### 两阶段发布协议

1. `prepare`
   - 获取 release lane lease 和 fencing token。
   - 固定 changed files、risk classes、ExpectedConsumerSet。
   - 固定 generation vector 和 rollback vector。
   - 验证所有 artifact/image/config/schema rollback target 存在。
   - 创建 shadow/canary objects，但不改变 full serving pointer。

2. `commit`
   - 每个 consumer 只接受当前 fencing token。
   - 按 release dependency DAG 提交对象。
   - 每个 apply 操作必须幂等。
   - 部分失败立即停止扩大，不允许继续 promotion。

3. `verify`
   - 等待 consumer convergence。
   - 执行 local/public probe。
   - 完成 watch window 和 baseline comparison。

4. `finalize`
   - 标记 release set `verified_good`。
   - 晋升 verified LKG。
   - 释放 lane lease。

5. `abort/rollback`
   - 使用 prepare 阶段固定的 rollback vector。
   - 对已 commit 对象执行 compensation。
   - 验证所有消费者收敛到 rollback vector。
   - rollback 自身失败时冻结 lane，禁止自动重复循环。

#### Generation Vector

Release Set 不应假设所有对象共享一个 generation。必须保存：

```text
api_image
controller_image
edge_worker_image
dns_server_image
edge_route_bundle
dns_answer_bundle
caddy_config
node_desired_state
gate_policy
guardian_policy
traffic_safety_policy
database_schema
```

Incident、diagnosis 和 consumer heartbeat 都应引用 release-set id 和 generation vector。

### 4.16 数据库迁移和混合版本兼容

#### 问题

Helm rollback 无法撤销已提交的数据库 schema 变化。API/controller 启动时自动执行 DDL，也可能在新旧 Pod 混合运行期间改变旧版本依赖的表结构。

#### 方案

把数据库 schema 作为独立 release domain 和 Release Set 成员。

迁移采用：

```text
expand
  -> mixed-version serve
  -> data backfill
  -> verify
  -> code cutover
  -> contract
```

规则：

- expand 阶段只能做旧版本可容忍的 additive change。
- destructive contract 必须延迟到所有旧 reader/writer 退出以后。
- 每个 release 声明 min/max readable schema 和 writable schema。
- migration 必须有独立 generation、状态、耗时、锁等待和影响行数。
- 发布前执行 migration dry-run 和 lock-risk preflight。
- 长时间 backfill 必须可暂停、限速、恢复和审计。
- release rollback 前验证旧二进制仍兼容当前 schema。
- 不可逆 migration 必须显式阻断自动 rollback，并要求先完成数据恢复方案。
- schema 变更前必须验证备份新鲜度和 restore drill 状态。
- migration coordinator 使用数据库 advisory lock 之外，还要受 Release Set fencing token 约束。

### 4.17 确定性的 DNS 和 Edge Failover

#### DNS 全部过滤语义

DNS filtering 必须显式区分：

- `healthy_candidates_available`
- `only_verified_lkg_candidates_available`
- `all_candidates_suspect`
- `all_candidates_confirmed_failed`
- `evidence_unknown`

不能由最后一层 live-health filter 隐式返回空答案。每个 hostname policy 必须明确：

- 是否允许保留一个 verified LKG candidate。
- 哪些 confirmed failure 可以 fail-closed。
- unknown/stale evidence 时是否保持 previous answer。
- answer change 的最小 dwell time。
- filter TTL、恢复阈值和最大连续 filter 时间。

必须增加“所有 candidate 被过滤”的 property test，证明除 explicit fail-closed 外 eligible set 不会被清零。

#### Edge peer fallback

Peer fallback 必须选择确定的 alternate edge，不能只再次请求原 hostname 并依赖递归 DNS：

- 从经过签名和 freshness 验证的 peer inventory 选择目标。
- 排除当前 edge、已尝试 edge、draining/quarantined edge。
- 直连 peer IP，同时保持原 Host 和 TLS SNI。
- 每次 attempt 记录 peer edge id、IP、generation、connect/TLS/TTFB/error class。
- 限制 attempts、单次 timeout 和总 request budget。
- 避免 fallback storm，按 hostname/edge 设置 circuit breaker。
- peer inventory 不可用时不执行伪 failover。

### 4.18 Release Baseline、风险归因和回滚验证

#### Baseline 单调性

Baseline 不能只按 `check name + subject + severity` 判断“是否已存在”。同名问题可能从一个节点扩大到所有节点。

每个 blocker 必须包含：

- stable identity。
- quantitative observed value。
- affected scope/cardinality。
- evidence fingerprint。
- first/last observed time。
- tolerated budget。
- baseline expiry。

发布后必须比较：

- 是否新增 blocker。
- 是否 severity 升级。
- 是否影响面扩大。
- 是否关键指标越过预算。
- 是否 evidence 从可信变成 unknown/stale。

被本次 changed subsystem 直接影响的 blocker 默认不能被旧 baseline 容忍。

#### 风险归因

changed-file 风险分类不能长期依赖手写文件名 case。建立 component ownership/dependency manifest：

- 每个 package、Helm template、script、OpenAPI/schema 文件声明影响组件。
- 共享 model/store/config 变化通过依赖图传播到消费者。
- 生成的 build plan 输出 risk classes、required gates、watch windows 和 canary cohorts。
- 未知文件或无法归因的变更默认进入高风险 hold，而不是零 watch window。

#### 回滚验证

Rollback 不能在 Kubernetes Deployment Ready 后结束。必须重新验证：

- control-plane API/DB readiness。
- public synthetic。
- 每个 active edge 直连。
- authoritative DNS answer。
- consumer convergence 到 rollback vector。
- verified LKG 指针。
- node desired-state generation。
- schema compatibility。

Rollback 连续失败或来回 oscillation 时，应冻结 release lane 并升级人工事件，禁止无限自动重试。

### 4.19 时间语义、冲突仲裁和控制循环稳定性

#### 时间语义

TTL、lease、soak、watch window、LKG expiry 和 evidence freshness 都依赖时间。必须定义：

- duration 使用 monotonic clock 计算。
- 跨节点比较使用 wall clock，并携带 clock uncertainty。
- issued_at/not_before/expires_at 的允许偏差。
- NTP 未同步或时钟跳变时的行为。
- 时间倒退不能延长已过期 lease/LKG/action TTL。
- 时间前跳不能直接触发全局 quarantine。

#### 冲突仲裁

明确动作优先级：

```text
kernel kill switch
  > operator emergency hold
  > release rollback
  > control-plane desired state
  > local guardian recovery
  > stale WAL replay
```

所有 action 必须带：

- idempotency key。
- source generation。
- fencing token。
- expected current version。
- compensation action。

WAL replay 只能补交 audit 或重试仍然适用于当前 generation 的动作，不能覆盖更新 desired state。

#### 防抖和动作预算

- health 变化需要连续样本和最小 dwell time。
- recover threshold 与 fail threshold 使用 hysteresis。
- quarantine、DNS filter、restart、rollback 有独立 cooldown。
- 同一 subject 有 action rate budget。
- 全局有最大并发自动动作数。
- 反复 fail/recover 进入 flapping 状态，只降权/hold，不持续执行破坏性动作。

### 4.20 诊断快照、资源预算、保留、安全和 SLO

#### 一致诊断快照

一次 diagnosis 必须定义：

- `diagnosis_epoch`
- `started_at`
- `completed_at`
- `as_of_watermark`
- `release_set_id`
- `generation_vector`
- 每个 source 的 `observed_at`、freshness、clock uncertainty。

Classifier 不能把事故前日志、恢复后 DNS 答案和不同 generation 的 edge 状态拼成一个 confirmed 根因。

控制面不可用时，L0 使用本地签名 diagnostic discovery bundle 获取最近已知 edge/DNS inventory。bundle 必须有 generation、valid_until、revocation 信息和 max-stale 行为。

#### 资源和优先级预算

诊断和自愈不能成为新的故障放大器：

- serving traffic 优先级高于 control loop。
- control loop 优先级高于 deep diagnosis/export。
- aggregator 有全局和 per-source concurrency budget。
- 所有 fanout 有 deadline、cancellation 和 circuit breaker。
- 日志/指标查询有最大时间窗口、结果行数和字节数。
- debug bundle 有最大大小。
- L2 probes 有 per-node 和 cluster-wide rate limit。
- metrics/logs backend 不可用时返回 partial result，不能拖死 API。

#### Retention 和 GC

为以下对象定义 retention、compaction 和容量告警：

- platform artifacts/content。
- release messages/ledger。
- incident DAG。
- consumer heartbeat history。
- action audit/WAL。
- debug bundle。
- incident replay snapshot。
- synthetic/diagnosis evidence。

GC 必须保护：

- active release。
- verified LKG。
- pinned rollback target。
- active incident 引用对象。
- 尚未完成的 release set。
- 最近 restore drill 所需对象。

#### 平台安全和供应链

- 平台镜像必须 digest pin。
- 关键镜像和 artifact 支持 provenance/signature verification。
- rollback image 必须在发布前验证可拉取，并受 retention 保护。
- 密钥/证书/token 支持轮换、撤销、重叠生效和过期告警。
- compromised node 的单点 evidence 只能标记 suspect，不能直接全局 enforcement。
- break-glass 和 kernel override 使用 tamper-evident audit。

#### Fugue 平台 SLO

定义只针对 Fugue 自身的 SLO：

- control-plane API availability/latency。
- authoritative DNS availability 和 answer correctness。
- edge route correctness。
- platform 5xx。
- consumer convergence latency。
- verified LKG freshness。
- release rollback 成功率和耗时。
- node guardian detection/action latency。
- diagnosis completeness 和 evidence freshness。

SLO 产生 error budget。预算耗尽时：

- 冻结高风险 platform release。
- 自动策略只允许降级到更保守模式。
- 不自动扩大 blast radius。
- 继续服务数据面 LKG。

#### Canary Cohort

Canary 不只按 failure domain 数量选择，还要覆盖：

- component role。
- OS/distribution。
- kernel。
- k3s/container runtime 版本。
- iptables/nftables backend。
- network/CNI 模式。
- provider/region。
- hardware architecture。

如果没有代表性 cohort，promotion 状态只能是 evidence insufficient。

## 5. 发布与验证策略

所有改造必须按以下策略发布：

1. 新 invariant 默认只 shadow。
2. 新 automatic action 默认只 report-only。
3. 新 node guardian 行为先单节点 canary。
4. 新 edge/DNS filtering 先 shadow audit。
5. 新 release set 先只记录，不阻断。
6. 新 request-level failover 先只对 Fugue 内部低风险 API 开启。
7. 新 unified service diagnosis 先只读聚合，不执行修复。
8. node probes 默认关闭，先由平台管理员显式 `--include-node-probes` 使用。
9. debug bundle 默认 redacted。
10. 新 full release 不得立即覆盖 verified LKG。
11. 新 consumer heartbeat 必须先启用身份绑定和防重放，再参与门禁。
12. 新 release-set coordinator 必须先完成并发、fencing 和 crash-recovery 测试。
13. 数据库迁移先走 expand/mixed-version，contract 阶段单独发布。
14. 未知 changed-file risk 默认按高风险处理。
15. 每个 enforcement 必须有：
   - soak。
   - samples。
   - failure domain 覆盖。
   - rollback signal。
   - kill switch。
   - runbook。
   - public synthetic 验证。
   - verified rollback target。
   - trusted evidence。
   - expected consumer convergence。

## 6. 验收标准

### 6.1 坏更新防护

一次会导致 edge route/DNS 全部不可用的坏更新，必须被以下任意一层挡住：

- artifact validation。
- release guard。
- blast-radius evaluator。
- public synthetic。
- consumer convergence。
- release safety watch window。
- automatic rollback。

### 6.2 控制面短暂不可用

控制面 API 短暂不可用时：

- edge 继续服务已验证 LKG。
- DNS 继续服务已验证 LKG。
- consumer 记录 serving LKG。
- node guardian 继续本地观测。
- 本地自治动作写 WAL。
- 控制面恢复后 replay WAL 并 reconcile generation。

### 6.3 edge worker 异常

单个 edge worker 异常时：

- public synthetic 或 peer/local probe 发现异常。
- DNS answer-time filter 在 TTL 内临时移除异常 edge，且不清零 eligible set。
- Node Guardian 可以 guarded restart edge service。
- 修复/过滤动作有 audit 和 recovery condition。

### 6.4 DNS bundle 异常

坏 DNS bundle 发布时：

- DNS server 拒绝 apply。
- 继续服务 previous LKG。
- consumer heartbeat 上报 apply/probe error。
- release guard 或 release set 阻断扩大。
- public synthetic 不应出现全线 503。

### 6.5 Caddy config 异常

坏 Caddy config 发布时：

- Caddy reload 失败不影响上一代配置继续服务。
- edge 上报 caddy apply error。
- route bundle/caddy artifact 不进入 full。
- release set 可回滚到上一代 Caddy config。

### 6.6 node-updater / guardian 异常

坏 node guardian policy 或 deep health 检查发布时：

- 只影响 canary node。
- 不允许一次 quarantine 所有 edge/DNS/node。
- 超过 blast-radius cap 时保持 LKG 并生成 block incident。
- node-updater desired generation 可冻结。
- rollback 可恢复上一代 generation。

### 6.7 统一服务诊断

服务诊断必须满足：

- 一条命令能返回服务级 verdict。
- 输出必须包含 public path、Fugue routing、origin path、recent changes、logs/metrics、platform health、missing evidence。
- 能区分 Fugue 平台问题、用户服务问题和证据不足。
- confirmed 根因必须有直接证据。
- probable 根因必须列出缺失证据。
- insufficient evidence 必须明确列出缺什么。
- 控制面不可用时仍能执行 L0 本地公网探测。
- debug bundle 默认脱敏。
- debug bundle 能离线复盘诊断结论。

### 6.8 Verified LKG

LKG 生命周期必须满足：

- 新 full generation 在验证完成前只能是 `serving_unverified`。
- `serving_unverified` 失败时，旧 verified LKG 仍然存在且可加载。
- verified LKG 晋升必须有 release-set、consumer convergence、local/public probe 和 watch-window evidence。
- release 前可以明确指出 pinned rollback generation。
- 至少保留当前和前两代 verified LKG。
- GC 不会删除 active、verified LKG 或 pinned rollback target。
- corrupt、expired、signature-invalid LKG 不会被标记 healthy。

### 6.9 Consumer Evidence

Consumer convergence 必须满足：

- topology 中每个 required consumer 都出现在 ExpectedConsumerSet。
- required consumer 完全缺失时状态是 unknown/fail，不是 pass。
- heartbeat stale、generation 为空、probe unknown 都不能判 convergence。
- tenant API key 无法提交 platform consumer heartbeat。
- consumer 无法冒充其他 component/node/scope。
- 重放、过期、未来时间和 generation 倒退的 heartbeat 被拒绝。
- mixed-version consumer 不满足 compatibility floor 时阻断扩大。

### 6.10 Release Set 并发和崩溃恢复

Release Set 必须满足：

- 同一 release lane 只能有一个有效 coordinator。
- 并发 GitHub Actions run 不会留下两个 active full release。
- stale fencing token 无法 commit。
- prepare 完成前无法进入 commit。
- coordinator 在部分 commit 后崩溃，接管者可以确定 abort、resume 或 rollback。
- promotion 和 rollback 并发时，rollback/hold 优先。
- rollback 完成后所有 required consumer 收敛到同一 rollback generation vector。
- rollback 连续失败会冻结 lane，不会形成自动循环。

### 6.11 数据库迁移

数据库迁移必须满足：

- expand 阶段旧 API/controller 可以继续运行。
- 新旧 API/controller 混合运行时读写协议兼容。
- contract migration 不与旧 binary 同时运行。
- migration lock 等待和执行时长有预算。
- backfill 可暂停、恢复、限速并可观测。
- Helm rollback 前能够判断旧 binary 是否兼容当前 schema。
- 不可逆 migration 不会被错误标记为自动可回滚。
- schema 变更前备份新鲜，且最近 restore drill 通过。

### 6.12 DNS 和 Peer Failover

DNS/edge failover 必须满足：

- 所有 candidate 都被判 unhealthy/suspect 时走显式 policy，不由最终 filter 隐式返回空答案。
- 除 explicit fail-closed 外，DNS filtering 不把 eligible set 清零。
- unknown/stale evidence 不会触发全局 filter。
- answer change 有 dwell/hysteresis，避免 DNS 抖动。
- peer fallback 明确排除当前 edge。
- peer fallback 直连被选择的 alternate edge，并保持 Host/SNI。
- peer inventory 不可用时不伪装成成功 failover。
- fallback attempt、预算和最终错误可以通过 request explain 查看。

### 6.13 安全内核和可信发布

安全内核必须满足：

- 普通 artifact/gate/config 无法关闭安全内核。
- `force_publish` 不能绕过 schema/hash/signature/generation/rollback-target 等硬规则。
- shadow 机制在 property test 中证明不会影响生产。
- canary 机制在 property test 中证明不会越过 scope。
- kill switch 在并发 action 中仍然优先。
- artifact/image provenance 不可信时不能进入 full。
- kernel break-glass 有独立权限、短 TTL、双重确认和自动恢复。

### 6.14 Baseline 和风险归因

发布比较必须满足：

- 同名 blocker 的影响面扩大能够被识别。
- 同名 blocker 的定量指标恶化能够被识别。
- stale/unknown evidence 增加能够被识别。
- baseline 有有效期和 tolerance budget。
- changed subsystem 的 blocker 不被旧 baseline 静默容忍。
- shared model/store/config/OpenAPI/Helm 变化可以传播到受影响组件。
- 无法归因的 changed file 默认进入高风险 hold。

### 6.15 诊断一致性和资源保护

统一诊断必须满足：

- 所有 evidence 带 observed_at、freshness、source generation。
- verdict 只组合相容 generation 和时间窗口的证据。
- 控制面不可用时 L0 可以从本地签名 discovery bundle 获得 edge/DNS inventory。
- aggregator 单个 source 超时不会耗尽整体 worker pool。
- metrics/logs backend 故障不会拖垮 control-plane API。
- debug bundle、日志和指标查询有大小/时间窗口限制。
- serving traffic 不会被 deep diagnosis 或 chaos drill 抢占关键资源。

### 6.16 平台 SLO、Retention 和恢复资产

长期运行必须满足：

- Fugue 平台核心路径有明确 SLO 和 error budget。
- error budget 耗尽时会冻结高风险发布。
- release ledger、incident、evidence、WAL 和 debug bundle 有 retention/GC。
- GC 保留 active、verified LKG、rollback target 和 active incident 引用对象。
- rollback 镜像和 artifact 在发布前确认仍可获取。
- 每个核心 LKG/rollback 路径都有最近一次成功演练时间。

## 7. Todo List

### Phase -1: Safety Semantics 和可信控制闭环基础

#### Phase -1A: 不可绕过安全内核

- [ ] 定义 `PlatformSafetyKernel` 的职责边界。
- [ ] 列出所有不可由配置关闭的 invariant。
- [ ] 定义 artifact schema/hash/signature 硬规则。
- [ ] 定义 generation 单调性硬规则。
- [ ] 定义 shadow no-production-impact 硬规则。
- [ ] 定义 canary scope isolation 硬规则。
- [ ] 定义 full release rollback-target 硬规则。
- [ ] 定义 blast-radius maximum hard cap。
- [ ] 定义 kill switch precedence。
- [ ] 定义 expired/corrupt/signature-invalid LKG 硬规则。
- [ ] 将普通 `force_publish` 拆成 `soft_override`。
- [ ] 设计独立 `kernel_break_glass` 权限。
- [ ] kernel break-glass 要求 reason、双重确认和短 TTL。
- [ ] kernel break-glass 到期自动恢复默认保护。
- [ ] kernel break-glass 写入 tamper-evident audit。
- [ ] 增加配置只能收紧、不能放宽安全内核的测试。
- [ ] 增加 shadow 不影响生产的 property test。
- [ ] 增加 canary 不越过 scope 的 property test。
- [ ] 增加 kill switch 并发优先级测试。
- [ ] 新增 runbook: platform safety kernel。
- [ ] 新增 runbook: kernel break-glass。

#### Phase -1B: Verified LKG 语义

- [ ] 盘点当前所有把 full generation 立即写成 LKG 的路径。
- [ ] 定义 `candidate_generation`。
- [ ] 定义 `serving_unverified_generation`。
- [ ] 定义 `verified_lkg_generation`。
- [ ] 定义 `previous_verified_lkg_generations`。
- [ ] 定义 `pinned_rollback_generation`。
- [ ] 定义 `verification_state` 和 evidence ref。
- [ ] artifact full release 不再立即覆盖 verified LKG。
- [ ] release prepare 阶段固定 rollback target。
- [ ] 验证 rollback artifact content/hash/signature。
- [ ] 验证 rollback image digest 可拉取。
- [ ] 验证 rollback Caddy/DNS/route config 存在。
- [ ] 验证 rollback node desired-state generation 存在。
- [ ] verified LKG 晋升检查 consumer convergence。
- [ ] verified LKG 晋升检查 local probe。
- [ ] verified LKG 晋升检查 public synthetic。
- [ ] verified LKG 晋升检查 watch window。
- [ ] verified LKG 晋升检查 baseline monotonic comparison。
- [ ] verified LKG 晋升检查 database rollback compatibility。
- [ ] verified LKG 晋升检查 fencing token。
- [ ] 每个 artifact kind 定义 LKG TTL/max stale。
- [ ] 至少保留三代 verified LKG。
- [ ] GC 保护 verified LKG 和 pinned rollback target。
- [ ] rollback 后重新验证旧 generation。
- [ ] 增加 bad full generation 不覆盖旧 LKG 的回归测试。
- [ ] 增加 serving-unverified crash recovery 测试。
- [ ] 增加 LKG retention/GC 测试。
- [ ] 增加 rollback target missing 硬阻断测试。
- [ ] 新增 runbook: verified LKG promotion。
- [ ] 新增 runbook: pinned rollback recovery。

#### Phase -1C: Evidence 四态和可信身份

- [ ] 将核心 evidence 状态从 bool 扩展为 pass/fail/unknown/stale。
- [ ] 为每个 invariant 定义 evidence freshness。
- [ ] 为每个 invariant 定义 unknown behavior。
- [ ] 为每个 invariant 定义 stale behavior。
- [ ] 为每个 invariant 定义最少独立来源数。
- [ ] 为需要 consensus 的 invariant 定义 failure-domain 要求。
- [ ] 设计 `ExpectedConsumerSet` 模型。
- [ ] ExpectedConsumerSet 记录 topology revision。
- [ ] ExpectedConsumerSet 记录 required/optional consumer。
- [ ] ExpectedConsumerSet 记录 component/node/scope。
- [ ] ExpectedConsumerSet 记录 failure domain 和 cohort。
- [ ] ExpectedConsumerSet 记录 protocol/schema version。
- [ ] ExpectedConsumerSet 记录 heartbeat/convergence deadline。
- [ ] release prepare 从服务端拓扑生成 ExpectedConsumerSet。
- [ ] required consumer 完全缺失时生成 unknown/block signal。
- [ ] heartbeat stale 时生成 stale/block signal。
- [ ] desired/actual/LKG generation 为空时不判 convergence。
- [ ] apply/probe empty/unknown 时不判 convergence。
- [ ] expected cardinality 与实际 heartbeat 数不一致时阻断扩大。
- [ ] 为 edge worker 定义专用 platform component identity。
- [ ] 为 DNS server 定义专用 platform component identity。
- [ ] 为 Caddy edge front 定义专用 platform component identity。
- [ ] 为 node guardian 定义专用 platform component identity。
- [ ] heartbeat endpoint 不接受普通 tenant API key。
- [ ] 服务端从凭证绑定 component/node/scope。
- [ ] heartbeat 请求增加 sequence。
- [ ] heartbeat 请求增加 issued_at。
- [ ] heartbeat 请求增加 nonce。
- [ ] heartbeat 请求增加 evidence hash。
- [ ] 拒绝 heartbeat replay。
- [ ] 拒绝 heartbeat future timestamp。
- [ ] 拒绝 heartbeat generation rollback。
- [ ] 拒绝跨 component/node/scope 冒充。
- [ ] consumer credential 支持轮换和撤销。
- [ ] consumer heartbeat 写入可信 audit。
- [ ] 增加 missing consumer 不得 pass 的回归测试。
- [ ] 增加 tenant key 无法写 heartbeat 的权限测试。
- [ ] 增加 heartbeat replay/impersonation 测试。
- [ ] 新增 runbook: platform consumer identity。
- [ ] 新增 runbook: missing/stale consumer。

#### Phase -1D: Release Lane 和 Fencing

- [ ] 定义 release lane key。
- [ ] 为 release lane 增加唯一 active release 约束。
- [ ] 设计 coordinator lease。
- [ ] 设计 monotonic fencing token。
- [ ] 设计 release-set CAS/version 字段。
- [ ] 设计 release-set idempotency key。
- [ ] 定义 prepare/commit/verify/finalize 状态机。
- [ ] 定义 abort/rollback 状态机。
- [ ] prepare 阶段固定 generation vector。
- [ ] prepare 阶段固定 rollback vector。
- [ ] commit 前验证所有对象 prepared。
- [ ] consumer apply 校验 fencing token。
- [ ] stale coordinator 写入被拒绝。
- [ ] promotion 与 rollback 并发时 rollback/hold 优先。
- [ ] coordinator crash 后支持安全 takeover。
- [ ] takeover 能区分 resume、abort 和 rollback。
- [ ] rollback 失败时冻结 release lane。
- [ ] 限制同一 lane 自动 rollback attempt。
- [ ] 增加并发 release 不产生双 active 的数据库测试。
- [ ] 增加 stale fencing token 测试。
- [ ] 增加 coordinator crash/recovery 集成测试。
- [ ] 增加 promotion/rollback race 测试。
- [ ] 新增 runbook: release lane recovery。
- [ ] 新增 runbook: frozen release lane。

#### Phase -1E: 数据库迁移协议

- [ ] 将 database schema generation 纳入 PlatformReleaseSet。
- [ ] 定义 expand/mixed-version/backfill/cutover/contract 阶段。
- [ ] 每个 binary 声明 min/max readable schema。
- [ ] 每个 binary 声明 min/max writable schema。
- [ ] migration artifact 记录 forward/rollback compatibility。
- [ ] 增加 migration dry-run。
- [ ] 增加 table-lock risk preflight。
- [ ] 增加 migration execution time budget。
- [ ] 增加 lock wait timeout。
- [ ] backfill 支持 checkpoint。
- [ ] backfill 支持暂停和恢复。
- [ ] backfill 支持限速。
- [ ] backfill 写入 audit/metrics。
- [ ] contract migration 等待旧 binary 全部退出。
- [ ] Helm rollback 前检查 schema compatibility。
- [ ] 不可逆 migration 禁止错误自动 rollback。
- [ ] schema 变更前检查 backup freshness。
- [ ] schema 变更前检查最近 restore drill。
- [ ] migration coordinator 校验 release fencing token。
- [ ] 增加 N/N-1 mixed-version migration 测试。
- [ ] 增加 migration crash/retry 测试。
- [ ] 增加 old binary rollback compatibility 测试。
- [ ] 新增 runbook: platform schema migration。
- [ ] 新增 runbook: incompatible schema rollback。

### Phase 0: 现状梳理和边界确认

- [ ] 列出所有当前平台 artifact kind 和 consumer。
- [ ] 列出所有当前 gate policy。
- [ ] 列出所有当前 automatic action。
- [ ] 列出所有当前 autonomy env / kill switch。
- [ ] 列出所有当前 release guard signal。
- [ ] 列出所有当前 public synthetic probe。
- [ ] 列出所有当前 LKG cache path 和 max stale 策略。
- [ ] 标记哪些机制目前只是文档设计。
- [ ] 标记哪些机制已经在代码里 enforcement。
- [ ] 标记哪些机制是 shadow/canary。

### Phase 1: Invariant Registry

- [ ] 定义 `InvariantDefinition` 模型。
- [ ] 为每个 invariant 增加 scope/category/owner/evidence source。
- [ ] 为每个 invariant 绑定 gate policy。
- [ ] 为每个 invariant 绑定 automatic action contract。
- [ ] 为每个 invariant 绑定 runbook。
- [ ] 将 release guard 使用的 invariant 接入 registry。
- [ ] 将 robustness status 使用的 invariant 接入 registry。
- [ ] 将 node deep health 的 hard/warning 语义接入 registry。
- [ ] 将 edge eligible set invariant 接入 registry。
- [ ] 将 DNS answer invariant 接入 registry。
- [ ] 将 artifact validation invariant 接入 registry。
- [ ] 将 platform consumer drift invariant 接入 registry。
- [ ] 增加 `fugue admin invariant ls`。
- [ ] 增加 `fugue admin invariant show <id>`。
- [ ] 增加 invariant registry 单元测试。

### Phase 2: Action Safety Evaluator

- [ ] 定义 `ActionSafetyRequest`。
- [ ] 定义 `ActionSafetyDecision`。
- [ ] 实现 contract lookup。
- [ ] 实现 gate mode 检查。
- [ ] 实现 kill switch 检查。
- [ ] 实现 canary scope 检查。
- [ ] 实现 blast-radius cap 检查。
- [ ] 实现 minimum samples 检查。
- [ ] 实现 minimum failure domains 检查。
- [ ] 实现 soak window 检查。
- [ ] 实现 rollback target 检查。
- [ ] 实现 human approval boundary 检查。
- [ ] 将 node quarantine 接入 evaluator。
- [ ] 将 edge quarantine 接入 evaluator。
- [ ] 将 DNS answer-time filtering 接入 evaluator。
- [ ] 将 edge route filtering 接入 evaluator。
- [ ] 将 node repair task claim 接入 evaluator。
- [ ] 将 LKG reload 接入 evaluator。
- [ ] 将 endpoint fallback 接入 evaluator。
- [ ] 将 release rollback 接入 evaluator。
- [ ] 增加 action safety 单元测试。
- [ ] 增加 blast-radius 回归测试。

### Phase 3: Platform Release Set

- [ ] 设计 `PlatformReleaseSet` 模型。
- [ ] 增加 release set store migration。
- [ ] release set 增加 release lane。
- [ ] release set 增加 state version/CAS 字段。
- [ ] release set 增加 coordinator lease。
- [ ] release set 增加 fencing token。
- [ ] release set 增加 idempotency key。
- [ ] release set 增加 generation vector。
- [ ] release set 增加 rollback vector。
- [ ] release set 增加 database schema generation。
- [ ] release set 增加 ExpectedConsumerSet revision。
- [ ] 记录 git sha / workflow run id / actor。
- [ ] 记录 changed files。
- [ ] 记录 subsystem risk classes。
- [ ] 记录 required gates。
- [ ] 记录 watch windows。
- [ ] 记录 artifact generations。
- [ ] 记录 image tags。
- [ ] 记录 Helm revision。
- [ ] 记录 public data-plane slot。
- [ ] 记录 node-updater desired generation。
- [ ] 记录 gate policy generation。
- [ ] 记录 guardian policy generation。
- [ ] 记录 rollback targets。
- [ ] 记录 candidate/serving-unverified/verified-LKG generations。
- [ ] 发布脚本开始时创建 release set。
- [ ] 发布脚本开始时获取 release lane lease。
- [ ] 发布脚本开始时固定 rollback vector。
- [ ] prepare 阶段验证 rollback artifact/image/config 可用。
- [ ] shadow 发布时更新 release set。
- [ ] canary 发布时更新 release set。
- [ ] full 发布时更新 release set。
- [ ] full 发布后进入 verifying，不立即标记 verified-good。
- [ ] consumer convergence 后更新 release set。
- [ ] public/local probe 后更新 release set。
- [ ] watch window 后更新 release set。
- [ ] verified-good 后才晋升 verified LKG。
- [ ] rollback 时更新 release set。
- [ ] rollback 验证所有 consumer 收敛到 rollback vector。
- [ ] rollback 验证 public API/DNS/edge 直连。
- [ ] rollback 失败时冻结 release lane。
- [ ] coordinator crash 后支持 takeover。
- [ ] stale coordinator 无法更新 release set。
- [ ] CLI 增加 `fugue admin release-set ls`。
- [ ] CLI 增加 `fugue admin release-set show <id>`。
- [ ] CLI 增加 `fugue admin release-set rollback <id>`。
- [ ] CLI 增加 `fugue admin release-set hold <id>`。
- [ ] CLI 增加 `fugue admin release-set resume <id>`。
- [ ] CLI 展示 generation/rollback vector。
- [ ] CLI 展示 lease/fencing/ExpectedConsumerSet。
- [ ] 增加 release set 单元测试。
- [ ] 增加 release set rollback 集成测试。
- [ ] 增加两个并发 release-set 竞争同一 lane 的测试。
- [ ] 增加 prepare 中途失败测试。
- [ ] 增加 commit 中途 coordinator crash 测试。
- [ ] 增加 rollback/promotion race 测试。
- [ ] 增加 verified LKG 晋升时点测试。

### Phase 4: Consumer Convergence

- [ ] 定义 platform consumer contract。
- [ ] 定义 consumer protocol version。
- [ ] 定义 consumer compatibility capability。
- [ ] 定义 consumer heartbeat freshness。
- [ ] 定义 consumer required/optional 语义。
- [ ] 实现 ExpectedConsumerSet topology builder。
- [ ] 发布期间固定 ExpectedConsumerSet revision。
- [ ] 节点拓扑变化时记录 convergence scope 变化。
- [ ] edge worker 上报 desired/actual/LKG/apply/probe。
- [ ] DNS server 上报 desired/actual/LKG/apply/probe。
- [ ] Caddy edge front 上报 config generation。
- [ ] node-updater 上报 desired state generation。
- [ ] runtime agent 上报 placement/continuity generation。
- [ ] 所有 heartbeat 上报 release-set id。
- [ ] 所有 heartbeat 上报 fencing token。
- [ ] 所有 heartbeat 上报 protocol/schema version。
- [ ] 所有 heartbeat 上报 sequence/issued-at/evidence hash。
- [ ] heartbeat 认证使用 platform component identity。
- [ ] heartbeat handler 服务端绑定 component/node/scope。
- [ ] heartbeat handler 拒绝 tenant API key。
- [ ] heartbeat handler 拒绝 replay。
- [ ] heartbeat handler 拒绝 generation 倒退。
- [ ] release guard 检查 consumer drift。
- [ ] release guard 检查 expected consumer missing。
- [ ] release guard 检查 expected cardinality。
- [ ] release guard 检查 heartbeat stale。
- [ ] release guard 检查 empty/unknown apply/probe。
- [ ] release guard 检查 compatibility floor。
- [ ] release set 检查 consumer convergence。
- [ ] consumer stale heartbeat 生成 incident。
- [ ] consumer completely missing 生成 incident。
- [ ] consumer identity mismatch 生成 security incident。
- [ ] LKG expired 生成 block incident。
- [ ] CLI 增加 `fugue admin consumer ls`。
- [ ] CLI 增加 `fugue admin consumer show <id>`。
- [ ] CLI 增加 `fugue admin consumer expected --release-set <id>`。
- [ ] CLI 区分 pass/fail/unknown/stale。
- [ ] 增加 consumer convergence 单元测试。
- [ ] 增加 zero consumer 不得 pass 的回归测试。
- [ ] 增加 stale/empty heartbeat 回归测试。
- [ ] 增加 heartbeat 身份冒充测试。
- [ ] 增加 consumer N/N-1 compatibility 测试。
- [ ] 增加 LKG expired 回归测试。

### Phase 5: Node Guardian

- [ ] 把 node-updater timer 行为拆分为 guardian loop 和 task runner。
- [ ] guardian loop 支持 observe-only。
- [ ] guardian loop 支持 canary scope。
- [ ] guardian loop 支持 local WAL。
- [ ] guardian loop 支持控制面不可用时本地观测。
- [ ] guardian loop 支持控制面恢复后 WAL replay。
- [ ] 增加 k3s agent process probe。
- [ ] 增加 kubelet process probe。
- [ ] 增加 local apiserver / remotedialer probe。
- [ ] 增加 node lease freshness probe。
- [ ] 增加 CRI probe。
- [ ] 增加 pod-netns kube-dns Service probe。
- [ ] 增加 pod-netns CoreDNS pod probe。
- [ ] 增加 same namespace service DNS/TCP probe。
- [ ] 增加 CNI bridge probe。
- [ ] 增加 PodCIDR route probe。
- [ ] 增加 kube-proxy / iptables probe。
- [ ] 增加 conntrack probe。
- [ ] 增加 disk / inode probe。
- [ ] 增加 time sync probe。
- [ ] 增加 edge/dns systemd probe。
- [ ] 增加 LKG bundle file probe。
- [ ] refresh desired state 接入 Action Safety Evaluator。
- [ ] reload LKG bundle 接入 Action Safety Evaluator。
- [ ] restart stateless Fugue service 接入 Action Safety Evaluator。
- [ ] managed iptables repair 接入 Action Safety Evaluator。
- [ ] 增加 guardian canary rollout 测试。
- [ ] 增加 guardian bad probe shadow-only 回归测试。

### Phase 6: DNS / Edge 自治

- [ ] edge worker 启动前强制 load verified LKG。
- [ ] edge worker 新 bundle apply 失败时继续服务 LKG。
- [ ] edge worker Caddy reload 失败时保留上一代 config。
- [ ] edge worker fallback 行为写 WAL。
- [ ] DNS server 启动前强制 load verified LKG。
- [ ] DNS server 新 bundle apply 失败时继续服务 previous LKG。
- [ ] DNS LKG 超过 max stale 后进入 degraded/fail-closed。
- [ ] DNS answer-time filtering 接入 Action Safety Evaluator。
- [ ] DNS answer filtering 增加 eligible set 不清零保护。
- [ ] DNS filtering 定义 healthy/LKG/suspect/confirmed-failed/unknown 状态。
- [ ] 每个 hostname policy 显式声明 preserve-LKG 或 fail-closed。
- [ ] 最终 live-health filter 不允许隐式返回空答案。
- [ ] unknown/stale evidence 默认保持 previous verified answer。
- [ ] DNS answer change 增加 minimum dwell time。
- [ ] DNS fail/recover threshold 增加 hysteresis。
- [ ] DNS temporary filter 增加最大连续时长。
- [ ] peer health 信号增加 generation。
- [ ] peer health 信号增加 TTL。
- [ ] peer health 信号增加 evidence hash。
- [ ] peer health 信号增加 signer/component identity。
- [ ] peer health 信号增加 sequence 和 issued-at。
- [ ] 单 peer 证据只能 shadow/降权。
- [ ] 多 failure domain 证据一致才允许 temporary filter。
- [ ] peer 信号过期自动清除。
- [ ] edge worker 获取签名 peer inventory。
- [ ] peer fallback 排除当前 edge。
- [ ] peer fallback 排除已尝试/draining/quarantined edge。
- [ ] peer fallback 直连 alternate peer IP。
- [ ] peer fallback 保持原 Host 和 TLS SNI。
- [ ] peer fallback 增加 per-host/edge circuit breaker。
- [ ] peer fallback 记录完整 attempt trace。
- [ ] peer inventory unavailable 时禁止伪 fallback。
- [ ] 增加 DNS bad bundle LKG 回归测试。
- [ ] 增加 DNS answer-time filtering blast-radius 测试。
- [ ] 增加 all candidates filtered property test。
- [ ] 增加 unknown evidence 不清空 answer 测试。
- [ ] 增加 DNS flapping/hysteresis 测试。
- [ ] 增加 peer fallback 不返回当前 edge 测试。
- [ ] 增加 peer fallback budget/circuit-breaker 测试。
- [ ] 增加 Caddy bad config rollback 测试。

### Phase 7: Fugue 平台 API 请求级 Failover

- [ ] 定义平台 API idempotency contract。
- [ ] 增加 idempotency key middleware。
- [ ] 增加 operation idempotency store。
- [ ] edge attempt trace 支持多 attempt。
- [ ] request explain 展示 failover attempt。
- [ ] 定义签名 alternate peer inventory。
- [ ] alternate peer 选择排除当前 edge。
- [ ] alternate peer 选择排除同一失败域。
- [ ] alternate peer 选择校验 generation/freshness。
- [ ] alternate peer 直连保持 Host/SNI。
- [ ] 定义单 attempt timeout。
- [ ] 定义总 replay budget。
- [ ] 定义最大 attempt 数。
- [ ] 定义 per-host/edge circuit breaker。
- [ ] 定义 retry storm 全局预算。
- [ ] GET/HEAD/OPTIONS 保持现有 safe replay。
- [ ] POST 默认不 replay。
- [ ] 声明 idempotent 的平台 POST 可 replay。
- [ ] replay 前确认 response header 未写。
- [ ] replay 前确认 request body 完整 buffer。
- [ ] streaming 禁止 replay。
- [ ] upload 禁止 replay。
- [ ] 增加平台 API POST failover 单元测试。
- [ ] 增加重复 idempotency key 回归测试。
- [ ] 增加 fallback DNS 再次返回当前 edge 的回归测试。
- [ ] 增加 alternate peer inventory stale 测试。
- [ ] 增加 fallback storm/backpressure 测试。

### Phase 8: Release Ledger

- [ ] 设计 `ReleaseAttempt` / `ReleaseLedger` 模型。
- [ ] 增加 store migration。
- [ ] 发布脚本创建 release attempt。
- [ ] 记录 changed files。
- [ ] 记录 risk class。
- [ ] 记录 required gates。
- [ ] 记录 watch windows。
- [ ] 记录 baseline robustness。
- [ ] 记录 post-deploy robustness。
- [ ] 记录 release guard。
- [ ] 记录 public synthetic。
- [ ] 记录 platform autonomy。
- [ ] 记录 consumer convergence。
- [ ] 记录 rollback target。
- [ ] 记录 final decision。
- [ ] CLI 增加 `fugue admin release attempts ls`。
- [ ] CLI 增加 `fugue admin release attempts show <id>`。
- [ ] CLI 增加 `fugue admin release attempts explain <id>`。
- [ ] 增加 release ledger 单元测试。
- [ ] 增加发布失败自动 rollback ledger 测试。

### Phase 9: Incident DAG / Explain

- [ ] 设计 incident DAG 数据模型。
- [ ] public synthetic failure 写入 DAG。
- [ ] DNS answer decision 写入 DAG。
- [ ] edge request error 写入 DAG。
- [ ] edge route bundle generation 写入 DAG。
- [ ] Caddy apply generation 写入 DAG。
- [ ] platform artifact release 写入 DAG。
- [ ] platform consumer heartbeat 写入 DAG。
- [ ] node deep health 写入 DAG。
- [ ] node repair action 写入 DAG。
- [ ] release attempt 写入 DAG。
- [ ] rollback action 写入 DAG。
- [ ] 实现 caused_by 边。
- [ ] 实现 observed_by 边。
- [ ] 实现 blocked_by 边。
- [ ] 实现 recovered_by 边。
- [ ] 实现 same_generation 边。
- [ ] 实现 same_request 边。
- [ ] 实现 same_release_set 边。
- [ ] CLI 增加 `fugue admin incident ls`。
- [ ] CLI 增加 `fugue admin incident show <id>`。
- [ ] CLI 增加 `fugue admin incident explain <id>`。
- [ ] 增加 incident DAG 单元测试。

### Phase 10: Emergency Status

- [ ] 增加 emergency status API。
- [ ] 增加 global autonomy mode 输出。
- [ ] 增加 global kill switch 输出。
- [ ] 增加 repair/quarantine/DNS filtering/peer overlay/endpoint fallback 输出。
- [ ] 增加 gate policy mode 摘要。
- [ ] 增加 enforced gate 列表。
- [ ] 增加 canary gate 列表。
- [ ] 增加 active quarantine 列表。
- [ ] 增加 temporary DNS filters 列表。
- [ ] 增加 LKG serving consumer 列表。
- [ ] 增加 expired LKG consumer 列表。
- [ ] 增加 node-updater desired generation 输出。
- [ ] 增加 edge route bundle generation 输出。
- [ ] 增加 DNS bundle generation 输出。
- [ ] 增加 Caddy config generation 输出。
- [ ] 增加 public data-plane slot 输出。
- [ ] 增加 rollback target 输出。
- [ ] CLI 增加 `fugue admin emergency status`。
- [ ] 增加 emergency status 快照测试。

### Phase 11: Chaos Drill

- [ ] 增加控制面 API 不可达 drill。
- [ ] 增加 DNS bad bundle drill。
- [ ] 增加 edge bad route bundle drill。
- [ ] 增加 Caddy reload fail drill。
- [ ] 增加 edge worker crash drill。
- [ ] 增加 node guardian bad probe drill。
- [ ] 增加 LKG expired drill。
- [ ] 增加 peer false positive drill。
- [ ] drill 默认 dry-run / shadow。
- [ ] drill 结果写入 release readiness。
- [ ] CLI 增加 `fugue admin drill run <id> --dry-run`。
- [ ] 增加 drill 单元测试。

### Phase 12: 文档与 Runbook

- [ ] 更新 `docs/self-organization-recovery-resilience-upgrade-plan.md`，引用本文作为平台控制闭环实施文档。
- [ ] 更新 `docs/bad-update-release-safety-plan.md`，说明 release set 和 action safety evaluator。
- [ ] 更新 `docs/control-plane-outage-data-plane-autonomy-plan.md`，说明 consumer convergence 和 incident DAG。
- [ ] 新增 runbook: invariant registry。
- [ ] 新增 runbook: action safety evaluator。
- [ ] 新增 runbook: platform release set rollback。
- [ ] 新增 runbook: node guardian recovery。
- [ ] 新增 runbook: DNS answer-time filtering。
- [ ] 新增 runbook: emergency status。
- [ ] 新增 runbook: incident DAG explain。

### Phase 13: 统一服务诊断和 Debug Bundle

- [ ] 梳理现有诊断入口：app status。
- [ ] 梳理现有诊断入口：app diagnose。
- [ ] 梳理现有诊断入口：request explain。
- [ ] 梳理现有诊断入口：edge route-check。
- [ ] 梳理现有诊断入口：DNS answer-check。
- [ ] 梳理现有诊断入口：edge quality-rank。
- [ ] 梳理现有诊断入口：robustness status。
- [ ] 梳理现有诊断入口：release guard status。
- [ ] 梳理现有诊断入口：node-updater health。
- [ ] 梳理现有诊断入口：operation/debug bundle。
- [ ] 定义 `ServiceDiagnosisRequest`。
- [ ] 定义 `ServiceDiagnosisResponse`。
- [ ] 定义 `ServiceDiagnosisVerdict`。
- [ ] 定义 `ServiceDiagnosisSection`。
- [ ] 定义 `ServiceDiagnosisCheck`。
- [ ] 定义 `ServiceDiagnosisEvidence`。
- [ ] ServiceDiagnosisEvidence 增加 source identity/trust。
- [ ] ServiceDiagnosisEvidence 增加 observed-at/freshness/expires-at。
- [ ] ServiceDiagnosisEvidence 增加 release-set id。
- [ ] ServiceDiagnosisEvidence 增加 source generation/vector。
- [ ] ServiceDiagnosisEvidence 增加 clock uncertainty。
- [ ] 定义 `ServiceDiagnosisMissingEvidence`。
- [ ] 定义 `ServiceDiagnosisRootCauseCandidate`。
- [ ] 定义 `ServiceDiagnosisRecommendedAction`。
- [ ] 定义 root cause enum。
- [ ] 在 OpenAPI 中增加 `POST /v1/admin/diagnostics/service`。
- [ ] 在 OpenAPI 中增加 `GET /v1/admin/diagnostics/service/{diagnosis_id}`。
- [ ] 在 OpenAPI 中增加 `GET /v1/admin/diagnostics/service/{diagnosis_id}/bundle`。
- [ ] 生成 OpenAPI 派生产物。
- [ ] 实现 target resolver：app name。
- [ ] 实现 target resolver：hostname。
- [ ] 实现 target resolver：URL。
- [ ] 实现 target resolver：request id。
- [ ] 实现 L0 本地公网探测模块。
- [ ] L0 支持 recursive DNS 查询。
- [ ] L0 支持 authoritative DNS 查询。
- [ ] L0 支持 TLS handshake probe。
- [ ] L0 支持 HTTP probe。
- [ ] L0 支持 per-edge `--resolve` probe。
- [ ] L0 支持 status/TTFB/total/body summary。
- [ ] 设计签名 diagnostic discovery bundle。
- [ ] CLI 本地缓存 diagnostic discovery bundle。
- [ ] diagnostic discovery bundle 包含 edge/DNS inventory。
- [ ] diagnostic discovery bundle 包含 generation/valid-until/revocation。
- [ ] 控制面不可用时 L0 使用缓存 bundle。
- [ ] 缓存 bundle 超过 max-stale 时明确标记 evidence stale。
- [ ] 实现 L1 控制面 aggregator。
- [ ] L1 创建 diagnosis epoch。
- [ ] L1 固定 as-of watermark。
- [ ] L1 固定 release-set/generation vector。
- [ ] L1 并发查询 app status。
- [ ] L1 并发查询 app diagnosis。
- [ ] L1 并发查询 route-check。
- [ ] L1 并发查询 DNS answer-check。
- [ ] L1 并发查询 edge quality-rank。
- [ ] L1 并发查询 request explain。
- [ ] L1 并发查询 robustness status。
- [ ] L1 并发查询 release guard status。
- [ ] L1 并发查询 platform autonomy status。
- [ ] L1 并发查询 recent operations。
- [ ] L1 并发查询 recent release attempts。
- [ ] L1 并发查询 recent edge/DNS bundle generations。
- [ ] L1 实现统一 timeout budget。
- [ ] L1 实现 partial failure，不因单项查询失败导致整体失败。
- [ ] L1 实现全局 concurrency budget。
- [ ] L1 实现 per-source concurrency budget。
- [ ] L1 实现 fanout cancellation。
- [ ] L1 实现 dependency circuit breaker。
- [ ] L1 限制日志/指标查询窗口。
- [ ] L1 限制结果行数和字节数。
- [ ] L1 不组合不相容 generation 的证据。
- [ ] L1 不组合超出 diagnosis epoch 的旧 evidence。
- [ ] 实现 L2 node probe 请求模型。
- [ ] L2 支持 edge node probe origin。
- [ ] L2 支持 DNS node probe authoritative answer。
- [ ] L2 支持 runtime node probe service DNS。
- [ ] L2 支持 runtime node probe ClusterIP。
- [ ] L2 probe 默认关闭。
- [ ] L2 probe 需要平台管理员权限。
- [ ] L2 probe 有 TTL。
- [ ] L2 probe 有 audit。
- [ ] L2 probe 有 rate limit。
- [ ] L2 probe 有 per-node concurrency budget。
- [ ] L2 probe 有 cluster-wide concurrency budget。
- [ ] L2 probe 不抢占 serving/control-loop 关键资源。
- [ ] 实现 Verdict classifier。
- [ ] Verdict classifier 支持 confirmed。
- [ ] Verdict classifier 支持 probable。
- [ ] Verdict classifier 支持 insufficient_evidence。
- [ ] Verdict classifier 禁止无证据猜根因。
- [ ] 实现 root cause: public DNS failure。
- [ ] 实现 root cause: authoritative DNS failure。
- [ ] 实现 root cause: DNS answer contains unhealthy edge。
- [ ] 实现 root cause: edge unreachable。
- [ ] 实现 root cause: edge TLS failure。
- [ ] 实现 root cause: edge no active route。
- [ ] 实现 root cause: no healthy edge group。
- [ ] 实现 root cause: edge origin DNS failure。
- [ ] 实现 root cause: edge origin connect failure。
- [ ] 实现 root cause: edge origin request write failure。
- [ ] 实现 root cause: edge origin TTFB timeout。
- [ ] 实现 root cause: edge body read slow。
- [ ] 实现 root cause: edge body read error。
- [ ] 实现 root cause: cluster service DNS failure。
- [ ] 实现 root cause: cluster ClusterIP connect failure。
- [ ] 实现 root cause: runtime pod not ready。
- [ ] 实现 root cause: runtime node not ready。
- [ ] 实现 root cause: control-plane API unavailable。
- [ ] 实现 root cause: release regression。
- [ ] 实现 root cause: platform artifact consumer drift。
- [ ] 实现 root cause: LKG expired。
- [ ] CLI 增加 `fugue diagnose service <target>`。
- [ ] CLI 增加 `fugue diagnose service <target> --since`。
- [ ] CLI 增加 `fugue diagnose service <target> --request-id`。
- [ ] CLI 增加 `fugue diagnose service <target> --deep`。
- [ ] CLI 增加 `fugue diagnose service <target> --json`。
- [ ] CLI 增加 `fugue admin diagnose service <target>`。
- [ ] CLI 增加 `fugue admin diagnose service <target> --include-node-probes`。
- [ ] CLI 增加 `fugue debug bundle service <target>`。
- [ ] CLI 文本输出包含 verdict。
- [ ] CLI 文本输出包含各 section summary。
- [ ] CLI 文本输出包含 missing evidence。
- [ ] CLI JSON 输出包含完整 evidence。
- [ ] Debug bundle 包含 metadata。
- [ ] Debug bundle 包含 diagnosis response。
- [ ] Debug bundle 包含 public probe result。
- [ ] Debug bundle 包含 route/DNS/edge snapshots。
- [ ] Debug bundle 包含 request explain。
- [ ] Debug bundle 包含 relevant logs。
- [ ] Debug bundle 包含 relevant metrics summary。
- [ ] Debug bundle 包含 release/recent changes。
- [ ] Debug bundle 包含 redaction report。
- [ ] Debug bundle 可 zip 导出。
- [ ] Debug bundle 包含 diagnosis epoch/watermark。
- [ ] Debug bundle 包含 release-set/generation vector。
- [ ] Debug bundle 包含 evidence freshness/trust report。
- [ ] Debug bundle 有最大大小。
- [ ] Debug bundle 有生成 timeout。
- [ ] Debug bundle 有 retention/expiry。
- [ ] Debug bundle GC 不删除 active incident 引用 bundle。
- [ ] 普通用户权限只返回自己 app/hostname 的证据。
- [ ] 平台管理员权限返回 edge/DNS/node/platform evidence。
- [ ] 默认 redacted。
- [ ] `--redact=false` 必须要求 `--confirm-raw-output`。
- [ ] 增加 target resolver 单元测试。
- [ ] 增加 public probe 单元测试。
- [ ] 增加 aggregator partial failure 测试。
- [ ] 增加 aggregator source timeout/circuit-breaker 测试。
- [ ] 增加 diagnosis mixed-generation 不得 confirmed 测试。
- [ ] 增加 stale cached discovery bundle 测试。
- [ ] 增加 diagnosis concurrency/backpressure 测试。
- [ ] 增加 classifier confirmed 测试。
- [ ] 增加 classifier probable 测试。
- [ ] 增加 classifier insufficient evidence 测试。
- [ ] 增加 CLI golden output 测试。
- [ ] 增加 debug bundle redaction 测试。
- [ ] 增加权限隔离测试。
- [ ] 新增 runbook: unified service diagnosis。
- [ ] 新增 runbook: service debug bundle。

### Phase 14: 长期鲁棒性演进机制

- [ ] 定义事故复盘到 invariant 的标准流程。
- [ ] 定义事故复盘到 release gate 的标准流程。
- [ ] 定义事故复盘到 synthetic probe 的标准流程。
- [ ] 定义事故复盘到 chaos drill 的标准流程。
- [ ] 定义事故复盘到 diagnosis classifier 的标准流程。
- [ ] 设计 `IncidentLearningRecord` 模型。
- [ ] 记录 confirmed root cause。
- [ ] 记录 probable root cause。
- [ ] 记录 missing evidence。
- [ ] 记录新增 invariant 建议。
- [ ] 记录新增 metric 建议。
- [ ] 记录新增 probe 建议。
- [ ] 记录新增 release gate 建议。
- [ ] 记录新增 chaos drill 建议。
- [ ] 记录新增 runbook 建议。
- [ ] 实现 incident learning CLI：`fugue admin incident learn <incident-id>`。
- [ ] 实现 incident learning API。
- [ ] incident learning 默认只生成建议，不自动 enforcement。
- [ ] incident learning 建议必须可审计。
- [ ] incident learning 建议必须可 dismiss。
- [ ] incident learning 建议必须可转成 issue/checklist。
- [ ] 建立 failure exercise registry。
- [ ] 建立周期性 failure exercise runner。
- [ ] failure exercise 默认 dry-run。
- [ ] failure exercise 默认 shadow。
- [ ] failure exercise 支持控制面 API 临时不可达场景。
- [ ] failure exercise 支持坏 route bundle 场景。
- [ ] failure exercise 支持坏 DNS bundle 场景。
- [ ] failure exercise 支持 Caddy reload fail 场景。
- [ ] failure exercise 支持 edge worker crash 场景。
- [ ] failure exercise 支持 node guardian bad generation 场景。
- [ ] failure exercise 支持 LKG expired 场景。
- [ ] failure exercise 支持 artifact consumer drift 场景。
- [ ] failure exercise 支持 metrics backend unavailable 场景。
- [ ] failure exercise 支持 registry unavailable 场景。
- [ ] failure exercise 结果写入 release readiness。
- [ ] failure exercise 结果写入 robustness status。
- [ ] failure exercise 结果写入 incident DAG。
- [ ] 建立核心安全属性列表。
- [ ] 为 DNS answer filtering 建立 property-based test。
- [ ] 为 quarantine blast-radius 建立 property-based test。
- [ ] 为 release set rollback consistency 建立 property-based test。
- [ ] 为 canary scope isolation 建立 property-based test。
- [ ] 为 LKG expiry semantics 建立 property-based test。
- [ ] 为 shadow gate no-production-impact 建立 property-based test。
- [ ] 为 action safety evaluator 建立 fuzz/property test。
- [ ] 为 artifact validation 建立 fuzz/property test。
- [ ] 建立平台依赖图模型。
- [ ] 建立 control-plane API 依赖图。
- [ ] 建立 controller 依赖图。
- [ ] 建立 edge worker 依赖图。
- [ ] 建立 DNS server 依赖图。
- [ ] 建立 Caddy edge front 依赖图。
- [ ] 建立 node guardian 依赖图。
- [ ] 建立 registry/image-cache 依赖图。
- [ ] 建立 observability/metrics 依赖图。
- [ ] 为每个依赖定义 unavailable 行为。
- [ ] 为每个依赖定义 degraded 行为。
- [ ] 为每个依赖定义 stale generation 行为。
- [ ] 为每个依赖定义 bad output 行为。
- [ ] 为每个依赖定义 fail-open / fail-closed 策略。
- [ ] 为每个依赖定义 release blocking 策略。
- [ ] 为每个依赖定义 LKG 策略。
- [ ] 建立诊断质量指标。
- [ ] 统计 confirmed root cause 比例。
- [ ] 统计 probable root cause 比例。
- [ ] 统计 insufficient evidence 比例。
- [ ] 统计每类 incident 的 missing evidence 分布。
- [ ] 统计平均诊断耗时。
- [ ] 统计 debug bundle 离线复现成功率。
- [ ] 统计需要 SSH 才能确认的问题比例。
- [ ] 统计 diagnosis classifier 误判回滚率。
- [ ] 统计人工改判 root cause 次数。
- [ ] 建立诊断质量 dashboard。
- [ ] 建立策略推荐引擎。
- [ ] 根据 soak/samples 建议 gate 从 shadow 升级 canary。
- [ ] 根据 soak/samples 建议 gate 从 canary 升级 enforced。
- [ ] 根据误报率建议 gate 降级 shadow。
- [ ] 根据事故数据建议新增 invariant。
- [ ] 根据事故数据建议新增 probe。
- [ ] 根据事故数据建议新增 chaos drill。
- [ ] 根据 edge 质量历史建议 exploration budget。
- [ ] 策略推荐默认只建议，不自动执行。
- [ ] 策略推荐必须要求人工 promotion。
- [ ] 策略推荐必须经过 action safety evaluator。
- [ ] 策略推荐必须有 rollback path。
- [ ] 将 gate policy 完全数据化。
- [ ] 将 automatic action contract 完全数据化。
- [ ] 将 invariant registry 完全数据化。
- [ ] 将 release watch window 完全数据化。
- [ ] 将 diagnosis classifier 规则数据化。
- [ ] 将 edge/DNS traffic safety 策略 artifact 化。
- [ ] 将 node guardian policy artifact 化。
- [ ] 将 chaos drill registry artifact 化。
- [ ] 所有数据化策略必须有 schema validation。
- [ ] 所有数据化策略必须有 dry-run diff。
- [ ] 所有数据化策略必须有 shadow/canary/full 发布路径。
- [ ] 所有数据化策略必须有 rollback target。
- [ ] 建立鲁棒性成熟度 scorecard。
- [ ] scorecard 覆盖发布安全。
- [ ] scorecard 覆盖自动修复安全。
- [ ] scorecard 覆盖数据面自治。
- [ ] scorecard 覆盖诊断质量。
- [ ] scorecard 覆盖演练覆盖率。
- [ ] scorecard 覆盖证据完整性。
- [ ] scorecard 覆盖配置化程度。
- [ ] CLI 增加 `fugue admin resilience scorecard`。
- [ ] CLI 增加 `fugue admin resilience gaps`。
- [ ] 建立控制循环健康模型。
- [ ] 控制循环健康覆盖 invariant evidence freshness。
- [ ] 控制循环健康覆盖 gate policy owner/runbook/kill switch 完整性。
- [ ] 控制循环健康覆盖 automatic action contract 完整性。
- [ ] 控制循环健康覆盖 artifact consumer convergence。
- [ ] 控制循环健康覆盖 release set rollback target 可用性。
- [ ] 控制循环健康覆盖 diagnosis confirmed/probable/insufficient_evidence 趋势。
- [ ] 控制循环健康覆盖 chaos drill 最近运行时间。
- [ ] 控制循环健康覆盖 LKG 路径最近演练时间。
- [ ] 控制循环健康覆盖 release guard signal 覆盖率。
- [ ] 控制循环健康覆盖 public synthetic 覆盖率。
- [ ] 控制循环健康覆盖 incident learning backlog。
- [ ] CLI 增加 `fugue admin resilience control-loop status`。
- [ ] CLI 增加 `fugue admin resilience control-loop gaps`。
- [ ] 控制循环健康异常写入 robustness status。
- [ ] 控制循环健康异常写入 release guard report-only signal。
- [ ] 设计 `IncidentReplaySnapshot` 模型。
- [ ] Incident replay snapshot 包含 platform artifact generations。
- [ ] Incident replay snapshot 包含 edge route bundle。
- [ ] Incident replay snapshot 包含 DNS answer bundle。
- [ ] Incident replay snapshot 包含 Caddy config signature。
- [ ] Incident replay snapshot 包含 edge/DNS/node health snapshot。
- [ ] Incident replay snapshot 包含 release set state。
- [ ] Incident replay snapshot 包含 gate policy state。
- [ ] Incident replay snapshot 包含 action contract state。
- [ ] Incident replay snapshot 包含 request/edge metrics摘要。
- [ ] Incident replay snapshot 包含 relevant logs 摘要。
- [ ] Incident replay snapshot 默认 redacted。
- [ ] 实现 `fugue admin incident snapshot <incident-id>`。
- [ ] 实现 `fugue admin incident replay <snapshot>`。
- [ ] 实现 replay 本地 dry-run 模式。
- [ ] replay 验证新 invariant 是否能识别旧事故。
- [ ] replay 验证新 diagnosis classifier 是否能给出 confirmed/probable 结论。
- [ ] replay 验证 release guard 是否会阻断同类坏发布。
- [ ] replay 验证 rollback/LKG 路径是否可用。
- [ ] replay 结果可写回 incident learning record。
- [ ] 建立 simulation harness。
- [ ] simulation harness 支持 artifact mutation。
- [ ] simulation harness 支持 edge health mutation。
- [ ] simulation harness 支持 DNS answer mutation。
- [ ] simulation harness 支持 consumer drift mutation。
- [ ] simulation harness 支持 gate policy mutation。
- [ ] simulation harness 支持 LKG expiry/corruption mutation。
- [ ] simulation harness 不访问生产系统。
- [ ] simulation harness 结果进入 CI。
- [ ] 新增 runbook: incident learning。
- [ ] 新增 runbook: failure exercise。
- [ ] 新增 runbook: property-based safety checks。
- [ ] 新增 runbook: dependency degradation matrix。
- [ ] 新增 runbook: resilience scorecard。
- [ ] 新增 runbook: control loop health。
- [ ] 新增 runbook: incident replay and simulation。

### Phase 15: 运行级安全、SLO 和长期容量治理

#### Phase 15A: Baseline 单调性和风险归因

- [x] 设计 blocker stable identity。
- [ ] blocker 记录 quantitative observed value。
- [ ] blocker 记录 affected scope/cardinality。
- [ ] blocker 记录 evidence fingerprint。
- [ ] blocker 记录 first/last observed time。
- [ ] blocker 记录 tolerance budget。
- [ ] blocker 记录 baseline expiry。
- [x] baseline comparison 检测新 blocker。
- [x] baseline comparison 检测 severity 升级。
- [x] baseline comparison 检测影响面扩大。
- [x] baseline comparison 检测定量指标恶化。
- [ ] baseline comparison 检测 pass 退化为 unknown/stale。
- [ ] changed subsystem blocker 默认不享受旧 baseline 容忍。
- [x] baseline tolerance 到期自动失效。
- [ ] CLI 展示 blocker baseline diff。
- [ ] CLI 展示 tolerated budget 和 expiry。
- [ ] 建立 component ownership manifest。
- [ ] 建立 package-to-component dependency graph。
- [ ] 建立 Helm template-to-component dependency graph。
- [ ] 建立 OpenAPI/schema-to-consumer dependency graph。
- [x] shared model/store/config 变化传播风险分类。
- [x] 无法归因的 changed file 默认 high-risk hold。
- [ ] build plan 输出 risk classes。
- [ ] build plan 输出 required gates。
- [ ] build plan 输出 watch windows。
- [ ] build plan 输出 canary cohorts。
- [ ] public synthetic 使用 machine-readable Fugue error class。
- [ ] 不再只依赖英文响应 body substring 分类。
- [ ] synthetic rollback policy 支持 hostname/platform-scope。
- [x] 增加同名 blocker 影响面扩大回归测试。
- [x] 增加同名 blocker 定量恶化回归测试。
- [x] 增加 shared dependency risk propagation 测试。
- [x] 增加 unknown changed file high-risk 测试。
- [ ] 新增 runbook: release baseline regression。
- [ ] 新增 runbook: changed-file risk attribution。

#### Phase 15B: 时间、冲突和控制循环稳定性

- [ ] 定义所有 TTL/lease/soak/watch duration 的 monotonic clock 语义。
- [ ] 定义跨节点 wall-clock uncertainty 字段。
- [ ] 定义 issued-at/not-before/expires-at 允许偏差。
- [ ] NTP 未同步时 evidence 标记 unknown/stale。
- [ ] 时钟倒退不能延长 expired lease。
- [ ] 时钟倒退不能延长 expired LKG。
- [ ] 时钟前跳不能直接触发全局 quarantine。
- [ ] 增加 clock-jump simulation。
- [ ] 定义自动动作优先级矩阵。
- [ ] kernel kill switch 高于所有普通 action。
- [ ] operator emergency hold 高于 promotion。
- [ ] rollback 高于本地 guardian recovery。
- [ ] 新 desired state 高于 stale WAL replay。
- [ ] 所有 action 增加 idempotency key。
- [ ] 所有 action 增加 source generation。
- [ ] 所有 action 增加 fencing token。
- [ ] 所有 action 增加 expected-current-version。
- [ ] 所有 action 增加 compensation action。
- [ ] WAL replay 只重放仍适用于当前 generation 的动作。
- [ ] stale WAL 只允许补交 audit，不允许覆盖新状态。
- [ ] health fail/recover 使用不同 threshold。
- [ ] health 状态变化要求连续样本。
- [ ] health 状态变化要求 minimum dwell time。
- [ ] quarantine 增加 cooldown。
- [ ] DNS filtering 增加 cooldown。
- [ ] service restart 增加 cooldown。
- [ ] rollback 增加 cooldown。
- [ ] 每个 subject 增加 automatic action rate budget。
- [ ] cluster 增加全局并发 automatic action budget。
- [ ] 定义 flapping 状态。
- [ ] flapping 状态停止重复破坏性动作。
- [ ] flapping 状态生成 incident 和人工建议。
- [ ] 增加 action precedence race 测试。
- [ ] 增加 WAL/new desired-state conflict 测试。
- [ ] 增加 flapping/oscillation simulation。
- [ ] 新增 runbook: control-loop flapping。
- [ ] 新增 runbook: clock uncertainty。

#### Phase 15C: Retention、GC 和容量治理

- [ ] 定义 platform artifact retention。
- [ ] 定义 artifact content dedup/compaction。
- [ ] 定义 artifact release message retention。
- [ ] 定义 release ledger retention。
- [ ] 定义 consumer heartbeat history retention。
- [ ] 定义 incident DAG retention。
- [ ] 定义 automatic action audit retention。
- [ ] 定义 node/edge/DNS WAL retention。
- [ ] 定义 debug bundle retention。
- [ ] 定义 incident replay snapshot retention。
- [ ] 定义 synthetic/diagnosis evidence retention。
- [ ] GC 保护 active release。
- [ ] GC 保护 verified LKG。
- [ ] GC 保护 pinned rollback target。
- [ ] GC 保护 active incident 引用对象。
- [ ] GC 保护未完成 release set。
- [ ] GC 保护最近 restore drill 对象。
- [ ] GC 执行前生成 dry-run plan。
- [ ] GC 有 max delete objects/bytes budget。
- [ ] GC 有 kill switch。
- [ ] GC 结果写 audit。
- [ ] WAL 支持 rotation 和 checksum。
- [ ] WAL corruption 不阻塞 serving path。
- [ ] 为各类控制面数据增加容量指标。
- [ ] 增加 retention 即将删除 rollback target 告警。
- [ ] 增加 artifact/message/incident cardinality 告警。
- [ ] 增加 GC 不能删除 protected generation 测试。
- [ ] 增加 WAL rotation/corruption 测试。
- [ ] 增加长期数据量 compaction benchmark。
- [ ] 新增 runbook: resilience data retention。
- [ ] 新增 runbook: protected rollback assets。

#### Phase 15D: 供应链和控制闭环信任

- [ ] 控制面镜像使用 digest pin。
- [ ] edge/DNS/node guardian 镜像使用 digest pin。
- [ ] 关键平台镜像支持 signature verification。
- [ ] 关键平台 artifact 支持 provenance。
- [ ] Release Set 记录镜像 digest/signature/provenance。
- [ ] 发布前验证 rollback image 可拉取。
- [ ] rollback image 受 retention protection。
- [ ] 发布前验证 artifact signing key 未撤销。
- [ ] 定义 component identity key rotation。
- [ ] 定义 bundle/artifact signing key rotation。
- [ ] key rotation 支持重叠生效窗口。
- [ ] 定义 key/certificate/token revocation。
- [ ] 定义 credential expiry 告警。
- [ ] compromised node 单点 evidence 只能标记 suspect。
- [ ] 多 failure-domain consensus 才允许扩大生产动作。
- [ ] evidence signer 与 topology identity 交叉验证。
- [ ] security-sensitive audit 使用 hash chain 或等价防篡改机制。
- [ ] 增加 invalid/revoked signer 测试。
- [ ] 增加 rollback image missing 测试。
- [ ] 增加 compromised consumer false heartbeat drill。
- [ ] 新增 runbook: platform supply-chain verification。
- [ ] 新增 runbook: component credential compromise。

#### Phase 15E: Fugue 平台 SLO 和 Error Budget

- [ ] 定义 control-plane API availability SLO。
- [ ] 定义 control-plane API latency SLO。
- [ ] 定义 authoritative DNS availability SLO。
- [ ] 定义 authoritative DNS answer-correctness SLO。
- [ ] 定义 edge route correctness SLO。
- [ ] 定义 Fugue platform 5xx SLO。
- [ ] 定义 consumer convergence latency SLO。
- [ ] 定义 verified LKG freshness SLO。
- [ ] 定义 release rollback success-rate SLO。
- [ ] 定义 release rollback latency SLO。
- [ ] 定义 node guardian detection latency SLO。
- [ ] 定义 node guardian safe-action latency SLO。
- [ ] 定义 diagnosis completeness SLO。
- [ ] 定义 evidence freshness SLO。
- [ ] 为每个 SLO 定义 measurement source。
- [ ] 为每个 SLO 定义 missing-data 语义。
- [ ] 建立平台 error budget。
- [ ] error budget 耗尽时冻结高风险发布。
- [ ] error budget 耗尽时禁止扩大 blast radius。
- [ ] error budget 耗尽时自动策略只允许转向更保守模式。
- [ ] error budget 不影响数据面继续服务 verified LKG。
- [ ] release guard 展示 error-budget state。
- [ ] CLI 增加 `fugue admin resilience slo status`。
- [ ] CLI 增加 `fugue admin resilience error-budget`。
- [ ] 增加 SLO missing-data 测试。
- [ ] 增加 error-budget release-freeze 测试。
- [ ] 新增 runbook: platform SLO breach。
- [ ] 新增 runbook: error-budget release freeze。

#### Phase 15F: Canary Cohort 和资源优先级

- [ ] 定义 canary cohort 模型。
- [ ] cohort 记录 component role。
- [ ] cohort 记录 OS/distribution。
- [ ] cohort 记录 kernel。
- [ ] cohort 记录 k3s/container runtime version。
- [ ] cohort 记录 iptables/nftables backend。
- [ ] cohort 记录 CNI/network mode。
- [ ] cohort 记录 provider/region/failure-domain。
- [ ] cohort 记录 hardware architecture。
- [ ] promotion 检查 changed subsystem 的代表性 cohort。
- [ ] 没有代表性 cohort 时 promotion 返回 evidence insufficient。
- [ ] node/edge/DNS rollout 自动选择最小代表性 canary set。
- [ ] 定义 serving/control-loop/diagnosis 三层优先级。
- [ ] serving traffic 优先于所有 deep diagnosis。
- [ ] control-loop action 优先于 debug bundle/export。
- [ ] diagnosis aggregator 有全局 worker budget。
- [ ] diagnosis aggregator 有 per-source worker budget。
- [ ] node probes 有 per-node 和 cluster-wide budget。
- [ ] chaos drill 有独立资源预算。
- [ ] metrics/logs query 有窗口、行数和字节上限。
- [ ] debug bundle 有大小和生成时间上限。
- [ ] resource budget exhaustion 返回 partial/insufficient evidence。
- [ ] resource budget exhaustion 不触发错误自动修复。
- [ ] 增加 canary cohort coverage 测试。
- [ ] 增加 diagnosis/control-loop resource contention 测试。
- [ ] 增加 metrics backend slow/unavailable backpressure 测试。
- [ ] 新增 runbook: canary cohort selection。
- [ ] 新增 runbook: control-loop resource saturation。

## 8. 推荐实施顺序

不能再直接从 Invariant Registry 开始实现。最先完成的必须是 Phase -1，否则后续 registry、DAG、scorecard 和 replay 会建立在不可靠的 LKG、consumer evidence 和 release state 上。

第一阶段：安全语义地基。

1. 不可绕过 Platform Safety Kernel。
2. Verified LKG 状态机和 pinned rollback target。
3. Evidence pass/fail/unknown/stale 四态。
4. ExpectedConsumerSet 和 platform component identity。
5. Release Lane、lease、fencing、CAS 和 crash recovery。
6. 数据库 expand/contract 和 mixed-version compatibility。

这六项完成前：

- 不应把现有 platform consumer inventory 当成完整 release gate。
- 不应把最新 full generation 自动视为 verified LKG。
- 不应允许新 automatic action 直接进入全量 enforcement。
- 不应声称 Platform Release Set 已经具备原子 rollback。

第二阶段：最小可执行控制闭环。

7. Invariant Registry。
8. Action Safety Evaluator。
9. Platform Release Set。
10. Consumer Convergence。
11. Release Baseline 单调比较和 changed-file 风险归因。
12. Emergency Status。
13. 统一服务诊断。

第三阶段：数据面和节点自治。

14. Node Guardian。
15. DNS / Edge 自治强化。
16. 确定性 alternate-edge request failover。
17. Release Ledger。
18. Incident DAG。

第四阶段：长期运行和证明能力。

19. Debug Bundle 深度归档。
20. Chaos Drill。
21. Incident Replay / Simulation。
22. Retention、GC 和 rollback asset protection。
23. 平台 supply-chain 和 component identity rotation。
24. Fugue 平台 SLO、error budget 和 release freeze。
25. Canary Cohort 和 control-loop resource budget。
26. 长期鲁棒性演进机制。

## 9. 长期演进：这不是终点

本文 Phase -1 到 Phase 15 全部完成后，Fugue 会拥有一套完整的平台鲁棒性控制闭环，但这仍然不是自组织、自恢复、鲁棒性能力的尽头。

原因是鲁棒性不是静态功能，而是持续逼近未知故障的工程体系。Phase -1 到 Phase 15 主要解决的是已知平台故障类型的发现、限制、解释、回滚、修复和长期运行安全。长期看，Fugue 还需要具备从新事故中学习、将经验自动固化为检查和演练、验证组合状态安全性的能力。

本节仍然只讨论 Fugue 平台自身，不讨论用户服务，不讨论新增服务器，不讨论物理限制。

### 9.1 事故经验自动固化

每次平台事故结束后，不能只产生人工总结。系统应把事故转成可执行的工程资产。

事故复盘至少沉淀：

- confirmed root cause。
- probable root cause。
- missing evidence。
- 新增 invariant 建议。
- 新增 metric 建议。
- 新增 synthetic probe 建议。
- 新增 release gate 建议。
- 新增 chaos drill 建议。
- 新增 diagnosis classifier 规则建议。
- 新增 runbook 建议。

输出形式：

```text
incident
  -> learning record
  -> proposed invariant / probe / gate / drill / classifier / runbook
  -> human review
  -> shadow artifact
  -> canary
  -> enforced
```

关键原则：

- 系统可以自动生成建议。
- 系统不能绕过人类 review 自动 enforcement。
- 所有建议必须有 evidence。
- 所有建议必须可 dismiss。
- 所有建议必须可转成 issue 或 checklist。
- 所有建议必须进入 artifact / gate / invariant 的正常发布路径。

### 9.2 故障注入常态化

Chaos drill 不应只是发布前的临时动作，而应成为平台周期性体检。

需要周期性覆盖：

- 控制面 API 临时不可达。
- route bundle 为空或 schema 错误。
- DNS bundle 为空或签名错误。
- DNS LKG 过期。
- edge worker crash。
- edge worker serving stale LKG。
- Caddy reload fail。
- node guardian bad generation。
- node guardian false positive。
- artifact consumer drift。
- release guard blocked。
- registry unavailable。
- image-cache unavailable。
- metrics backend unavailable。
- public synthetic 503。

默认策略：

- dry-run。
- shadow。
- 限定 scope。
- 不影响真实流量。
- 演练结果写入 robustness status。
- 演练结果写入 release readiness。
- 演练结果写入 incident DAG。

目标不是制造故障，而是持续证明：

- 坏 artifact 不会进入 full。
- LKG 路径真的可用。
- rollback 路径真的可用。
- diagnosis 能解释故障。
- release guard 能挡住回归。

### 9.3 核心安全属性形式化

一些平台安全规则不适合只靠普通单元测试。它们应该变成 property-based test、fuzz test 或轻量形式化约束。

必须长期验证的属性：

- DNS answer filtering 不能把 eligible edge 清零，除非 explicit fail-closed。
- quarantine blast-radius 不能超过 cap。
- canary gate 不能影响非 canary scope。
- shadow gate 不能影响生产流量。
- release set rollback 必须恢复一致 generation。
- LKG expired 不能被标记 healthy。
- LKG corrupt 不能被继续 serving 为 healthy。
- artifact validation fail 不能进入 full release。
- automatic action 没有 contract 时不能影响生产。
- kill switch 开启时 automatic action 必须停止。

这些规则需要覆盖组合状态，而不是只覆盖单个 happy path。

### 9.4 平台依赖图和降级矩阵

Fugue 平台自身需要维护一张明确的依赖图。

至少覆盖：

- control-plane API。
- controller。
- Postgres。
- registry。
- image-cache。
- edge worker。
- DNS server。
- Caddy edge front。
- node guardian。
- release guard。
- artifact store。
- observability metrics。
- GitHub Actions runner。

每个依赖都要定义：

- unavailable 时哪些能力继续可用。
- degraded 时哪些能力只读可用。
- stale generation 时哪些发布必须暂停。
- bad output 时如何 fail closed。
- 是否允许 serve LKG。
- 是否允许 automatic repair。
- 是否允许 automatic quarantine。
- 是否 block release。
- 是否产生 incident。
- 是否需要人工确认。

目标是让 Fugue 不再只回答“健康/不健康”，而是回答：

```text
这个依赖坏了以后，平台哪些路径继续服务，哪些路径降级，哪些路径必须停止，哪些动作可以自动做。
```

### 9.5 诊断质量指标

统一诊断上线后，还必须持续度量诊断系统自身质量。

核心指标：

- confirmed root cause 比例。
- probable root cause 比例。
- insufficient evidence 比例。
- 每类 incident 的 missing evidence 分布。
- 平均诊断耗时。
- debug bundle 离线复现成功率。
- 需要 SSH 才能确认的问题比例。
- diagnosis classifier 被人工改判次数。
- diagnosis classifier 误判导致错误行动次数。
- request explain 命中率。
- release explain 命中率。

目标：

- 逐步降低 insufficient evidence。
- 逐步降低必须 SSH 才能确认的问题。
- 逐步提高 debug bundle 离线复盘成功率。
- 逐步把人工经验固化成 classifier / invariant / probe。

### 9.6 保守的策略推荐

Fugue 可以根据历史数据自动建议策略变化，但不能绕过安全流程自动 enforcement。

可建议：

- 某个 gate 从 shadow 进入 canary。
- 某个 gate 从 canary 进入 enforced。
- 某个 gate 因误报率高降级到 shadow。
- 某个 invariant 缺少 evidence。
- 某个 synthetic probe 应新增。
- 某个 chaos drill 应新增。
- 某个 edge exploration budget 应调整。
- 某个 diagnosis classifier 规则需要更新。

不允许：

- 自动把建议直接变成 enforced。
- 自动扩大 blast-radius。
- 自动取消 kill switch。
- 自动绕过 soak。
- 自动绕过 samples。
- 自动绕过 human approval boundary。

### 9.7 全面配置化

长期目标是 Fugue 行为不再频繁通过改代码改变，而是通过可审计、可验证、可回滚的平台配置 artifact 改变。

应数据化：

- invariant registry。
- gate policy registry。
- automatic action contracts。
- release watch windows。
- release rollback signals。
- diagnosis classifier rules。
- edge ranking policy。
- traffic safety policy。
- node guardian policy。
- chaos drill registry。
- dependency degradation matrix。

代码负责执行通用机制，行为通过 artifact 发布。

所有配置 artifact 必须：

- schema validate。
- signature/provenance validate。
- dry-run diff。
- shadow publish。
- canary publish。
- full publish。
- rollback target。
- consumer convergence。
- public synthetic verification。

全面配置化必须受 `PlatformSafetyKernel` 约束：

- 配置可以降低动作范围、提高样本要求、延长 soak、缩短 TTL。
- 配置不能关闭 generation monotonicity、trusted evidence、rollback target、shadow isolation 和 blast-radius hard cap。
- 配置不能让 unknown/stale evidence 伪装成 pass。
- 配置不能授权 consumer 声明任意 component/node/scope。
- 配置不能让未验证 generation 直接成为 verified LKG。
- 修改 safety kernel 本身必须走代码发布、专门 review、property test 和独立 release gate，不能通过普通 artifact 在线变更。

### 9.8 鲁棒性成熟度 Scorecard

为了避免鲁棒性建设变成一次性项目，Fugue 应有长期 scorecard。

Scorecard 维度：

- 发布安全成熟度。
- 自动修复安全成熟度。
- 数据面自治成熟度。
- 诊断质量成熟度。
- 演练覆盖率。
- evidence 完整性。
- 配置化程度。
- LKG/rollback 可用性。
- incident learning 转化率。

CLI：

```bash
fugue admin resilience scorecard
fugue admin resilience gaps
```

Scorecard 不是为了打分好看，而是为了持续暴露下一批最应该修的系统缺口。

### 9.9 控制循环自身健康

当前文档的大部分设计都是为了让 Fugue 具备控制闭环。但控制闭环本身也会退化，因此后续必须监控“自愈系统是否健康”。

不能只监控：

- API 是否 200。
- edge 是否 ready。
- DNS 是否能回答。
- node 是否 Ready。

还必须监控：

- invariant 是否都有新鲜 evidence。
- invariant 是否都有 owner。
- gate policy 是否都有 runbook。
- gate policy 是否都有 kill switch。
- canary gate 是否真的有 canary scope。
- automatic action contract 是否完整。
- action contract 是否声明 TTL、blast-radius、recovery condition、rollback action。
- release set 是否都有 rollback target。
- artifact consumer 是否持续 convergence。
- LKG 是否近期演练过。
- public synthetic 是否覆盖核心路径。
- diagnosis 的 confirmed 比例是否下降。
- insufficient evidence 是否上升。
- chaos drill 是否长期未执行。
- incident learning backlog 是否堆积。

新增 CLI：

```bash
fugue admin resilience control-loop status
fugue admin resilience control-loop gaps
```

控制循环健康异常默认不应直接阻断生产流量，但应该进入：

- robustness status。
- release guard report-only signal。
- resilience scorecard。
- incident learning backlog。

当控制循环健康持续恶化时，再通过 gate policy 正常 promotion 进入 release blocking，而不是临时硬编码。

### 9.10 事故快照重放和仿真

更高阶的鲁棒性不是只在生产上“看见事故”，而是能够把事故保存成可重放样本。

每次 confirmed 平台事故都应能生成 `IncidentReplaySnapshot`。

Snapshot 至少包含：

- incident 元数据。
- confirmed/probable root cause。
- missing evidence。
- 相关 request id。
- 相关 release set。
- 相关 artifact generations。
- edge route bundle。
- DNS answer bundle。
- Caddy config signature。
- gate policy state。
- action contract state。
- edge/DNS/node health snapshot。
- platform consumer state。
- LKG state。
- public synthetic result。
- relevant logs 摘要。
- relevant metrics 摘要。

Replay 目标：

- 验证新 invariant 能否识别旧事故。
- 验证 diagnosis classifier 能否给出同样或更好的结论。
- 验证 release guard 能否阻断同类坏发布。
- 验证 rollback/LKG 路径是否可用。
- 验证 action safety evaluator 是否会拒绝危险动作。
- 验证 debug bundle 是否足够离线复盘。

CLI：

```bash
fugue admin incident snapshot <incident-id> --output incident.json
fugue admin incident replay incident.json
```

Replay 必须默认：

- 本地执行。
- dry-run。
- 不访问生产系统。
- 不执行修复。
- 不修改线上状态。
- 默认 redacted。

在此基础上，可以建立 simulation harness：

- mutation route bundle。
- mutation DNS answer bundle。
- mutation edge health。
- mutation consumer drift。
- mutation LKG expiry/corruption。
- mutation gate policy。
- mutation action contract。

Simulation harness 应进入 CI，用历史事故样本和合成样本证明核心安全属性没有回归。

### 9.11 长期目标

长期目标不是让 Fugue “永远不出故障”。这是不现实的。

长期目标是：

- 新故障能被快速定位。
- 新故障不会扩大成全局事故。
- 新故障能产生可复用 evidence。
- 新故障能转成新的 invariant / gate / probe / drill。
- 坏自动修复不会造成更大事故。
- 坏发布能被自动挡住或回滚。
- 控制面短暂不可用时数据面能按 LKG 自治。
- 操作员不需要每次重新发明排障流程。
- 控制循环自身退化时能被发现。
- 旧事故能被重放，新机制能证明可以挡住同类事故。

也就是说，Phase -1 到 Phase 13 是 Fugue 鲁棒性的控制闭环地基；Phase 14 负责让机制持续学习和演进；Phase 15 负责证明这些机制在长期运行、容量增长、供应链变化和资源竞争下仍然可靠。

## 10. 不做的事情

本文不包含：

- 用户服务 safe rollout。
- 用户服务 failover。
- 用户服务 stateful recovery。
- 新增控制面服务器。
- 新增 edge 服务器。
- 新增 DNS 服务器。
- 云厂商级物理 HA。
- anycast。

这些能力可以单独规划，但不应混入本文的 Fugue 平台自身鲁棒性控制闭环。
