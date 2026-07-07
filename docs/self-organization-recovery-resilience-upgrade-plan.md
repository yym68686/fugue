# Fugue self-organization, self-healing, and resilience upgrade plan

本文档固定 Fugue 系统性提升自组织、自恢复和鲁棒性的下一阶段改造方案。它基于近期两类生产事故抽象通用能力：

- edge / DNS / route 机制更新后，真实流量调度、健康判断和回退保护暴露不足。
- `ns101351` 节点上的 kube-dns Service `10.43.0.10:53` 被过期 Fugue local DNS escape-hatch iptables DNAT 规则转发到不存在的 `10.42.8.1:53`，导致 0-0 Web/API pod 内部 DNS 解析失败，进而出现前端认证不可用和 API `503 upstream unavailable`。

本文不是单次事故热修记录，也不是替代已有计划。它补充并收敛以下文档：

- `docs/self-healing-robustness-plan.md`
- `docs/edge-quality-ranking-scoped-plan.md`
- `docs/dynamic-edge-onboarding-plan.md`
- `docs/connection-aware-zero-downtime-drain-plan.md`
- `docs/zero-downtime-rollout-incidents.md`

本文也吸收 Apollo 配置中心的架构经验。参考资料以 Apollo 官方仓库和官方文档为准：

- `https://github.com/apolloconfig/apollo`
- `https://www.apolloconfig.com/#/zh/design/apollo-design`
- `https://www.apolloconfig.com/#/zh/client/java-sdk-user-guide`
- `https://www.apolloconfig.com/#/zh/portal/apollo-user-guide`

所有影响 control plane、edge proxy、edge-front、DNS、Caddy、Ingress、cluster bootstrap、node updater、runtime 路由或平台级流量规则的实现，都必须回到本仓库走正式发布链路：提交到 `main`，再由 `.github/workflows/deploy-control-plane.yml` 更新远端控制平面。只有明确的紧急恢复才允许手工线上热修；热修后必须把同等修复回写到本仓库。

## 1. 总体判断

Fugue 当前的问题不是缺少某一个指标或某一个脚本，而是缺少完整闭环。

当前很多子系统已经能生成 desired state、下发配置、采集指标和执行更新，但系统还没有持续证明以下事实：

```text
actual state still satisfies the core invariants required for safe traffic.
```

因此当实际节点状态发生漂移时，系统可能继续认为一切正常。例如这次 DNS 事故中：

- control plane desired state 并没有明确表达“把 pod DNS 转发到 10.42.8.1”。
- `10.42.8.1` 已经不是任何当前节点的 PodCIDR。
- 但节点 NAT 表实际仍存在旧 DNAT 规则。
- 系统没有持续验证 pod DNS 这个基础不变量。
- 系统没有在节点级 DNS 失败时自动隔离该节点上的工作负载。
- 系统没有把前端认证失败和 API upstream unavailable 自动归因到节点 DNS。

一个真正鲁棒的 Fugue 不应该只相信 desired state，也必须持续审计 actual state，并在 actual state 违反关键不变量时自动隔离、回退或修复。

## 2. 目标运行模型

Fugue 的目标运行模型应从“执行命令和发布配置”升级为“持续证明和闭环恢复”：

```text
observe actual state
  -> derive desired state
  -> validate hard invariants
  -> publish with canary and release gates
  -> probe published behavior
  -> compare user-impact metrics
  -> quarantine bad nodes/routes/edges
  -> repair reversible drift
  -> rollback unsafe changes
  -> explain exact cause and action
```

核心目标：

1. 任何单节点漂移不应默默影响生产用户。
2. 任何发布不应在关键不变量失败时继续扩大影响。
3. 任何 edge / DNS / route 变更都必须有 canary、观测、自动回退和服务级兜底。
4. 任何 5xx、认证不可用、upstream unavailable 都必须能自动归因到 edge、origin、DNS、runtime、DB、auth、quota 或 upstream 的明确类别。
5. 自动修复必须优先执行可逆动作，危险动作必须停在人工审批边界。

## 3. 设计原则

### 3.1 不变量优先

自恢复动作不能靠猜测触发。每一个自动动作都要由明确不变量驱动。

示例：

- pod 必须能解析 `kubernetes.default.svc`。
- app pod 必须能解析同 namespace 内 service。
- edge 必须能解析并访问 origin service。
- DNS answer 不得返回不 route-ready 的 edge。
- service-level edge exclusion 不得被 ranking 或 exploration 绕过。
- control plane 发布后 API/controller 必须保持多副本 ready。
- node updater 不得留下过期 iptables、systemd、Caddy、env 或 route bundle 状态。

### 3.2 隔离优先于修复

发现节点、edge 或 route 不满足硬不变量时，第一动作应该是降低用户影响，而不是立刻尝试复杂修复。

优先级：

1. 停止把新流量发往坏节点、坏 edge、坏 route。
2. 保留已有连接，能 drain 就 drain。
3. 记录 incident 和证据。
4. 执行可逆修复。
5. 修复后通过探针才恢复流量。

### 3.3 fail closed and serve LKG

如果新生成的 DNS bundle、route bundle、Caddy config、edge discovery bundle 或 node desired state 未通过验证，不能发布到全局。

安全默认行为：

- 不发布新 artifact。
- 继续服务 last known good。
- 标记 degraded。
- 记录被拒绝的 diff 和证据。
- 等输入状态变化或人工处理后重试。

### 3.4 作用域化健康，而非全局平均

edge、DNS、origin 和 runtime 健康不能只看全局平均值。必须按作用域归因：

- service / hostname
- edge group
- edge node
- client country / region / ASN
- traffic class
- request size class
- time window
- route generation

否则某个运营商、某个服务、某个请求类型上的问题会被全局平均掩盖。

### 3.5 自动化必须可解释

每个自动隔离、修复、回滚和拒绝发布动作都必须可解释：

- 触发了哪个不变量？
- 期望状态是什么？
- 实际状态是什么？
- 证据来自哪里？
- 系统采取了什么动作？
- 为什么该动作被认为安全？
- 什么情况下恢复？

### 3.6 假设每个子组件都会失败

Fugue 的新架构必须显式假设每个子组件都有可能出问题。鲁棒性不能建立在“某个 guardian、controller、edge、DNS server、release pipeline 永远正确”的前提上。

每个子组件都必须有 failure contract：

```text
component
  -> possible failure modes
  -> detection signals
  -> quarantine / blast-radius control
  -> fallback / LKG behavior
  -> reversible repair actions
  -> rollback path
  -> request / incident attribution
  -> human approval boundary
```

没有 failure contract 的子组件不能承载生产关键路径，也不能执行自动修复动作。

通用处理顺序：

```text
detect
  -> classify
  -> attribute
  -> quarantine or stop rollout
  -> serve LKG or fallback
  -> repair if safe
  -> probe
  -> restore traffic
  -> audit and incident summary
```

自动化组件本身也必须被视为可能失败：

- guardian 可能漏报。
- guardian 可能误报。
- repair 可能修错。
- release guard 可能误放行。
- request attribution 可能缺字段。
- LKG 可能过期或损坏。
- metrics 可能延迟或丢失。

因此所有 guardian / repair / rollback 都必须支持 shadow、dry-run、限流、幂等、TTL、audit、blast-radius cap 和 emergency disable switch。

## 4. 核心不变量

### 4.1 节点不变量

每个可承载 workload 的节点必须持续满足：

- kubelet / k3s-agent ready。
- CNI bridge 存在且 PodCIDR 与 Kubernetes Node `.spec.podCIDR` 一致。
- pod 内 DNS 到 kube-dns Service IP 可用。
- pod 内 DNS 到 CoreDNS pod IP 可用，作为诊断对照。
- pod 内能解析 `kubernetes.default.svc`。
- pod 内能解析并访问同 namespace service。
- iptables NAT 表不存在指向非当前 PodCIDR、非当前节点、非已知 managed target 的 Fugue 直连 DNAT 残留。
- conntrack 未超过安全阈值。
- 磁盘、inode、内存、CPU steal、load、time sync 在安全范围内。
- node updater 最近一次运行成功，且 generation 与 control plane desired generation 不长期漂移。

### 4.2 runtime / app 不变量

每个服务必须持续满足：

- desired replicas 与 ready replicas 在策略允许范围内。
- public route 指向的 service endpoint 存在。
- service DNS 可解析。
- service TCP connect 可用。
- HTTP readiness path 或 lightweight probe 可用。
- stateful app 的 DB service DNS 可解析。
- DB TCP connect 可用。
- DB credential 可加载，但诊断输出不得泄露 secret。
- app env generation 与 control plane 期望一致。

### 4.3 edge / DNS / route 不变量

每个公开 hostname 必须持续满足：

- DNS answer 只包含 eligible edge。
- eligible edge 必须通过 online、health、route-ready、TLS-ready、service exclusion、maintenance/drain gate。
- edge route bundle generation 与 control plane active generation 一致，或显式处于 LKG serving。
- edge 能解析 origin。
- edge 能连接 origin。
- edge 到 origin TTFB、request write、response wait、total time 在阈值内。
- client-to-edge body read rate、min-window rate、read gap、TCP retrans/RTO/lost 信号可采集并进入作用域评分。
- 新 edge exploration 不能绕过 hard gate。
- service exclusion 生效后，系统必须确认剩余 healthy edge 数量大于服务最低阈值。

### 4.4 控制面发布不变量

每次控制面、edge、DNS、route 或 node updater 相关发布必须满足：

- 发布前集群没有未确认的 block-rollout incident。
- 新 artifact shadow generation 通过 invariant validation。
- canary 副本通过 deep health。
- 发布期间 control plane API/controller 保持 quorum / min ready。
- 发布后 synthetic probe 对 platform hostname、核心 API、route/DNS/TLS、edge/origin 链路通过。
- 用户影响指标未显著恶化。
- 失败时自动 rollback 或停止 rollout，并保留 LKG。

## 5. 目标架构

### 5.1 Node Guardian

将当前 node updater 从“配置同步脚本”升级为“节点守护器”。

Node Guardian 负责：

- 采集节点 actual state。
- 执行节点 deep health。
- 持续审计 iptables、systemd、Caddy、edge env、node labels、taints、PodCIDR。
- 清理可逆漂移。
- 在节点违反硬不变量时自动 taint / quarantine。
- 把节点健康状态上报 control plane。
- 给 edge ranking 和 runtime scheduler 提供节点级健康信号。

Node Guardian 不应该只在 join 或定时更新时运行。它应该是持续的节点健康闭环。

初始检查：

- `pod_dns_kube_service_lookup`
- `pod_dns_direct_coredns_lookup`
- `same_namespace_service_lookup`
- `same_namespace_service_http_probe`
- `external_dns_lookup`
- `origin_connect_probe`
- `iptables_managed_rule_audit`
- `podcidr_actual_vs_kubernetes`
- `node_updater_generation_drift`
- `conntrack_saturation`

初始可逆修复：

- 删除 Fugue 管理范围内的过期 DNAT 规则。
- 重载 Caddy/edge bundle 的 LKG。
- 重新拉取 node desired state。
- 重启 node updater 生成的非状态服务。
- 为节点加临时 taint，阻止新 workload 调度。
- 将 edge 节点标记为 degraded，停止新 DNS answer。

### 5.2 Release Guardian

为所有控制面和数据面发布增加 release guardian。

Release Guardian 负责：

- 发布前读取 robustness baseline。
- 发布前阻止已有重大 incident 下的普通发布。
- 对 route/DNS/TLS/node updater/edge 相关变更强制 canary。
- 对 API/controller 使用最小副本 canary + rollout status + synthetic probe。
- 发布后比较短窗口 SLO。
- 自动 rollback 可回滚 artifact。
- 把失败原因写入 release incident。

硬 gate：

- `block_rollout=true` 时禁止普通发布。
- route/DNS bundle invalid 时禁止发布。
- service 可用 edge 数可能变成 0 时禁止发布。
- control plane canary deep health 失败时禁止继续滚动。
- edge canary 的真实 5xx、origin connect error、body read error 超阈值时禁止扩大流量。

### 5.3 Traffic Safety Controller

将流量安全从单纯 edge ranking 中拆出来，作为独立硬 gate。

Traffic Safety Controller 负责：

- 维护每个 service/hostname 的 eligible edge set。
- 维护 service-level edge exclusion。
- 计算 service 的 minimum healthy edge count。
- 管理新 edge exploration budget。
- 当某 edge/node 变差时执行局部摘除，而不是全局下线。
- 当候选 edge 为空时执行服务级 fallback，而不是直接把用户暴露给不可解释 503。

关键规则：

- ranking 只在 eligible edge set 内排序。
- exploration 只能在 hard gate 通过后发生。
- service exclusion 永远优先于 exploration 和 ranking。
- 某服务只剩 1 个 healthy edge 时，任何 exclusion 或 rollout 都需要更严格 gate。
- edge 质量下降必须按 service/client scope 生效，不能用全局平均误伤或误保。

### 5.4 Runtime Continuity Controller

将节点健康与 app 连续性连接起来。

Runtime Continuity Controller 负责：

- 识别 app 是否因节点故障导致 ready replicas 下降。
- 对 stateless app 自动迁移或重建。
- 对 stateful app 先做 lease/fence/backup/restore preflight。
- 对关键服务维护 min-ready 和 route-ready 不变量。
- 当某节点 DNS/CNI/iptables 失败时，优先迁走关键 stateless workload。

初始策略：

- stateless app：允许自动迁移。
- stateful app：只自动执行安全 preflight，不自动删除/迁移数据。
- 关键平台服务：必须有更高优先级和更严格探针。

### 5.5 Request Attribution Pipeline

将请求级可观测性升级为自动归因系统。

每个请求至少应能关联：

- request id / trace id
- hostname / service / app id / route generation
- DNS answer edge group / edge id
- edge front / edge worker node
- origin service / runtime pod / runtime node
- client country / region / ASN
- traffic class / request size class
- body read bytes / body read duration / effective bps / min-window bps / max read gap
- edge-to-origin DNS / connect / request write / response wait / TTFB / total
- response write / egress bps
- cache status
- TCP RTT / retrans / RTO / lost / delivery rate when available
- error class
- retry / fallback / LKG / quarantine decisions

目标是让系统能自动回答：

```text
This 503 was caused by edge-to-origin DNS failure on node X,
not by the user's API key, not by the app DB, and not by global edge quality.
```

### 5.6 Platform State Release System

Apollo 的核心经验不是“引入一个配置中心”，而是把可变配置纳入明确的发布治理：

```text
edit/draft
  -> validate
  -> release
  -> notify consumers
  -> consumers pull and cache
  -> observe instance version
  -> gray/full release or rollback
```

Fugue 应该把这个模型提升到平台状态层，形成 `Platform State Release System`。它负责所有会影响生产流量、节点行为、edge 行为、DNS answer、route bundle、Caddy config、runtime placement 和 node updater 行为的平台状态。

#### 5.6.1 Apollo architecture lessons

Apollo 的设计可以抽象为以下架构原则：

- 读写分离：`Config Service` 面向客户端读取和通知，`Admin Service` 面向管理端修改和发布。
- 管理面与数据面分离：`Portal` 是人机管理界面，客户端不依赖 Portal。
- 服务发现封装：客户端和 Portal 通过 `Meta Server` 获取服务实例列表，不直接耦合底层注册中心。
- 无状态多实例：`Config Service` 和 `Admin Service` 多实例、无状态部署，依赖数据库保存权威状态。
- 客户端负载均衡和重试：客户端获取多个 Config Service 后自行选择、重试。
- App/Env/Cluster/Namespace 范围模型：配置不只是 key/value，而是有明确作用域和权限边界。
- 编辑与发布分离：修改配置不会影响客户端，只有发布或回滚后才会被应用读取。
- 发布消息持久化：发布后写 `ReleaseMessage`，Config Service 扫描后通知客户端，不强依赖外部消息队列。
- 长轮询通知：客户端长轮询配置变化，收到 namespace 变化后再拉取最新配置。
- 定时拉取兜底：即使通知丢失，客户端也会周期性拉取并上报本地版本。
- 本地缓存容灾：服务端不可用或网络断开时，客户端从本地缓存恢复配置。
- Kubernetes ConfigMap 缓存增强：在本地缓存丢失且服务不可用时，还可以通过集群内缓存恢复。
- 灰度发布：先创建灰度版本，配置灰度规则，按实例 IP 或 label 生效，观察后全量发布或放弃灰度。
- 实例可见性：可以看到哪些实例使用主版本，哪些实例使用灰度版本。
- 回滚语义清晰：回滚发布出去的版本，不回滚编辑态，便于修正后重新发布。
- 权限和审计：编辑权限、发布权限、回滚权限分离，操作有审计记录。
- 少外部依赖：为提高可用性和降低部署复杂度，尽量少依赖外部组件。

Fugue 要借鉴的是这些状态治理原则，而不是 Apollo 的具体 Java/Spring/Eureka 技术栈。

#### 5.6.2 Mapping Apollo concepts to Fugue

Apollo 到 Fugue 的概念映射：

```text
Apollo AppId
  -> Fugue service / node / edge group / runtime cell / platform subsystem

Apollo Env
  -> Fugue environment / release channel / prod / staging / canary / shadow

Apollo Cluster
  -> Fugue edge group / node pool / runtime cell / region / provider lane

Apollo Namespace
  -> Fugue artifact kind

Apollo Release
  -> Fugue immutable platform state generation

Apollo Gray Release
  -> Fugue canary / exploration / scoped traffic release

Apollo Rollback
  -> Fugue publish previous validated generation as active

Apollo Client
  -> Fugue edge, DNS, node guardian, runtime agent, controller worker

Apollo Local Cache
  -> Fugue node-side / edge-side / DNS-side LKG cache

Apollo Instance List
  -> Fugue consumer generation inventory

Apollo ReleaseMessage
  -> Fugue durable platform state release notification
```

Fugue artifact kind 应至少覆盖：

- `edge_route_bundle`
- `dns_answer_bundle`
- `caddy_route_config`
- `edge_ranking_policy`
- `traffic_safety_policy`
- `service_edge_exclusion`
- `node_desired_state`
- `node_updater_script`
- `runtime_placement_plan`
- `runtime_continuity_plan`
- `node_guardian_policy`
- `release_guard_policy`
- `request_attribution_policy`
- `chaos_drill_plan`

#### 5.6.3 Platform state lifecycle

每个会影响生产行为的平台状态必须按统一生命周期流转：

```text
draft
  -> schema_validated
  -> invariant_validated
  -> compatibility_validated
  -> shadow_generated
  -> canary_released
  -> observed
  -> fully_released
  -> lkg_eligible
  -> superseded
```

失败路径：

```text
draft
  -> rejected

canary_released
  -> aborted
  -> rolled_back

fully_released
  -> rolled_back
```

硬规则：

- `draft` 不能被数据面消费。
- 未通过 schema validation 的 artifact 不得进入 invariant validation。
- 未通过 invariant validation 的 artifact 不得进入 shadow generation。
- 未通过 compatibility validation 的 artifact 不得发给低版本 node/edge/runtime。
- 未通过 canary 观察窗口的 artifact 不得 full release。
- 只有 `fully_released` 且 probe 通过的 artifact 才能成为 LKG。
- rollback 不是手工改配置，而是发布一个指向旧 generation 的新 release record。

#### 5.6.4 Data model

新增或收敛一组平台状态表。命名可在实现时调整，但语义必须保留。

`platform_artifacts`：

- `id`
- `artifact_kind`
- `scope_type`
- `scope_id`
- `environment`
- `cluster_or_cell`
- `content_hash`
- `content_ref`
- `schema_version`
- `producer`
- `created_by`
- `created_at`

`platform_artifact_releases`：

- `id`
- `artifact_id`
- `generation`
- `status`
- `release_channel`
- `previous_generation`
- `rollback_target_generation`
- `validation_result_ref`
- `probe_result_ref`
- `canary_rule_ref`
- `lkg_eligible`
- `released_by`
- `released_at`

`platform_release_messages`：

- `id`
- `artifact_kind`
- `scope_type`
- `scope_id`
- `generation`
- `message_type`
- `created_at`
- `consumed_watermark`

`platform_consumer_instances`：

- `id`
- `consumer_type`
- `consumer_id`
- `node_name`
- `edge_id`
- `hostname`
- `artifact_kind`
- `scope_type`
- `scope_id`
- `desired_generation`
- `actual_generation`
- `lkg_generation`
- `last_poll_at`
- `last_apply_at`
- `last_health_at`
- `health_summary`
- `drift_seconds`

`platform_lkg_snapshots`：

- `artifact_kind`
- `scope_type`
- `scope_id`
- `generation`
- `content_hash`
- `stored_at`
- `expires_at`
- `last_verified_at`
- `verification_result_ref`

#### 5.6.5 Read/write plane separation

Apollo 的 `Config Service` / `Admin Service` 分离应映射为 Fugue 的平台状态读写分离。

写路径：

```text
operator / controller
  -> Platform State Admin API
  -> create draft
  -> validate
  -> release
  -> write release message
```

读路径：

```text
edge / DNS / node guardian / runtime agent
  -> Platform State Read API
  -> long-poll generation changes
  -> pull active generation
  -> local validate
  -> atomically apply
  -> report actual generation and health
```

关键边界：

- Admin API 可写，只允许控制器、CLI、自动修复器和授权 operator 使用。
- Read API 只读，面向数据面消费者，应更高可用、更简单、更容易缓存。
- Portal / Web console 不参与数据面读取。
- 数据面组件不应读取 draft。
- 数据面组件拉不到 control plane 时继续服务 LKG。

#### 5.6.6 Notification and reconciliation

Apollo 的 long polling + periodic pull 模型应成为 Fugue 数据面的默认状态同步机制。

Fugue consumer loop：

```text
load local LKG
start serving LKG if active state unavailable
long-poll active generation for subscribed artifact scopes
if generation changes:
  pull artifact
  validate compatibility
  stage locally
  apply atomically
  probe locally
  report actual generation
periodically reconcile even when no notification arrives
report drift and health
```

通知机制：

- release 后写 `platform_release_messages`。
- Read API 基于 release message 或 generation index 唤醒 long-poll。
- consumer 定时拉取上报本地 generation。
- control plane 维护 consumer generation drift。
- 通知丢失不影响最终收敛，定时拉取兜底。
- control plane 短时不可用不影响数据面服务 LKG。

#### 5.6.7 Local cache and LKG

Apollo 本地缓存的思想必须扩展到 Fugue 所有数据面组件。

建议路径：

```text
/var/lib/fugue/lkg/edge-route-bundle/<scope>.json
/var/lib/fugue/lkg/dns-answer-bundle/<scope>.json
/var/lib/fugue/lkg/caddy-route-config/<scope>.json
/var/lib/fugue/lkg/node-desired-state/<node>.json
/var/lib/fugue/lkg/node-updater-script/<node>.sh
/var/lib/fugue/lkg/runtime-placement/<runtime-cell>.json
```

LKG 写入规则：

- 只写入 validated + applied + probed 成功的 generation。
- 写入必须 atomic rename。
- 写入必须带 content hash。
- 读取必须校验 hash。
- LKG 必须带过期策略。
- LKG 过期后不能静默继续服务，必须进入 degraded 并告警。
- LKG serving 必须上报 control plane。

Kubernetes 场景可以参考 Apollo ConfigMap cache 的思想，为关键平台状态提供集群内备份缓存，但不能把 ConfigMap 作为唯一权威源。权威源仍是 control plane DB 和已发布 artifact record。

#### 5.6.8 Gray release and canary

Apollo 灰度发布按实例 IP 或 label 生效。Fugue 应采用更强的作用域灰度：

- node label
- edge id
- edge group
- runtime cell
- service / hostname
- client country / region / ASN
- traffic class
- request size class
- route generation
- percentage budget

Fugue gray release 示例：

```yaml
artifact_kind: edge_ranking_policy
scope:
  hostname: api.example.com
gray_rule:
  edge_groups: ["default"]
  client_asns: ["cn-mobile", "cn-unicom"]
  traffic_classes: ["api", "sse"]
  percent: 1
observe:
  min_duration: 30m
  min_samples: 500
abort_if:
  upstream_unavailable_rate_delta: "> 0.5%"
  body_read_error_rate_delta: "> 1%"
  p95_origin_connect_ms_multiplier: "> 1.5"
  healthy_edge_count: "< 2"
```

灰度规则：

- 灰度必须显式指定作用域。
- 灰度不得绕过 hard gate。
- 灰度必须记录命中请求和 consumer generation。
- 灰度必须可放弃。
- 灰度成功后才能 full release。
- full release 是 release 动作，不是直接修改当前 draft。

#### 5.6.9 Instance generation visibility

Apollo 可以看到实例使用主版本还是灰度版本。Fugue 必须把这类可见性产品化。

Fugue 必须能回答：

- 哪些 edge 正在服务 route bundle generation X？
- 哪些 DNS 节点正在回答 DNS answer bundle generation X？
- 哪些 node 正在使用 node desired state generation X？
- 哪些 node updater 仍运行旧 script generation？
- 哪些 runtime cell 使用 placement plan generation X？
- 哪些请求命中了 gray generation？
- 哪些 consumer 正在 LKG serving？
- 哪些 consumer drift 超过阈值？

这应成为 `fugue admin robustness status`、`traffic-safety explain`、`release guard status` 和 Web console 的基础数据。

#### 5.6.10 Permission, audit, and governance

Apollo 区分编辑权限和发布权限。Fugue 应建立更强的状态治理权限：

- `artifact.create_draft`
- `artifact.validate`
- `artifact.release_shadow`
- `artifact.release_gray`
- `artifact.release_full`
- `artifact.rollback`
- `artifact.force_publish`
- `traffic_safety.override`
- `node_quarantine.override`
- `repair.execute`

规则：

- 修改 desired state 不等于发布 active state。
- 影响流量的 full release 必须经过 Release Guardian。
- 自动修复动作也必须有 actor，actor 可以是 system guardian。
- `force_publish` 必须写高风险 audit event，并要求 reason。
- secret 不得进入 artifact diff、audit、request explain。
- audit 必须记录 expected/observed/validation/probe/canary result。

#### 5.6.11 Minimal dependency principle

Apollo 为可用性和部署简单性尽量减少外部依赖。Fugue 应坚持同样原则：

- 不引入 Apollo 作为 Fugue 自身配置中心。
- 不为 release notification 引入强依赖 MQ。
- 优先使用现有 control plane DB 保存 artifact、release message、consumer generation、incident。
- 数据面通过 local LKG 降低对 control plane 实时可用性的依赖。
- 新组件必须能在 control plane degraded 时 fail closed 或 serve LKG。

#### 5.6.12 What Fugue must add beyond Apollo

Apollo 解决的是配置发布和分发，不解决基础设施实际状态漂移。Fugue 必须在 Apollo 模型之上增加：

- node actual-state deep health。
- iptables/CNI/systemd/Caddy/edge env 漂移审计。
- edge-to-origin 网络健康。
- client-to-edge 网络质量。
- traffic safety hard gate。
- runtime continuity。
- request-level attribution。
- chaos drill。
- 自动 quarantine 和可逆 repair。

因此最终架构是：

```text
Platform State Release System
  -> safely publishes desired state

Node Guardian / Traffic Safety / Release Guardian / Runtime Continuity
  -> continuously prove actual state and user traffic are safe

LKG / rollback / gray / request attribution
  -> limit blast radius and explain failure
```

### 5.7 Subsystem Failure Contracts

每个核心子系统都必须定义故障契约。该契约是 guardian、release guard、traffic safety、request attribution 和 operator runbook 的共同输入。

#### 5.7.1 Control Plane API

可能故障：

- API 不可用。
- API 可用但 DB 慢或连接失败。
- API 返回旧 generation。
- API 返回错误 desired state。
- Admin API 被误用直接改变生产行为。

措施：

- API 多副本。
- Read API 与 Admin API 分离。
- readiness 同时检查 DB、artifact store、release read path。
- 数据面 consumer 无法拉取 control plane 时继续服务 validated LKG。
- Release Guardian 在 API 不健康时 block rollout。
- consumer generation drift 可见并告警。
- request attribution 区分 control-plane read failure 和数据面 failure。

#### 5.7.2 Controller

可能故障：

- 生成错误 route/DNS/edge artifact。
- reconcile 卡住。
- 重复执行危险动作。
- rollout 中途挂掉。
- controller 重启后 operation phase 丢失。

措施：

- 所有生成物先进入 draft artifact。
- schema validation + invariant validation + compatibility validation。
- artifact diff 和 shadow generation。
- operation phase 持久化、幂等、可恢复。
- release guard 阻止坏 artifact 发布。
- dangerous action 需要 human approval boundary。
- 所有 controller action 写 audit event。

#### 5.7.3 Platform State Release System

可能故障：

- 发布错误 artifact。
- release message 丢失。
- consumer 没收到 long-poll 通知。
- rollback 目标错误。
- LKG 过期或损坏。
- consumer apply 到一半失败。

措施：

- durable release message。
- long-poll + periodic pull 双机制。
- consumer actual generation / desired generation / LKG generation 上报。
- content hash 校验。
- local apply atomic。
- LKG atomic write 和 hash verify。
- LKG `expires_at`。
- rollback 通过新 release record 完成。
- consumer drift 告警。
- LKG corruption drill。

#### 5.7.4 Node Guardian / Node Updater

可能故障：

- 没检测到节点漂移。
- 清理规则不完整。
- 修复动作误删非 Fugue 规则。
- 自身卡住。
- 自身版本漂移。
- 探针误报。

措施：

- observe-only 先上线。
- Fugue managed rule provenance。
- repair dry-run。
- repair 限流、幂等、带锁。
- 不确定时 quarantine，不强修。
- 修复后重新跑 deep health。
- guardian heartbeat。
- node updater generation drift 检测。
- emergency disable switch。

#### 5.7.5 Kubernetes / CNI / kube-dns / CoreDNS

可能故障：

- pod DNS 超时。
- CNI bridge 异常。
- PodCIDR 漂移。
- kube-dns Service VIP 不通。
- CoreDNS pod 可用但 Service iptables 错。
- kubelet ready 但 pod 网络坏。

措施：

- pod 内 DNS 到 kube-dns Service IP 探针。
- pod 内 DNS 到 CoreDNS pod IP 对照探针。
- `kubernetes.default.svc` 探针。
- same namespace service DNS + TCP/HTTP 探针。
- PodCIDR actual vs Kubernetes Node spec 检查。
- 节点 DNS/CNI 失败时 quarantine。
- 关键 stateless workload replacement plan。
- request attribution 标记 `node_dns_failure` / `node_cni_failure`。

#### 5.7.6 Edge / Edge Front / Edge Worker

可能故障：

- client-to-edge 网络慢或丢包。
- edge-to-origin DNS/connect/TLS/TTFB 慢或失败。
- request body read 慢。
- response egress 慢。
- route bundle 过期。
- edge 仍服务旧 generation。
- Caddy/edge worker reload 失败。

措施：

- hard gate 与 score 分离。
- edge route bundle generation 上报。
- edge health 按 service/client scope 聚合。
- body read effective bps、min-window bps、max gap。
- TCP RTT/retrans/RTO/lost/delivery rate。
- origin DNS/connect/request write/response wait/TTFB 分开归因。
- edge quarantine。
- DNS answer 摘除坏 edge。
- LKG route bundle。
- canary / exploration budget。

#### 5.7.7 DNS Server / DNS Answer Policy

可能故障：

- 返回被 service exclusion 禁用的 edge。
- 返回不 route-ready 的 edge。
- 返回 TLS 未 ready 的 edge。
- 新 edge exploration 过大。
- DNS bundle 生成错。
- DNS 节点 stale generation。
- DNS server LKG 过期。

措施：

- DNS answer hard gate。
- Traffic Safety Controller 维护 eligible edge set。
- service minimum healthy edge count。
- DNS answer bundle artifact validation。
- DNS server active generation 上报。
- LKG DNS answer bundle。
- shadow answer comparison。
- gray DNS answer。
- `service.healthy_edge_count_zero` block release。

#### 5.7.8 Caddy / Route Bundle

可能故障：

- route 指向错误 origin。
- TLS cert 未 ready。
- Caddy config reload 失败。
- route generation 不一致。
- LKG 过期。
- old config 继续服务但 control plane 不可见。

措施：

- route bundle artifact。
- Caddy config artifact。
- Caddy config syntax validation。
- route-to-origin probe。
- TLS readiness gate。
- atomic reload。
- failed reload 保留旧 config。
- Caddy active generation 上报。
- route/DNS/TLS 一体 invariant。

#### 5.7.9 Runtime Scheduler / App Runtime

可能故障：

- pod 调度到坏节点。
- app ready replicas 掉到 0。
- stateless app 没迁移。
- stateful app 被错误迁移。
- runtime node 网络不通。
- service endpoint 存在但 HTTP 不可用。

措施：

- node quarantine 进入 scheduler hard gate。
- app continuity invariant。
- stateless app 自动 replacement plan。
- stateful app 只做 lease/fence/backup preflight。
- route shift only after readiness。
- service DNS/TCP/HTTP probe。
- min ready replicas gate。
- DB TCP/DNS failure 单独归因。

#### 5.7.10 Database / Stateful Services

可能故障：

- DB DNS 不可解析。
- DB TCP 不可达。
- DB 慢。
- DB schema migration 失败。
- backup 不新鲜。
- restore 不可用。
- stateful failover fencing 不明确。

措施：

- DB DNS/TCP/readiness probe。
- DB readiness 单独归因。
- backup freshness invariant。
- restore drill。
- migration release guard。
- stateful failover human approval boundary。
- lease/fence evidence before execution。
- 不把 DB error 混入 edge/upstream error。

#### 5.7.11 Observability / Metrics

可能故障：

- 指标缺失。
- 指标延迟。
- 指标误分类。
- 只有全局平均，掩盖局部故障。
- log/metric/request id 无法关联。

措施：

- metrics freshness check。
- 指标缺失本身是 degraded signal。
- request-level attribution 作为日志证据。
- scope-aware aggregation。
- fallback level 明确记录。
- 关键 gate 不只依赖单一指标源。
- request id / trace id 贯穿 edge、origin、runtime。

#### 5.7.12 Automatic Repair System

可能故障：

- 修错对象。
- 修复循环。
- 多个 repair 冲突。
- 修复扩大事故。
- repair 自身卡住。

措施：

- safety class。
- dry-run。
- rate limit。
- idempotency。
- lock / lease。
- blast-radius cap。
- repair audit。
- repair after-probe。
- repeated failure 后停止并要求人工介入。
- emergency disable switch。

## 6. 事故类型到系统能力映射

### 6.1 DNS escape-hatch 残留规则

事故模式：

- actual iptables 与 desired state 不一致。
- stale DNAT target 指向不存在 PodCIDR。
- pod DNS 失败导致多个上层服务同时异常。

目标能力：

- Node Guardian 检测 stale DNAT。
- Node Guardian 检测 pod DNS 失败。
- control plane 自动把节点标记 degraded。
- runtime scheduler 停止把新 app 调度到该节点。
- edge/DNS ranking 停止把该节点上的 edge 作为 healthy candidate。
- Runtime Continuity Controller 迁走 stateless critical app。
- Request Attribution 将 503 归因到 node DNS failure。

### 6.2 edge 机制更新导致不可预期调度

事故模式：

- 新 ranking / exclusion / dynamic edge 机制影响真实 DNS answer。
- 发布前缺少 shadow 与 canary 比较。
- 服务级兜底不足。

目标能力：

- Release Guardian 强制 shadow -> canary -> enforced。
- Traffic Safety Controller 在发布前模拟每个服务的 eligible edge set。
- 新机制不允许让任何服务 healthy edge count 变成 0。
- canary 只给小比例真实 DNS answer。
- 指标恶化自动停止扩大流量并回退 LKG。

### 6.3 控制平面更新关联业务异常

事故模式：

- 控制面发布改变数据面配置。
- 发布链路没有强制验证数据面结果。

目标能力：

- 发布前 baseline。
- canary control plane 副本。
- post-deploy synthetic probe。
- route/DNS/TLS artifact diff validation。
- block-rollout incident 阻止继续发布。
- 部署失败自动保留 LKG。

## 7. 指标和事件模型

### 7.1 新增核心指标

节点：

- `fugue_node_deep_health_pass`
- `fugue_node_deep_health_check_duration_seconds`
- `fugue_node_pod_dns_lookup_success`
- `fugue_node_service_dns_lookup_success`
- `fugue_node_external_dns_lookup_success`
- `fugue_node_managed_iptables_stale_rule_count`
- `fugue_node_podcidr_mismatch`
- `fugue_node_quarantine_active`
- `fugue_node_generation_drift_seconds`

流量：

- `fugue_service_eligible_edge_count`
- `fugue_service_healthy_edge_count`
- `fugue_service_edge_exclusion_active`
- `fugue_edge_exploration_traffic_ratio`
- `fugue_edge_quarantine_active`
- `fugue_dns_answer_rejected_total`
- `fugue_dns_answer_lkg_serving`

请求归因：

- `fugue_request_error_class_total`
- `fugue_request_origin_dns_error_total`
- `fugue_request_origin_connect_error_total`
- `fugue_request_body_read_slow_total`
- `fugue_request_body_read_error_total`
- `fugue_request_upstream_unavailable_total`
- `fugue_request_auth_error_total`
- `fugue_request_quota_error_total`

发布：

- `fugue_release_guard_block_total`
- `fugue_release_canary_pass`
- `fugue_release_rollback_total`
- `fugue_release_post_probe_success`
- `fugue_release_artifact_validation_failure_total`

平台状态发布：

- `fugue_platform_artifact_generation`
- `fugue_platform_artifact_validation_pass`
- `fugue_platform_artifact_release_total`
- `fugue_platform_artifact_gray_release_active`
- `fugue_platform_artifact_rollback_total`
- `fugue_platform_release_message_lag_seconds`
- `fugue_platform_consumer_generation_drift_seconds`
- `fugue_platform_consumer_lkg_serving`
- `fugue_platform_consumer_apply_failure_total`
- `fugue_platform_lkg_expired_total`

### 7.2 事件类型

- `node.deep_health_failed`
- `node.quarantined`
- `node.repair_attempted`
- `node.repair_succeeded`
- `node.repair_refused`
- `edge.quarantined`
- `edge.exploration_paused`
- `route.artifact_rejected`
- `dns.answer_rejected`
- `release.blocked`
- `release.rollback_started`
- `release.rollback_completed`
- `request.error_attributed`
- `platform_artifact.draft_created`
- `platform_artifact.validation_failed`
- `platform_artifact.gray_released`
- `platform_artifact.full_released`
- `platform_artifact.rollback_requested`
- `platform_artifact.rollback_completed`
- `platform_consumer.generation_drift_detected`
- `platform_consumer.lkg_serving_started`
- `platform_consumer.lkg_expired`

每个事件必须包含：

- scope
- expected state
- observed state
- evidence refs
- action taken
- action safety class
- next check time
- operator override hints

## 8. CLI / API 产品面

### 8.1 CLI

新增或收敛以下命令：

```text
fugue admin robustness status
fugue admin robustness incidents ls
fugue admin robustness incidents show <incident-id>
fugue admin robustness check node <node-name>
fugue admin robustness check service <hostname-or-app>
fugue admin robustness check edge <edge-id>
fugue admin robustness repair-plan <incident-id>
fugue admin robustness repair <incident-id> --dry-run
fugue admin release guard status
fugue admin traffic-safety explain <hostname>
fugue admin request explain <request-id>
fugue admin artifact ls --kind <artifact-kind> --scope <scope>
fugue admin artifact show <artifact-id-or-generation>
fugue admin artifact diff <generation-a> <generation-b>
fugue admin artifact validate <artifact-id> --dry-run
fugue admin artifact release <artifact-id> --channel shadow|gray|full
fugue admin artifact rollback <artifact-kind> --scope <scope> --to-generation <generation>
fugue admin artifact consumers <artifact-kind> --scope <scope>
fugue admin artifact lkg <artifact-kind> --scope <scope>
```

### 8.2 API

OpenAPI-first 新增/收敛：

- `GET /v1/admin/robustness/status`
- `GET /v1/admin/robustness/incidents`
- `GET /v1/admin/robustness/incidents/{id}`
- `POST /v1/admin/robustness/checks/node`
- `POST /v1/admin/robustness/checks/service`
- `POST /v1/admin/robustness/checks/edge`
- `POST /v1/admin/robustness/incidents/{id}/repair-plan`
- `POST /v1/admin/robustness/incidents/{id}/repair`
- `GET /v1/admin/release-guard/status`
- `GET /v1/admin/traffic-safety/explain`
- `GET /v1/admin/requests/{request_id}/explain`
- `GET /v1/admin/artifacts`
- `POST /v1/admin/artifacts`
- `GET /v1/admin/artifacts/{artifact_id}`
- `POST /v1/admin/artifacts/{artifact_id}/validate`
- `POST /v1/admin/artifacts/{artifact_id}/release`
- `POST /v1/admin/artifacts/{artifact_id}/rollback`
- `GET /v1/admin/artifacts/{artifact_id}/consumers`
- `GET /v1/admin/artifacts/{artifact_id}/lkg`
- `GET /v1/platform-state/notifications`
- `GET /v1/platform-state/artifacts/{artifact_kind}`
- `POST /v1/platform-state/consumers/heartbeat`
- `POST /v1/platform-state/consumers/apply-result`

所有 API 必须从 `openapi/openapi.yaml` 开始改，生成后端产物，再同步 `fugue-web`。

### 8.3 Consumer protocol

数据面 consumer 协议应独立于 admin API，保持小而稳定：

```text
consumer boots
  -> load local LKG
  -> register heartbeat with consumer type/id and supported schema versions
  -> long-poll subscribed artifact scopes
  -> pull active generation
  -> verify content hash and compatibility
  -> stage locally
  -> apply atomically
  -> run local probe
  -> report apply result and actual generation
  -> periodically reconcile even without notification
```

consumer 必须上报：

- `consumer_type`
- `consumer_id`
- `node_name`
- `supported_artifact_kinds`
- `supported_schema_versions`
- `actual_generation`
- `lkg_generation`
- `apply_status`
- `local_probe_status`
- `last_error_class`
- `last_error_detail_safe`

## 9. 发布策略

该改造必须分阶段上线，不能一次性开启自动修复。

### Phase 0: document and inventory

只做文档、inventory、只读检查和指标补齐。

输出：

- 不变量清单。
- Apollo-inspired platform state release system 数据模型。
- 节点实际状态 inventory。
- 当前 route/DNS/edge/node updater/runtime placement artifact inventory。
- 现有 updater / guardian / robustness 实现差距。
- 现有 CLI/API 差距。
- 当前数据面 consumer 和 generation 可见性差距。

### Phase 1: observe-only

所有新 guardian 只观察，不执行修复、不隔离、不影响流量。

输出：

- platform artifact 只读 registry。
- consumer generation heartbeat observe-only。
- node deep health 只读结果。
- traffic safety explain。
- request attribution explain。
- release guard dry-run 报告。
- LKG inventory 和过期风险报告。

### Phase 2: safe quarantine

允许自动执行低风险隔离：

- edge 从 DNS answer 暂停。
- 节点标记 degraded。
- service exploration 暂停。
- release 被 block。
- artifact release 在 validation 失败时被拒绝。
- consumer generation drift 超阈值时触发 degraded event。

不执行破坏性修复。

### Phase 3: reversible repair

允许自动执行可逆修复：

- 删除 Fugue managed stale iptables 规则。
- 重新拉取 desired state。
- 重载 LKG bundle。
- 重启无状态守护进程。
- 迁移 stateless workload。
- rollback 到上一代 validated platform artifact。
- 重新投递 release message。

### Phase 4: closed-loop recovery

系统形成完整闭环：

- 自动检测。
- 自动隔离。
- 自动修复。
- 自动验证。
- 自动恢复流量。
- 自动归档 incident。
- 自动执行 shadow -> gray -> full release。
- 自动根据 canary 结果 full release 或 rollback。

### Phase 5: chaos and autonomy drills

持续演练：

- kube-dns timeout。
- stale iptables DNAT。
- edge origin connect timeout。
- edge slow body read。
- route bundle bad generation。
- service edge exclusion leaves zero edge。
- control plane rollout bad image。
- node loss。
- CoreDNS pod loss。
- platform release message loss。
- consumer misses long-poll notification but catches up by periodic pull。
- LKG cache corruption。
- gray release abort and rollback。

## 10. 可以打勾的 todo list

### A. 文档和 inventory

- [x] 梳理当前 `docs/self-healing-robustness-plan.md` 已完成项与真实代码能力是否一致。
- [x] 建立核心不变量 registry，按 node/runtime/edge/DNS/release/request 分类。
- [x] 建立当前节点 actual-state inventory：PodCIDR、CNI、iptables、systemd、Caddy、edge env、node labels、taints。
- [x] 建立当前 service route inventory：hostname、app、origin service、eligible edge、excluded edge、route generation。
- [x] 建立当前 request attribution inventory：哪些字段已采集，哪些字段缺失。
- [x] 建立当前 release pipeline guard inventory。
- [x] 输出 gap report：已有能力、缺失能力、风险等级、实现入口。

### B. Node Guardian observe-only

- [x] 定义 `NodeDeepHealthCheck` 数据模型。
- [x] 定义 `NodeDeepHealthResult` 数据模型。
- [x] 增加 pod DNS 到 kube-dns Service IP 的探针。
- [x] 增加 pod DNS 到 CoreDNS pod IP 的对照探针。
- [x] 增加 `kubernetes.default.svc` 解析探针。
- [x] 增加 same-namespace service DNS 探针。
- [x] 增加 same-namespace service HTTP/TCP 探针。
- [x] 增加 external DNS 探针。
- [x] 增加 iptables managed stale rule audit。
- [x] 增加 PodCIDR actual vs Kubernetes Node spec 对比。
- [x] 增加 conntrack saturation 检查。
- [x] 增加 node updater generation drift 检查。
- [x] 所有检查先以 observe-only 上报，不自动修复。
- [x] 为 node deep health 增加单元测试。
- [x] 为 stale iptables 检测增加事故形态回归测试。

### C. Node Guardian quarantine

- [x] 定义 node quarantine 状态和原因枚举。
- [x] 定义 quarantine TTL 和自动恢复条件。
- [x] 控制平面接收节点 deep health 后计算 node degraded/quarantined。
- [x] 节点 DNS 硬失败时自动标记 degraded。
- [x] 节点 DNS 硬失败时给 runtime scheduler 提供不可调度信号。
- [x] edge 节点 DNS/CNI/iptables 硬失败时从 eligible edge set 移除。
- [x] quarantine 动作写 audit event。
- [x] CLI 展示 node quarantine 原因、证据和恢复条件。
- [x] 增加 quarantine dry-run。
- [x] 增加自动恢复前探针复核。

### D. Node Guardian reversible repair

- [x] 定义 repair action safety class。
- [x] 将 Fugue managed iptables 规则带上可识别 provenance。
- [x] 实现 stale managed DNAT 删除的 dry-run。
- [x] 实现 stale managed DNAT 删除的执行路径。
- [x] 实现 node desired state 重新拉取。
- [x] 实现 LKG edge/Caddy bundle reload。
- [x] 实现无状态 node-side service 安全重启。
- [x] 修复动作必须限流。
- [x] 修复动作必须幂等。
- [x] 修复动作必须写 audit event。
- [x] 修复后必须重新跑 deep health。
- [x] 修复失败时保持 quarantine，不恢复流量。

### E. Traffic Safety Controller

- [x] 定义 `ServiceTrafficSafetyState`。
- [x] 定义 service minimum healthy edge count。
- [x] 定义 eligible edge hard gates。
- [x] 将 service-level edge exclusion 纳入 hard gate。
- [x] 将 edge maintenance/drain 纳入 hard gate。
- [x] 将 TLS readiness 纳入 hard gate。
- [x] 将 route generation readiness 纳入 hard gate。
- [x] 将 node quarantine 纳入 hard gate。
- [x] 实现 `traffic-safety explain <hostname>`。
- [x] 发布前模拟每个 hostname 的 eligible edge set。
- [x] 禁止任何发布让服务 healthy edge count 变为 0。
- [x] 新 edge exploration 必须经过 traffic safety gate。
- [x] exploration 失败自动暂停。
- [x] 增加 service 只剩一个 edge 时的严格保护。

### F. Edge scoped health and ranking

- [x] 将 edge health 和 ranking 明确拆成 gate 层与 score 层。
- [x] 按 service/hostname 聚合 edge health。
- [x] 按 client country/region/ASN 聚合 edge health。
- [x] 按 traffic class 聚合 edge health。
- [x] 按 request size class 聚合 body read quality。
- [x] 将 TCP retrans/RTO/lost/delivery rate 纳入 scoped signal。
- [x] 将 origin DNS/connect/TTFB 与 client-to-edge body read 分开归因。
- [x] 将 body read slow/failed 纳入 score，但不能误判为业务错误。
- [x] 对低样本 scope 显式 fallback 并记录 fallback level。
- [x] CLI/API 展示 score breakdown。
- [x] DNS answer 记录使用的 ranking version 和 scope。
- [x] 增加 edge scoped ranking 的 shadow comparison 报告。

### G. Runtime Continuity

- [x] 定义 app continuity invariant。
- [x] 为 stateless app 定义自动迁移策略。
- [x] 为 stateful app 定义只读 preflight 策略。
- [x] 检测 node quarantine 导致的 app ready replicas 下降。
- [x] 检测 service DNS/HTTP probe 失败导致的 route 不可用。
- [x] stateless app 在坏节点上时自动创建 replacement plan。
- [x] replacement pod ready 后才切 route。
- [x] stateful app 必须检查 lease/fence/backup/restore 状态。
- [x] DB TCP/DNS failure 必须单独归因，不混入 app HTTP failure。
- [x] CLI 展示 app continuity 状态。

### H. Release Guardian

- [x] 发布前读取 robustness baseline。
- [x] 发布前如果存在 block-rollout incident，普通发布失败。
- [x] 控制平面发布先 canary 一个副本。
- [x] canary 通过 API readiness、DB readiness、route generation readiness 后才滚剩余副本。
- [x] edge/DNS/route 相关变更必须生成 shadow artifact。
- [x] shadow artifact 必须通过 invariant validation。
- [x] 发布后运行 platform synthetic probe。
- [x] 发布后运行 route/DNS/TLS synthetic probe。
- [x] 发布后比较 5xx、DNS answer rejection、edge origin connect、body read slow。
- [x] 指标恶化超过阈值自动停止 rollout。
- [x] 可回滚 artifact 自动 rollback。
- [x] rollback 失败时创建 high-severity incident。
- [x] GitHub Actions 输出 release guard summary。

### I. Request Attribution

- [x] 定义 request error class taxonomy。
- [x] 统一 edge、edge-front、origin proxy、runtime app 的 request id 传播。
- [x] 记录 DNS answer 的 edge id / edge group / route generation。
- [x] 记录 edge node 与 runtime node。
- [x] 记录 body read duration、effective bps、min-window bps、max read gap。
- [x] 记录 origin DNS/connect/request write/response wait/TTFB/total。
- [x] 记录 TCP info 网络损伤字段。
- [x] 区分 auth error、quota error、business 4xx、origin 5xx、edge 5xx、upstream unavailable。
- [x] `503 upstream unavailable` 必须细分为 DNS、connect、TLS、timeout、origin unavailable。
- [x] 实现 `fugue admin request explain <request-id>`。
- [x] 实现 `GET /v1/admin/requests/{request_id}/explain`。
- [x] request explain 输出不得泄露 secret。

### J. Observability and alerting

- [x] 增加 node deep health dashboard。
- [x] 增加 traffic safety dashboard。
- [x] 增加 release guard dashboard。
- [x] 增加 request attribution dashboard。
- [x] 增加 `node.pod_dns_failed` 告警。
- [x] 增加 `service.healthy_edge_count_zero` 告警。
- [x] 增加 `edge.origin_dns_error_spike` 告警。
- [x] 增加 `edge.body_read_slow_spike` 告警。
- [x] 增加 `release.guard_blocked` 告警。
- [x] 增加 `request.upstream_unavailable_spike` 告警。
- [x] 告警必须携带 incident id 和 explain 命令。

### K. CLI / API

- [x] 在 OpenAPI 中定义 robustness check schema。
- [x] 在 OpenAPI 中定义 traffic safety explain schema。
- [x] 在 OpenAPI 中定义 request explain schema。
- [x] 实现 `fugue admin robustness check node <node-name>`。
- [x] 实现 `fugue admin robustness check service <hostname-or-app>`。
- [x] 实现 `fugue admin robustness check edge <edge-id>`。
- [x] 实现 `fugue admin traffic-safety explain <hostname>`。
- [x] 实现 `fugue admin release guard status`。
- [x] 实现 `fugue admin request explain <request-id>`。
- [x] CLI 默认给出可操作摘要。
- [x] CLI `--json` 输出完整 evidence。
- [x] CLI 输出 secret-safe。

### L. Chaos and failure drills

- [x] 增加 stale iptables DNAT 本地集成测试。
- [x] 增加 pod DNS timeout drill。
- [x] 增加 CoreDNS pod loss drill。
- [x] 增加 service DNS failure drill。
- [x] 增加 edge origin connect timeout drill。
- [x] 增加 edge slow body read drill。
- [x] 增加 bad route bundle drill。
- [x] 增加 service edge exclusion leaves zero edge drill。
- [x] 增加 control plane bad image rollout drill。
- [x] 增加 node loss drill。
- [x] 每个 drill 必须验证 detection、quarantine、repair/rollback、explain。
- [x] drill 结果进入 release readiness。

### M. Documentation and runbooks

- [x] 编写 node DNS failure runbook。
- [x] 编写 stale iptables managed rule runbook。
- [x] 编写 edge quarantine runbook。
- [x] 编写 traffic safety zero eligible edge runbook。
- [x] 编写 release guard blocked runbook。
- [x] 编写 request attribution runbook。
- [x] 编写 stateless runtime migration runbook。
- [x] 编写 stateful app preflight runbook。
- [x] 将关键 runbook 链接到 `docs/ha-dr.md`。
- [x] 为每个自动修复动作写安全边界说明。

### N. Platform State Release System

- [x] 定义 `PlatformArtifactKind` 枚举。
- [x] 定义 `PlatformArtifactScope` 数据模型。
- [x] 定义 `PlatformArtifact` 数据模型。
- [x] 定义 `PlatformArtifactRelease` 数据模型。
- [x] 定义 `PlatformReleaseMessage` 数据模型。
- [x] 定义 `PlatformConsumerInstance` 数据模型。
- [x] 定义 `PlatformLKGSnapshot` 数据模型。
- [x] 建立 `platform_artifacts` 存储。
- [x] 建立 `platform_artifact_releases` 存储。
- [x] 建立 `platform_release_messages` 存储。
- [x] 建立 `platform_consumer_instances` 存储。
- [x] 建立 `platform_lkg_snapshots` 存储。
- [x] 实现 artifact draft 创建 API。
- [x] 实现 artifact schema validation。
- [x] 实现 artifact invariant validation。
- [x] 实现 artifact compatibility validation。
- [x] 实现 artifact content hash 和 content-addressed storage。
- [x] 实现 artifact diff。
- [x] 实现 shadow release。
- [x] 实现 gray release。
- [x] 实现 full release。
- [x] 实现 rollback release。
- [x] rollback 必须生成新的 release record，而不是直接覆盖 active state。
- [x] release 后写 durable release message。
- [x] 实现 release message 扫描和 long-poll 唤醒。
- [x] 实现 consumer heartbeat。
- [x] 实现 consumer actual generation 上报。
- [x] 实现 consumer apply result 上报。
- [x] 实现 consumer local probe result 上报。
- [x] 实现 consumer generation drift 检测。
- [x] 实现 consumer LKG serving 上报。
- [x] 实现 consumer LKG expired 上报。
- [x] 实现 node updater 对 platform state notification 的订阅。
- [x] 实现 edge worker 对 edge route bundle generation 的订阅。
- [x] 实现 DNS server 对 DNS answer bundle generation 的订阅。
- [x] 实现 Caddy / edge-front route config generation 的订阅。
- [x] 实现 runtime agent 对 node desired state / placement plan 的订阅。
- [x] 数据面 consumer 必须支持 long-poll。
- [x] 数据面 consumer 必须支持 periodic pull 兜底。
- [x] 数据面 consumer 必须先 load local LKG 再尝试拉 control plane。
- [x] 数据面 consumer apply 必须 atomic。
- [x] 数据面 consumer apply 后必须本地 probe。
- [x] LKG 写入必须 atomic rename。
- [x] LKG 读取必须校验 content hash。
- [x] LKG 必须有 expires_at。
- [x] LKG 过期必须进入 degraded，不得静默服务。
- [x] 增加 artifact release 权限模型。
- [x] 区分 create draft、validate、gray release、full release、rollback、force publish 权限。
- [x] 所有 release、rollback、force publish 写 audit event。
- [x] `force_publish` 必须要求 reason。
- [x] audit 和 diff 输出必须 secret-safe。
- [x] 实现 `fugue admin artifact ls`。
- [x] 实现 `fugue admin artifact show`。
- [x] 实现 `fugue admin artifact diff`。
- [x] 实现 `fugue admin artifact validate`。
- [x] 实现 `fugue admin artifact release`。
- [x] 实现 `fugue admin artifact rollback`。
- [x] 实现 `fugue admin artifact consumers`。
- [x] 实现 `fugue admin artifact lkg`。
- [x] 在 release guard 中接入 platform artifact validation。
- [x] 在 traffic safety 中接入 artifact gray release 作用域。
- [x] 在 request attribution 中记录 artifact generation。
- [x] 在 robustness status 中展示 consumer generation drift。
- [x] 为 release message 丢失增加 periodic pull 回归测试。
- [x] 为 gray release abort 增加回归测试。
- [x] 为 rollback release 增加回归测试。
- [x] 为 LKG cache corruption 增加回归测试。
- [x] 为 consumer generation drift 增加回归测试。
- [x] 编写 platform artifact release runbook。
- [x] 编写 platform artifact rollback runbook。
- [x] 编写 consumer generation drift runbook。
- [x] 编写 LKG expired runbook。

### O. Subsystem Failure Contracts

- [x] 定义 `SubsystemFailureContract` 数据模型。
- [x] 定义 `FailureMode` 数据模型。
- [x] 定义 `DetectionSignal` 数据模型。
- [x] 定义 `IsolationAction` 数据模型。
- [x] 定义 `FallbackBehavior` 数据模型。
- [x] 定义 `RepairAction` 数据模型。
- [x] 定义 `RollbackPath` 数据模型。
- [x] 定义 `HumanApprovalBoundary` 数据模型。
- [x] 为 Control Plane API 编写 failure contract。
- [x] 为 Controller 编写 failure contract。
- [x] 为 Platform State Release System 编写 failure contract。
- [x] 为 Node Guardian 编写 failure contract。
- [x] 为 Node Updater 编写 failure contract。
- [x] 为 Kubernetes/CNI/kube-dns/CoreDNS 编写 failure contract。
- [x] 为 Edge Front 编写 failure contract。
- [x] 为 Edge Worker 编写 failure contract。
- [x] 为 DNS Server 编写 failure contract。
- [x] 为 DNS Answer Policy 编写 failure contract。
- [x] 为 Caddy / Route Bundle 编写 failure contract。
- [x] 为 Runtime Scheduler 编写 failure contract。
- [x] 为 App Runtime 编写 failure contract。
- [x] 为 Database / Stateful Services 编写 failure contract。
- [x] 为 Observability / Metrics 编写 failure contract。
- [x] 为 Automatic Repair System 编写 failure contract。
- [x] 每个 failure contract 必须列出 possible failure modes。
- [x] 每个 failure contract 必须列出 detection signals。
- [x] 每个 failure contract 必须列出 quarantine / isolation actions。
- [x] 每个 failure contract 必须列出 fallback / LKG behavior。
- [x] 每个 failure contract 必须列出 reversible repair actions。
- [x] 每个 failure contract 必须列出 rollback path。
- [x] 每个 failure contract 必须列出 request / incident attribution class。
- [x] 每个 failure contract 必须列出 human approval boundary。
- [x] 每个 failure contract 必须标记哪些动作允许 observe-only。
- [x] 每个 failure contract 必须标记哪些动作允许 automatic quarantine。
- [x] 每个 failure contract 必须标记哪些动作允许 automatic repair。
- [x] 每个 failure contract 必须标记哪些动作必须人工确认。
- [x] 将 failure contract 接入 robustness status。
- [x] 将 failure contract 接入 release guard。
- [x] 将 failure contract 接入 traffic safety。
- [x] 将 failure contract 接入 request attribution。
- [x] 将 failure contract 接入 runbook 生成。
- [x] 为 guardian 误报增加回归测试。
- [x] 为 guardian 漏报增加 chaos drill。
- [x] 为 repair 修错对象增加防护测试。
- [x] 为 repair 循环增加限流测试。
- [x] 为多个 repair 冲突增加 lock/lease 测试。
- [x] 为 metrics 缺失增加 degraded signal 测试。
- [x] 为 LKG 过期增加 fail-closed 测试。
- [x] 为 control plane read failure 增加 LKG serving 测试。
- [x] 为 DNS hard gate 误放行增加 invariant 测试。
- [x] 为 runtime 调度到 quarantined node 增加 hard gate 测试。
- [x] 为 stateful failover 无 fence evidence 增加拒绝执行测试。
- [x] 编写 subsystem failure contract runbook。
- [x] 编写 automatic repair safety runbook。
- [x] 编写 emergency disable switch runbook。

## 11. Definition of Done

该计划完成的标准不是“有了更多监控”，而是 Fugue 能在测试、staging drill 和生产 observe-only 中证明以下行为：

- 节点 pod DNS 失败能在用户报告前被发现。
- stale Fugue managed iptables 规则能被自动识别、隔离影响并安全清理。
- 节点硬故障不会继续承接新 workload 或 edge 流量。
- edge/DNS/route 发布不会让任何服务进入 zero eligible edge 状态。
- 新 edge 只能通过受控 exploration 获得真实流量。
- edge quality ranking 能解释作用域、fallback、score breakdown 和 hard gate。
- 控制平面发布能被 release guard 阻止、回滚和解释。
- 关键平台状态都通过 versioned artifact release 发布，而不是直接写入数据面。
- 数据面组件能在 control plane 短时不可用时继续服务 validated LKG。
- 数据面 consumer 的 actual generation、LKG generation 和 drift 可见。
- gray release 能按 edge/node/service/client scope 限制真实影响。
- rollback 通过发布旧 generation 完成，并且可审计。
- 每个生产关键子组件都有 failure contract。
- 每个 failure contract 都覆盖检测、隔离、降级、修复、回滚、归因和人工边界。
- guardian 和 repair 系统自身也被纳入 failure contract，而不是被默认信任。
- metrics 缺失、LKG 过期、consumer drift 本身都能触发 degraded，而不是被忽略。
- `503 upstream unavailable` 能自动归因到 DNS、connect、TLS、timeout、origin 或业务上游类别。
- 操作者可以用一条 CLI 命令看到 incident 的证据、动作和下一步。
- 自动修复动作均可审计、幂等、限流，并有明确停止边界。

最终目标：

```text
No single node drift, edge rollout, DNS artifact, route generation, or
control-plane deployment should silently degrade production traffic without
Fugue detecting it, limiting blast radius, and explaining the exact cause.
```
