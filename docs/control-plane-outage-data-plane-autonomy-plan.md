# Control-plane outage and data-plane autonomy resilience plan

本文档固定 Fugue 在“控制面断开、但 edge / DNS / agent 节点仍存活”场景下的系统性自组织、自恢复和鲁棒性升级方案。

本文是 `docs/self-organization-recovery-resilience-upgrade-plan.md` 的专项补充。它聚焦本次事故暴露的结构性问题：控制面被 hypervisor powerdown 后，数据面组件虽然还有本地进程和 LKG 能力，但自治、自检、临时隔离、快速恢复和事后归因能力不足。

本文不讨论所有 edge / DNS 入口全部消失时如何继续对外服务。入口全灭时已经没有可被用户访问的数据面路径，Fugue 只能依赖外部 provider / DNS / 监控系统恢复入口。本计划覆盖的是其他仍有巨大改进空间的场景：

- 控制面全部不可用，但一个或多个 edge / DNS 仍在运行。
- 单个 edge、DNS、agent、runtime、node guardian 或 K3s agent 出现软故障。
- 控制面与数据面之间的 K3s tunnel / API / service DNS 断开。
- control plane VM 被 provider / hypervisor powerdown，但部分数据面节点仍然可访问。
- 控制面恢复后，需要把数据面临时自治决策收敛回权威状态。

所有影响 control plane、edge proxy、DNS、Caddy、Ingress、cluster bootstrap、node updater、runtime routing 或平台级流量规则的代码实现，都必须通过本仓库正式发布链路：提交到 `main`，由 `.github/workflows/deploy-control-plane.yml` 更新远端控制面。只有明确的紧急恢复才允许手工线上热修；热修后必须把同等修复回写本仓库。

## 1. Problem statement

Fugue 不能继续把 control plane 当作实时生命线。control plane 应该是权威编排面，而不是每个数据面组件每次请求、每次健康判断、每次故障处理都必须依赖的唯一中枢。

目标架构应满足：

```text
control plane online:
  control plane publishes authoritative desired state
  data-plane consumers pull, validate, apply, probe, report

control plane offline:
  data-plane consumers serve validated LKG
  node-local guardians run safe local repair
  DNS/edge use bounded emergency overlay to avoid clearly bad nodes
  all temporary actions are TTL-bound and written to local WAL

control plane recovered:
  consumers replay local WAL
  control plane reconciles actual state, incidents, and temporary decisions
  authority returns to control plane release records
```

核心判断：

- 控制面是权威源，但不是数据面活着的前提。
- 数据面可以短期自治，但不能永久改写权威状态。
- 自动修复要以不变量和证据触发，不能靠猜测。
- 无法 100% 归因的故障，优先补观测和隔离，不做危险修复。
- 所有临时自治动作必须有 TTL、audit、blast-radius cap 和 emergency disable switch。

## 2. Target architecture

Fugue 目标拆成四层自治。

### 2.1 Authoritative control plane

控制面职责：

- 维护租户、项目、服务、route、edge policy、DNS policy、release、billing 等权威状态。
- 生成并签名 platform artifacts。
- 维护发布记录、LKG 记录、consumer generation、incident、audit。
- 做全局调度、长期 edge ranking、release gate、traffic policy。
- 恢复后 reconcile 所有数据面临时自治动作。

控制面不应该是：

- edge 每次转发请求的实时依赖。
- DNS 每次回答的实时依赖。
- node-local 基础健康修复的实时依赖。
- 数据面服务 LKG 的实时依赖。

### 2.2 Data-plane autonomy layer

数据面自治层包括：

- edge front / worker / Caddy
- authoritative DNS server
- edge route bundle consumer
- DNS answer bundle consumer
- platform state read consumer

控制面断开时它们必须能：

- 先 load local LKG，再尝试连接 control plane。
- 验证 LKG hash、signature、schema、expires_at。
- 在 LKG 未过期时继续服务。
- 本地探测 edge listener、TLS、route match、origin DNS、origin TCP、origin HTTP。
- 对明显坏的本地 route / origin 做临时熔断。
- 把本地状态、故障和临时动作写入 local WAL。
- 控制面恢复后回放 WAL。

### 2.3 Node autonomy layer

节点自治层包括：

- node guardian
- node updater
- runtime agent
- image-cache agent
- local health probe runner

控制面断开时它们必须能：

- 持续检查本机 K3s agent、CNI、pod DNS、service DNS、iptables、conntrack、disk、inode、CPU steal、time sync。
- 执行安全、可逆、本地范围内的修复。
- 不能修复时本地 quarantine，停止承载新流量。
- 保持已有 workload，避免危险迁移。
- 对 stateful workload 不做无 fencing 证据的自动迁移。

### 2.4 Peer emergency overlay

同伴协作层用于控制面不可用期间的短期数据面安全决策。

允许的 peer overlay：

- edge 之间互相做公网 TLS / HTTP probe。
- DNS 节点汇总本地和 peer 的 edge health。
- 多个独立节点同时观测到某 edge 不可达时，DNS 临时过滤该 edge。
- edge 把 origin probe 失败状态广播给 DNS / peer edge。

不允许的 peer overlay：

- 永久修改 route policy。
- 覆盖 service-level exclusion。
- 修改 tenant / billing / auth / project 权限。
- 无 quorum / fencing 地迁移 stateful workload。
- 删除镜像、volume、PVC、数据库数据。

所有 peer overlay 结论必须：

- TTL-bound。
- 有 generation。
- 有 signer / node identity。
- 有 evidence summary。
- 有 blast-radius cap。
- 控制面恢复后重新确认或撤销。

## 3. Control-plane HA and recovery

### 3.1 Multi-node control plane

当前事故显示，单个 control-plane VM 被 hypervisor powerdown 会导致管理面和数据面控制通道同时失去权威协调能力。

重要约束：

- 本计划不要求在当前只有一台 control-plane 机器的物理限制下强行伪造 HA。
- 只有一个 control-plane VM 时，Fugue 不能承诺 control plane 自身零中断；此时应该优先保证 data plane autonomy、LKG 服务、node-local repair、外部 watchdog 和清晰告警。
- 代码、schema、发布链路和运行时协议应提前支持多 control-plane-capable 节点；当未来自然加入第二、第三个控制面节点时，系统可以自动进入更高鲁棒性，而不是再为特定地区或特定机器改代码。
- 多控制面能力必须是拓扑驱动的：节点通过 role、capability、failure domain、provider metadata、quorum membership 和 endpoint health 自组织，而不是写死机器名、地区名或 provider。

目标状态：

- 当集群只有 1 个 control-plane-capable 节点时：
  - 保持单控制面拓扑，不做危险的假 HA。
  - 所有 data-plane consumer 必须使用 validated LKG 继续服务。
  - node guardian、edge、DNS 必须能在 bounded scope 内自治。
  - external watchdog 负责发现 provider / hypervisor powerdown 并执行可审计恢复动作。
  - CLI / Web / alert 明确展示 `single-control-plane` 风险状态。
- 当集群有 2 个 control-plane-capable 节点时：
  - 支持 API / controller / telemetry / release guardian 多副本。
  - 支持 control-plane endpoint 多地址发布。
  - 明确标注 etcd / DB quorum 仍未达到完整 HA，不能把它宣传为完整三节点 quorum。
- 当集群有 3 个及以上 control-plane-capable 节点时：
  - 自动形成 3-node k3s server / etcd voting member quorum。
  - control-plane-capable 节点优先跨 provider / region / failure domain。
  - API、controller、telemetry、release guardian 多副本运行。
  - PodDisruptionBudget 和 topology spread 保障控制面副本不落在同一 VM 或同一 failure domain。
  - 控制面入口不绑定单个 VM；使用多 A 记录、外部 LB 或 dedicated HA endpoint。
  - 控制面 DB leader / replica 状态必须可解释。

实现要求：

- 新增或扩展 node capability model：`control-plane-capable`、`etcd-voter-capable`、`release-runner-capable`、`failure_domain`、`provider`、`region`。
- 控制面组件调度、endpoint 生成、runner 选择、watchdog 目标选择都读取 capability model。
- bootstrap / join / reconcile 流程必须支持未来节点加入后自动提升拓扑等级。
- 当前单控制面部署不应被新代码误判为部署失败；只能产生 risk warning 和 missing redundancy status。
- 多控制面升级必须有 dry-run / explain：展示当前节点能形成的 topology、quorum 风险和下一步缺口。

### 3.2 Release runner de-single-point

GitHub Actions self-hosted runner 不应只依赖单个控制面节点。

目标：

- 至少两个 runner，分布在不同 failure domain。
- runner 不与唯一 control-plane VM 绑定。
- runner 只通过正式 kube API / Helm / signed artifact 发布。
- 控制面不可用时，runner 明确失败并记录原因，而不是部分写入。
- 紧急恢复 runner 可以部署到备用管理节点，但不能绕过审计。

### 3.3 External watchdog

Fugue 集群内部无法可靠恢复“所有控制面 VM 被 provider powerdown”的情况，因此需要外部 watchdog。

外部 watchdog 检查：

- `api.fugue.pro` HTTP/TLS。
- Kubernetes API endpoint。
- control-plane DB leader / quorum。
- authoritative DNS health。
- edge public probes。
- GitHub runner availability。

外部 watchdog 动作：

- 如果 provider API 可用，power on / reboot 被关机的 control-plane VM。
- 如果存在 standby control-plane node，触发 promote runbook。
- 如果 provider API 不可用，只告警，不猜测修复。
- 记录 provider action id、request id、operator、结果。

## 4. Edge autonomy

### 4.1 LKG first startup

edge 启动流程必须改成：

```text
load local LKG
verify hash / signature / schema / expires_at
start serving LKG if valid
connect control plane read API
pull newer generation if available
stage -> apply -> probe -> promote to local active
write new LKG only after successful probe
report actual generation
```

禁止行为：

- 没有 LKG 时静默启动成“ready but no routes”。
- LKG hash 校验失败仍继续服务。
- LKG 过期仍静默服务。
- 新 bundle 未 probe 成功就覆盖 LKG。

### 4.2 Origin health matrix

每个 edge 必须为每个 active hostname / path prefix 维护本地 origin health。

最小探测项：

- service DNS resolve。
- ClusterIP TCP connect。
- endpoint IP TCP connect。
- HTTP lightweight probe。
- origin TLS / HTTP protocol match。
- first byte timeout。
- request body write / upstream body read rate。
- response write error。

输出分类：

- `origin_dns_failed`
- `origin_cluster_ip_connect_failed`
- `origin_endpoint_connect_failed`
- `origin_http_probe_failed`
- `origin_ttfb_timeout`
- `origin_body_write_slow`
- `origin_response_write_failed`
- `edge_listener_failed`
- `edge_tls_failed`
- `edge_route_missing`

### 4.3 Direct endpoint fallback

当 control plane 不可用且 service DNS / ClusterIP 失败时，edge 可以使用 endpoint LKG 做短期 fallback。

前提：

- endpoint LKG 未过期。
- endpoint IP 属于当前或最近 verified 的 runtime node PodCIDR。
- endpoint 对应服务和 route generation 匹配。
- fallback 只对 stateless HTTP route 默认启用。
- stateful 或 unsafe route 需要显式 policy。

限制：

- fallback TTL 默认 60 到 300 秒。
- fallback 失败后本地熔断，不无限重试。
- fallback 命中必须写 local WAL。
- 控制面恢复后必须上报 fallback 发生次数、成功率、失败原因。

### 4.4 Local edge repair

edge 本地自动修复分级：

```text
L0 observe: record and report only
L1 reload: reload route bundle / Caddy config / TLS material
L2 restart_component: restart edge worker / Caddy container
L3 restart_local_agent: restart k3s-agent only after strict local checks
L4 self_quarantine: stop accepting new DNS answers / mark edge unavailable locally
L5 human_or_provider: VM power action, disk repair, stateful recovery
```

所有自动动作必须：

- 幂等。
- 限流。
- 有 cooldown。
- 写 audit。
- 有 rollback / restore condition。
- 失败后升级 incident，而不是循环重启。

## 5. DNS autonomy

### 5.1 Answer-time filtering

DNS 节点在回答时不能只机械读取旧 bundle。它必须基于本地和 peer health 做 answer-time filtering。

硬门禁：

- service-level exclusion 永远生效。
- draining edge 不答。
- heartbeat stale 且无 valid peer proof 的 edge 不答。
- TLS / route-ready / LKG invalid 的 edge 不答。
- self-quarantined edge 不答。

临时过滤：

- edge public probe 连续失败。
- 多个 peer 从不同 failure domain 观测失败。
- edge 报告自身 origin 全失败。
- DNS 本地到 edge TLS probe 失败。

临时过滤必须：

- 有 TTL。
- 不修改 control plane policy。
- 不绕过 minimum edge count guard。
- 写 local WAL。
- 控制面恢复后上报。

### 5.2 Minimum answer policy

每个服务可配置最低可用 edge 数。

建议默认：

- 普通服务：至少 1 个 healthy edge。
- 平台关键 API：至少 2 个 healthy edge，低于阈值进入 critical incident。
- 被手工 exclusion 后剩余 edge 数低于阈值时，CLI / Web / release guard 必须明确提示。

低于最低阈值时：

- 不自动解除 service exclusion。
- 不自动把 bad edge 加回去。
- 告警并提示 operator 选择：解除 exclusion、恢复 edge、增加 edge、接受单点风险。

### 5.3 DNS local LKG

DNS LKG 必须包含：

- zone generation。
- answer bundle generation。
- edge eligibility snapshot。
- service exclusion snapshot。
- protected platform records。
- generated_at / expires_at。
- content hash / signature。

当前落地格式：

- 当前写入格式是 `lkgcache.Envelope(kind=dns_answer_bundle)`，外层包含 `schema_version`、`generation`、`content_hash`、`expires_at`、`created_at` 和 `payload`。
- `payload` 是 DNS `cacheFile(version=1, etag, cached_at, bundle)`；其中 `bundle` 仍使用 control plane 签名的 `EdgeDNSBundle`。
- 文件级 `content_hash` 防止本地 LKG payload 被部分写坏或篡改。
- 文件级 `expires_at` 使用 `bundle.valid_until + FUGUE_DNS_MAX_STALE`；无 `valid_until` 的开发/测试 bundle 使用保守 fallback TTL。
- 读取路径兼容旧 `cacheFile(version=1)`，用于线上已有缓存平滑迁移；一旦新版本成功同步，后续写入自动变成 envelope。
- 如果当前 envelope hash/schema/expires_at 校验失败，DNS 会拒绝当前文件并尝试 `.previous` / archive 中的已验证 LKG。

DNS 启动时：

```text
load DNS LKG
verify hash/signature/schema/expires_at
serve LKG if valid
start local edge probes
start peer health receiver
pull control plane if available
```

## 6. Node guardian closed loop

### 6.1 Continuous checks

node guardian 必须从定时 updater 升级为常驻闭环。

最小检查：

- `k3s-agent` / kubelet process。
- local apiserver `127.0.0.1:6444` latency / error。
- remotedialer to control plane endpoint。
- node lease freshness。
- pod sandbox creation。
- pod DNS to kube-dns Service IP。
- pod DNS to CoreDNS pod IP。
- `kubernetes.default.svc` resolve。
- same namespace service DNS + TCP。
- CNI bridge exists。
- PodCIDR matches Kubernetes node spec。
- kube-proxy / iptables / ipvs health。
- Fugue-managed iptables provenance and stale targets。
- conntrack utilization。
- disk / inode / memory / CPU steal / load。
- time sync / NTP skew。
- Caddy / edge worker listener and config generation。

### 6.2 Safe local repair

允许自动执行的本地安全修复：

- 删除 Fugue 管理、带 provenance、目标已过期的 iptables 规则。
- 重载 local LKG Caddy / edge route bundle。
- 重启明显 crashloop 的 edge worker / DNS component。
- 重启 local node guardian 子任务。
- 清理 Fugue 管理的 stale temp file / lock。
- 刷新 local generation cache。

需要更严格 guard 的动作：

- 重启 `k3s-agent`。
- 重启 CNI / kube-proxy。
- 重建 local route table。
- 从 image-cache 恢复缺失 image。

禁止自动执行的动作：

- 删除 PVC / volume / DB data。
- 迁移 stateful workload。
- 修改 tenant policy / service exclusion。
- provider poweroff / rebuild。
- 在无 fencing 证据下 takeover stateful primary。

### 6.3 Local quarantine

当节点违反硬不变量且本地修复失败时，node guardian 应进入 local quarantine。

效果：

- edge node 从本地 DNS answer 中剔除。
- runtime scheduler 后续不得调度新 workload 到该节点。
- 已有连接尽量 drain。
- stateful workload 仅标记风险，不自动迁移。
- 写 incident local WAL。

恢复条件：

- 所有 hard checks 连续通过 N 次。
- cooldown 已过。
- repair action 无未处理失败。
- control plane 恢复后能够 reconcile。

## 7. Peer health overlay

### 7.1 Peer signal model

peer signal 不是权威状态，只是控制面不可用时的数据面临时安全信号。

字段：

- `signal_id`
- `node_id`
- `node_role`
- `failure_domain`
- `subject_type`
- `subject_id`
- `check_name`
- `status`
- `observed_at`
- `expires_at`
- `confidence`
- `evidence_hash`
- `signature`

### 7.2 Consensus policy

建议策略：

- 单个节点观测失败：`suspect`，只降低权重。
- 两个不同 failure domain 观测失败：临时过滤。
- 三个及以上观测失败：延长过滤 TTL，并升级 incident。
- subject 自己报告 self-quarantine：立即过滤，但 TTL 较短。
- peer 信号过期：自动失效。

### 7.3 Control-plane reconciliation

控制面恢复后：

- 拉取所有 peer signal WAL。
- 合并相同 incident。
- 验证 signer 和时间线。
- 将临时过滤转成正式 incident。
- 如果当前探针已恢复，关闭 temporary quarantine。
- 如果仍失败，进入正式 quarantine workflow。

## 8. Recovery speed objectives

建议初始 SLO：

| Failure | Detect | First safe action | Full recovery target |
| --- | ---: | ---: | ---: |
| edge worker crash | 10s | 30s restart/reload | 1m |
| Caddy config bad | 10s | 30s reload LKG | 1m |
| edge public probe fail | 30s | 60s DNS temporary filter | 2m |
| origin service DNS fail | 30s | 60s endpoint LKG fallback or route breaker | 3m |
| node pod DNS fail | 30s | 2m repair or local quarantine | 5m |
| k3s-agent local API timeout | 30s | 2m guarded restart or quarantine | 5m |
| single control-plane pod fail | 15s | 30s replica takeover | 1m |
| single control-plane VM powerdown, only one control-plane-capable node exists | 30s | data plane serves LKG and watchdog opens incident | provider dependent |
| single control-plane VM powerdown, 3+ control-plane-capable nodes exist | 30s | 1m HA failover | 3m |
| all control-plane unavailable, data plane alive | 30s | data plane serves LKG | until control plane restored |
| provider VM powerdown | 30s | external watchdog action | provider dependent |

## 9. Observability and evidence

自动修复前必须补足证据，否则系统会把未知问题当成已知问题修。

### 9.1 Edge request evidence

每个 edge 请求至少记录：

- `request_id`
- `hostname`
- `path_class`
- `edge_id`
- `edge_group_id`
- `route_generation`
- `lkg_generation`
- `origin_resolution_mode`
- `origin_service_dns_ms`
- `origin_cluster_ip_connect_ms`
- `origin_endpoint_connect_ms`
- `origin_ttfb_ms`
- `client_body_read_bps`
- `origin_body_write_bps`
- `response_write_error`
- `failure_class`

### 9.2 DNS answer evidence

每次 DNS answer 采样记录：

- `query_name`
- `resolver_ip_hash`
- `answered_edge_ids`
- `filtered_edge_ids`
- `filter_reasons`
- `dns_bundle_generation`
- `local_overlay_generation`
- `served_lkg`
- `ttl`

### 9.3 Node guardian evidence

每次检查和修复记录：

- `node_id`
- `check_name`
- `expected`
- `observed`
- `evidence_ref`
- `action`
- `action_mode`
- `exit_code`
- `before_probe`
- `after_probe`
- `cooldown_until`
- `quarantine_state`

### 9.4 Provider power evidence

需要单独区分：

- guest initiated shutdown。
- hypervisor initiated shutdown。
- provider maintenance。
- provider API power action。
- kernel panic。
- OOM kill。
- manual reboot。
- unknown power loss。

guest 内部只能证明 `qemu-ga guest-shutdown` 和 systemd 行为；provider 控制台 / API 才能证明谁触发了 power action。

## 10. Failure contracts

每个核心组件必须有 failure contract。

### 10.1 Control plane API

- Failure modes: pod crash, bad image, DB unavailable, K8s API unavailable, VM powerdown。
- Detection: external watchdog, K8s readiness, synthetic API probe, release guard。
- Isolation: stop rollout, serve read API from healthy replica, fail closed for writes。
- Fallback: data plane serves LKG。
- Repair: restart pod, rollback image, HA failover, provider power action。
- Human boundary: DB restore, etcd restore, provider rebuild。

### 10.2 Edge node

- Failure modes: Caddy bad config, worker crash, route LKG expired, origin DNS fail, public TLS fail。
- Detection: local self probe, DNS peer probe, request failure attribution。
- Isolation: self-quarantine, DNS temporary filter, route breaker。
- Fallback: reload LKG, endpoint LKG fallback。
- Repair: reload Caddy, restart worker, restart k3s-agent under guard。
- Human boundary: disk corruption, repeated K3s failure, provider power action。

### 10.3 DNS node

- Failure modes: stale bundle, bad answers, peer overlay corruption, local DNS process fail。
- Detection: DNS synthetic probe, answer audit, peer cross-check。
- Isolation: stop serving unsafe generation, serve LKG, remove bad edge answers。
- Fallback: DNS LKG。
- Repair: reload bundle, restart DNS server。
- Human boundary: registrar / NS / glue update。

### 10.4 Node guardian

- Failure modes: false positive, false negative, repair loop, stale checks。
- Detection: guardian heartbeat, check freshness, peer comparison。
- Isolation: disable specific repair action, observe-only fallback。
- Fallback: no dangerous repair; report degraded。
- Repair: restart guardian, reset local state cache。
- Human boundary: repeated false quarantine, unknown drift。

### 10.5 Runtime agent

- Failure modes: service DNS fail, endpoint stale, app pod unreachable, scheduler stale placement。
- Detection: runtime probe, app service probe, node quarantine signal。
- Isolation: stop new placement to bad node, keep existing workload unless unsafe。
- Fallback: stable release / old pod / LKG placement。
- Repair: refresh endpoints, restart stateless pod if safe。
- Human boundary: stateful migration without fence evidence。

## 11. Rollout phases

### Phase 0: prove current gaps

Goal: 不改变生产流量，只证明当前系统在哪些环节没有闭环。

Deliverables:

- control-plane outage drill。
- edge local LKG drill。
- DNS stale answer drill。
- node DNS / CNI failure drill。
- k3s-agent local API timeout drill。
- provider hypervisor powerdown evidence collector。

### Phase 1: observability first

Goal: 自动动作前先把证据链补全。

Deliverables:

- edge request failure classification。
- DNS answer audit。
- node guardian action evidence。
- local WAL。
- consumer generation and LKG inventory。
- provider power event import interface。

### Phase 2: data-plane LKG hardening

Goal: 控制面断开时，edge / DNS 可靠服务 validated LKG。

Deliverables:

- LKG hash / signature / schema verify。
- LKG expires_at enforcement。
- startup load-LKG-first。
- degraded state when LKG invalid / expired。
- control-plane read failure drill。

### Phase 3: safe local repair

Goal: 只开放明确安全、可逆、本地范围内的修复。

Deliverables:

- Caddy / edge worker reload/restart。
- stale Fugue-managed iptables cleanup。
- local route bundle reload。
- node guardian cooldown / rate limit。
- repair audit。

### Phase 4: temporary quarantine

Goal: 在不改权威状态的情况下，临时保护用户流量。

Deliverables:

- edge self-quarantine。
- DNS answer-time filtering。
- peer signal model。
- TTL-bound temporary quarantine。
- control-plane reconciliation。

### Phase 5: HA control plane

Goal: 代码和协议先支持多控制面自组织；当前只有一台 control-plane 机器时不伪造 HA，只暴露风险并让数据面自治兜底。

Deliverables:

- control-plane capability model。
- topology discovery and explain。
- single-control-plane risk status。
- optional 3-node control-plane bootstrap path when enough nodes exist。
- multi-runner deploy path。
- external watchdog。
- provider power action integration。
- single-control-plane outage drill with data-plane autonomy validation。
- control-plane failover drill when 3+ control-plane-capable nodes exist。

### Phase 6: closed-loop recovery

Goal: 从 detection 到 repair 到 restore 到 incident summary 全闭环。

Deliverables:

- automatic restore after checks pass。
- replay local WAL。
- incident summary。
- runbook links。
- Web / CLI explain。
- chaos drill coverage。

## 12. Detailed TODO list

### A. Architecture and contracts

- [x] 固定本专项方案并在主 resilience 文档中链接。
- [x] 为 control plane API 定义 failure contract。
- [x] 为 controller 定义 failure contract。
- [x] 为 edge worker 定义 failure contract。
- [x] 为 Caddy / edge-front 定义 failure contract。
- [x] 为 DNS server 定义 failure contract。
- [x] 为 node guardian 定义 failure contract。
- [x] 为 runtime agent 定义 failure contract。
- [x] 为 image-cache agent 定义 failure contract。
- [x] 为 GitHub Actions runner 定义 failure contract。
- [x] 定义所有自动动作的 safety class。
- [x] 定义所有自动动作的 human approval boundary。
- [x] 定义 emergency disable switch。

### B. Control-plane HA

- [x] 盘点当前 control-plane 单点：VM、K8s server、API pod、controller pod、DB、runner、DNS endpoint；当前单机器物理限制只报告 `single-control-plane` risk，不强行伪造 HA。
- [x] 定义 control-plane capability model：`control-plane-capable`、`etcd-voter-capable`、`release-runner-capable`、`provider`、`region`、`failure_domain`。
- [x] 为当前单 control-plane 拓扑增加 `single-control-plane` risk status，不把它当作发布失败。
- [x] 为 CLI / Web / admin API 增加 control-plane topology explain，明确当前拓扑能力和缺口。
- [x] 设计 1-node / 2-node / 3-node+ 的分级 control-plane topology，不强迫当前单机器部署变成三节点。
- [x] 设计 3-node k3s server / etcd topology，作为未来节点足够后的自动升级目标。
- [x] 让 bootstrap / join / reconcile 支持未来 control-plane-capable 节点加入后自动提升拓扑等级。
- [x] 为 control-plane pod 增加 topology spread。
- [x] 为 API / controller 增加 PDB。
- [x] 让 API / controller 可跨多个 control-plane-capable 节点运行；当前只有一个节点时保持单副本或受限副本。
- [x] 将 control-plane endpoint 生成逻辑改为 topology-aware：单节点保持单入口，多节点发布 HA endpoint。
- [x] 增加 control-plane external synthetic probe。
- [x] 增加当前单 control-plane VM powerdown drill，验证 edge / DNS LKG 和 watchdog 行为。
- [x] 增加 3-node+ control-plane VM powerdown drill 定义；当前未具备 3 个 control-plane-capable 节点时标记 deferred physical capacity，只验证单控制面风险提示、数据面 LKG 和 watchdog 行为。
- [x] 增加 etcd quorum loss drill 定义；当前未形成 quorum 时只验证风险提示、fail-closed 和只读降级。
- [x] 增加 API bad image rollback drill。

### C. Release runner de-single-point

- [x] 盘点当前 self-hosted runner 所在节点和依赖，并在每次发布 artifact 中记录 runner attribution。
- [x] 当前物理限制下不强行新增备用 runner；代码和 workflow attribution 预留多 runner 健康选择所需证据。
- [x] 当前物理限制下不强行伪造跨 failure domain runner；capability model 支持未来 runner 节点跨 failure domain。
- [x] deploy workflow 能在 GitHub runner 调度层选择健康 self-hosted runner；当前只有一个 runner 时以 fail-closed 和 attribution 解释失败。
- [x] runner 不健康时发布失败原因可解释。
- [x] 控制面不可用时 deploy workflow fail closed。
- [x] 记录每次 runner 发布的 target cluster、commit、image、helm diff。

### D. External watchdog

- [x] 定义 watchdog 部署位置，不依赖 Fugue 主集群。
- [x] 增加 `api.fugue.pro` HTTP/TLS probe。
- [x] 增加 Kubernetes API probe。
- [x] 增加 control-plane DB / quorum probe。
- [x] 增加 authoritative DNS probe。
- [x] 增加 edge public probe。
- [x] 增加 GitHub runner probe。
- [x] 设计 provider power action 接口。
- [x] 记录 provider action id 和结果。
- [x] 当 provider API 不可用时只告警，不自动猜测修复。

### E. Edge LKG hardening

- [x] 定义 edge route LKG 文件格式。
- [x] 定义 Caddy config LKG 文件格式。
- [x] 定义 TLS material LKG 引用格式。
- [x] LKG 写入使用 atomic rename。
- [x] LKG 写入保存 content hash。
- [x] LKG 读取校验 hash。
- [x] LKG 读取校验 signature。
- [x] LKG 读取校验 schema version。
- [x] LKG 读取校验 expires_at。
- [x] edge 启动改为 load-LKG-first。
- [x] LKG invalid 时 edge 不报告 healthy。
- [x] LKG expired 时 edge 进入 degraded。
- [x] 新 bundle apply 后必须 probe 成功才能成为 LKG。
- [x] 增加 LKG corruption test。
- [x] 增加 control-plane read failure LKG serving test。

### F. Edge origin health

- [x] 为每个 hostname / path prefix 建立 origin health record。
- [x] 增加 service DNS resolve probe。
- [x] 增加 ClusterIP TCP connect probe。
- [x] 增加 endpoint IP TCP connect probe。
- [x] 增加 HTTP lightweight probe。
- [x] 增加 origin TTFB timeout classification。
- [x] 增加 request body write rate classification。
- [x] 增加 response write error classification。
- [x] 请求日志写入 origin_resolution_mode。
- [x] 请求日志写入 route_generation。
- [x] 请求日志写入 lkg_generation。
- [x] request explain 能解释 edge-origin failure class。

### G. Direct endpoint fallback

- [x] 定义 endpoint LKG 数据模型。
- [x] endpoint LKG 绑定 route generation。
- [x] endpoint LKG 绑定 service identity。
- [x] endpoint LKG 绑定 PodCIDR / node identity。
- [x] control plane 不可用时允许短 TTL fallback。
- [x] fallback 仅默认作用于 stateless HTTP route。
- [x] stateful fallback 需要显式 policy。
- [x] fallback 命中写 local WAL。
- [x] fallback 成功率和失败原因上报。
- [x] fallback TTL 过期后 fail closed。
- [x] 增加 service DNS fail -> endpoint fallback drill。

### H. Edge local repair

- [x] 定义 edge repair safety classes L0-L5。
- [x] 实现 Caddy reload LKG。
- [x] 实现 edge worker restart with cooldown。
- [x] 实现 edge route bundle reload。
- [x] 实现 guarded k3s-agent restart preflight。
- [x] repair action 写 audit / local WAL。
- [x] repair action 有 cooldown。
- [x] repair action 有 max attempts。
- [x] 连续失败进入 self_quarantine。
- [x] CLI 展示 repair history。

### I. DNS autonomy

- [x] 定义 DNS LKG 文件格式。
- [x] DNS 启动 load-LKG-first。
- [x] DNS LKG 校验 hash / signature / schema / expires_at。
- [x] DNS answer-time filtering 接入 edge local health。
- [x] DNS answer-time filtering 接入 peer health。
- [x] DNS 永远尊重 service-level exclusion。
- [x] DNS 不回答 draining edge。
- [x] DNS 不回答 self-quarantined edge。
- [x] DNS 不回答 LKG invalid edge。
- [x] DNS 记录 answer audit sample。
- [x] DNS 记录 filtered_edge_ids 和 filter_reasons。
- [x] DNS temporary filter 有 TTL。
- [x] DNS temporary filter 写 local WAL。
- [x] 增加 stale edge answer drill。

### J. Minimum edge policy

- [x] 为 service / hostname 增加 minimum healthy edge policy。
- [x] 平台关键 API 默认 minimum healthy edge >= 2。
- [x] 普通服务默认 minimum healthy edge >= 1。
- [x] service exclusion 后检查剩余 healthy edge。
- [x] 低于 minimum 时 CLI 明确提示单点风险。
- [x] 低于 minimum 时 Web console 明确提示单点风险。
- [x] 低于 minimum 时不自动解除 exclusion。
- [x] 低于 minimum 时生成 incident。

### K. Node guardian continuous checks

- [x] node guardian 常驻化。
- [x] 检查 k3s-agent / kubelet process。
- [x] 检查 local apiserver `127.0.0.1:6444`。
- [x] 检查 remotedialer 到 control plane endpoint。
- [x] 检查 node lease freshness。
- [x] 检查 pod sandbox creation。
- [x] 检查 pod DNS 到 kube-dns Service IP。
- [x] 检查 pod DNS 到 CoreDNS pod IP。
- [x] 检查 `kubernetes.default.svc` resolve。
- [x] 检查 same namespace service DNS。
- [x] 检查 same namespace service TCP。
- [x] 检查 CNI bridge。
- [x] 检查 PodCIDR 与 Kubernetes node spec。
- [x] 检查 kube-proxy / iptables / ipvs。
- [x] 检查 Fugue-managed stale iptables target。
- [x] 检查 conntrack utilization。
- [x] 检查 disk / inode / memory / CPU steal / load。
- [x] 检查 time sync / NTP skew。
- [x] 检查 edge / Caddy listener。

### L. Node guardian safe repair

- [x] 实现 Fugue-managed stale iptables cleanup。
- [x] 实现 local LKG bundle reload。
- [x] 实现 edge worker restart with guard。
- [x] 实现 DNS component restart with guard。
- [x] 实现 node guardian subtask restart。
- [x] 实现 local generation cache refresh。
- [x] 为 k3s-agent restart 定义严格 preflight。
- [x] 为 k3s-agent restart 定义 cooldown。
- [x] 为 CNI / kube-proxy repair 定义 human boundary。
- [x] 禁止自动 stateful migration without fence evidence。
- [x] 禁止自动删除 PVC / volume / DB data。

### M. Local quarantine

- [x] 定义 local quarantine state。
- [x] 定义 local quarantine reasons。
- [x] hard check failed 后进入 suspect。
- [x] repair failed 后进入 local quarantine。
- [x] local quarantine 从本地 DNS answer 剔除 edge。
- [x] local quarantine 阻止新 workload placement。
- [x] local quarantine 不中断已有连接，尽量 drain。
- [x] local quarantine 写 local WAL。
- [x] 连续 N 次 hard checks 通过后解除 local quarantine。
- [x] control plane 恢复后 reconcile local quarantine。

### N. Peer health overlay

- [x] 定义 peer signal schema。
- [x] peer signal 使用 node identity 签名。
- [x] peer signal 有 expires_at。
- [x] peer signal 有 evidence_hash。
- [x] edge 互相做 public TLS probe。
- [x] DNS 收集 peer edge health。
- [x] 单点失败标记 suspect。
- [x] 多 failure domain 失败触发 temporary filter。
- [x] subject self-quarantine 触发短 TTL filter。
- [x] peer signal 过期自动失效。
- [x] 控制面恢复后回放 peer WAL。
- [x] 增加 peer false positive drill。
- [x] 增加 peer stale signal drill。

### O. Local WAL and reconciliation

- [x] 定义 local WAL record schema。
- [x] edge 写 local WAL。
- [x] DNS 写 local WAL。
- [x] node guardian 写 local WAL。
- [x] runtime agent 写 local WAL。
- [x] WAL 记录 action、evidence、generation、expires_at。
- [x] WAL 写入 durable fsync 策略。
- [x] 控制面恢复后 consumer 回放 WAL。
- [x] control plane 合并相同 incident。
- [x] control plane 验证 WAL signer。
- [x] control plane 生成 incident summary。
- [x] control plane 清理已过期 temporary actions。

### P. Observability

- [x] edge request sample 增加 `edge_id`。
- [x] edge request sample 增加 `route_generation`。
- [x] edge request sample 增加 `lkg_generation`。
- [x] edge request sample 增加 `origin_resolution_mode`。
- [x] edge request sample 增加 service DNS timing。
- [x] edge request sample 增加 ClusterIP connect timing。
- [x] edge request sample 增加 endpoint connect timing。
- [x] edge request sample 增加 origin failure class。
- [x] DNS answer audit 记录 answered_edge_ids。
- [x] DNS answer audit 记录 filtered_edge_ids。
- [x] DNS answer audit 记录 filter_reasons。
- [x] node guardian 记录 before / after probe。
- [x] provider power event 分类入库。
- [x] `fugue admin robustness status` 展示 LKG serving consumers。
- [x] `fugue admin request explain` 展示 control-plane read failure vs data-plane failure。

### Q. Drills and verification

- [x] control plane API pod kill drill。
- [x] control-plane VM powerdown drill。
- [x] all control-plane unavailable but edge alive drill。
- [x] edge Caddy bad config drill。
- [x] edge worker crash drill。
- [x] service DNS failure drill。
- [x] ClusterIP connect failure drill。
- [x] endpoint fallback drill。
- [x] node pod DNS failure drill。
- [x] k3s-agent local API timeout drill。
- [x] stale iptables drift drill。
- [x] DNS stale answer drill。
- [x] peer false-positive drill。
- [x] LKG expired drill。
- [x] LKG corruption drill。
- [x] provider power event attribution drill。

### R. Rollout controls

- [x] 所有新自治能力先 shadow / observe-only。
- [x] 每个 repair action 单独 feature flag。
- [x] 每个 quarantine action 单独 feature flag。
- [x] 每个 DNS filtering action 单独 feature flag。
- [x] 自动动作有 global kill switch。
- [x] 自动动作有 per-node kill switch。
- [x] 自动动作有 per-service kill switch。
- [x] 自动动作有 blast-radius cap。
- [x] 自动动作有 rollback path。
- [x] 生产开启前至少完成一次非破坏性 readiness drill；3-node+ / multi-runner drill 在当前物理条件不足时标记 deferred physical capacity。

## 13. Definition of done

本计划完成不以“代码写完”作为标准，而以生产和 drill 能证明以下行为作为标准：

- 单个 control-plane VM 被 powerdown 时，管理面自动 failover 或外部 watchdog 明确拉起。
- control plane 全不可用但 edge / DNS 存活时，数据面继续服务 validated LKG。
- control plane 全不可用时，DNS 可以临时过滤明显坏的 edge。
- edge 本地 origin DNS / ClusterIP 失败时，可以 fallback 或明确熔断，不长时间挂死请求。
- node DNS / CNI / k3s-agent 软故障能在本地修复或 quarantine。
- 所有临时自治动作都有 TTL、evidence、audit、reconcile。
- 控制面恢复后能解释事故时间线，而不是依赖人工 SSH 拼日志。
- 无法 100% 归因的故障不会触发危险自动修复，只会触发观测、隔离或人工边界。
