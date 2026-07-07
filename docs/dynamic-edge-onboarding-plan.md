# Dynamic edge onboarding plan

本文档固定 Fugue 动态 edge 接入改造方案。目标是让 Fugue 集群未来可以加入任意国家、任意机房、任意运营商路线的服务器作为 edge，而不需要每次为一个新地区修改 Helm values、生产配置或代码。

本文不是线上热修记录。所有影响 control plane、edge proxy、edge-front、DNS、route bundle、cluster bootstrap、node updater、runtime 路由或平台级流量规则的实现，都必须回到本仓库走正式发布链路：提交到 `main`，再由 `.github/workflows/deploy-control-plane.yml` 更新远端控制平面。

## 背景

`dmit` VPS 已通过 `join-cluster.sh --edge-only` 加入 Fugue 集群，并且 NodePolicy 已经正确表达：

- `allow_edge=true`
- `allow_dns=false`
- `dedicated_mode=edge`
- `allow_app_runtime=false`
- `allow_builds=false`
- `allow_shared_pool=false`

但该节点没有真正运行 edge-front / edge-worker，也不会承接流量。直接原因是当前生产公共 edge workload 仍依赖静态 Helm group：

- 主 edge group 通过 `edge.nodeSelector` 固定到 `fugue.io/location-country-code=us`。
- 德国 edge group 通过 `edge.groups[].nodeSelector` 固定到 `fugue.io/location-country-code=de`。
- DNS group 同样静态渲染，当前只有主 US DNS 和 DE DNS。
- 新节点如果被识别为 `jp`、`sg`、`hk`、`kr` 或其他地区，除非生产 Helm values 新增对应 group，否则不会有 edge DaemonSet 调度到它。

这暴露出一个架构问题：Fugue 的节点接入已经逐步数据化，但公共 edge workload 仍然地区静态化。用户购买一台新 VPS，不应该触发“为这个地区新增一段生产 Helm 配置”的流程。

## 问题陈述

当前模型的问题不是 `fugue` CLI 不能创建 node key，也不是 `join-cluster.sh --edge-only` 不能表达 edge-only。真正的问题是：

```text
node intent is dynamic,
but edge workload placement is still static.
```

当前链路大致是：

```text
node key
  -> join-cluster.sh --edge-only
  -> Kubernetes node labels / taints
  -> NodePolicy allow_edge=true
  -> static Helm edge groups select only known countries
  -> unknown country has no edge workload
```

目标链路应该是：

```text
node key
  -> join-cluster.sh --edge-only
  -> Kubernetes node labels / taints
  -> NodePolicy allow_edge=true
  -> dynamic edge workload runs automatically
  -> control plane assigns edge group from node metadata
  -> edge reports readiness
  -> DNS canary/exploration starts safely
```

## 目标

1. 新增任意地区 edge 节点不需要改代码。
2. 新增任意地区 edge 节点不需要改生产 Helm values。
3. `join-cluster.sh --edge-only` 必须成为一次性接入路径。
4. edge-only 节点必须只承担 edge，不自动承担 DNS、app runtime、builder、shared pool。
5. 控制平面必须根据节点 metadata 自动创建或复用 edge group。
6. edge workload 必须能在动态 edge 节点上自动运行。
7. 新 edge 默认进入受控 canary / exploration，不直接吃满生产流量。
8. 新 edge 必须按节点单独观测质量指标。
9. 服务级 edge exclusion 必须继续作为硬 gate。
10. 现有 US/DE 静态 edge 必须能平滑兼容，不能因为动态化改造造成中断。

## 非目标

- 不在本方案里引入 anycast。
- 不要求新增 edge 节点同时成为 DNS 节点。
- 不要求每个国家都部署独立 DNS。
- 不把所有现有静态 edge 一次性迁移到动态模型。
- 不把低质量新 edge 直接提升为生产主力。
- 不通过手工 SSH patch live Deployment 作为正式修复路径。

## 设计原则

### 不允许地区特例

不能出现类似下面的长期实现：

```text
if country == "jp" { render jp daemonset }
if node == "dmit" { special case }
if provider == "dmit" { special case }
```

所有差异必须来自通用数据：

- NodePolicy
- Kubernetes node labels
- node updater desired state
- edge node heartbeat
- edge group inventory
- route bundle
- DNS answer policy
- service-level route policy

### 角色和地区分离

`edge` 是节点角色，`jp/us/de` 是位置属性。一个节点是否能跑 edge 由 role 决定，一个节点属于哪个 edge group 由位置和策略决定。

```text
role.edge=true
location-country-code=jp
public-ip=...
  -> node can run dynamic edge workload
  -> control plane derives edge-group-country-jp
```

### DNS 与 edge 分离

新增 edge 不代表新增 DNS。DNS 节点负责回答 edge IP；edge 节点负责处理用户流量。一个 edge-only 节点不应该监听 53，也不应该获得 DNS token。

### 先 gate 后 score

新节点即使成功加入，也必须先经过 hard gate：

- Kubernetes node Ready
- NodePolicy `effective_edge=true`
- node not draining
- edge pods Ready
- edge heartbeat fresh
- route bundle ready
- TLS ready
- public 80/443 probe pass
- service-level exclusion allow

通过 gate 后才进入 canary / ranking。

### 动态模型兼容静态模型

第一阶段不能拆掉 US/DE 静态 DaemonSet。新增 dynamic workload 只匹配新标签，不抢现有节点的 80/443 hostPort。

## 目标架构

### 动态 edge workload

新增一套通用 dynamic edge DaemonSet：

```text
fugue-fugue-edge-dynamic-front
fugue-fugue-edge-dynamic-worker-a
fugue-fugue-edge-dynamic-worker-b
```

selector：

```yaml
fugue.io/role.edge: "true"
fugue.io/schedulable: "true"
fugue.io/edge-workload: "dynamic"
```

这套 DaemonSet 不关心 `us/de/jp`。只要节点是动态 edge，就会自动调度。

### 动态 edge desired state

edge 进程不再必须依赖静态 `FUGUE_EDGE_GROUP_ID`。动态模式下，edge 通过自己的 node name 向控制平面获取 desired state：

```text
GET /v1/edge/nodes/{edge_id}/desired-state
```

返回：

```json
{
  "edge_id": "dmit",
  "edge_group_id": "edge-group-country-jp",
  "region": "asia",
  "country": "jp",
  "public_ipv4": "x.x.x.x",
  "route_bundle_generation": "routegen_...",
  "tls_mode": "public-on-demand",
  "canary_policy": {
    "state": "probing",
    "weight": 0
  },
  "service_exclusions": []
}
```

静态 env 仍然保留兼容。优先级：

1. dynamic desired state
2. static env values
3. fail closed

### 控制平面 edge group 自动 upsert

控制平面根据节点 metadata 自动 upsert edge group：

```text
country_code=jp
  -> edge_group_id=edge-group-country-jp
  -> region=asia
  -> country=jp
```

如果用户显式指定 `--edge-group`，则优先使用显式值，但仍必须校验该 group 不违反平台规则。

### DNS answer 仍由现有 DNS 节点发布

动态 edge 不需要运行 `fugue-dns`。现有 DNS 节点从控制平面拿到 route/DNS bundle 后，可以把动态 edge 的 public IP 作为 answer candidate。

DNS answer candidate 必须带上：

- `edge_id`
- `edge_group_id`
- `public_ip`
- `canary_state`
- `canary_weight`
- `quality_score`
- `hard_gate_reason`
- `service_exclusion_reason`

## 数据模型改造

### EdgeNode

目标字段：

```text
edge_id
cluster_node_name
tenant_id
machine_id
runtime_id
node_key_id
edge_group_id
region
country
country_code
public_ipv4
public_ipv6
mesh_ip
workload_mode        static | dynamic
status               joined | warming | probing | canary | active | draining | disabled
healthy
draining
dns_enabled
app_runtime_enabled
route_bundle_version
serving_generation
lkg_generation
tls_status
cache_status
caddy_route_count
caddy_applied_version
last_seen_at
last_heartbeat_at
created_at
updated_at
```

### EdgeGroup

目标字段：

```text
edge_group_id
kind                 country | region | custom
region
country
country_code
status
node_count
healthy_node_count
canary_node_count
active_node_count
created_at
updated_at
```

### EdgeNodePolicy

目标字段：

```text
edge_id
enabled
draining
canary_enabled
canary_weight
max_dns_answer_weight
min_quality_gate
service_exclusion_mode
updated_by
updated_at
```

第一版可以不单独建新表，而是扩展现有 edge node / machine policy；但 API response 必须呈现这些语义。

## join-cluster.sh 改造

### 参数

新增或固定这些参数：

```text
--edge-only
--country <country_code>
--region <region>
--public-ip <ip>
--edge-group <edge_group_id>
--edge-workload <static|dynamic>
```

环境变量等价项：

```text
FUGUE_EDGE_ONLY=true
FUGUE_NODE_COUNTRY_CODE=jp
FUGUE_NODE_REGION=asia
FUGUE_NODE_PUBLIC_IP=x.x.x.x
FUGUE_EDGE_GROUP_ID=edge-group-country-jp
FUGUE_EDGE_WORKLOAD=dynamic
```

### 自动探测

当前只依赖一个 GeoIP JSON 源时容易失败。应改成多源 fallback：

1. 控制平面 GeoIP endpoint，传入 detected public IP。
2. `ipapi.co/json`
3. `ipinfo.io/country`
4. `ifconfig.co/country-iso`
5. 用户显式参数兜底。

如果 GeoIP 失败但用户传了 `--country`，join 不应失败。

如果既无法探测也没有显式 country：

- node 可以加入集群。
- `allow_edge=true` 可以记录。
- 但不打 `fugue.io/edge-workload=dynamic`，避免启动无法归组的 public edge。
- CLI / node-policy status 必须显示明确原因：`missing_location_country_code`。

### edge-only labels

`--edge-only` 目标 labels：

```yaml
fugue.io/role.edge: "true"
fugue.io/edge-workload: "dynamic"
fugue.io/location-country-code: "jp"
fugue.io/public-ip: "x.x.x.x"
fugue.io/schedulable: "true"
```

不得出现：

```yaml
fugue.io/role.dns: "true"
fugue.io/role.app-runtime: "true"
fugue.io/role.builder: "true"
fugue.io/shared-pool: "internal"
```

### node updater

`fugue-node-updater` 必须持续 reconcile：

- location labels
- public IP label
- `edge-workload=dynamic`
- node-scoped edge credential
- edge desired-state cache
- k3s config reload

如果用户后续修正 country，node updater 必须能把节点从 `missing_location` 修复到 `dynamic edge ready`。

## Edge credential 改造

当前静态 group 依赖 Kubernetes Secret 注入 edge token。动态节点不能每新增一个地区就新增一个 Secret。

目标：

1. node key 只用于 join。
2. join 后控制平面签发 node-scoped edge credential。
3. node updater 写入节点本地 root-only 文件：

```text
/etc/fugue/edge-node.env
```

示例：

```text
FUGUE_EDGE_NODE_ID=dmit
FUGUE_EDGE_NODE_TOKEN=<redacted>
FUGUE_EDGE_DESIRED_STATE_URL=https://api.fugue.pro/v1/edge/nodes/dmit/desired-state
```

4. dynamic edge DaemonSet 通过 hostPath 只读挂载该文件。
5. edge heartbeat 使用 node-scoped token。
6. 控制平面校验：
   - token 属于该 node。
   - node name 与 token 绑定一致。
   - NodePolicy `effective_edge=true`。
   - node 未被 drain / disabled。

## Helm / Kubernetes 改造

### values.yaml

新增：

```yaml
edge:
  dynamic:
    enabled: false
    nodeSelector:
      fugue.io/role.edge: "true"
      fugue.io/schedulable: "true"
      fugue.io/edge-workload: "dynamic"
    credentialHostPath: /etc/fugue/edge-node.env
    blueGreen:
      enabled: true
```

生产第一阶段开启：

```yaml
edge:
  dynamic:
    enabled: true
```

现有静态 US/DE 配置保留。

### DaemonSet

新增动态 front/worker DaemonSet，并保持：

- OnDelete / blue-green worker 语义。
- hostPort 80/443 只由 front 占用。
- worker 使用 18080/18443 和 28080/28443。
- toleration 支持 `fugue.io/dedicated=edge`。
- 不匹配 `role.dns`。
- 不创建 DNS DaemonSet。

### 资源约束

动态 edge 默认资源请求应低于当前大节点假设，适配 1C/1G VPS：

```yaml
edge worker:
  requests:
    cpu: 25m
    memory: 128Mi
  limits:
    memory: 512Mi

edge front:
  requests:
    cpu: 10m
    memory: 32Mi
  limits:
    memory: 128Mi
```

如果节点太小导致不可调度，CLI 必须显示具体原因，而不是只显示没有 healthy edge。

## 控制平面 API / CLI 改造

### API

新增或扩展：

```text
GET  /v1/edge/nodes/{edge_id}/desired-state
POST /v1/edge/nodes/{edge_id}/probe
POST /v1/edge/nodes/{edge_id}/canary
POST /v1/edge/nodes/{edge_id}/drain
POST /v1/edge/nodes/{edge_id}/undrain
```

如果新增 HTTP API，必须先更新 `openapi/openapi.yaml`，再生成：

```bash
make generate-openapi
make test
```

### CLI

增强命令：

```bash
fugue admin edge nodes ls
fugue admin edge nodes get dmit
fugue admin edge node desired-state dmit
fugue admin edge node probe dmit
fugue admin edge node canary set dmit --weight 1
fugue admin edge node drain dmit
fugue admin edge node undrain dmit
```

输出必须能一眼看出：

- 是否 joined
- 是否 Kubernetes Ready
- 是否 edge-only
- 是否 DNS 节点
- country / region / public IP 是否存在
- dynamic workload 是否匹配
- edge pods 是否 Ready
- desired edge group
- heartbeat 是否 fresh
- route bundle 是否 ready
- TLS 是否 ready
- public probe 是否 pass
- 当前 canary state / weight
- 为什么还不能进入 DNS answer

## DNS / route bundle 改造

### Candidate 生成

DNS candidate 生成不应该只从静态 edge groups 出发。应从控制平面动态 edge inventory 出发：

```text
List eligible edge nodes
  -> group by edge_group_id
  -> apply route readiness
  -> apply service exclusion
  -> apply node health gate
  -> apply canary / quality ranking
  -> emit DNS answer candidate
```

### Canary 状态机

新 edge 状态：

```text
joined
  -> warming
  -> probing
  -> canary
  -> active
```

异常状态：

```text
draining
disabled
missing_location
workload_not_scheduled
probe_failed
tls_not_ready
route_bundle_not_ready
```

默认策略：

- 新 edge 不直接进入 active。
- 新 edge 通过 probe 后进入 canary。
- canary 初始权重建议 1%。
- 最多不超过 5%，直到有足够质量样本。
- canary 也必须遵守 service exclusion。

### 服务级 exclusion

已有“某服务不走某 edge”的能力必须继续作为 hard gate。

优先级：

```text
service exclusion > edge health > canary > ranking
```

即使 edge 质量分最高，只要服务显式排除，也不能返回。

## 质量指标接入

动态 edge 上线后必须按 `edge_id` 单独观测：

- request count
- body read duration
- body read bps
- min window bps
- max read gap
- body read error rate
- edge 5xx
- client cancel / 499
- TTFB
- total duration
- origin wait
- response egress bps
- TLS ready / errors
- cache ready / hit ratio
- edge-front TCP RTT
- edge-front TCP retrans / RTO / lost
- active requests
- active body buffers
- memory / goroutine / CPU saturation

这些指标不只是展示，还要进入 edge quality ranking 的 scoped 体系。具体 ranking 逻辑继续遵循 `docs/edge-quality-ranking-scoped-plan.md`。

## 迁移策略

### Phase 0: 文档和诊断

- 固定本文档。
- 在 CLI 输出里明确区分：
  - node joined
  - edge policy enabled
  - edge workload scheduled
  - DNS answer eligible

### Phase 1: dynamic edge workload shadow

- 新增 dynamic edge DaemonSet。
- 默认只匹配 `fugue.io/edge-workload=dynamic`。
- 不影响现有 US/DE 静态 DaemonSet。
- `dmit` 作为第一台 dynamic edge 验证。

### Phase 2: dynamic desired state

- edge 支持 desired-state API。
- dynamic edge 不依赖静态 `FUGUE_EDGE_GROUP_ID`。
- 控制平面自动 upsert `edge-group-country-<country_code>`。

### Phase 3: canary DNS answer

- DNS bundle 纳入 dynamic edge candidate。
- 新 edge 通过 probe 后进入低比例 canary。
- 按 `edge_id` 观测真实请求质量。

### Phase 4: active ranking

- 样本足够后进入 active。
- scoped quality ranking 决定同 group 或跨 group 的权重。
- 质量差自动降权或 drain。

### Phase 5: 静态 group 退场

- 逐步把 US/DE 老 edge 标记为 dynamic workload。
- 删除静态 country-specific Helm groups。
- 保留显式 static escape hatch 作为运维兜底，但不作为默认路径。

## 兼容与风险控制

### 与现有 US/DE edge 兼容

第一阶段不改变现有 `vps-591f4447`、`vps-84c8f0a9` 的 DaemonSet selector 和 hostPort 占用。

dynamic DaemonSet 只调度到带 `fugue.io/edge-workload=dynamic` 的节点，避免同一节点两个 front 争抢 80/443。

### 与 DNS 兼容

新增 dynamic edge 不新增 DNS DaemonSet。现有 DNS 节点继续负责回答。

如果 dynamic edge 不健康，DNS answer 不包含它。

### 与服务排除兼容

服务级排除必须在候选阶段直接过滤，不能留到 score 阶段。

### 回滚

回滚方式：

1. 设置节点 `draining=true`，停止新 DNS answer。
2. 设置 canary weight 为 0。
3. 移除 `fugue.io/edge-workload=dynamic` label，dynamic DaemonSet 不再运行。
4. 如需完全退出，revoke node key / decommission node。

不需要删除现有 US/DE 静态 edge。

## 验收标准

一个新 edge 节点验收通过必须满足：

```text
fugue admin cluster node-policy get <node>
  effective_edge=true
  effective_dns=false
  effective_app_runtime=false

fugue admin edge nodes get <node>
  workload_mode=dynamic
  edge_group_id=edge-group-country-<country>
  status in [canary, active]
  route_bundle_ready=true
  tls_ready=true
  public_probe_ready=true

fugue admin dns answer-check <hostname> --explain
  candidate contains <node> only when canary/ranking allows
  hard gates visible
  service exclusions respected
```

`dmit` 验收目标：

- `dmit` 不运行 `fugue-dns`。
- `dmit` 运行 dynamic edge-front / worker-a / worker-b。
- `dmit` edge group 为 `edge-group-country-jp`，除非用户显式改为其他 group。
- `dmit` 先进入 canary，不直接全量。
- `api.0-0.pro` 如存在 service-level exclusion，必须继续生效。

## Todo list

### A. 文档和现状诊断

- [x] 固定动态 edge 接入方案到本地文档。
- [x] 在 `docs/regional-edge-data-plane.md` 中补充本文档链接和“静态 country group 只作为过渡状态”的说明。
- [x] 增强 CLI/诊断输出，明确区分 node joined、policy enabled、workload scheduled、DNS eligible。
- [x] 为 `dmit` 记录当前状态：edge-only policy 正确，但缺少 dynamic workload 支持。

### B. join-cluster.sh

- [x] 给 `join-cluster.sh` 增加 `--country` 参数。
- [x] 给 `join-cluster.sh` 增加 `--region` 参数。
- [x] 给 `join-cluster.sh` 增加 `--public-ip` 参数。
- [x] 给 `join-cluster.sh` 增加 `--edge-group` 参数。
- [x] 给 `join-cluster.sh` 增加 `--edge-workload` 参数，默认 edge-only 使用 `dynamic`。
- [x] 增加多源 GeoIP fallback。
- [x] 增加控制平面 GeoIP/metadata 探测入口。
- [x] 当 GeoIP 失败但用户显式指定 country 时允许继续。
- [x] 当缺少 country 时把节点标为 `missing_location`，不启动 dynamic public edge。
- [x] edge-only labels 中加入 `fugue.io/edge-workload=dynamic`。
- [x] edge-only labels 保证不包含 DNS / app runtime / builder / shared pool 角色。
- [x] 为 join 脚本新增单元测试或脚本渲染测试。

### C. node updater

- [x] node updater desired state 增加 location labels reconcile。
- [x] node updater desired state 增加 `fugue.io/edge-workload=dynamic` reconcile。
- [x] node updater 支持 public IP 变更检测和更新。
- [x] node updater 支持 node-scoped edge credential 下发。
- [x] node updater 写入 `/etc/fugue/edge-node.env`，权限 root-only。
- [x] node updater 发现 edge credential 变化后能安全触发 edge pod reload/restart。
- [x] 增加 node updater 测试，覆盖 edge-only dynamic 节点。

### D. 控制平面 edge inventory

- [x] 定义 dynamic edge desired-state response。
- [x] 控制平面根据 node labels 自动 upsert edge group。
- [x] 控制平面支持 `edge-group-country-<country_code>` 泛化命名。
- [x] 控制平面支持 missing location 状态和可解释错误。
- [x] 控制平面记录 workload mode：`static` / `dynamic`。
- [x] 控制平面记录 canary state 和 canary weight。
- [x] 控制平面记录 public probe 状态。
- [x] 控制平面记录 dynamic edge credential 绑定。
- [x] 为 edge group upsert 和 desired state 增加 store 测试。

### E. OpenAPI / API

- [x] 在 `openapi/openapi.yaml` 中新增 desired-state endpoint。
- [x] 在 `openapi/openapi.yaml` 中新增 edge probe endpoint。
- [x] 在 `openapi/openapi.yaml` 中新增 edge canary/drain/undrain endpoint。
- [x] 执行 `make generate-openapi`。
- [x] 确认 generated route/spec drift 已更新。
- [x] 增加 API handler 测试。
- [x] 如 fugue-web 消费这些 API，同步前端契约产物。

### F. Helm dynamic edge workload

- [x] 在 `values.yaml` 增加 `edge.dynamic.enabled`。
- [x] 在 `values.yaml` 增加 `edge.dynamic.nodeSelector`。
- [x] 在 `values.yaml` 增加 `edge.dynamic.credentialHostPath`。
- [x] 新增 dynamic edge-front DaemonSet。
- [x] 新增 dynamic edge worker-a DaemonSet。
- [x] 新增 dynamic edge worker-b DaemonSet。
- [x] dynamic DaemonSet 只匹配 `fugue.io/edge-workload=dynamic`。
- [x] dynamic DaemonSet toleration 支持 `fugue.io/dedicated=edge`。
- [x] dynamic DaemonSet 不匹配 DNS-only 节点。
- [x] dynamic DaemonSet 不创建 DNS pod。
- [x] dynamic front 继续使用 80/443 hostPort。
- [x] dynamic workers 继续使用 blue/green worker 端口。
- [x] 增加 Helm chart 测试，证明新增 dynamic workload 不影响现有 US/DE 静态 DaemonSet。

### G. Edge runtime

- [x] edge 支持读取 `/etc/fugue/edge-node.env`。
- [x] edge 支持 node-scoped token heartbeat。
- [x] edge 支持 desired-state API。
- [x] edge 在 dynamic 模式下不要求静态 `FUGUE_EDGE_GROUP_ID`。
- [x] edge 缓存 desired state，控制平面短暂不可用时使用 last-known-good。
- [x] edge desired state 变化时安全 reload route/caddy 配置。
- [x] edge heartbeat 上报 workload mode、edge group、public IP、TLS/cache/route 状态。
- [x] 增加 edge runtime 单元测试。

### H. DNS / route bundle

- [x] DNS candidate 从 dynamic edge inventory 读取 eligible nodes。
- [x] route-ready gate 支持 dynamic edge group。
- [x] service-level exclusion 在 dynamic candidate 上继续 hard gate。
- [x] 新 edge 初始只进入 canary，不直接 active。
- [x] DNS answer explanation 显示 canary state、hard gate 和 exclusion reason。
- [x] DNS bundle 继续兼容静态 edge groups。
- [x] 增加 DNS answer-check 测试，覆盖 dynamic JP edge。
- [x] 增加 service exclusion 测试，覆盖 dynamic edge 被排除。

### I. Canary / quality ranking

- [x] 新 edge 状态机实现 joined -> warming -> probing -> canary -> active。
- [x] public probe 覆盖 80、443、TLS、route bundle、healthz。
- [x] canary 默认权重 1%。
- [x] canary 权重上限默认 5%，除非显式提升。
- [x] 样本不足时保持 canary 或降权，不进入 active。
- [x] quality ranking 以 `edge_id` 单独聚合 dynamic edge 指标。
- [x] request body read bps / min window bps / read error rate 进入 node-level 质量视图。
- [x] TCP RTT / retrans / RTO / lost 指标进入 scoped 质量视图。
- [x] CLI 输出 ranking breakdown。

### J. CLI

- [x] `fugue admin edge nodes ls` 显示 workload mode。
- [x] `fugue admin edge nodes ls` 显示 DNS eligible。
- [x] `fugue admin edge nodes get <node>` 显示 desired edge group。
- [x] 新增 `fugue admin edge node desired-state <node>`。
- [x] 新增 `fugue admin edge node probe <node>`。
- [x] 新增 `fugue admin edge node canary set <node> --weight <n>`。
- [x] 新增 `fugue admin edge node drain <node>`。
- [x] 新增 `fugue admin edge node undrain <node>`。
- [x] CLI 对 missing location / workload not scheduled 给出明确修复建议。

### K. 测试和发布

- [x] `make test` 通过。
- [x] Helm chart tests 通过。
- [x] OpenAPI generated drift check 通过。
- [x] 在 staging 或 shadow 模式验证 dynamic DaemonSet 不影响 US/DE。
- [ ] 推送到 `main` 触发 GitHub Actions 控制平面发布。
- [ ] 监控 deploy-control-plane workflow 成功。
- [ ] 验证线上 API / controller Ready。
- [ ] 验证现有 US/DE edge 仍 healthy。
- [ ] 验证现有 DNS answer 未异常变化。
- [ ] 给 `dmit` 补齐 dynamic labels / credentials。
- [ ] 验证 `dmit` dynamic edge pods Ready。
- [ ] 验证 `dmit` 不运行 DNS pod。
- [ ] 验证 `dmit` 进入 canary。
- [ ] 观察 `dmit` 真实请求质量指标。

## 最终完成定义

本改造只有在以下条件全部满足时才算完成：

- 新建任意国家 edge 节点不需要修改 Helm values。
- 新建任意国家 edge 节点不需要提交地区特例代码。
- `join-cluster.sh --edge-only` 一次性完成可观测的 edge 接入。
- edge-only 节点不会被误设为 DNS。
- 控制平面自动归组并解释归组结果。
- dynamic edge workload 自动调度。
- 新 edge 默认 canary，不直接全量。
- DNS answer 可解释地包含或排除新 edge。
- 服务级 exclusion 对 dynamic edge 生效。
- 现有 US/DE 静态 edge 在迁移期间无中断。
