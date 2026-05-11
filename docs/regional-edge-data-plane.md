# Fugue 区域边缘数据平面重构方案

本文档描述 Fugue 将公共应用流量从控制平面热路径中拆出的目标架构和渐进式重构步骤。

当前 Route A 实现适合作为早期启动路径，但它把公共应用流量和控制平面入口耦合在一起。现在一个公共应用域名请求会先到 Caddy edge，再进入 `fugue-api`，然后由 `fugue-api` 反向代理到应用的 Kubernetes Service。如果用户在中国，控制平面在美国，应用运行在新加坡，请求路径可能变成：

```text
中国用户
  -> 美国 Route A edge / fugue-api
  -> 新加坡节点 / Service / Pod
  -> 美国 Route A edge / fugue-api
  -> 中国用户
```

目标架构是把控制面和数据面拆开：

```text
中国用户
  -> 亚洲 edge
  -> 新加坡节点 / Service / Pod
  -> 亚洲 edge
  -> 中国用户
```

控制平面继续负责路由分配、证书策略、域名验证、runtime 状态、健康状态和流量策略。真正用户流量默认进入离用户或业务 runtime 更近的区域 edge，而不是美国控制平面。

## 目标

- 保持现有公共应用域名可用，不要求业务迁移到新的 app base domain。
- 保持已有自定义域名可用，客户侧 CNAME target 例如 `d-xxxx.dns.fugue.pro` 保持稳定。
- 默认公共应用请求路径中移除 `fugue-api` 反代。
- 支持靠近用户或业务 runtime 的区域 edge。
- 支持 managed-shared、managed-owned、user-owned 节点和 external-owned runtime。
- 支持通过本地 edge、tunnel、mesh、regional gateway 或未来 anycast 承载业务流量。
- 控制平面短暂不可用时，edge 和 DNS 仍能基于最后一份有效配置继续服务已有路由。
- 迁移期保留当前 Route A 路径作为 legacy fallback 和诊断入口。

## 第一阶段非目标

- 第一阶段不做完整 CDN。
- 第一阶段不做缓存规则、purge API、WAF、图片优化、多级缓存或 origin shield。
- 不要求用户可见 hostname 变更。
- edge 不自动执行 app failover，除非应用显式配置了 failover 或 routing policy。

## 目标职责拆分

### 控制平面

控制平面拥有持久产品状态和策略：

- 应用 route 归属
- 自定义域名归属和验证
- TLS allowlist 和证书策略
- runtime inventory 和健康状态
- edge inventory 和健康状态
- DNS node inventory 和健康状态
- route binding policy
- 生成 edge route bundle
- 生成 DNS bundle
- DNS 委托 preflight 和回滚计划
- 审计事件和运维 API

控制平面不再作为默认公共应用反向代理。

### 区域 edge

新增 `fugue-edge` 组件，负责区域内公共流量：

- 为平台域名和自定义域名终止 TLS
- 保留本地 route bundle cache
- 保留本地 TLS allowlist cache
- 根据 `Host` / SNI 路由到正确 app upstream
- 支持 WebSocket、SSE、上传和流式响应
- 向控制平面上报 route sync、TLS 和 upstream 健康状态
- 控制平面不可用时继续使用最后一份有效 bundle 服务已有路由

第一版建议做成 Go 进程管理 Caddy admin API，或 Go 进程内嵌反向代理。更推荐先走 Caddy-backed 形态，因为当前 Route A 已经依赖 Caddy 处理 wildcard hostname、streaming 行为和 custom-domain TLS。

### Runtime / App upstream

edge 到 app upstream 应尽量走本地或私有路径：

- edge 和业务在同一 Kubernetes 集群或相邻网络时，直接走 in-cluster Service DNS
- external-owned runtime 通过 mesh IP 或私有 overlay endpoint
- user-owned 节点可运行本地 regional gateway
- 受保护公网 endpoint 只作为 fallback

如果应用已经运行在亚洲，避免亚洲 edge 再回美国 origin。

## Route 模型

现有 `AppRoute` 只描述 public hostname、base domain、public URL 和 service port，不足以表达区域数据面路由。需要新增显式 route binding。

推荐 route binding 字段：

```text
hostname
app_id
tenant_id
runtime_id
runtime_type
runtime_edge_group_id
runtime_cluster_node
edge_group_id
policy_edge_group_id
route_policy
upstream_kind
upstream_scope
upstream_url
service_port
route_generation
status
fallback_edge_group_id
created_at
updated_at
```

初期可以从现有 app state 派生 route binding；长期 edge 数据面应该以 route binding 作为权威契约。

`route_policy` 必须显式表达该 hostname 是否允许进入某个 `edge_group`：

```text
route_policy=route_a_only   默认状态，只走现有 Route A / fugue-api app proxy
route_policy=edge_canary    只允许指定 edge_group 接这个 hostname
route_policy=edge_enabled   允许指定 edge_group 承接生产流量
```

如果 app 所在区域或 runtime 没有可用 edge group，默认保持 `route_a_only`，不能自动借用其他区域 edge 承接生产流量。跨区域 edge 只适合作为显式配置的应急 fallback 或人工测试路径，不能作为默认迁移行为。

`runtime_edge_group_id` 是由 runtime 位置派生出的本地数据面归属；优先使用 runtime labels，缺失时使用 runtime 绑定的 Kubernetes Node labels，例如 `fugue.io/location-country-code=us`。`policy_edge_group_id` 是 hostname 级 canary policy 的目标。二者必须一致，且目标 edge group 必须有健康 edge 成员，route 才能从 `route_a_only` 进入 `edge_canary` / `edge_enabled`。这条规则防止“入口在美国，upstream 又随机绕到远端 runtime”的伪 canary。

2026-05-11 线上状态：hostname 级 `EdgeRoutePolicy` 已随控制平面版本 `d213572e943980aa4e119ac76788cf74fd3933fc` 发布。控制面从 `AppRoute` / verified `AppDomain` 派生 route binding 后，会叠加 policy；默认 `route_policy=route_a_only`，只有显式设置为 `edge_canary` 或 `edge_enabled` 且绑定具体 `edge_group_id` 的 hostname 才允许进入 regional edge。`fugue-edge` 的 Caddy dynamic config 和直接 proxy 都会跳过 `route_a_only` route。

## Edge 模型

新增一等 edge inventory：

```text
edge_id
edge_group_id
region
country
public_hostname
public_ipv4
public_ipv6
mesh_ip
status
healthy
draining
last_seen_at
last_heartbeat_at
route_bundle_version
dns_bundle_version
caddy_route_count
caddy_applied_version
caddy_last_error
cache_status
last_error
created_at
updated_at
```

`edge_group` 是 DNS 和 route policy 的选择单位，单个 edge node 是该 group 下的健康和容量成员。

每个要切流的区域都应先具备对应 edge group；没有 edge group 的区域继续走 Route A。例如只有某个区域具备健康 edge 时，只有该区域 runtime 上的低风险 hostname 可以 opt-in 到对应 edge group；其他区域 runtime 上的 app 仍应继续走控制平面旧入口，除非显式配置跨区 fallback。

2026-05-12 线上状态：持久化 edge / edge group 模型、typed edge inventory API、edge heartbeat API、edge-scoped token 和 admin CLI 已随控制平面版本 `48b82de510633f653279d061016e27dd34b77360` 发布。控制面可通过 `GET /v1/edge/nodes`、`GET /v1/edge/nodes/{edge_id}` 查询 edge group 是否有健康成员；`fugue-edge` 会周期性 `POST /v1/edge/heartbeat` 上报 bundle version、Caddy apply/cache 状态和健康状态。美国和德国 edge 节点已切到 scoped token，`last_seen_at` / `last_heartbeat_at` 会持续更新。

示例：

```text
edge-group-asia-hk
edge-group-asia-sg
edge-group-us
edge-group-eu
```

## 统一 join-cluster 节点接入规划

区域 edge 和 DNS 节点可以优先沿用现有 `join-cluster.sh` 接入方式。也就是说，新增机器先作为中心 k3s 集群的 agent node 加入，然后由控制平面决定它承担什么角色。

这个模型里要区分两件事：

```text
join cluster = 机器进入 Fugue 可管理范围
承担角色 = 控制平面通过 NodePolicy / labels / taints / workload 调度决定
```

不要把所有跨区域节点都变成 control-plane / etcd 节点。大多数区域机器应作为 k3s agent node 加入，只接受调度和节点策略。

### 节点角色

每台 joined node 可以有一个或多个角色：

```text
app-runtime      跑租户 app
shared-pool      可加入共享池
edge             跑 fugue-edge / Caddy，监听 TCP 80 / 443
dns              跑 fugue-dns，监听 UDP 53 / TCP 53
builder          跑构建任务
registry-mirror  本地镜像缓存，可选
storage          区域存储角色，可选
```

控制平面持久化 desired role，然后 controller reconcile 到 Kubernetes：

```text
Fugue NodePolicy
  -> Kubernetes Node labels / taints
  -> DaemonSet / Deployment nodeSelector / affinity / tolerations
  -> 对应角色组件被调度到目标节点
```

NodePolicy 必须能在扩大 edge / DNS / runtime 节点前被直接审计：

```text
fugue admin cluster node-policy ls
fugue admin cluster node-policy get <node>
fugue admin cluster node-policy status
```

这些命令应显示 desired role、实际 Kubernetes labels / taints、`Ready` / `DiskPressure` gate，以及当前 reconcile 是否收敛。

示例：

```text
edge-hk-1:
  fugue.io/role.edge=true
  fugue.io/role.dns=true
  fugue.io/schedulable=true
  fugue.io/region=asia-hk
  fugue.io/edge-group=edge-group-asia-hk
  taint: fugue.io/dedicated=edge:NoSchedule

sg-app-1:
  fugue.io/role.app-runtime=true
  fugue.io/schedulable=true
  fugue.io/region=asia-sg
  fugue.io/runtime-id=runtime_sg_xxx
  taint: fugue.io/tenant=<tenant-id>:NoSchedule
```

### Edge 和 DNS 合并部署

第一阶段可以把 edge 和 authoritative DNS 放在同一批区域节点上：

```text
edge-hk-1:
  fugue-edge / Caddy  TCP 80 / 443
  fugue-dns           UDP 53 / TCP 53
  route bundle cache
  DNS bundle cache
  certificate cache

edge-sg-1:
  fugue-edge / Caddy  TCP 80 / 443
  fugue-dns           UDP 53 / TCP 53
  route bundle cache
  DNS bundle cache
  certificate cache
```

Cloudflare 父区可指向这些节点：

```text
ns1.dns.fugue.pro.   A     <edge-hk-1-public-ip>     DNS only
ns2.dns.fugue.pro.   A     <edge-sg-1-public-ip>     DNS only

dns.fugue.pro.       NS    ns1.dns.fugue.pro.        DNS only
dns.fugue.pro.       NS    ns2.dns.fugue.pro.        DNS only
```

同机部署时，端口不冲突：

```text
fugue-dns  -> UDP 53 / TCP 53
Caddy      -> TCP 80 / TCP 443
fugue-edge -> 127.0.0.1 或私网管理端口
```

生产最少两台，最好三台，且跨区域。单台机器故障会同时损失一个 authoritative DNS 节点和一个 regional edge 节点，因此不能单节点承载生产流量。

### Kubernetes 部署方式

如果 edge / DNS 节点已经 join cluster，优先用 DaemonSet 部署角色组件。默认 shadow 模式不暴露公网端口；只有进入显式 canary 或生产承载时，才打开 hostPort 53 / 80 / 443：

```text
fugue-dns DaemonSet:
  nodeSelector: fugue.io/role.dns=true, fugue.io/schedulable=true
  tolerations: fugue.io/dedicated=dns 或 fugue.io/dedicated=edge
  shadow: UDP/TCP 127.0.0.1:5353
  canary/production: hostPort UDP 53 / TCP 53

fugue-edge / Caddy DaemonSet:
  nodeSelector: fugue.io/role.edge=true, fugue.io/schedulable=true
  shadow: localhost Caddy admin/data ports
  canary/production: hostPort TCP 80 / TCP 443
```

2026-05-12 当前状态：已在两个 joined node 上做 edge + DNS shadow / direct query 验证。美国节点 `vps-591f4447` 属于 `edge-group-country-us`，DNS answer IP 为 `15.204.94.71`；德国节点 `vps-84c8f0a9` 属于 `edge-group-country-de`，DNS answer IP 为 `51.38.126.103`。两台节点都由 NodePolicy 标记为 `fugue.io/role.edge=true`、`fugue.io/role.dns=true` 和 `fugue.io/schedulable=true`，并通过 `fugue.io/dedicated=edge:NoSchedule` 隔离普通调度。两台节点的 `fugue-dns` 都打开 UDP 53 / TCP 53 hostPort，`dig @15.204.94.71 ...` 和 `dig @51.38.126.103 ...` 均已直接验证。DNS inventory / heartbeat 和 `fugue admin dns status` preflight 已上线，当前 preflight 在两个健康 DNS 节点上通过，并输出 `ns1` / `ns2` A 记录、`dns.fugue.pro` NS 记录和回滚删除计划。CLI 已补 `fugue admin dns delegation plan|apply|rollback`，默认 dry-run，写入前强制 preflight pass；这仍不代表 `dns.fugue.pro` 已委托，生产委托前还需要继续观察双节点并用工具化 rollback 做演练。

同一美国 edge 节点也已进入单 hostname public canary：部署变量显式打开 edge Caddy hostPort 80 / 443，Caddy listen 为 `:443`，TLS 为 public on-demand。该能力只服务显式 `edge_canary` / `edge_enabled` 且通过本地 edge group 防线的 hostname；wildcard、`api.fugue.pro` 和 `dns.fugue.pro` 仍不进入 regional edge。

德国 edge 当前只作为 shadow / DNS direct query 节点运行，`edge-group-country-de` 健康但 Caddy route count 为 `0`；尚未给德国 edge 设置任何 hostname exact DNS canary，也未让德国 runtime 默认借用美国 edge。

这让控制平面可以通过 NodePolicy 调整角色，同时让公网 DNS 和 HTTPS 流量直接打到节点公网 IP。

### 自治和逃生口

这个模型会把 edge / DNS 的生命周期部分绑定到中心 k3s。需要接受并缓解以下风险：

- 控制平面 API 或 kubelet 连接异常时，已运行 Pod 通常继续服务，但新调度和配置更新会受影响。
- 节点重启且暂时连不上控制平面时，edge / DNS 重新拉起可能变慢。
- 如果 edge / DNS 与普通租户 app 混跑，资源争抢会直接影响入口流量。

因此必须保留：

- `fugue-edge` 本地 route bundle cache
- `fugue-dns` 本地 DNS bundle cache
- Caddy certificate cache 持久化
- edge / DNS 节点的 taint 和资源 request / limit
- 至少两台可服务的 DNS / edge 节点
- `fugue-dns` systemd 独立运行逃生口
- `fugue-edge` systemd 独立运行逃生口

长期设计上，`fugue-edge` 和 `fugue-dns` 必须既能作为 Kubernetes workload 运行，也能脱离 Kubernetes 以 systemd 方式运行。这样某个区域无法或不适合 join 中心 k3s 时，仍可作为独立 regional edge 接入控制平面拉 bundle。当前 `fugue-dns` 已有 `scripts/render_fugue_dns_systemd_unit.sh` 可渲染 `fugue-dns.service` 和不含 token 的环境文件；`fugue-edge` 已补 `scripts/render_fugue_edge_systemd_unit.sh`，可生成 `fugue-edge.service` 和 `fugue-edge.env`，token 仍放在单独 secret env file 中。该脚本支持 route cache、Caddy config/cache 目录、edge group、公网 IP、Caddy listen/TLS mode，并有 shell test 覆盖参数校验和 token 不落盘。

## Route Bundle API

新增控制平面 endpoint，供区域 edge 拉取允许服务的路由：

```http
GET /v1/edge/routes?edge_id=<edge-id>
GET /v1/edge/routes?edge_group_id=<edge-group-id>
```

响应示例：

```json
{
  "version": "routegen_db340e63f06e153b",
  "generated_at": "2026-05-06T00:00:00Z",
  "edge_group_id": "edge-group-asia-sg",
  "routes": [
    {
      "hostname": "demo.fugue.pro",
      "route_kind": "platform",
      "app_id": "app_123",
      "tenant_id": "tenant_123",
      "runtime_id": "runtime_sg",
      "runtime_type": "managed-shared",
      "runtime_edge_group_id": "edge-group-asia-sg",
      "runtime_cluster_node": "sg-app-1",
      "edge_group_id": "edge-group-asia-sg",
      "policy_edge_group_id": "edge-group-asia-sg",
      "route_policy": "edge_canary",
      "upstream_kind": "kubernetes-service",
      "upstream_scope": "local-service",
      "upstream_url": "http://app-123.fg-tenant.svc.cluster.local:3000",
      "service_port": 3000,
      "tls_policy": "platform",
      "streaming": true,
      "status": "active",
      "route_generation": "routegen_..."
    },
    {
      "hostname": "www.customer.com",
      "route_kind": "custom-domain",
      "app_id": "app_123",
      "tenant_id": "tenant_123",
      "runtime_id": "runtime_sg",
      "runtime_type": "managed-shared",
      "runtime_edge_group_id": "edge-group-asia-sg",
      "runtime_cluster_node": "sg-app-1",
      "edge_group_id": "edge-group-asia-sg",
      "policy_edge_group_id": "edge-group-asia-sg",
      "route_policy": "edge_canary",
      "upstream_kind": "kubernetes-service",
      "upstream_scope": "local-service",
      "upstream_url": "http://app-123.fg-tenant.svc.cluster.local:3000",
      "service_port": 3000,
      "tls_policy": "custom-domain",
      "streaming": true,
      "status": "active",
      "route_generation": "routegen_..."
    }
  ],
  "tls_allowlist": [
    {
      "hostname": "www.customer.com",
      "app_id": "app_123",
      "status": "verified"
    }
  ]
}
```

要求：

- 支持 `ETag` 或等价 route generation token
- 支持 conditional fetch
- route generation / `ETag` 只基于稳定的 `routes` 和 `tls_allowlist` 内容计算，不包含 `generated_at` 或随机值
- 要求 edge 专用认证
- 每个 edge 将最新成功 bundle 写入本地磁盘
- 控制平面不可用时从本地磁盘 cache 恢复
- 迁移期保留现有 `/v1/edge/domains`，但长期以 route bundle 作为数据面 API

### EdgeRoutePolicy 管理 API

hostname 级 edge opt-in 由 platform admin 管理，不由租户 API key 或 edge token 修改：

```http
GET    /v1/edge/route-policies
GET    /v1/edge/route-policies/{hostname}
PUT    /v1/edge/route-policies/{hostname}
DELETE /v1/edge/route-policies/{hostname}
```

`PUT` 请求示例：

```json
{
  "edge_group_id": "edge-group-country-us",
  "route_policy": "edge_canary"
}
```

语义：

- `route_a_only`：默认值，不进入 edge Caddy public listener，继续走 Route A。
- `edge_canary`：只允许指定 `edge_group_id` 的 edge 承接该 hostname，用于人工或 exact DNS canary。
- `edge_enabled`：允许指定 `edge_group_id` 承接生产流量。
- 删除 policy 会让 hostname 回到 `route_a_only`，作为 canary 回滚路径的一部分。

CLI：

```bash
fugue admin edge route-policy ls
fugue admin edge route-policy set <hostname> --edge-group <edge-group-id> --policy edge_canary
fugue admin edge route-policy delete <hostname>
```

## Edge 认证

不要复用 tenant API key。

当前已落地的认证方式：

- 每个 edge 或 edge group 一个 scoped edge token

后续可选增强：

- 每个 edge 一张 mTLS 证书
- edge 运行在可信集群内时使用 workload identity

edge 身份只允许：

- 拉取自己 edge / edge group 的 route bundle
- 上报 edge health
- 上报 TLS status
- 上报 route sync status

edge 身份不能修改租户 app。

## DNS 架构

DNS 需要处理两类域名：

- 平台应用域名，例如 `foo.fugue.pro`
- 自定义域名 CNAME target，例如 `d-xxxx.dns.fugue.pro`

现有应用 hostname 必须保持稳定。新架构改变的是这些 hostname 背后的解析和入口，不是用户看到的 URL。

### 自定义域名 target 流程

客户 DNS：

```text
www.customer.com CNAME d-abc123.dns.fugue.pro
```

解析链路：

```text
www.customer.com
  -> CNAME d-abc123.dns.fugue.pro
  -> Fugue 自建权威 DNS 返回某个区域 edge IP
```

浏览器请求：

```text
TLS SNI: www.customer.com
HTTP Host: www.customer.com
TCP target: d-abc123.dns.fugue.pro 解析出的区域 edge IP
```

因此 edge route bundle 必须包含 `www.customer.com`，不能只包含 `d-abc123.dns.fugue.pro`。`d-xxxx` 是稳定 DNS routing target，不是应用 Host header。

### 自建 `dns.fugue.pro` 权威 DNS

Fugue 应自建 `dns.fugue.pro` 的 authoritative DNS，让每个 `d-xxxx` target 能按 app、runtime、edge health 和 policy 解析。

DNS 服务只做 authoritative，不做 recursive resolver。

推荐实现：

- `fugue-dns` Go 服务，基于 `github.com/miekg/dns`
- 或 CoreDNS + Fugue plugin

运行方式：

```text
fugue-dns
  -> 周期性从控制平面拉 DNS bundle
  -> 将最新成功 bundle 写入本地磁盘
  -> DNS query 只查本地内存
  -> 控制平面不可用时从本地磁盘 cache 回答
```

当前已落地的控制面闭环：

```http
GET  /v1/dns/nodes
GET  /v1/dns/nodes/{dns_node_id}
POST /v1/dns/heartbeat
GET  /v1/dns/delegation/preflight
```

CLI：

```bash
fugue admin dns nodes ls
fugue admin dns nodes get <dns-node-id>
fugue admin dns status
fugue admin dns delegation plan
fugue admin dns delegation apply --confirm
fugue admin dns delegation rollback --confirm
```

`fugue-dns` 周期性 heartbeat 会上报 `dns_node_id`、`edge_group_id`、公网 IP、zone、DNS bundle version、record count、cache 状态、UDP/TCP listen 状态、query/error counters 和 `last_seen_at`。`fugue admin dns status` 是只读委托 preflight：检查至少两个 healthy DNS 节点、UDP/TCP 53 可达、`d-test.dns.fugue.pro` 回答预期 IP、bundle version 在同一 edge group 内稳定、cache error 为 0、节点 `Ready=True` 且 `DiskPressure=False`，并输出计划添加和回滚删除的父区记录。`fugue admin dns delegation apply` / `rollback` 默认 dry-run；只有传 `--confirm` 才会调用 Cloudflare API，并且只允许改 `ns1.dns.fugue.pro`、`ns2.dns.fugue.pro` 的 glue A/AAAA 和 `dns.fugue.pro` 的 NS，不触碰 wildcard、`api.fugue.pro` 或其他父区记录。

查询处理：

```text
A d-abc123.dns.fugue.pro
  -> 在本地 bundle 找 d-abc123
  -> 找 app runtime 和 route policy
  -> 过滤健康 edge group
  -> 返回被选中的 edge group A/AAAA
```

第一版策略保持简单：

- app runtime 在亚洲时返回亚洲 edge group
- app runtime 在美国时返回美国 edge group
- 首选 edge group 不健康时返回 fallback edge group
- target 不存在时返回 NXDOMAIN
- TTL 使用 60 到 300 秒

后续再加入 client geography、EDNS Client Subnet、latency scoring、weighted records 和 anycast。

### Cloudflare 父区委托

当前 `fugue.pro` 托管在 Cloudflare。这个前提和自建 `dns.fugue.pro` 不冲突。

第一阶段建议继续让 Cloudflare 托管 `fugue.pro` 父区，只在 Cloudflare 里把 `dns.fugue.pro` 委托给 Fugue 自己的 nameserver。

在 Cloudflare 的 `fugue.pro` zone 里添加：

```text
ns1.dns.fugue.pro.   A     <dns-node-1-public-ip>     DNS only
ns2.dns.fugue.pro.   A     <dns-node-2-public-ip>     DNS only

dns.fugue.pro.       NS    ns1.dns.fugue.pro.         DNS only
dns.fugue.pro.       NS    ns2.dns.fugue.pro.         DNS only
```

这些 `A` 和 `NS` 记录必须是 DNS only，不能走 Cloudflare proxy。

委托写入应通过受控 CLI，而不是临场手工点击：

```bash
fugue admin dns delegation plan
fugue admin dns delegation apply --confirm --cloudflare-env-file /path/to/cloudflare.env
fugue admin dns delegation rollback --confirm --cloudflare-env-file /path/to/cloudflare.env
```

`plan` 和不带 `--confirm` 的 `apply` / `rollback` 都是 dry-run。`apply --confirm` 前会强制要求 preflight 通过、至少两个 DNS 节点 healthy、cache write/load error 为 0，并再次校验变更范围只包含 `ns1.dns.fugue.pro`、`ns2.dns.fugue.pro` 和 `dns.fugue.pro NS`。

委托完成后，`d-xxxx.dns.fugue.pro` 由 Fugue 自己的 DNS 节点回答，不再由 Cloudflare 或 Google Cloud DNS 回答。

如果以后完全离开 Cloudflare，需要在域名注册商处把 `fugue.pro` 的 nameserver 改成 Fugue 自己的 nameserver，并配置 glue records：

```text
fugue.pro.      NS    ns1.fugue.pro.
fugue.pro.      NS    ns2.fugue.pro.
ns1.fugue.pro.  A     <dns-node-1-public-ip>
ns2.fugue.pro.  A     <dns-node-2-public-ip>
```

### Google Cloud DNS 迁移

旧的 Google Cloud DNS `dns.fugue.pro` managed zone 不应继续作为动态 edge routing 的长期权威。类似下面的 wildcard 记录：

```text
*.dns.fugue.pro. A <legacy-route-a-ip>
```

只能把所有 target 固定到一个 IP，不能表达 per-app、per-runtime、per-edge policy。

迁移规则：

1. 先直接验证 `fugue-dns`：

   ```bash
   dig @<ns1-ip> d-test.dns.fugue.pro A
   ```

2. 在 Cloudflare 父区把 `dns.fugue.pro` 委托给 Fugue nameserver。

3. 验证公网递归链路：

   ```bash
   dig d-test.dns.fugue.pro A +trace
   ```

4. 确认 public trace 已经到达 Fugue nameserver 后，再退役 Google Cloud DNS 的 `dns.fugue.pro` zone。

委托迁移完成后，不要继续维护 Google Cloud DNS 里的 `dns.fugue.pro` 记录，因为它们不再是公网权威事实源。

### 平台应用域名

现有平台应用域名例如 `foo.fugue.pro` 不应改变。

有两条迁移路径：

1. 继续让 Cloudflare 托管 `fugue.pro`，通过 Cloudflare API 管理 exact app record 或 wildcard regional record。
2. 后续把整个 `fugue.pro` 也迁到 Fugue 自建权威 DNS。

如果只使用一个统一 wildcard：

```text
*.fugue.pro -> GeoDNS edge group
```

平台可以按用户区域选择 edge，但很难按 app runtime 做不同解析。如果需要 `foo.fugue.pro` 优先新加坡，`bar.fugue.pro` 优先美国，就需要 exact DNS record，或把 `fugue.pro` 也纳入 Fugue 自建权威 DNS。

推荐迁移方式：

- 保留现有 `*.fugue.pro` 行为作为 fallback
- 先给 canary app hostname 添加 exact record
- 逐步把更多 app hostname 切到 edge-aware record
- 等 `dns.fugue.pro` 稳定后，再评估是否迁移整个 `fugue.pro`

## Edge 到 Runtime 的连接方式

每类 runtime 选择一种 upstream connectivity。

### 同一 k3s 集群内的 managed-shared / managed-owned

最佳路径：

```text
regional edge
  -> 本地 Kubernetes Service DNS
  -> app Pod
```

如果 edge 跑在同一集群内或靠近集群网络，可以直接 target：

```text
http://<service>.<tenant-namespace>.svc.cluster.local:<port>
```

如果一个集群跨地域，必须避免 edge 误选远端 endpoint。route bundle 应优先表达 region-local Service、EndpointSlice、node label 或 local gateway。

### External-owned runtime

最佳路径：

```text
regional edge
  -> WireGuard / Tailscale / Headscale mesh
  -> runtime local gateway
  -> app Service
```

runtime agent 可以通过现有 runtime cell substrate 暴露本地 route metadata，但 edge 不应依赖 peer gossip 做权威路由决策。控制平面应把最终 upstream 写进 edge route bundle。

### User-owned 本地 edge

高流量 user-owned 节点可以支持本地 edge：

```text
customer domain
  -> Fugue DNS 返回 customer-local edge IP
  -> local edge
  -> local app Service
```

local edge 仍然向控制平面认证并接收 route bundle，但不需要运行完整控制平面。

### Anycast 后置

anycast 可以在 regional edge group 稳定后再加入。它不适合作为第一阶段迁移机制，因为它会增加路由、故障隔离和调试复杂度。

## TLS 和证书策略

### 平台域名

`fugue.pro` 下的平台应用域名优先使用 wildcard cert 或 DNS-01 managed certificate。

edge 应本地终止 TLS，不应为每次 TLS 判断实时请求美国控制平面。

### 自定义域名

控制平面验证所有权：

```text
www.customer.com CNAME d-abc123.dns.fugue.pro
```

验证成功后：

- 控制平面把 `www.customer.com` 放入 edge route bundle
- 控制平面把 `www.customer.com` 放入 TLS allowlist
- edge 本地签发或续期证书
- edge 向控制平面上报 TLS status

控制平面短暂不可用时，已验证自定义域名继续依赖 edge 本地 TLS allowlist 和证书 cache 服务。

## 故障行为

### 控制平面不可用

regional edge：

- 继续服务最后一份有效 route bundle
- 继续使用本地证书 cache
- 继续使用本地 TLS allowlist
- 将 health / TLS report 排队，等待控制平面恢复
- 不发明新 route
- 不验证新自定义域名

Fugue DNS：

- 继续用最后一份有效 DNS bundle 回答
- 在策略允许范围内返回 stale-but-known edge IP
- 不在每次 DNS query 时查询数据库

### Edge 不可用

DNS 在 edge health 过期后停止返回该 edge group。已有 DNS cache 会继续使用旧 IP 直到 TTL 过期，因此动态 target 的 TTL 应保持较低。

控制平面应将该 edge 标记为 unhealthy，并将后续 DNS 答案切到 fallback edge group。

### Runtime 不可用

除非应用配置了 failover 或 routing policy，否则 edge 对该 route 返回 upstream error。

edge 不应自己把流量转移到另一个 runtime。

## 可观测性

regional edge 必须上报：

- edge health
- route bundle version 和最近成功 sync 时间
- 每个自定义域名的 TLS issuance status
- per-route request count
- per-route status code
- per-route upstream latency
- upstream connection failure
- WebSocket / SSE upgrade success 与 failure
- fallback route hit
- bytes in / out

DNS 必须上报：

- bundle version
- query count by qtype
- NXDOMAIN count
- stale bundle serving
- selected edge group
- health-based fallback count

这些信号齐备后，才能迁移大范围生产流量。

## 安全要求

- Edge token 只授权 edge API。
- Route bundle 不包含 tenant secret。
- Edge log 不能泄露 Authorization header、cookie 或 app secret。
- Edge 到 runtime 优先走私有网络或 mesh。
- 公网 upstream 必须有 mTLS 或等价认证层。
- DNS server 只做 authoritative，必须拒绝 recursive query。
- 如果使用 Caddy admin API，只能绑定 localhost 或私有接口。

## 线上现状核查

线上 baseline 可能包含生产 IP、节点名、域名清单、app 清单、runtime ID 和内部 Service 地址，不应提交到版本库。

本仓库只保留脱敏结论：

- 当前默认生产热路径仍是 Route A：公网入口先进入现有 Caddy / control-plane API，再由 `fugue-api` app proxy 反代到业务 Service。只有少量显式 opt-in canary hostname 例外。
- 当前已经具备只读 route binding 派生、typed `/v1/edge/routes` route bundle API、稳定 `ETag` / conditional fetch、`fugue-edge` shadow DaemonSet、本地 route cache、health 和 metrics。
- 线上已经具备 hostname 级 `EdgeRoutePolicy`、`/v1/edge/route-policies` 管理 API 和 `fugue admin edge route-policy` CLI；默认 `route_a_only`，只有显式 `edge_canary` / `edge_enabled` opt-in 的 hostname 才会进入 edge Caddy / proxy。
- 当前 `fugue-edge` 仍是 shadow / canary workload：已支持 Caddy-backed dynamic config，并已在美国 edge 节点对单 hostname public canary 显式开放 80/443 hostPort；它不承接 wildcard 或默认生产流量。
- Caddy-backed edge 已发布到美国 edge 节点，Caddy admin 绑定 localhost；当前 public canary 使用 `FUGUE_EDGE_GROUP_ID=edge-group-country-us`、Caddy listen `:443` 和 public on-demand TLS。正式切流仍必须通过显式 hostname / route policy opt-in，并且必须通过 runtime edge group 本地防线。
- `fugue-dns` 已具备 typed `/v1/edge/dns` DNS bundle API、本地 DNS cache、authoritative-only DNS responder、health/metrics 和 Helm DNS DaemonSet；已在美国和德国两个 edge/DNS 节点上做直接 DNS shadow 验证。
- DNS inventory / heartbeat 已上线：`GET /v1/dns/nodes`、`GET /v1/dns/nodes/{dns_node_id}`、`POST /v1/dns/heartbeat`、`GET /v1/dns/delegation/preflight` 和 `fugue admin dns nodes ls|get|status` 可用于集中查看 DNS 节点健康、bundle/cache/query 状态和委托 preflight。
- 当前 `fugue admin dns status` 已在美国 / 德国两个 healthy DNS 节点上通过；preflight 会按 edge group 校验 DNS bundle version 稳定性，并输出 `ns1` / `ns2` A 记录、`dns.fugue.pro` NS 记录和回滚删除计划。实际 Cloudflare 父区记录尚未创建。
- NodePolicy 最小模型已经上线：控制面持久化 `app-runtime`、`shared-pool`、`edge`、`dns`、`builder`、`internal-maintenance` 等 desired role，并 reconcile 到 Kubernetes Node labels / taints。
- NodePolicy 可视化接口和 CLI 已补齐：`GET /v1/cluster/node-policies`、`GET /v1/cluster/node-policies/{name}`、`GET /v1/cluster/node-policies/status` 对应 `fugue admin cluster node-policy ls|get|status`，用于查看 desired role、实际 labels / taints、`Ready` / `DiskPressure` gate 和 reconcile drift。
- edge / edge group 持久模型、`/v1/edge/nodes` inventory API、`/v1/edge/heartbeat`、edge-scoped token 和 `fugue admin edge nodes` CLI 已发布到控制面；当前美国和德国 edge 节点都已使用 scoped token 上报健康状态，route bundle、Caddy apply 和 cache 状态均可从 admin CLI 读取。
- route binding 已补本地 upstream 防线：`edge_canary` / `edge_enabled` 只有在 policy 目标 edge group 与 runtime 派生 edge group 一致，且该 group 有健康 edge member 时才会生效；runtime edge group 从 runtime labels 派生，缺失时从绑定 Node labels 派生；managed-shared / managed-owned 使用 `upstream_scope=local-service` 的 in-cluster Service DNS，external-owned 先标记为 mesh upstream 未就绪。
- `fugue-edge` 观测已扩展：Caddy dynamic config 开启 JSON access log，edge proxy metrics 增加 fallback hit、WebSocket / SSE / streaming 成功率和 upload request 计数。
- 控制面现在会读取 Kubernetes Node condition，把 `Ready=False` 或 `DiskPressure=True` 的节点标为 `fugue.io/schedulable=false`、`fugue.io/node-health=blocked`，并加 `fugue.io/node-unhealthy=true:NoSchedule`；除 `node-janitor` 这类清理组件外，普通维护组件、edge、dns、runtime workload 都不应继续调度到异常节点。
- `fugue-edge` 和 `fugue-dns` 现在都要求对应 `fugue.io/role.*=true` 且 `fugue.io/schedulable=true`；edge / DNS 节点默认带 `fugue.io/dedicated` taint，初期允许同一节点同时承担 edge + DNS。为兼容已作为 runtime join 的美国 edge 节点，edge / DNS DaemonSet 也 tolerate 旧的 `fugue.io/tenant` taint，但仍必须先匹配显式 role 和健康 label。
- `dns.fugue.pro` 尚未从 Google Cloud DNS / Cloudflare 父区委托到 Fugue DNS；双节点 direct query 和只读 preflight 已通过，但仍处于 shadow 验证阶段，不作为生产权威 DNS。
- `dns.fugue.pro` 的现有公网权威和具体记录属于迁移前 baseline，具体值放在本地私有附录中。
- 第一阶段不能全局切 DNS，也不能替换现有入口。`cerebr.fugue.pro` 已作为第一个 hostname 级 `edge_canary` 进入美国 edge 内部 canary；`edge-protocol-canary.fugue.pro` 已作为第一个 public exact DNS canary 指向美国 edge，并验证 HTTPS、Host route、WebSocket、SSE、upload 和 streaming。当前 NodePolicy drift 和 `DiskPressure` 已收敛为 0；下一步应继续观察单 hostname public canary 和双节点 DNS shadow，不扩大 wildcard、默认流量或 `dns.fugue.pro` 委托。

本地私有附录路径：

```text
docs/private/regional-edge-current-state.local.md
```

该路径已通过 `.gitignore` 忽略，只用于本机排障和迁移前后对照。正式提交中不得包含生产 token、IP inventory、完整 app inventory、节点清单或内部 Service 地址。

## 迁移策略补充

当前迁移策略采用双入口并存，而不是一次性切换：

```text
旧路径:
  用户
    -> 现有 Route A / control-plane Caddy
    -> fugue-api app proxy
    -> app Service / Pod

新路径:
  用户或人工 canary
    -> regional edge / Caddy
    -> app Service / Pod
```

迁移期的关键边界：

- `api.fugue.pro`、controller、etcd、control-plane API 和控制平面核心服务不走 regional edge。
- 普通业务 Pod 可以逐步从 control-plane 节点迁到 agent/runtime 节点，但迁移 Pod 位置不等于自动切 edge。
- 是否走 edge 必须由 hostname / route policy 显式 opt-in 控制。
- opt-in 必须绑定到具体 `edge_group`；没有本区域或近区域 edge 的 app 继续走 Route A，不因为其他区域已有 edge 就自动跨区切流。
- 现有业务默认继续走 Route A，直到单个 hostname 通过 edge canary。
- 不全局切 `*.fugue.pro`，不修改 `dns.fugue.pro` 委托作为早期步骤。
- 任何 canary 失败时，应能删除 exact DNS record 或关闭 route policy，回到 Route A。

因此近期顺序是：

1. 保持 Route A fallback，继续观察美国和德国 edge / DNS shadow workload、edge inventory heartbeat，以及当前单 hostname public canary。
2. 用 `fugue admin cluster node-policy status` 持续确认 desired role / actual labels / health gate / reconcile drift；当前 `drifted=0`、`disk_pressure=0`，扩大 edge、DNS 或 runtime 节点前仍要短窗口复查。
3. 继续把控制面节点保持在低业务负载状态；普通业务迁移可以逐批推进，但控制面核心服务和 `api.fugue.pro` 不进入 regional edge 数据面。
4. 逐批把普通业务从 control-plane 节点迁出，避免控制面节点混跑业务 workload。
5. 继续观察 `cerebr.fugue.pro` 的 hostname 级内部 `edge_canary`：route bundle、Caddy host route、edge proxy metrics、cache/304 行为和 Route A fallback 都必须稳定。
6. 对没有本区域 edge 的 runtime，继续保持 Route A，直到该区域部署并观察通过 edge shadow；已有德国 edge 但尚未给德国 hostname 开 public canary。
7. 继续观察美国 / 德国两个 `fugue-dns` direct query 节点，确认 bundle sync、cache、UDP/TCP 53、Pod restart 和节点健康长期稳定。
8. 当前只保留已验证的单 hostname exact DNS canary；扩大到更多 hostname 前必须再次确认 edge health、route bundle、Caddy apply、edge metrics 和回滚路径。`dns.fugue.pro` 委托排在双节点 DNS 稳定观察、edge systemd 逃生口验证、DNS delegation apply/rollback dry-run 和回滚演练之后。

## 平滑重构 TODO List

下面 TODO 按顺序执行。每一步都应该能独立上线、验证和回滚，不要求一次性完成大迁移。

### 0. 冻结当前事实和保护 fallback

- [x] 记录当前 Route A 热路径：公网 DNS、Caddy、`fugue-api` Service、app proxy、业务 Service。
- [x] 确认所有现有 `*.fugue.pro` app hostname 和 custom domain 清单。
- [x] 确认每个 app 的 runtime、节点区域、route hostname、custom-domain target。
- [x] 明确当前 `fugue-api` app proxy 作为 legacy fallback 保留，不在早期删除。
- [x] 给当前 Route A 增加最小观测：请求量、502、upstream error、WebSocket/SSE 错误。

### 1. 统一 join-cluster 节点接入和 NodePolicy

- [x] 明确所有新增区域机器默认通过 `join-cluster.sh` 作为 k3s agent node 接入，不默认加入 control-plane / etcd。
- [x] 修复 Debian / systemd-resolved stub resolver 场景下 k3s/containerd 拉 GHCR 镜像失败的问题：join 脚本会把 `/etc/resolv.conf` 指向 `/run/systemd/resolve/resolv.conf`。
- [x] 实现 NodePolicy 最小模型，表达 `app-runtime`、`shared-pool`、`edge`、`dns`、`builder`、`internal-maintenance` 等角色。
- [x] 持久化 desired policy，并 reconcile 到 Kubernetes Node labels / taints。
- [x] 通过 Node condition 生成 `fugue.io/schedulable`、`fugue.io/node-health` 和 `fugue.io/node-unhealthy` 健康 gate。
- [x] 定义 edge / dns 节点的 role labels、dedicated taint、resource request / limit 和默认调度边界。
- [x] 设计并实现 `fugue-edge` / `fugue-dns` Helm DaemonSet 调度规则：必须匹配 role label 和健康 label；edge 默认不开放公网 80/443；DNS 只有显式开启 hostPort 时开放 53。
- [x] 增加 CLI 和 admin endpoint 查看节点角色、实际 labels / taints、`Ready` / `DiskPressure` gate 和 reconcile 状态：`fugue admin cluster node-policy ls|get|status`。
- [ ] 后续扩展 NodePolicy 角色：`registry-mirror`、`storage`、区域性网关等长期角色。
- [x] 为 `fugue-dns` 提供 systemd 逃生口渲染脚本，生成不含 token 的 env 文件和 `fugue-dns.service`。
- [x] 为 `fugue-edge` 补齐 systemd 逃生口，避免 edge 完全依赖 Kubernetes 才能启动；脚本生成不含 token 的 env 文件和 `fugue-edge.service`，token 从独立 secret env file 读取。

### 2. 建立 edge inventory 和认证模型

- [x] 在 OpenAPI 里设计 edge inventory、edge heartbeat、edge route pull 的 API 契约。
- [x] 新增 edge / edge group 数据模型和存储迁移。
- [x] 新增 edge-scoped token，不复用 tenant API key。
- [x] 新增 edge heartbeat，上报 region、public IP、mesh IP、route / DNS bundle version、Caddy apply/cache 状态和健康状态。
- [x] 增加 admin API 和 CLI 查看 edge 列表和健康状态：`fugue admin edge nodes ls`、`fugue admin edge nodes get <edge-id>`。
- [x] 发布到控制面并完成初始线上验证：美国 edge 使用 scoped token 上报，`last_seen_at` / `last_heartbeat_at` 持续更新，bundle API 仍保持稳定 `ETag`。
- [x] 完成 edge inventory 初始长时间观察：heartbeat 持续更新，scoped token 生效，route bundle 大量命中 `304 not_modified`，cache write/load error 为 0。
- [x] 第二个 edge group `edge-group-country-de` 已上线：德国 edge 节点 `vps-84c8f0a9` 使用 scoped token 上报 `region=europe`、`country=de`、`public_ipv4=51.38.126.103`，当前 healthy 且 Caddy route count 为 0。
- [ ] 在每次扩大 canary 前继续做短窗口复查：heartbeat、scoped token、route / DNS bundle sync、Caddy apply 和 Pod restart 都不能出现持续异常。
- [ ] 后续评估是否补 mTLS / workload identity，减少长期 secret token 暴露面。

### 3. 建立 route binding 派生层

- [x] 从现有 `AppRoute`、`AppDomain`、runtime 和 app spec 派生只读 route binding。
- [x] 不改变现有 app 创建、route patch、custom domain 流程。
- [x] 为每个 route binding 计算候选 edge group 和 fallback edge group。
- [x] 为每个 route binding 计算 edge 可用的 upstream URL。
- [x] 增加测试覆盖：platform route、custom domain、disabled app、internal/background app、runtime missing。

### 4. 新增 edge route bundle API

- [x] 在 `openapi/openapi.yaml` 新增 `/v1/edge/routes`。
- [x] 生成 OpenAPI artifacts。
- [x] 实现 route bundle handler，先只读派生 binding。
- [x] 支持 `edge_id` / `edge_group_id` 过滤。
- [x] 支持 route generation / `ETag` / conditional fetch。
- [x] route bundle version / `ETag` 只基于稳定 route / TLS allowlist 内容计算，不随 `generated_at` 或非数据面元数据漂移。
- [x] 确保 bundle 不包含 secret。
- [x] 增加 handler 和 contract tests。

### 5. 实现 fugue-edge shadow 模式

- [x] 新增 `cmd/fugue-edge`。
- [x] 实现 edge auth、拉取 route bundle、写本地磁盘 cache。
- [x] 实现本地 health endpoint 和 metrics。
- [x] 先不接公网流量，只验证 bundle sync。
- [x] 支持控制平面不可用时从本地 cache 启动。
- [x] 增加 `fugue-edge` shadow DaemonSet 和发布镜像链路，默认只调度到 `fugue.io/role.edge=true` 且 `fugue.io/schedulable=true` 的节点，不监听公网 80/443，不生成 Caddy config。
- [x] 增强 shadow 自观测：bundle sync/cache load/cache write counters、bundle age、sync duration 和结构化同步日志。
- [x] 在一台已 join cluster 且带 `fugue.io/role.edge=true` / `fugue.io/schedulable=true` 的美国候选节点部署 shadow edge，确认能稳定拉取 bundle 并大量命中 304。
- [x] 在德国 joined node `vps-84c8f0a9` 部署第二个 regional edge shadow workload，绑定 `edge-group-country-de`，确认 `/healthz` healthy、route bundle 走 `304 not_modified`、Caddy apply 成功且 route count 为 0。
- [ ] 在下一个目标 canary 区域的已 join cluster 节点上部署 shadow edge，例如亚洲 canary 前先部署亚洲 edge，并确认能长期拉取 bundle。
- [x] 完成初始 shadow 观察：`sync error` 未持续增长，cache write/load error 为 0，edge Pod 不重启，内容不变时 cache 不被反复重写。
- [ ] 扩大 hostname canary 前继续复查 edge `/healthz`、`/metrics`、route cache mtime 和 Caddy apply 结果。

### 5.5 控制平面减负和 Route A agent canary

- [x] 新增或确认美国 app-runtime agent node `ns101351`，只作为 k3s agent，不加入 control-plane / etcd。
- [x] 选择低风险普通业务 app `cerebr` 调度到 `ns101351`。
- [x] `cerebr` canary 仍走现有 Route A：`现有 Caddy -> fugue-api app proxy -> agent node app Pod`。
- [x] 验证 `cerebr` Pod 位置、Route A 502 / upstream error、重启恢复和回滚路径。
- [x] 对 edge Caddy dynamic config 补齐 per-request JSON access log；`cerebr` 静态站仍只能验证基础 HTTP/TLS/Host route，不能替代 WebSocket/SSE/upload 专项 app。
- [ ] 逐批将普通业务 workload 从 control-plane 节点迁到 agent/runtime 节点。
- [ ] 控制面核心服务、etcd、controller 和 `api.fugue.pro` 不迁到单台 agent，也不进入 regional edge 数据面。
- [ ] 明确业务 Pod 位于 agent 节点不自动代表走 edge；edge 入口必须由 hostname / route policy 显式 opt-in。

### 6. 接入 Caddy-backed 反代但仍不切 DNS

- [x] `fugue-edge` 根据 route bundle 生成 Caddy dynamic config。
- [x] Caddy admin API 只绑定 localhost。
- [x] Caddy sidecar / dynamic config 通过 Helm opt-in 开启，默认关闭；不加 hostNetwork、hostPort 或公网 80/443。
- [x] Caddy-backed shadow 已发布到美国 edge 节点；Caddy 数据面先只用于 shadow canary，不改 wildcard、不改 `api.fugue.pro`、不改 `dns.fugue.pro`。
- [x] 支持 platform hostname 和 custom domain Host route。
- [x] 支持 WebSocket、SSE、upload、stream response。
- [x] 加 Caddy access log、edge proxy per-route request count、status code、upstream latency、upstream error、fallback hit、WebSocket / SSE / streaming result 和 upload request metrics。
- [x] 新增 hostname 级 EdgeRoutePolicy，让 route 明确处于 `route_a_only`、`edge_canary` 或 `edge_enabled`。
- [x] Caddy dynamic config 只生成当前 edge group 被允许承接的 hostname；没有 opt-in 的 route 即使出现在 bundle 中也不能进入 public listener。
- [x] `fugue-edge` 直接 proxy 跳过 `route_a_only` route，避免绕过 Caddy gating。
- [x] `fugue-edge` Caddy 模式要求显式 `FUGUE_EDGE_GROUP_ID`，并在 Caddy config 和直接 proxy 两层本地过滤非本 edge group route。
- [x] 对没有对应 edge group 的 runtime，保持 Route A，不自动经由其他区域 edge 反代。
- [x] 发布包含 EdgeRoutePolicy 的控制面版本 `d213572e943980aa4e119ac76788cf74fd3933fc`，并在线上确认 `/v1/edge/route-policies` 和 OpenAPI 契约生效。
- [x] 发布包含 edge group 本地防线的控制面版本 `3c6f807f01389a09bb01c16f99b8f01faaea8610`，并在线上确认 edge DaemonSet 带 `FUGUE_EDGE_GROUP_ID=edge-group-country-us`。
- [x] 在线上设置第一个 `edge_canary` policy：`cerebr.fugue.pro -> edge-group-country-us`。
- [x] 确认 `cerebr.fugue.pro` route bundle / Caddy config / proxy gating 的实际 hostname 承接行为。
- [x] 用 `curl --resolve` 带正确 SNI 验证 Caddy-backed 内部 TLS canary，未 opt-in hostname 不进入 edge proxy。
- [x] 只有显式 opt-in 的 canary hostname 才能进入 edge 路径。
- [x] 通过正式发布链路接入 public canary 部署变量：`publicHostPorts.enabled=true`、Caddy listen `:443`、TLS `public-on-demand`。
- [x] 发布后确认美国 edge `15.204.94.71:443` 可服务，Caddy public on-demand TLS 能成功签发 canary hostname 证书。
- [x] 对 `edge-protocol-canary.fugue.pro` 做 public canary：Host route、WebSocket、SSE、upload、streaming 和 edge metrics 均已验证。

### 7. 建立 edge 到 runtime 的本地 upstream 路径

- [x] 对 managed-shared / managed-owned，在 route bundle 中明确 `upstream_scope=local-service`，使用 in-cluster Service DNS。
- [x] runtime edge group 优先从 runtime labels 派生，缺失时从绑定的 Kubernetes Node labels 派生，避免已标记美国节点的 runtime 因自身 labels 为空而被错误归入 default group。
- [x] route policy 目标 edge group 必须等于 runtime 派生 edge group，且该 edge group 有健康成员；否则 route 保持不可接流，继续 Route A。
- [x] 在 Caddy config 和 direct proxy 两层继续校验 `edge_group_id`、`runtime_edge_group_id`、`policy_edge_group_id` 与本地 edge group 一致，避免 edge 本地接入远端 runtime。
- [ ] 对 external-owned，验证 WireGuard / Tailscale / Headscale mesh upstream。
- [x] 对 external-owned，route binding 先标记 `upstream_kind=mesh` / `upstream_scope=mesh` 且不可用，直到 mesh upstream 实现。
- [ ] 对 user-owned 高流量节点，定义 local edge / regional gateway 部署方式。
- [ ] 明确公网 upstream 只作为 fallback，且必须有 mTLS 或等价保护。

### 8. 自建 fugue-dns shadow 模式

- [x] 新增 `cmd/fugue-dns`。
- [x] 新增 typed `/v1/edge/dns` DNS bundle API，先输出 `d-test.dns.fugue.pro` probe 和 verified custom domain 的稳定 `d-xxxx.dns.fugue.pro` target。
- [x] `fugue-dns` 周期性拉 bundle，写本地磁盘 cache。
- [x] DNS query 只查内存，不查数据库。
- [x] 拒绝 recursive query，只回答 authoritative zone。
- [x] 增加 `/healthz`、`/metrics`、bundle sync/cache/query counters。
- [x] 增加 Helm DNS DaemonSet，默认关闭，调度到 `fugue.io/role.dns=true` 且 `fugue.io/schedulable=true`，不默认开放公网 53。
- [x] 在美国 edge 节点 `vps-591f4447` 上启用 `fugue.io/role.dns=true`，与 edge 同节点运行首个 DNS shadow，打开 UDP 53 / TCP 53 hostPort。
- [x] 直接验证 `d-test.dns.fugue.pro A`：从集群外节点查询 `@15.204.94.71` 的 UDP 和 TCP DNS 均返回 `15.204.94.71`。
- [x] 在两个已 join cluster 且带 `fugue.io/role.dns=true` / `fugue.io/schedulable=true` 的节点部署 shadow 服务，开放 UDP 53 / TCP 53。
- [x] 在第二个 DNS 节点重复直接验证：`dig @51.38.126.103 d-test.dns.fugue.pro A` 和 TCP DNS 均返回 `51.38.126.103`。
- [x] 新增 DNS inventory / heartbeat API：`GET /v1/dns/nodes`、`GET /v1/dns/nodes/{dns_node_id}`、`POST /v1/dns/heartbeat`。
- [x] 让 `fugue-dns` 周期性上报 DNS node health、edge group、public IP、zone、bundle version、record count、cache 状态、UDP/TCP listen 和 query/error counters。
- [x] 增加 admin CLI：`fugue admin dns nodes ls`、`fugue admin dns nodes get <dns-node-id>`、`fugue admin dns status`。
- [x] 增加只读 DNS delegation preflight：验证双节点健康、UDP/TCP 53、`d-test.dns.fugue.pro`、bundle version、cache error 和 Kubernetes health gate，并输出父区记录和回滚计划。
- [x] 修复 preflight 的 bundle version 判定：不同 edge group 可以有不同 DNS bundle version，同一 edge group 内必须一致。
- [x] 增加 `fugue-dns` systemd 逃生口渲染脚本，用于脱离 Kubernetes 使用本地 cache 启动 authoritative DNS。
- [ ] 双节点 DNS shadow 继续观察 12-24 小时，确认 bundle sync error、cache write/load error、query error、Pod restart 和节点健康没有持续异常。

### 9. Cloudflare 委托 dns.fugue.pro

- [x] 增加 `fugue admin dns status` 作为委托前只读 preflight 和 runbook 输出；当前美国 / 德国两个 DNS 节点 preflight 已通过。
- [x] 增加 `fugue admin dns delegation plan|apply|rollback`，默认 dry-run；`apply --confirm` 前强制 preflight pass、双 DNS 节点 healthy、cache error 为 0，并把 Cloudflare 写入限制到 `ns1` / `ns2` glue 和 `dns.fugue.pro` NS。
- [ ] 在 Cloudflare `fugue.pro` zone 添加 `ns1.dns.fugue.pro` / `ns2.dns.fugue.pro` 的 DNS-only A/AAAA。
- [ ] 在 Cloudflare 添加 `dns.fugue.pro NS ns1.dns.fugue.pro` 和 `dns.fugue.pro NS ns2.dns.fugue.pro`，必须 DNS-only。
- [ ] 用 `dig d-test.dns.fugue.pro A +trace` 验证公网递归已进入 Fugue DNS。
- [ ] 保留 Google Cloud DNS zone 只作观察，不再写新事实。
- [ ] 确认 trace 稳定后，退役 Google Cloud DNS 的 `dns.fugue.pro` managed zone。

### 10. Custom domain canary

- [ ] 选择一个低风险测试 custom domain。
- [ ] 将客户侧 CNAME 指向 `d-test.dns.fugue.pro` 或真实 `d-xxxx.dns.fugue.pro`。
- [ ] 控制平面验证 CNAME 所有权。
- [ ] edge route bundle 包含真实 Host，例如 `www.customer.com`。
- [ ] edge 本地 TLS allowlist 包含该 Host。
- [ ] 验证 HTTPS、证书签发、WebSocket/SSE、上传、错误页。
- [ ] 监控 DNS fallback、edge 502、upstream latency。

### 11. Platform hostname canary

- [x] 选择低风险 `edge-protocol-canary.fugue.pro` app hostname，作为第一个 public exact DNS canary。
- [x] 保持用户可见 hostname 不变。
- [x] 在 Cloudflare 使用 DNS-only exact A record 将该 hostname 指到 `edge-group-country-us` 的健康美国 edge 节点。
- [x] 不改 wildcard `*.fugue.pro` fallback，不改 `api.fugue.pro`，不改 `dns.fugue.pro`。
- [x] 验证 edge route bundle 中该 Host 指向正确 app upstream。
- [x] 验证 HTTPS、Host route、WebSocket、SSE、upload、streaming 和 edge request/status/latency metrics。
- [x] 验证失败时可快速把 exact record 删除，回落到现有 wildcard / Route A。
- [ ] 观察该单 hostname canary 12-24 小时，确认 bundle sync error、Caddy apply error、upstream error、cache write/load error 和 Pod restart 没有持续异常。
- [ ] 通过后再选择 `cerebr.fugue.pro` 或另一个低风险 hostname 做第二个 exact DNS canary。

### 12. 小批量区域切流

- [ ] 按已有 edge group 覆盖的 runtime 区域挑选一批 app。
- [ ] 优先切 app runtime 和 edge 同区域或近区域的服务；没有本区域 edge 的 runtime 继续走 Route A。
- [ ] 每批切流前记录 baseline latency、错误率和 route generation。
- [ ] 每批切流后观察 24 小时。
- [ ] 出现异常时回滚 DNS exact record 或 route policy。
- [ ] 保持 `fugue-api` app proxy fallback 可用。

### 13. 将 regional edge 设为默认数据面

- [ ] 当 custom domain 和 platform hostname canary 稳定后，扩大 exact record 或 GeoDNS 策略。
- [ ] 将大多数公共 app route 默认绑定到 regional edge。
- [ ] Route A / `fugue-api` app proxy 降级为 legacy fallback 和诊断路径。
- [ ] 在产品和 CLI 中标明 route 当前服务 edge、fallback edge 和 route generation。
- [ ] 对新 app 默认生成 regional edge route binding。

### 14. 清理旧路径和补齐长期能力

- [ ] 评估是否把整个 `fugue.pro` 迁入 Fugue 自建权威 DNS。
- [ ] 评估是否添加 anycast。
- [ ] 评估是否添加 CDN 能力：static cache、cache rules、purge API、WAF、rate limit。
- [ ] 删除不再使用的 Google Cloud DNS 配置。
- [ ] 将旧 `/v1/edge/domains` 降级为兼容 API 或删除。
- [ ] 保留明确的 emergency fallback runbook。

## 基础设施准备清单

需要准备：

- 至少两个 regional edge 节点；当前已具备美国 `edge-group-country-us` 和德国 `edge-group-country-de` 两个健康 edge，亚洲 canary 前仍需准备亚洲 edge。
- 至少两个 `dns.fugue.pro` authoritative DNS 节点；当前美国和德国两个 direct-query shadow 节点已通过，但尚未做父区委托。
- 新增区域机器默认通过 `join-cluster.sh` 作为 k3s agent node 接入。
- NodePolicy / labels / taints 控制每台 joined node 的 `edge`、`dns`、`app-runtime` 等角色。
- 稳定公网 IPv4，最好同时有 IPv6。
- DNS 节点开放 UDP 53 和 TCP 53。
- Edge 节点开放 TCP 80 和 TCP 443。
- Edge route cache 和 certificate cache 的持久盘。
- Edge 到 runtime 的私有网络或 mesh 路径。
- Cloudflare 中 `dns.fugue.pro` 的 DNS-only 父区委托记录。
- Edge 专用认证凭据。
- Edge 和 DNS 的 metrics / logs 收集。

不要把手工 SSH 修改 live edge 节点当作正常发布路径。持久变更必须回写到本仓库，并通过正式控制平面发布链路上线。
