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
route_policy=route_a_only   只走现有 Route A / fugue-api app proxy
route_policy=edge_canary    只允许指定 edge_group 接这个 hostname
route_policy=edge_enabled   允许指定 edge_group 承接生产流量
```

平台生成的 `*.fugue.pro` hostname 已进入下一阶段：当存在健康 edge group 时，控制面会默认生成 `edge_enabled` route，而不是要求每个 hostname 先手工创建 exact DNS canary。为了兼容当前 Cloudflare 只有一条 `*.fugue.pro` wildcard 记录的现实，默认平台 route 会展开到所有健康 edge group；这样 wildcard 命中美国 edge 时，也能服务运行在德国或其他已知 runtime 上的 app，而不是因为 runtime 本地 edge 不匹配而 404。

自定义域名和显式 route policy 仍保持保守：custom domain 默认不因为平台 wildcard 切换而自动进入 edge；设置了 `edge_canary` / `edge_enabled` 的 hostname 仍绑定到指定 `edge_group_id`，用于精确 canary、回滚和排障。没有任何健康 edge group 时，route 继续保持 Route A fallback。

`runtime_edge_group_id` 是由 runtime 位置派生出的本地数据面归属；优先使用 runtime labels，缺失时使用 runtime 绑定的 Kubernetes Node labels，例如 `fugue.io/location-country-code=us`。`policy_edge_group_id` 是 hostname 级 canary policy 的目标。对显式 policy route，目标 edge group 必须有健康成员，并且仍应优先选择与 runtime 同区域或近区域的 edge。对默认平台 wildcard route，当前实现允许健康 edge 承接跨区域 upstream，这是为了在单条 Cloudflare wildcard 切到 edge 后避免远端 runtime app 404；后续真正区域化选择应交给 GeoDNS、Cloudflare Load Balancer、anycast 或自建 `fugue.pro` 权威 DNS。

2026-05-13 线上状态：hostname 级 `EdgeRoutePolicy` 仍用于显式 canary / override；平台生成的 `*.fugue.pro` route 已默认进入 edge，并由 `7f7a152 Serve platform routes from healthy edges` 展开到健康 edge group。`fugue-edge` 的 Caddy dynamic config 和直接 proxy 仍会先校验 `route.edge_group_id == FUGUE_EDGE_GROUP_ID`，确保本地 edge 只服务属于本 group 的 bundle route；`runtime_edge_group_id` 和 `policy_edge_group_id` 保留为观测、策略和后续区域化 DNS 选择依据。

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

2026-05-12 当前状态：已在两个 joined node 上完成 edge + DNS shadow / direct query 验证，并已把 `dns.fugue.pro` 子域从 Cloudflare 父区委托到 Fugue DNS。美国节点 `vps-591f4447` 属于 `edge-group-country-us`，DNS answer IP 为 `15.204.94.71`；德国节点 `vps-84c8f0a9` 属于 `edge-group-country-de`，DNS answer IP 为 `51.38.126.103`。两台节点都由 NodePolicy 标记为 `fugue.io/role.edge=true`、`fugue.io/role.dns=true` 和 `fugue.io/schedulable=true`，并通过 `fugue.io/dedicated=edge:NoSchedule` 隔离普通调度。两台节点的 `fugue-dns` 都打开 UDP 53 / TCP 53 hostPort，`dig @15.204.94.71 ...` 和 `dig @51.38.126.103 ...` 均已直接验证。Cloudflare 父区现在返回 `dns.fugue.pro NS ns1.dns.fugue.pro` / `ns2.dns.fugue.pro`，并带 `ns1 A 15.204.94.71`、`ns2 A 51.38.126.103` glue；Cloudflare DoH 和 Google DoH 已验证公网递归进入 Fugue DNS。`c09470117e446988e34b6db80a2986f92f479991` 已通过正式控制平面发布链路上线，并发布 CLI `v0.1.41`，修复 DNS delegation apply 对 split NS record set 的幂等性问题。后续生产操作应使用 `v0.1.41` 或更新版本。

同一美国 edge 节点已经从单 hostname canary 进入平台 wildcard 数据面：部署变量显式打开 edge Caddy hostPort 80 / 443，Caddy listen 为 `:443`，TLS mode 为 `public-on-demand`，同时通过 Kubernetes Secret 静态加载 `*.fugue.pro` / `fugue.pro` wildcard 证书。Cloudflare 中 `*.fugue.pro` 现在指向美国 edge 入口；`api.fugue.pro` 仍保留在控制平面 Route A，不进入 regional edge。先前用于 public canary 的 exact app A 记录已经删除，平台 app hostname 依赖 wildcard 进入 edge。`dns.fugue.pro` 只承担 DNS 子区委托，不作为 HTTP edge hostname 切流。

德国 edge 当前也会同步并生成平台 route bundle，`edge-group-country-de` 健康且 Caddy 已加载平台 route；但公网 Cloudflare wildcard 仍只有一个入口，当前解析到美国 edge。因此德国 edge 具备服务能力，但还不是公网区域选择入口。要让欧洲用户优先进入德国 edge，需要后续接入 Cloudflare Load Balancer / GeoDNS / anycast，或把 `fugue.pro` 平台 hostname 也迁到 Fugue 自建权威 DNS。

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

长期设计上，`fugue-edge` 和 `fugue-dns` 必须既能作为 Kubernetes workload 运行，也能脱离 Kubernetes 以 systemd 方式运行。这样某个区域无法或不适合 join 中心 k3s 时，仍可作为独立 regional edge 接入控制平面拉 bundle。当前 `fugue-dns` 已有 `scripts/render_fugue_dns_systemd_unit.sh` 可渲染 `fugue-dns.service` 和不含 token 的环境文件；`fugue-edge` 已在 `964d5f2662769832ddf22fa53115d23aafa579fd` 补齐并发布 `scripts/render_fugue_edge_systemd_unit.sh`，可生成 `fugue-edge.service` 和 `fugue-edge.env`，token 仍放在单独 secret env file 中。该脚本支持 route cache、Caddy config/cache 目录、edge group、公网 IP、Caddy listen/TLS mode，并有 shell test 覆盖参数校验和 token 不落盘。

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

`fugue-dns` 周期性 heartbeat 会上报 `dns_node_id`、`edge_group_id`、公网 IP、zone、DNS bundle version、record count、cache 状态、UDP/TCP listen 状态、query/error counters 和 `last_seen_at`。`fugue admin dns status` 是只读委托 preflight：检查至少两个 healthy DNS 节点、UDP/TCP 53 可达、`d-test.dns.fugue.pro` 回答预期 IP、bundle version 在同一 edge group 内稳定、cache error 为 0、节点 `Ready=True` 且 `DiskPressure=False`，并输出计划添加和回滚删除的父区记录。`fugue admin dns delegation apply` / `rollback` 默认 dry-run；只有传 `--confirm` 才会调用 Cloudflare API，并且只允许改 `ns1.dns.fugue.pro`、`ns2.dns.fugue.pro` 的 glue A/AAAA 和 `dns.fugue.pro` 的 NS，不触碰 wildcard、`api.fugue.pro` 或其他父区记录。生产委托已用修复后的 `v0.1.41` CLI 复跑 confirm，四条 Cloudflare 记录均为 `unchanged`。

2026-05-13 full-zone 准备：仓库新增 full-zone `fugue.pro` authoritative 能力，但公网注册商 NS 尚未切换。`/v1/edge/dns?zone=fugue.pro` 会把从 Cloudflare 导出的静态记录作为 `record_kind=protected` 注入 bundle，动态平台 app 记录不能覆盖这些受保护名称；同时会为平台 app hostname 派生 `record_kind=platform` 的 A/AAAA，按 runtime / edge health 选择目标 edge IP。`fugue-dns` authoritative server 已支持 `A`、`AAAA`、`CNAME`、`TXT`、`MX`、`CAA`、`NS` 和 `*.fugue.pro` wildcard fallback。Cloudflare apex CNAME flattening 不能原样迁入 authoritative DNS，因为 zone apex 不能和 NS/SOA 同时为 CNAME；导出脚本会默认把 `fugue.pro` apex CNAME 解析并转换为 A/AAAA protected records。

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

如果以后完全离开 Cloudflare，需要在域名注册商处把 `fugue.pro` 的 nameserver 改成 Fugue 自己的 nameserver，并配置 glue records。当前注册商是 Spaceship，Cloudflare DNS API token 不能修改注册商 nameserver / glue；这一步必须在 Spaceship 执行。

当前 full-zone 方案使用已有 `dns.fugue.pro` 子域下的 in-bailiwick nameserver：

```text
fugue.pro.              NS    ns1.dns.fugue.pro.
fugue.pro.              NS    ns2.dns.fugue.pro.
ns1.dns.fugue.pro.      A     15.204.94.71
ns2.dns.fugue.pro.      A     51.38.126.103
```

在 Spaceship 中需要同时设置：

```text
nameserver: ns1.dns.fugue.pro
nameserver: ns2.dns.fugue.pro
glue / host record: ns1.dns.fugue.pro -> 15.204.94.71
glue / host record: ns2.dns.fugue.pro -> 51.38.126.103
```

如果注册商要求 nameserver 直接在 zone apex 下，也可以改用：

```text
fugue.pro.      NS    ns1.fugue.pro.
fugue.pro.      NS    ns2.fugue.pro.
ns1.fugue.pro.  A     <dns-node-1-public-ip>
ns2.fugue.pro.  A     <dns-node-2-public-ip>
```

但这需要同步调整 `FUGUE_DNS_NAMESERVERS`、静态 records 和注册商 glue，不应和 `ns1.dns.fugue.pro` 方案混用。

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

2026-05-13 当前实现采用“外部 DNS-01 签 wildcard 证书 + Caddy 静态加载 + on-demand 兜底 custom domain”的组合：运维脚本使用 Cloudflare DNS token 签发 `*.fugue.pro` / `fugue.pro` 证书并写入 Kubernetes TLS Secret；`fugue-edge` 只把证书和私钥文件路径传给 Caddy，Caddy runtime 不持有 Cloudflare token。Caddy JSON 同时配置 `tls.certificates.load_files` 和 existing `public-on-demand` automation：平台 app hostname 优先命中静态 wildcard 证书，非 wildcard 覆盖的 custom domain 仍可走 on-demand 兜底。

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

- 当前平台 app hostname 的默认生产热路径已经切到 edge：Cloudflare `*.fugue.pro` wildcard 指向美国 edge，edge Caddy 终止 TLS 后直接反代到 app Service / Pod。`api.fugue.pro`、控制平面 API、controller、etcd 等核心控制面入口仍保留 Route A，不进入 regional edge。
- 当前已经具备只读 route binding 派生、typed `/v1/edge/routes` route bundle API、稳定 `ETag` / conditional fetch、`fugue-edge` shadow DaemonSet、本地 route cache、health 和 metrics。
- 线上已经具备 hostname 级 `EdgeRoutePolicy`、`/v1/edge/route-policies` 管理 API 和 `fugue admin edge route-policy` CLI；它现在主要用于显式 canary / override / 回滚。平台生成的 `*.fugue.pro` route 默认 `edge_enabled` 并展开到健康 edge group；custom domain 仍不随平台 wildcard 自动切入。
- 当前 `fugue-edge` 已从 shadow / canary 进入平台 wildcard 数据面：美国 edge 对公网 80/443 开放 hostPort，Caddy-backed dynamic config 承接 `*.fugue.pro` app hostname；先前的 exact app canary 记录已经删除。
- Caddy-backed edge 已发布到美国和德国 edge 节点，Caddy admin 绑定 localhost；美国 edge 当前是 Cloudflare wildcard 入口。Caddy 同时使用静态加载的 `*.fugue.pro` wildcard TLS Secret 和 public on-demand TLS，避免平台 app hostname 触发 Let’s Encrypt exact-set 限流。德国 edge healthy 且生成平台 routes，但当前公网 wildcard 没有区域 DNS 选择，仍主要作为备用/后续区域入口能力。
- `fugue-dns` 已具备 typed `/v1/edge/dns` DNS bundle API、本地 DNS cache、authoritative-only DNS responder、health/metrics 和 Helm DNS DaemonSet；已在美国和德国两个 edge/DNS 节点上完成直接 DNS 验证，并进入 `dns.fugue.pro` 初期生产权威委托。
- 仓库已补 full-zone `fugue.pro` DNS bundle 支持：受保护 static records、平台 app A/AAAA 派生、NS/TXT/MX/CAA/CNAME authoritative answer、wildcard fallback 和 Cloudflare apex CNAME flatten 导出脚本。正式公网切换仍等待发布后 shadow direct query 通过，以及用户在 Spaceship 配置 `ns1.dns.fugue.pro` / `ns2.dns.fugue.pro` nameserver 和 glue。
- DNS inventory / heartbeat 已上线：`GET /v1/dns/nodes`、`GET /v1/dns/nodes/{dns_node_id}`、`POST /v1/dns/heartbeat`、`GET /v1/dns/delegation/preflight` 和 `fugue admin dns nodes ls|get|status` 可用于集中查看 DNS 节点健康、bundle/cache/query 状态和委托 preflight。
- `fugue admin dns delegation plan|apply|rollback` 已随 CLI 发布；美国 / 德国两个 healthy DNS 节点 preflight 通过。2026-05-12 已在 Cloudflare `fugue.pro` 父区创建 `ns1` / `ns2` glue A 记录和 `dns.fugue.pro` NS 委托，Google Cloud DNS zone 暂时保留作回滚观察。生产操作使用 `v0.1.41` 或更新版本，避免 `v0.1.40` 的 split NS record set 幂等性问题。
- NodePolicy 最小模型已经上线：控制面持久化 `app-runtime`、`shared-pool`、`edge`、`dns`、`builder`、`internal-maintenance` 等 desired role，并 reconcile 到 Kubernetes Node labels / taints。
- NodePolicy 可视化接口和 CLI 已补齐：`GET /v1/cluster/node-policies`、`GET /v1/cluster/node-policies/{name}`、`GET /v1/cluster/node-policies/status` 对应 `fugue admin cluster node-policy ls|get|status`，用于查看 desired role、实际 labels / taints、`Ready` / `DiskPressure` gate 和 reconcile drift。
- edge / edge group 持久模型、`/v1/edge/nodes` inventory API、`/v1/edge/heartbeat`、edge-scoped token 和 `fugue admin edge nodes` CLI 已发布到控制面；当前美国和德国 edge 节点都已使用 scoped token 上报健康状态，route bundle、Caddy apply 和 cache 状态均可从 admin CLI 读取。
- `fugue-edge` systemd 逃生口已随 `964d5f2662769832ddf22fa53115d23aafa579fd` 发布；它只生成 service/env，不写 token，真正独立 systemd 演练仍需在目标 edge 节点单独执行。
- route binding 已补本地 upstream 防线：runtime edge group 从 runtime labels 派生，缺失时从绑定 Node labels 派生；显式 route policy 仍要求目标 edge group 有健康成员并优先匹配 runtime 区域；平台 wildcard 默认 route 会展开到健康 edge group，避免单 wildcard 入口下远端 runtime hostname 404；managed-shared / managed-owned 使用 `upstream_scope=local-service` 的 in-cluster Service DNS，external-owned 先标记为 mesh upstream 未就绪。
- `fugue-edge` 观测已扩展：Caddy dynamic config 开启 JSON access log，edge proxy metrics 增加 fallback hit、WebSocket / SSE / streaming 成功率和 upload request 计数。
- 控制面现在会读取 Kubernetes Node condition，把 `Ready=False` 或 `DiskPressure=True` 的节点标为 `fugue.io/schedulable=false`、`fugue.io/node-health=blocked`，并加 `fugue.io/node-unhealthy=true:NoSchedule`；除 `node-janitor` 这类清理组件外，普通维护组件、edge、dns、runtime workload 都不应继续调度到异常节点。
- `fugue-edge` 和 `fugue-dns` 现在都要求对应 `fugue.io/role.*=true` 且 `fugue.io/schedulable=true`；edge / DNS 节点默认带 `fugue.io/dedicated` taint，初期允许同一节点同时承担 edge + DNS。为兼容已作为 runtime join 的美国 edge 节点，edge / DNS DaemonSet 也 tolerate 旧的 `fugue.io/tenant` taint，但仍必须先匹配显式 role 和健康 label。
- `dns.fugue.pro` 已从 Cloudflare 父区委托到 Fugue DNS 的 `ns1.dns.fugue.pro` / `ns2.dns.fugue.pro`；双节点 direct query、Cloudflare authoritative referral 和 DoH 递归验证均已通过。Google Cloud DNS 旧 zone 仍保留作短期回滚观察，不再作为新的事实源。
- `dns.fugue.pro` 的迁移前 Google Cloud DNS baseline 和具体旧记录放在本地私有附录中；当前公网父区委托事实源已经是 Cloudflare 中的 `ns1` / `ns2` 记录。
- 平台 wildcard 已切到 edge，但不是整个 `fugue.pro` 全量迁移：`api.fugue.pro` 和控制面核心服务仍走 Route A；`dns.fugue.pro` 仅作为自建权威 DNS 子区；custom domain 仍保持独立验证和 TLS 策略。`edge-protocol-canary.fugue.pro` 已完成 HTTPS、Host route、WebSocket、SSE、upload 和 streaming 验证；当前重点转为观察 wildcard edge 默认路径、双 edge/DNS heartbeat、route bundle 304、Caddy route、edge metrics 和回滚能力。

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
  用户
    -> regional edge / Caddy
    -> app Service / Pod
```

迁移期的关键边界：

- `api.fugue.pro`、controller、etcd、control-plane API 和控制平面核心服务不走 regional edge。
- 普通业务 Pod 可以逐步从 control-plane 节点迁到 agent/runtime 节点；平台 app hostname 现在默认从 edge 进入，但 workload 迁移和 edge 入口仍是两个独立维度。
- 平台生成的 `*.fugue.pro` hostname 已由 wildcard DNS 进入美国 edge；`fugue-edge` 会基于 route bundle 反代到对应 app Service / Pod。
- 显式 `EdgeRoutePolicy` 仍用于单 hostname canary、覆盖和回滚，不再是平台 app 进入 edge 的唯一入口。
- custom domain 不随平台 wildcard 自动切换，仍需要 verified domain、TLS allowlist 和 route policy / bundle 明确支持。
- 当前只有一个 Cloudflare wildcard 入口，实际公网入口是美国 edge。德国 edge 已健康并生成平台 route，但还需要 GeoDNS、Cloudflare Load Balancer、anycast 或自建 `fugue.pro` 权威 DNS 才能成为欧洲用户的默认入口。
- 回滚平台 app 默认 edge 的主路径是把 Cloudflare `*.fugue.pro` wildcard 指回旧 Route A 入口，或临时关闭默认 edge route 派生；不是逐个删除 exact record。

因此近期顺序是：

1. 继续保持 Route A fallback 可恢复，同时观察美国 wildcard edge 入口、德国 edge standby 路由、DNS authority workload 和 edge inventory heartbeat。
2. 用 `fugue admin cluster node-policy status` 持续确认 desired role / actual labels / health gate / reconcile drift；当前 `drifted=0`、`disk_pressure=0`，扩大 edge、DNS 或 runtime 节点前仍要短窗口复查。
3. 继续把控制面节点保持在低业务负载状态；普通业务迁移可以逐批推进，但控制面核心服务和 `api.fugue.pro` 不进入 regional edge 数据面。
4. 逐批把普通业务从 control-plane 节点迁出，避免控制面节点混跑业务 workload。
5. 继续观察已迁到 agent 节点的普通 app：平台 wildcard edge 路径、route bundle、Caddy host route、edge proxy metrics、cache/304 行为和 Route A fallback 都必须稳定。
6. 对没有本区域 edge 的 runtime，允许平台 wildcard 先由健康 edge 承接，避免默认 hostname 404；但这只是当前单 wildcard 过渡形态，不代表已经完成区域最优入口。
7. 继续观察美国 / 德国两个 `fugue-dns` authority 节点，确认 bundle sync、cache、UDP/TCP 53、递归查询分布、Pod restart 和节点健康长期稳定。
8. exact app DNS canary 已收敛到 wildcard 默认入口；继续确认 edge health、route bundle、Caddy apply、edge metrics 和 Cloudflare wildcard 回滚路径。`dns.fugue.pro` 委托已完成，后续重点是持续观察、回滚 dry-run 和暂缓退役 Google Cloud DNS。

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
- [x] 为 `fugue-edge` 补齐并发布 systemd 逃生口，避免 edge 完全依赖 Kubernetes 才能启动；脚本生成不含 token 的 env 文件和 `fugue-edge.service`，token 从独立 secret env file 读取。

### 2. 建立 edge inventory 和认证模型

- [x] 在 OpenAPI 里设计 edge inventory、edge heartbeat、edge route pull 的 API 契约。
- [x] 新增 edge / edge group 数据模型和存储迁移。
- [x] 新增 edge-scoped token，不复用 tenant API key。
- [x] 新增 edge heartbeat，上报 region、public IP、mesh IP、route / DNS bundle version、Caddy apply/cache 状态和健康状态。
- [x] 增加 admin API 和 CLI 查看 edge 列表和健康状态：`fugue admin edge nodes ls`、`fugue admin edge nodes get <edge-id>`。
- [x] 发布到控制面并完成初始线上验证：美国 edge 使用 scoped token 上报，`last_seen_at` / `last_heartbeat_at` 持续更新，bundle API 仍保持稳定 `ETag`。
- [x] 完成 edge inventory 初始长时间观察：heartbeat 持续更新，scoped token 生效，route bundle 大量命中 `304 not_modified`，cache write/load error 为 0。
- [x] 第二个 edge group `edge-group-country-de` 已上线：德国 edge 节点 `vps-84c8f0a9` 使用 scoped token 上报 `region=europe`、`country=de`、`public_ipv4=51.38.126.103`，当前 healthy 且 Caddy route count 已随平台默认 edge route 增加。
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
- [x] 在德国 joined node `vps-84c8f0a9` 部署第二个 regional edge workload，绑定 `edge-group-country-de`，确认 `/healthz` healthy、route bundle 走 `304 not_modified`、Caddy apply 成功且能加载平台 route。
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
- [x] 明确 workload 调度和 edge 入口是两个独立维度；平台生成 hostname 已随 wildcard 默认进入 edge，custom domain 和显式 canary / override 仍由 route policy 控制。

### 6. 接入 Caddy-backed 反代和平台 wildcard 数据面

- [x] `fugue-edge` 根据 route bundle 生成 Caddy dynamic config。
- [x] Caddy admin API 只绑定 localhost。
- [x] Caddy sidecar / dynamic config 通过 Helm opt-in 开启，默认关闭；不加 hostNetwork、hostPort 或公网 80/443。
- [x] Caddy-backed shadow 已发布到美国 edge 节点，并经过单 hostname public canary 验证；随后平台 wildcard 已切到美国 edge。`api.fugue.pro` 和 `dns.fugue.pro` 不作为 HTTP app wildcard 流量进入 edge。
- [x] 支持 platform hostname 和 custom domain Host route。
- [x] 支持 WebSocket、SSE、upload、stream response。
- [x] 加 Caddy access log、edge proxy per-route request count、status code、upstream latency、upstream error、fallback hit、WebSocket / SSE / streaming result 和 upload request metrics。
- [x] 新增 hostname 级 EdgeRoutePolicy，让 route 明确处于 `route_a_only`、`edge_canary` 或 `edge_enabled`。
- [x] Caddy dynamic config 只生成当前 edge group 被允许承接的 hostname；平台生成 hostname 默认 `edge_enabled`，custom domain 和显式 policy 继续受策略约束。
- [x] `fugue-edge` 直接 proxy 跳过 `route_a_only` route，避免绕过 Caddy gating；平台默认 edge route 不再是 `route_a_only`。
- [x] `fugue-edge` Caddy 模式要求显式 `FUGUE_EDGE_GROUP_ID`，并在 Caddy config 和直接 proxy 两层本地过滤非本 edge group route。
- [x] 对没有健康 edge group 的 route 保持 Route A；对平台 wildcard 默认 route，控制面会展开到所有健康 edge group，避免单 wildcard 入口下远端 runtime hostname 404。
- [x] 发布包含 EdgeRoutePolicy 的控制面版本 `d213572e943980aa4e119ac76788cf74fd3933fc`，并在线上确认 `/v1/edge/route-policies` 和 OpenAPI 契约生效。
- [x] 发布包含 edge group 本地防线的控制面版本 `3c6f807f01389a09bb01c16f99b8f01faaea8610`，并在线上确认 edge DaemonSet 带 `FUGUE_EDGE_GROUP_ID=edge-group-country-us`。
- [x] 在线上设置第一个 `edge_canary` policy：`cerebr.fugue.pro -> edge-group-country-us`。
- [x] 确认 `cerebr.fugue.pro` route bundle / Caddy config / proxy gating 的实际 hostname 承接行为。
- [x] 用 `curl --resolve` 带正确 SNI 验证 Caddy-backed 内部 TLS canary，未 opt-in hostname 不进入 edge proxy。
- [x] 单 hostname canary 阶段只有显式 opt-in hostname 进入 edge 路径；平台 wildcard 切换后，生成的 platform hostname 默认进入 edge。
- [x] 通过正式发布链路接入 public canary 部署变量：`publicHostPorts.enabled=true`、Caddy listen `:443`、TLS `public-on-demand`。
- [x] 发布后确认美国 edge `15.204.94.71:443` 可服务，Caddy public on-demand TLS 能成功签发 canary hostname 证书。
- [x] 对 `edge-protocol-canary.fugue.pro` 做 public canary：Host route、WebSocket、SSE、upload、streaming 和 edge metrics 均已验证。
- [x] 增加 edge 静态 wildcard TLS：Caddy 通过 `tls.certificates.load_files` 读取 Kubernetes TLS Secret 中的 `*.fugue.pro` / `fugue.pro` 证书，Cloudflare token 只在签发脚本中使用，不进入 Caddy runtime。
- [x] 将 Cloudflare `*.fugue.pro` wildcard 切到美国 edge，并删除先前的 app exact canary 记录；`api.fugue.pro` 保持 Route A。

### 7. 建立 edge 到 runtime 的本地 upstream 路径

- [x] 对 managed-shared / managed-owned，在 route bundle 中明确 `upstream_scope=local-service`，使用 in-cluster Service DNS。
- [x] runtime edge group 优先从 runtime labels 派生，缺失时从绑定的 Kubernetes Node labels 派生，避免已标记美国节点的 runtime 因自身 labels 为空而被错误归入 default group。
- [x] 显式 route policy 目标 edge group 必须有健康成员，且应优先与 runtime 派生 edge group 匹配；不满足时可保持不可接流或回到 Route A。
- [x] 在 Caddy config 和 direct proxy 两层继续校验 `edge_group_id` 与本地 edge group 一致；`runtime_edge_group_id` / `policy_edge_group_id` 保留用于观测、策略和后续区域 DNS 选择。
- [ ] 对 external-owned，验证 WireGuard / Tailscale / Headscale mesh upstream。
- [x] 对 external-owned，route binding 先标记 `upstream_kind=mesh` / `upstream_scope=mesh` 且不可用，直到 mesh upstream 实现。
- [ ] 对 user-owned 高流量节点，定义 local edge / regional gateway 部署方式。
- [ ] 明确公网 upstream 只作为 fallback，且必须有 mTLS 或等价保护。

### 8. 自建 fugue-dns shadow / 初期权威模式

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
- [ ] 双节点 DNS 生产委托继续观察 24-72 小时，确认 bundle sync error、cache write/load error、query error、Pod restart、递归解析分布和节点健康没有持续异常。

### 9. Cloudflare 委托 dns.fugue.pro

- [x] 增加 `fugue admin dns status` 作为委托前只读 preflight 和 runbook 输出；当前美国 / 德国两个 DNS 节点 preflight 已通过。
- [x] 增加并发布 `fugue admin dns delegation plan|apply|rollback`，默认 dry-run；`apply --confirm` 前强制 preflight pass、双 DNS 节点 healthy、cache error 为 0，并把 Cloudflare 写入限制到 `ns1` / `ns2` glue 和 `dns.fugue.pro` NS。
- [x] 在 Cloudflare `fugue.pro` zone 添加 `ns1.dns.fugue.pro` / `ns2.dns.fugue.pro` 的 DNS-only glue A 记录。
- [x] 在 Cloudflare 添加 `dns.fugue.pro NS ns1.dns.fugue.pro` 和 `dns.fugue.pro NS ns2.dns.fugue.pro`，必须 DNS-only。
- [x] 用 Cloudflare authoritative query 和 Cloudflare / Google DoH 验证公网递归已进入 Fugue DNS；本机 `+trace` 受本地网络路径影响，已至少确认 trace 到 Cloudflare 父区返回 `ns1` / `ns2` 委托。
- [x] 保留 Google Cloud DNS zone 只作观察，不再写新事实。
- [ ] 确认 trace 稳定后，退役 Google Cloud DNS 的 `dns.fugue.pro` managed zone。

### 9b. Full-zone `fugue.pro` authoritative DNS

- [x] 导出 Cloudflare 当前 `fugue.pro` zone 的 A / AAAA / CNAME / TXT / MX / CAA / NS 记录，并转换为 Fugue protected static DNS records。
- [x] 处理 Cloudflare apex CNAME flattening：导出脚本默认把 `fugue.pro` apex CNAME 展开成 A / AAAA，避免 authoritative DNS 中 apex CNAME 与 NS/SOA 冲突。
- [x] `/v1/edge/dns?zone=fugue.pro` 合并 protected static records 和平台 app 动态 records；protected 名称不能被 app route 覆盖。
- [x] `fugue-dns` authoritative server 支持 NS / TXT / MX / CAA / CNAME，并支持 `*.fugue.pro` wildcard fallback。
- [x] Helm / GitHub Actions 发布变量支持 `FUGUE_DNS_STATIC_RECORDS_JSON` 和 `FUGUE_DNS_NAMESERVERS`。
- [ ] 发布 full-zone 版本后，直接验证双 DNS 节点：`api.fugue.pro`、`oaix.fugue.pro`、`argus.fugue.pro`、`fugue.pro A/MX/TXT/NS` 均返回预期结果。
- [ ] full-zone preflight 通过后，在 Spaceship 把 `fugue.pro` nameserver 切到 `ns1.dns.fugue.pro` / `ns2.dns.fugue.pro`，并设置 glue：`15.204.94.71` / `51.38.126.103`。
- [ ] 切后验证 `dig fugue.pro NS +trace`、公共递归 `@1.1.1.1` / `@8.8.8.8`、核心 app HTTPS 和 `fugue admin dns status` / `fugue admin edge nodes ls`。
- [ ] 若异常，先在 Spaceship 恢复 Cloudflare nameserver；Cloudflare zone 和旧 `dns.fugue.pro` 委托在观察期内保留作回滚事实源。

### 10. Custom domain canary

- [ ] 选择一个低风险测试 custom domain。
- [ ] 将客户侧 CNAME 指向 `d-test.dns.fugue.pro` 或真实 `d-xxxx.dns.fugue.pro`。
- [ ] 控制平面验证 CNAME 所有权。
- [ ] edge route bundle 包含真实 Host，例如 `www.customer.com`。
- [ ] edge 本地 TLS allowlist 包含该 Host。
- [ ] 验证 HTTPS、证书签发、WebSocket/SSE、上传、错误页。
- [ ] 监控 DNS fallback、edge 502、upstream latency。

### 11. Platform hostname canary 和 wildcard 默认入口

- [x] 选择低风险 `edge-protocol-canary.fugue.pro` app hostname，作为第一个 public exact DNS canary。
- [x] 保持用户可见 hostname 不变。
- [x] 在 Cloudflare 使用 DNS-only exact A record 将该 hostname 指到 `edge-group-country-us` 的健康美国 edge 节点。
- [x] 单 hostname canary 阶段不改 wildcard `*.fugue.pro` fallback，不改 `api.fugue.pro`，不改 `dns.fugue.pro`。
- [x] 验证 edge route bundle 中该 Host 指向正确 app upstream。
- [x] 验证 HTTPS、Host route、WebSocket、SSE、upload、streaming 和 edge request/status/latency metrics。
- [x] 验证失败时可快速把 exact record 删除，回落到现有 wildcard / Route A。
- [x] 观察单 hostname canary 后，将平台 `*.fugue.pro` wildcard 指向美国 edge，并删除先前 app exact records，避免长期靠 Cloudflare 特例维护平台 hostname。
- [x] 平台生成 hostname 默认派生 edge route，且 route bundle 展开到所有健康 edge group；美国 wildcard 入口能服务已迁移到美国 agent 和德国 runtime 的平台 app。
- [ ] 继续观察 wildcard 默认入口 24-72 小时，确认 bundle sync error、Caddy apply error、upstream error、cache write/load error、Pod restart、证书续期和 Cloudflare 回滚路径没有持续异常。

### 12. 小批量区域切流

- [x] 平台 app hostname 已从 exact canary 扩大到 wildcard 默认 edge 入口；当前公网入口仍是美国 edge。
- [ ] 按已有 edge group 覆盖的 runtime 区域继续观察分批 workload 迁移，优先确保 app Service / Pod、managed backing service 和 PVC/data localization 都已稳定。
- [ ] 接入区域 DNS 选择前，德国 edge 作为健康 standby / 后续区域入口；欧洲用户不会仅凭德国 edge healthy 自动优先进入德国 edge。
- [ ] 每批切流前记录 baseline latency、错误率和 route generation。
- [ ] 每批切流后观察 24 小时。
- [ ] 出现异常时回滚 Cloudflare wildcard 到 Route A，或关闭默认 edge route 派生 / 修改 route policy。
- [ ] 保持 `fugue-api` app proxy fallback 可用。

### 13. 将 regional edge 设为默认数据面

- [x] platform hostname 已默认进入 regional edge 数据面，当前通过 Cloudflare wildcard 到美国 edge。
- [ ] 当 wildcard 默认入口稳定后，补区域选择能力：Cloudflare Load Balancer / GeoDNS、anycast，或把 `fugue.pro` 平台 hostname 纳入 Fugue 自建权威 DNS。
- [x] 将大多数公共 platform app route 默认绑定到 regional edge。
- [ ] Route A / `fugue-api` app proxy 降级为 legacy fallback 和诊断路径。
- [ ] 在产品和 CLI 中标明 route 当前服务 edge、fallback edge 和 route generation。
- [x] 对新 platform app 默认生成 regional edge route binding；custom domain 仍需要 verified domain / TLS / route policy 明确支持。

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
- 至少两个 `dns.fugue.pro` authoritative DNS 节点；当前美国和德国两个节点已通过 direct query、Cloudflare authoritative referral 和 DoH 递归验证，并已完成父区委托。
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
