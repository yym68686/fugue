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
- route binding policy
- 生成 edge route bundle
- 生成 DNS bundle
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
edge_group_id
route_policy
upstream_kind
upstream_url
service_port
route_generation
status
fallback_edge_group_id
created_at
updated_at
```

初期可以从现有 app state 派生 route binding；长期 edge 数据面应该以 route binding 作为权威契约。

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
draining
last_seen_at
route_generation
supported_zones
created_at
updated_at
```

`edge_group` 是 DNS 和 route policy 的选择单位，单个 edge node 是该 group 下的健康和容量成员。

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

示例：

```text
edge-hk-1:
  fugue.io/role.edge=true
  fugue.io/role.dns=true
  fugue.io/region=asia-hk
  fugue.io/edge-group=edge-group-asia-hk
  taint: fugue.io/dedicated=edge:NoSchedule

sg-app-1:
  fugue.io/role.app-runtime=true
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
ns1.fugue.pro.       A     <edge-hk-1-public-ip>     DNS only
ns2.fugue.pro.       A     <edge-sg-1-public-ip>     DNS only

dns.fugue.pro.       NS    ns1.fugue.pro.            DNS only
dns.fugue.pro.       NS    ns2.fugue.pro.            DNS only
```

同机部署时，端口不冲突：

```text
fugue-dns  -> UDP 53 / TCP 53
Caddy      -> TCP 80 / TCP 443
fugue-edge -> 127.0.0.1 或私网管理端口
```

生产最少两台，最好三台，且跨区域。单台机器故障会同时损失一个 authoritative DNS 节点和一个 regional edge 节点，因此不能单节点承载生产流量。

### Kubernetes 部署方式

如果 edge / DNS 节点已经 join cluster，优先用 `hostNetwork` 的 DaemonSet 部署角色组件：

```text
fugue-dns DaemonSet:
  nodeSelector: fugue.io/role.dns=true
  hostNetwork: true
  ports: UDP 53 / TCP 53

fugue-edge / Caddy DaemonSet:
  nodeSelector: fugue.io/role.edge=true
  hostNetwork: true
  ports: TCP 80 / TCP 443
```

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
- systemd 独立运行 `fugue-edge` / `fugue-dns` 的逃生口

长期设计上，`fugue-edge` 和 `fugue-dns` 必须既能作为 Kubernetes workload 运行，也能脱离 Kubernetes 以 systemd 方式运行。这样某个区域无法或不适合 join 中心 k3s 时，仍可作为独立 regional edge 接入控制平面拉 bundle。

## Route Bundle API

新增控制平面 endpoint，供区域 edge 拉取允许服务的路由：

```http
GET /v1/edge/routes?edge_id=<edge-id>
GET /v1/edge/routes?edge_group_id=<edge-group-id>
```

响应示例：

```json
{
  "version": "routegen_1770000001",
  "generated_at": "2026-05-06T00:00:00Z",
  "edge_group_id": "edge-group-asia-sg",
  "routes": [
    {
      "hostname": "demo.fugue.pro",
      "app_id": "app_123",
      "tenant_id": "tenant_123",
      "runtime_id": "runtime_sg",
      "upstream_url": "http://app-123.fg-tenant.svc.cluster.local:3000",
      "service_port": 3000,
      "tls_policy": "platform",
      "streaming": true
    },
    {
      "hostname": "www.customer.com",
      "app_id": "app_123",
      "tenant_id": "tenant_123",
      "runtime_id": "runtime_sg",
      "upstream_url": "http://app-123.fg-tenant.svc.cluster.local:3000",
      "service_port": 3000,
      "tls_policy": "custom-domain",
      "streaming": true
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
- 要求 edge 专用认证
- 每个 edge 将最新成功 bundle 写入本地磁盘
- 控制平面不可用时从本地磁盘 cache 恢复
- 迁移期保留现有 `/v1/edge/domains`，但长期以 route bundle 作为数据面 API

## Edge 认证

不要复用 tenant API key。

可选认证方式：

- 每个 edge 或 edge group 一个 scoped edge token
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
ns1.fugue.pro.       A     <dns-node-1-public-ip>     DNS only
ns2.fugue.pro.       A     <dns-node-2-public-ip>     DNS only

dns.fugue.pro.       NS    ns1.fugue.pro.             DNS only
dns.fugue.pro.       NS    ns2.fugue.pro.             DNS only
```

这些 `A` 和 `NS` 记录必须是 DNS only，不能走 Cloudflare proxy。

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

- 当前生产仍是 Route A 热路径：公网入口先进入现有 Caddy / control-plane API，再由 `fugue-api` app proxy 反代到业务 Service。
- 当前还没有独立 `fugue-edge`、`fugue-dns`、route bundle、DNS bundle 或区域化 `d-xxxx.dns.fugue.pro` 解析。
- `dns.fugue.pro` 的现有公网权威和具体记录属于迁移前 baseline，具体值放在本地私有附录中。
- 第一阶段不能直接切 DNS，也不能先替换现有入口；应先做只读 route binding 派生、shadow edge 和 shadow DNS。

本地私有附录路径：

```text
docs/private/regional-edge-current-state.local.md
```

该路径已通过 `.gitignore` 忽略，只用于本机排障和迁移前后对照。正式提交中不得包含生产 token、IP inventory、完整 app inventory、节点清单或内部 Service 地址。

## 平滑重构 TODO List

下面 TODO 按顺序执行。每一步都应该能独立上线、验证和回滚，不要求一次性完成大迁移。

### 0. 冻结当前事实和保护 fallback

- [x] 记录当前 Route A 热路径：公网 DNS、Caddy、`fugue-api` Service、app proxy、业务 Service。
- [x] 确认所有现有 `*.fugue.pro` app hostname 和 custom domain 清单。
- [x] 确认每个 app 的 runtime、节点区域、route hostname、custom-domain target。
- [x] 明确当前 `fugue-api` app proxy 作为 legacy fallback 保留，不在早期删除。
- [x] 给当前 Route A 增加最小观测：请求量、502、upstream error、WebSocket/SSE 错误。

### 1. 统一 join-cluster 节点接入和 NodePolicy

- [ ] 明确所有新增区域机器默认通过 `join-cluster.sh` 作为 k3s agent node 接入，不默认加入 control-plane / etcd。
- [ ] 设计 NodePolicy 数据模型，表达 `app-runtime`、`shared-pool`、`edge`、`dns`、`builder`、`registry-mirror`、`storage` 等角色。
- [ ] 设计 NodePolicy 到 Kubernetes Node labels / taints 的 reconcile 流程。
- [ ] 定义 edge / dns 节点的 labels、taints、resource request / limit 和禁止普通租户 app 混跑的默认策略。
- [ ] 设计 `fugue-edge` / `fugue-dns` 的 hostNetwork DaemonSet 调度规则。
- [ ] 保留 systemd 独立运行 `fugue-edge` / `fugue-dns` 的逃生口，避免 edge / DNS 完全依赖 Kubernetes 才能启动。
- [ ] 增加 CLI 或 admin endpoint 查看节点角色、实际 labels / taints、角色 reconcile 状态。

### 2. 建立 edge inventory 和认证模型

- [ ] 在 OpenAPI 里设计 edge 注册、edge heartbeat、edge route pull 的 API 契约。
- [ ] 新增 edge / edge group 数据模型和存储迁移。
- [ ] 新增 edge-scoped token 或 mTLS 身份，不复用 tenant API key。
- [ ] 新增 edge heartbeat，上报 region、public IP、mesh IP、route generation、健康状态。
- [ ] 增加 CLI 或 admin endpoint 查看 edge 列表和健康状态。

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
- [x] 确保 bundle 不包含 secret。
- [x] 增加 handler 和 contract tests。

### 5. 实现 fugue-edge shadow 模式

- [x] 新增 `cmd/fugue-edge`。
- [x] 实现 edge auth、拉取 route bundle、写本地磁盘 cache。
- [x] 实现本地 health endpoint 和 metrics。
- [x] 先不接公网流量，只验证 bundle sync。
- [x] 支持控制平面不可用时从本地 cache 启动。
- [x] 增加 `fugue-edge` shadow DaemonSet 和发布镜像链路，默认只调度到 `fugue.io/role.edge=true` 节点，不监听公网 80/443，不生成 Caddy config。
- [ ] 在已 join cluster 且带 `fugue.io/role.edge=true` 的亚洲节点部署 shadow edge，确认能长期拉取 bundle。

### 6. 接入 Caddy-backed 反代但仍不切 DNS

- [ ] `fugue-edge` 根据 route bundle 生成 Caddy dynamic config。
- [ ] Caddy admin API 只绑定 localhost 或私有接口。
- [ ] 支持 platform hostname 和 custom domain Host route。
- [ ] 支持 WebSocket、SSE、upload、stream response。
- [ ] 加 edge access log、per-route metrics、upstream latency。
- [ ] 用 `/etc/hosts` 或直接指定 Host header 做人工 canary 验证。

### 7. 建立 edge 到 runtime 的本地 upstream 路径

- [ ] 对 managed-shared / managed-owned，验证 edge 能走本地 Service DNS 或 local gateway。
- [ ] 对跨区域集群，避免 Service 随机打到远端 endpoint。
- [ ] 对 external-owned，验证 WireGuard / Tailscale / Headscale mesh upstream。
- [ ] 对 user-owned 高流量节点，定义 local edge / regional gateway 部署方式。
- [ ] 明确公网 upstream 只作为 fallback，且必须有 mTLS 或等价保护。

### 8. 自建 fugue-dns shadow 模式

- [ ] 新增 `cmd/fugue-dns` 或 CoreDNS Fugue plugin。
- [ ] 新增 DNS bundle 派生逻辑，先只包含测试 target。
- [ ] `fugue-dns` 周期性拉 bundle，写本地磁盘 cache。
- [ ] DNS query 只查内存，不查数据库。
- [ ] 拒绝 recursive query，只回答 authoritative zone。
- [ ] 在两个已 join cluster 且带 `fugue.io/role.dns=true` 的节点部署 shadow 服务，开放 UDP 53 / TCP 53。
- [ ] 直接验证：`dig @<ns1-ip> d-test.dns.fugue.pro A`。

### 9. Cloudflare 委托 dns.fugue.pro

- [ ] 在 Cloudflare `fugue.pro` zone 添加 `ns1.fugue.pro` / `ns2.fugue.pro` 的 DNS-only A/AAAA。
- [ ] 在 Cloudflare 添加 `dns.fugue.pro NS ns1.fugue.pro` 和 `dns.fugue.pro NS ns2.fugue.pro`，必须 DNS-only。
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

- [ ] 选择一个低风险 `foo.fugue.pro` app hostname。
- [ ] 保持用户可见 hostname 不变。
- [ ] 在 Cloudflare 使用 exact DNS record 将该 hostname 指到亚洲 edge。
- [ ] 不改 wildcard `*.fugue.pro` fallback。
- [ ] 验证 edge route bundle 中该 Host 指向正确 app upstream。
- [ ] 验证失败时可快速把 exact record 删除，回落到现有 wildcard / Route A。

### 12. 小批量区域切流

- [ ] 按 runtime 区域挑选一批亚洲 app。
- [ ] 优先切 app runtime 和 edge 同区域或近区域的服务。
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

- 至少两个 regional edge 节点，第一阶段至少一个在亚洲。
- 至少两个 `dns.fugue.pro` authoritative DNS 节点。
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
