# Fugue 公共流量平面重构升级方案

本文档描述 Fugue 公共应用流量平面的下一阶段重构目标。它建立在 `docs/regional-edge-data-plane.md` 已有的区域 edge / DNS / route bundle 架构之上，重点补齐 GeoDNS、CDN 边缘缓存、TLS 预热、route/DNS 一致性和跨 edge 兜底能力。

本文不是线上热修记录。所有涉及 control plane、edge proxy、Caddy、DNS、Ingress、cluster bootstrap、runtime 路由或平台级流量规则的实现，都必须回到本仓库走正式发布链路：提交到 `main`，再由 `.github/workflows/deploy-control-plane.yml` 更新远端控制平面。

## 背景

2026-05-17 对 `oaix.fugue.pro` 和 `argus.fugue.pro` 的线上链路调查暴露出几个系统性优化点：

- app 容器和 runtime 节点没有明显资源瓶颈，内部访问通常在数百毫秒内完成。
- 公网访问慢主要来自边缘接入链路、TLS 首连、跨区域 edge 到 upstream 的路径成本，以及静态资源回源。
- `argus.fugue.pro` 的 Next.js 静态资源在德国 edge 上每个请求都要付约 `0.64s - 0.67s` 的 edge 到 upstream 成本，资源瀑布会放大页面体感延迟。
- `oaix.fugue.pro` 正常公共 DNS 指向美国 edge，但强制打到德国 edge 时会得到 Caddy `404`，说明 route bundle 和 DNS answer 之间仍可能出现不一致。
- 当前只有美国和德国 edge/DNS 节点，没有香港或亚太 edge。亚洲用户访问平台应用时，TLS 终止和首跳入口仍可能落在较远区域。
- 当前 edge 更像 reverse proxy / Ingress 数据面，还不是完整 CDN。它能接入和反代，但还没有系统性承担静态资源缓存、cache purge、cache observability 和区域命中职责。

这些现象的共同结论是：Fugue 已经具备区域 edge 的雏形，但还需要把公共流量平面从“能转发”升级为“能正确选路、能本地兜底、能缓存静态资源、能预热证书、能观测和回滚”。

## 术语边界

### GeoDNS

GeoDNS 是 DNS 层的粗粒度选路能力。它通常根据客户端递归 DNS、EDNS Client Subnet、国家、大区、ASN、健康状态或权重返回不同 edge IP。

GeoDNS 不是逐请求实时测速。DNS 有 TTL 和递归缓存，不能保证每次都选到当下 RTT 最低或 TTFB 最低的 edge。

### Latency-aware steering

Latency-aware steering 是基于观测数据的性能选路。它可以输入主动探测、edge access log、client geography、ASN、runtime location 和 error rate，然后调整 DNS answer set 或权重。

第一版不应做过度复杂的实时调度。应先做健康感知和区域感知，再加入稳定的延迟评分。

### CDN / Edge cache

CDN 的核心能力是把可缓存对象存到离用户更近的 edge。对 Fugue 来说，第一阶段 CDN 不需要包含图片优化、WAF、多级缓存或复杂规则引擎，但至少要支持：

- hashed static assets 缓存，例如 `/_next/static/*`。
- 字体、JS、CSS、图片等 immutable 对象缓存。
- HTML、API、SSE、WebSocket、上传和流式响应默认不缓存。
- cache hit / miss / bypass / stale / revalidate 指标。
- app deploy 后的缓存失效策略。

### TLS pre-warm

TLS pre-warm 指在真实用户访问前，把活跃域名的证书、SNI route、Caddy certificate cache 和必要的握手路径准备好。

它和 GeoDNS 是不同问题：

- GeoDNS 负责把用户送到哪个 edge。
- TLS pre-warm 负责让被选中的 edge 第一次握手也走热路径。

## 目标不变量

Fugue 公共流量平面的目标不变量如下：

1. 所有 Fugue authoritative DNS 节点都应该能应答系统内所有公开应用域名。
2. 对任意 hostname `H`，DNS 返回的每一个 edge IP 都必须有 active route 可以服务 `H`。
3. 第一阶段默认把所有 public platform app route 展开到所有 public edge group，让每个 public edge 都能兜底服务所有公开平台应用。
4. DNS 负责选择“返回哪些 route-ready edge”，不能返回一个没有该 hostname route 的 edge。
5. Edge 可以跨区域回源，但必须在 route metadata、access log 和 metrics 中标明 runtime region、edge region、fallback 状态和 upstream latency。
6. 控制平面不可用时，edge 和 DNS 必须继续使用最后一份有效 bundle 服务已有路由。
7. 平台域名、custom domain、reserved control-plane hostname 必须有明确边界。`api.fugue.pro` 等管理面入口不应被普通 public app 策略误接管。
8. 静态资源缓存必须只缓存明确安全的 GET / HEAD 响应，不能缓存 API、鉴权响应、SSE、WebSocket、上传和流式响应。
9. 活跃域名不应依赖真实用户触发冷证书路径。On-demand TLS 只能作为兜底，不应是活跃生产域名的主路径。
10. 所有策略变更必须可观测、可回滚，并能通过 CLI 或 CI 做一致性检查。

可以把最重要的 route/DNS 不变量写成：

```text
For every public hostname H:
  every authoritative DNS node can answer H;
  every edge IP returned for H is route-ready for H;
  every route-ready edge either serves from cache or proxies to a valid upstream;
  no DNS policy may publish an answer set that is not covered by route bundles.
```

## 目标流量路径

```text
User browser
  -> Fugue authoritative DNS
  -> selected route-ready edge
  -> TLS termination with warm certificate path
  -> edge cache lookup for safe static assets
  -> app upstream through Kubernetes service / mesh / private endpoint
  -> edge response with cache and routing telemetry
```

对于静态资源，目标路径是：

```text
User browser
  -> nearest healthy route-ready edge
  -> edge cache hit
  -> response
```

对于 HTML / API / streaming 请求，目标路径仍然回源：

```text
User browser
  -> selected healthy route-ready edge
  -> no-cache / bypass
  -> app upstream
  -> streamed or dynamic response
```

## DNS 升级设计

### Full-zone 视图一致

`fugue-dns` 的每个 authoritative 节点必须持有同一份逻辑 zone 视图。不同 DNS 节点可以基于地理、健康或策略返回不同 A/AAAA answer，但不能出现某个节点不知道某个公开 hostname 的情况。

要求：

- DNS bundle 包含所有公开 hostname 的候选 edge set。
- DNS node 本地只从已签名或已校验的 bundle 回答。
- 控制平面不可用时使用 last-known-good bundle。
- DNS bundle 中每个 answer 必须引用可审计的 edge group / edge node。

### DNS answer set 必须由 route-ready edge 派生

DNS 不能独立发明 answer。它必须从 route bundle 和 edge inventory 中派生 answer set：

```text
candidate_edges(hostname)
  = healthy_edges
  ∩ dns_publishable_edges
  ∩ route_ready_edges(hostname)
  ∩ policy_allowed_edges(hostname)
```

其中 `route_ready_edges(hostname)` 是硬约束。这个集合为空时，DNS 应该 fail closed：

- 优先返回明确的 fallback edge，如果 fallback edge 也 route-ready。
- 如果没有 route-ready fallback，不应返回一个会导致 Caddy `404` 的 edge。
- 可以返回 SERVFAIL 或保留上一份仍满足不变量的 last-known-good answer，但必须暴露告警。

### GeoDNS 第一版

第一版 GeoDNS 做健康感知和区域感知，不做高频实时性能调度。

输入：

- client country / region。
- EDNS Client Subnet，若递归 DNS 提供。
- recursive resolver IP 的 GeoIP / ASN。
- edge group region / country。
- runtime region / country。
- edge health、draining、capacity。
- route policy 和 DNS policy。

输出：

- 有序 edge answer set。
- 每个 answer 的 policy reason，例如 `same_region`、`nearest_region`、`fallback_healthy`、`global_route_ready`。

默认策略：

- 用户区域有健康 route-ready edge 时，优先返回本区域 edge。
- 用户区域没有 edge 时，选择最近的健康 route-ready edge。
- 如果 app runtime 所在区域和用户所在区域不同，仍优先让用户就近 TLS 终止，但在 metrics 中标记 cross-region upstream。
- 对 latency 差异长期稳定的区域，可以通过权重修正 GeoDNS 结果。

### Latency-aware 第二版

第二版在 GeoDNS 上叠加稳定延迟评分，不做每个 DNS query 的实时测速。

数据来源：

- edge access log 的 `dns_region`、`edge_region`、`runtime_region`、`ttfb_ms`、`upstream_ms`、`cache_status`。
- control plane 主动探测不同 edge 的 TLS handshake、TTFB 和 static asset latency。
- app request compare 诊断结果。
- health check 和 error rate。

策略：

- 按 region / ASN / country 聚合，而不是按单个用户。
- 使用窗口化评分，避免短时抖动导致 DNS answer 来回切。
- 所有 latency-based answer 仍必须满足 route-ready 不变量。

## Route bundle 升级设计

### Public route 默认全 edge 兜底

在当前规模下，最稳妥的策略是：

```text
所有 public platform app route 默认下发到所有 public edge group。
DNS 再根据健康、地域和策略选择返回哪个 edge。
```

这样每个 public edge 都能服务所有公开平台应用。即使 DNS、代理、调试命令或故障切流把请求送到非首选 edge，也不会因为 route 缺失而返回 Caddy `404`。

### Selective route 只用于显式场景

Selective route bundle 可以保留，但只用于明确场景：

- custom domain canary。
- 私有或受限 route。
- 合规或数据驻留要求。
- edge group capacity 隔离。
- 故障演练或灰度。

使用 selective route 时，DNS 必须只返回包含该 route 的 edge。任何 selective route 都必须带有自动校验：

```text
dns_answer_edges(hostname) ⊆ route_ready_edges(hostname)
```

### Route metadata

route bundle 应显式包含用于观测和策略的字段：

```text
hostname
route_kind
app_id
tenant_id
runtime_id
runtime_region
runtime_edge_group_id
edge_group_id
route_policy
dns_policy
cache_policy_id
tls_policy
upstream_kind
upstream_url
service_port
route_generation
status
fallback_reason
```

`edge_group_id` 表示当前 route 被下发到哪个 edge group。`runtime_edge_group_id` 表示 app 运行位置。两者可以不同，但不同就意味着跨区域 upstream，应进入 metrics 和日志。

## CDN / Edge cache 升级设计

### 第一阶段缓存范围

第一阶段只缓存低风险静态资源：

```text
/_next/static/*
/assets/*
/static/*
*.js
*.css
*.woff
*.woff2
*.ttf
*.otf
*.png
*.jpg
*.jpeg
*.webp
*.svg
*.ico
```

缓存前提：

- method 是 `GET` 或 `HEAD`。
- response status 是 `200`、`203`、`204`、`206`、`301`、`302`、`304`、`404` 中明确允许的子集。第一版建议只缓存 `200` 和 immutable asset 的 `404` 短 TTL。
- response 不含 `Set-Cookie`。
- request 不含 `Authorization`，除非 route cache policy 明确允许并能隔离 cache key。
- response `Cache-Control` 没有 `private`、`no-store`。
- path 不匹配 API、SSE、WebSocket、upload 或 streaming route。

### 默认缓存策略

Next.js hashed asset：

```text
path: /_next/static/*
ttl: 1y
browser_cache: public, max-age=31536000, immutable
edge_cache: public, max-age=31536000, immutable
purge: not required for content-hashed filenames
```

字体和带 hash 的 assets：

```text
ttl: 1y
browser_cache: public, max-age=31536000, immutable
edge_cache: public, max-age=31536000, immutable
purge: not required if filename is content-addressed
```

HTML：

```text
edge_cache: bypass
browser_cache: no-cache or short revalidate
```

API / streaming / upload：

```text
edge_cache: bypass
browser_cache: app-defined, default no-store for authenticated responses
```

### Cache key

默认 cache key：

```text
scheme
host
normalized path
normalized query
accept-encoding bucket
```

第一版不要把所有 headers 都放进 key。只允许白名单 `Vary`，例如 `Accept-Encoding`。如果需要支持 image format negotiation，再显式加入 `Accept` bucket。

### Purge 和 deploy generation

对 content-hashed assets，优先依赖 immutable filename，不做 purge。

对非 hashed assets，必须引入 deploy generation 或 app generation：

```text
cache namespace = app_id + deployment_id
```

新 deploy 后：

- 新 asset 进入新 namespace。
- 旧 namespace 保留短窗口，用于旧 HTML 仍引用旧 chunk 的情况。
- 过期后自动清理。

### 实现形态

可以选择以下实现之一：

- 在 `fugue-edge` 内实现轻量 HTTP cache，再由 Caddy 只做 TLS 和前置代理。
- 使用 Caddy cache 插件或 sidecar cache，但必须确保构建、配置和观测可控。
- 在 edge 前面接外部 CDN。这个路径可以作为过渡，但如果目标是 Fugue 平台能力，长期仍应在 Fugue edge 模型中有一等 cache policy。

无论实现形态如何，cache policy 都应来自控制平面 bundle，而不是手写在线上 Caddy 配置里。

### Cache observability

edge access log 和 metrics 至少应包含：

```text
cache_status=hit|miss|bypass|stale|revalidated|error
cache_key_hash
cache_policy_id
edge_region
runtime_region
upstream_ms
response_bytes
asset_class
```

核心指标：

- static asset hit ratio。
- cache miss upstream latency。
- cache storage usage。
- cache evictions。
- cache errors。
- per-host top miss paths。

## TLS / Certificate 升级设计

### 平台域名

`*.fugue.pro` 和 `fugue.pro` 这类平台域名应优先使用静态预签发证书或由 Fugue DNS-01 自动续期的 wildcard 证书。

要求：

- wildcard 证书由 Fugue ACME DNS-01 challenge API 管理。
- 证书进入 Kubernetes Secret 或 edge certificate store 后，由 edge bundle 引用。
- 每个 public edge 都能加载平台证书。
- 证书过期、加载失败、edge 未加载等状态进入 edge heartbeat。

### Custom domain

Custom domain 仍可保留 on-demand TLS 兜底，但活跃域名上线流程应改为显式 activation：

```text
domain verified
  -> create certificate intent
  -> present DNS-01 or HTTP-01 challenge
  -> issue certificate
  -> distribute or make available to route-ready edges
  -> warm SNI handshake on every DNS-answerable edge
  -> mark tls_status=ready
  -> publish DNS answer
```

关键点：

- DNS answer 不应早于 route-ready 和 tls-ready，除非这是明确的 canary。
- On-demand TLS 失败不能只留在 Caddy 日志里，必须回报到控制平面。
- 活跃域名应有证书预热任务，不能等第一个真实用户触发签发或冷加载。

### Pre-warm 行为

Pre-warm 任务应对每个活跃 hostname 和每个可能被 DNS 返回的 edge 做：

```text
TCP connect edge:443
TLS ClientHello with SNI=hostname
verify certificate hostname and expiry
optional HTTP GET /.well-known/fugue/edge-warmup or HEAD /
record tls_handshake_ms and edge certificate fingerprint
```

结果写入：

```text
hostname
edge_id
edge_group_id
tls_status
cert_fingerprint
cert_not_after
tls_handshake_ms
last_warmed_at
last_error
```

只有 warmup 通过的 edge 才进入该 hostname 的正常 DNS answer set。失败 edge 可保留为 emergency fallback，但必须带告警和策略标记。

## 亚太 edge / DNS 升级

当前美国和德国 edge 不能覆盖亚洲用户的首跳体验。下一阶段应新增香港或亚太 edge group，例如：

```text
edge-group-country-hk
edge-group-region-apac
```

接入要求：

- 节点由 NodePolicy 标记 `fugue.io/role.edge=true` 和 `fugue.io/role.dns=true`。
- edge / DNS DaemonSet 或 systemd 逃生口都能运行。
- UDP/TCP 53、TCP 80、TCP 443 可达。
- route bundle、DNS bundle、certificate cache 都有本地 last-known-good。
- DNS status preflight 通过。
- edge heartbeat 上报健康、bundle version、Caddy route count、cache status 和 certificate status。

DNS 层可以先把亚洲 client answer 到香港 edge。如果 app runtime 在美国或德国，第一阶段仍然允许香港 edge 跨区 upstream，但必须观测跨区 upstream latency。后续再结合 runtime placement，把亚洲用户和亚洲 runtime 尽量收敛到同一 cell。

## 控制平面模型补充

为支撑上述能力，建议把以下对象或字段一等化。

### DNSAnswerPolicy

```text
hostname
policy_kind = global | geo | weighted | latency_aware | pinned | disabled
allowed_edge_groups
preferred_edge_groups
fallback_edge_groups
ttl_seconds
ecs_enabled
health_required
route_ready_required
```

### CachePolicy

```text
cache_policy_id
hostname_scope
path_patterns
method_allowlist
status_allowlist
ttl_seconds
stale_while_revalidate_seconds
browser_cache_control
edge_cache_control
bypass_on_authorization
bypass_on_cookie
vary_allowlist
purge_mode
```

### TLSActivation

```text
hostname
domain_kind = platform | custom
tls_policy = static_wildcard | preissued | on_demand_fallback
certificate_status
answerable_edge_groups
warmed_edge_groups
last_warmed_at
last_error
```

### EdgePerformanceSample

```text
edge_id
edge_group_id
hostname
client_region
client_asn
runtime_region
route_generation
cache_status
dns_policy
tls_handshake_ms
ttfb_ms
upstream_ms
total_ms
status_code
sampled_at
```

这些字段不一定要一次进入公开 API，但至少应进入内部 store、diagnostics 和 admin CLI。

## 分阶段迁移计划

### 阶段 0：固化当前诊断基线

目标：

- 把 `oaix`、`argus` 这类真实路径的 public/internal 对比变成可重复诊断。

工作项：

- 扩展 `fugue app request compare` 或新增 edge path compare，支持强制指定 edge IP / edge group。
- 在 edge access log 中稳定输出 `edge_group_id`、`runtime_region`、`upstream_ms`、`cache_status`、`tls_policy`。
- 增加 `fugue admin edge route-check <hostname>`，列出每个 edge 是否 route-ready。
- 增加 `fugue admin dns answer-check <hostname>`，列出每个 DNS node 会返回哪些 edge，以及是否满足 route-ready。

验收：

- 能一条命令回答“这个 hostname 会被 DNS 返回到哪些 edge，这些 edge 是否都有 route，是否 TLS ready”。

### 阶段 1：修正 route/DNS 一致性

目标：

- 彻底消除 “DNS 可能送到没有 route 的 edge” 这一类问题。

工作项：

- 在 DNS bundle 生成前校验 answer set 是 route-ready set 的子集。
- 对 public platform app route 默认展开到所有 public edge group。
- 对 selective route 增加强校验和告警。
- 禁止发布会导致 route drop 的 bundle，除非显式 emergency override。

验收：

- 对所有 public hostname，强制 `curl --resolve <hostname>:443:<edge_ip>` 到任何 DNS 可能返回的 edge，都不会因为 route 缺失得到 Caddy `404`。
- `fugue admin dns status` 增加 route/DNS invariant 检查，并在 CI 或发布 smoke 中执行。

### 阶段 2：TLS 预热和证书状态闭环

目标：

- 活跃域名的首个真实用户不再触发冷证书路径。

工作项：

- 平台 wildcard 证书续期迁到 Fugue ACME DNS-01 hook。
- custom domain 增加 certificate intent / activation 状态。
- 对 DNS 可能返回的 edge 执行 SNI warmup。
- edge heartbeat 上报证书加载和 warmup 状态。
- DNS answer 生成时过滤未 tls-ready 的 edge，或显式标记 fallback。

验收：

- 活跃 hostname 的每个 DNS-answerable edge 都有 `tls_status=ready`。
- cold-start TLS handshake p95 显著下降，并且 TLS 失败能在控制平面直接看到。

### 阶段 3：GeoDNS 健康感知选路

目标：

- 用户优先进入更近且健康的 route-ready edge。

工作项：

- 在 DNS bundle 中加入 geo answer policy。
- 接入 GeoIP / ECS 解析逻辑。
- 对 edge group 配置 region / country / priority / weight。
- DNS answer 输出 policy reason。
- TTL 初期使用 `60s - 120s`，避免策略错误长期缓存。

验收：

- 亚洲 client 默认返回 APAC/HK edge，欧洲 client 默认返回 DE/EU edge，美国 client 默认返回 US edge。
- 首选 edge 不健康时自动回退到健康 route-ready edge。
- 回退事件在 metrics 和 audit 中可见。

### 阶段 4：第一版 CDN / Edge cache

目标：

- 静态资源不再每次回源，尤其是 Next.js chunk、CSS 和 font。

工作项：

- 增加 CachePolicy bundle。
- 在 edge 实现静态资源 cache。
- 对 `/_next/static/*`、字体和 hashed assets 默认启用 immutable cache。
- 增加 cache namespace / deployment generation。
- 增加 cache hit/miss metrics 和 admin diagnosis。

验收：

- `argus` 这类 Next.js app 的 `_next/static/*` 第二次请求应命中 edge cache。
- 静态资源 cache hit 的 edge-side latency 应接近本地磁盘/内存返回，而不是每个请求约 `0.65s` 回源。
- HTML、API、SSE、WebSocket、上传和流式接口不能被缓存。

### 阶段 5：APAC/HK edge 上线

目标：

- 亚洲用户拥有就近 TLS 终止和缓存入口。

工作项：

- 通过 NodePolicy 接入香港或亚太 edge/DNS 节点。
- 完成 route bundle、DNS bundle、Caddy、certificate cache 和 edge cache 验证。
- 加入 DNS answer policy。
- 对平台核心 hostname 和低风险 app 做 canary，再扩展到默认入口。

验收：

- 亚洲探测点访问 public app 的 DNS answer 指向 APAC/HK route-ready edge。
- APAC/HK edge 对所有 public platform app route 具备兜底服务能力。
- APAC/HK edge cache 命中率和 upstream latency 可观测。

### 阶段 6：Latency-aware steering

目标：

- 在 GeoDNS 基础上修正“地理近但真实慢”的情况。

工作项：

- 采集按 region / ASN 聚合的 edge latency 样本。
- 建立稳定评分窗口和权重调整机制。
- 对 DNS answer set 做权重或优先级修正。
- 防止短时抖动造成频繁切换。

验收：

- 像“当前网络访问 US edge 比 DE edge 快”这类稳定现象，可以被策略捕获并调整 answer。
- 所有 latency-aware answer 仍满足 route-ready 和 TLS-ready 不变量。

## TODO list

### P0：正确性和诊断底座

- [x] 增加 route/DNS invariant 检查：任意 DNS answer edge 必须是该 hostname 的 route-ready edge。
- [x] 在 DNS bundle 发布前执行 invariant gate，失败时拒绝发布新 bundle，继续使用 last-known-good。
- [x] 增加 `fugue admin edge route-check <hostname>`，输出每个 edge group / edge node 是否拥有该 hostname route、upstream 和 TLS policy。
- [x] 增加 `fugue admin dns answer-check <hostname>`，输出每个 DNS node 对该 hostname 的 answer set、policy reason 和 route-ready 校验结果。
- [x] 扩展 `fugue app request compare`，支持指定 `--edge-ip`、`--edge-group` 或 `--resolve`，用于复现不同 edge 路径。
- [x] 在发布 smoke 中加入 `curl --resolve` 检查，覆盖所有 DNS 可能返回的 edge IP，防止 Caddy `404` 重新出现。
- [x] 在 edge access log 中补齐 `edge_group_id`、`runtime_region`、`runtime_edge_group_id`、`route_generation`、`fallback_reason`。
- [x] 在 `fugue admin dns status` 中显示 route/DNS invariant 状态，而不只检查 DNS 节点健康。

### P1：Public route 全 edge 兜底

- [x] 明确 public platform app route 默认下发到所有 public edge group。
- [x] 保留 selective route，但限制在 custom domain canary、私有 route、合规隔离和灰度场景。
- [x] 为 selective route 增加 DNS 发布保护：DNS answer set 必须是 selective route-ready set 的子集。
- [x] 在 route bundle 中显式区分 `runtime_edge_group_id` 和当前下发的 `edge_group_id`。
- [x] 对跨区域 upstream 增加观测字段和告警阈值，避免“能兜底”被误认为“最优路径”。
- [x] 给 `oaix.fugue.pro` 这类真实 hostname 建立回归用例，验证任意 public DNS-answerable edge 都不会因 route 缺失返回 `404`。

### P1：TLS 预热和证书闭环

- [x] 把平台 wildcard 证书续期正式切到 Fugue ACME DNS-01 hook。
- [x] 为 custom domain 建立 certificate intent / activation 状态机。
- [x] 在 DNS 发布前确认 hostname 在目标 edge 上 `tls_status=ready`，否则不进入普通 answer set。
- [x] 为每个活跃 hostname 和每个 DNS-answerable edge 执行 SNI warmup。
- [x] 在 warmup 中记录 `tls_handshake_ms`、certificate fingerprint、expiry 和 last error。
- [x] 让 edge heartbeat 上报平台证书、custom domain 证书和 Caddy certificate cache 状态。
- [x] 增加 TLS cold-path 告警：活跃 hostname 出现 on-demand 签发或冷加载时必须可见。

### P2：第一版 CDN / Edge cache

- [x] 定义 `CachePolicy` 内部模型，覆盖 path pattern、method、status、TTL、bypass 条件和 purge mode。
- [x] 在 route bundle 或独立 cache bundle 中下发 cache policy。
- [x] 对 `/_next/static/*`、hashed JS/CSS、font 和常见静态图片启用 immutable edge cache。
- [x] 默认 bypass HTML、API、SSE、WebSocket、上传、流式响应和带 `Authorization` 的请求。
- [x] 定义 cache key：scheme、host、normalized path、normalized query、accept-encoding bucket。
- [x] 引入 app deployment generation / cache namespace，避免非 hashed asset 在新旧部署之间互相污染。
- [x] 在 edge access log 和 metrics 中输出 `cache_status=hit|miss|bypass|stale|revalidated|error`。
- [x] 针对 `argus.fugue.pro` 建立验收用例：`/_next/static/*` 第二次访问必须命中 edge cache。

### P2：GeoDNS 健康感知选路

- [x] 在 DNS answer policy 中引入 region、country、priority、weight 和 TTL。
- [x] 接入 GeoIP 和 EDNS Client Subnet，缺失 ECS 时退回 recursive resolver GeoIP。
- [x] DNS answer 输出 policy reason，例如 `same_region`、`nearest_region`、`fallback_healthy`。
- [x] 对 unhealthy / draining edge 自动从 answer set 移除，除非进入显式 emergency fallback。
- [x] 初期 TTL 使用 `60s - 120s`，便于策略回滚和故障切换。
- [x] 建立地区探测脚本，验证亚洲、欧洲、美国 client 得到预期 answer。

### P3：APAC / HK edge 上线

- [ ] 选择 APAC/HK 节点并通过 NodePolicy 标记 edge + DNS 角色。
- [ ] 验证 UDP/TCP 53、TCP 80、TCP 443 可达。
- [ ] 验证 APAC/HK edge route bundle、DNS bundle、Caddy config、certificate cache 和 last-known-good cache。
- [ ] 把 APAC/HK edge 加入 DNS answer policy，先低风险 canary，再扩大到默认平台入口。
- [ ] 对亚洲探测点验证 TLS handshake、TTFB、static asset cache hit 和 upstream latency。
- [ ] 明确 APAC/HK edge 跨区回源的告警阈值，避免长期把跨洋 upstream 当作正常状态。

### P3：Latency-aware steering

- [ ] 定义 `EdgePerformanceSample` 采集格式。
- [ ] 按 country / region / ASN 聚合 edge latency、TTFB、upstream latency、cache hit ratio 和 error rate。
- [ ] 建立稳定评分窗口，避免短时抖动导致 DNS answer 频繁切换。
- [ ] 允许 DNS policy 用权重修正纯 GeoDNS 的错误判断。
- [ ] 对“地理近但真实慢”的路径输出解释字段，方便排障。
- [ ] 保证 latency-aware steering 永远不能绕过 route-ready、TLS-ready 和 health gate。

### P4：发布和回滚

- [ ] 给每个阶段补 release checklist，明确发布前 smoke、发布后观测和回滚动作。
- [ ] 所有 control plane / edge / DNS 改动必须通过 GitHub Actions 控制平面发布链路。
- [ ] 禁止把手工 SSH patch Caddy、手工改 DNS bundle、手工 patch Deployment 当成正式修复。
- [ ] 对每个新 policy 增加 dry-run / plan 输出，先展示 DNS answer 和 route bundle 变化再应用。
- [ ] 维护一组真实域名 smoke：`oaix.fugue.pro`、`argus.fugue.pro`、一个 custom domain、一个 streaming app。

## 验收清单

发布任一阶段前，至少验证：

- `fugue admin cluster node-policy status` 无相关 drift。
- `fugue admin edge nodes ls` 中 public edge healthy，bundle version 非空，cache 状态 ready。
- `fugue admin dns status` pass，DNS 节点 healthy，bundle version 稳定。
- 每个 public hostname 的 DNS answer set 都是 route-ready edge set 的子集。
- 对 DNS 可能返回的每个 edge 做 `curl --resolve`，不能出现 route 缺失导致的 Caddy `404`。
- 活跃 hostname 的 TLS warmup 在每个 DNS-answerable edge 上通过。
- static asset cache hit / miss / bypass 可以在 metrics 中区分。
- HTML、API、SSE、WebSocket、上传和 streaming 请求明确 bypass cache。
- 控制平面短暂不可用时，edge / DNS 使用 last-known-good bundle 继续服务已有路由。

## 推荐优先级

推荐按风险和收益排序：

1. 先做 route/DNS invariant 检查，避免 DNS 返回无 route edge。
2. 再把 public platform app route 展开到所有 public edge，让每个 public edge 都能兜底服务所有公开应用。
3. 给活跃域名加 TLS pre-warm 和证书状态闭环，降低首连不确定性。
4. 给静态资源加第一版 edge cache，优先解决 `/_next/static/*` 和 font 回源瀑布。
5. 新增 APAC/HK edge/DNS，并用 GeoDNS 把亚洲用户导到就近 edge。
6. 最后叠加 latency-aware steering，用真实观测修正纯 GeoDNS 的误判。

这个顺序的原因是：route/DNS 一致性是正确性底座；TLS 和 cache 是直接改善体感延迟；GeoDNS 和 APAC edge 是区域体验提升；latency-aware steering 需要前面这些观测和不变量稳定后再做。

## 明确不做

第一轮升级不做：

- 每个 DNS query 的实时测速。
- 复杂多级 CDN、图片优化、WAF、Bot 管理。
- 对用户应用提供自动跨区域数据复制或应用级 failover。
- 线上手工 patch Caddy、Deployment 或 DNS bundle 作为正式修复。
- 为单个 app hostname 写死特殊逻辑。

## 设计结论

Fugue 下一阶段公共流量平面的核心不是单独“加 GeoDNS”或“加 CDN”，而是建立一组可验证的不变量：

```text
DNS knows every public hostname.
DNS only returns route-ready edges.
Every public edge can serve every public platform app as fallback.
Static assets are cached at edge when safe.
Active domains are TLS-warm before users arrive.
GeoDNS chooses a good edge, latency-aware steering later corrects geography.
Control plane publishes intent; edge and DNS serve from verified local bundles.
```

只要这组不变量成立，`oaix` 打到非首选 edge 返回 `404`、`argus` 每个静态资源都跨区回源、活跃域名首连走冷 TLS 路径、亚洲用户只能进欧美 edge 这些问题都会有明确的架构归属和可测试的修复路径。
