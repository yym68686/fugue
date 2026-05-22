# Fugue Edge Origin Pool 高并发升级方案

本文档描述 Fugue 公共 edge 数据面在上万 RPS 场景下的 origin 连接池、过载保护、缓存回源抑制和协议升级方案。

本文不是线上热修记录。所有影响 control plane、edge proxy、Caddy、Ingress、runtime 路由或平台级流量规则的实现，都必须回到本仓库走正式发布链路：提交到 `main`，再由 `.github/workflows/deploy-control-plane.yml` 更新远端控制平面。

## 背景

2026-05-18 对 `oaix.fugue.pro` 和 `argus.fugue.pro` 的复测显示：

- edge 到 origin 的细粒度 timing 已经可观测，包括 cache lookup、origin connect、origin TTFB、origin total 和 response write。
- edge proxy 已经从每请求创建 transport 改成 Service 级共享 transport，线上日志已经能看到 `origin_conn_reused=true`。
- `argus /` 已能通过 `html-documents-short-v1` 命中 HTML 短 TTL edge cache，cache hit / stale 时 edge 侧基本不再回源。
- `oaix /` 返回 `cache-control: no-store, max-age=0`，edge 必须安全绕过 HTML cache。
- 剩余慢点主要来自 client 到 edge 的 TLS / 网络路径、少量 origin 新建连接，以及不可缓存动态 HTML / API 的 origin TTFB。

这些改造已经降低了当前低中流量下的明显回源成本，但如果 Fugue 要支撑单 edge 或单热 origin 上万 RPS，需要从“共享 transport”升级为“有边界、有公平性、有过载退让能力的 origin pool 数据面”。

## 设计目标

1. 支持单 edge 上万 RPS 的公开流量入口，不依赖无限连接或无限队列。
2. 在高并发下最大化 origin connection reuse，降低 `origin_connect_ms` 的出现频率。
3. 保护 edge 节点资源，避免文件描述符、内存、conntrack 或 upstream pod 被单个热 app 打满。
4. 按 origin / app / tenant 做基本公平性，避免一个热点 route 抢占整个 edge。
5. cache miss / stale revalidate 不产生回源尖峰。
6. route generation、upstream URL 或 rollout 变化后，旧连接池能主动收敛。
7. 所有关键行为都必须有 metrics、logs 和可回滚配置。

## 非目标

- 第一阶段不自研完整 L7 proxy，不替换现有 Go reverse proxy / Caddy TLS 入口。
- 第一阶段不直接开启 upstream HTTP/2 / h2c 的自动模式，避免影响现有 app 协议兼容性。
- 不做无限连接池，不做无限排队。
- 不把 app 业务慢请求伪装成 edge 可解决的问题。动态接口仍需要 app 自身优化。

## 高并发容量模型

上万 RPS 下不能凭经验设置连接数，必须用并发模型估算：

```text
origin_inflight ~= RPS * origin_latency_seconds
```

示例：

```text
10,000 RPS * 0.1s origin latency = 1,000 origin 并发
10,000 RPS * 0.3s origin latency = 3,000 origin 并发
```

如果 edge 到 origin 使用 HTTP/1.1，活跃并发通常需要接近相同数量的连接。如果未来支持 upstream HTTP/2 / h2c，多路复用可以减少连接数量，但必须在 route 或 app 能力维度显式启用。

## 目标架构

```text
User
  -> Caddy TLS / HTTP入口
  -> fugue-edge route lookup
  -> cache lookup
  -> per-origin limiter
  -> per-origin transport pool
  -> Kubernetes Service / runtime upstream
  -> response / cache store / metrics
```

关键原则：

- 每个 origin 使用共享 pool，而不是每个请求创建 transport。
- 每个 origin 有独立并发上限和短队列。
- 全局有资源上限，不能让单 origin 抢光 edge。
- cache miss 使用 singleflight，stale response 优先快速返回。
- route generation 变化时主动淘汰旧 pool。

## P0：高边界共享 Transport

目标：先把当前共享 transport 的连接池参数从 Go 默认值提升到适合高并发 edge 的有界配置。

推荐默认值：

```go
MaxIdleConns:          32768
MaxIdleConnsPerHost:   512
MaxConnsPerHost:       2048
IdleConnTimeout:       3 * time.Minute
TLSHandshakeTimeout:   10 * time.Second
ExpectContinueTimeout: 1 * time.Second
ForceAttemptHTTP2:     false
```

新增配置：

```text
FUGUE_EDGE_ORIGIN_MAX_IDLE_CONNS
FUGUE_EDGE_ORIGIN_MAX_IDLE_CONNS_PER_HOST
FUGUE_EDGE_ORIGIN_MAX_CONNS_PER_HOST
FUGUE_EDGE_ORIGIN_IDLE_CONN_TIMEOUT
FUGUE_EDGE_ORIGIN_RESPONSE_HEADER_TIMEOUT
```

实现要求：

- 在 `internal/config` 中增加 edge origin pool 配置字段。
- 在 `internal/edge/service.go` 的 `newDefaultEdgeProxyTransport()` 应用配置。
- 默认值必须保守但比 Go 默认值更适合 edge 高并发。
- 配置值必须可通过 Helm / systemd / env 下发。

## P1：Per-origin Pool Manager

目标：把连接池生命周期从全局共享 transport 细化为 per-origin 管理，便于按 origin 限流、观测和回收。

建议结构：

```go
type originPoolKey struct {
    Scheme          string
    Host            string
    RouteGeneration string
}

type originPoolManager struct {
    mu    sync.Mutex
    pools map[originPoolKey]*originPool
}

type originPool struct {
    key       originPoolKey
    transport *http.Transport
    limiter   *originLimiter
    createdAt time.Time
    lastUsedAt time.Time
}
```

pool key 建议：

```text
target.Scheme + target.Host + route.RouteGeneration
```

理由：

- 同一 upstream service 复用连接。
- route generation 变化后自然切换新 pool。
- 旧 generation 的 idle 连接可以主动 `CloseIdleConnections()`。

## P2：Route 变更主动清理旧连接

目标：避免 rollout、route generation 或 upstream 变化后，旧连接长期粘住旧 service / pod。

触发点：

- route bundle apply 成功后。
- route generation 改变。
- upstream URL 改变。
- route 被删除或状态变为 inactive。

行为：

```go
pool.transport.CloseIdleConnections()
delete(manager.pools, key)
```

新增 metrics：

```text
fugue_edge_origin_pool_active
fugue_edge_origin_pool_evictions_total
fugue_edge_origin_pool_close_idle_total
fugue_edge_origin_pool_created_total
```

日志字段：

```text
origin_pool_key
origin_pool_eviction_reason
route_generation_old
route_generation_new
upstream_old
upstream_new
```

## P3：过载保护和短队列

目标：上万 RPS 下保护 edge 和 origin，避免无限排队导致尾延迟爆炸。

每个 origin 增加并发 limiter：

```go
type originLimiter struct {
    sem chan struct{}
}
```

推荐默认配置：

```text
FUGUE_EDGE_ORIGIN_MAX_INFLIGHT_PER_HOST=4096
FUGUE_EDGE_ORIGIN_QUEUE_TIMEOUT=50ms
FUGUE_EDGE_ORIGIN_OVERLOAD_STATUS=503
```

行为：

- 请求进入 origin 前尝试获取 token。
- 获取成功后进入 reverse proxy。
- `QueueTimeout` 内拿不到 token，返回 `503` 或 `429`。
- 响应带：

```http
Retry-After: 1
X-Fugue-Origin-Overloaded: 1
```

新增 metrics：

```text
fugue_edge_origin_inflight
fugue_edge_origin_queue_wait_seconds
fugue_edge_origin_overload_total
fugue_edge_origin_limiter_acquire_total{result="acquired|timeout|canceled"}
```

公平性要求：

- limiter 必须按 origin 生效。
- 后续可扩展为按 tenant / app / route 的 token bucket。
- 单个 origin 超限不能拖垮其他 origin。

## P4：Cache Singleflight / Stale Revalidate

目标：避免 HTML / static cache 过期瞬间多个请求同时回源。

行为：

- `hit`：直接返回。
- `stale`：优先立即返回 stale，并后台 revalidate。
- `miss`：同一 cache key 只允许一个请求回源，其余请求短暂等待或返回 stale。
- `origin error`：如果 stale 可用，返回 stale-if-error。

建议结构：

```go
type cacheFillGroup struct {
    group singleflight.Group
}
```

新增配置：

```text
FUGUE_EDGE_CACHE_FILL_WAIT_TIMEOUT=50ms
FUGUE_EDGE_CACHE_STALE_REVALIDATE_BACKGROUND=true
FUGUE_EDGE_CACHE_STALE_IF_ERROR=true
```

新增 metrics：

```text
fugue_edge_route_cache_fill_total{result="leader|shared|timeout|error"}
fugue_edge_route_cache_revalidate_total{result="success|error"}
fugue_edge_route_cache_stale_served_total{reason="expired|origin_error|fill_in_progress"}
```

## P5：Upstream HTTP/2 / h2c 评估

目标：在高 RPS 下减少 HTTP/1.1 连接数量，提升长连接和多路复用效率。

当前 edge proxy 保守设置 `ForceAttemptHTTP2=false`。后续应做成显式配置，而不是全局自动启用：

```text
FUGUE_EDGE_ORIGIN_HTTP2_MODE=off|tls|h2c|auto
```

建议策略：

- 默认 `off`。
- 对明确支持 h2c 的 runtime / route 显式开启。
- 先在 canary route 和压测环境验证。
- 需要观测 h2 stream 数、连接数、错误率和 app 协议兼容性。

## 节点资源和系统参数

高并发 edge 需要和节点资源一起设计。每个 edge 节点至少要核对：

```text
ulimit -n
fs.file-max
net.ipv4.ip_local_port_range
net.ipv4.tcp_tw_reuse
nf_conntrack_max
pod memory limit
pod ephemeral storage
node conntrack usage
```

连接池上限必须小于节点和容器资源上限，不能只按业务 RPS 推导。

## 压测要求

上线前需要覆盖：

- 单 origin 1k / 5k / 10k RPS。
- 多 origin 同时流量，验证公平性。
- cache hit、cache miss、stale revalidate 三种路径。
- origin 延迟从 50ms 到 500ms 的尾延迟变化。
- route generation 变更时旧 pool 清理。
- upstream pod rollout 时连接收敛。
- origin 5xx / timeout 时 limiter、stale-if-error 和 fallback 行为。

建议关注指标：

```text
origin_conn_reused ratio
origin_connect_ms p50/p95/p99
origin_ttfb_ms p50/p95/p99
origin_inflight
origin_queue_wait p95/p99
edge memory
edge fd usage
node conntrack usage
cache hit ratio
cache fill shared ratio
overload total
```

## 发布顺序

1. P0 单独发布，验证连接复用率和 FD / memory 稳定性。
2. P1 / P2 一起发布，验证 route generation 变更后 pool 收敛。
3. P3 先以较高阈值和 shadow metrics 发布，再启用实际 overload response。
4. P4 先覆盖 HTML document cache，再扩展到 static / API safe cache。
5. P5 只在 canary route 开启，压测通过后再扩大。

每个阶段都必须：

- `make test` 通过。
- 发布到 `main`。
- GitHub Actions 控制平面升级成功。
- `fugue admin platform autonomy status --json` 通过。
- 线上 edge image 和 control plane live version 对齐。
- 复测 `oaix.fugue.pro`、`argus.fugue.pro` 和至少一个 custom domain。

## Todo List

### 已完成基线

- [x] 增加 edge origin timing：cache lookup、origin connect、origin TTFB、origin total、response write。
- [x] edge proxy 改为 Service 级共享 transport，避免每个请求创建全新 transport。
- [x] 增加 HTML document 短 TTL cache policy。
- [x] 验证 `argus /` 可以命中 HTML edge cache。
- [x] 验证 TLS session resumption 可用。

### P0：高边界共享 Transport

- [ ] 增加 `FUGUE_EDGE_ORIGIN_MAX_IDLE_CONNS` 配置。
- [ ] 增加 `FUGUE_EDGE_ORIGIN_MAX_IDLE_CONNS_PER_HOST` 配置。
- [ ] 增加 `FUGUE_EDGE_ORIGIN_MAX_CONNS_PER_HOST` 配置。
- [ ] 增加 `FUGUE_EDGE_ORIGIN_IDLE_CONN_TIMEOUT` 配置。
- [ ] 增加 `FUGUE_EDGE_ORIGIN_RESPONSE_HEADER_TIMEOUT` 配置。
- [ ] 将默认 transport 参数提升到高边界有界池化。
- [ ] 增加配置解析和默认值测试。
- [ ] 线上验证 `origin_conn_reused` 比例提升，`origin_connect_ms` 次数下降。

### P1：Per-origin Pool Manager

- [ ] 新增 `originPoolKey`、`originPool`、`originPoolManager`。
- [ ] 按 `scheme + host + route_generation` 选择 transport pool。
- [ ] `newEdgeReverseProxy()` 改为从 pool manager 获取 transport。
- [ ] 增加 pool 创建、复用和并发安全测试。
- [ ] 增加 pool active / created metrics。

### P2：Route 变更清池

- [ ] 在 route bundle apply 成功后比较新旧 route generation。
- [ ] upstream URL 改变时关闭旧 idle connections。
- [ ] route 删除或 inactive 时淘汰旧 pool。
- [ ] 增加 pool eviction metrics 和日志字段。
- [ ] 增加 rollout / route generation 变更回归测试。

### P3：过载保护

- [ ] 增加 per-origin inflight limiter。
- [ ] 增加 `FUGUE_EDGE_ORIGIN_MAX_INFLIGHT_PER_HOST` 配置。
- [ ] 增加 `FUGUE_EDGE_ORIGIN_QUEUE_TIMEOUT` 配置。
- [ ] 超限时返回 `503` 或 `429`，并带 `Retry-After`。
- [ ] 增加 queue wait / overload / acquire result metrics。
- [ ] 增加不同 origin 公平性测试。
- [ ] 压测验证短队列不会放大 p99 延迟。

### P4：Cache Singleflight / Stale Revalidate

- [ ] 对相同 cache key 的 miss 增加 singleflight。
- [ ] stale 命中时立即返回并后台 revalidate。
- [ ] origin error 时支持 stale-if-error。
- [ ] 增加 cache fill / revalidate / stale served metrics。
- [ ] 增加 HTML cache 过期尖峰回归测试。
- [ ] 线上验证 `argus /` cache 过期时不会产生多请求回源尖峰。

### P5：Upstream HTTP/2 / h2c

- [ ] 增加 `FUGUE_EDGE_ORIGIN_HTTP2_MODE` 配置。
- [ ] 明确 route / runtime 是否支持 upstream h2c。
- [ ] 先在 canary route 开启 h2c。
- [ ] 增加 HTTP/1.1 与 h2c 压测对比。
- [ ] 增加协议兼容性回滚开关。

### 线上验证

- [ ] 每阶段发布后确认 GitHub Actions deploy-control-plane 成功。
- [ ] 确认 control plane `live_version` 等于目标 commit。
- [ ] 确认 US / DE edge DaemonSet 均已更新到目标 image。
- [ ] 复测 `oaix.fugue.pro` 正常动态绕过缓存且 origin 连接复用。
- [ ] 复测 `argus.fugue.pro` HTML cache hit / stale 路径。
- [ ] 记录优化前后 `TTFB`、`total`、`origin_connect_ms`、`origin_ttfb_ms` 对比。
