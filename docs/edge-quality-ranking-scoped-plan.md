# Edge scoped quality ranking optimization plan

本文档描述 Fugue edge 质量排名的系统性优化方案。目标是把当前偏全局、偏 DNS 候选排序的质量逻辑，升级为按服务、流量类型、客户端网络作用域、时间窗口和 edge 节点分层的可解释 ranking framework。

本文不是线上热修记录。所有涉及 control plane、edge proxy、edge-front、DNS、route bundle、cluster bootstrap、runtime 路由或平台级流量规则的实现，都必须回到本仓库走正式发布链路：提交到 `main`，再由 `.github/workflows/deploy-control-plane.yml` 更新远端控制平面。

## 背景

近期线上调查暴露出几个 edge 排名相关问题：

- 新 edge 加入后需要小比例真实流量验证质量，否则无法知道它在真实运营商线路上的表现。
- 某些请求在 edge 侧 request body 读取非常慢，不能只看总耗时，必须看读取速率、读阻塞、最小窗口速率和 TCP 损伤信号。
- edge 质量强烈依赖客户端所在运营商、国家/地区、请求类型和时间段，不能只用一个全局平均分。
- 当前 Fugue 已采集较多 request body、origin、cache、egress 和 saturation 指标，但这些指标还没有被系统性地按作用域组织和解释。
- edge-front 已经能获得 Linux `TCP_INFO` 里的 RTT、retrans、RTO、lost、delivery rate 等网络损伤信号，但这些信号尚未进入 edge DNS 排名。

本方案的核心判断是：Fugue 不应该维护一个简单的“全局 edge 好坏排名”。正确模型应该是：

```text
For this hostname / service / traffic class / client scope / time window,
which edge group and edge node should receive this request?
```

## 当前实现画像

当前相关实现主要分布在：

- `internal/model/edge_routes.go`
  - 定义 `EdgePerformanceSample`、`EdgeDNSAnswerCandidate`、DNS policy 和质量样本字段。
- `internal/edge/service.go`
  - 采集并上报 edge request、body read、origin、response egress、cache、saturation 等样本。
- `internal/api/edge_dns.go`
  - 在控制平面构建 DNS answer candidate、latency-aware score、scoped candidate。
- `internal/dnsserver/service.go`
  - 在 DNS server 内根据 candidate score、geo hint、exploration 和 policy 返回 answer。
- `internal/edgefront/service.go`
  - 采集 public TCP connection 的 `TCP_INFO` 和 node TCP counters。
- `internal/tcpdiag/tcpdiag.go`
  - 解析 Linux TCP diagnostics 字段。
- `internal/store/edge_performance.go`
  - 存储和查询 edge performance samples。

当前已经采集并部分参与 ranking 的指标包括：

- `ttfb_ms`
- `upstream_ms`
- `total_ms`
- `status_code`
- `error_count`
- `cache_hit_count`
- `cache_observation_count`
- `body_read_block_ms`
- `file_write_ms`
- `upload_effective_bps`
- `min_window_bps`
- `max_read_gap_ms`
- `request_body_bytes`
- `request_body_read_bytes`
- `body_incomplete_count`
- `body_read_error_count`
- `response_write_ms`
- `response_egress_bps`
- `origin_dns_ms`
- `origin_connect_ms`
- `origin_request_write_ms`
- `origin_response_wait_ms`
- `origin_ttfb_ms`
- `origin_total_ms`
- `streaming_request_count`
- `websocket_request_count`
- `sse_request_count`
- `client_cancel_count`
- `active_requests`
- `active_body_buffers`
- `goroutine_count`
- `memory_alloc_bytes`

当前已经存在但不完整的作用域包括：

- `hostname`
- `path_prefix`
- `method`
- `traffic_class`
- `client_country`
- `client_region`
- `client_asn`
- `runtime_region`
- `edge_group_id`
- `edge_id`
- `route_generation`

当前 DNS latency-aware score 大体已经考虑 latency、error rate、origin、upload、download、cache 和 saturation，但仍存在以下缺口：

- `traffic_class` 主要作为样本属性和 dominant label 存在，尚未形成独立排名 profile。
- `client_country`、`client_region`、`client_asn` 已有 scope 基础，但 CLI 和解释能力不足。
- `edge_id` 级 node ranking 不够明确，容易只看到 edge group 的综合质量。
- TCP retrans/RTO/lost/delivery rate 未进入 performance sample 和 DNS score。
- 24h 窗口适合作 baseline，但对运营商线路短时波动反应太慢。
- origin/app 慢和 client-to-edge 网络差混在同一个 score 中，容易错误归因。
- 当前 min sample 门槛过低，不适合直接驱动生产流量大幅切换。

## 目标不变量

1. DNS 不得返回不 route-ready 的 edge。
2. Service-level edge exclusion 必须作为硬 gate，不能被质量分覆盖。
3. Edge health、drain、maintenance、route generation、TLS readiness 必须先判断 eligibility，再参与 ranking。
4. 排名必须按 traffic class 区分，不能让大 body 请求、静态资源、普通 API、SSE/WebSocket 混用一套权重。
5. 排名必须按客户端网络作用域区分，至少支持 global、country、country+region、ASN。
6. 任何作用域样本不足时必须显式 fallback，并记录 fallback level。
7. 新 edge 必须有受控 exploration，但 exploration 不能绕过 service exclusion 和 hard health gate。
8. 网络损伤指标不能用单个全局丢包率替代，必须按客户端 scope 和时间窗口聚合。
9. Origin/runtime 慢不能被简单等同为 edge-client 网络差。
10. 每一次 DNS 质量决策都应该能被 CLI/API 解释：使用了什么 scope、哪些 candidate、各自 score breakdown、为什么选中。

## 设计原则

### 从全局排名改为上下文排名

旧问题：

```text
Which edge is best globally?
```

目标问题：

```text
For hostname H, traffic class T, client scope S, and window W,
which eligible edge group and edge node has the best expected user outcome?
```

### 先 gate 后 score

不能参与流量的 edge 不应该靠低 score 混进候选集。先执行 hard gates：

- edge online
- edge group enabled
- DNS publishable
- route-ready for hostname
- TLS/cert ready or known warm path
- not draining
- not in maintenance
- service policy allowed
- route generation current or last-known-good valid

只有通过 gate 的 candidate 才进入质量排名。

### 指标按真实用途分组

不要继续把所有观测值塞进一个不可解释的大 score。目标 score 必须拆成可解释维度：

- `network_score`
- `availability_score`
- `latency_score`
- `upload_score`
- `download_score`
- `origin_score`
- `cache_score`
- `saturation_score`
- `confidence_penalty`
- `exploration_adjustment`

### 短窗口快速降权，长窗口稳定排序

建议同时维护：

- `5m`: 只用于严重故障和快速降权。
- `30m`: 主要实时 ranking 窗口。
- `6h`: 日内稳定趋势。
- `24h`: 长期 baseline。

默认选择可以使用：

```text
final_score =
  live_30m_score * 0.55
+ baseline_6h_score * 0.30
+ baseline_24h_score * 0.15
+ confidence_penalty
+ exploration_adjustment
```

严重错误率、严重 retrans/RTO、严重 body incomplete、极低 body read throughput 可以让 `5m` 窗口触发快速降权，但恢复必须慢于降权。

## 作用域模型

### 目标 scope key

Ranking rollup 的推荐 key：

```text
window
hostname
traffic_class
method
path_prefix_bucket
client_scope_kind
client_scope_value
edge_group_id
edge_id
```

其中：

- `hostname` 表示服务入口，例如 custom domain 或 platform hostname。
- `traffic_class` 表示请求类型。
- `method` 用于区分 GET/HEAD 与 POST/PUT/PATCH 等 body 请求。
- `path_prefix_bucket` 必须是归一化后的低基数字段，不能使用原始 path。
- `client_scope_kind` 可取 `global`、`country`、`country_region`、`asn`。
- `client_scope_value` 示例：`global`、`CN`、`CN-HK`、`AS4134`。
- `edge_group_id` 用于 group-level ranking。
- `edge_id` 用于 node-level ranking 和同 group 内 canary/exploration。

### Scope fallback 顺序

DNS 决策时建议按以下顺序寻找可用 rollup：

1. `hostname + traffic_class + method + path_prefix_bucket + ASN + country/region`
2. `hostname + traffic_class + method + ASN + country`
3. `hostname + traffic_class + ASN`
4. `hostname + traffic_class + country/region`
5. `hostname + traffic_class + country`
6. `hostname + traffic_class + global`
7. `hostname + global`
8. `platform + edge_group/global`

每次使用 fallback 时都必须记录：

- requested scope
- selected scope
- fallback level
- fallback reason
- selected window
- sample count
- confidence

### Cardinality 控制

禁止把以下字段直接纳入 ranking key：

- 原始 URL path
- 单个 client IP
- request id
- user agent
- 原始 query string
- 高基数 header

如果确实需要 path 维度，必须先映射为少量 `path_prefix_bucket`，例如：

- `/api/*`
- `/_next/static/*`
- `/assets/*`
- `/upload/*`
- `/stream/*`
- `/`

## Traffic class profile

### `large_body_api`

适用于大 request body、上传、长 POST/PUT/PATCH 请求。

高权重：

- `upload_effective_bps`
- `min_window_bps`
- `max_read_gap_ms`
- `body_read_block_ms`
- `body_incomplete_rate`
- `body_read_error_rate`
- `client_tcp_retrans_rate`
- `client_tcp_rto_rate`
- `client_tcp_bytes_retrans_rate`

中权重：

- `ttfb_ms`
- `total_ms`
- `error_rate`
- `active_body_buffers`

低权重或不用：

- cache hit ratio
- response egress for small responses

### `dynamic_api`

适用于普通动态 API。

高权重：

- `ttfb_ms`
- `error_rate`
- `upstream_ms`
- `origin_response_wait_ms`

中权重：

- client TCP retrans/RTO
- total latency
- origin connect/write
- saturation

低权重：

- upload throughput, unless request has body
- cache hit ratio

### `static_cacheable`

适用于 hashed assets、字体、JS、CSS、图片等可缓存静态资源。

高权重：

- cache hit ratio
- `response_egress_bps`
- `response_write_ms`
- `ttfb_ms`

中权重：

- client TCP retrans/RTO
- origin total, only for miss/bypass

低权重：

- upload score
- request body metrics

### `html_dynamic`

适用于 HTML 页面和 SSR。

高权重：

- `ttfb_ms`
- `origin_response_wait_ms`
- error rate

中权重：

- total latency
- client TCP retrans/RTO
- cache status if explicitly cacheable

### `streaming`, `sse`, `websocket`

适用于长连接和流式请求。

高权重：

- connection setup success
- early disconnect / client cancel rate
- TCP retrans/RTO
- RTT variance
- active connection pressure

不应简单惩罚：

- request total duration
- long response time

长连接本身就可能持续很久，不能把持续时间直接视为慢。

## 指标设计

### Network score

目标是衡量 client-to-edge 网络质量。推荐输入：

- `client_tcp_rtt_ms`
- `client_tcp_min_rtt_ms`
- `client_tcp_rttvar_ms`
- `client_tcp_total_retrans`
- `client_tcp_retrans_rate`
- `client_tcp_bytes_retrans`
- `client_tcp_bytes_retrans_rate`
- `client_tcp_lost`
- `client_tcp_unacked`
- `client_tcp_total_rto`
- `client_tcp_rto_rate`
- `client_tcp_delivery_rate_bps`

推荐派生：

```text
client_tcp_retrans_rate =
  total_retrans / max(data_segments_out, segments_out, 1)

client_tcp_bytes_retrans_rate =
  bytes_retrans / max(bytes_sent, 1)

client_tcp_rto_rate =
  total_rto / max(connection_count, 1)

network_impairment_score =
  normalized_retrans * w1
+ normalized_bytes_retrans * w2
+ normalized_rto * w3
+ normalized_rttvar * w4
```

注意：

- 不建议使用全局 ping loss 作为主要 ranking 指标。
- 不建议把所有客户端的丢包率混成一个 global loss rate。
- 网络损伤必须按 ASN/country/region/window/edge 统计。

### Upload score

目标是衡量 edge 读取 request body 的真实能力。

推荐输入：

- `upload_effective_bps`
- `min_window_bps`
- `max_read_gap_ms`
- `body_read_block_ms`
- `file_write_ms`
- `request_body_bytes`
- `request_body_read_bytes`
- `body_incomplete_rate`
- `body_read_error_rate`

推荐判断：

- 不按绝对读取时间排序，因为 body 大小不同。
- 按吞吐、最差窗口吞吐、最大读空洞、未读完比例和错误率排序。
- 对小 body 请求降低 upload score 权重，避免噪音。

### Download score

推荐输入：

- `response_egress_bps`
- `response_write_ms`
- `response_bytes`
- `client_cancel_rate`
- TCP retrans/RTO

对于大响应和静态资源，应提高 download 权重。

### Availability score

推荐输入：

- HTTP 5xx rate
- edge proxy error rate
- body read error rate
- body incomplete rate
- TLS handshake failure rate, if available
- upstream connection failure rate

注意：

- 4xx 通常不应直接作为 edge 质量坏的信号。
- 用户取消请求需要结合 traffic class 和阶段判断。

### Latency score

推荐输入：

- `ttfb_ms`
- `upstream_ms`
- `total_ms`
- p50/p95/p99 latency

当前主要是 average，建议 rollup 增加 p95/p99 或近似分位数。

### Origin score

推荐输入：

- `origin_dns_ms`
- `origin_connect_ms`
- `origin_request_write_ms`
- `origin_response_wait_ms`
- `origin_ttfb_ms`
- `origin_total_ms`

Origin score 不能主导 edge ranking。判断原则：

- 如果所有 edge 到同一 origin 都慢，大概率是 origin/runtime 问题。
- 如果只有某个 edge 到 origin 慢，才应该影响该 edge 对该 hostname 的排名。

### Cache score

仅对 `static_cacheable` 或明确 cacheable 请求启用。

推荐输入：

- `cache_hit_count`
- `cache_observation_count`
- cache hit ratio
- miss/bypass/stale/revalidate 分类, if available

### Saturation score

推荐输入：

- `active_requests`
- `active_body_buffers`
- `goroutine_count`
- `memory_alloc_bytes`
- CPU usage, if available
- edge-front active connections, if available
- body buffer disk pressure, if available

该 score 主要用于避免把新流量继续压到已拥塞节点。

## Rollup 数据模型

建议新增低基数质量 rollup，而不是每次 DNS 排名扫描 raw samples。

### `edge_quality_rollups`

建议字段：

```text
window
window_started_at
window_ended_at
hostname
traffic_class
method
path_prefix_bucket
client_scope_kind
client_scope_value
edge_group_id
edge_id
sample_count
request_count
error_count
error_rate
cache_hit_count
cache_observation_count
cache_hit_rate
p50_ttfb_ms
p95_ttfb_ms
p99_ttfb_ms
avg_upstream_ms
avg_total_ms
avg_origin_dns_ms
avg_origin_connect_ms
avg_origin_request_write_ms
avg_origin_response_wait_ms
avg_origin_ttfb_ms
avg_origin_total_ms
avg_upload_effective_bps
p10_upload_effective_bps
avg_min_window_bps
p10_min_window_bps
p95_max_read_gap_ms
avg_body_read_block_ms
body_incomplete_rate
body_read_error_rate
avg_response_egress_bps
p10_response_egress_bps
p95_response_write_ms
client_cancel_rate
avg_client_tcp_rtt_ms
avg_client_tcp_min_rtt_ms
avg_client_tcp_rttvar_ms
client_tcp_retrans_rate
client_tcp_bytes_retrans_rate
client_tcp_rto_rate
avg_client_tcp_delivery_rate_bps
avg_active_requests
avg_active_body_buffers
avg_goroutine_count
avg_memory_alloc_bytes
confidence
score
score_breakdown_json
updated_at
```

实现备注：对外模型和 API 字段继续叫 `window`；PostgreSQL 物理列名使用
`window_name`，避免 `window` 关键字导致 schema 初始化失败。2026-06-23
上一版 rollout 的 API/controller CrashLoopBackOff 已本地复现为
`syntax error at or near "window"`，本方案用 `window_name` 和 live
Postgres schema integration test 固化回归。

### Retention

建议：

- raw samples 保留 7 天。
- 5m rollup 保留 48 小时。
- 30m rollup 保留 14 天。
- 6h rollup 保留 45 天。
- 24h rollup 保留 180 天。

### Confidence

建议 confidence 输入：

- sample count
- request count
- candidate edge group coverage
- candidate edge node coverage
- recency
- window completeness
- metric completeness

示例：

```text
confidence =
  sample_confidence
* recency_confidence
* candidate_coverage_confidence
* metric_completeness_confidence
```

低 confidence 时：

- 不做大幅切流。
- 只允许 exploration。
- 对 score 增加 penalty。
- CLI 必须显示样本不足。

## DNS ranking 集成

### Candidate 生成

DNS candidate 必须来自：

```text
candidate_edges(hostname)
  = healthy_edges
  ∩ dns_publishable_edges
  ∩ route_ready_edges(hostname)
  ∩ service_policy_allowed_edges(hostname)
```

然后再按 scoped quality score 排序。

### Score 公式

推荐结构：

```text
candidate_score =
  network_score
+ availability_score
+ latency_score
+ upload_score
+ download_score
+ origin_score
+ cache_score
+ saturation_score
+ confidence_penalty
+ exploration_adjustment
```

低分更好。每个子分必须出现在 `score_breakdown` 中。

### Exploration

新 edge 或低样本 edge 需要真实流量验证，但必须受控：

- 默认 1% - 5%。
- 按 edge group 和 edge node 分开。
- 按 traffic class 分开。
- 大 body 请求的 exploration 可以更保守。
- exploration 不得绕过 service-level exclusion。
- exploration 命中必须记录 reason。

### Hysteresis

避免 DNS answer 因短时抖动来回切换：

- 常规切换需要最小 score delta。
- 恢复慢于降权。
- 严重故障可绕过 cooldown 快速降权。
- 低 confidence 不允许 aggressive promotion。

## API 和 CLI 可解释性

必须补齐 operator 能力，否则后续排障仍然会依赖猜测。

### 推荐 CLI

```sh
fugue admin edge quality-rank api.0-0.pro \
  --traffic-class large_body_api \
  --scope asn:4134 \
  --since 30m
```

输出必须包含：

- hostname
- traffic class
- requested scope
- selected scope
- fallback level
- window
- edge group
- edge id
- score
- score breakdown
- confidence
- sample count
- request count
- ranking position
- whether excluded by service policy
- whether selected by exploration

### DNS explain

建议增强：

```sh
fugue admin dns answer-check api.0-0.pro --explain
```

输出必须能回答：

- 哪些 edge 是候选？
- 哪些 edge 被 hard gate 排除？
- 哪个 scope 被使用？
- 为什么 fallback？
- 每个 candidate 的 score breakdown 是什么？
- 为什么最终返回这个 answer？

### API

如果对 fugue-web 或外部工具开放，需要先修改 `openapi/openapi.yaml`，再按本仓库 OpenAPI-first 流程生成后端和前端派生产物。

## Operator runbook

### 1. 判断当前模式

控制平面通过 `FUGUE_EDGE_QUALITY_RANKING_MODE` 控制行为：

- `shadow`：默认模式；计算 scoped ranking，DNS answer 仍按旧 geo/legacy 逻辑返回。
- `active`：允许 scoped ranking 改变 latency-aware DNS candidates / scoped candidates。
- `legacy`、`off`、`disabled`：kill switch；跳过 scoped quality ranking。

发布路径必须走 GitHub Actions `deploy-control-plane.yml`。不要手工 patch
Deployment 或 SSH 改线上文件来切换长期配置。

### 2. 查询某个服务的 edge 排名

```sh
fugue admin edge quality-rank api.0-0.pro \
  --traffic-class large_body_api \
  --method POST \
  --path-prefix /api \
  --scope asn:4134 \
  --window 30m
```

重点看：

- `selected_scope` 和 `fallback_level`：确认是否退到了 country/global/platform。
- `score_breakdown`：确认是 network、upload、origin、cache、saturation 还是 confidence 导致排名。
- `hard_gated`：确认 service exclusion、route-ready、TLS-ready、health 是否挡住候选。

### 3. 查询 DNS answer 与 shadow 差异

```sh
fugue admin dns answer-check api.0-0.pro --explain
```

在 `shadow` 模式下：

- `answer_policy.selected_edge_group_id` 表示当前 legacy/geo 选择。
- `answer_policy.shadow_selected_edge_group_id` 表示 scoped ranking 建议选择。
- `answer_policy.shadow_reason` 表示新 ranking 的理由。
- `scoped_candidates` 不发布给 DNS server，真实 answer 不被改变。

在 `active` 模式下：

- `answer_policy.policy_kind=latency_aware` 表示新 ranking 已参与 answer。
- `scoped_candidates` 会按 client scope 被 DNS server 使用。

### 4. 观察 rollup builder

Prometheus 指标：

- `fugue_edge_quality_ranking_active`
- `fugue_edge_quality_ranking_shadow`
- `fugue_edge_quality_rollup_runs_total`
- `fugue_edge_quality_rollup_errors_total`
- `fugue_edge_quality_rollup_last_duration_seconds`
- `fugue_edge_quality_rollup_last_count`
- `fugue_edge_quality_rollup_last_error`

如果 `errors_total` 增长或 `last_error=1`，先查 API pod 日志里的
`edge quality rollup builder failed`。如果 `last_count=0`，再查 edge heartbeat
是否继续上报 `performance_samples`。

### 5. 快速止血

如果新 ranking 出现非预期 DNS 分布：

1. 通过仓库变量或发布配置把 `FUGUE_EDGE_QUALITY_RANKING_MODE` 改为 `legacy`。
2. 走 `main` 分支 push / `deploy-control-plane.yml` 正式发布。
3. 用 `fugue admin dns answer-check <hostname> --explain` 验证 `shadow_selected_edge_group_id` 为空。
4. 用 `fugue admin edge quality-rank <hostname> --window 5m` 保留排障视图，不直接改线上 DNS bundle。

### 6. rollout 失败排查

若 API/controller 同时 CrashLoopBackOff，优先怀疑共同启动路径：

- `Store.Init()` / Postgres schema bootstrap。
- 共享环境变量解析。
- OpenAPI/generated init-time panic。

先看 GitHub Actions deploy 日志、Kubernetes events 和 pod logs；如果 pod 已被回滚删除，
用本地 `FUGUE_TEST_DATABASE_URL` 跑 live Postgres schema integration test 复现 schema
错误。2026-06-23 已新增该 opt-in 测试。

## Rollout 策略

### 阶段 1: Shadow mode

先只计算新 scoped score，不改变 DNS answer。

对比：

- 当前 DNS 选择。
- 新 ranking 建议选择。
- 每次建议切换的原因。
- 预估影响的 hostname / traffic class / scope。

Shadow 至少覆盖：

- 24 小时正常周期。
- 一个国内高峰时段。
- 一个低流量时段。
- 至少一个新 edge exploration 场景。

### 阶段 2: Read-only CLI

上线 CLI/API 查询能力，但仍不改变 DNS 生产行为。

目标是让 operator 可以验证：

- 样本是否按 scope 正确聚合。
- fallback 是否符合预期。
- score breakdown 是否能解释线上现象。

### 阶段 3: Low-percent enablement

对 latency-aware DNS policy 小比例启用新 score。

建议：

- 先 5%。
- 仅启用低风险 traffic class。
- 大 body 请求单独灰度。
- 保留快速 kill switch。

### 阶段 4: Full enablement

当 shadow 和低比例行为稳定后，再作为默认 latency-aware ranking。

### 阶段 5: Fast degrade

最后启用快速降权规则，只针对严重信号：

- 5m error rate 明显异常。
- body incomplete/read error 暴涨。
- upload throughput 断崖式下降。
- TCP retrans/RTO 明显异常。
- edge saturation 明显异常。

## 详细 TODO list

### 0. Scope and safety

- [x] 确认本方案覆盖 control plane、DNS、edge proxy、edge-front、CLI 和 store 的变更边界。
- [x] 确认所有线上行为变更都走正式 GitHub Actions control-plane 发布链路。
- [x] 确认第一阶段只做 shadow/read-only，不直接改变 DNS answer。
- [x] 确认 service-level edge exclusion 是 hard gate。
- [x] 确认 route-ready invariant 是 hard gate。
- [x] 确认新 edge exploration 不绕过 health、route、TLS、service policy gates。

### 1. Current state audit

- [x] 列出 `EdgePerformanceSample` 当前所有字段和缺失字段。
- [x] 列出 `EdgeDNSAnswerCandidate` 当前所有 quality 字段。
- [x] 列出当前 latency-aware score 的所有权重。
- [x] 列出当前 scoped candidate 的生成逻辑。
- [x] 列出当前 DNS server 使用 score 的排序逻辑。
- [x] 列出当前 CLI 可以查询的 edge quality 信息。
- [x] 对比 edge-front TCP_INFO 指标和 edge quality sample 的差距。
- [x] 写出当前数据链路图：edge-front -> edge -> control plane -> DNS bundle -> DNS answer。

### 2. Schema design

- [x] 设计 `edge_quality_rollups` 表结构。
- [x] 设计 rollup unique key。
- [x] 设计 rollup indexes。
- [x] 设计 rollup retention。
- [x] 设计 `score_breakdown_json` schema。
- [x] 设计 `client_scope_kind` 和 `client_scope_value` 枚举。
- [x] 设计 `path_prefix_bucket` 归一化规则。
- [x] 设计 traffic class 枚举和 fallback。
- [x] 评估是否需要扩展 `EdgePerformanceSample`，还是先通过旁路 TCP rollup 接入。

### 3. TCP/network metrics ingestion

- [x] 确认 edge-front public TCP connection event 当前字段。
- [x] 确认 TCP_INFO 字段在不同 Linux 内核上的可用性。
- [x] 设计 edge-front 到 control plane 或 edge sample 的低基数上报方式。
- [x] 增加 `client_tcp_rtt_ms`。
- [x] 增加 `client_tcp_min_rtt_ms`。
- [x] 增加 `client_tcp_rttvar_ms`。
- [x] 增加 `client_tcp_total_retrans`。
- [x] 增加 `client_tcp_retrans_rate`。
- [x] 增加 `client_tcp_bytes_retrans`。
- [x] 增加 `client_tcp_bytes_retrans_rate`。
- [x] 增加 `client_tcp_total_rto`。
- [x] 增加 `client_tcp_rto_rate`。
- [x] 增加 `client_tcp_delivery_rate_bps`。
- [x] 处理 TCP_INFO 不可用时的 metric completeness。
- [x] 确保 TCP 指标不引入 client IP、request id 等高基数 labels。

### 4. Rollup builder

- [x] 实现 5m rollup。
- [x] 实现 30m rollup。
- [x] 实现 6h rollup。
- [x] 实现 24h rollup。
- [x] 实现 raw sample 到 `path_prefix_bucket` 的归一化。
- [x] 实现 raw sample 到 `traffic_class` profile 的映射。
- [x] 实现 global scope rollup。
- [x] 实现 country scope rollup。
- [x] 实现 country+region scope rollup。
- [x] 实现 ASN scope rollup。
- [x] 实现 edge group rollup。
- [x] 实现 edge node rollup。
- [x] 实现 p50/p95/p99 或近似分位数。
- [x] 实现 confidence 计算。
- [x] 实现 rollup retention cleanup。
- [x] 增加 rollup builder metrics 和 logs。

### 5. Score engine

- [x] 新增独立 score engine，避免继续把逻辑散落在 DNS candidate builder 内。
- [x] 实现 `network_score`。
- [x] 实现 `availability_score`。
- [x] 实现 `latency_score`。
- [x] 实现 `upload_score`。
- [x] 实现 `download_score`。
- [x] 实现 `origin_score`。
- [x] 实现 `cache_score`。
- [x] 实现 `saturation_score`。
- [x] 实现 `confidence_penalty`。
- [x] 实现 `exploration_adjustment`。
- [x] 实现 `large_body_api` 权重 profile。
- [x] 实现 `dynamic_api` 权重 profile。
- [x] 实现 `static_cacheable` 权重 profile。
- [x] 实现 `html_dynamic` 权重 profile。
- [x] 实现 `streaming` / `sse` / `websocket` 权重 profile。
- [x] 实现 5m severe degrade 逻辑。
- [x] 实现 slow recovery / hysteresis。
- [x] 实现 score breakdown 输出。
- [x] 为每个子分加入上限，避免单个 noisy metric 完全支配排名。

### 6. Scope selection and fallback

- [x] 实现 requested scope 到 candidate rollup scope 的匹配。
- [x] 实现 ASN + country/region 优先级。
- [x] 实现 ASN fallback。
- [x] 实现 country+region fallback。
- [x] 实现 country fallback。
- [x] 实现 hostname + traffic class global fallback。
- [x] 实现 hostname global fallback。
- [x] 实现 platform global fallback。
- [x] 在 score decision 中记录 fallback level。
- [x] 在 score decision 中记录 fallback reason。
- [x] 在低 confidence 时阻止 aggressive promotion。

### 7. DNS integration

- [x] 保留 route-ready hard gate。
- [x] 保留 DNS publishable hard gate。
- [x] 保留 service-level exclusion hard gate。
- [x] 保留 edge health hard gate。
- [x] 在 DNS bundle 中携带 scoped score decision 所需字段。
- [x] 在 DNS server 中使用新 score 排序候选。
- [x] 在 DNS server 中保留 legacy score fallback。
- [x] 在 DNS answer 中记录 selected edge group 和 edge id。
- [x] 实现 node-level canary/exploration 与 scoped score 协同。
- [x] 增加 kill switch 回退到旧 latency-aware score。

### 8. API and CLI

- [x] 设计 `edge quality rank` API，先改 `openapi/openapi.yaml`。
- [x] 生成 OpenAPI 后端派生产物。
- [x] 实现 handler。
- [x] 实现 `fugue admin edge quality-rank`。
- [x] 支持 `--hostname` 或位置参数 hostname。
- [x] 支持 `--traffic-class`。
- [x] 支持 `--scope global`。
- [x] 支持 `--scope country:<country>`。
- [x] 支持 `--scope region:<country>-<region>`。
- [x] 支持 `--scope asn:<asn>`。
- [x] 支持 `--since` 或 `--window`。
- [x] 输出 score breakdown。
- [x] 输出 confidence。
- [x] 输出 fallback level。
- [x] 输出 hard-gated candidates。
- [x] 增强 `dns answer-check --explain`。
- [x] 当前 fugue-web 不消费该 admin API；若未来消费，再同步 `fugue-web` OpenAPI 派生产物。

### 9. Tests

- [x] 单测：route-ready gate 不能被 score 覆盖。
- [x] 单测：service exclusion 不能被 score 覆盖。
- [x] 单测：traffic class 不互相污染。
- [x] 单测：ASN scope 优先于 country scope。
- [x] 单测：样本不足时 fallback。
- [x] 单测：低 confidence 不 aggressive promote。
- [x] 单测：TCP retrans/RTO 异常会提高 network score。
- [x] 单测：large body 请求低 upload bps 会降权。
- [x] 单测：static cacheable 请求 cache hit ratio 影响排序。
- [x] 单测：streaming 请求不因长 total duration 被误判。
- [x] 单测：origin 全局慢不会错误惩罚单一 edge。
- [x] 单测：单一 edge 到 origin 慢会影响该 edge。
- [x] 单测：5m severe degrade 可快速降权。
- [x] 单测：recovery 需要 hysteresis。
- [x] 集成测试：DNS bundle 携带 scoped score。
- [x] 集成测试：DNS server 使用 scoped score 返回 answer。
- [x] 集成测试：CLI 输出 score breakdown。
- [x] 集成测试：OpenAPI generated artifacts 无漂移。

### 10. Shadow rollout

- [x] 增加 shadow mode 配置。
- [x] 记录 legacy selected edge。
- [x] 记录 scoped ranking selected edge。
- [x] 记录两者差异原因。
- [x] 记录预计影响 hostname。
- [x] 记录预计影响 traffic class。
- [x] 记录预计影响 client scope。
- [ ] 连续观察 24 小时。
- [ ] 覆盖国内高峰时段。
- [ ] 覆盖低流量时段。
- [ ] 覆盖新 edge exploration 场景。
- [ ] 输出 shadow report。

### 11. Production rollout

- [x] 启用 read-only CLI/API。
- [ ] 对低风险 traffic class 启用 5%。
- [ ] 对大 body traffic class 单独启用小比例。
- [ ] 观察 error rate。
- [ ] 观察 request body read throughput。
- [ ] 观察 TCP retrans/RTO。
- [ ] 观察 DNS answer 分布。
- [ ] 观察 edge saturation。
- [ ] 扩到 25%。
- [ ] 全量启用。
- [x] 保留 kill switch。
- [ ] 发布后 24 小时复盘排名和真实质量是否一致。

### 12. Documentation

- [x] 更新 edge request body / TCP observability 文档。
- [x] 更新 DNS latency-aware 文档。
- [x] 更新 CLI 文档。
- [x] 更新 operator runbook。
- [x] 记录 score formula 和每个 traffic class profile。
- [x] 记录 scope fallback 规则。
- [x] 记录如何调查“为什么这个请求去了这个 edge”。
- [x] 记录如何调查“某个 edge 为什么排名差还拿流量”。
- [x] 记录如何调查“新 edge 是否在 exploration”。

## Definition of done

本方案完成时，Fugue 应该满足：

- 可以按 hostname、traffic class、client scope 和 window 查看 edge 排名。
- 可以区分 edge group 质量和 edge node 质量。
- 可以解释 DNS 为什么返回某个 edge。
- 可以看到 score breakdown，而不是只有一个 opaque score。
- 大 body 慢读会进入 `large_body_api` 排名。
- TCP retrans/RTO 等网络损伤会进入 scoped network score。
- 新 edge 能获得受控 exploration 流量。
- Service-level edge exclusion、route-ready、health、TLS readiness 永远是 hard gate。
- 样本不足时系统会明确 fallback，而不是假装有精确判断。
- 线上启用前可以 shadow 对比旧逻辑和新逻辑。
