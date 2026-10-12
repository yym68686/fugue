# BWG 预期下线后的 DNS / edge 故障转移调查

调查日期：2026-10-12。本文面向用户的时间均为北京时间（UTC+8）；原始证据保留 UTC。主要在线取证窗口约为 10:13–10:26。

用户已说明 BWG 每月 1 TB 流量用尽，预计 2026-10-18 恢复额度。本次把 BWG 不可达作为预期输入，不尝试重启、恢复或修改该节点。DMIT 流量耗尽下线同样不作为待修复的主机故障。

## 结论

**没有完成可靠的端到端故障转移。节点健康过滤起作用，仍在线的 edge 可以提供路由服务，但 DNS 的故障隔离存在已证实的缺陷。**

| 层次 | 证据及结论 |
| --- | --- |
| 失联节点识别 | 当前健康节点列表不再包含 BWG；美国 authority 的有效 inventory 为两个节点，德国为一个。代码以最近节点活动的 90 秒为新鲜度上限。 |
| 健康备用入口 | OVH 美国与 vmarckedge 对 `i00.pro` 的 TLS nonce 路由探测均返回 204、正确 route digest 和与当前 DNS 完全匹配的 traffic release；不是“所有备用 edge 都坏了”。 |
| DNS 选址 | 当前 DNS Pod 内查询 `i00.pro`、`api.fugue.pro`、`www.fugue.pro` 返回 SERVFAIL。部分排在探测计划前面的记录仍能回答健康 IP。未形成稳定的备用入口答案。 |
| 公网 DNS 入口 | 之前的受控抓包已确认两地公网查询被送到旧 hostPort DNS Pod，并由旧 Pod 返回 SERVFAIL。当前公共 Service 选择的新 DNS Pod 因 Ready=false 被移出可用端点。 |
| 可观测性 | `traffic_release_mismatch` 会覆盖“没有开始探测/探测不可达/探测失败”等原始原因；节点详情与期望状态又保留旧 healthy/eligible 字段，不能直接当实时健康事实。 |

可以确定存在 Fugue 的 DNS 运行时与健康状态耦合缺陷，不能把现象归结为 BWG 流量耗尽本身。**本报告不声称已还原每一个历史失败探测的原始原因，也不能声称所有客户端的实际流量都在某一秒完成切换。** 被覆盖的探测结果和缺失的历史选址记录不能通过猜测补齐。

本轮没有修改仓库运行代码、生产配置、DNS 记录、Pod/Deployment、主机信任或密钥，也没有重新发布旧版本。仅新增本报告及本机诊断材料。

## 调查方式和范围

- 先尝试本机 Fugue CLI。CLI `admin edge nodes ls` 因普通 API 域名路径 EOF 失败；随后用现有认证，通过仅作用于该诊断进程的 `curl --resolve api.fugue.pro:443:15.204.94.71` 读取 API，TLS 主机名与证书验证保持开启。
- 节点 SSH 用于读取 Pod 状态、有限健康接口、现有决策记录、网络命名空间和有限报文头。没有安装程序或改写节点配置。
- 查询数据库使用 `BEGIN READ ONLY`，6 秒 statement timeout、1 秒 lock timeout，有明确时间范围和数量上限。
- 路由检查使用平台已有的 HEAD + nonce 路由证明机制，不调用业务应用完成请求，不下载业务内容。
- 本机使用 Go overlay 加载临时诊断测试，调用仓库的真实实现。没有把诊断测试或生产数据写进仓库测试集，没有启动生产发布。
- 当前 backend HEAD 为 `fd292096`。部署的 DNS 源码 `f1037f6c` 与当前 HEAD 在本文涉及的三个 DNS 文件上没有差异。最近 CI `38179202565` 是已成功的定时审计/内存策略任务，其内存策略结果为零变更、零 Pod resize；没有证据表明它发布了本次 DNS/edge 代码。

## 健康节点和实际备用服务

当前 API 健康节点列表包含：

| 节点 | 地址 | 当前证据 |
| --- | --- | --- |
| `vps-591f4447` / OVH 美国 | `15.204.94.71` | 新鲜心跳、worker 健康、`i00.pro` / API / 其他抽样路由 nonce 证明成功。 |
| `vps-84c8f0a9` / OVH 德国 | `51.38.126.103` | 新鲜心跳、API 及允许的抽样路由证明成功；`i00.pro` 本身有美国组约束，不能把它在德国返回 503 泛化为德国节点故障。 |
| `86eabf26-42a9-4f9d-998a-754115f8b829` / vmarckedge | `154.51.41.196` | 通过主机 hostname 只读核对身份；新鲜心跳，`i00.pro` / API / 抽样路由证明成功。 |
| BWG | `95.169.10.156` | 不在当前健康列表；旧心跳停止，端口不可达，符合用户说明。 |

当前美国 `edge-control` 报告两个有效 inventory 节点，德国报告一个。`i00.pro` 的业务路由在美国 OVH 与 vmarckedge 的公开入口均返回相同 route digest：

`sha256:71715c53b795907ad21a7d60914b907d3caac6b7db85a527e68e8edf7fab4692`

解析并逐字段核对响应中的 `X-Fugue-Traffic-Release`，两个入口的 release set、route artifact、intent/policy/input digests、release ID、channel、scope、fencing token 均与当前 DNS 所要求的绑定一致；mismatch 列表为空。

更进一步，使用主机已有 `curl` 在美国 DNS Pod 的原网络命名空间内做同样的 TLS 校验探测：美国 OVH 在约 48 ms、vmarckedge 在约 98 ms 返回 204 和正确证明；BWG 在约 3.06 秒后连接失败。这个检查排除了“只有本机能访问，DNS Pod 网络完全不能访问备用入口”这一解释，但不用于声称所有历史瞬间网络都正常。

物理 edge 的 Front 选择本节点 worker；它不会将已经到达失联 BWG 的 TCP 连接搬到另一台物理机。新流量转移需要 DNS 或另外明确部署的入口故障转移层完成。已有连接的中断与 DNS 新答案的生效时间必须分开统计。

## 时间线：能证实的切换边界

| 北京时间，2026-10-12 | 已保存的事实 |
| --- | --- |
| 06:10:25.587 | BWG 最后一次 edge 心跳。 |
| 06:11:09 | Kubernetes 将 BWG Ready 置为 Unknown，原因是 kubelet 停止上报。 |
| 06:11:55.587 | 按当前实现、假设没有更晚成功活动，最后 edge 心跳的 90 秒新鲜度在此到期。这是代码计算出的边界，**不是历史 DNS 答案的实际切换回执**。 |
| 06:11:19.137 | 美国 DNS 的被接受心跳仍报告 probe_status=passed。 |
| 06:12:08.869 | 德国 DNS 的被接受心跳仍报告 probe_status=passed。 |
| 06:12:16.497 | 美国 DNS 首个已取到的 failed 心跳，fencing token 仍为 3201。 |
| 06:12:27 | 美国当前 DNS Pod 的 Ready 条件转为 False。 |
| 06:13:09.820 | 德国 DNS 首个已取到的 failed 心跳，fencing token 仍为 3201。 |
| 06:13:19 | 德国当前 DNS Pod 的 Ready 条件转为 False。 |

这是约两三分钟内从一个入口失联扩大到两地 DNS 不就绪的证据链，不是成功故障转移的证据链。没有足够的历史 DNS 决策回执和全量业务分布数据，可以承诺“最后一个 BWG 答案的发出时间”“所有递归缓存何时失效”或“流量切走比例”。实际 A 回答 TTL 会受有效路由证明剩余寿命约束；不能仅用 90 秒心跳阈值推算所有客户端的恢复时间。

## 已证实缺陷一：探测队列缺少故障隔离与公平推进

当前签名 DNS 计划包含 671 个路由探测，其中 171 个指向 BWG。策略是 8 个并发、每轮 30 秒、单探测最多 5 秒、证明新鲜度 120 秒。

`internal/dnsserver/platform_readiness.go:99` 的实际实现每次调用都从计划索引 0 开始，所有物理节点共享一个队列；预算用完即退出，下一轮又从 0 开始，没有保存轮转位置，也没有隔离某个失联物理地址占用的预算。

线上冻结计划中的代表位置（从 0 开始）：

- API 在德国的探测：16；美国：497；vmarckedge：608。
- `i00.pro` 在 vmarckedge：266；美国：292；BWG：345。

实际 DNS 回执中，API 的德国证明有效，后面的两个健康美国入口证明为空，API 要求至少两个健康 edge，故返回 `readiness_quorum_unavailable`。`i00.pro` 的三个证明均为空，也无法满足最小一个健康 edge。与此同时，独立的同命名空间探测证明美国和 vmarckedge 可达且返回匹配版本。

对真实 DNS 进程的被动、有限报文头采样，在 17.713 秒内记录到：

| 目标 | 不同 TCP 连接尝试 | SYN 报文数 |
| --- | ---: | ---: |
| BWG | 46 | 141 |
| OVH 德国 | 36 | 36 |
| OVH 美国 | 28 | 28 |
| vmarckedge | 34 | 34 |

其中 36 个 BWG 连接发生 SYN 重传。这是重复向同一个失联物理入口消耗探测资源的直接观察。

本机临时测试调用未修改的真实 collector：前面的失联目标占满并发，后面的健康目标在连续两轮均被调用 0 次、保持 `not_observed`。测试通过，明确复现了跨轮饥饿缺陷。

**证据边界：** 生产实现随后覆盖了原始 reason，故不能把某条已经落盘的空证明 100% 区分为“未调度”“超时”或其他 transport failure。上述队列缺陷本身已复现、线上资源竞争和空证明已观察到；每条历史结果的准确起因仍应通过下一节的可观测性补齐，而不能改写成虚假的精确历史。

## 已证实缺陷二：记录健康损失撤下整个 DNS listener

当前同一签名发布在重新检查时，只要不能满足全部记录的 readiness：

1. `internal/dnsserver/platform_serving.go:319` 保留原 AppliedAt，却把内存中的 `record.Positive` 改为 false。
2. `internal/dnsserver/listener_readiness.go:24` 据此返回 503、`checkpoint_unavailable`。
3. Kubernetes 将整个 DNS Pod 从公共 Service 的 Ready 端点中移除，即使其他记录、NS 或静态记录仍可回答。

线上已取得 `/readyz` 的精确响应 `{"ready":false,"reason":"checkpoint_unavailable"}`；磁盘正向 checkpoint 的 `positive=true` 且原配置并未过期。新 DNS Pod 内某些记录仍能返回健康 IP，但两个公共 EndpointSlice 都是 ready=false、serving=false。

隔离测试通过完整的相同 release 同步流程复现：初始 `/readyz=200`，一个负向路由观察后 `/readyz=503`，内存 Positive 被清除，NS 查询仍然成功。由此确认“有效配置身份”与“当前个别记录的运行时健康”仍被混为同一状态，破坏了记录级故障隔离。

这不是建议直接删除 `/readyz` 的检查。修复必须保留被验证的正向配置身份和原有效期，同时让当前失败记录继续严格失败；未经验证的候选、过期 checkpoint、无效签名、缺失 listener 仍须拒绝。

## 已证实缺陷三：旧 DNS hostPort 仍可实际接收公共流量

此前本聊天保存的精确 query ID 抓包确认：

| 公共入口 | 实际收到请求并发出 SERVFAIL 的旧 Pod |
| --- | --- |
| `15.204.94.71:53` | `10.42.2.184`，`fugue-fugue-dns-w9jsg` |
| `51.38.126.103:53` | `10.42.1.31`，`fugue-fugue-dns-country-de-6j6kb` |

原始报文关联使用 query ID、五元组和收发方向；美国节点的 CNI DNAT 规则也明确指向旧 Pod。旧进程使用过期的历史 checkpoint，fresh health 显示 `max_stale_exceeded=true`。不能因旧 Pod 还在运行就将它视为正向 LKG。

当前公共 DNS Service 选择的是新 Pod `10.42.2.63` / `10.42.1.177`，但它们退出 Ready 后，实际公共端口仍存在旧 hostPort 的接收路径。这使“保留旧进程”与“允许旧进程继续拥有公共流量”没有完全隔离。旧代码和旧 checkpoint 问题早于本次 BWG 下线；本次现象是这条旧路径实际暴露，而不是本次新发布了旧 DNS。

不能以手工删旧 Pod、改 iptables 或把未验证的新配置标为健康来恢复。应审查声明式公共端口所有权：保留 executor 和历史会话不应隐含保留新流量所有权；配置恢复必须有正向、可核验的目标。

## 已证实的可观测性问题

`platform_serving.go:293` 对每个不匹配的 proof 无条件设置 `traffic_release_mismatch`。空 proof 本身没有 release；原先的 `not_observed`、`probe_unavailable` 和 `probe_failed` 被覆盖。本机完整同步复现和线上 DNS 决策记录均证实这个覆盖，不能把日志里的几百条 mismatch 当作几百个真实的版本不一致。

其他口径也需要明确：

- `/v1/edge/nodes` 过滤新鲜度，BWG 已不存在；单节点 get 和 desired-state 仍展示旧 `healthy=true`、`dns_eligible=true`。后者主要是持久化状态/策略许可，不是当前可服务证据。界面应同时暴露 observation time、freshness、effective eligibility 和拒绝原因。
- 同一个 `i00.pro`，traffic-safety 返回 pass=true、两个健康可选 edge，而 routes/explain 返回发布消费者证据缺失、route unavailable。二者观测范围不同，不能把前一个 pass 当作公共 DNS 与实际入口验收通过。
- DNS 决策日志最多保留 64 条且异步有界。部分即时查询只有 AAAA 回执被保留，A 回执缺失；缺失不等于没回答，不能声称已经拿到全量历史切换审计。
- BWG 不可达期间，固定到该节点的 SSH front Deployment 持续产生 Pending/terminating 替代 Pod。这不是业务重启，但属于长期预期离线下的控制面对象积累，需要独立、通用的离线调谐策略；不能强删带身份材料的 Pod/卷来清理表象。

## 后续改进顺序及验收要求

本轮按“调查、精确归因、不影响业务”范围完成证据收集，未直接修复线上。建议将改进拆开，避免一次发布同时改变流量政策、配置权威和 DNS 执行行为。

1. **先补可观测性。** 记录每轮 ID、签名计划 digest、队列长度、已开始/完成/超时/未调度数量、按物理 edge 的预算和耗时；保留原始 transport reason，把 release binding 检查作为独立结果。关联到原 DNS decision ID。只记录固定分类和对象 ID，不记录密钥、用户请求内容或业务响应。
2. **修复 listener 与记录状态的耦合。** 不因一条记录的 readiness 失败否定已经验证的正向配置。保留原始过期时间；失败记录仍不能返回失联地址；正常记录、SOA/NS 与其他记录仍可服务。增加完整同步到 `/readyz` 的回归测试，而非只手动修改 facts 测试。
3. **修复探测调度公平性。** 隔离物理入口故障占用的预算，跨轮公平推进，避免固定队头反复重试；健康备用 edge 的每个 hostname/path 仍需自身的新鲜、匹配证明，不能以其他路径的成功代替。对长期离线节点使用有界的恢复检测，不放宽证明或授权期限。
4. **独立审查 DNS 公共端口所有权。** 明确旧 executor 保留与公共地址选择是两件事；在当前目标退为 NotReady 时，验证真实 UDP/TCP 流量不会进入过期旧进程。通过正常 CI 的独立配置发布与带补偿条件的验收完成，禁止手工热修。
5. **验证真实故障转移。** 在隔离环境模拟一个物理入口黑洞、671 条类似规模路由、多个健康备用、跨轮压力和证明过期。验收每条允许的记录及时去掉失联 edge、健康记录继续返回正向答案、所有 record/route/TLS/tenant 绑定不变、没有无限 Pending 复制；再用合成生产 canary 观察，不拿真实业务做故障注入。

如果还要给出“实际用户流量切走用了多少秒”，需要在实际权威答案、递归观测与 Front 接收计数之间保留可关联的时间线。现在的历史数据不支持给出一个 100% 置信度的全客户端切换时长。

## 证据位置

本轮材料位于 `/tmp/fugue-bwg-investigation-20261012/`，文件为本机受限权限，没有输出认证密钥。

- `dns-heartbeat-timeline.json`：只读数据库时间线。
- `dns-us-checkpoint.json`：冻结的签名 DNS 计划和正向 checkpoint。
- `replay-de-1.json`、`dns-decision-*.json`、`local-dns-samples-*.json`：实际回答、计划索引、原始证明与结果。
- `public-route-probes.json`、`dns-netns-route-probes.json`：公开及相同 DNS 网络命名空间中的 TLS nonce 路由证明。
- `dns-probe-scheduling-capture.json`、`probe-capture-summary.json`：有限 SYN 报文与连接统计。
- `dns-listener-ready.json`：当前 listener 503 的精确原因。
- `local-reproduction.txt`：两个本机诊断复现均通过；`diagnostic_test.go` 和 `overlay.json` 是临时测试输入，未加入仓库。
- `_v1_edge_nodes*.json`、`_v1_admin_edge_authorities.json`、`worker-*-health.json`：节点过滤、权威 inventory 与备用 edge。
- 之前的两地精确 DNS 抓包：`/tmp/fugue-ssh-heartbeat-20261012T002044Z-dns-packet-witness.json` 和 `-dns-packet-witness-de.json`。

第一轮 40 秒被动采样的 SSH 输出收集曾超时，已确认没有残留采样进程；它没有纳入结论。后续 18 秒采样正常到达显式时间上限，239 个报文头、零内核丢包。所有复现和采样均有界，调查未修改业务。
