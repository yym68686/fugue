# 两阶段完成性审计

2026-10-12。以原 `bwg-edge-failover-repair-plan-20261012.md` 为范围，不将当前已实现内容反过来当作完成标准。前一轮属于实质进展：生产配置、代码、测试、CI、当前 full/LKG 与只读观测均形成了新证据。本审计仍不批准关闭目标。

## 要求与证据

| 原方案要求 | 当前证据 | 判定 |
| --- | --- | --- |
| 1.1 原始采集原因与release绑定原因分开 | platform_serving只对Ready事实执行release重分类；真实sync回归测试和线上probe日志 | 已证明 |
| 1.2 每轮唯一观察关联、预算、开始/结束/未调度与目标消耗 | readiness_scheduler输出scope/plan digest及开始时间、计数、期限、32个目标和省略量；尚无明确observation_id字段 | 部分完成，需补显式关联 |
| 1.3 DNS决策关联实际观察和原始证明 | decision replay保存原始proof时间、绑定和动态事实；尚未逐事实关联新探测轮次ID | 部分完成 |
| 1.4 有界、脱敏、丢弃量、诊断不阻塞查询 | 8轮128KiB探测文件、64回执/32队列及dropped/evicted/persistence指标；单独查询处理路径 | 已证明；低频失败长期保留仍依赖采集 |
| 2 已验证配置/记录健康/listener/候选验收分开 | TestDNSArtifactApplyProbeCheckpointRestartAndFailedCandidate覆盖同assignment的负面观察、签名、到期与重启；线上171负面探测时256记录通过 | 同source context已证明 |
| 2 source transition负面观察、并发assignment、撤销 | 动态source单元测试覆盖观察、选源竞态、回放、失效和重启；完整sync改变source context后部分失败的listener保持尚未直接验收 | 待补完整状态链证据，不据此宣称已修复所有上下文 |
| 3.1–3.2 跨目标公平、跨轮推进、按证明寿命刷新 | 671/171/500、8并发压力与短窗口轮转测试通过；当前排序只有last attempt，未使用proof expiry | 公平已证明；寿命优先级未完成 |
| 3.3–3.5 连接级退避、恢复、逐路由证明及assignment隔离 | connection typed error、backoff/reset、source变体不占网络worker测试；线上两地500/500正常 | 已证明 |
| 3.6 超预算明确报告且不放宽策略 | 输出unobserved及轮次deadline；没有统一budget_exhausted分类或多慢目标的明确断言 | 待补诊断与测试 |
| 4.1–4.2 单一公共端口所有权 | 独立transport31/32、两个旧DaemonSet已无hostPort/hostNetwork、EndpointSlice精确目标、发布工具拒绝重叠DNS53 | 本次线上所有权已证明 |
| 4.3–4.5 正常交接与过期旧端恢复、排空、冲突/空端点 | recover_dns_transport及retire_dns_hostport的身份/签名/full/quorum/CAS测试与CI记录；旧实例材料保留 | 当前实施已证明；空端点隔离测试需与完整转发表断言逐项核对 |
| 5 计划离线意图与实际健康分开，日期不自动放流 | signed offline_edges、producer fence38、声明式独立CI、full3202及后续full通过；身份/日期/约束/旧LKG测试 | BWG全局DNS能力已证明 |
| 5 固定节点对象有界且保留材料 | TaintManager直接原因、Pending容器未启动条件与CAS、150个stage-owned对象持续稳定、旧对象未删 | 已证明 |
| 5 恢复的配置和完整新证明 | 显式移除hold、仍需gray/full与fresh proof的测试；本次未向真实BWG注入恢复 | 机制已验证，真实恢复须10月18日后独立验收 |
| 6 列表/详情/desired-state的配置意图、观察新鲜度与资格 | inventory observation已发布；计划停用尚未在这些响应与前端并列展示 | 本轮补齐中 |
| 6 traffic-safety和route explain证据边界 | traffic-safety明确inventory_and_route_policy、DNS not_evaluated、route proof另查；旧字段未改语义 | 已证明 |
| 6 首次失败/证明到期/最后坏答案/首次备用/TTL/递归/接收量 | probe journal、decision receipts、48项权威/递归样本和接收counter；历史遗漏明确记录，新增counter增量采集 | 系统性观察已增加；历史丢失不可补造，需用受控合成故障转移演示完整时间线 |
| 验收矩阵：地域/quorum/新候选/签名/回放/撤销/期限 | 原有平台/DNS测试及新增相关回归、make test和race记录 | 需最终逐场景聚合证据，不能只引用一次全量绿色 |
| 发布：先候选后独立配置、main/CI、真实运行版本 | DNS候选→美国→德国→hostPort退休→API/离线配置；CI与实际镜像、full/LKG、ACK | 已证明 |
| 业务/身份不变 | 原样SSH Bash登录、known_hosts哈希、两个原业务UID/containerID、restartCount0、授权续期与在线frontACK | 已证明至最近采样，后续继续观察 |

## 未归因运行观察

德国 A/SERVFAIL 累计7→17→21→27。64条普通回执覆盖窗口约十几秒，已有几次增量发生在采样窗口之间，未取得足够因果证据；不能称为已修复或直接认定是这次发布引起。只读增量采集运行时才可保留新失败原始回执，必须报告基线重置、遗漏和具体覆盖时段，不能将静默采样当作历史无故障证明。

本轮延长一次有界观察至15分钟，使用复用SSH传输减少只读取样往返间隔，仍仅kubectl get，不制造断网、删除Endpoint或重启业务。发生新失败时先关联query ID、hostname、时间、实际Pod、plan与release/fence，再用已有deterministic replay验证。若无法取得精确原因，只补观察，不修改选址/签名/授权/有效期/quorum规则。
