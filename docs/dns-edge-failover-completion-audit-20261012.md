# 两阶段完成性审计

2026-10-12。以原 `bwg-edge-failover-repair-plan-20261012.md` 为范围，不将当前已实现内容反过来当作完成标准。补验及后续最小修复已完成，最终运行事实与验收矩阵见 `dns-edge-failover-final-acceptance-20261012.md`。两阶段交付验收完成；小时观察继续，不把有限观察窗口等同于永久无故障。

## 要求与证据

| 原方案要求 | 当前证据 | 判定 |
| --- | --- | --- |
| 1.1 原始采集原因与release绑定原因分开 | platform_serving只对Ready事实执行release重分类；真实sync回归测试和线上probe日志 | 已证明 |
| 1.2 每轮唯一观察关联、预算、开始/结束/未调度与目标消耗 | afadd66b增加observation_id、budget_exhausted，与scope/plan digest、时间、目标消耗及省略量并存；有界预算测试通过 | 已发布至两地公共d实例 |
| 1.3 DNS决策关联实际观察和原始证明 | fact保留observation_id，source变体继承物理观察ID，decision replay原样保存事实和proof时间/绑定；新增回执关联测试，08:38真实回执核验 | 已证明；精简runtime-facts不代替诊断回执 |
| 1.4 有界、脱敏、丢弃量、诊断不阻塞查询 | 8轮128KiB探测文件、64回执/32队列及dropped/evicted/persistence指标；单独查询处理路径 | 已证明；低频失败长期保留仍依赖采集 |
| 2 已验证配置/记录健康/listener/候选验收分开 | TestDNSArtifactApplyProbeCheckpointRestartAndFailedCandidate覆盖同assignment的负面观察、签名、到期与重启；线上171负面探测时256记录通过 | 同source context已证明 |
| 2 source transition负面观察、并发assignment、撤销 | 新完整sync测试先复现source context变更后的负面观察清除Positive；修复保留原已签checkpoint和payload，独立刷新原事实并再次核对assignment/source，失败记录仍拒绝 | 11f0a22e经候选验收后完成两地公共交接 |
| 3.1–3.2 跨目标公平、跨轮推进、按证明寿命刷新 | 671/171/500、8并发压力与跨轮测试；新增expiry优先、未尝试与超两轮等待优先、防饥饿断言，原proof期限不变 | 代码、race及实际500/500新证明已验证 |
| 3.3–3.5 连接级退避、恢复、逐路由证明及assignment隔离 | connection typed error、backoff/reset、source变体不占网络worker测试；线上两地500/500正常 | 已证明 |
| 3.6 超预算明确报告且不放宽策略 | 新增budget_exhausted、两慢目标/有限并发断言、8个要求中6个未调度均关联轮次，未伪造Ready | 已测试并发布 |
| 4.1–4.2 单一公共端口所有权 | 独立transport31/32、两个旧DaemonSet已无hostPort/hostNetwork、EndpointSlice精确目标、发布工具拒绝重叠DNS53 | 本次线上所有权已证明 |
| 4.3–4.5 正常交接与过期旧端恢复、排空、冲突/空端点 | recover_dns_transport及retire_dns_hostport的身份/签名/full/quorum/CAS测试与CI记录；本机一次性kind通过真实退役patch及空Ready端点UDP/TCP故障注入，旧进程存活但接收counter不增长 | 25项相关Python测试及真实转发表隔离验收通过 |
| 5 计划离线意图与实际健康分开，日期不自动放流 | signed offline_edges、producer fence38、声明式独立CI、full3202及后续full通过；身份/日期/约束/旧LKG测试 | BWG全局DNS能力已证明 |
| 5 固定节点对象有界且保留材料 | TaintManager直接原因、Pending容器未启动条件与CAS、150个stage-owned对象持续稳定、旧对象未删 | 已证明 |
| 5 恢复的配置和完整新证明 | 显式移除hold、仍需gray/full与fresh proof的测试；本次未向真实BWG注入恢复 | 机制已验证，真实恢复须10月18日后独立验收 |
| 6 列表/详情/desired-state的配置意图、观察新鲜度与资格 | aecbcffd / CI38213865735、前端236227e1 / CI38213922926成功；线上BWG显示suspended、quota_exhausted、日期及producer/input证据，同时保留heartbeat_stale与原healthy字段 | 已发布并验证 |
| 6 traffic-safety和route explain证据边界 | traffic-safety明确inventory_and_route_policy、DNS not_evaluated、route proof另查；旧字段未改语义 | 已证明 |
| 6 首次失败/证明到期/最后坏答案/首次备用/TTL/递归/接收量 | probe journal、decision receipts、48项权威/递归样本和接收counter；新增受控故障转移时间线测试，旧答案TTL不越原proof期限，随后转备用；明确递归与旧连接不在合成测试内 | 测试及真实抽样通过；历史缺失明确不作精确切换耗时承诺 |
| 验收矩阵：地域/quorum/新候选/签名/回放/撤销/期限 | 最终验收文档逐场景列出可重跑测试及实际运行材料，新增DNS race33.810秒与prepush22.782秒通过 | 已聚合验收 |
| 发布：先候选后独立配置、main/CI、真实运行版本 | DNS候选→美国→德国→hostPort退休→API/离线配置；CI与实际镜像、full/LKG、ACK | 已证明 |
| 业务/身份不变 | 原样SSH Bash登录、known_hosts哈希、两个原业务UID/containerID、restartCount0、授权续期与在线frontACK | 已证明至最近采样，后续继续观察 |

## 未归因运行观察

德国 A/SERVFAIL 累计7→17→21→27。64条普通回执覆盖窗口约十几秒，已有几次增量发生在采样窗口之间，未取得足够因果证据；不能称为已修复或直接认定是这次发布引起。只读增量采集运行时才可保留新失败原始回执，必须报告基线重置、遗漏和具体覆盖时段，不能将静默采样当作历史无故障证明。

本轮延长一次有界观察至15分钟，使用复用SSH传输减少只读取样往返间隔，仍仅kubectl get，不制造断网、删除Endpoint或重启业务。发生新失败时先关联query ID、hostname、时间、实际Pod、plan与release/fence，再用已有deterministic replay验证。若无法取得精确原因，只补观察，不修改选址/签名/授权/有效期/quorum规则。

该15分钟观察实际完成101次采样，捕获1次增量和完整失败回执，遗漏0、采集错误0。精确定位了共享probe的DNS别名变化影响未变hostname的缺陷，重放matched=true；最小修复、反例及边界测试详见 `dns-shared-proof-record-isolation-20261012.md`。不能将这条原因推广成此前每条未知计数的原因。新修复先发布隔离dns-us-d/dns-de-d，无公网selector改动。

随后两地d候选经完整验收，再以独立transport33/34完成公共交接，CI38220906302及38222741482成功。08:29–08:44双入口只读观察152次、SERVFAIL新增0、采集错误与遗漏增量0；起始累计US0/DE1，这条窗口前的德国计数仍不能归因。没有为追求“零累计错误”改策略、延长期限或重置计数。后续小时观察继续保留准确覆盖范围与失败证据。
