# DNS / edge 故障转移修复与验收

2026-10-12。用户已授权两个阶段、测试、push main、经 GitHub Actions 发布并观察。两个阶段已发布并完成主要线上验收，小时观察继续。本文是后续自动观察的当前基线。

## 最终发布基线

后端 main 的运行代码为 `65314250e27cf05179ef4905241263f08dab8f18`，CI38209314445成功，实际 API 镜像 `sha256:d355102f72ab05e7577564de6e0b518b73d5030236c91e0b158fa54e3ea223ba`，2/2副本 Ready。前端 `bd3e1fb0119d38ba3d937769785ad79d6ffb6d36` 的契约 CI38209387470成功；Fugue 自动部署了 git-bd3e1fb0119d 镜像，2/2副本 Ready。

BWG 离线意图已由 CI38207414787 发布为 producer fence38，release `artifactrel_1791782707_5fc3b4a75142`，策略 `artifact_1791781856_c6215c270ceb`。当前策略保留 BWG quota_exhausted / 2026-10-18 提示，禁止按日期或心跳自动解除停用。

新流量产物 `artifact_1791784345_d2939663580e` 于05:52:26 UTC进入灰度、05:54:30进入 full fence3202、05:56:34完成 LKG 验证，未缩短120秒灰度/全量观察窗口。required 集合为3个 Worker（美国、德国、vmarckedge）及2个 DNS；route、TLS、DNS 的真实当前实例分别3/3、3/3、2/2通过。DNS计划变为500个探测、256个记录，不含 BWG。后续正常 producer 更新的 full fence3203（artifact_1791784718_719bae3f65cc）也已验证，说明持续发布没有再次被离线 BWG 阻断。自动观察不得把合法后续版本当成回归或重复发布旧产物。

停用后的美国05:58 UTC、德国06:04 UTC独立签名校验均通过：500/500新证明、完整记录quorum、当前已验证full匹配、原进程零重启；新plan digest为 `sha256:1e927867cf708c2922610c28a403179a25ef8862587c184f39f91e116a64ca8c`。06:02 UTC两应用授权剩余约198–205秒、三个在线front ACK新鲜且路由匹配，BWG为声明中的预期Pending。

05:55:53–05:56:27 UTC 的48项权威/递归 UDP/TCP 抽样通过；05:58 UTC 的原样 SSH 双应用登录通过，严格指纹与 known_hosts 哈希未变。05:58 UTC 两个业务 Pod UID、容器 ID 不变，业务/workspace 重启均0；BWG stage-owned对象仍150个（148历史 terminating、2 Pending），约97分钟没有新一轮驱逐替代。

停用发布过程中发现并修复两个交付缺口：首次预览不相同被严格拒绝，未放流；因当时未保存差异，不能推断是哪项业务输入变化。新增有界路径/digest诊断后重试，前后 intent digest严格相同才通过。随后精确日志及 expected-set证明，候选 DNS已不含 BWG但新 Worker执行集仍包含它，导致 fresh capability门禁拒绝；修复为按候选自身签名来源准备执行集，旧 LKG成员不变。未绕过门禁、缩减存活成员或修改独立 Cell拓扑。

独立 `cell-public-b` 的固定签名拓扑仍含 BWG，它自己的新发布等待该成员能力是现有安全约束；本次全局 DNS停用没有授权静默删除独立 Cell成员。SSH控制器仍可记录预期离线 front缺失和毫秒级 Secret投影期间的候选digest暂不一致；原生登录、有效授权续期及三个在线入口ACK另行验证，不把累计错误计数直接当成本次回归。德国 DNS累计7条A/SERVFAIL的首发时间未由当前快照证明，保留计数基线并观察增量，不猜测归因或放宽证明。

## 已证明的原因与修复

| 已证明的问题 | 已发布修复 |
| --- | --- |
| 固定顺序的共享探测队列，使失联入口消耗预算，后段健康目标连续多轮未开始 | 按物理连接目标分配有界并发、轮转和最后尝试时间；空闲配额复用；仅连接层失败触发有界退避，每条路由仍独立验证 |
| 同一已接纳 assignment 的部分负面事实清除正向标记，整个 listener 退出 Ready | 同 assignment、同 source context 保留完整原正向记录及原始签名/期限；记录健康独立更新，不合格记录拒绝，未验证候选不能借用 LKG |
| 空证明的原始原因被覆盖成 release mismatch | 保留 not_observed、probe_unavailable 等原因；真实错误绑定仍拒绝；增加有界持久探测与消费者失败观察 |
| 两地旧 hostPort Pod 实际回答公网 SERVFAIL，Service 无端点时仍存在旧接收路径 | 独立恢复交接到已验证候选，再按精确实例、端口和 CAS 清退两个旧 hostPort；发布时拒绝重叠端口 |
| 固定到 BWG 的 SSH front 每300秒被 NoExecute TaintManager 驱逐，同一 ReplicaSet 再创建替代 Pending | 声明式保留两个未启动容器的 Pending，移除驱逐期限；新固定节点 front 使用同样语义，已有运行实例不因代码变化重建 |

原始证据与复现见 `bwg-edge-failover-investigation-20261012.md`。单条历史空证明的原始原因及完整客户端切换时长无法从丢失数据恢复，未据此猜测修复。BWG、DMIT 额度耗尽属于预期离线，本次没有恢复其网络。

## 第一阶段线上结果

DNS 代码 `d2b042de` 的隔离候选经 CI 发布，公共 Service 分别以 generation31、32 选择到 `dns-us-c`、`dns-de-c`。配置恢复与代码构建分开。

| 公网入口 | 实际接收 Pod / UID | 镜像 digest |
| --- | --- | --- |
| 美国15.204.94.71 | fugue-dns-us-c-s24qw / a94c22b8-e6d7-47b2-8387-67a34402f62c | sha256:f5eaf96407ef46fc9473e84542eb13750fd14c19b0a096f60a5bd200772c9dfe |
| 德国51.38.126.103 | fugue-dns-de-c-dtmwh / 2561a8f8-4d06-4664-840f-481b5e56707d | sha256:9ed5dda47e452310015f2f6a401c09646b3e0a245c346ed8a9c20bdcfffba9a7 |

截至05:03 UTC，独立签名校验匹配已验证 full fence3201，plan digest `sha256:a3588aa99c8915e4093ff91d5ceb3ce27aad38e69273b0388a94c41adbb004d1`。671个要求中500个正向证明、171个 BWG 连接失败，256/256记录满足 quorum，两个进程零重启。BWG 未进入当前答案，`i00.pro` 返回 vmarckedge 154.51.41.196。

校验器检查真实 checkpoint HMAC、产物签名、当前 full、身份/租户/路由/release 绑定、原始期限及完整记录 quorum。跨 SSH 采集偶尔经过 checkpoint 更新边界，曾观察到期限相差约60秒；严格拒绝后有界重读通过，未放宽一致性或延长期限。

05:06 UTC 两地 EndpointSlice 唯一指向上述 Ready、serving、非 terminating Pod。旧两个 DNS DaemonSet 均带退休标记，无 hostPort/hostNetwork；镜像与 hostPath 材料保留。DNS CPU59–127m、内存90–92Mi，仅代表该时刻样本。

外部 GitHub Runner 先通过24项权威 UDP/TCP 检查；补充递归样本后，本机48项检查全通过，外部 CI 也通过。覆盖两地权威、1.1.1.1、8.8.8.8，各自 UDP/TCP 的 A、AAAA、NS、SOA 及空 AAAA 语义。维护工具 `cmd/fugue-dns-observe` 保存时间、query ID、RCODE、地址、TTL 和来源，不赋予放流权。样本不代表全量客户端流量比例或首次历史故障转移耗时。

## 第二阶段

`bd5419aa` / CI38203193758 发布一致的 inventory observation API，列表显式支持 include_stale，详情与 desired-state 同口径。历史 healthy、心跳新鲜度、准入、route/TLS 状态分开；route_proof_status=not_evaluated 明确边界。前端4f21fc20 已消费，契约 CI38203377482 和实际自动部署通过。

`f2c23197` / CI38205003686 增加 traffic-safety 证据范围。线上 i00.pro 返回 evaluation_scope=inventory_and_route_policy、public_dns_verification=not_evaluated、consumer_proof_verification=see_route_explain；admission pass 不代表公网证明。

`a49e5cea` / CI38203482606 已应用 fixed-node-retention。至05:03 UTC，受 fugue-ssh-front-stage 管理的 BWG 对象保持150个：148个历史 terminating、2个 Pending；再次只读 plan 为0变更。两个 Pending UID 为3e467db1-206e-4cd9-a083-5562f2da8eb2、84ac299f-5a1e-4dfa-8a6b-ce0555e1ec4b，创建于04:21:13 UTC。全组件盘点另有1个旧 DaemonSet 对象，9月27日创建、10月11日22:11:49 UTC 开始 terminating；它不是修复后新增长，不能混用统计口径。

对象保留不等于流量停用。`b627c614` / CI38206127709 发布通用 offline_edges 签名意图能力，独立配置提交4947df75 / 2d4f784d以及执行集修复65314250最终完成发布；实际 full/LKG验收见本页最终基线。预计恢复日只记录2026-10-18。

停用/恢复见 `edge-offline-maintenance.md`。代码没有节点名或日期特例；原始优先序、地域、quorum、期限、签名和旧正向 LKG 保持。日期或心跳不会解除 hold，恢复需要新配置及正常 gray/full、逐路由新证明。

探测与消费者观察各保留8轮；DNS 回执有界队列及 dropped、evicted、persistence_errors 指标。05:06 UTC 两地 dropped、持久化错误均0，历史淘汰数7777、6445，不能把保留窗口外缺失理解成从未发生。公网 CI 样本保存90天；小时观察保存增量证据与计数变化。

## SSH 与业务保护

05:04 UTC 无 remote command 的原生 ssh poly、ssh argus 均通过，进入 -bash，SHELL=/bin/bash、HOME=/workspace。严格主机校验仍为已批准指纹：poly SHA256:D1U9XQ7oTud3JorLgCG6niNL2Oyarz3thEwZ0zxFNeE；argus SHA256:NzpjX49MTVtTVo2ix2NnEwpSbCTeQO5mnqTIQUArx8M。known_hosts 哈希不变；Warp UI 未测试。

05:06 UTC 业务 Pod UID 仍为2bf8eb72-35df-4997-9d31-a759770cda5a、e2e360b4-0d88-49f6-ad04-534dcd10d8da，容器 ID 未变，业务/workspace restartCount 均0。没有修改 Argus 代码、重建业务镜像、旋转身份、删除密钥/备份或通过 SSH 热修生产。

## 测试、发布与观察

完成671探测/171共享黑洞/500健康/8并发的负载测试、局部竞态、退避恢复、原始原因、完整 sync 状态链、签名/绑定/撤销/期限回归。端口交接、退休、保留与独立配置使用隔离测试及服务端 dry-run，未向真实业务注入故障。

OpenAPI 先改契约再生成，后端 make test 全通过；停用能力 prepush 使用 CI 相同240秒预算，用时214.160秒通过，局部竞态通过。配置 prepush10.474秒通过，组件 release plan 为空。前端生成检查及最新 CI38206160013 通过；本地完整 tsconfig 包含被 Git 忽略的历史调查文件，临时排除这些文件的检查通过，未改项目 tsconfig 或忽略测试。

主要成功 CI：美国恢复38200977411，德国恢复38201555518，旧端口退休/Guardian38202514211，对象保留38203482606，观测38204049037，证据范围38205003686，停用能力38206127709。首次德国候选2分钟内未 Ready 被正常补偿，原诊断未完整保存，未猜测每条原始原因；现已补持久诊断。前端5872b1b8 先于对应后端 push，漂移检查正确拒绝；后端发布后重跑通过，未绕过检查。

本机证据目录：/tmp/fugue-bwg-investigation-20261012/。关键材料：authoritative-recursive-observation.json、verified-before-offline-config.jsonl、postrelease-runtime.json、native-ssh-after-dns-repair.json、ssh-front-eviction-cause.json。复用脚本前先核对最新合法实例与版本。

最终追加材料：offline-configuration-ci/edge-offline-configuration.json、public-during-gray.json、ci-public-recursive/dns-public-observation.json、verified-final-offline-config.jsonl、verified-final-renewed-config.jsonl、ssh-health-before.json、ssh-health-after.json。运行期校验跨过合法下一轮灰度/full时仍会严格拒绝“当前已验证full不匹配”；这不代表签名失效，不得修改验证器来强行通过。最终成员修复的make test、竞态通过，prepush146.780秒通过。

已有 fugue-ssh 自动化更新为“Fugue DNS、edge 与 SSH 上线后观察”，每小时24次，完成后暂停；新周期0/24，不混入修复前8次。正常无可处理变化时安静，仅异常、完成或需要用户行动时通知。
