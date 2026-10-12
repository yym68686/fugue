# DNS / edge 两阶段最终验收

2026-10-12，08:44 UTC 运行观察基线。原范围是 `bwg-edge-failover-repair-plan-20261012.md`，逐项审计见 `dns-edge-failover-completion-audit-20261012.md`。代码、配置、运行事实分别验收；以下版本是本次采样事实，不是禁止后续合法更新的硬编码要求。

## 发布与实际运行

| 项目 | 发布与线上证据 |
| --- | --- |
| 探测调度、原始原因、listener 与记录隔离 | 初始修复 d2b042de；之后 afadd66b 增加观察ID、预算不足、到期优先级和共享证明的逐记录隔离；11f0a22e 补齐 source context 变更后的完整 sync 状态链 |
| 最终 DNS 候选 | CI38218836953 成功；先独立部署 dns-us-d/dns-de-d，未修改公共 selector；完整签名、当前 verified full、proof、记录 quorum 和直接 UDP/TCP 校验通过后才交接 |
| 美国公共入口 | 8b890fad / CI38220906302 成功；transport generation33，dns-us-d，Pod UID 7ad805ed-204b-44e9-b930-646beb93fa08，镜像 sha256:dc904fe9a9d8222538a4397867ffb6f6750e5ef28643522b80d2ded80e06fc55 |
| 德国公共入口 | f57324b8 / CI38222741482 成功；transport generation34，dns-de-d，Pod UID f27f2163-0f27-4f90-a2a8-a26dcfbc3936，镜像 sha256:d556abe934dbf36c3238c95b553fdb095036987aeadaa8b59af613fe8a4c9a4a |
| API 状态语义 | aecbcffd 增加签名暂停意图，afadd66b 更新兼容契约；实际 API 来源 afadd66b，镜像 sha256:574269839748570c76b81e9367c98b118cd44e4ed61bfeb8594d0701f2700904，2/2 Ready、零重启 |
| 前端 | 236227e1 消费独立暂停意图，CI38213922926 成功；3e476129 同步后续可选诊断契约，CI38217428022 成功 |
| BWG 长期离线 | 签名 producer fence38，quota_exhausted、预计2026-10-18；新全局DNS计划不包含其端点，3个在线 Worker 与2个 DNS 的原验证条件继续生效；独立 Cell 固定拓扑不被静默改写 |

08:27 UTC 两地 EndpointSlice 各自唯一指向表中 Ready、serving、非 terminating 实例。旧两个 DNS DaemonSet 无 hostPort、无 hostNetwork，退休标记及原身份/缓存材料保留。正常 transport 再次只读 plan 为0变更。两次公共交接均只选择配置，不构建或重启业务。

08:31 UTC 德国、08:35 UTC 美国独立密码学校验通过，各500/500新鲜正向证明、完整记录 quorum、当前已验证 full；DNS Pod 重启均0。plan digest 为 sha256:f93ee8a160024f6fddbaba0ec7f31451f407ff13b8f88d302a6f186699af207f。采样经过正常 gray/full 更新时，严格校验会暂时拒绝未匹配当前 verified full 的快照；有界重读后通过，未放宽校验或延长期限。

## 已证明的额外缺陷

06:44:18 UTC 捕获的德国 SERVFAIL 有完整 query ID、decision ID、实际实例及不可变产物，原样重放 matched=true。一个改变选择规则的DNS别名与未改变的主机记录共享probe ID；全局保守过滤被错误用于逐记录transition，连带阻止了未改变记录转入完整合格的新候选。修复只拆分兼容性判断，旧proof直接桥接仍保守，变更记录仍拒绝，不混合两代证明凑quorum，不推进未验收checkpoint。详见 `dns-shared-proof-record-isolation-20261012.md`。

完整 source context sync 测试又证明：相同assignment、不同source context的负面观察仍会清除原Positive。11f0a22e保留原已签checkpoint，独立刷新原facts并再次核对assignment/source；未验证候选不能获得正向身份。测试先在旧代码失败，再由最小修复通过。

## 验收矩阵

| 场景 | 可重跑证据 |
| --- | --- |
| 671要求、171共享黑洞、500健康、8并发；跨轮推进 | TestDNSReadinessBlackholeDoesNotStarveHealthyRoutes、TestDNSReadinessProgressSurvivesShortObservationWindows、TestDNSReadinessReusesCapacityAfterAnotherTargetCompletes |
| 多慢入口、预算不足、连接退避、assignment隔离、恢复与到期优先 | TestDNSReadinessBackoffIsConnectionScopedAndAssignmentBound、TestDNSReadinessRefreshPrioritizesExpiryWithoutStarvingUnobservedOrAgedWork、TestDNSReadinessReportsBudgetExhaustionAndBindsEveryFactToSweep |
| 完整sync、负面记录不撤销listener、新候选失败、重启与控制面失败 | TestDNSArtifactApplyProbeCheckpointRestartAndFailedCandidate、TestSourceContextFailureKeepsVerifiedListenerWithoutAdmittingNegativeCandidate、TestDNSAssignmentRacePreservesServingAndRetriesBoundedly |
| 签名、绑定、遗漏/重复proof、过期/未来、期限伪造、撤销 | TestServingObservationAuthenticatesCheckpointAndEveryPositiveProof、TestNeutralDNSRejectsForeignIdentityBeforeFetchingOrChangingState |
| 逐记录隔离、地域/原约束、完整quorum与原正向checkpoint | TestDNSRecordTransitionPreservesAuthorizationAndCompleteQuorum、TestDNSRecordTransitionIsolatesChangedAliasSharingRouteProofs、TestDNSDeploymentTransitionKeepsPerRecordAnswersWithoutPromotingLKG |
| 观察关联、回放与诊断不续租 | TestDNSDecisionRetainsOriginalSweepAndProofDeadlines、TestDNSDecisionReplayActualAnswersAndPrivacy、TestDNSRuntimeFactsNeverRenewEvidenceOrMutateServingState |
| 故障转移时间线 | TestDNSFailoverTimelineBoundsLastPrimaryAnswerByOriginalProofExpiry：首次失败+1秒，旧证明+2秒到期，最后主入口答案TTL至多1秒，+2.001秒使用独立合格备用；递归缓存和已有TCP会话不在该合成测试模拟范围 |
| 正常交接、过期旧端独立恢复、实例/CAS冲突、空端点与排空 | scripts.test_reconcile_dns_transport、scripts.test_recover_dns_transport、scripts.test_retire_dns_hostport；不以过期旧端作正向证明 |
| 真实转发表空端点故障注入 | python3 -m scripts.verify_dns_transport_isolation：一次性本机 kind 集群，使用实际 retirement CAS patch；旧进程内网仍可回答，撤销候选Ready后3轮外部UDP/TCP均无法进入旧进程，旧接收counter未增加，恢复候选后返回正确身份 |
| 离线意图、明确恢复、日期不自动放流 | 签名offline_edges编译/身份/约束与候选执行集测试、fixed-node-retention测试；实际BWG恢复只能在网络恢复后另行验收，不伪造生产恢复 |

真实转发表测试耗时56.87秒，16项查询完成，隔离集群及测试镜像已清理。测试使用kind v1.34的实际kube-proxy/CNI，不能冒充生产k3s故障注入；生产只做所有权、EndpointSlice与公网报文的只读验收。没有删除生产Endpoint或断网。

新增补验后 DNS 包 race 33.810秒通过；transport/recovery/retirement/failure-sampler共25个Python测试通过；prepush使用CI相同240秒预算，22.782秒通过。前面的API变更已运行make test和OpenAPI生成/漂移检查，前端契约、118项测试及类型检查已通过。新增回执关联和隔离测试仅为测试资产，不选择运行组件发布。

## 公网、SSH 与对象边界

- 外部GitHub Runner在08:22:54–08:23:01 UTC完成48/48抽样：两地权威及1.1.1.1/8.8.8.8，UDP/TCP的A/AAAA/NS/SOA，含正确空AAAA语义。
- 08:29:07–08:44:05 UTC只读观察两地实际接收实例，共152次采样，SERVFAIL新增0、采集错误0、遗漏增量0。起始累计基线美国0、德国1；德国这一条发生在窗口之前，没有足够回执证明其原因，不能声称全部历史失败已归因或从未发生失败。
- 08:38 UTC真实决策回执保留observation_id、原CheckedAt/ValidUntil，可关联探测轮次；`runtime-facts`精简快照仍不输出原始错误文本或轮次ID，不应把它当作完整诊断回执。没有输出上游秘密或业务响应。
- 08:29 UTC原样ssh poly/argus均进入-bash，SHELL=/bin/bash、HOME=/workspace，严格批准指纹及known_hosts哈希不变。Warp UI未测试。
- 08:28→08:38 UTC授权generation为poly1019→1028、argus1015→1024，同一身份及slot持续续期，最后剩余约234–240秒；三个在线front精确实例ACK新鲜且路由匹配，BWG为预期Pending。
- 两个业务Pod UID/容器ID与基线一致，业务/workspace重启均0。BWG stage-owned对象150个，148历史terminating及2个04:21创建的Pending，未新增替换、未删除历史材料。

## 证据与后续观察

本机私有目录 `/tmp/fugue-bwg-investigation-20261012/`：`d-public-cryptographic-final.jsonl`、`postrelease-runtime.json`、`ci-public-d-final/dns-public-observation.json`、`dns-failure-timeline-both-d-20261012-final.json`、`de-d-current-decision.json`、`ssh-health-final-d-before.json`、`ssh-health-final-d-after.json`。隔离测试回执 `/tmp/fugue-dns-transport-isolation.json`。

历史缺失不可重建：不能给出BWG最初断网到所有客户端恢复的精确耗时，也不能把此前累计7→27条错误全部归因于唯一捕获案例。现有采集报告覆盖窗口、丢弃/淘汰、实例重置和未关联增量；证据不足时继续补观测，不猜测改选址或放宽安全约束。DNS只影响新解析，不能迁移已有TCP连接或强制外部递归缓存失效。

既有fugue-ssh自动化继续每小时观察24次，正常安静，完成后暂停；人工验收不计入次数。新错误先保存精确回执并重放，再决定修复。2026-10-18是BWG提示日期，不触发自动放流。保留签名、身份、quorum、原始有效期与正向LKG，不修改Argus应用、SSH身份、信任文件或备份。
