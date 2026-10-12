# 计划离线与恢复

计划离线是签名配置意图。节点心跳、连接探测、运行实例 ACK 和逐路由证明是运行时事实，两者分别管理。

全局 producer 的固定投影策略支持 `offline_edges`。每条记录绑定 physical edge ID、group、IPv4、原因和可选的预计恢复日期。配置按 edge ID 排序，最多 64 条；原因限于 `quota_exhausted`、`maintenance`、`network_unavailable`。日期不构成租期，也不解除停用。配置没有任何业务项目或节点名称分支。

该输入过滤将要编译的 DNS candidate endpoints。原始节点健康没有被改写，Host 路由、容器、卷、密钥及既有连接没有被撤销。原始选址顺序仍保存在策略中；实际生成的记录只保留符合地域、路由约束且未被停用的候选，并保持剩余候选的顺序。新的域名同样使用该全局输入，避免只修改事故发生时存在的 hostname 清单。

全局候选的 required Worker membership 使用该候选签名 parent 所固定的 producer release、静态输入和投影策略。只有三者签名和 digest 匹配、声明端点身份一致，并确认 DNS 探测及记录计划完全不包含该端点时，才从新候选的预期执行集移除已停用 Worker。仍需新建精确的 immutable expected-set revision；存活 Worker 与所有 DNS consumer 的能力、身份、有效期、ACK 和 quorum 门禁不变。旧 full/LKG 使用自己的原策略，后来的 hold 不能改写它的成员。独立 Cell 的显式拓扑仍完整保留，不能借全局 hold 静默删减。

`edge_offline` 操作只允许一次增加、修改或移除一条记录，以及推进策略/producer generation；其余选址策略、quorum、证明期限、静态意图、发布节奏与观察窗口保持完全相同。后端在 producer-before-target 事务锁中校验原 producer、当前 full、未过期的正向 LKG、签名和来源绑定。配置写入并不直接切换当前 DNS 答案；正常 gray/full 流程继续要求实际 consumer 验收。

配置入口为 `deploy/environments/production/edge-offline/*.json`，执行器为 `scripts/configure_edge_offline.py`。仅 `ci.yml` 的独立 `edge_offline_configuration` lane 可以执行生产写入，且一次提交只能选择一个声明。该 job 不依赖镜像构建或组件发布，仍有精确前置条件、Git supersession 检查和正式生产环境约束。代码能力先发布验收，再以单独提交选择配置。失败的候选不覆盖正在服务的正向 LKG。

执行前检查当前 policy/full 来源，生成下一份完整投影策略，仅改一条 `offline_edges`。声明中的 `latest_verified_same_policy` 只允许接纳同一旧 producer 的更高 fence 且已验证的新 full；旧 producer 本身变化时停止，不能用“最新”绕过前置条件。执行器保存声明 digest、解析后的前置条件、变化的 edge、预览候选集合和最终 producer publication；`routing_acceptance_complete=false` 明确表示还需后续实际流量验收。

恢复需要显式移除对应记录的新配置提交。日期到期或心跳恢复不会执行该操作。移除后只是恢复候选资格，新的 route/TLS proof、身份绑定、quorum、gray/full 观察窗口仍必须通过；历史成功证明和旧 ACK 不能代替。实际端点身份发生变化时必须单独审阅更新，不能在原 hold 上静默换地址。

固定节点 SSH Pod 的 `fixed-node-retention` 配置解决另一件事：阻止离线节点的默认 300 秒 NoExecute 驱逐反复创建 Pending 对象。它只修改已确认没有启动容器的 Pending 实例，保留所有历史 terminating 对象与材料；不会自动加入新流量，也不充当上述签名停用策略。两个配置可以独立恢复。

验收需检查新的 full/LKG 与 DNS 实际装载版本、每条记录 quorum、两个公共入口 UDP/TCP、抽样递归答案/TTL、业务实例与重启计数，以及原样严格 SSH 登录。`cmd/fugue-dns-observe` 的样本不具有放流权，也不代表所有客户端流量比例；CI 保留 90 天样本，DNS 实例保留有界探测观察和决策回执，后者包含丢弃/淘汰计数。
