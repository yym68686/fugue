# 共享探测的逐记录隔离

## 已捕获的失败

2026-10-12 06:44:18.125790632 UTC，德国实际接收实例 `fugue-dns-de-c-dtmwh` / UID `2561a8f8-4d06-4664-840f-481b5e56707d` 记录 query ID22262，UDP A查询，decision ID `e14602a50d3c8b3085fc12004c64d2a9-14611`，实际写出SERVFAIL。主机记录为 `argus-telegram-bot.fugue.pro`。只读采集窗口15分钟记录101次采样，捕获1次SERVFAIL增量和对应完整回执，遗漏增量0、采集错误0。

`fugue admin dns decisions replay` 对原始回执离线重放，evidence digest校验通过，matched=true、rcode=2。原始材料位于 `/tmp/fugue-bwg-investigation-20261012/de-servfail-064418-receipt.json`、`de-servfail-064418-replay.json` 和 `dns-failure-timeline-20261012T0633Z.json`。这只能精确解释该条已捕获请求，不能反推此前全部计数增长的原因。

## 因果链

1. DNS仍保留已接纳的full3209，期望切换到新的gray3208。full和gray的fence属于不同channel，不按数值跨channel比较。
2. 三个物理入口实际返回的proof均属于新gray，原始CheckedAt约06:43:53–06:43:58、ValidUntil约06:45:53–06:45:58，查询时并未过期。主机记录的route digest、端点、quorum、规则、选择策略、业务流量约束均与旧版本相同。
3. 一个由同样HTTP route证明支持的DNS别名在该次合法候选中改变了physical selection/order模式。别名与主机记录共享三个probe ID。
4. `compatibleDNSReleaseProbes`按probe ID做保守全局删除：任何共享记录不兼容，就禁止这些probe为旧记录提供桥接证据。这对旧记录直接复用proof是必要的，但`dnsTransitionRecords`错误地复用了该全局删除后的集合。
5. 因此未变化的主机记录也没有生成独立候选transition。旧记录不能使用新发布proof（正确），又无法转向该记录完全相同且已满足quorum的新候选（缺陷），所有地址被filtered，最终SERVFAIL。

以上由原始decision回执、两份不可变DNS产物和当前代码离线运行共同证明。`de-servfail-064418-bridge-diagnostics.txt` 显示common/query/rule/traffic均相同、该记录plan相同，但transition=false；共享别名的规则改变是删除路径。修复后的同一输入见 `de-servfail-064418-bridge-fixed.txt`，旧proof桥接仍为false，未变化记录的独立transition变为true。

## 最小修复

将共同发布约束、逐probe等价和逐record等价的计算分开。旧checkpoint直接使用后继proof的路径仍采用原来的全局保守删除；独立record transition根据自己的record规则与完整依赖判断，不被另一个共享probe的记录连带删除。

查询仍使用一份完整、原始签名候选计划及其精确publication proofs，不把两代proof拼起来凑quorum，不授权已变更的别名，不推进候选正向checkpoint，不改原始CheckedAt/ValidUntil、租户、端点、地域或选择权重。

中性合成回归 `TestDNSRecordTransitionIsolatesChangedAliasSharingRouteProofs` 先在旧实现失败，修复后通过；同时验证变更别名仍拒绝、未变记录正常、proof到期后拒绝、旧checkpoint内容不变、候选未被提升。既有完整sync、绑定、quorum、身份、期限、部署transition测试及race全部继续运行。

与该修复同时补齐原计划中的探测观察ID、预算耗尽分类和证明到期优先级。新排序优先未尝试/超过两轮未尝试的工作，然后按原proof期限刷新，避免到期优化再次引入饥饿；每条事实保留原轮次ID。诊断不授予新资格，不延长期限。

上线使用独立 `dns-us-d` / `dns-de-d` 候选槽位，独立缓存及ServiceAccount，无hostPort，不重建当前公共接收Pod或业务。必须先完成候选验收，再由独立transport配置逐地交接；代码发布本身不切公共流量。
