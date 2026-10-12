# DNS / edge 故障转移修复进度

用户已授权完成两阶段、测试、push main、经 CI 正常发布并验收。目标仍在进行；本文不代表全部完成。

## 已发布的候选代码

- `ff750b2b2e9ffe03795d88a71616fd90f20d01ad`：连接目标预算隔离、跨轮推进、连接故障退避、保留原始失败原因，以及同 assignment / 同 source context 的负面观察不清除已验证正向 checkpoint。
- `d2b042de3ec80bc48d408b307ec8aeff8a3f614f`：空闲并发容量复用、8 轮有界持久探测/失败观察，以及独立签名观察校验工具。
- 使用新的 `dns-us-c` / `dns-de-c` 隔离槽位，缓存路径独立，无 hostPort，也没有重建或修改业务容器。

首轮 CI `38197538646` 美国候选成功，德国候选在两分钟健康检查窗口内未 Ready，被正常补偿删除。删除前没有保存完整运行时诊断，不能把首次失败的每条事实归因到某个猜测。后续增加了持久诊断，并通过局部测试证明静态并发配额会浪费已经完成目标的容量，修复为可复用预算。

第二轮 CI `38198775594` 的两个候选发布 job 都成功。两轮 CI 的 `front_external_observation` 对历史探测入口超时；它当时在没有 Front 配置变更的每次 push 都运行，不是 DNS 候选发布失败。后续修改把该验收绑定到真正选择的 Front transport 操作及其目标入口。

| 候选 | Pod UID | 镜像 digest | 验收 |
| --- | --- | --- | --- |
| 美国 `fugue-dns-us-c-s24qw` | `a94c22b8-e6d7-47b2-8387-67a34402f62c` | `sha256:f5eaf96407ef46fc9473e84542eb13750fd14c19b0a096f60a5bd200772c9dfe` | 671 个探测要求中 500 个健康证明，完整记录 quorum，通过当前发布及 checkpoint 签名校验，零重启 |
| 德国 `fugue-dns-de-c-dtmwh` | `2561a8f8-4d06-4664-840f-481b5e56707d` | `sha256:9ed5dda47e452310015f2f6a401c09646b3e0a245c346ed8a9c20bdcfffba9a7` | 同上 |

2026-10-12 03:30–03:32 UTC 的独立验证匹配当前 global full 的 fencing token 3201；plan digest 为 `sha256:a3588aa99c8915e4093ff91d5ceb3ce27aad38e69273b0388a94c41adbb004d1`。另外从控制平面直接查询两个候选 Pod，`api.fugue.pro`、`i00.pro`、`ssh.fugue.pro` 的 UDP 和 TCP 共 12 次均返回正确权威答案；`i00.pro` 返回 vmarckedge，未返回 BWG。

校验工具开发时发现两个独立的问题，均未触发公网修改：初版 CLI 漏传候选主区域，导致区域绑定校验失败；跨 SSH 读取材料曾耗时 30.584 秒，超过 30 秒观察新鲜度。现已传入实际区域，并在取得验证材料后重新读快照。另一次采集跨过 checkpoint 刷新，观察到租期差 59.573 秒；严格一致性校验拒绝，后续有界重读通过，没有延长或忽略租期。

## 公共入口恢复配置

`e0ae55f1` 增加独立恢复交接，并声明 generation 31 将美国公共 Service 选择到 `dns-us-c`。这次变更的正常组件发布计划为空，公共配置恢复不重建 DNS 或业务代码。

恢复交接要求：明确旧实例 NotReady；精确 Pod/Node/Service UID；候选不可变镜像；真实 checkpoint 签名及完整产物绑定；每条正向证明和完整记录 quorum；当前已验证 full 发布；两次身份/assignment 核验；Service resource-version CAS。正常健康交接仍走原严格流程。CI 保存预写与执行证据，不将过期旧实例当作回退目标。

该提交首次 CI 因独立配置 job 没有初始化 Go，在任何写入前失败；`ddf8d2e8` 补齐 toolchain，CI `38200977411` 完整成功。generation 31 美国公共 DNS 恢复，真实公网 UDP/TCP 对三个关键记录均正确。`177162bc` 独立切换 generation 32 的德国入口，CI `38201555518` 完整成功，两轮公网 UDP/TCP 共 12 次查询通过。两个公共入口均返回合法备用 edge，不含 BWG；`i00.pro` 返回 vmarckedge。

旧 hostPort 接收权清退仍需完成，不能因为候选 Ready 或一次公网响应正常就宣布全部修复。新的 retirement 流程只改声明中的旧 DNS 容器 hostPort，不改镜像、应用、身份或 hostPath；前提是替代端完整签名/当前发布/记录/EndpointSlice 验收通过，旧进程 NotReady、无活动 TCP 53 连接，并通过完整 UID/RV/spec CAS。另加代码发布冲突检查，防止未来旧配置重新引入与公共 Service 重叠的 DNS hostPort。

## 长期离线积累的新增直接证据

保存于 `/tmp/fugue-bwg-investigation-20261012/ssh-front-eviction-cause.json` 的 Pod 元数据已经明确显示：固定 `nodeName=bwg` 的 SSH front Pod 带默认 300 秒的 `not-ready` / `unreachable` NoExecute tolerations。Pod 的 `DisruptionTarget` 原因为 `DeletionByTaintManager`，消息为 `Taint manager: deleting due to NoExecute taint`；同一个 ReplicaSet 随后创建替代 Pod，约五分钟后再次触发。观察时累计 112 个对象。

因此，固定节点实例被反复驱逐、重建的原因已从推测变为直接证据。后续修复须保持现有连接、身份、卷和备份，不强删历史 terminating 对象；离线生命周期配置与镜像构建分别管理，不能因修改 staging 代码而隐式重建所有现有 SSH front。

## 待完成

1. 验收美国公网恢复，然后独立恢复德国；核对真实 UDP/TCP 路径、实例 ACK、完整记录与节点资源。
2. 清理公共 DNS 的旧 hostPort 接收权，并阻止后续代码发布重新引入重叠所有权；保留所需 checkpoint/状态材料。
3. 完成长时间离线的通用配置、恢复门槛、固定节点对象防反复驱逐，以及一致的 API/前端状态口径。API 变化按 OpenAPI-first，同步前端生成契约。
4. 完成故障转移时间线与实际入口观测、必要回归测试和正式发布，继续观察新问题。

本机材料目录为 `/tmp/fugue-bwg-investigation-20261012/`；CI 材料位于 `/tmp/fugue-dns-repair-ci-38197538646/`。验证脚本为 `/tmp/fugue-dns-verify-candidates.py` 和 `/tmp/fugue-dns-wire.py`，前者仅通过 stdin 传递签名验证材料，不把密钥写入报告或终端。
