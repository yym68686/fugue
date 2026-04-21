# Fugue CLI 验收标准

## 文档目的

这份文档定义改进后的 Fugue CLI 的验收标准。目标不是补几个零散命令，而是把 **常见部署、调查、定位、修复、回归验证** 做成一条完整的 CLI 闭环。

验收通过的定义是：

- 操作者只使用 `fugue` CLI 和现有授权凭据，就能完成标准调查与修复。
- 不需要额外使用 `ssh`、`kubectl`、浏览器 DevTools、原始 `curl`、数据库直连，才能回答关键问题。
- CLI 输出的结论必须能直接支撑决策，而不是只返回一堆需要人工二次拼接的中间对象。

## 总体验收原则

- 每一类高频故障都必须有一条一等公民的 CLI 调查路径，不能要求操作者回退到集群层工具。
- CLI 必须优先输出“根因分类 + 证据 + 下一步”，而不是只输出对象原始 JSON。
- 同一个问题的调查链，默认不应超过 3 条语义化命令。
- 文本输出必须适合人在终端里直接读；JSON 输出必须稳定、结构化、可脚本消费。
- 所有结论都必须带证据来源，至少包含相关 app、operation、route、runtime、release、dependency、source 中的一个或多个对象引用。
- CLI 必须明确区分以下故障层级：
  - 公网入口 / 反向代理 / 静态站点层
  - 应用路由层
  - 鉴权层
  - 应用配置层
  - 项目拓扑与依赖解析层
  - 构建 / 发布 / 部署 / rollout 层
  - 控制器调度与 operation claim 层

## 核心能力验收标准

### 1. 公网路由与入口诊断

- CLI 必须能对任意 app 的公网地址执行真实 HTTP 探测，并显示：
  - 请求方法、路径、query、header 注入方式
  - 最终状态码
  - 响应头摘要
  - 响应体摘要
  - 失败分类
- CLI 必须能并排对比：
  - 公网路由响应
  - app 内部 service 响应
  - 如果两者不同，CLI 必须自动指出差异归因。
- CLI 必须能判断某个路径失败属于哪一类：
  - 公网入口未路由到 app
  - 反向代理未转发该前缀
  - 静态站点 fallback 吃掉了动态路径
  - 应用本身返回了 4xx/5xx
  - 请求到达了应用，但鉴权失败
- CLI 必须能在不需要人工读 Nginx/Caddy/edge 配置的情况下，回答“这个路径是静态处理、代理转发，还是未配置”。
- 对于反向代理 + 静态站点混合应用，CLI 必须能列出当前已生效的代理路径前缀和静态路径策略。

### 2. Operation 生命周期与 claim 诊断

- CLI 必须能为任意 app 输出最近一次完整链路：
  - source decision
  - import
  - build
  - publish
  - deploy
  - rollout
  - live pod/image
- 对于 `pending` operation，CLI 必须明确说明属于以下哪一类：
  - 只是等待 controller claim
  - 被 leader election / controller 不健康阻塞
  - 被 app-local preflight 阻塞
  - 被项目依赖解析阻塞
  - 被运行时容量或 rollout 状态阻塞
  - 存在异常卡死，需要人工干预
- CLI 必须能把一个 import 后续排出的 deploy operation 自动串起来，不要求操作者手工查下游 operation id。
- CLI 必须支持 watch 模式，在 operation 状态变化时连续输出：
  - claim 时间
  - builder 开始时间
  - publish 完成时间
  - deploy 开始 / 完成时间
  - rollout 完成时间
- 如果一个 app 同时出现多个重复 import/deploy，CLI 必须能识别并说明：
  - 它们是否等价
  - 是否发生了 coalescing
  - 哪一个是当前生效链路

### 3. 项目拓扑与依赖解析

- CLI 必须能输出项目级拓扑视图，至少包含：
  - service 名
  - compose / topology 身份
  - depends_on 关系
  - backing service 关系
  - 当前映射到的 app
- 对于任意依赖失败，CLI 必须能直接回答：
  - 缺的是哪个 dependency 名
  - 解析失败发生在哪个阶段
  - 当前项目里是否存在同名 service identity
  - 是 app 不存在、identity 丢失、source 污染、还是 project-local alias 缺失
- CLI 必须能检测“app 还在，但拓扑身份丢了”这类 drift。
- CLI 必须能显示 app 当前是否仍携带：
  - compose_service
  - dependency graph
  - project-local service alias
- CLI 必须提供 repair path，让操作者能重新绑定或修复 service identity，而不需要删 app 重建。
- 在 topology deploy 之前，CLI 必须能预检当前项目是否存在 dependency identity 缺失，并在真正 deploy 前就给出阻断性提示。

### 4. Source ownership、build source 与 GitHub sync

- CLI 必须明确区分：
  - origin source / durable ownership
  - build source / last deployed build input
  - current release image
- CLI 必须能回答：
  - 这个 app 当前是谁的 source-of-truth
  - 最近一次 build 来自哪里
  - 最近一次本地 upload 是否只是 override
  - GitHub sync 是否仍然会继续追踪它
- 对于本地 upload override，CLI 必须清楚显示：
  - origin 是否保留 GitHub
  - build 是否切成 upload
  - 下次 GitHub sync 是否会覆盖当前 build
- CLI 必须能修复 source ownership，不要求用户删 app 重建。
- CLI 必须能解释 GitHub sync 是否触发 rebuild：
  - 触发了
  - 没触发
  - 被跳过
  - rebuild 成功但 deploy 失败
- 对于同一项目的多 service app，CLI 必须能列出当前 commit 对齐情况，直接看出谁落后。

### 5. 配置与 env 归因

- CLI 必须能显示 app 的有效 env，并区分：
  - app spec 显式配置
  - service attachment 注入
  - runtime 推导值
  - 缺失但必须存在的值
- CLI 必须能对“某 endpoint 依赖某个 env 才能工作”的情况直接给出诊断。
- CLI 必须能在发起请求前就告诉操作者：
  - 哪些 env 是该路径的前置条件
  - 当前是否已满足
  - 如果缺失，应该改哪个 key
- CLI 必须支持 live 更新 env 并跟踪其 deploy operation，直到 rollout 完成。
- CLI 必须能在 rollout 后再次验证同一条路径，确认修复是否生效。

### 6. 生成型公网 URL 与安装/引导脚本

- 对于会返回安装脚本、回调 URL、public base URL、WebSocket 路径、CLI connect 命令的接口，CLI 必须能直接验证这些派生值。
- CLI 必须能回答：
  - 公网 base URL 从哪里来
  - 当前实际会渲染成什么值
  - 是来自显式 env 还是请求上下文推导
- 如果生成值会因为缺失 public base 配置而失败，CLI 必须在失败前指出根因。
- CLI 必须能对这类接口做端到端探测，并校验返回 payload 中的 URL/command 是否与当前公网入口一致。

### 7. Agent / device / bootstrap 类接口调查

- CLI 必须支持对 agent bootstrap、device enrollment、runtime claim、install script 这类接口做真实请求验证。
- 对于同一功能链路，CLI 必须至少支持两类探测：
  - list/read 探测
  - issue/create 探测
- 如果 read 成功而 issue 失败，CLI 必须自动指出“这不是入口路径全坏，而是写路径或依赖配置有问题”。
- 如果 issue 路径返回 404/401/500，CLI 必须能直接区分：
  - 入口未路由
  - 鉴权未通过
  - 后端实现缺失
  - 后端实现存在，但依赖配置缺失

### 8. Rollout 与 live 状态核验

- CLI 必须能把当前 live pod/replicaset/image 与目标 deploy operation 自动关联起来。
- 对于“构建已完成，但线上还没切”的情况，CLI 必须能明确显示：
  - 当前运行镜像
  - 目标镜像
  - rollout 当前阶段
  - 是否存在旧 pod 正在 terminate / 新 pod 正在 create
- CLI 必须能在 deploy 完成后直接验证：
  - 新 pod 已 ready
  - live image 与目标 image 一致
  - 公网探测结果与修复预期一致

### 9. 文本输出与 JSON 输出

- 文本模式必须在首屏提供：
  - 当前结论
  - 根因分类
  - 关键证据
  - 下一步建议
- JSON 模式必须稳定，至少包含：
  - `summary`
  - `category`
  - `evidence`
  - `related_objects`
  - `next_actions`
- 任何语义化诊断命令都不能只输出一句泛化文本，例如：
  - `request failed`
  - `deploy in progress`
  - `no blocker detected`
  除非同时附带结构化证据。

## 场景化验收用例

以下场景全部通过，才视为 CLI 改进达标。

### AC-01: 公网子路径 404，但应用本身并未缺路由

- 给定一个公网 app，某个子路径返回 404。
- 操作者只用 CLI，必须能在不读入口配置文件的情况下判断：
  - 是公网入口未转发
  - 是静态站点 fallback
  - 还是应用自己返回 404
- 验收标准：不超过 2 条语义化命令得出结论。

### AC-02: 读接口正常，写接口 500

- 给定同一功能链路下的读接口返回 200，写接口返回 500。
- CLI 必须能指出问题不是入口路径整体损坏，而是写路径依赖缺失或后端逻辑报错。
- 验收标准：CLI 输出必须明确列出“读成功、写失败、写失败根因”。

### AC-03: import 成功，但 deploy 因 dependency 缺失失败

- 给定一个 topology app，import/build 成功，deploy 失败。
- CLI 必须自动指出：
  - 缺失的 dependency 名称
  - dependency identity 为什么缺失
  - 当前项目里哪个 app 本应提供这个 identity
- 验收标准：不需要 `kubectl`、不需要人工比对 service 名。

### AC-04: operation 长时间 pending

- 给定一个 pending import 或 deploy。
- CLI 必须告诉操作者它到底是：
  - 未 claim
  - 已 claim 但前置检查未过
  - controller 不健康
  - 还是正常排队
- 验收标准：CLI 结论必须带时间线证据。

### AC-05: GitHub sync rebuild 已经发生，但 app 仍停在旧 commit

- CLI 必须能说明：
  - rebuild 是否发生
  - build artifact 是否已发布
  - deploy 是否失败
  - 失败后 app 为什么仍停在旧版本
- 验收标准：CLI 不能让操作者手工拼 import op、deploy op、app source 三块信息。

### AC-06: 本地 upload 覆盖 build，但不应覆盖 GitHub ownership

- 给定一个 GitHub 管理的 app，执行一次本地 upload deploy。
- CLI 必须清楚显示：
  - origin source 仍是 GitHub
  - build source 已切到 upload
  - app 后续仍具备 GitHub sync 资格
- 验收标准：CLI 文本和 JSON 都必须能直接读出这三个结论。

### AC-07: 线上缺少 public base URL

- 给定某些接口需要生成公网命令、安装脚本或回调地址。
- 如果缺少显式 public base 配置，CLI 必须能：
  - 在请求前发现问题，或
  - 在失败后直接点名缺失 key
- 验收标准：CLI 不能只显示一个裸 `500`。

### AC-08: 修复后回归验证

- 对任意一次配置修复、env 修复、identity 修复、source repair。
- CLI 必须能在同一条工具链里完成：
  - 提交修复
  - 等待 deploy/rollout
  - 复测原始失败路径
  - 给出“已恢复”结论
- 验收标准：不需要再切回原始 `curl` 或浏览器手工验证。

## 最终 Definition of Done

当以下条件全部满足时，CLI 改进才算完成：

- 标准 app 事故调查不再依赖 `ssh`、`kubectl`、原始 `curl`。
- 所有高频问题都可以通过 Fugue CLI 的语义化命令直接归因到正确层级。
- CLI 能把 source、topology、operation、route、runtime、env 这五类对象自动串成一条证据链。
- CLI 同时支持“调查”和“修复后验证”，而不是只负责其中一半。
- 文本模式适合人直接读，JSON 模式适合自动化系统消费。
- 新用户不需要先理解控制平面内部实现细节，仍然能依靠 CLI 得到可执行结论。
