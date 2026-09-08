# Fugue CLI 能力、语义与删除计划

日期：2026-09-08。第 1–10 节保留调查时的背景；第 8 节勾选及第 11 节为后续实施状态。调查基线：`1553ad08` 加当前工作区已有改动。

本轮交付是调查与实施计划；未修改 CLI/API 实现、未删除命令、未访问生产。当前工作区存在其他任务的未提交改动，尤其是 Platform State/consumer 验证，因此下文描述的是本地代码能力，不代表已发布版本。命令清单由当前源码构建出的 CLI 递归执行 `--help` 获得；行为问题使用本地模拟 HTTP API 验证。

**主要结论：优先修复命令的行为契约，再收敛发布对象和兼容入口，最后补齐底层能力。** 当前已经是 Cobra 语义 CLI，并非四月时只有 deploy 的薄封装。应保留现有成熟功能和用户操作习惯，避免再次整体改名。

本次盘点得到 **384 个 OpenAPI HTTP operation、317 个路径、587 个可见帮助树节点，其中 447 个叶节点**。帮助树节点包含分组和 completion，不等于业务功能数；带子命令的节点也可能可执行。未把隐藏兼容路径和别名重复计入。API 与 CLI 是多对多关系，不以“每个 endpoint 都新增一个命令”衡量覆盖率。

证据附件：

- [命令清单](/Users/yanyuming/Downloads/GitHub/fugue/docs/cli-refactor-audit-2026-09-08/command-inventory.tsv)
- [OpenAPI operation 清单](/Users/yanyuming/Downloads/GitHub/fugue/docs/cli-refactor-audit-2026-09-08/api-inventory.tsv)
- [等待与错误行为复现](/Users/yanyuming/Downloads/GitHub/fugue/docs/cli-refactor-audit-2026-09-08/behavior.json)
- [弃用提示、脱敏、部分取证失败复现](/Users/yanyuming/Downloads/GitHub/fugue/docs/cli-refactor-audit-2026-09-08/supplemental-behavior.json)
- [基线与关键文件摘要](/Users/yanyuming/Downloads/GitHub/fugue/docs/cli-refactor-audit-2026-09-08/baseline.json)

## 1. 先承认已经做好的部分

| 能力域 | 已有产品入口 | 本轮判断 |
| --- | --- | --- |
| 部署与来源 | `deploy`、`deploy inspect`、`app create/build/deploy/source` | 已形成高层入口；需要澄清导入、构建、应用期望态、发布流量的边界 |
| 配置与文件 | `app env/config/fs/command/network/workload/storage` | 声明式配置与实时文件系统已分开；不能为了减命令把两者合并 |
| 发布与故障切换 | `app release`、`app rollback`、`app failover`、`app rollout timeline` | 功能多，但 image、release、attempt、traffic、policy 混在同一组中 |
| 诊断与证据 | `app diagnose/request/requests/traces/metrics`、`logs query/collect`、`debug bundle`、`operation explain/evidence/timeline` | 多数四月验收能力已经存在；重点是统一入口指引、证据完备性和结果语义 |
| 项目拓扑 | `project routes/runtimes/split/move/verify` | 已覆盖多资源操作；保留整项目工作流与单 app 操作的区别 |
| 服务与数据库 | `service postgres`、`app service`、`app db`、`service suspend/resume` | 保留“服务实例、绑定关系、应用数据库”三个对象，不可强行并为 db |
| 备份与数据 | `backup`、`app backup/restore`、`data` | 高层和底层入口均有；缺少少数生命周期闭环，不能认定整个数据 CLI 落后 |
| 平台运行与恢复 | `admin artifact/consumer/gate/invariant/robustness/edge/dns/node-updater` | 平台控制能力已大量产品化；目前更像一组专家工具，缺聚合状态和可恢复工作流 |
| 自动化基础 | `--json`、脱敏、退出码、completion、命令帮助、版本检查与升级 | 基础设施已存在，但实现并未在所有命令上遵守同一契约 |

## 2. 已确认的优先问题

### F01：`operation wait` 的成功语义受终端与输出格式影响，P0

`wait` 是 `operation watch` 的别名。非交互文本路径会直接渲染一次快照并返回；JSON 路径才调用等待循环。

本地实测：

| 模拟状态 / 调用 | 实际结果 | 应有语义 |
| --- | --- | --- |
| pending + `operation wait`，stdout 为管道 | 一次 GET，立即退出 0 | 等到终态或明确超时，pending 不能当作等待成功 |
| failed + `operation wait`，stdout 为管道 | 打印 failed，退出 0 | 返回失败退出码 |
| failed + `operation wait --json` | 退出 6，stdout 为空 | 输出可解析的终态与证据，返回系统故障类别 |

将 `wait` 从别名拆为独立业务命令：`show` 是快照，`watch` 是持续观察，`wait` 是终态断言。TTY 只能改变展示方式，不能改变等待条件、退出码与动作结果。`--once` 只属于快照/监视行为。

证据：[ops.go:323](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/ops.go:323)、[deploy_commands.go:1234](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/deploy_commands.go:1234)。

### F02：错误分类已有定义，但仍依赖英文报错字符串，P0

当前已有 0/2/3/4/5/6 六类退出码，不应重新发明一套。问题在于 `apiServerError` 保存了 HTTP status/code/category，最终分类却大多回到 `err.Error()` 文本匹配；`Error()` 又可能只返回服务端 message。

本地模拟 HTTP 403、404，body 为通用错误文案时均退出 6；未知参数 `--typo` 也退出 6。`--json` 错误路径常只在 stderr 输出普通文本。

改为：先解析类型化 HTTP 状态及服务端 code/category，再处理参数解析错误、取消/超时和传输错误，最后才兼容旧字符串。保留既有成功 JSON；为错误增加版本化结构，至少包含 code、category、message、retryable、request_id、operation_id、outcome、next_commands。请求结果未知时明确 `outcome=unknown`，不能误判为“未执行”。

证据：[exit_codes.go:58](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/exit_codes.go:58)、[client.go:435](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/client.go:435)、[main.go:10](/Users/yanyuming/Downloads/GitHub/fugue/cmd/fugue/main.go:10)。

### F03：部分取证失败在 JSON 中被静默丢弃，P0

`loadAppOverview` 对 domains、bindings、operations、images、pods 等失败只调用 `progressf`；后者在 JSON 模式直接静默。模拟这些子接口均返回 403，`app overview --json` 仍退出 0，仅返回 app，没有来源状态或 warnings。调用方无法区别“确实没有域名”和“没权限读取域名”。

需要每个证据源带 `state=available/empty/permission_denied/unavailable/stale`、observed_at、error_code；聚合结果带 completeness 和 missing_evidence。保留已获得事实；普通 overview 可返回部分结果，`--require-complete` 或断言命令才因取证不足返回 6，不能让可选数据源故障破坏所有日常查询。

证据：[app_overview.go:126](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/app_overview.go:126)、[root.go:399](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/root.go:399)。

### F04：非秘密查看命令存在脱敏遗漏，P0

`app release policy show --json` 返回完整 app，没有调用统一脱敏。使用合成 env 值的本地测试可见该值原样输出；同一 fixture 的 overview 能正常脱敏。

应统一所有状态、策略、证据与操作结果的输出边界。明确保留 `app env ls/export` 主动查看 env 的既有原文行为；这和诊断/策略命令意外带出 env 是不同场景。不要用“全局强制脱敏”破坏可复制的 `.env` 导出。

证据：[app_release.go:1191](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/app_release.go:1191)、[输出兼容规则](/Users/yanyuming/Downloads/GitHub/fugue/docs/cli-output-compatibility.md)。

### F05：release 命名同时指向三个对象，P1

- `app release ls` 实际调用 `/apps/{id}/images`，列镜像库存。
- `app release status` 实际查最新 release attempt。
- canary/probe/promote/abort 操作真正的 AppRelease。
- `app release policy` 只管理镜像保留数，零停机配置却在 `app failover policy set --zero-downtime ...`。
- `app release traffic` 是修改命令；没有等价清晰的独立 traffic show 子命令。底层已有 GET traffic，failover 审计可间接看到部分流量信息。

需要分清 image（可复用产物）、release（一次可服务部署）、attempt（执行过程）、traffic（独立流量意图）。旧 JSON 不可借重构名称之机静默换成另一种对象。

证据：[app_release.go:73](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/app_release.go:73)、[app_release.go:877](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/app_release.go:877)、[client_app_releases.go:85](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/client_app_releases.go:85)。

### F06：隐藏不等于完成弃用，P1

`hideCompatCommand` 只为父命令设置 Deprecated，对子命令只递归设置 Hidden。本地 `env ls audit --json` 成功执行，stderr 没有弃用提示。现有兼容树可能长期被脚本使用而没有迁移信号。

应为实际执行叶节点或统一入口提供准确的 replacement、参数映射和移除版本；提示只写 stderr。不要把整个旧树笼统指向一个语义不同的新分组。

证据：[compat.go:5](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/compat.go:5)。

### F07：帮助信息有自动补文案，但不保证例子可执行，P1

当前帮助由构造器、覆盖表、默认例子生成三处组成。实测 `backup --help` 给出 `fugue backup artifact delete` 和 `... show`，缺必需参数；`app config --help` 仍指引使用已隐藏的 workspace 名称。

构建“命令语义目录”，从声明的参数、flag、约束、示例生成文档和机器目录，测试例子能通过参数解析。避免仅断言 Long/Example 非空。保留 Cobra；无需为此更换框架。

证据：[help_docs.go:1135](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/help_docs.go:1135)、[files.go:32](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/files.go:32)。

### F08：部分被隐藏的旧命令比标注的新入口更强，P1

旧 `app route check/set` 支持 `--path-prefix`；`app route show` 调用 `loadAppRouteShowResult`，核验实际路由并根据 in_sync/inconclusive/异常返回不同退出码。被标注为替代的 `app domain primary check/set` 没有 path-prefix 参数，show 只返回 app.Route。

因此当前不能删除 app route。先将路径前缀、真实路由观测和结论分类迁入规范入口，保留稳定的原有轻量读取方式（例如新 `verify` 或显式观测选项），再迁移旧脚本。隐藏标记是历史设计意图，不是功能等价的证据。

证据：[app_route.go:25](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/app_route.go:25)、[app_route.go:111](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/app_route.go:111)、[domain.go:78](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/domain.go:78)。

## 3. 底层能力缺口与产品化方向

“已有 API”“已有部分命令”“仍需服务端实现”分别处理。下列命令形态为提案，未实现。

| 编号 / 优先级 | 能力与当前证据 | 建议入口 | 依赖与验收重点 |
| --- | --- | --- | --- |
| C01 / P1 | 全局镜像实体、replicas、verify、pin 创建/删除、replication tasks 已有 API；CLI 只有 app 镜像库存、image-cache 清理和 retention | `image ls/show/replicas/pin/unpin/replicate`、`image transfer ls/watch`；平台扩展使用作用域权限 | 优先接入只读 inventory。保留租户隔离；pin/复制意图与节点报告事实分开。Store 有 ListImagePins，但公开契约还需补 GET pins 才能完整管理现有 pin。名称按 digest/tag 解析，显示实际目标和故障域 |
| C02 / P1 | `/images/{id}/verify` 当前只查 status=present 的库存，不能证明此刻真实可拉取 | `image verify` 明示 inventory 检查；未来 `--probe` 才进行实际校验 | 服务端先补 freshness/TTL、真实 digest/readability 校验；不得把库存计数包装为实时健康证明 |
| C03 / P1 | `data workspace` 缺 delete；`data snapshot` 缺 delete；`data grant` 缺短期授权 ls；三个 API 均存在 | `data workspace delete`、`data snapshot delete`、`data grant ls` | 删除前展示引用、活跃 transfer、保留策略与回收方式；先确认 API 的软删除语义，不把逻辑删除宣称为已回收存储 |
| C04 / P2 | data prewarm API 创建 planned transfer；源码搜索仅见模型常量和建单，未找到实际消费执行器 | `data prewarm <workspace> --version ... --runtime ...` | 这是端到端能力未完成，不能只加 CLI。先实现调度/执行/进度/失败/取消，再开放命令 |
| C05 / P1 | 真正 AppRelease 和 traffic API 已有；命令列表/状态却以 image/attempt 为中心 | `app release versions/show`、`app traffic show/set`、`app release attempt ...` | 列出 stable/candidate/previous、镜像 digest、spec revision、实际流量、健康证据；完整展示而非要求用户拼 API |
| C06 / P1 | `app config verify/reconcile` 已比较单文件并能重启、等 revision/endpoints、校验；尚无独立完整 app drift 工作流 | `app drift show/check`、`app reconcile --plan/--apply` | 复用文件验证，同时覆盖 env、command、mount、replica、digest、route。缺少的 live facts 先补 API；无权限/无证据时 unknown，不猜测 |
| C07 / P1 | `admin artifact` 已 create/validate/release/rollback/verify-lkg，但需手写 JSON、fencing、多个 evidence 参数 | `admin artifact inspect/plan/wait`、按 scope 的 `admin state show/explain`；保留现有专家命令 | 自动读取可信证据、显示意图/策略/产物/consumer facts 五层关联；验证仍由服务端决定。人工 flag 不等于可信心跳，不自动填 true |
| C08 / P1 | 本地源码导入走内存 multipart；GitHub 导入有 idempotency，source-upload 只有元信息/下载 | `deploy ... --request-id`、`source-upload resume/status`、`operation recover --request-id` | 请求收据、分片/续传和请求→operation 查询需新 API。先改善 unknown outcome 与可恢复信息；不把客户端重发当幂等 |
| C09 / P2 | auth 可按 base URL 保存凭证；tenant/project 主要由 flag/env/自动选择决定，缺完整命名 context | `context ls/show/use`、`auth status` 扩展能力信息 | context 只记录目标与凭证引用，不存服务配置。输出每个有效值来源；冲突需显式处理，写操作不得模糊猜目标 |
| C10 / P2 | `find` 搜索资源，不是命令能力目录；部分示例由字符串模板补齐 | `help --json`、`help search`、`capabilities` | 分离本地命令目录、服务器协议支持、当前 principal 权限、功能开关；API 缺字段时通过 OpenAPI 扩展，不能根据版本猜能力 |
| C11 / P2 | 有单独 request/trace/operation/incident 解释与证据包，用户需知道对象类型和入口 | `diagnose request <id>`、`diagnose operation <id>` 等显式路由，复用现有处理器 | 先做指引和聚合，不新造另一套诊断算法；自动识别只有在服务端关系明确时启用 |
| C12 / P2 | `app sync resume` 仍只在隐藏树，`source show` 或 `app build` 不表达恢复自动同步 | `app source sync status/run/resume` | 映射旧恢复行为；若增加 pause，需定义持久同步策略和权限，不能假设后端已支持 |
| C13 / P1 | 旧 app route 的 path-prefix 和实际路由观测未被 domain primary 完整覆盖 | `app domain primary check/set --path-prefix`、`app domain primary verify` | 复用 loadAppRouteShowResult 与已有 API；保留当前 show 快照响应，先新增验证入口，再删旧 route |

直接证据：[images.go:12](/Users/yanyuming/Downloads/GitHub/fugue/internal/api/images.go:12)、[images.go:70](/Users/yanyuming/Downloads/GitHub/fugue/internal/api/images.go:70)、[data_workspace.go:460](/Users/yanyuming/Downloads/GitHub/fugue/internal/api/data_workspace.go:460)、[data_workspace.go:736](/Users/yanyuming/Downloads/GitHub/fugue/internal/api/data_workspace.go:736)、[data.go:1673](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/data.go:1673)、[admin_artifact.go:39](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/admin_artifact.go:39)、[client.go:666](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/client.go:666)、[app_sync.go:174](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/app_sync.go:174)。

**不应面向用户逐个暴露的底层 API：** agent/node-updater 心跳、任务 claim/complete、consumer trusted-heartbeat、DNS/edge bundle 消费和 TLS 自动签发回调。它们属于工作协议。CLI 可查看其事实或触发受控检查，不能通过“友好命令”代替节点伪造 ACK、serving 状态或已加载 digest。

`operation cancel` 也不能仅由 CLI 发明。目前没有通用 operation cancel 契约；data transfer cancel 和 release abort 只适用于各自状态机。应先定义可取消阶段、副作用和补偿，再考虑统一入口。

## 4. 目标语义与命令边界

### 4.1 高频操作保持稳定

保留 `fugue deploy .`、`app status`、`app logs`、`app env`、`app config`、`app fs`、`app restart`、`app db`、`backup`、`data`。不要让用户为一次部署先认识所有平台内部对象。

| 用户问题 | 目标命令职责 |
| --- | --- |
| 把本地/GitHub/镜像交给平台 | `deploy`；来源导入和高层编排 |
| 从已保存来源构建 | `app build`；help 必须明确当前是否也部署，不能直接改变旧副作用 |
| 应用当前期望配置 | `app deploy`；不隐式更换持久 source ownership |
| 现在服务的是什么 | `app status`；少量核心 serving facts |
| 相关对象和证据是什么 | `app overview`；聚合视图，允许部分结果 |
| 为什么异常 | `app diagnose`；带证据的结论 |
| 必須等待什么条件 | `operation wait` 或资源 `wait --for=...`；有 timeout 与明确终态 |
| 哪里与期望不一致 | `app drift`；只读比较和断言 |
| 怎样将期望应用到运行态 | `app reconcile`；展示范围、执行、验证，不改代码产物 |

`app build` 当前调用 rebuild API；如果要增加真正 build-only，必须先定义服务端输出为 immutable image，并明确 `--deploy` 兼容模式。不能为了名字直观，偷偷把原来的 build+deploy 拆断。

### 4.2 发布模型收敛

提议的稳定职责：

```text
app image          应用镜像库存、保留、来源跟踪
image              全局可授权镜像、replica、pin、复制任务
app release        可服务版本、探测、门禁、晋级、中止、发布尝试
app traffic        stable/candidate 引用、权重、粘性和实际观测
app rollout        发布策略与部署时间线
app failover       跨 runtime 故障切换策略与操作
```

同一 app 的配置修正、镜像切换、流量变更分别展示 diff 与影响。`app rollback` 当前是选择历史镜像重部署，必须继续明确显示“只回退镜像”；不能声称恢复了 env、数据库或完整 release。新增按 release ID 的回退须先定义 spec/config/traffic 范围与数据库兼容性，默认保持独立配置恢复能力。

### 4.3 读取、规划、执行、验证分离

读取使用 `show/ls/status`，诊断使用 `diagnose/explain`，只读计划使用 `plan`，改变配置使用 `set/apply`，主动效果验证使用 `verify/check`。不机械统一全部动词，保留数据库 query、文件 get、日志 follow 等自然用法。

`plan` 必须说明 local inspection、server validation、dry-run 与会写入记录的 preflight 区别。对分步骤发布，不承诺跨 API 自动原子性：每步保存 request/operation/release ID，失败后可以查询已完成副作用。高风险多步骤 apply 使用版本前置条件/ETag 或等价 fencing，拒绝基于过期计划静默应用。

### 4.4 自动化契约

- 保留旧成功 JSON；新增版本化结果时采用显式版本选择，避免现有 jq/CI 顶层字段变化。
- `--json` 默认单个结果对象；持续事件使用明确的 NDJSON/event 模式。流输出与最终结果分别文档化。
- stdout 只放数据；stderr 放进度、弃用提示和人类诊断。失败也能留下机器可读结果。
- `wait` 的 timeout 不代表服务端取消；退出时提供追踪命令和最后已确认阶段。
- 成功/失败/unknown/partial 的词汇和退出码在 text、JSON、非 TTY 中一致。
- 统一 `--wait` 的默认和完成定义之前先记录现状。备份、部署、资源推荐应用目前默认不同，迁移不能偷偷改变阻塞行为。
- `--background` 当前表示无公网入口的后台工作负载，不是异步退出；help 要明确。异步提交应使用清楚的 `--wait=false`。
- 读路径可以安全重试；写路径必须有 idempotency 或明确的可重试证明。

## 5. 命令合并与拆分计划

| 调整 | 原因 | 实施方法 |
| --- | --- | --- |
| 拆分 `operation wait` 与 watch | F01 的可复现正确性问题 | 独立等待器，共享数据读取与渲染；不修改 show 的快照职责 |
| 将 image inventory/retention 从 release 概念中抽出 | image 与部署实例不同 | 先增加 `app image` 入口；旧 release ls/policy/prune 保留原响应直至迁移 |
| 拆分 `app release traffic` 为 show/set | 读写意图不清晰，读接口已有 | 新增只读 show；旧 traffic 参数按原意路由到 set |
| 将零停机策略从 failover 文案中分出 | 发布稳定性与跨节点故障切换不同 | 新增 `app rollout policy show/set`；旧 flag 转发但保留 JSON 兼容 |
| 合并 top-level logs 与 app logs 的实现层 | 查询来源和过滤器不统一 | 优先统一 source/window/request/filter 类型；业务数据库日志仍使用专用 table 适配 |
| 统一证据包导出实现 | app、operation、release-attempt 格式和入口分散 | 共享 manifest、redaction、source status；保留 scoped 快捷命令，未来可增 `debug bundle --operation/--attempt` |
| 平台多个 explain 共用事实模型 | gate/invariant/robustness 等结论可能各自组织 | 增聚合入口，复用 typed server evidence；原命令保持精确查询能力 |
| 名称解析集中处理 | app 已服务端搜索，service/project/key 仍有部分全量拉取/本地筛选 | 复用有 scope 的搜索/resolve，新增必要服务端过滤；不能把 fuzzy 搜索直接用于写操作 |
| 帮助和弃用信息单一声明 | 现有多处文案漂移 | 命令目录记录对象、输入、读取/写入、副作用、权限、等待、输出版本、替代关系；help/补全/文档派生 |

## 6. 过时命令删除计划

### 6.1 删除必须有终点

删除的是冗余入口和重复适配代码；共享的能力实现保留。仓库当前已经隐藏了许多旧命令，本轮应结束长期兼容堆积。

使用相对发布里程碑，避免虚构下一版版本号：

- **R1：替代与迁移发布。** 补齐能力，叶节点可靠发出弃用提示，更新仓库脚本、README、帮助、安装示例、fugue-web 部署指引。建立可本地运行的命令迁移扫描，不上传用户命令或凭证。
- **R2：首批删除发布。** 对已证明等价且已迁移的路径删除命令注册与专用构造器。至少经过一个明确标注的迁移版本；已隐藏很久但没有有效提示，不算完成迁移。发布说明明确 breaking changes 与固定旧 CLI 的方式。
- **R3：语义迁移删除发布。** 对 JSON 对象或副作用有变化的入口，在新契约验收后删除旧入口/旧 mode。没有达标的个别项明确列为未完成，不阻塞其余条目删除，也不宣称整个兼容层已清理。

R2 之后被删除路径必须报清晰错误且不发业务 HTTP 请求；可保留小型移除提示表，不能保留第二套执行实现。最终再删提示表前，也应有明确版本，而非无限期存留兼容命令。

### 6.2 第一批：已有明确替代，可进入等价验证和迁移

以下替代路径当前已存在；“可删除”指验证和迁移后删除，不代表本次已执行。每项需对参数、默认值、权限、HTTP 请求、退出码、JSON、脱敏、等待语义做对照。

| 删除对象 | 已有上位/规范替代 | 能力证明与特殊处理 |
| --- | --- | --- |
| 顶层 `env`，含其兼容名称 | `app env` | 复用 `newEnvCommand`；保留原文 env/export 行为 |
| 顶层 `files` | `app config` | 同一构造器；仅改资源归属。不要删共享 files.go |
| 顶层 `domain` | `app domain` | 同一构造器；不影响独立 dns zone/record |
| 顶层 `workspace`、`app workspace` | `app fs` | `newWorkspaceCommand` 包装 newFilesystemCommand；source/component/pod 参数全保留 |
| `curl` | `api request` | 同一 raw request 构造器；保留 method、headers、body、timing 与脱敏 |
| `template inspect`、其 github 子命令 | `deploy inspect`、`deploy inspect github` | 共用 inspect helper；保留 private/repo-token/branch |
| 旧 `deploy plan`、`deploy plan github` | `deploy inspect`、`deploy inspect github` | 共用 helper，text 的 mode 值不同；先迁移解析 mode 的脚本。未来真正 plan 不复用旧伪计划语义 |
| `app binding` / `bindings` | `app service` | 同一 service connection 构造器；绑定资源 ID 仍需支持 |
| `app redeploy`，含旧 `app apply` 别名 | `app deploy` | 同一 deploy 构造器；不能变成“从源码构建” |
| `app rebuild` | `app build` | 同一 rebuild 构造器；保留 clear-files、branch、image 等参数 |
| `app release deploy` | `app deploy` | 同一执行实现；同时迁移帮助中历史示例 |
| `app release rebuild` | `app build` | 同一执行实现；不能删除共享 rebuild helper |
| `app release rollback` | `app rollback` | 同一镜像回退实现；保持 image-ref 和选默认镜像行为 |
| `project show/get/status/info` 兼容入口 | `project overview` | newProjectShowCommand 基于 newProjectOverviewCommand；保留可选 project 参数 |
| `project rename <project> <name>` | `project edit <project> <name>` | 后者还支持 description/default-runtime，是真正上位替代 |
| `project storage`、`project usage` | `project images usage` | 旧命令只统计镜像存储，替代名称更准确；不误导为 PVC/data/backup 总量 |
| `runtime attach` | `runtime enroll create` | enroll create 包装 attach 实现；保留 token TTL 和加入方式 |
| `runtime access ...` | `admin runtime access ...` | 迁移所有 show/set/grant/revoke；显式补全旧 `runtime access <name>` 简写 |
| `runtime pool ...` | `admin runtime pool ...` | 同样 pool show/set 能力；核对默认 mode |
| `runtime offer ...` | `admin runtime offer ...` | 同样 public offer 语义；权限由服务端判断，不因 admin 前缀扩大权限 |
| `runtime delete` | `admin runtime delete` | 保留 backend 的删除前置条件与结果 |
| `admin runtime share`、`unshare` | `admin runtime access grant`、`revoke` | 参数顺序 runtime/tenant 显式映射；验证返回结构 |
| `admin runtime share-mode` | `admin runtime access set` | mode 枚举/默认值一致后删 |
| `admin runtime pool-mode` | `admin runtime pool set` | 同上 |
| `service create --type postgres` | `service postgres create` | 同一 createPostgresService；去掉只支持 postgres 的 type 伪扩展；未知 type 仍应拒绝 |
| `app failover configure/disable` | `app failover policy set/clear` | 同一 continuity set/off helper；保留 app/db/zero-downtime/rebalance-now 参数 |
| artifact release/rollback 的 `--force-publish` | `--soft-override` | 当前已 MarkDeprecated；删除只影响 CLI flag，API 兼容字段单独评估。绝不把它映射成 kernel break-glass |

关键代码：[root.go:262](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/root.go:262)、[app.go:73](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/app.go:73)、[project.go:447](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/project.go:447)、[runtime.go:18](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/runtime.go:18)、[service.go:106](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/service.go:106)、[app_failover.go:390](/Users/yanyuming/Downloads/GitHub/fugue/internal/cli/app_failover.go:390)。

### 6.3 第二批：先补强上位替代，再删除旧语义

| 旧能力/入口 | 目标替代 | 删除前必须解决 |
| --- | --- | --- |
| `app route show/check/set` | 补强后的 `app domain primary verify/check/set` | 当前替代尚不等价：缺 path-prefix，show 缺 live route 观测及结论退出码；F08/C13 完成后才删除 |
| `app sync status/run/resume` | `app source sync status/run/resume`，run 可指向既有 build 工作流 | resume 不等于 show/build；保留自动同步恢复能力和 source_sync 字段 |
| `app continuity audit/show/enable/disable` | `app failover status/policy` 加 `app rollout policy` | set/off 已复用，但 show 的配置结构与 audit 的评估结构不同；零停机策略要有清晰独立入口与 JSON 迁移 |
| `app release ls` 的镜像列表语义 | `app image ls`；真正 release 列表先用新 `app release versions` | 禁止在同一个旧命令下悄悄替换 JSON schema；完成显式新版本切换后才考虑 reclaim `release ls` 名称 |
| `app release policy` 的镜像保留语义 | `app image retention show/set` | 原来的 retain 默认值、未设置语义、返回字段迁移后才能让 rollout policy 使用 policy 概念 |
| `app release prune` | `app image prune` | 保留 dry-run/筛选、不可删除镜像原因、实际回收解释；不能只换名字删保护 |
| `app release tracking` | `app image tracking` | set/disable/sync/history/diagnose 全覆盖；与 GitHub source sync 不强合并 |
| `app release status/attempts/explain/debug-bundle` 中 attempt 专属部分 | `app release attempt status/ls/explain/bundle` | 状态与版本列表分开；旧输出格式、latest 选择和权限兼容 |
| 旧 `app release traffic --stable/--candidate` | `app traffic set`，新增 show | 读写分离；权重、粘性、stable/candidate ID 必须有完整映射 |
| `operation wait` 的 watch 别名关系 | 独立 `operation wait` 实现 | 删除的是错误别名关系，保留并修复 wait 命令本身；单独发布语义修正说明 |
| 多处旧日志/证据聚合实现 | 共享日志与 evidence service | 来源、时间窗、字段过滤、raw chunk、档案内容全部对照后才删重复实现；先不删对用户有价值的 scoped 快捷入口 |

### 6.4 不能因“有更大命令”就删除

| 保留项 | 理由 |
| --- | --- |
| `app status` 与 overview/diagnose | status 应便宜且依赖少；overview 聚合更多接口，不能作为强制替代 |
| `app config` 与 `app fs` | 一个声明意图，一个操作实际文件；合并会损害配置/运行事实边界 |
| `app logs runtime/query/table` | 分别是即时日志、平台保留日志、业务数据库表，来源和权限不同 |
| `app request` 与 `app requests` | 主动探测和已有请求记录查询不同，可改帮助分组，不能删除能力 |
| `app backup/restore` 与 `backup` | 前者自动选择应用范围，后者管理 backend/policy/artifact；共享实现而非强迫用户使用长底层命令 |
| `data workspace` 与 app fs | 数据集快照/传输/授权和应用文件系统不是同一个 workspace |
| `api request`、受控 cluster diagnostics | 高层命令尚未覆盖全部能力，仍是必要扩展入口；不以清理低层接口为目标 |
| `ls/list`、`rm/delete` 等低成本习惯别名 | 仅一个 handler 的惯用拼写不会造成第二套语义，收益小于破坏脚本的成本 |
| 显式 ID 与 `--show-ids` | 精确定位和自动化需要；不能为了“用户只看名称”移除逃生口 |
| 各类精确 admin explain | 聚合诊断不能替代范围更小、权限更少、成本更低的专家查询 |

## 7. 架构与实现边界

沿用现有 `internal/cli/viewmodel`，增量拆分命令 handler 中混杂的解析、HTTP、聚合、等待、渲染，不做全仓大重写。建议边界：

1. 命令目录：用户语义、参数、权限、输出版本、deprecated/replacement 元信息。
2. 作用域与解析：base URL、principal、tenant/project、名称/ID、值来源。
3. 用例层：deploy、wait、diagnose、artifact rollout、backup restore；描述工作流和阶段结果。
4. 类型化 API client：严格消费 OpenAPI，不把所有 HTTP operation 自动变成公开命令。
5. 证据/视图层：partial、freshness、source、warnings、desired/live，复用现有 viewmodel。
6. renderer：text、JSON、事件流/TUI；不决定业务是否成功。

平台配置继续遵守五层分离：configuration intent、constraint policy、signed immutable artifact、runtime facts、code execution。CLI 只提交意图、查询事实和调用授权动作。配置恢复不得依赖重新编译/重新部署代码；代码失败不能让 CLI 自动废弃正在服务的 artifact；失败配置 rollout 保持之前 positive verified LKG。

`admin artifact wait` 可以帮用户等待 consumer convergence，但不能把“已提交”显示成“已验证”；服务端 trusted evidence 和既有 Safety Kernel 仍决定晋级。恢复入口要有版本兼容的只读识别与指定 API endpoint 能力，避免只支持最新代码的客户端阻断恢复。

## 8. 分阶段原子 Todo

任务量是规划估计，不是工时承诺。先完成 P0 和 R1 迁移基础，即可交付第一轮明显收益；高级能力按依赖分批推进。

### A. 契约基线与自动化正确性，P0，约 5–7 工程日

- [x] A01 固定当前命令/flag/别名/JSON/副作用清单，新增语义目录与兼容基线。
- [x] A02 独立 `operation wait`，text/JSON/TTY/pipe 共用同一等待器。
- [x] A03 为 wait 增 timeout、取消、last-known operation 和恢复命令；区分超时与服务端失败。
- [x] A04 类型化退出码映射；覆盖 400/401/403/404/409/429/5xx、参数错误、网络错误。
- [x] A05 失败 JSON 和 unknown outcome 结构化；保留原成功 JSON。
- [x] A06 overview 的 source status、partial 与 missing evidence；提供完整性断言。
- [x] A07 所有非秘密查看命令统一脱敏，保留 env export 原文契约。
- [x] A08 运行本地行为回归和现有 CLI 相关测试，通过后独立发布。

### B. 帮助与首批删除准备，P1，约 4–6 工程日

- [x] B01 为第 6.2 节逐项生成旧→新参数映射与 parity case。
- [x] B02 弃用提示覆盖实际执行叶节点，不能污染 stdout/JSON。
- [x] B03 修正 app config 中旧 workspace 指引，修正 backup 等缺参示例。
- [x] B04 本地目录/补全/Markdown 文档从同一命令定义派生，建立可解析示例检查。
- [x] B05 搜索并迁移仓库 scripts、workflow、README、runbook 和 fugue-web 中旧命令。
- [x] B06 迁移扫描仅报告命令片段与位置，遇到 shell 动态构造给人工检查标记；不盲目替换字符串。
- [x] B07 发布 R1，列出 R2 将删除的确切路径和版本。
- [x] B08 到达 R2 条件后逐组删旧注册、专属构造器和兼容测试，保留必要共享实现与移除提示。

### C. 发布与配置语义收敛，P1，约 6–10 工程日

- [x] C01 新增 app image 镜像库存、保留、tracking 规范入口，复用现有实现。
- [x] C02 新增真正 AppRelease 列表/详情，区分 releases 与 release attempts。
- [x] C03 新增 app traffic show/set，展示 intent 与已观测流量分别是什么。
- [x] C04 将零停机发布策略归到 rollout policy，failover 保留跨 runtime 职责。
- [x] C05 文档化 build/deploy/reconcile/rollback 的具体副作用；新增模式不得静默改变旧行为。
- [x] C06 建立多步发布收据，canary create 成功但 traffic 更新失败时保留已创建 release ID。
- [x] C07 为 drift 汇总补齐 live facts，复用单文件 verify，提供范围明确的 reconcile plan。
- [x] C08 为 source sync resume 提供可见入口，补齐第 6.3 节删除前置能力。
- [x] C09 为 domain primary 补 path-prefix 和实际路由 verify，保留旧路由观测能力。
- [x] C10 使用新输出版本完成 image/release/attempt 迁移，再执行第二批删除。

### D. 已有 API 的缺口补齐，P1，约 5–8 工程日

- [x] D01 全局 image ls/show/replicas，服务端作用域和名称解析。
- [x] D02 image pin/unpin 与 replicate；明确意图和异步任务状态，支持恢复追踪。
- [x] D03 image verify 先明确 inventory 语义；真实 probe 需要后端工作项，不能虚标完成。
- [x] D04 data workspace/snapshot delete，短期 grant ls，引用保护和逻辑删除说明。
- [x] D05 artifact scope 总览、计划和等待，复用现有 signed artifact/LKG/consumer API。
- [x] D06 针对上述命令的 tenant scope、重复请求、partial 状态做契约验收。

### E. 服务端能力与体验改造，P2，约 2–4 周，按收益独立拆分

- [x] E01 source upload 建收据、分块/续传、内容摘要和幂等提交契约。
- [x] E02 请求 ID 可反查 operation，连接丢失后先核对副作用再重试。
- [x] E03 data prewarm 实现 runtime executor、真实进度、取消及失败回收，再开放 CLI。
- [x] E04 命名 context 与能力协商；不把配置意图存进 CLI context。
- [x] E05 资源解析和列表下推过滤，控制 overview 的串行 fan-out；设明确超时和部分结果预算。
- [x] E06 request/operation/trace/incident 诊断共享 evidence 模型和关联查询。
- [x] E07 通用 cancel 如有实际需求，单独设计状态机与补偿后再立项，不能占用已完成能力的名义。

每个 API 修改从 `openapi/openapi.yaml` 开始，重新生成、运行 `make test`。涉及 fugue-web 的响应契约，同步 OpenAPI snapshot、生成 TS 并运行 contract check。纯 CLI 改动运行相关 CLI/契约/输出测试；不因文档或帮助修改无条件跑整个生产发布链。

## 9. 验收场景与删除完成定义

| 场景 | 完成标准 |
| --- | --- |
| pending/failed operation，终端和管道、text/JSON 四种组合 | wait 从不因渲染模式提前成功；失败终态返回一致非零类别，JSON 保留对象 |
| 403/404 通用文案、unknown flag、连接中断 | 分类与文案无关；请求未发送/服务端拒绝/结果未知清晰区分 |
| overview 的 pods 或 domains 无权限 | source state 明确，已有事实保留，无权限不当作空数据 |
| 配置漂移 | 支持同一 app 多个 ready pods，区分 desired/live，未观察到的字段标 unknown |
| canary 第二步失败 | 报已创建的 release 与未完成步骤，不输出笼统 deploy failed 后让用户重建一遍 |
| image replica 过期 | inventory 状态与现场可拉取验证分开，未探测不得显示 verified |
| 失败代码版本下配置回退 | 当前签名 artifact 可读取验证，旧 positive LKG 可独立恢复，不需代码部署 |
| data workspace/snapshot 删除 | 引用保护、活跃任务处理、软删除/存储回收明确，越权拒绝 |
| 被删除旧命令 | R1 有准确替代指引，R2/R3 移除后不发业务请求；用户不会落入另一条同名新语义 |
| 新旧等价命令 | 参数、defaults、权限、HTTP payload、等待、JSON、redaction 对照；仅命令名和弃用提示可不同 |
| 帮助与补全 | 高优先级工作流在两次 help 内可发现，示例通过解析且给出必需参数，机器目录含替代关系 |

删除验收逐项勾选，不能以命令数量下降代替功能保留。最终报告应列出：实际删除路径、保留的习惯别名、尚未覆盖的旧能力、仍在迁移的输出版本、已更新的脚本与文档。

## 10. 方法与边界

本报告做了全量命令帮助树和 OpenAPI operation 索引，并对高价值缺口、发布/配置/诊断核心路径和兼容构造器进行了源码核对；未逐一执行全部 384 个 API，更没有在生产执行修改。静态 endpoint 候选匹配仅用于找线索，未将它作为覆盖率证明。

本地验证完成：当前源码 CLI 构建；递归 help 枚举；9 组 loopback 模拟 API/参数行为探针。模拟值均为合成标识与合成秘密，无真实业务数据或访问凭证。未声称测试通过即代表线上发布已验证。

迁移原则与现有框架一致：Cobra 区分隐藏与弃用，并建议带明确指引保留一个迁移版本；本计划在此基础上为命令树补上明确移除里程碑。[Cobra 官方 flag 指南](https://cobra.dev/docs/how-to-guides/working-with-flags/)

服务器验证、并发冲突与 dry-run 必须有明确契约；可以借鉴 Kubernetes API 对资源版本、冲突和 dry-run 的区分，但 Fugue 自己的实现仍需由 OpenAPI 定义和测试证明。[Kubernetes API Concepts](https://kubernetes.io/docs/reference/using-api/api-concepts/)


## 11. 实施进度（持续更新）

- 实施工作区：`/Users/yanyuming/Downloads/GitHub/fugue-cli-refactor-20260908`，分支 `codex/cli-refactor-20260908`。基于远端 main `0e7cb988`，未携带原目录其他任务的未提交代码。
- 最新仓库规则已将生产发布入口改为 `.github/workflows/ci.yml`；实施以新规则为准。
- 第一批完成独立 operation wait（deadline、取消本地等待、最后状态）、类型化错误分类与失败 JSON、overview 部分证据及完整性断言、嵌入 app spec 的统一 JSON 脱敏。
- 已提供 `help --json --all` 命令目录及 Markdown 导出；保守标记 caller-defined/may-change-state，不推断服务器授权。基线保存在 `implementation-command-catalog.json`。
- 已新增 app image、release versions/version/attempt、traffic show/set、rollout policy、source sync、domain primary verify/path-prefix。canary 第二步失败保留已创建 release 收据。
- 第一批验证：`go test ./internal/cli -count=1` 通过；新增等待/错误/脱敏/证据/参数迁移/部分发布失败场景。全量测试和发版验证正在进行。
- 调查与最新 main 的差异：main 已提供 pending-only operation cancel、部署结果/请求证据和若干诊断 API；后续核对其状态机和契约后计入对应任务，不重复实现。
- 尚未完成的项保持未勾选；正式 tag、生产发布结果和本机升级结果将在完成后补入。

- R1 已发布：main `2154efd7`，tag `v0.1.124`；[release-cli 工作流](https://github.com/yym68686/fugue/actions/runs/34249116561) 全量测试、打包和 GitHub Release 均成功。

### 2026-09-09 实现与本地验收

- B01–B08：109 个规范旧路径的参数/flag/default 映射及 R1 基线已固化到 `migration-parity.json`；删去 28 个专属构造器，保留共享实现。移除提示在任何业务请求和保存凭证钩子之前执行；习惯性别名与规范命令发生碰撞的旧别名不会覆盖新命令。`migrate scan` 仅输出固定命令片段、位置和动态构造提示。仓库文档/提示已迁移；唯一非历史扫描提示为 `helm template fugue` 的 release 名称，人工核对后确认不是 CLI 调用。fugue-web 未发现旧命令调用，已同步 OpenAPI snapshot 和生成类型，并通过 `npm run contract:check`。
- B04：帮助、机器目录、Markdown 导出和 completion 以 Cobra 定义为准；所有可直接解析的文档示例通过参数、flag 和必需 flag 校验，测试不执行业务请求。
- C03–C05/C07/C10：traffic 的控制意图与 `--observed` 采样证据分开；rollout/failover 写入职责拆分。drift 覆盖进程 env/command、文件摘要、mount、replicas、image digest、endpoints 与路由证据；路由采样不足以证明全局 serving 时保持 unknown。reconcile 只重应用 committed app spec，有原子哈希/活跃任务前置条件，验收绑定服务端接受的 exact spec。新输出契约以显式 `--json --output-version v1` 选择，默认保留原对象。详见 `cli-v0.2.0-workflows.md`。
- D01–D06：全局 image/pin/replication、真实 node graph probe、数据删除计划/引用保护、grant ls、平台 artifact plan/wait 和 scope state 均已实现。image 与 transfer 列表将 project/tenant 过滤下推；数据删除与新 app/operation 引用写入使用相容事务锁。测试覆盖来源缺失、租户隔离、重复会话/分块请求、超时、真实 probe 收据和拒绝伪造成功。
- E01–E02：4 MiB 分块、完整内容摘要、24 小时会话、不可变归档、请求意图冻结、请求→operation 原子关联与客户端私有收据。未知提交不自动重放；`operation recover --request-id` 只查询已确认副作用。
- E03：controller 调度真实 runtime Job 和独立 PVC，worker 下载并验证 blob 摘要，再写 ready 收据；controller 校验 Job UID、Pod ownership、目标 node 和完整字节/文件计数。取消/失败/过期/evict 进入可观察的清理流程；旧 inert prewarm 不会被突然执行。范围为 owned managed runtime + S3-compatible backend，缓存不自动挂载到 app。
- E04–E06：连接 context 不存服务配置或凭证；能力查询区分本地命令、服务端契约、授权与未知开关。overview 4 路并发且有总 deadline；写目标不再模糊匹配项目。request/operation/trace/incident 复用服务端诊断，共享 evidence v1、source status、partial 与确定关联。
- E07：复核最新 main 的 pending-only operation cancel 和状态机测试；没有增加无法补偿的通用运行中取消。各对象仍使用 operation cancel、release abort、data transfer cancel 各自契约。
- 验证：完整 `make test` 通过；CLI 示例解析、迁移/移除零请求、显式输出版本、overview 并发/截止时间、真实 image probe、source 分块恢复/并发提交锁、runtime cache 摘要/失败清理/Pod 身份、reconcile CAS、平台 LKG 及上下文等测试通过。生产发布仍需以下记录证明，尚未把本地测试当作生产完成。

### 最终发布验收（进行中）

- API / controller / schema：待声明式 CI 发布及健康验证。
- CLI v0.2.0 tag / GitHub Release：待发布。
- 本机 `/opt/homebrew/bin/fugue`：仍为 v0.1.123，待最终升级。
