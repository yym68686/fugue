# Fugue CLI TUI 系统重构计划
日期：2026-09-09

## 实施记录（2026-09-09 重新审计）

工作区：`/Users/yanyuming/Downloads/GitHub/fugue-tui-modernization-20260909`。
上一版把未实现或未验证的项目打勾，并过早宣称完成；该记录已撤回。本节仅在有代码及验收证据时打勾，v0.3.0 不代表本计划完成。

- [x] Bubble Tea v2 / Lip Gloss v2 与独立 `internal/tui` Provider 边界已经存在。
- [x] M0：逐入口兼容基线和明确迁移清单；纠正与原计划冲突的删除声明。
- [x] M1：统一命令参数、真实事件循环、上下文取消、路由订阅隔离和退出恢复。
- [x] M2：响应式布局、可滚动长详情、完整键鼠等价、文本选择、主题/偏好验证与视觉矩阵。
- [x] M3：应用/pod/runtime/operation 详情链、日志 SSE 游标恢复、部分失败保留证据。
- [x] M4：真实历史指标、正确采样来源与间隔、空值/权限/stale、窗口/阈值/选点和采样预算。
- [x] M5：节点/容量/policy、runtime、组件/workload/发布证据，管理员权限和 scope 导航。
- [x] M6：服务端 CAS/幂等、精确确认、未知提交恢复与操作追踪；凭据脱敏验收。
- [ ] M7：PTY、10 分钟稳定性、性能、终端矩阵、beta/反馈门槛和正式发布。
- [ ] OpenAPI 生成、前端同步、全量测试、正式生产 lane 完成证据。
- [ ] 全平台 release 资产及校验和、本机正式升级、示例命令真实启动验收。

已修复的审计缺陷：管理员命令遗漏 `--mouse/--theme`；console 丢失鼠标 flag 值；SSE 未绑定取消上下文；旧订阅无 epoch 隔离；日志未正确增量消费；resource history 使用错误 target kind/采样间隔；节点和组件详情不完整；发布验证及长时测试此前无充分证据，M7 仍待正式发布。

### 当前实现与复验依据

- 参数/兼容：`TestTUIEntrypointsExposeSharedFlags`、`TestAdminTUIAppearanceParsingAndSnapshotCompatibility`、既有 console/monitor JSON/plain/once 测试，四入口共用 `bindTUIAppearanceFlags`。
- 订阅：`TestWatchStopsOnNavigationPauseAndIgnoresOldEvents`、`TestTUILogStreamResumesAndCancelsAtServer`；HTTP context 取消、路由 epoch/generation 隔离、Last-Event-ID 恢复；2000 行上限且按游标而非文本去重。
- 图表：`TestTUIResourceHistoryUsesCorrectPerReplicaSeries`、`TestTUILiveSamplesRetainKubeletTimeAcrossCachedReads`；修正 5 分钟历史采样类型和间隔，实时样本保留 kubelet 时间；没有 exporter 的 network 明示 unavailable。1 个点只显示 collecting，两个点起绘图。
- 详情：`TestClusterComponentScopeAndPodDrilldown`；`--scope control-plane/nodes/runtimes/all`，节点容量/policy、组件 workload、最近 workflow 证据；长详情与帮助可滚动。
- 操作：`TestAppActionReceiptReplaysAfterCASChangesAndIsActorScoped`、`TestImageActionRejectsChangedDigest`、`TestTUIUnknownSubmissionRecoversReadOnly`。操作和幂等回执原子写入；16 个并发重复请求在隔离 PostgreSQL 中只创建一个 operation；换进程后可读回本地仅含 app/request ID 的回执日志，再 GET 恢复，禁止透明 POST 重放。
- 终端：仓库 `scripts/tui-qa/smoke.cjs` 真 PTY 覆盖四入口、鼠标/日志/resize/断线恢复、q/Ctrl-C，truecolor/256 色/ANSI/NO_COLOR。最近候选复验 first paint 167–708ms；并行编译时曾有 1.074–3.443s 的冷启动失败，正式产物需在无编译竞争下复验，不能忽略失败记录。
- 视觉：xterm/Playwright 生成 60/80/100/140/200 列快照；宽/窄屏已人工查看。Go 测试覆盖 8/18/30/50 行、CJK/组合字符和无色输出。
- 稳定性：第一轮真实 10 分钟（1000 行/4 图/10 operations）通过，goroutine 12→5、堆 4.99MB→3.98MB、日志 2000、活动订阅 0。五图 200 列的第二轮正在复验。
- 全量：`GOFLAGS=-p=2 make test`、TUI/CLI/API/store 定向 race、go vet、前端 OpenAPI sync/generate/contract:check 已通过。最后一次代码/发布变更仍需对应检查。
- `[cli-tui]` 用户反馈：v0.3.0 管理员示例 unknown flag；本轮已据此修复共享参数并增加所有入口的真实 PTY 测试。beta `v0.3.1-beta.1` 已推送，等待 release 工作流资产。


## 兼容与迁移决策

| 入口/逻辑 | 实施要求 |
| --- | --- |
| `console`、`app top`、`project top`、`admin cluster top` | 使用统一交互 TUI；参数行为和帮助必须一致并通过真实终端验证。 |
| `project watch`、`operation watch` | 按原计划保留窄用途 monitor 及脚本语义，不谎称已经迁移或删除。 |
| `--plain/--once/--json` | 保持原有输出兼容，不发送控制字节。 |
| legacy console/monitor/ui | 继续承担兼容输出，不为删除包破坏既有脚本；统一新 TUI 展示代码。 |
| 写动作 | 只在读到服务端能力、确认计划且有 CAS/幂等证据后开放。 |

## 调查结论

当前实现不是完整 TUI，而是“数据加载器 + ANSI 字符串渲染器”的预览版。主要入口是：

- `fugue console`：`internal/cli/console_command.go`
- `fugue project watch`：`internal/cli/project_overview.go`
- `fugue operation watch`：`internal/cli/ops.go`
- `fugue admin cluster top`：`internal/cli/admin_cluster_top.go`
- 共享状态和刷新：`internal/cli/monitor/monitor.go`
- 共享布局：`internal/cli/ui/renderer.go`
- 终端会话：`internal/cli/terminal/session.go`

当前没有 Bubble Tea、tview、tcell 或图表依赖。自定义渲染器把整个页面和每个表格包在 Unicode 方框内，使用固定的 `COLUMNS` 环境变量推测宽度。已有颜色角色、状态标签、表格宽度收缩、筛选、排序和快照测试，但这些是渲染辅助能力，不是完整交互框架。

### 已确认的具体问题

1. **console 不是持续交互程序。** `console_command.go` 进入会话后只调用一次 `Render`；没有事件读取、刷新 tick、模型更新循环或退出事件处理。Alt screen 只是发送进入/退出控制码。
2. **鼠标没有实现。** `--mouse` 只在页面上显示 `mouse=optional`；没有启用 mouse tracking、解析坐标、命中测试、滚轮、点击选择或点击后的路由。
3. **排版是调试信息式布局。** 页面主体是 `preview=true`、`state=...`、`project=...` 等 key/value 行；summary、表格、日志和 actions 纵向堆叠，缺少固定区域、视觉层级、选中态、空白预算和响应式重排。
4. **没有实时性能曲线。** 现有 `MetricBar` 只画单个数值条；console 的 app 表没有时间序列。API 已有 observability metrics summary/query 和 resource usage snapshot，但 TUI 没有统一采样器、环形缓冲、时间窗口或降采样策略。
5. **更新逻辑不完整。** `project watch` 和 `cluster top` 有轮询；console 没有轮询。轮询失败只在旧画面上追加错误文本，缺少 stale age、重试退避和每个数据源的独立状态。
6. **终端能力检测不完整。** 渲染器依赖环境变量宽度；没有真正处理 resize 事件。Unicode 宽度函数把每个 rune 视为 1 列，CJK、emoji、组合字符和宽字符会错位。
7. **数据边界混乱。** 普通用户的 app/project 视图、管理员 cluster 视图和专家诊断动作共用页面模型；`--admin` 是命令 flag，而不是根据服务端 principal 能力协商出的导航模型。
8. **动作模型不一致。** console 显示 restart/redeploy/cancel action plan，但当前页面是只读且没有真正的按钮命中、确认弹窗、操作提交和结果追踪。
9. **测试只证明字符串。** 测试覆盖宽窄快照和状态文本，不能证明刷新节奏、鼠标命中、resize、断线恢复、图表边界或终端性能。
10. **API 采集会重复放大负载。** app、project、cluster 页面各自调用多组列表接口；没有按页面共享缓存、请求合并、采样频率分级和并发预算。TUI 不能因为刷新而重新触发昂贵的 inventory 查询。

## 目标体验

目标不是把 agent 的 JSON 输出加颜色，而是一个人类操作的 terminal workbench：

- 首屏 1 秒内显示结构化骨架和上次已确认的数据。
- 2–3 秒刷新一次状态，性能曲线 5–10 秒刷新一次；不同数据源使用独立 TTL。
- 鼠标可点击所有可交互标题、tab、表格行、筛选器和动作按钮；滚轮可滚动表格和日志。
- 键盘和鼠标完全等价，`q` 退出，`r` 刷新，`?` 帮助，`/` 搜索，`tab` 切换面板，`enter` 打开详情。
- 页面始终显示数据时间、刷新状态、权限缺失、部分结果和错误来源。
- 状态颜色之外还使用文字和图标，满足无色终端、低对比度和色盲场景。
- `--json`、`--plain`、`--once` 保持机器输出和 CI 语义不变。

### 用户视图

建议入口：

```text
fugue app top <app>
fugue app top <app> --window 15m --interval 3s
fugue project top <project>
```

页面区域：

1. 顶栏：app 名称、project、当前状态、region/runtime、最后采样时间、连接状态。
2. 左上：CPU、内存、网络、请求速率、错误率、p50/p95/p99 延迟曲线。
3. 右上：replicas、ready/desired、版本、route、active operation 和最近变更。
4. 中部：pod/runtime 表，支持排序、过滤、选中后打开 pod/app 详情。
5. 底部：事件时间线、最近错误、可复制的下一步命令。
6. 详情层：按 `enter` 或鼠标点击行打开，不离开主页面。

### 管理员视图

```text
fugue admin cluster top
fugue admin cluster top --scope control-plane
```

区域应包括：

- 集群摘要：节点 ready 数、runtime 容量、控制面版本、发布状态。
- 节点矩阵：CPU、内存、磁盘、网络、workload 数、policy drift、最近 heartbeat。
- runtime/pool：类型、区域、容量、健康门禁、调度占用。
- control plane：API/controller/guardian/schema 的 desired/live/ready、版本和最近发布。
- 告警/事件：按严重度、时间和组件聚合。
- 管理员专属动作必须由 server capability 和 scope 决定，不能只相信 `--admin`。

## 推荐技术架构

### 1. 使用成熟 TUI runtime

优先评估 Bubble Tea v2 + Bubbles + Lip Gloss + BubbleZone + ntcharts：

- Bubble Tea 提供 model/update/view 事件循环、键盘和鼠标消息、resize、alt screen 和 cell renderer。
- Lip Gloss 负责样式和布局，不再自己拼接 ANSI。
- BubbleZone 负责鼠标区域命中。
- ntcharts 负责 line/area/bar/sparkline 等图表。

Bubble Tea 官方说明其 runtime 支持高性能 cell renderer、键盘鼠标处理和声明式 view；btop 的公开特性也明确包含全鼠标支持、滚动、筛选、排序、自动缩放网络图和主题系统。[Bubble Tea](https://github.com/charmbracelet/bubbletea)、[btop features](https://github.com/aristocratos/btop#features)、[ntcharts](https://github.com/NimbleMarkets/ntcharts)

不要把 Bubble Tea 直接散落到 API client。分成四层：

```text
API adapters -> sampling/cache store -> view model -> TUI runtime/rendering
```

### 2. 统一数据采样层

定义只读的 `SnapshotStore`：

- 每种资源一个 cache key、TTL、last_success、last_error、stale_since。
- 并发刷新去重；同一 app 的多个 panel 共享一次请求。
- 状态列表按 2–3 秒刷新；metrics 按 5–10 秒刷新；logs/events 用流或增量 cursor。
- 请求超时、指数退避、最大并发和取消都由采样层负责。
- TUI 永不伪造缺失指标；没有历史数据就显示 `collecting`，没有权限显示 `permission denied`。

### 3. 指标契约

优先复用已有 OpenAPI metrics：

- app：CPU、memory、network、request rate、error rate、latency quantiles、replica ready/desired。
- runtime/node：CPU、memory、disk、network、workload count。
- control plane：component readiness、release age、operation backlog、database connection/lock indicators（只暴露已授权聚合值）。

新字段必须先进入 `openapi/openapi.yaml`，再生成后端和前端 artifacts。时间序列采用 `observed_at`、单位、来源、采样间隔、完整性状态；不得把一次 snapshot 当成曲线。

### 4. 响应式布局

采用 grid/flex layout，而不是固定字符串：

- 宽度 >= 140：四区 dashboard。
- 100–139：三列，日志折叠。
- 72–99：两列。
- <72：单列摘要 + 可进入的详情页。
- 高度不足时按优先级隐藏次要面板，并在 footer 显示 `N more panels`。
- 监听真实 resize，重新计算 panel bounds；用 cell width 计算 Unicode/CJK/emoji。

### 5. 交互模型

定义统一 message：

- `tick`、`metricsTick`、`dataLoaded`、`dataFailed`
- `keyPress`、`mouseClick`、`mouseWheel`、`windowSize`
- `openDetail`、`openHelp`、`confirmAction`

鼠标区域由 view model 注册，不以屏幕坐标硬编码。危险操作流程：

```text
点击 restart -> ActionPlan -> 明确影响和 operation -> 输入精确确认 -> 提交 -> operation detail -> wait
```

## 分阶段实施

### P0：先修正确性和交互骨架（3–5 天）

- 保留现有 `--plain/--once/--json`。
- 引入事件循环，真正处理 q/Ctrl-C、键盘、resize、刷新 tick。
- 为 console 接入持续 refresh；删除 `preview=true` 等调试字段的视觉主位。
- 引入 layout model、panel bounds、focus model 和 footer help。
- 加入 no-color、窄屏、CJK、断线、stale 数据测试。
- 验收：console 持续运行 10 分钟不增长 goroutine；刷新失败不丢旧画面；q/Ctrl-C 正常恢复终端。

### P1：btop 风格视觉系统和鼠标（1 周）

- 主题 token：background、panel、border、accent、positive、warning、danger、muted、selection。
- 面板标题、状态 chip、数值对齐、异常 badge、选中行和滚动条统一。
- BubbleZone 命中区域和 mouse tracking；鼠标滚轮、点击 tab、点击行、点击刷新。
- 自适应 grid 和真实终端 resize。
- 视觉回归：宽度 60/80/100/140/200，高度 18/30/50，16 色/256 色/truecolor/no-color。

### P2：实时指标和图表（1–2 周）

- SnapshotStore、TTL、请求去重和采样预算。
- app/runtime/node 的 ring buffer，至少支持 5m/15m/1h 窗口。
- line chart、area chart、sparkline、阈值线、空数据和降采样。
- tooltip/选中点详情、暂停/继续采样、窗口切换。
- 图表数字与 API 原始指标保留单位和时间戳。

### P3：用户和管理员两个信息架构（1 周）

- `app top` 与 `admin cluster top` 共享组件但不共享权限假设。
- 根据 `capabilities` 和 principal 自动生成导航；无权限面板显示原因。
- app 视图支持 app -> pod -> runtime -> operation 详情链。
- 管理员视图支持 component -> workload -> release evidence 链。
- 保留 `project watch` 和 `operation watch` 作为窄用途 monitor，避免所有功能挤进 console。

### P4：安全动作和性能打磨（1 周）

- 只读默认；restart/redeploy/rollback/scale 通过 ActionPlan、确认、operation wait。
- 日志、诊断和环境变量遵守既有 redaction；TUI 不渲染 secret。
- 采样慢请求可见但不阻塞画面；单 panel 失败不拖垮全屏。
- 基准：启动、刷新、resize、1000 rows、5 个图表、断线重连的 CPU/内存上限。
- 发布 beta，收集 `[cli-tui]` 反馈后再默认替换旧 console。

## 验收标准

- 交互：键盘、鼠标、滚轮、resize、alt screen、Ctrl-C 全部可重复测试。
- 视觉：不再出现每层嵌套方框；面板层级、空白、颜色、数值和长文本在五档宽度稳定。
- 数据：曲线来自至少两个时间点；每个值显示来源和 observed_at；缺失、权限和 stale 不混为 0。
- 性能：首屏、刷新、图表更新不阻塞输入；同一刷新周期不重复请求同一资源。
- 可靠性：API 超时和断线保持旧画面，明确 stale age 和重试；退出后终端状态完整恢复。
- 兼容：JSON/plain/once 输出不变，非 TTY 不发送 ANSI/control bytes，旧 monitor 命令仍可用于脚本。
- 安全：所有写动作显式 ActionPlan 和确认，TUI 不绕过服务端 scope/CAS/idempotency。

## 暂不做

- 不把 agent workflow、原始 OpenAPI、完整 debug bundle 塞进主 dashboard。
- 不在客户端伪造服务器没有提供的 CPU、GPU、网络或全局 route 指标。
- 不直接复制 btop 的进程监控模型；Fugue 需要资源、发布、路由和 operation 关联。
- 不在没有 API 契约和权限模型前开放管理员写操作。

## 参考三个项目后的架构升级

### Grok Build：全屏工作台和长任务可见性

Grok Build 的 pager 将 TUI、agent runtime、tools、workspace 分成独立 crates；TUI 本身拥有 scrollback、prompt、modal 和 rendering。它把任务 pane、todo pane、命令面板、fullscreen/minimal 两种模式和可恢复 session 做成一等功能。其用户指南还规定命令菜单会根据当前模式过滤命令，避免用户进入不可用页面。[grok-build README](https://github.com/xai-org/grok-build)、[Grok slash commands](https://github.com/xai-org/grok-build/blob/main/crates/codegen/xai-grok-pager/docs/user-guide/04-slash-commands.md)

对 Fugue 的可迁移做法：

- 将主界面拆为 **Dashboard、Details、Events、Tasks、Help** 五类 screen；screen 之间通过 route stack 切换，不用把所有表格堆在一帧里。
- 增加右侧 Tasks pane，展示刷新请求、operation、日志流和诊断任务的状态、耗时、重试次数和最近活动；它是可观察性面板，不是 agent 子任务。
- 增加 command palette，命令根据角色、scope、当前资源和 screen capability 过滤；不可用命令要说明原因。
- 支持 `fullscreen` 和 `compact` 两种显示模式；compact 模式退化为可复制的流式文本，和 `--plain` 不冲突。
- 对 operation、日志流和 metrics 查询保存 session cursor；网络断开后恢复到最近确认位置，不重放写操作。

### OpenCode / OpenTUI：独立 TUI 包和边界

OpenCode 的 TUI 抽取规范要求一个独立的 TUI package，只依赖 SDK，不依赖 CLI/backend 私有实现；CLI host 负责认证、进程、配置和启动，server/SDK 负责领域数据、操作和 capabilities。它还使用组件、routes、dialogs、themes、keymaps、toast 和 plugin slots 组织大型 TUI。[OpenCode TUI package spec](https://github.com/anomalyco/opencode/blob/dev/specs/tui-package.md)

对 Fugue 的可迁移做法：

- 建立 `internal/tui` 独立模块，依赖 `internal/tuiapi` 或生成 API client，不直接导入 `internal/api`、store、controller。
- CLI host 只负责 Cobra 参数、context/auth、终端初始化、signal 和退出码；TUI 模块负责 model、screen、component、keymap、theme、dialog、toast、clipboard。
- 将现有 `internal/cli/console`、`monitor`、`ui` 的纯展示逻辑迁入单一 canonical package，禁止 console、project watch、admin top 各自复制布局。
- view model 只接收稳定的 SDK wire objects 和 capability；未知字段/未来 operation 用通用 fallback，不因服务端新增字段崩溃。
- 主题、键位和面板可见性使用版本化 `tui.toml`，配置只保存本地 UI 偏好，不保存 serving intent、token 或租户数据。
- 预留只读插件 slot：自定义 status item、detail panel、diagnostic card；插件不能直接获得写 API 或绕过 server scope。

OpenTUI 的 flexbox、原生 cell renderer 和 keyboard/mouse controls 可以作为性能目标；Fugue CLI 仍应优先选择 Go 原生方案，除非决定把 TUI 单独拆成 TypeScript/Zig 二进制。跨语言重写会显著增加发布、签名和本地安装复杂度。

### Codex：事件流、屏幕尺寸和鼠标命中

Codex TUI 的实现以事件流驱动 screen：读取 `TuiEvent`，在 `Resize`、`Draw`、`Resume` 时重绘，并在绘制前按事件更新 screen size。Codex 的鼠标选择讨论特别强调，开启捕获后必须由应用自己完成 click/motion/release 的命中和选择状态，不能捕获一半再退回 terminal native selection。[Codex event-driven screen example](https://github.com/openai/codex/blob/main/codex-rs/tui/src/cwd_prompt.rs)、[Codex mouse selection analysis](https://github.com/openai/codex/issues/41211)

对 Fugue 的可迁移做法：

- 用单一事件总线处理 key、mouse press/move/release、wheel、paste、resize、timer、network response；所有 screen 共享同一套 lifecycle。
- 每次 resize 先更新 layout tree，再计算所有 hitbox；禁止把按钮坐标写死在 renderer。
- 开启 mouse capture 后，日志和表格的文本选择由 Fugue 自己管理；同时提供 `copy`、`select all` 和外部终端复制降级。
- 异步 API response 只能通过 message 回到主 model；网络 goroutine 不得直接改渲染状态。
- 退出时统一恢复 raw mode、mouse tracking、bracketed paste、cursor、alt screen；异常和 panic 也走同一 cleanup。
- Draw 事件只渲染当前 immutable model，避免一半更新导致表格和图表不一致。

## Fugue 的最终目标架构

```text
Cobra host
  ├─ auth / context / signal / exit code
  └─ TUI host adapter
       └─ internal/tui
            ├─ app model + route stack
            ├─ event reducer
            ├─ capability-aware command palette
            ├─ layout tree + hitbox registry
            ├─ theme / keymap / accessibility
            ├─ snapshot store + metrics ring buffers
            ├─ screens: dashboard, details, events, tasks, help
            └─ components: table, chart, timeline, log viewport, dialog, toast
                 └─ TUI API client (generated OpenAPI SDK)
                      └─ Fugue control-plane API
```

数据流必须是单向的：

```text
API/SSE -> adapter -> cache/event store -> message -> reducer -> immutable model -> view
user input -> hitbox/keymap -> command intent -> action plan -> API client -> operation event
```

这样可以同时提供：

- TUI 实时模式：事件驱动 + 定时采样。
- `--once`：同一 model 的一次 render。
- `--plain`：同一 view model 的 scrollback renderer。
- `--json`：同一 adapter 的机器 envelope。
- 测试：不启动真实终端即可对 reducer、layout、hitbox、snapshot 和 view 做确定性测试。

## 重点取舍

| 选择 | Fugue 方案 | 原因 |
| --- | --- | --- |
| TUI runtime | 先用 Bubble Tea v2；图表评估 ntcharts | Go 生态、事件循环、鼠标、resize 和 chart 组件成熟 |
| 布局 | flex/grid layout tree | 解决现在固定宽度和嵌套方框问题 |
| 数据更新 | SSE/增量事件 + TTL polling fallback | 状态实时性和 API 负载可控 |
| 图表 | client ring buffer + server metric query | 5m/15m/1h 窗口，不伪造缺失历史 |
| 鼠标 | application-owned hitboxes and selection | click、wheel、text selection 语义一致 |
| 配置 | versioned local TUI preferences | 主题、键位、默认 screen 可持久化 |
| 插件 | 只读 presentation slots | 可扩展展示，不扩大凭证和写权限边界 |
| 多语言 | 暂不重写 Rust/TypeScript | 避免第二个发布链和安装器 |
| 默认入口 | 先保留 `fugue console`，新增 `app top` | 兼容旧命令，同时给 app/管理员清晰入口 |

## 具体里程碑

1. **M0 基线（1 天）**：录制现有 console/project watch/admin top 的宽度、颜色、非 TTY、断线和请求次数；建立可重复 fixture。
2. **M1 事件骨架（3–5 天）**：Bubble Tea host、screen stack、resize/key/mouse event、cleanup、plain fallback；console 先做到真正持续运行。
3. **M2 单一布局和主题（3–5 天）**：去除外层大方框，完成 panel/grid/status bar/footer、主题 token、键位帮助和 hitbox。
4. **M3 App Dashboard（1 周）**：`app top`、pod/runtime/operation drill-down、日志 viewport、SSE/轮询刷新和 stale 状态。
5. **M4 实时指标（1 周）**：OpenAPI metrics query、ring buffer、降采样、CPU/memory/request/error/latency 曲线和暂停/窗口切换。
6. **M5 Admin Cluster（1 周）**：集群、节点、runtime、控制面和发布任务 pane；根据 capability 自动隐藏无权限面板。
7. **M6 安全动作（3–5 天）**：ActionPlan dialog、精确确认、CAS/idempotency、operation wait；默认保持只读。
8. **M7 Beta（3–5 天）**：性能基准、terminal matrix、视觉快照、长时间稳定性、release channel 和反馈指标；达到门槛后才考虑替换旧 console 默认界面。

## 必须守住的门槛

- 首屏骨架 <=1s；输入响应 p95 <=50ms；普通刷新不超过 3s；图表绘制不阻塞输入。
- 1000 行表格、5 个图表、10 个并发 operation 时 UI 仍可交互。
- 断网 30s 后恢复不重复写操作；旧数据显示 stale age。
- 任何指标没有来源、时间戳或权限时不显示为 0。
- 任何鼠标点击都能由键盘完成；无鼠标终端仍完整可用。
- 非 TTY、`--plain`、`--json` 不发控制序列；原有脚本输出契约保持不变。
- TUI 与 API、配置、controller 保持解耦；失败的 TUI release 不影响当前 serving artifact。
