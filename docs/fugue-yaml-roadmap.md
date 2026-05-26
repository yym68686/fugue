# Fugue YAML 改造路线图

Fugue 现在的 `fugue.yaml` 更像“导入拓扑 manifest”，还不是完整的项目 manifest。

下一阶段应该把它演进成一个能完整描述、重建、迁移一个真实应用的声明式文件，但必须保留当前 `version: 1` 的兼容路径，不能让老项目被迫一次性重写。

## 目标边界

- `fugue.yaml` 只描述期望状态，不描述运行时瞬时状态。
- 路由、域名、服务、存储、秘钥、观测、发布计划都应能从同一份文件重建。
- `domain_routes` 不建议做成单独文件，但也不应被揉进 `domains` 的附属字段里；`domains` 只负责 host / TLS / 归属，HTTP 路由应作为一等入口表存在，再由 `domains` 绑定到入口表。
- `version: 1` 继续兼容，`version: 2` 才承载完整项目能力。

## 1. 现有支持的字段

### 顶层字段

| 字段 | 状态 | 说明 |
| --- | --- | --- |
| `version` | 已支持 | 仅接受空值或 `1`。 |
| `primary_service` | 已支持 | 指定主公开服务；缺省时会从公开服务里推断。 |
| `template` | 已支持 | 只用于模板元数据，不直接参与 runtime 部署。 |
| `services` | 已支持 | 服务定义集合。 |
| `backing_services` | 已支持 | 后端服务定义集合。 |
| `env_file` | CLI-only | 只用于本地 `fugue deploy` 的默认 env 文件，不属于核心 manifest schema。 |

### template 字段

`template` 当前支持这些元数据字段：

| 字段 | 说明 |
| --- | --- |
| `name` | 模板名。 |
| `slug` | 模板 slug。 |
| `description` | 模板说明。 |
| `docs_url` | 文档链接。 |
| `demo_url` | Demo 链接。 |
| `default_runtime` | 默认 runtime。 |
| `source_mode` | 来源模式。 |
| `variables[]` | 模板变量列表，支持 `key`、`label`、`description`、`default`、`generate`、`required`、`secret`。 |

### service 字段

#### 已生效

- `image`
- `build`
- `port`
- `public`
- `network_mode`
- `network_policy`
- `env`
- `environment`
- `env_file`
- `generated_env`
- `persistent_storage`
- `depends_on`
- `bindings`
- `owner_service`
- `type`
- `service_type`
- `database`
- `user`
- `password`
- `service_name`

#### 部分生效

- `volumes`：符合仓库相对 bind mount 的会被吸收到 `persistent_storage`，其他写法会被忽略或降级为提示。
- `build.args`
- `build.target`

#### 仅保留，不作为当前运行时输入

- `command`
- `entrypoint`
- `healthcheck`
- `profiles`
- `labels`
- `deploy`
- `secrets`
- `configs`
- `networks`

### 当前行为补充

- `service` 名称会被规范化为 slug。
- `service_type` / `type` 会参与分类，当前可识别 `app`、`postgres`、`redis`、`mysql`、`object-storage`、`custom`。
- `public: true` 现在表示这个服务要有公开 route，但它仍然是 hostname-centric，不支持 path-level routing。
- `network_mode` 目前只支持 `background` / `internal`。
- `network_policy` 目前只支持受限模型及其 allow list。
- `generated_env` 目前只支持随机生成，编码支持 `base64url`、`base64`、`hex`。
- `build` 目前支持 `strategy` / `build_strategy`、`context` / `source_dir` / `build_context_dir`、`dockerfile` / `dockerfile_path`、`args`、`target`。
- `persistent_storage` 目前支持 `mode`、`storage_path`、`storage_size`、`storage_class_name`、`shared_sub_path`、`reset_token`、`mounts[].kind`、`mounts[].path`、`mounts[].seed_content`、`mounts[].secret`、`mounts[].mode`。
- `env_file` 在 service 级别支持 compose 风格输入，包含 string、array、以及带 `path` / `required` 的对象形式。

## 2. 最终版本应该是什么样

建议把新版本记为 `version: 2`，并把 `fugue.yaml` 定义为完整的项目合同，而不是单纯的导入文件。

### 推荐顶层结构

```yaml
version: 2

project:
  name: uni-api-web
  description: App lifecycle project for 0-0.pro

domains:
  - name: production
    host: 0-0.pro
    tls: managed

entrypoints:
  - name: public
    domain: production
    routes:
      - path: /
        service: web
      - path: /v1/*
        service: api

services:
  web:
    image: yym68686/uni-api-frontend:main
    port: 3000
    env:
      NEXT_PUBLIC_API_BASE_URL: /v1

  api:
    build:
      context: .
      dockerfile: Dockerfile
    port: 8000
    network_mode: internal
    env_file: .env.api
    secrets:
      JWT_SECRET:
        value: "..."
      DATAOCEAN_SERVER_KEY:
        from_secret: dataocean_server_key
      SESSION_SECRET:
        generate: random
        encoding: base64url
        length: 32

backing_services:
  postgres:
    type: postgres
    image: postgres:17
    database: uni
    user: uni
    password:
      from_secret: postgres_password
    persistent_storage:
      mode: movable_rwo
      storage_size: 20Gi

observability:
  dataocean:
    enabled: true
    collect_url: https://collect.example.com
    server_key:
      from_secret: dataocean_server_key

release:
  preflight:
    shadow: true
    compare:
      status: true
      latency: true
  rollback:
    auto: true

intent:
  availability: 99.9%
  region_preference: [hk]
  budget_monthly_usd: 20
```

### 设计上应该新增的能力

- `project`：项目身份、描述、默认策略、复制导出入口。
- `domains`：域名、TLS、所有权与验证状态的唯一归属地。
- `entrypoints` / `http_routes`：`path`、`service`、`strip_prefix`、`rewrite`、`weight`、`mirror` 这类 HTTP 匹配与转发规则。
- `secrets`：真正的 secret 值和 secret 引用统一进这一层，而不是继续混在普通 `env` 里。
- `observability`：DataOcean、日志、指标、回放、转化事件都从这里接。
- `release`：预检、影子流量、比较、回滚、canary 和 weighted traffic。
- `intent`：延迟、预算、可用性、区域偏好、合规偏好。

## 3. 详细方案

### 3.1 不要把路由揉成域名属性

路由不是 domain 的附属字段，也不是 release 的附属字段。

如果把它们揉在一起，会出现三个问题：

- 域名、路由和发布策略的生命周期被绑死，后续很难独立演进。
- path 匹配、backend 切换、shadow compare、回滚会被迫共用一套字段。
- UI / diff / export 会把“入口定义”和“切流策略”混成一个对象。

所以更好的方式是：

- `domains` 只负责 host、TLS、所有权、验证与 DNS 归属。
- `entrypoints` / `http_routes` 负责 path matching、backend mapping、rewrite 和 strip prefix。
- `release` 负责同一路由的版本切流、影子流量、比较和回滚。
- `intent` 负责区域偏好、容量偏好和故障切换意图。
- 当前 `public: true` 只是兼容旧模型的快捷入口，最终会被编译成默认入口和默认路由表。
- 服务是否默认拥有公网 hostname，和是否能被 `entrypoints` 引用，是两件事；`public` 只应该决定是否生成默认入口，而不是唯一曝光开关。

换句话说，`domains` 可以承载 route table 的绑定关系，但不应该成为 route table 本身。

### 3.2 secret 可以有真实值，但必须有独立通道

`fugue.yaml` 需要支持真实 secret 值，否则很多项目无法做到完整重建。

但我不建议把 secret 值继续塞进普通 `env` 里，因为那会把导出、脱敏、Diff、Git 同步、复制按钮全都搞复杂。

更稳的做法是：

- `env` 只放普通明文配置。
- `secrets` 放真正的敏感项。
- `secrets` 支持 `value`、`from_env`、`from_secret`、`generate`。
- `generated_env` 作为旧字段保留一段时间，逐步迁移到 `secrets.generate`。

这样用户既能上传完整配置，也能在 Git 里保留安全版本。

### 3.3 `fugue.yaml` 应该支持两种导出模式

前端一键复制当前项目配置时，至少要有两种模式：

- **安全导出**：默认脱敏，适合提交 GitHub。
- **私密导出**：保留真实 secret，适合用户本地迁移或私密上传。

建议再加一个第三种模式：

- **legacy 导出**：只导出当前 `version: 1` 可识别的子集，方便旧链路继续用。

### 3.4 导出对象应该来自“当前期望状态”，不是 runtime 偶发状态

复制当前项目的 `fugue.yaml` 时，前端应该组装这些来源：

- app spec
- route/domain 绑定
- backing services
- persistent storage
- generated env 定义
- observability 配置
- project 元数据

不应该把这些东西混进去：

- pod IP
- 当前 ready 状态
- deployment revision
- 临时日志
- 操作审计流水

这份文件应该能重建项目，而不是转储现场。

### 3.5 兼容策略

- `version: 1` 继续走当前 import 逻辑。
- `version: 2` 走新 schema。
- 老字段保留 shim，不要一次性断掉。
- 新 schema 要严格校验，不能把 `domains` 这类关键字段静默忽略。

## 4. 前端体验

前端应该把 `fugue.yaml` 作为一等对象，而不是藏在高级设置里。

建议直接提供这些动作：

- Copy current `fugue.yaml`
- Download `fugue.yaml`
- View diff against live state
- Copy legacy manifest
- Copy private migration bundle

对于已经迁移到 Fugue 的项目，这个入口本质上是在告诉用户：

> 这个项目可以被完整理解，也可以被完整带走。

## 5. TODO list

- [x] **阶段：先支持 `domains` + `entrypoints`。**  
  目标是把 hostname、TLS、path routing、backend 归属分层放进同一个项目合同里，并把当前 `public` / `primary_service` 兼容编译进去。
  - [x] Parser：`fugue.yaml` 能解析 `domains[]`、`entrypoints[]`、`routes[]`、`path` / `path_prefix`、`strip_prefix`、`rewrite`。
  - [x] Schema：OpenAPI 暴露 `TopologyDomain`、`TopologyEntrypoint`、`TopologyEntrypointRoute`，并在 template inspect 响应里保留这些字段。
  - [x] 导入器：GitHub / upload topology import 能把 entrypoint route 编译成 app route。
  - [x] 路由归属：app route 持久化 `path_prefix`、`domain_name`、`entrypoint_name`，方便导出和 diff 找回来源。
  - [x] 兼容编译：旧的 `public: true` / `primary_service` 继续生成默认 `/` route。
  - [x] Route 生成器：edge route bundle 按 hostname + path prefix 输出，edge 侧能做 path-level routing。
  - [x] 控制平面发布：变更已通过 GitHub Actions 控制平面链路发布到线上。
  - [x] 前端入口：Console 已提供 route `path_prefix` 的可视化编辑入口。
  - [ ] 前端完整编辑器：补完整 `domains` / `entrypoints` / route table 的结构化编辑、排序、冲突提示和删除确认。

- [x] **阶段：补齐 Fugue CLI 对 `domains` + `entrypoints` 的适配。**  
  目标是让 CLI 与 parser、schema、导入器、route 生成器看到同一份项目合同。
  - [x] `fugue app route check <app> <hostname> --path-prefix <path>` 支持检查 hostname + path prefix 组合。
  - [x] `fugue app route set <app> <hostname> --path-prefix <path>` 支持设置非根路径 route。
  - [x] `fugue app route show` 显示 `path_prefix`、`domain_name`、`entrypoint_name`。
  - [x] `fugue app domain primary ...` 兼容旧命令，并在输出里显示默认 `/` path prefix。
  - [x] `fugue app status` 输出补充 route provenance，包含 `route_path_prefix`、`route_domain_name`、`route_entrypoint_name`。
  - [x] `fugue deploy inspect` / `fugue template inspect` 展示 `domains`、`entrypoints`、route count。
  - [x] `fugue deploy --dry-run` plan service 行携带 `path_prefix`、`domain_name`、`entrypoint_name`，文本表格能显示带 path 的 public URL。
  - [x] `fugue admin routes ls` 按 hostname + path prefix 分组，避免同 hostname 的不同 path route 被折叠。
  - [x] `fugue admin routes explain` 的 route binding 表显示 path prefix。
  - [x] CLI 相关 OpenAPI 派生产物和 `fugue-web` contract snapshot 已同步。

- [ ] **阶段：支持 `secrets`。**  
  把真实 secret 值、`from_env`、`from_secret`、`generate` 收敛到统一模型里，同时实现默认脱敏导出。
  - [ ] 设计顶层 `secrets` schema，区分明文配置、敏感值、引用和生成规则。
  - [ ] 支持 `value`、`from_env`、`from_secret`、`generate`、`encoding`、`length`。
  - [ ] 导入时把 `secrets` 编译到 app files / env / secret storage 的正式通道。
  - [ ] 导出时默认脱敏，私密导出才保留真实值。
  - [ ] 兼容 `generated_env`，提供迁移提示而不是立即删除旧字段。

- [ ] **阶段：补 `project`、`intent`、`observability`。**  
  让 manifest 能表达项目身份、预算、区域偏好、DataOcean 之类的业务观测配置。
  - [ ] `project` 支持 name、description、默认 runtime / region 策略。
  - [ ] `intent` 支持 availability、region preference、budget、合规偏好。
  - [ ] `observability` 支持 DataOcean 接入、日志策略、指标开关和事件采集配置。
  - [ ] 导入器能把这些字段映射到 project / app / integration 的期望状态。
  - [ ] 前端在项目设置里展示这些字段，并能参与 diff。

- [ ] **阶段：补 `release`、预检、影子流量、权重切流和回滚。**  
  让 `fugue.yaml` 不只是“怎么跑”，还包括“怎么安全发布”。
  - [ ] `release.preflight` 表达部署前检查、健康门禁和阻断条件。
  - [ ] `release.shadow` 表达影子流量、比较窗口和采样策略。
  - [ ] `release.canary` / route weights 表达渐进式切流。
  - [ ] `release.rollback` 表达自动回滚条件和人工确认策略。
  - [ ] Route 生成器能把 release 策略和 entrypoint route 解耦编译。

- [ ] **阶段：做 `fugue.yaml` 的复制 / 导出 / Diff。**  
  前端直接从当前项目状态生成可复制 manifest，并提供安全导出和私密导出两种版本。
  - [ ] 从 project、apps、routes、domains、backing services、storage、env、observability 组装当前期望状态。
  - [ ] 安全导出：默认脱敏，适合提交 Git。
  - [ ] 私密导出：保留真实 secret，适合用户本地迁移或私密上传。
  - [ ] Legacy 导出：只导出 `version: 1` 可识别子集。
  - [ ] Diff：比较当前 live desired state 与给定 `fugue.yaml`，突出 route、secret、storage、release 差异。
  - [ ] CLI 补 `fugue project export` / `fugue app export` 或等价命令，输出安全 / 私密 / legacy 模式。

- [ ] **阶段：做 v1 -> v2 转换器。**  
  让老 manifest 可以一键升级，不要求用户手写迁移。
  - [ ] 把 `public: true` / `primary_service` 转换成默认 `domains` + `entrypoints`。
  - [ ] 把 `generated_env` 转换成 `secrets.generate`。
  - [ ] 把 service-level `env_file` 保留为兼容字段或转换为明确的 import-only 输入。
  - [ ] 输出迁移报告，说明无法无损转换的字段。
  - [ ] CLI 补 `fugue yaml convert --to-version 2` 或等价命令。

- [ ] **阶段：最后再收紧旧字段。**  
  等 v2 稳定以后，再逐步收敛 `public`、`generated_env`、`env_file` 这些历史兼容入口。
  - [ ] 为旧字段增加 warning 和替代建议。
  - [ ] 文档明确 v1 shim 的支持窗口。
  - [ ] 新项目默认生成 `version: 2`。
  - [ ] 控制台新建 / 导出默认使用 v2，legacy 只作为显式选项。

## 结论

`fugue.yaml` 最终应该是 Fugue 的项目合同。

`domains` 应该只承担 host / TLS / 归属；HTTP 路由应该是独立入口表。  
secret 值可以存在，但要走独立安全通道。  
前端一键复制当前项目配置也应该成为核心能力，而不是附加功能。
