# Fugue

[English README](README.md)

Fugue 是一个面向多租户场景的 k3s 控制平面 MVP，核心目标是：

- 租户与项目隔离
- 基于 API key 的访问控制
- 在你的 k3s 集群内提供共享托管运行时
- 通过可复用 node key + agent 接入用户自有 VPS
- 异步部署、扩容、迁移操作
- 从 GitHub 公共仓库导入静态站点，并自动分配默认域名
- 为控制面动作保留审计事件

> Fugue 的本意是古典乐中严密、精巧的“赋格”曲，词根代表着“转移与遁走”。
> 我的系统 `fugue.pro` 就像是在服务器集群上演奏赋格：当流量来袭，它能像增加交响乐声部一样自动扩容；当节点宕机，它能像音符游走一样实现毫秒级的自动转移。
> 它把混乱、复杂的底层服务器运维，变成了一场严密、全自动、永不停歇的优雅编排。

## 仓库当前实现

- `fugue-api`：北向 REST API
- `fugue-controller`：托管运行时的异步操作协调器
- `fugue-agent`：用户自有 VPS 的附加运行时 agent
- 基于 PostgreSQL 关系表的状态存储，并支持从旧 `fugue_state` / `store.json` 自动导入
- `ManagedApp` CRD + operator 风格 reconcile：controller 把托管 app 的期望状态写入 Kubernetes，自定义资源再负责收敛 Deployment / Service / Secret
- 托管 app 的观测状态写回 `ManagedApp.status`，API 读取时优先以 Kubernetes 观测态覆盖数据库里的乐观状态
- 导入 GitHub 项目后的内置镜像仓库推送链路
- GitHub 导入 app 的后台 commit 轮询：检测到上游分支更新后自动排入 rebuild / deploy，并等待 rollout ready 后再完成替换
- 用于在 k3s 上安装控制面的 Helm Chart

## 当前 MVP 限制

- 核心控制面现在已经切到 PostgreSQL 关系表，并使用 `LISTEN/NOTIFY` 在新 operation 到达时唤醒 controller
- 第一次以新的 PostgreSQL 关系表模式启动时，如果检测到旧的 `fugue_state` 行或 `/var/lib/fugue/store.json`，Fugue 会自动导入历史状态
- Helm Chart 现在会把 `fugue-api` 和 `fugue-controller` 部署成独立的 Deployment，默认都使用 `replicaCount=2`，并为 controller 开启 leader election，因此 API 和 controller 可以独立扩缩
- 当前附带的一键安装路径仍把 PostgreSQL、内置 registry 和其他有状态组件放在集群内，并使用 `hostPath` 持久化，所以它仍是偏 MVP 的部署形态，还不是完全外部化的生产拓扑

## 托管 API

设置你的 HTTPS API 入口：

```bash
export FUGUE_BASE_URL="https://<your-fugue-api-domain>"
```

健康检查：

```bash
curl -sS "${FUGUE_BASE_URL}/healthz"
```

期望返回：

```json
{"status":"ok"}
```

## 当前托管控制面的能力

已经可用：

- 多租户 tenant / project / app / runtime / backing-service / operation / audit-event API
- bootstrap admin 全平台管理流
- 带 scope 的租户级 API key，以及 patch / rotate / disable / enable / delete 生命周期 API
- 可复用的租户级 node key，用于一条命令接管 VPS，并继续保留给旧 agent 的一次性 enroll token
- runtime 资源视图与真实 cluster node 视图，并支持 runtime sharing grant 与托管运行时 pool 控制
- 一个内置共享托管运行时：`runtime_managed_shared`
- 通过 node bootstrap、cluster join 与 `fugue-agent` 接入外部节点
- 异步 app 部署、扩容、停用、迁移、删除
- 自动分配 Fugue 托管路由，并提供路由可用性检查与修改 API
- 自定义域名 claim / list / verify / delete API，以及供 edge 使用的 TLS ask、域名清单同步、TLS 状态回传 API
- `POST /v1/apps/import-github`：导入 GitHub 公共或私有仓库，支持幂等键、`auto / static-site / dockerfile / buildpacks / nixpacks` 构建策略，以及 `fugue.yaml` / Compose 拓扑导入
- `POST /v1/apps/import-image`：导入已有 Docker / OCI 镜像；`POST /v1/apps/import-upload`：上传 `.tgz` 源码包为新 app 或已有 app 重新导入
- `POST /v1/apps/{id}/rebuild`：对 GitHub / upload / image 来源的 app 原地重建
- GitHub 导入 app 的自动后台更新：controller 会轮询上游分支最新 commit，发现变化后自动触发重建，并以零不可用滚动更新等待新版本 ready 后再替换旧副本
- `GET/PATCH /v1/apps/{id}/env`、`GET/PUT/DELETE /v1/apps/{id}/files`、live `/filesystem/*`、`POST /v1/apps/{id}/restart`：查看和修改 app 配置
- `GET/POST/DELETE /v1/backing-services`、`GET/POST/DELETE /v1/apps/{id}/bindings`，以及托管 Postgres 的 binding env 注入
- 构建日志 / 运行日志快照与 SSE 流式 API
- `DELETE /v1/tenants/{id}`：平台管理员删除 tenant，并返回尽力清理 namespace 的结果
- `GET /install/join-cluster.sh`、`POST /v1/nodes/join-cluster`、`POST /v1/nodes/join-cluster/env`，以及内部 `/v1/nodes/join-cluster/cleanup`：在开启 cluster join 时提供一条命令接管节点
- runtime-agent 拉模式：legacy enroll、heartbeat、拉任务、回传任务完成状态
- 控制面审计日志

尚未实现：

- 通用的 runtime 元数据 update 或 runtime delete API
- 除 route / domain / env / files / bindings / import / rebuild 之外，更通用的 app 元数据 patch API
- backing-service update API，或超出 managed / external Postgres 之外的服务类型
- 类似 kpack 的 buildpacks operator 集成
- HPA / VPA 等自动扩缩容策略
- 调度策略、租户配额、计费或付费逻辑

## 鉴权模型

所有已鉴权请求都使用：

```bash
-H "Authorization: Bearer <token>"
```

当前有 4 类凭证：

- Bootstrap admin key：全平台访问能力，可创建 tenant、跨租户查看资源
- Tenant API key：绑定到单个 tenant；除非带有 `platform.admin`，否则只能访问自己的 tenant
- Node key：租户级 VPS 接管凭证，可重复用于多台机器注册
- Runtime key：只供 `fugue-agent` 使用，不能访问北向租户 / 管理 API

统一错误格式：

```json
{"error":"..."}
```

## Scope 一览

租户 API key 可以带这些 scope：

| Scope | 能力 |
| --- | --- |
| `project.write` | 创建 / 更新 / 删除项目，也可创建或删除 backing service |
| `apikey.write` | 创建 / 更新 / rotate / disable / enable / delete 租户 API key |
| `runtime.attach` | 创建 node key 与外部 runtime 接入凭证 |
| `runtime.write` | 直接创建 runtime，并管理 runtime sharing |
| `app.write` | 创建 app，以及大多数 app 侧配置 / 路由 / 域名修改 |
| `app.deploy` | 创建 deploy / import / rebuild / restart 操作，并执行 service bind / unbind |
| `app.scale` | 创建 scale 操作 |
| `app.disable` | 在不授予广义 `app.scale` 的情况下停用 app |
| `app.migrate` | 创建 migrate 操作 |
| `app.delete` | 在不授予广义 `app.write` 的情况下删除 app |
| `platform.admin` | 平台管理员行为 |

说明：

- 即使是 `GET` 列表或详情接口，也必须携带有效 bearer token
- 所有 create 类接口返回的 `secret` 只会展示一次，请自行保存

## API 参考

### 公共端点

| Method | Path | 鉴权 | 说明 |
| --- | --- | --- | --- |
| `GET` | `/healthz` | 无 | 控制面健康检查 |
| `GET` | `/readyz` | 无 | API 可接收请求时返回 `200`，进入优雅退出阶段时返回 `503` |
| `GET` | `/install/join-cluster.sh` | 无 | 在 cluster join 已配置时返回一键接入集群的辅助脚本 |
| `GET` | `/v1/source-uploads/{id}/archive` | query 里的 download token | 返回已保存的 `.tgz` 上传归档，供内部工具或受控下载链路使用 |
| `POST` | `/v1/nodes/bootstrap` | 无 | 用可复用 node key 换取单机 runtime key |
| `POST` | `/v1/nodes/join-cluster` | 无 | 用可复用 node key 换取 runtime 记录和一份 k3s join 计划 |
| `POST` | `/v1/nodes/join-cluster/env` | 无 | `form` 版本，返回 shell 可直接 `eval` 的 `FUGUE_JOIN_*` 变量 |
| `POST` | `/v1/nodes/join-cluster/cleanup` | 无 | join 脚本内部使用的辅助端点，用于节点重接入后清理过期 cluster-node 记录 |

### Edge 集成端点

这些端点服务于 HTTPS edge / TLS 自动化流程，使用 query string 里的共享 token，而不是 bearer auth。

| Method | Path | 鉴权 | 说明 |
| --- | --- | --- | --- |
| `GET` | `/v1/edge/tls/ask` | edge token | 按需 TLS ask hook；当 DNS 已正确时会自动把 pending 域名升级为 verified |
| `GET` | `/v1/edge/domains` | edge token | 列出已经 verified、应被 edge 挂载的自定义域名 |
| `POST` | `/v1/edge/domains/tls-report` | edge token | 回传某个 verified 自定义域名的 `pending` / `ready` / `error` TLS 状态 |

### 兼容保留端点

这些端点仍然存在，供旧客户端兼容使用；新接入请尽量不要依赖它们。

| Method | Path | 鉴权 | 说明 |
| --- | --- | --- | --- |
| `POST` | `/v1/agent/enroll` | 无 | 旧的一次性 enroll-token 流程；新接入应优先使用 `/v1/nodes/bootstrap` 或 cluster join |
| `GET` | `/v1/nodes` | 任意 API 凭证 | 已废弃的兼容 runtime 视图；新接入请改用 `/v1/runtimes` 与 `/v1/cluster/nodes` |
| `GET` | `/v1/nodes/{id}` | 任意 API 凭证 | 已废弃的兼容 runtime 详情视图 |

`POST /v1/nodes/bootstrap` 请求体：

```json
{
  "node_key": "<fugue_nk_...>",
  "machine_name": "alicehk2",
  "machine_fingerprint": "6d6e7b1d9c...",
  "endpoint": "https://tenant-vps-1.example.com",
  "labels": {
    "region": "ap-east-1",
    "provider": "gcp"
  }
}
```

`node_name` 和 `machine_name` 都可省略。如果你使用 Fugue 的一键接入脚本且不传 `FUGUE_NODE_NAME`，脚本会默认取 VPS 主机名。`machine_fingerprint` 也可省略，但生产环境里应该保证它对同一台机器稳定不变，这样机器重复接入时会更新同一条 runtime 记录，而不是制造重复记录。

`POST /v1/nodes/join-cluster` 使用与 `POST /v1/nodes/bootstrap` 相同的 JSON 请求体，但返回值里除了创建出来的 `node`，还会带一份 `join` 计划，包含 k3s server URL、token、labels、taints、runtime id、registry endpoint，以及可选的 mesh 参数，供安装脚本直接使用。

`POST /v1/nodes/join-cluster/env` 接收等价的 `application/x-www-form-urlencoded` 字段（`node_key`、`node_name`、`runtime_name`、`machine_name`、`machine_fingerprint`、`endpoint`、`labels`），并返回 shell 引用安全的 `FUGUE_JOIN_*` 变量，适合你自己拼装安装器。

`POST /v1/nodes/join-cluster/cleanup` 接收 `application/x-www-form-urlencoded` 字段（`node_key`、`machine_fingerprint`、`current_node_name`），并返回 shell 引用安全的 `FUGUE_JOIN_CLEANUP_*` 变量，用于清理与同一机器指纹匹配、但已经过期的 cluster-node 记录。

`GET /install/join-cluster.sh` 会返回 Fugue 的一键接入脚本。这个脚本会调用 `/v1/nodes/join-cluster/env`，写入 k3s agent 配置，附带处理 registry / mesh 参数，并在成功重接入后调用 `/v1/nodes/join-cluster/cleanup` 清理陈旧节点记录。

`GET /v1/source-uploads/{id}/archive` 需要在 query string 里提供 `download_token`，主要供内部 builder 或其他受控下载流程拉取此前上传的源码归档。

兼容说明：`POST /v1/agent/enroll` 仍支持一次性 enroll token。它的请求体与 `/v1/nodes/bootstrap` 基本一致，只是把 `node_key` 换成了 `enroll_token`。新的接入流程应优先使用 node bootstrap 或 cluster join。

### 平台与租户端点

| Method | Path | 所需 scope | 说明 |
| --- | --- | --- | --- |
| `GET` | `/v1/tenants` | 任意 API 凭证 | 平台管理员可见全部；租户 key 只能看自己 |
| `POST` | `/v1/tenants` | `platform.admin` | 创建 tenant |
| `DELETE` | `/v1/tenants/{id}` | `platform.admin` | 删除 tenant，并返回 namespace / 节点的尽力清理结果 |
| `GET` | `/v1/projects` | 任意 API 凭证 | 平台管理员应传 `tenant_id` 查询参数 |
| `POST` | `/v1/projects` | `project.write` | 租户 key 可不传 `tenant_id` |
| `PATCH` | `/v1/projects/{id}` | `project.write` | 更新 project 的 name 和 / 或 description |
| `DELETE` | `/v1/projects/{id}` | `project.write` | 删除 project；要求其中的 live app 和 backing service 已经清理掉 |
| `GET` | `/v1/api-keys` | 任意 API 凭证 | 列出可见 API key，密钥部分会脱敏 |
| `POST` | `/v1/api-keys` | `apikey.write` | 非管理员 key 不能签发自己没有的 scope |
| `PATCH` | `/v1/api-keys/{id}` | `apikey.write` | 更新 label 和 / 或 scopes |
| `POST` | `/v1/api-keys/{id}/rotate` | `apikey.write` | 旋转密钥，并可选更新 label / scopes；新 secret 只返回一次 |
| `POST` | `/v1/api-keys/{id}/disable` | `apikey.write` | 立即禁用一把 key |
| `POST` | `/v1/api-keys/{id}/enable` | `apikey.write` | 重新启用已禁用的 key |
| `DELETE` | `/v1/api-keys/{id}` | `apikey.write` | 永久撤销一把 key |
| `GET` | `/v1/node-keys` | 任意 API 凭证 | 列出可见 node key，密钥部分会脱敏 |
| `POST` | `/v1/node-keys` | `runtime.attach` | 创建可复用 tenant node key |
| `GET` | `/v1/node-keys/{id}/usages` | 任意 API 凭证 | 查看某把 node key 实际被哪些 runtime 使用 |
| `POST` | `/v1/node-keys/{id}/revoke` | `runtime.attach` | 撤销 node key，之后不能再注册新机器 |
| `GET` | `/v1/cluster/nodes` | 任意 API 凭证 | 列出真实 Kubernetes 节点；租户只会看到属于自己的 cluster 节点 |
| `GET` | `/v1/runtimes` | 任意 API 凭证 | 列出当前可见的 Fugue runtime，并带上 access / pool mode 与合并后的机器身份字段 |
| `POST` | `/v1/runtimes` | `runtime.write` | 手动创建 runtime；`managed-shared` 仅限平台管理员，`managed-owned` 必须通过节点接入流创建 |
| `GET` | `/v1/runtimes/{id}` | 任意 API 凭证 | 租户 key 可以看到自己租户的、被 grant 的、或 `platform-shared` 的 runtime |
| `GET` | `/v1/runtimes/{id}/sharing` | runtime 所属租户的 API 凭证 | 查看某个自有 runtime 的 sharing grants |
| `POST` | `/v1/runtimes/{id}/sharing/grants` | `runtime.write` + 所属租户 | 向另一个 tenant 授权某个 private runtime 的可见性 |
| `DELETE` | `/v1/runtimes/{id}/sharing/grants/{tenant_id}` | `runtime.write` + 所属租户 | 撤销某个 runtime sharing grant |
| `POST` | `/v1/runtimes/{id}/sharing/mode` | `runtime.write` + 所属租户 | 把 runtime 切到 `private` 或 `platform-shared`；后者还需要 `platform.admin` |
| `POST` | `/v1/runtimes/{id}/pool-mode` | `platform.admin` | 把 `managed-owned` runtime 切到 `dedicated` 或 `internal-shared` pool 行为 |
| `GET` | `/v1/runtimes/enroll-tokens` | 任意 API 凭证 | 平台管理员应传 `tenant_id` |
| `POST` | `/v1/runtimes/enroll-tokens` | `runtime.attach` | 创建 legacy 一次性 enroll token |
| `GET` | `/v1/backing-services` | 任意 API 凭证 | 列出当前可见的 backing service |
| `POST` | `/v1/backing-services` | `app.write` 或 `project.write` | 创建 backing service，目前主要是托管 Postgres |
| `GET` | `/v1/backing-services/{id}` | 任意 API 凭证 | 在可见范围内查看 backing service 详情 |
| `DELETE` | `/v1/backing-services/{id}` | `app.write` 或 `project.write` | 删除 backing service |
| `GET` | `/v1/apps` | 任意 API 凭证 | 列出可见 app |
| `POST` | `/v1/apps` | `app.write` | 创建 app 元数据与期望 spec；如果已配置 app base domain，还会自动分配 Fugue 托管路由 |
| `POST` | `/v1/apps/import-github` | `app.write` + `app.deploy` | 导入 GitHub 公共 / 私有仓库，可展开 `fugue.yaml` 或 Compose 多服务拓扑，分配默认域名，并支持 `Idempotency-Key` |
| `POST` | `/v1/apps/import-image` | `app.write` + `app.deploy` | 导入一个已有镜像引用并排入部署 |
| `POST` | `/v1/apps/import-upload` | 新 app 需要 `app.write` + `app.deploy`；传 `app_id` 时只需 `app.deploy` | 上传 `.tgz` 源码包，并创建或重新导入 app |
| `GET` | `/v1/apps/{id}` | 任意 API 凭证 | 查看 app 详情 |
| `GET` | `/v1/apps/{id}/route/availability` | 任意 API 凭证 | 校验 app base domain 下的 Fugue 托管主机名是否可用 |
| `PATCH` | `/v1/apps/{id}/route` | `app.write` | 修改 Fugue 托管路由主机名 |
| `GET` | `/v1/apps/{id}/domains` | 任意 API 凭证 | 列出 app 已 claim 的自定义域名及其验证 / TLS 状态 |
| `GET` | `/v1/apps/{id}/domains/availability` | 任意 API 凭证 | 检查自定义域名是否可 claim |
| `POST` | `/v1/apps/{id}/domains` | `app.write` | claim 或重新检查一个自定义域名 |
| `POST` | `/v1/apps/{id}/domains/verify` | `app.write` | 强制重新做一次 DNS 校验 |
| `DELETE` | `/v1/apps/{id}/domains` | `app.write` | 通过 `hostname` 查询参数删除一个已 claim 的自定义域名 |
| `GET` | `/v1/apps/{id}/bindings` | 任意 API 凭证 | 返回 app 到 service 的 binding，以及对应的 backing service |
| `POST` | `/v1/apps/{id}/bindings` | `app.write` 或 `app.deploy` | 创建 binding，并排入 deploy operation |
| `DELETE` | `/v1/apps/{id}/bindings/{binding_id}` | `app.write` 或 `app.deploy` | 删除 binding，并排入 deploy operation |
| `GET` | `/v1/apps/{id}/build-logs` | 任意 API 凭证 | 查看最近一次导入/构建日志，也支持指定 `operation_id` |
| `GET` | `/v1/apps/{id}/build-logs/stream` | 任意 API 凭证 | 以 Server-Sent Events 流式返回构建日志和 operation 状态 |
| `GET` | `/v1/apps/{id}/runtime-logs` | 任意 API 凭证 | 查看 `app` 或 `postgres` 的 Kubernetes Pod 日志 |
| `GET` | `/v1/apps/{id}/runtime-logs/stream` | 任意 API 凭证 | 以 Server-Sent Events 流式返回运行日志，并支持断线续传 cursor |
| `GET` | `/v1/apps/{id}/env` | 任意 API 凭证 | 返回合并后的运行时环境变量，包括 binding 注入的变量 |
| `PATCH` | `/v1/apps/{id}/env` | `app.write` 或 `app.deploy` | 当环境变量变化时排入 deploy operation |
| `GET` | `/v1/apps/{id}/files` | 任意 API 凭证 | 返回 `spec.files` 里的期望文件集合 |
| `PUT` | `/v1/apps/{id}/files` | `app.write` 或 `app.deploy` | upsert 期望文件，并在变化时排入 deploy operation |
| `DELETE` | `/v1/apps/{id}/files` | `app.write` 或 `app.deploy` | 通过重复 `path` 查询参数删除文件，并排入 deploy operation |
| `GET` | `/v1/apps/{id}/filesystem/tree` | 任意 API 凭证 | 列出 app 持久 workspace volume 里的 live 目录项，默认根目录是 `/workspace` |
| `GET` | `/v1/apps/{id}/filesystem/file` | 任意 API 凭证 | 读取 app 持久 workspace volume 里的一个 live 文件 |
| `PUT` | `/v1/apps/{id}/filesystem/file` | `app.write` 或 `app.deploy` | 在 app 持久 workspace volume 内创建或覆盖一个 live 文件 |
| `POST` | `/v1/apps/{id}/filesystem/directory` | `app.write` 或 `app.deploy` | 在 app 持久 workspace volume 内创建一个 live 目录 |
| `DELETE` | `/v1/apps/{id}/filesystem` | `app.write` 或 `app.deploy` | 在 app 持久 workspace volume 内删除 live 文件或目录 |
| `POST` | `/v1/apps/{id}/rebuild` | `app.deploy` | 对 GitHub / image / upload 来源的 app 原地重建；若配置了 workspace 则刷新 reset token |
| `POST` | `/v1/apps/{id}/deploy` | `app.deploy` | 创建异步 deploy 操作 |
| `POST` | `/v1/apps/{id}/restart` | `app.deploy` | 生成新的 restart token 并排入 deploy operation；disabled app 不能 restart，且持久 workspace 会被保留 |
| `POST` | `/v1/apps/{id}/scale` | `app.scale` | 创建异步 scale 操作；`replicas` 可以是 `0` |
| `POST` | `/v1/apps/{id}/disable` | `app.scale` 或 `app.disable` | 幂等地把 app 缩到 `0` |
| `POST` | `/v1/apps/{id}/migrate` | `app.migrate` | 创建异步 migrate 操作 |
| `DELETE` | `/v1/apps/{id}` | `app.write` 或 `app.delete` | 创建异步 delete 操作，并把 app 从可见列表中移除 |
| `GET` | `/v1/operations` | 任意 API 凭证 | 查看当前租户可见的操作列表 |
| `GET` | `/v1/operations/{id}` | 任意 API 凭证 | 查看操作详情 |
| `GET` | `/v1/audit-events` | 任意 API 凭证 | 按时间倒序返回审计事件 |

关键请求体示例：

资源视图语义：

- `/v1/runtimes`：Fugue 的部署目标清单。接入 VPS 后的 `machine_name`、`connection_mode`、`cluster_node_name`、fingerprint，以及 sharing / pool 状态都合并到这里。
- `/v1/cluster/nodes`：直接来自 Kubernetes API 的真实集群节点清单。
- `/v1/node-keys/{id}/usages`：一把可复用 node key 到实际 runtime 使用记录的映射。
- `/v1/nodes`：仅为旧客户端保留的兼容 runtime 视图；新接入建议改用 `/v1/runtimes` 和 `/v1/cluster/nodes`。

`POST /v1/tenants`

```json
{
  "name": "tenant-a"
}
```

`POST /v1/projects`

```json
{
  "tenant_id": "tenant_xxx",
  "name": "default",
  "description": "default project"
}
```

`PATCH /v1/projects/{id}`

```json
{
  "name": "production",
  "description": "shared production workloads"
}
```

`DELETE /v1/projects/{id}`

无需请求体。

行为说明：

- 需要 `project.write`
- 当 project 下仍有 live app 或 backing service 时会返回 `409 Conflict`
- 删除成功后，project 记录会被彻底移除

`POST /v1/api-keys`

```json
{
  "tenant_id": "tenant_xxx",
  "label": "tenant-admin",
  "scopes": [
    "project.write",
    "apikey.write",
    "runtime.attach",
    "runtime.write",
    "app.write",
    "app.deploy",
    "app.scale",
    "app.disable",
    "app.migrate",
    "app.delete"
  ]
}
```

`PATCH /v1/api-keys/{id}`

```json
{
  "label": "preview-ops",
  "scopes": [
    "app.write",
    "app.deploy"
  ]
}
```

`POST /v1/api-keys/{id}/rotate`

请求体可省略。如果传入，则字段与 `PATCH /v1/api-keys/{id}` 相同，也就是 `label` / `scopes`，并会返回新的 `secret`。

`POST /v1/api-keys/{id}/disable`、`POST /v1/api-keys/{id}/enable`、`DELETE /v1/api-keys/{id}`

无需请求体。

行为说明：

- 所有 API key 生命周期操作都需要 `apikey.write`
- rotate 会立即使旧 secret 失效，并只返回一次新的 secret
- disable / enable 会立即影响该 key 的鉴权行为
- delete 会永久撤销该 key，并把它从后续列表结果中移除

`POST /v1/node-keys`

```json
{
  "tenant_id": "tenant_xxx"
}
```

`label` 可省略，默认值为 `default`。
如果你用的是租户 API key，那么请求体本身也可以省略；直接发一个空的 `POST` 就会为当前租户创建默认的可复用 node key。

`POST /v1/runtimes`

```json
{
  "tenant_id": "tenant_xxx",
  "name": "manual-runtime-1",
  "type": "external-owned",
  "endpoint": "https://runtime.example.com",
  "labels": {
    "region": "asia-east1"
  }
}
```

`POST /v1/runtimes/enroll-tokens`

```json
{
  "tenant_id": "tenant_xxx",
  "label": "tenant-vps-1",
  "ttl_seconds": 3600
}
```

`POST /v1/runtimes/{id}/sharing/grants`

```json
{
  "tenant_id": "tenant_yyy"
}
```

`POST /v1/runtimes/{id}/sharing/mode`

```json
{
  "access_mode": "private"
}
```

或者，在所属租户 key 同时带有 `platform.admin` 时：

```json
{
  "access_mode": "platform-shared"
}
```

`POST /v1/runtimes/{id}/pool-mode`

```json
{
  "pool_mode": "internal-shared"
}
```

行为说明：

- sharing 相关写操作要求调用方来自 runtime 所属租户；grant / mode 修改还需要 `runtime.write`
- `platform-shared` 只有在调用方同时带有 `platform.admin` 时才允许
- pool mode 仅平台管理员可用，并且只适用于 `managed-owned` runtime
- 如果该 `managed-owned` runtime 已经对应某个 Kubernetes node，切换 pool mode 时还会同步调整调度使用的 node labels / taints

`POST /v1/apps`

```json
{
  "tenant_id": "tenant_xxx",
  "project_id": "project_xxx",
  "name": "nginx-demo",
  "description": "demo app",
  "spec": {
    "image": "nginx:1.27",
    "command": [],
    "args": [],
    "env": {
      "ENV": "prod"
    },
    "ports": [80],
    "replicas": 1,
    "runtime_id": "runtime_managed_shared",
    "workspace": {
      "mount_path": "/workspace"
    }
  }
}
```

`spec.workspace` 是可选项。配置后，Fugue 会为 app 挂一个可写的持久 workspace volume，默认挂载到 `/workspace`，并为该 app 开启 live `/filesystem/*` 接口。当前这项能力只支持 `managed-owned` runtime，因为底层使用的是节点本地 `hostPath` 存储。

当控制面配置了 `appBaseDomain` 时，`POST /v1/apps` 还会自动给 app 分配一个 Fugue 托管路由。

`POST /v1/apps/import-github`

请求头可选：

```bash
Idempotency-Key: import-<unique-key>
```

```json
{
  "tenant_id": "tenant_xxx",
  "repo_url": "https://github.com/example/static-site",
  "branch": "main",
  "build_strategy": "auto",
  "source_dir": "dist",
  "name": "marketing-site",
  "description": "imported from github",
  "runtime_id": "runtime_managed_shared",
  "replicas": 1,
  "service_port": 3000
}
```

当前 GitHub 导入行为：

- 支持 GitHub 公共仓库与私有仓库
- `repo_visibility` 可选；当它是 `private` 时，必须同时提供 `repo_auth_token`
- `project_id` 可省略；如果不传，Fugue 会复用当前租户的 `default` 项目，不存在就自动创建
- 也可以传 `project` 代替 `project_id`，在导入时内联创建 project
- `build_strategy` 可省略，默认是 `auto`
- `auto` 会先尝试 `fugue.yaml`，再尝试 Compose 文件，最后才回退到单应用检测管线
- 如果检测到 `fugue.yaml` 或 Compose，响应仍会返回主 `app` + `operation`，但还会额外带上 `apps`、`operations`，以及 `fugue_manifest` 或 `compose_stack` 元数据
- 在拓扑导入之外，`auto` 当前按这个顺序判断：`Dockerfile` -> 已准备好的静态站 -> 对受支持项目优先 `buildpacks` -> `nixpacks`
- `static-site` 要求仓库里已经存在 `index.html`，位置可以在根目录、`dist/`、`build/`、`public/` 或 `site/`
- `buildpacks` 使用 Paketo builders，适合常见的 Node.js / Python / Go / Java / Ruby / PHP / .NET 仓库
- `nixpacks` 是当前的免配置应用构建器，主要覆盖常见的 Node.js、Python、Go 等项目
- Dockerfile 导入支持 `dockerfile_path` 与 `build_context_dir`
- `service_port` 可省略；如果不传，Fugue 会使用检测到的端口或该构建策略的默认端口
- Git submodule 默认会递归拉取
- Fugue 会按项目类型选择：静态目录打包成 Caddy 镜像、直接使用 Dockerfile、走 Buildpacks/Paketo，或用 Nixpacks 生成构建上下文，再推送到内置 registry
- 返回的 app 会带一个在配置好的 app base domain 下生成的默认公网域名
- 如果同一个 `Idempotency-Key` 配合同一份请求体被重复提交，Fugue 会返回原来的 app + operation，而不会再创建一个重复 app
- 如果同一个 `Idempotency-Key` 被用于不同的请求体，Fugue 会返回 `409 Conflict`

`POST /v1/apps/import-image`

```json
{
  "tenant_id": "tenant_xxx",
  "project_id": "project_xxx",
  "image_ref": "ghcr.io/example/demo:1.2.3",
  "name": "demo-image",
  "description": "imported from image",
  "runtime_id": "runtime_managed_shared",
  "replicas": 1,
  "service_port": 8080,
  "env": {
    "APP_ENV": "production"
  }
}
```

行为说明：

- 直接导入一个现成的 Docker / OCI 镜像引用，不需要 Git clone 或源码上传
- 如果不传 `name`，会默认从 `image_ref` 推导 app 名称
- 支持与 GitHub 导入相同的 `project_id` / 内联 `project` 解析逻辑
- 会排入一条 `import` operation，由 controller 继续完成规范化与部署

`POST /v1/apps/import-upload`

Multipart 表单字段：

- `request`：一个 JSON 对象，字段与 GitHub 导入基本一致，并额外支持可选的 `app_id`
- `archive`：一个 `.tgz` 或 `.tar.gz` 源码归档

示例：

```bash
curl -sS "${FUGUE_BASE_URL}/v1/apps/import-upload" \
  -H "Authorization: Bearer ${FUGUE_TENANT_TOKEN}" \
  -F 'request={"project_id":"project_xxx","name":"demo-upload","build_strategy":"static-site"};type=application/json' \
  -F 'archive=@./demo-upload.tgz;type=application/gzip'
```

行为说明：

- 归档必须是 gzip 压缩 tar，目前大小上限是 `128 MiB`
- 不传 `app_id` 时，Fugue 会创建一个新的 imported app，并排入 `import` operation
- 传入 `app_id` 时，Fugue 会保存这次上传的归档，并对已有 app 重新排入 import；这条路径只需要 `app.deploy`
- 上传归档会被保存为 `upload` source，因此后续 `rebuild` 可以直接复用已保存归档，不必再次上传

`GET /v1/source-uploads/{id}/archive`

行为说明：

- 必须通过 query string 提供 `download_token=<upload-download-token>`
- 主要给内部工具或受控下载流程使用，用来直接拿到上传时保存的原始归档字节
- 返回原始归档内容，类型通常是 `application/gzip`

`POST /v1/apps/{id}/rebuild`

```json
{}
```

可选覆盖参数：

```json
{
  "branch": "main",
  "image_ref": "ghcr.io/example/demo:1.2.4",
  "source_dir": "apps/web",
  "dockerfile_path": "deploy/Dockerfile",
  "build_context_dir": "apps/web",
  "repo_auth_token": "<private-rebuild-token>"
}
```

当前 rebuild 行为：

- 适用于最初由 `github-public`、`github-private`、`docker-image`、或 `upload` 来源创建的 app
- 对 GitHub 来源，会从保存的仓库 URL 与分支拉取最新代码；`branch` 与 `repo_auth_token` 覆盖只对 GitHub 来源生效
- 对 `docker-image`，会基于已保存的 `image_ref` 重新排队导入，也可以通过 `image_ref` 临时覆盖
- 对 `upload`，复用保存下来的 `upload_id` 归档，并带着原有构建元数据重新排入 import
- `source_dir`、`dockerfile_path`、`build_context_dir` 会覆盖下一次导入使用的源码布局
- 对 GitHub 导入会递归拉取 Git submodule
- 按保存下来的构建策略（`static-site`、`dockerfile`、`buildpacks` 或 `nixpacks`）重新构建镜像并推送到内置 registry
- 保持原有 app id、project 与公网域名不变，然后把更新后的来源定义重新排入一条新的 `import` operation

`GET /v1/apps/{id}/route/availability`

查询参数：

- `hostname` 必填；既可以是 app base domain 下的完整主机名，也可以只传想要的 label

行为说明：

- 只校验位于已配置 app base domain 下的 Fugue 托管主机名
- 返回规范化后的 `hostname`、`public_url`、`valid`、`available`、`current`，以及失败时的 `reason`

`PATCH /v1/apps/{id}/route`

```json
{
  "hostname": "fresh-name"
}
```

行为说明：

- 需要 `app.write`
- 可以传裸 label，也可以传 app base domain 下的完整主机名
- 如果目标路由已经属于当前 app，会返回 `already_current: true`

`GET /v1/apps/{id}/domains`、`GET /v1/apps/{id}/domains/availability`、`POST /v1/apps/{id}/domains`、`POST /v1/apps/{id}/domains/verify`、`DELETE /v1/apps/{id}/domains`

create / verify 的请求体：

```json
{
  "hostname": "www.example.com"
}
```

删除时使用 query string 里的 `hostname`，例如 `/v1/apps/<app-id>/domains?hostname=www.example.com`。

行为说明：

- 自定义域名与 Fugue 托管路由是两套独立能力，且不能位于平台托管的 app base domain 下
- `POST /v1/apps/{id}/domains` 会 claim 或重新检查一个 hostname，并返回当前 `domain`、`availability`、`already_current`
- 当 hostname 的 CNAME 指向 Fugue 目标，或被 flatten 到与 Fugue 目标相同的 IP 集合时，就会校验成功；否则会保持 `pending`，并通过 `last_message` 返回 DNS 指引
- `POST /v1/apps/{id}/domains/verify` 用于对已 claim 的 hostname 强制再做一次 DNS 检查
- edge 自动化会通过 `/v1/edge/domains/tls-report` 回填 TLS 状态，因此每个 domain 记录还会带有 `tls_status`、`tls_last_message`、以及 `verified_at` / `tls_ready_at` 等时间字段

`GET /v1/apps/{id}/build-logs`

查询参数：

- `operation_id` 可选；默认读取这个 app 最新一次 `import` 操作
- `tail_lines` 可选；默认 `200`，最大 `5000`

行为说明：

- 优先读取最近的 Kubernetes builder Job 日志
- 如果 Job 已被清理，则回退到保存下来的 operation 错误/结果文本

`GET /v1/apps/{id}/runtime-logs`

查询参数：

- `component` 可选；默认是 `app`，也可以传 `postgres`
- `pod` 可选；限制到某一个 pod 名称
- `tail_lines` 可选；默认 `200`，最大 `5000`
- `previous` 可选；传 `true` 时读取容器上一次重启前的日志

行为说明：

- 仅适用于 managed runtime
- 直接读取租户 namespace 里的 Pod 日志

`GET /v1/apps/{id}/build-logs/stream`

查询参数：

- `operation_id` 可选；默认读取这个 app 最新一次 `import` 操作
- `tail_lines` 可选；默认 `200`，最大 `5000`，仅在没有 cursor 时用于首屏回放
- `follow` 可选；默认 `true`
- `cursor` 可选；不透明重连 cursor，也可以通过 `Last-Event-ID` 头传入

行为说明：

- 返回 `text/event-stream`
- 会发送 `ready`、`status`、`log`、`heartbeat`、`warning`、`end` 事件
- `status` 会带上 `operation_status`、`job_name`、`build_strategy` 和最终 result/error 文本
- `log` 事件按 pod/container 单行输出，每个事件 id 都可作为重连 cursor
- 非 follow 快照完成，或构建 operation 进入终态时，会发送 `end`

`GET /v1/apps/{id}/runtime-logs/stream`

查询参数：

- `component` 可选；默认是 `app`，也可以传 `postgres`
- `pod` 可选；限制到某一个 pod 名称
- `tail_lines` 可选；默认 `200`，最大 `5000`，仅在没有 cursor 时用于首屏回放
- `previous` 可选；传 `true` 时流式返回 previous container logs，且该流是有限快照
- `follow` 可选；默认 `true`，但 `previous=true` 时默认变为 `false`
- `cursor` 可选；不透明重连 cursor，也可以通过 `Last-Event-ID` 头传入

行为说明：

- 返回 `text/event-stream`
- 会发送 `ready`、`state`、`log`、`heartbeat`、`warning`、`end` 事件
- `state` 表示当前已接入的 pod 集合
- `log` 事件按 pod/container 单行输出，每个事件 id 都可作为重连 cursor
- `follow=false` 时，回放完当前快照后会以 `end.reason = "snapshot_complete"` 结束

`DELETE /v1/tenants/{id}`

无需请求体。

行为说明：

- 需要 platform-admin 凭证
- 会从 Fugue 状态里删除该 tenant，并返回一个 `cleanup` 对象，其中包含 namespace 名称、是否已发起 namespace 删除、owned node 数量以及告警信息
- namespace 清理是尽力而为；managed-owned 节点可能仍需要你到租户 VPS 上手动卸载 k3s agent

`GET/POST/DELETE /v1/backing-services` 与 `GET/POST/DELETE /v1/apps/{id}/bindings`

行为说明：

- `POST /v1/backing-services` 允许直接创建 backing service；当前北向写路径主要是托管 Postgres
- `DELETE /v1/backing-services/{id}` 用于删除 backing service 记录
- `GET /v1/apps/{id}/bindings` 同时返回 binding 记录和它们引用的 backing service 对象
- `POST /v1/apps/{id}/bindings` 与 `DELETE /v1/apps/{id}/bindings/{binding_id}` 都会排入 deploy operation，确保 app 的有效 env 与 binding 期望一致

`POST /v1/backing-services`

```json
{
  "project_id": "project_xxx",
  "name": "main-db",
  "description": "primary postgres",
  "spec": {
    "postgres": {
      "database": "app",
      "user": "app",
      "password": "secret"
    }
  }
}
```

`POST /v1/apps/{id}/bindings`

```json
{
  "service_id": "svc_xxx",
  "alias": "db"
}
```

`GET /v1/apps/{id}/env`

行为说明：

- 返回 app 实际可见的合并环境变量视图，也就是 binding 注入的 env 与 `spec.env` 叠加后的结果

`PATCH /v1/apps/{id}/env`

```json
{
  "set": {
    "LOG_LEVEL": "debug"
  },
  "delete": [
    "OLD_FLAG"
  ]
}
```

行为说明：

- 除平台管理员外，需要 `app.write` 或 `app.deploy`
- 当最终生效的 env 发生变化时，会排入一个 `deploy` operation
- 如果请求后的 env 与当前期望 spec 完全一致，则返回 `already_current: true`，不会新建 operation

`GET /v1/apps/{id}/files`

行为说明：

- 返回当前保存在 `spec.files` 里的期望文件集合

`PUT /v1/apps/{id}/files`

```json
{
  "files": [
    {
      "path": "/etc/caddy/Caddyfile",
      "content": ":8080\nrespond \"hello\"",
      "mode": 420
    }
  ]
}
```

行为说明：

- 按 `path` 对文件进行 upsert
- 除平台管理员外，需要 `app.write` 或 `app.deploy`
- 当文件内容发生变化时，会排入一个 `deploy` operation

`DELETE /v1/apps/{id}/files`

无需请求体。通过重复的 `path` 查询参数指定要删除的文件，例如 `/v1/apps/<app-id>/files?path=/etc/caddy/Caddyfile&path=/app/.env`。

行为说明：

- 除平台管理员外，需要 `app.write` 或 `app.deploy`
- 只要至少删除了一个文件，就会排入一个 `deploy` operation

`GET /v1/apps/{id}/filesystem/tree`

查询参数：

- `path` 默认为 workspace 根目录
- `component` 当前只支持 `app`
- `depth` 当前只支持 `1`

行为说明：

- 返回的是持久 app workspace 里的 live 内容，不是 `spec.files`
- 只允许访问配置好的 workspace 根目录之内的路径
- rebuild reset 使用的 workspace 元数据目录不会出现在 API 返回里

`GET /v1/apps/{id}/filesystem/file`

查询参数：

- `path` 必填，且必须位于 app workspace 根目录内
- `max_bytes` 默认是 `262144`
- `component` 当前只支持 `app`

行为说明：

- 读取持久 app workspace 里的一个 live 文件
- 对合法 UTF-8 内容返回 `encoding: "utf-8"`；其他内容返回 `encoding: "base64"`
- 如果文件大于 `max_bytes`，会返回 `truncated: true`

`PUT /v1/apps/{id}/filesystem/file`

```json
{
  "path": "/workspace/notes/hello.txt",
  "content": "hello",
  "encoding": "utf-8",
  "mode": 420,
  "mkdir_parents": true
}
```

行为说明：

- 直接写入 live 持久 workspace，不会创建 deploy operation
- 只允许操作配置好的 workspace 根目录内的路径
- `encoding` 支持 `utf-8`（默认）或 `base64`

`POST /v1/apps/{id}/filesystem/directory`

```json
{
  "path": "/workspace/assets",
  "mode": 493,
  "parents": true
}
```

行为说明：

- 在持久 app workspace 里创建一个 live 目录
- 只允许操作配置好的 workspace 根目录内的路径

`DELETE /v1/apps/{id}/filesystem`

无需请求体。通过 `path=/workspace/...` 指定路径，也可以传 `recursive=true`。

行为说明：

- 从持久 app workspace 里删除一个 live 文件或目录
- 这个接口不允许删除 workspace 根目录本身

`POST /v1/apps/{id}/deploy`

```json
{}
```

也可以在部署时覆盖 app spec：

```json
{
  "spec": {
    "image": "nginx:1.27",
    "ports": [80],
    "replicas": 2,
    "runtime_id": "runtime_managed_shared"
  }
}
```

`POST /v1/apps/{id}/restart`

无需请求体。

行为说明：

- 需要 `app.deploy`
- 只有当 app 当前 `replicas > 0` 时才能执行
- 会生成一个新的 `restart_token`，并排入 `deploy` operation
- 如果配置了 `spec.workspace`，重启会保留这个持久 workspace volume

`POST /v1/apps/{id}/rebuild`

行为说明：

- 需要 `app.deploy`
- 按 app 当前保存的 GitHub / image / upload source 定义重新构建
- 如果配置了持久 workspace，会刷新 `spec.workspace.reset_token`，让下一次 rollout 只在这一次重建时清空并重建 workspace 内容

`POST /v1/apps/{id}/scale`

```json
{
  "replicas": 3
}
```

`POST /v1/apps/{id}/disable`

```json
{}
```

行为说明：

- 需要 `app.scale` 或 `app.disable`
- 如果 app 已经完全缩到 `0`，会返回 `already_disabled: true`

`POST /v1/apps/{id}/migrate`

```json
{
  "target_runtime_id": "runtime_xxx"
}
```

`DELETE /v1/apps/{id}`

无需请求体。

### Runtime-agent 端点

这些端点仅供 `fugue-agent` 使用，要求 bearer token 为 runtime key。

| Method | Path | 用途 |
| --- | --- | --- |
| `POST` | `/v1/agent/heartbeat` | 刷新 runtime 存活状态，并可更新 endpoint |
| `GET` | `/v1/agent/operations` | 获取分配给该 runtime 的待执行任务 |
| `POST` | `/v1/agent/operations/{id}/complete` | 标记任务完成并回传结果元数据 |

`POST /v1/agent/heartbeat`

```json
{
  "endpoint": "https://tenant-vps-1.example.com"
}
```

`POST /v1/agent/operations/{id}/complete`

```json
{
  "manifest_path": "/var/lib/fugue/manifests/app-123.yaml",
  "message": "applied successfully"
}
```

## 托管 API 快速开始

设置公共变量：

```bash
export FUGUE_BASE_URL="https://<your-fugue-api-domain>"
export FUGUE_BOOTSTRAP_KEY="<bootstrap-admin-key>"
```

创建 tenant：

```bash
curl -sS "${FUGUE_BASE_URL}/v1/tenants" \
  -H "Authorization: Bearer ${FUGUE_BOOTSTRAP_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo-tenant"}'
```

创建项目：

```bash
curl -sS "${FUGUE_BASE_URL}/v1/projects" \
  -H "Authorization: Bearer ${FUGUE_BOOTSTRAP_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"<tenant-id>","name":"demo-project","description":"default project"}'
```

创建一把 tenant admin API key：

```bash
curl -sS "${FUGUE_BASE_URL}/v1/api-keys" \
  -H "Authorization: Bearer ${FUGUE_BOOTSTRAP_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id":"<tenant-id>",
    "label":"tenant-admin",
    "scopes":[
      "project.write",
      "apikey.write",
      "runtime.attach",
      "runtime.write",
      "app.write",
      "app.deploy",
      "app.scale",
      "app.disable",
      "app.migrate",
      "app.delete"
    ]
  }'
```

把返回里的 `secret` 保存为租户 token：

```bash
export FUGUE_TENANT_TOKEN="<tenant-api-key-secret>"
```

创建一把可复用 node key：

```bash
curl -sS "${FUGUE_BASE_URL}/v1/node-keys" \
  -H "Authorization: Bearer ${FUGUE_TENANT_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{}'
```

把返回里的 `secret` 作为用户 VPS 上的接管凭证：

```bash
export FUGUE_NODE_KEY="<node-key-secret>"
```

## 仓库结构

```text
cmd/fugue
cmd/fugue-api
cmd/fugue-controller
cmd/fugue-agent
internal/api
internal/auth
internal/config
internal/controller
internal/runtime
internal/store
deploy/helm/fugue
docs/deploy.md
```

## 本地开发

```bash
make test
make build
```

只编译 CLI：

```bash
make build-cli
./bin/fugue deploy --help
```

现在 push 到 `main` 后，`build-cli` GitHub Actions workflow 也会自动编译 Linux 和 macOS 的 `fugue` CLI 二进制 artifact。

分别在两个终端启动 API 与 controller：

```bash
export FUGUE_BOOTSTRAP_ADMIN_KEY='fugue_bootstrap_admin_local'
make run-api
```

```bash
make run-controller
```

## 端到端快速上手

下面这段可以从“创建新用户”一直跑到“部署第一个 GitHub 项目”。示例依赖 `jq`。

本地开发环境：

```bash
export FUGUE_BASE_URL="http://127.0.0.1:8080"
export FUGUE_BOOTSTRAP_KEY="fugue_bootstrap_admin_local"
```

线上环境：

```bash
export FUGUE_BASE_URL="https://<your-fugue-api-domain>"
export FUGUE_BOOTSTRAP_KEY="<your-bootstrap-admin-key>"
```

用 CLI 直接部署当前目录：

```bash
export FUGUE_BASE_URL="https://<your-fugue-api-domain>"
export FUGUE_TOKEN="<tenant-api-key-or-bootstrap-key>"
./bin/fugue deploy --name cerebr --project default
```

如果你使用的是 bootstrap admin key，而且当前可见多个 tenant，还需要额外传 `--tenant` 或 `--tenant-id`。

完整流程：

```bash
set -euo pipefail

TENANT_NAME="demo-tenant"
TENANT_ADMIN_LABEL="demo-tenant-admin"
PROJECT_NAME="default"
PROJECT_DESC="default project"

REPO_URL="https://github.com/yym68686/Cerebr"
BRANCH="main"
SOURCE_DIR=""
APP_NAME="cerebr"

TENANT_JSON=$(
  curl -fsS "${FUGUE_BASE_URL}/v1/tenants" \
    -H "Authorization: Bearer ${FUGUE_BOOTSTRAP_KEY}" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc --arg name "${TENANT_NAME}" '{name:$name}')"
)
TENANT_ID=$(echo "${TENANT_JSON}" | jq -r '.tenant.id')

TENANT_KEY_JSON=$(
  curl -fsS "${FUGUE_BASE_URL}/v1/api-keys" \
    -H "Authorization: Bearer ${FUGUE_BOOTSTRAP_KEY}" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc \
      --arg tenant_id "${TENANT_ID}" \
      --arg label "${TENANT_ADMIN_LABEL}" \
      '{
        tenant_id:$tenant_id,
        label:$label,
        scopes:[
          "project.write",
          "apikey.write",
          "runtime.attach",
          "runtime.write",
          "app.write",
          "app.deploy",
          "app.scale",
          "app.disable",
          "app.migrate",
          "app.delete"
        ]
      }')"
)
FUGUE_TENANT_TOKEN=$(echo "${TENANT_KEY_JSON}" | jq -r '.secret')

PROJECT_JSON=$(
  curl -fsS "${FUGUE_BASE_URL}/v1/projects" \
    -H "Authorization: Bearer ${FUGUE_TENANT_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc \
      --arg name "${PROJECT_NAME}" \
      --arg description "${PROJECT_DESC}" \
      '{name:$name,description:$description}')"
)
PROJECT_ID=$(echo "${PROJECT_JSON}" | jq -r '.project.id')

IMPORT_JSON=$(
  curl -fsS "${FUGUE_BASE_URL}/v1/apps/import-github" \
    -H "Authorization: Bearer ${FUGUE_TENANT_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc \
      --arg project_id "${PROJECT_ID}" \
      --arg repo_url "${REPO_URL}" \
      --arg branch "${BRANCH}" \
      --arg source_dir "${SOURCE_DIR}" \
      --arg name "${APP_NAME}" \
      '{
        project_id:$project_id,
        repo_url:$repo_url,
        branch:$branch,
        source_dir:$source_dir,
        name:$name,
        runtime_id:"runtime_managed_shared",
        replicas:1
      }')"
)

APP_ID=$(echo "${IMPORT_JSON}" | jq -r '.app.id')
OP_ID=$(echo "${IMPORT_JSON}" | jq -r '.operation.id')
APP_URL=$(echo "${IMPORT_JSON}" | jq -r '.app.route.public_url')

while true; do
  OP_JSON=$(
    curl -fsS "${FUGUE_BASE_URL}/v1/operations/${OP_ID}" \
      -H "Authorization: Bearer ${FUGUE_TENANT_TOKEN}"
  )
  STATUS=$(echo "${OP_JSON}" | jq -r '.operation.status')
  echo "operation_status=${STATUS}"
  if [ "${STATUS}" = "completed" ]; then
    break
  fi
  if [ "${STATUS}" = "failed" ]; then
    echo "${OP_JSON}" | jq .
    exit 1
  fi
  sleep 2
done

echo "TENANT_ID=${TENANT_ID}"
echo "PROJECT_ID=${PROJECT_ID}"
echo "APP_ID=${APP_ID}"
echo "APP_URL=${APP_URL}"
```

对已经导入的 GitHub app 拉取最新代码并原地重部署：

```bash
curl -sS "${FUGUE_BASE_URL}/v1/apps/<app-id>/rebuild" \
  -H "Authorization: Bearer ${FUGUE_TENANT_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{}'
```

## 部署

部署说明见 [docs/deploy.md](docs/deploy.md)。

## 三台 VPS 一键安装

如果你已经有 `gcp1`、`gcp2`、`gcp3` 三个 SSH 别名，并且远端用户要么是 `root`，要么拥有免密 `sudo`，可以直接这样安装当前 all-in-one MVP：

```bash
FUGUE_DOMAIN=<your-fugue-api-domain> ./scripts/install_fugue_ha.sh
```

安装脚本会：

- 在本地构建 `fugue-api` 和 `fugue-controller` 镜像
- 在 `gcp1/gcp2/gcp3` 上创建 3 节点 k3s HA 集群
- 把镜像导入到每个节点的 `containerd`
- 安装 Helm Chart，并把 `fugue-api` 与 `fugue-controller` 部署成独立 Deployment
- 默认让 API 和 controller 都以 2 个副本运行，并为 controller 开启 leader election
- 通过集群级 `NodePort` Service 暴露 Fugue API
- 可选地在 `gcp1` 上配置 Caddy，作为 HTTPS 边缘入口代理到该 `NodePort`

生成出来的 kubeconfig 与 bootstrap key 会写入 `.dist/fugue-install/`。
