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
- Deployment / Service 清单渲染
- 通过 Kubernetes API 在托管运行时内直接 apply
- 导入 GitHub 项目后的内置镜像仓库推送链路
- 用于在 k3s 上安装控制面的 Helm Chart

## 当前 MVP 限制

- 核心控制面现在已经切到 PostgreSQL 关系表，并使用 `LISTEN/NOTIFY` 在新 operation 到达时唤醒 controller
- 第一次以新的 PostgreSQL 关系表模式启动时，如果检测到旧的 `fugue_state` 行或 `/var/lib/fugue/store.json`，Fugue 会自动导入历史状态
- 当前 Helm Chart 仍会把 `fugue-api` 和 `fugue-controller` 跑在同一个 Pod 中，并保持 `replicaCount=1`，所以控制面仍缺少 leader election，以及 API / controller 的独立横向扩展能力

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

- 多租户 tenant / project / app / runtime / operation / audit-event API
- bootstrap admin 全平台管理流
- 带 scope 的租户级 API key
- 可复用的租户级 node key，用于一条命令接管 VPS
- 一个内置共享托管运行时：`runtime_managed_shared`
- 通过 node bootstrap + `fugue-agent` 接入外部节点
- 异步 app 部署、扩容、迁移
- `POST /v1/apps/import-github`：导入 GitHub 公共静态站点
- `POST /v1/apps/{id}/rebuild`：对已导入的 GitHub 项目拉取最新代码后重新构建并重部署
- runtime-agent 拉模式：enroll、heartbeat、拉任务、回传任务完成状态
- 控制面审计日志

尚未实现：

- 资源 update / delete API
- 任意 Dockerfile / buildpack 自动识别
- HPA / VPA 等自动扩缩容策略
- 调度策略、租户配额、计费或付费逻辑
- 带 leader election 的控制面横向扩展能力

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
| `project.write` | 创建项目 |
| `apikey.write` | 创建更多租户 API key |
| `runtime.attach` | 创建 node key 与外部 runtime 接入凭证 |
| `runtime.write` | 直接创建 runtime |
| `app.write` | 创建 app |
| `app.deploy` | 创建 deploy 操作 |
| `app.scale` | 创建 scale 操作 |
| `app.migrate` | 创建 migrate 操作 |
| `platform.admin` | 平台管理员行为 |

说明：

- 即使是 `GET` 列表或详情接口，也必须携带有效 bearer token
- 所有 create 类接口返回的 `secret` 只会展示一次，请自行保存

## API 参考

### 公共端点

| Method | Path | 鉴权 | 说明 |
| --- | --- | --- | --- |
| `GET` | `/healthz` | 无 | 控制面健康检查 |
| `POST` | `/v1/nodes/bootstrap` | 无 | 用可复用 node key 换取单机 runtime key |
| `POST` | `/v1/agent/enroll` | 无 | 用 enroll token 换取 runtime 记录与 runtime key |

`POST /v1/nodes/bootstrap` 请求体：

```json
{
  "node_key": "<fugue_nk_...>",
  "endpoint": "https://tenant-vps-1.example.com",
  "labels": {
    "region": "ap-east-1",
    "provider": "gcp"
  }
}
```

`node_name` 可省略。如果你使用 Fugue 的一键接入脚本且不传 `FUGUE_NODE_NAME`，脚本会默认取 VPS 主机名；如果你直接调 API 且不传名字，Fugue 会自动分配 `node`、`node-2` 这类名称。

兼容说明：`POST /v1/agent/enroll` 仍支持一次性 enroll token。

`POST /v1/agent/enroll` 请求体：

```json
{
  "enroll_token": "<fugue_enroll_...>",
  "endpoint": "https://tenant-vps-1.example.com",
  "labels": {
    "region": "ap-east-1",
    "provider": "gcp"
  }
}
```

`runtime_name` 可省略；省略时 Fugue 会自动分配 `node`、`node-2` 这类名称。

### 平台与租户端点

| Method | Path | 所需 scope | 说明 |
| --- | --- | --- | --- |
| `GET` | `/v1/tenants` | 任意 API 凭证 | 平台管理员可见全部；租户 key 只能看自己 |
| `POST` | `/v1/tenants` | `platform.admin` | 创建 tenant |
| `GET` | `/v1/projects` | 任意 API 凭证 | 平台管理员应传 `tenant_id` 查询参数 |
| `POST` | `/v1/projects` | `project.write` | 租户 key 可不传 `tenant_id` |
| `GET` | `/v1/api-keys` | 任意 API 凭证 | 列出可见 API key，密钥部分会脱敏 |
| `POST` | `/v1/api-keys` | `apikey.write` | 非管理员 key 不能签发自己没有的 scope |
| `GET` | `/v1/node-keys` | 任意 API 凭证 | 列出可见 node key，密钥部分会脱敏 |
| `POST` | `/v1/node-keys` | `runtime.attach` | 创建可复用 tenant node key |
| `POST` | `/v1/node-keys/{id}/revoke` | `runtime.attach` | 撤销 node key，之后不能再注册新机器 |
| `GET` | `/v1/nodes` | 任意 API 凭证 | 列出租户附加的 external-owned 节点 |
| `GET` | `/v1/nodes/{id}` | 任意 API 凭证 | 查看节点详情 |
| `GET` | `/v1/runtimes` | 任意 API 凭证 | 包含 managed shared runtime 与可见的 external runtimes |
| `POST` | `/v1/runtimes` | `runtime.write` | 手动创建 runtime；`managed-shared` 仅限平台管理员 |
| `GET` | `/v1/runtimes/{id}` | 任意 API 凭证 | 租户 key 只能看到 shared 或自己租户的 runtime |
| `GET` | `/v1/runtimes/enroll-tokens` | 任意 API 凭证 | 平台管理员应传 `tenant_id` |
| `POST` | `/v1/runtimes/enroll-tokens` | `runtime.attach` | 创建一次性 enroll token |
| `GET` | `/v1/apps` | 任意 API 凭证 | 列出可见 app |
| `POST` | `/v1/apps` | `app.write` | 创建 app 元数据与期望 spec |
| `POST` | `/v1/apps/import-github` | `app.write` + `app.deploy` | 导入 GitHub 公共静态站点，分配默认域名，并排入部署 |
| `GET` | `/v1/apps/{id}` | 任意 API 凭证 | 查看 app 详情 |
| `POST` | `/v1/apps/{id}/rebuild` | `app.deploy` | 重新拉取 `github-public` app 的最新代码，重建并排入部署 |
| `POST` | `/v1/apps/{id}/deploy` | `app.deploy` | 创建异步 deploy 操作 |
| `POST` | `/v1/apps/{id}/scale` | `app.scale` | 创建异步 scale 操作 |
| `POST` | `/v1/apps/{id}/migrate` | `app.migrate` | 创建异步 migrate 操作 |
| `GET` | `/v1/operations` | 任意 API 凭证 | 查看当前租户可见的操作列表 |
| `GET` | `/v1/operations/{id}` | 任意 API 凭证 | 查看操作详情 |
| `GET` | `/v1/audit-events` | 任意 API 凭证 | 按时间倒序返回审计事件 |

关键请求体示例：

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
    "app.migrate"
  ]
}
```

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
    "runtime_id": "runtime_managed_shared"
  }
}
```

`POST /v1/apps/import-github`

```json
{
  "tenant_id": "tenant_xxx",
  "project_id": "project_xxx",
  "repo_url": "https://github.com/example/static-site",
  "branch": "main",
  "source_dir": "dist",
  "name": "marketing-site",
  "description": "imported from github",
  "runtime_id": "runtime_managed_shared",
  "replicas": 1
}
```

当前 GitHub 导入行为：

- 仅支持 GitHub 公共仓库
- 仓库中必须已经存在 `index.html`，位置可以在根目录、`dist/`、`build/`、`public/` 或 `site/`
- Git submodule 默认会递归拉取
- Fugue 会把静态目录打包成基于 Caddy 的镜像，推送到内置 registry，创建 app，并自动排入 deploy 操作
- 返回的 app 会带一个在配置好的 app base domain 下生成的默认公网域名

`POST /v1/apps/{id}/rebuild`

```json
{}
```

可选覆盖参数：

```json
{
  "branch": "main",
  "source_dir": "dist"
}
```

当前 rebuild 行为：

- 仅适用于最初由 `github-public` 来源创建的 app
- 从保存的仓库 URL 与分支拉取最新代码
- 递归拉取 Git submodule
- 重新构建镜像并推送到内置 registry
- 保持原有 app id、project 与公网域名不变，只更新镜像与 source 元数据，然后排入 deploy 操作

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

`POST /v1/apps/{id}/scale`

```json
{
  "replicas": 3
}
```

`POST /v1/apps/{id}/migrate`

```json
{
  "target_runtime_id": "runtime_xxx"
}
```

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
      "app.migrate"
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
          "app.migrate"
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
- 在集群上安装 Helm Chart
- 把单实例 Fugue 控制面 Pod 固定到 `gcp1`
- 通过内部 `NodePort` 暴露 Fugue API
- 可选地在 `gcp1` 上配置 Caddy，把你的 HTTPS API 域名代理到该内部 NodePort

生成出来的 kubeconfig 与 bootstrap key 会写入 `.dist/fugue-install/`。
