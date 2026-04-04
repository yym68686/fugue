# Fugue

[English README](README.md)

Fugue 是一个面向 k3s 的多租户应用控制平面。它把 OpenAPI-first 的控制面 API、异步 controller 和语义化 CLI 组合在一起，用来在共享托管运行时和接入的自有运行时上部署、运维应用。

## 当前状态

- `fugue-api` 和 `fugue-controller` 已拆分为独立控制面组件，并可以独立扩缩。常规控制面部署路径使用 PostgreSQL 作为权威状态存储，新的 operation 会通过 `LISTEN/NOTIFY` 唤醒 controller。
- HTTP API 已切到 OpenAPI-first 工作流。`openapi/openapi.yaml` 是单一事实来源，生成路由由它派生，服务端会直接提供 `/openapi.yaml`、`/openapi.json` 和 `/docs`。
- CLI 已经是主操作入口：支持本地源码部署、GitHub 仓库导入、镜像直部署，以及 app / runtime / service / operation 的日常运维。
- GitHub 导入不再只覆盖公开静态站点，现已支持公开/私有仓库、自动构建策略识别（`static-site`、`dockerfile`、`buildpacks`、`nixpacks`）、`fugue.yaml` 或 Compose 的 stack 导入，以及对已跟踪仓库的后台同步。
- 连续性能力已经成为一等工作流：可以审计 failover 就绪度、配置 app / database 的 failover 目标，并对托管运行时执行 controller 驱动的 failover。
- 默认 Helm Chart 仍是偏自托管的一体化基线；生产 HA 路径则把 PostgreSQL、registry、secret 和 edge 外部化。

## 当前已支持的能力

- 多租户 tenant / project / API key / audit event，以及 platform admin 视角的控制面能力。
- `managed-shared`、`managed-owned`、`external-owned` 三类 runtime，以及通过可复用 node key + `fugue-agent` 接入的自有节点。
- 从本地上传、GitHub 仓库或容器镜像创建并部署应用。
- 异步 deploy / rebuild / scale / restart / migrate / failover / delete 操作。
- app 的 domain / route、env / config / files / workspace、运行日志 / 构建日志、operation 历史。
- backing service 和 service binding，包括托管 PostgreSQL 流程。
- 集群 inventory、app/service 当前资源使用量叠加、runtime sharing，以及控制面状态检查。

## CLI 快速开始

安装已发布的 CLI：

macOS / Linux：

```bash
curl -fsSL https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.sh | sh
```

Windows PowerShell：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "irm https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.ps1 | iex"
```

使用一个已签发的 API key 即可开始：

```bash
export FUGUE_API_KEY=<your-api-key>
fugue deploy .
fugue app ls
```

如果你用的是自托管控制面，只需要先设置一次地址：

```bash
export FUGUE_BASE_URL=https://api.example.com
export FUGUE_API_KEY=<your-api-key>
fugue app ls
```

常用流程：

- `fugue deploy github owner/repo --branch main`
- `fugue deploy github https://github.com/example/app --private --repo-token $GITHUB_TOKEN`
- `fugue deploy image nginx:1.27`
- `fugue app status my-app`
- `fugue app logs runtime my-app --follow`
- `fugue app binding bind my-app postgres`
- `fugue app continuity audit my-app`
- `fugue app failover run my-app --to runtime-b`
- `fugue operation ls --app my-app`

`build-cli` 会在 `main` 上的相关变更合入后打包 CLI 压缩包，`release-cli` 会在推送 `v*` tag 时把这些压缩包发布为 GitHub Release 资产。

## 控制面发布与部署

远端 control plane 的常规发布路径是 [`.github/workflows/deploy-control-plane.yml`](.github/workflows/deploy-control-plane.yml)。推送到 `main` 或手动触发该 workflow，它会构建并推送 `fugue-api` / `fugue-controller` 镜像，然后在自托管 runner 上执行升级。

`scripts/install_fugue_ha.sh` 只用于当前“三台 VPS 打包拓扑”的首次引导，不应用于日常 control-plane 更新。

更多部署文档：

- [一体化 / 自托管部署指南](docs/deploy.md)
- [生产 HA / DR 指南](docs/ha-dr.md)
- [默认 Helm values](deploy/helm/fugue/values.yaml)
- [生产 HA values](deploy/helm/fugue/values-production-ha.yaml)

## 本地开发

```bash
make test
make build
```

如果你只想构建 CLI：

```bash
make build-cli
./bin/fugue --help
```

如果你修改了 HTTP API 契约，先改 `openapi/openapi.yaml`，再重新生成派生产物：

```bash
make generate-openapi
```

`make test` 已经包含 OpenAPI 生成物漂移检查。

为了快速本地运行，在未设置 `FUGUE_DATABASE_URL` 时，二进制会回退到 `./data/store.json`。

在两个终端里分别运行 API 和 controller：

```bash
export FUGUE_BOOTSTRAP_ADMIN_KEY='fugue_bootstrap_admin_local'
make run-api
```

```bash
make run-controller
```

本地 API 启动后，可以直接访问 `http://127.0.0.1:8080/openapi.yaml`、`http://127.0.0.1:8080/openapi.json` 和 `http://127.0.0.1:8080/docs` 查看契约与文档。

## 仓库结构

```text
cmd/fugue                  CLI
cmd/fugue-api              API server
cmd/fugue-controller       Async controller
cmd/fugue-agent            Attached runtime agent
openapi/                   权威 API 契约
internal/api               HTTP handler 与契约输出
internal/cli               CLI 命令与交互层
internal/controller        Operation worker 与 reconcile 逻辑
internal/runtime           托管运行时渲染与应用逻辑
internal/sourceimport      源码导入与构建识别
internal/store             PostgreSQL 状态存储
deploy/helm/fugue          控制面 Helm Chart
docs/                      部署与 HA/DR 文档
```
