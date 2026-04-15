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

先在“访问密钥 / Access keys”页面创建或复制一个访问密钥：

- Fugue Cloud：`https://fugue.pro/app/api-keys`
- 自托管：你的 Fugue Web 地址加上 `/app/api-keys`，例如 `https://app.example.com/app/api-keys`

普通部署优先使用 tenant 级访问密钥。`fugue admin ...`、跨 tenant 排障、产品层管理员诊断这类场景，再使用 platform-admin key 或 bootstrap key。

拿到密钥后即可开始：

```bash
export FUGUE_API_KEY=<copied-access-key>
fugue deploy .
fugue app ls
```

后续如果你想查看当前 CLI 版本或原地升级：

```bash
fugue version --check-latest
fugue upgrade
```

如果你用的是自托管控制面，只需要先设置一次地址：

```bash
export FUGUE_BASE_URL=https://api.example.com
export FUGUE_WEB_BASE_URL=https://app.example.com
export FUGUE_API_KEY=<copied-access-key>
fugue app ls
```

如果你想让 Codex 直接接手部署，把密钥导出到 Codex 会使用的 shell 里，然后给它一句明确指令，例如：

```text
使用 fugue CLI 和当前的 FUGUE_API_KEY 部署这个项目。
```

常用流程：

- `fugue deploy github owner/repo --branch main`
- `fugue deploy github owner/repo --service-env-file gateway=.env.gateway --service-env-file runtime=.env.runtime`
- `fugue deploy github https://github.com/example/app --private --repo-token $GITHUB_TOKEN`
- `fugue deploy image nginx:1.27`
- `fugue app create my-app --github owner/repo --branch main`
- `fugue app status my-app`
- `fugue app overview my-app`
- `fugue app env ls my-app`
- `fugue app fs ls my-app / --source live`
- `fugue app db query my-app --sql "select * from gateway_request_logs order by created_at desc limit 50"`
- `fugue app logs query my-app --table gateway_request_logs --since 1h --match status=500`
- `fugue app logs pods my-app`
- `fugue app request my-app GET /admin/requests --query page=2 --query status=500 --header-from-env X-Service-Key=SERVICE_KEY`
- `fugue app logs runtime my-app --follow`
- `fugue app service attach my-app postgres`
- `fugue app failover status my-app`
- `fugue app failover run my-app --to runtime-b`
- `fugue runtime enroll create edge-a`
- `fugue runtime doctor shared`
- `fugue project images usage marketing`
- `fugue operation ls --app my-app`
- `fugue operation show op_123 --show-secrets`
- `fugue api request GET /v1/apps`
- `fugue diagnose timing -- app overview my-app`
- `fugue admin cluster status`
- `fugue admin cluster pods --namespace kube-system`
- `fugue admin cluster events --namespace kube-system --limit 20`
- `fugue admin cluster logs --namespace kube-system --pod coredns-abc --container coredns --tail 200`
- `fugue admin cluster exec --namespace kube-system --pod coredns-abc -- cat /etc/resolv.conf`
- `fugue admin cluster exec --namespace app-demo --pod postgres-0 --retries 4 --timeout 2m -- sh -lc "psql -c 'select now()'"`
- `fugue admin cluster workload show kube-system deployment coredns`
- `fugue admin cluster rollout status kube-system deployment coredns`
- `fugue admin cluster dns resolve api.github.com --server 10.43.0.10`
- `fugue admin cluster net connect api.github.com:443`
- `fugue admin cluster net websocket my-app --path "/socket.io/?EIO=4&transport=websocket"`
- `fugue admin cluster tls probe 104.18.32.47:443 --server-name api.github.com`
- `fugue admin users ls`
- `fugue admin users show user@example.com`
- `fugue web diagnose admin-users`
- `fugue web diagnose /api/fugue/console/pages/api-keys --cookie 'fugue_session=...'`

`fugue app overview` 和 `fugue operation ls/show/watch` 在 JSON 输出里默认会脱敏 env 值、密码、repo token 和 secret 文件内容。只有在确实需要原始值排障时才显式传 `--show-secrets`。

`fugue app fs` 现在同时支持持久化存储根目录和 live runtime filesystem。传 `--source persistent` 时会限制在 workspace / persistent storage 挂载点内；传 `--source live` 时会直接查看运行中容器里的 `/`、`/app`、`/tmp`、`/etc` 等路径。

`fugue app db query` 现在可以直接基于应用的有效 PostgreSQL 连接执行只读 SQL，不需要先 `cluster exec` 进 Postgres pod。它适合直接查业务表，例如 `users`、`gateway_request_logs`、请求审计表，并且默认会限制返回行数，避免日常排障时一次拉太多数据。

`fugue app logs query` 是面向业务日志表的语义化封装。如果日志本身存放在应用数据库里，不需要每次都手写 SQL；你可以直接指定表名，加上 `--since` / `--until` 和字段过滤，让 CLI 自动生成只读查询。

`fugue app logs pods` 会展示当前 pod 组以及最近的 ReplicaSet rollout 上下文，包括哪一个 revision 替换了旧 pod 组。这个命令适合在 `app overview` 已经切到新 revision 之后，继续查看旧 rollout 的上下文。

`fugue app request` 允许你从控制面侧直接请求应用自己的内部 HTTP 路由，包括那些依赖 app env 里 service key 的管理接口。通过 `--header-from-env Header=ENV_KEY` 可以直接从应用的有效 env 填充认证头，不用把 secret 再复制到本地 shell。

`fugue app env ls` 的 text 输出现在会直接渲染成带 `source`、`ref` 和覆盖信息的表格，正常终端使用时不再必须依赖 `--json`。

`fugue api request` 会直接展示任意控制面接口的 status、headers、server-timing、body 和传输层耗时。`fugue diagnose timing -- <command...>` 则会包装任意 Fugue CLI 命令，输出它发出的每个 HTTP 请求的 DNS / connect / TLS / TTFB / total timing。

`fugue deploy github ... --service-env-file service=.env.file` 允许 topology 导入时按 service 单独注入 env 覆盖。`gateway`、`runtime`、`worker` 这类服务需要不同密钥或 feature flag 时，不必再把所有配置揉进一个共享 env 文件。

`fugue admin cluster net websocket` 会对同一个 websocket 端点做两次握手：一次直连 app 的 cluster service，一次走 app 的 public route。CLI 会把两边的状态和自动结论一起返回，所以像 `service=101 / public_route=502` 这类问题，不需要再 SSH 到节点上做 `kubectl` 对照实验。

`fugue admin cluster exec` 现在默认会对瞬时 EOF 和 stream reset 失败做重试，并暴露 `--retries`、`--retry-delay`、`--timeout` 给长耗时诊断命令使用。

已发布的 CLI 现在可以直接用 `fugue upgrade` 自升级。当前二进制如果落后于最新 GitHub Release，普通 text 模式命令也会提示你从哪个版本升级到哪个版本。若你在某个 shell 会话里不想看到这个提醒，可以设置 `FUGUE_SKIP_UPDATE_CHECK=1`。

`fugue admin users` 和 `fugue web diagnose` 下的 admin alias 读取的是和 `fugue-web` 管理员产品 UI 相同的 page snapshot 路径。使用这些命令前先设置 `FUGUE_WEB_BASE_URL`，或者显式传 `--web-base-url`。admin page snapshot 接受 bootstrap bearer 鉴权；如果你要排查 workspace 级 console page route，也可以通过 `--cookie` 传入浏览器 session cookie。

当 API 侧配置了 `FUGUE_CONTROL_PLANE_GITHUB_REPOSITORY` 后，`fugue admin cluster status` 还会附带最近一次 `deploy-control-plane` GitHub Actions workflow run，便于把 control plane 升级和当前集群状态对上。

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
