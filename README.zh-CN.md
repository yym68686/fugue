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
- 基于 PostgreSQL 关系表的状态存储
- `ManagedApp` CRD + operator 风格 reconcile：controller 把托管 app 的期望状态写入 Kubernetes，自定义资源再负责收敛 Deployment、Service 和 Secret
- 托管 app 的观测状态写回 `ManagedApp.status`，API 读取时优先以 Kubernetes 观测态覆盖数据库里的乐观状态
- 导入 GitHub 项目后的内置镜像仓库推送链路
- GitHub 导入 app 的后台 commit 轮询：检测到上游分支更新后自动排入 rebuild / deploy，并等待 rollout ready 后再完成替换
- 用于在 k3s 上安装核心控制面的 Helm Chart

## 当前 MVP 限制

- 核心控制面现在已经切到 PostgreSQL 关系表，并使用 `LISTEN/NOTIFY` 在新 operation 到达时唤醒 controller
- Helm Chart 现在会把 `fugue-api` 和 `fugue-controller` 部署成独立的 Deployment，默认都使用 `replicaCount=2`，并为 controller 开启 leader election，因此 API 和 controller 可以独立扩缩
- 当前附带的一键安装路径仍把 PostgreSQL、内置 registry 和其他有状态组件放在集群内，并使用 `hostPath` 持久化，所以它仍是偏 MVP 的部署形态，还不是完全外部化的生产拓扑

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

仅构建 CLI：

```bash
make build-cli
./bin/fugue deploy --help
```

一行安装已发布的 CLI：

macOS / Linux：

```bash
curl -fsSL https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.sh | sh
```

Windows PowerShell：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "irm https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.ps1 | iex"
```

安装脚本会从最新的 GitHub Release 下载匹配当前系统和架构的压缩包，并把 `fugue` 安装到一个可写的 bin 目录。如果你想固定版本或自定义安装目录：

```bash
curl -fsSL https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.sh | env FUGUE_VERSION=v0.1.0 FUGUE_INSTALL_DIR=$HOME/.local/bin sh
```

```powershell
$env:FUGUE_VERSION='v0.1.0'
$env:FUGUE_INSTALL_DIR="$env:LOCALAPPDATA\Programs\Fugue\bin"
irm https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.ps1 | iex
```

`build-cli` GitHub Actions workflow 会在匹配的变更推送到 `main` 后打包 Linux、macOS、Windows 的 `fugue` 压缩包；`release-cli` workflow 会在推送 `v*` tag 时把这些压缩包发布成 GitHub Release 资产。

在两个终端里分别运行 API 和 controller：

```bash
export FUGUE_BOOTSTRAP_ADMIN_KEY='fugue_bootstrap_admin_local'
make run-api
```

```bash
make run-controller
```

## 部署

部署说明见 [docs/deploy.md](docs/deploy.md)。
生产可用的 HA / DR 路径见 [docs/ha-dr.md](docs/ha-dr.md) 和 [deploy/helm/fugue/values-production-ha.yaml](deploy/helm/fugue/values-production-ha.yaml)。CLI 里也新增了 `fugue app failover`，可以直接审计哪些 app 已经具备无状态故障转移条件，哪些还被托管数据库或持久工作区阻塞。
真正的托管有状态 failover 由独立的 controller/API failover workflow 提供，见 [docs/ha-dr.md](docs/ha-dr.md)。

## 三台 VPS 一键安装

如果你已经有 `gcp1`、`gcp2`、`gcp3` 三个 SSH 别名，并且远端用户要么是 `root`，要么拥有免密 `sudo`，可以直接这样安装当前 all-in-one MVP：

```bash
FUGUE_DOMAIN=<your-fugue-api-domain> ./scripts/install_fugue_ha.sh
```

这个安装脚本会：

- 在本地构建 `fugue-api` 和 `fugue-controller` 镜像
- 在 `gcp1/gcp2/gcp3` 上创建一个 3 节点 k3s HA 集群
- 把镜像导入每个节点的 `containerd`
- 用拆分后的 `fugue-api` 和 `fugue-controller` Deployment 安装 Helm Chart
- 默认把 API 和 controller 都设为 2 个副本，并为 controller 开启 leader election
- 通过集群内 `NodePort` Service 暴露 Fugue API
- 可选地在 `gcp1` 上配置 Caddy，作为代理到该 `NodePort` 的 HTTPS 边缘入口

生成的 kubeconfig 和 bootstrap key 会写入 `.dist/fugue-install/`。
