# Fugue 应用生命周期平台愿景

Fugue 不应该从“边缘节点 / Worker 运行时”这个维度去和 Cloudflare 正面竞争。

Cloudflare 强在“请求进入网络以后怎么处理”。  
Fugue 更适合强在“一个真实应用从代码、镜像、env、DB、域名、路由、发布、回滚、观测、迁移到多地运行，全部被平台掌控”。

换句话说，Fugue 的目标不是只帮用户部署一个新 app，而是让一个已有业务可以被完整理解、迁移、自治运行。

## 1. 一键迁移现有应用

这是最适合 Fugue 的杀手级能力。

目标不是把空白项目部署上去，而是：

- 输入 `docker-compose`
- 输入 `Caddy` / `Nginx` 配置
- 输入 `.env`
- 输入数据库信息

Fugue 自动生成：

- app 拓扑
- web / api / worker / db 拆分
- `path-level routing`
- env / secret 映射
- 数据库迁移计划
- DNS 切换计划
- 回滚计划
- 风险报告

这比单纯提供一个 Worker 运行时更贴近真实用户的痛点。很多人缺的不是“再来一个运行时”，而是有一堆跑在 VPS 上的服务，不敢迁移。

## 2. Stateful App Teleport

如果 Fugue 能安全迁移有状态应用，就会有很强的护城河。

例如：

- DigitalOcean VPS -> Fugue
- 香港节点 -> 美国节点
- 共享集群 -> 用户自己的机器

Fugue 迁移的不能只是容器，还要包括：

- 数据库
- volumes
- env / secrets
- 域名
- 路由
- TLS
- 健康检查
- 回滚点

真正有价值的体验是：应用可以搬家，但业务不停。

## 3. 应用拓扑作为一等对象

Cloudflare 更像“请求处理平台”。  
Fugue 应该成为“应用拓扑平台”。

一个项目不再只是一个 app，而是一张完整的系统图：

```text
Project: uni-api-web
  domain: 0-0.pro
  /        -> web
  /v1/*    -> api
  cron     -> billing worker
  db       -> postgres
  data     -> DataOcean
  secrets  -> managed
```

在这个视角里，`path-level routing` 只是平台表达应用拓扑的自然结果，不是最终目的。

用户看到的是完整系统，而不是一堆彼此割裂的容器和配置项。

## 4. 自治运维 Agent

这是 Fugue 很有想象力的方向。

Fugue 应该能回答并处理：

```text
为什么 0-0.pro 慢了？
为什么 /v1/models 500？
这次发布有没有让注册转化下降？
数据库迁移安全吗？
可以自动回滚吗？
```

进一步，Fugue 还应该能：

- 发现新版本错误率上升
- 自动比对 release
- 自动查日志、env、DB migration
- 自动回滚
- 或给出修复 PR

这不是 Cloudflare 的主战场。Cloudflare 能看请求，Fugue 可以理解应用。

## 5. Intent-Based Deployment

用户不应该总是手动选节点、runtime、replicas。

更好的方式是表达意图：

```text
这个 API 要低延迟，预算每月 $20
这个后台任务要便宜，不需要公网
这个数据库必须留在香港
这个 app 要 99.9%，允许多地副本
```

Fugue 再把意图编译成：

- placement
- replicas
- routing
- failover
- backup
- resource limits
- cost guardrail

这会比传统 PaaS 更高级。

## 6. 发布前仿真和流量影子测试

迁移和发布最怕“不知道会不会炸”。

Fugue 可以在切流前做：

- 把生产流量复制一份给新版本
- 对比响应状态、耗时、关键字段
- 不影响真实用户
- 通过后再切流

还可以叠加：

- 数据库 schema 检查
- env diff
- route diff
- secret 缺失检查
- 回滚可行性检查

这对生产用户会非常有价值。

## 7. 数据中台内建

如果 Fugue 只有部署能力，价值会比较有限。  
如果 Fugue 天然知道业务数据闭环，它就不只是 PaaS，而是应用运营平台。

它应该能天然理解：

- 这个用户从哪里来
- 访问了哪个 landing page
- 注册了吗
- 调用 API 了吗
- 充值了吗
- 哪个版本发布后转化下降了

这也是 DataOcean 这样的能力为什么重要：Fugue 不只管流量，还能管业务结果。

## 结论

最值得押注的方向，不是去拼 Worker 运行时，而是：

- 一键迁移现有应用
- 有状态应用搬家
- 应用拓扑管理
- 自治运维
- 业务数据闭环

一句话概括：

> Cloudflare 管请求，Fugue 管应用的一生。

只要 Fugue 能把“一个真实业务从 VPS 安全迁到平台，并持续帮它运行好”做到极致，它就已经是在另一个维度上建立竞争优势了。

更细的 manifest 演进路线见 [Fugue YAML 改造路线图](docs/fugue-yaml-roadmap.md)。
