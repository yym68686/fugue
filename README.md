# Fugue

[中文 README](README.zh-CN.md)

Fugue is a multi-tenant k3s control plane MVP for:

- tenant and project isolation
- API-key based access control
- shared managed runtime inside your k3s cluster
- attached user-owned nodes via reusable node key + agent
- async deploy, scale, and migrate operations
- GitHub public repo import for static sites with automatic default hostname
- audit events for control-plane actions

> Fugue 的本意是古典乐中严密、精巧的“赋格”曲，词根代表着“转移与遁走”。
> 我的系统 `fugue.pro` 就像是在服务器集群上演奏赋格：当流量来袭，它能像增加交响乐声部一样自动扩容；当节点宕机，它能像音符游走一样实现毫秒级的自动转移。
> 它把混乱、复杂的底层服务器运维，变成了一场严密、全自动、永不停歇的优雅编排。

## What is implemented in this repository

- `fugue-api`: northbound REST API
- `fugue-controller`: async operation reconciler for the managed runtime
- `fugue-agent`: attached runtime agent for user-owned VPS
- PostgreSQL-backed relational state store
- `ManagedApp` CRD plus operator-style reconcile for managed apps, with Deployments, Services, and Secrets derived from Kubernetes custom resources
- managed app observed state written back to `ManagedApp.status`, with API reads preferring Kubernetes-observed runtime state over optimistic database status
- internal registry flow for imported app images
- background commit polling for imported GitHub apps, with automatic rebuild/redeploy and rollout completion only after the new revision is ready
- Helm chart for installing the core control plane on k3s

## Current MVP constraints

- The core control plane now stores state in PostgreSQL tables and uses `LISTEN/NOTIFY` to wake the controller when new operations arrive.
- The Helm chart now deploys `fugue-api` and `fugue-controller` as separate Deployments, defaults both to `replicaCount=2`, and enables controller leader election so API and controller can scale independently.
- The bundled install path still keeps PostgreSQL, the internal registry, and other stateful pieces inside the cluster with `hostPath` storage, so it is still an opinionated MVP deployment rather than a fully externalized production topology.

## Repository layout

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

## Local development

```bash
make test
make build
```

Build only the CLI:

```bash
make build-cli
./bin/fugue deploy --help
```

The `build-cli` GitHub Actions workflow now compiles `fugue` binaries for Linux and macOS whenever matching changes are pushed to `main`.

Run the API and controller in separate terminals:

```bash
export FUGUE_BOOTSTRAP_ADMIN_KEY='fugue_bootstrap_admin_local'
make run-api
```

```bash
make run-controller
```

## Deployment

See [docs/deploy.md](docs/deploy.md).

## One-command install for 3 VPS

If you already have SSH aliases `gcp1`, `gcp2`, and `gcp3`, and each remote user is either `root` or has passwordless `sudo`, you can install the current all-in-one MVP with:

```bash
FUGUE_DOMAIN=<your-fugue-api-domain> ./scripts/install_fugue_ha.sh
```

This installer:

- builds `fugue-api` and `fugue-controller` images locally
- creates a 3-node k3s HA cluster on `gcp1/gcp2/gcp3`
- imports the images into each node's `containerd`
- installs the Helm chart with separate `fugue-api` and `fugue-controller` Deployments
- defaults both API and controller to 2 replicas, with controller leader election enabled
- exposes the Fugue API through a cluster `NodePort` Service
- optionally configures Caddy on `gcp1` as the HTTPS edge that proxies to that `NodePort`

The generated kubeconfig and bootstrap key are written into `.dist/fugue-install/`.
