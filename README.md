# Fugue

Fugue is a multi-tenant k3s control plane MVP for:

- tenant and project isolation
- API-key based access control
- shared managed runtime inside your k3s cluster
- attached user-owned runtimes via enroll token + agent
- async deploy, scale, and migrate operations
- audit events for control-plane actions

## What is implemented in this repository

- `fugue-api`: northbound REST API
- `fugue-controller`: async operation reconciler for the managed runtime
- `fugue-agent`: attached runtime agent for user-owned VPS
- file-backed state store with cross-process file locking
- manifest rendering for Deployments and Services
- in-cluster apply for the managed runtime via Kubernetes API
- Helm chart for installing the core control plane on k3s

## Current MVP constraints

- The core control plane uses a file-backed store, so the Helm chart runs `fugue-api` and `fugue-controller` in the same Pod and keeps `replicaCount=1`.
- This is enough for a functional v0, but not for horizontal scaling.
- The next production step is to swap the file store for PostgreSQL and split API/controller into separate Deployments.

## Repository layout

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

## Local development

```bash
make test
make build
```

Run the API and controller in separate terminals:

```bash
export FUGUE_BOOTSTRAP_ADMIN_KEY='fugue_bootstrap_admin_local'
make run-api
```

```bash
make run-controller
```

## Quick bootstrap

Create a tenant:

```bash
curl -sS http://127.0.0.1:8080/v1/tenants \
  -H 'Authorization: Bearer fugue_bootstrap_admin_local' \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo-tenant"}'
```

Create a project:

```bash
curl -sS http://127.0.0.1:8080/v1/projects \
  -H 'Authorization: Bearer fugue_bootstrap_admin_local' \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"<tenant-id>","name":"demo-project","description":"default project"}'
```

Create an app:

```bash
curl -sS http://127.0.0.1:8080/v1/apps \
  -H 'Authorization: Bearer fugue_bootstrap_admin_local' \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id":"<tenant-id>",
    "project_id":"<project-id>",
    "name":"nginx-demo",
    "description":"demo",
    "spec":{
      "image":"nginx:1.27",
      "ports":[80],
      "replicas":1,
      "runtime_id":"runtime_managed_shared"
    }
  }'
```

Deploy the app:

```bash
curl -sS http://127.0.0.1:8080/v1/apps/<app-id>/deploy \
  -H 'Authorization: Bearer fugue_bootstrap_admin_local' \
  -H 'Content-Type: application/json' \
  -d '{}'
```

## Deployment

See [docs/deploy.md](/Users/yanyuming/Downloads/GitHub/fugue/docs/deploy.md).

## One-command install for 3 VPS

If you already have SSH aliases `gcp1`, `gcp2`, and `gcp3`, and each remote user is either `root` or has passwordless `sudo`, you can install the current all-in-one MVP with:

```bash
./scripts/install_fugue_ha.sh
```

This installer:

- builds `fugue-api` and `fugue-controller` images locally
- creates a 3-node k3s HA cluster on `gcp1/gcp2/gcp3`
- imports the images into each node's `containerd`
- installs the Helm chart on the cluster
- pins the single Fugue control-plane Pod to `gcp1`
- exposes the Fugue API through a `NodePort`

The generated kubeconfig and bootstrap key are written into `.dist/fugue-install/`.
