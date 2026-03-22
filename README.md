# Fugue

Fugue is a multi-tenant k3s control plane MVP for:

- tenant and project isolation
- API-key based access control
- shared managed runtime inside your k3s cluster
- attached user-owned nodes via reusable node key + agent
- async deploy, scale, and migrate operations
- GitHub public repo import for static sites with automatic default hostname
- audit events for control-plane actions

## What is implemented in this repository

- `fugue-api`: northbound REST API
- `fugue-controller`: async operation reconciler for the managed runtime
- `fugue-agent`: attached runtime agent for user-owned VPS
- file-backed state store with cross-process file locking
- manifest rendering for Deployments and Services
- in-cluster apply for the managed runtime via Kubernetes API
- internal registry flow for imported app images
- Helm chart for installing the core control plane on k3s

## Current MVP constraints

- The core control plane uses a file-backed store, so the Helm chart runs `fugue-api` and `fugue-controller` in the same Pod and keeps `replicaCount=1`.
- This is enough for a functional v0, but not for horizontal scaling.
- The next production step is to swap the file store for PostgreSQL and split API/controller into separate Deployments.

## Hosted API

Set your hosted HTTPS entrypoint:

```bash
export FUGUE_BASE_URL="https://<your-fugue-api-domain>"
```

Quick health check:

```bash
curl -sS "${FUGUE_BASE_URL}/healthz"
```

Expected response:

```json
{"status":"ok"}
```

## Current hosted API capabilities

What is already usable on the deployed control plane:

- multi-tenant tenant, project, app, runtime, operation, and audit-event APIs
- bootstrap admin flow for platform-wide management
- tenant-scoped API keys with per-scope authorization
- reusable tenant-scoped node keys for one-command VPS onboarding
- one built-in managed shared runtime: `runtime_managed_shared`
- external node attachment through node bootstrap plus `fugue-agent`
- asynchronous app deploy, scale, and migrate operations
- `POST /v1/apps/import-github` for public GitHub static sites
- `POST /v1/apps/{id}/rebuild` to rebuild an imported GitHub app from the latest repo state and redeploy it
- runtime-agent pull model: enroll, heartbeat, fetch tasks, mark task complete
- audit trail for control-plane actions

What is not implemented yet:

- resource update and delete APIs
- arbitrary Dockerfile or buildpack detection for imported repositories
- autoscaling policies such as HPA/VPA
- scheduling policies, quotas, billing, or paywall logic
- horizontally scalable control plane storage

## Auth model

All authenticated requests use:

```bash
-H "Authorization: Bearer <token>"
```

There are 4 credential types:

- Bootstrap admin key: full platform access, including tenant creation and cross-tenant visibility.
- Tenant API key: scoped to one tenant; can only access that tenant's resources unless the key has `platform.admin`.
- Node key: tenant-scoped bootstrap credential that can be reused to register multiple VPS nodes.
- Runtime key: used only by `fugue-agent`; cannot call northbound tenant/admin endpoints.

Common error format:

```json
{"error":"..."}
```

## Scope reference

Tenant API keys can be minted with these scopes:

| Scope | Capability |
| --- | --- |
| `project.write` | create projects |
| `apikey.write` | create more tenant API keys |
| `runtime.attach` | create node keys and external runtime bootstrap credentials |
| `runtime.write` | create runtimes directly |
| `app.write` | create apps |
| `app.deploy` | create deploy operations |
| `app.scale` | create scale operations |
| `app.migrate` | create migrate operations |
| `platform.admin` | full platform admin behavior |

Notes:

- `GET` list and detail endpoints still require a valid bearer token, even when they do not check a dedicated write scope.
- Secrets returned by create endpoints are only shown once. Persist them on your side.

## API reference

### Public endpoints

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| `GET` | `/healthz` | none | control-plane health check |
| `POST` | `/v1/nodes/bootstrap` | none | exchanges a reusable node key for one machine-specific runtime key |
| `POST` | `/v1/agent/enroll` | none | exchanges an enroll token for a runtime record plus runtime key |

`POST /v1/nodes/bootstrap` request body:

```json
{
  "node_key": "<fugue_nk_...>",
  "node_name": "tenant-vps-1",
  "endpoint": "https://tenant-vps-1.example.com",
  "labels": {
    "region": "ap-east-1",
    "provider": "gcp"
  }
}
```

Legacy compatibility: `POST /v1/agent/enroll` still accepts one-time enroll tokens.

`POST /v1/agent/enroll` request body:

```json
{
  "enroll_token": "<fugue_enroll_...>",
  "runtime_name": "tenant-vps-1",
  "endpoint": "https://tenant-vps-1.example.com",
  "labels": {
    "region": "ap-east-1",
    "provider": "gcp"
  }
}
```

### Platform and tenant endpoints

| Method | Path | Required scope | Notes |
| --- | --- | --- | --- |
| `GET` | `/v1/tenants` | any API credential | platform admin sees all; tenant key only sees its own tenant |
| `POST` | `/v1/tenants` | `platform.admin` | create tenant |
| `GET` | `/v1/projects` | any API credential | platform admin should pass `tenant_id` query param |
| `POST` | `/v1/projects` | `project.write` | `tenant_id` optional for tenant key |
| `GET` | `/v1/api-keys` | any API credential | lists visible API keys, secrets are redacted |
| `POST` | `/v1/api-keys` | `apikey.write` | non-admin key cannot mint scopes it does not already hold |
| `GET` | `/v1/node-keys` | any API credential | lists visible node keys, secrets are redacted |
| `POST` | `/v1/node-keys` | `runtime.attach` | creates a reusable tenant node key |
| `POST` | `/v1/node-keys/{id}/revoke` | `runtime.attach` | revokes a node key so it cannot register more machines |
| `GET` | `/v1/nodes` | any API credential | lists attached external-owned nodes |
| `GET` | `/v1/nodes/{id}` | any API credential | fetch attached node detail |
| `GET` | `/v1/runtimes` | any API credential | includes managed shared runtime plus visible external runtimes |
| `POST` | `/v1/runtimes` | `runtime.write` | manual runtime creation; `managed-shared` is platform-admin only |
| `GET` | `/v1/runtimes/{id}` | any API credential | tenant key can only see shared or same-tenant runtime |
| `GET` | `/v1/runtimes/enroll-tokens` | any API credential | platform admin should pass `tenant_id` |
| `POST` | `/v1/runtimes/enroll-tokens` | `runtime.attach` | creates one-time enroll token |
| `GET` | `/v1/apps` | any API credential | lists visible apps |
| `POST` | `/v1/apps` | `app.write` | creates app metadata and desired spec |
| `POST` | `/v1/apps/import-github` | `app.write` + `app.deploy` | imports a public GitHub static site, allocates a default hostname, and queues deployment |
| `GET` | `/v1/apps/{id}` | any API credential | fetch app detail |
| `POST` | `/v1/apps/{id}/rebuild` | `app.deploy` | rebuilds a `github-public` app from the latest GitHub code and queues deployment |
| `POST` | `/v1/apps/{id}/deploy` | `app.deploy` | creates async deploy operation |
| `POST` | `/v1/apps/{id}/scale` | `app.scale` | creates async scale operation |
| `POST` | `/v1/apps/{id}/migrate` | `app.migrate` | creates async migrate operation |
| `GET` | `/v1/operations` | any API credential | lists operations for visible tenant |
| `GET` | `/v1/operations/{id}` | any API credential | fetch operation detail |
| `GET` | `/v1/audit-events` | any API credential | newest first |

Important request payloads:

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
  "tenant_id": "tenant_xxx",
  "label": "default-node-key"
}
```

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

Import behavior in the current MVP:

- only public GitHub repositories are supported
- the repository must already contain `index.html` in the root, `dist/`, `build/`, `public/`, or `site/`
- Git submodules are cloned recursively by default
- Fugue packages that directory into a Caddy-based image, pushes it into the internal registry, creates the app, and queues a deploy operation
- the returned app includes a generated public hostname under your configured app base domain

`POST /v1/apps/{id}/rebuild`

```json
{}
```

Optional override:

```json
{
  "branch": "main",
  "source_dir": "dist"
}
```

Rebuild behavior:

- only works for apps originally created from `github-public` source
- pulls the latest code from the saved repository URL and branch
- clones Git submodules recursively
- rebuilds and pushes a new image into the internal registry
- keeps the same app id, project, and public hostname, then queues a deploy operation with the new image

`POST /v1/apps/{id}/deploy`

```json
{}
```

Or override the app spec during deployment:

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

### Runtime-agent endpoints

These endpoints are for `fugue-agent` only and require a runtime key in the bearer token.

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/agent/heartbeat` | refresh runtime liveness and optionally update endpoint |
| `GET` | `/v1/agent/operations` | fetch pending tasks assigned to this runtime |
| `POST` | `/v1/agent/operations/{id}/complete` | mark a task complete and attach result metadata |

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

## Hosted API quick start

Set common variables:

```bash
export FUGUE_BASE_URL="https://<your-fugue-api-domain>"
export FUGUE_BOOTSTRAP_KEY="<bootstrap-admin-key>"
```

Create a tenant:

```bash
curl -sS "${FUGUE_BASE_URL}/v1/tenants" \
  -H "Authorization: Bearer ${FUGUE_BOOTSTRAP_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo-tenant"}'
```

Create a project:

```bash
curl -sS "${FUGUE_BASE_URL}/v1/projects" \
  -H "Authorization: Bearer ${FUGUE_BOOTSTRAP_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"<tenant-id>","name":"demo-project","description":"default project"}'
```

Create a tenant admin API key:

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

Use the returned `secret` as your tenant token:

```bash
export FUGUE_TENANT_TOKEN="<tenant-api-key-secret>"
```

Create a reusable node key:

```bash
curl -sS "${FUGUE_BASE_URL}/v1/node-keys" \
  -H "Authorization: Bearer ${FUGUE_TENANT_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"label":"default-node-key"}'
```

Use the returned `secret` as the bootstrap credential on any user VPS:

```bash
export FUGUE_NODE_KEY="<node-key-secret>"
```

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

## End-to-end quickstart

These examples use `jq`.

For local development:

```bash
export FUGUE_BASE_URL="http://127.0.0.1:8080"
export FUGUE_BOOTSTRAP_KEY="fugue_bootstrap_admin_local"
```

For a deployed control plane:

```bash
export FUGUE_BASE_URL="https://<your-fugue-api-domain>"
export FUGUE_BOOTSTRAP_KEY="<your-bootstrap-admin-key>"
```

Create a tenant, mint a tenant admin key, create a project, import the first GitHub app, and wait for deployment:

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

Rebuild an imported GitHub app from the latest code and redeploy it in place:

```bash
curl -sS "${FUGUE_BASE_URL}/v1/apps/<app-id>/rebuild" \
  -H "Authorization: Bearer ${FUGUE_TENANT_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{}'
```

## Deployment

See [docs/deploy.md](/Users/yanyuming/Downloads/GitHub/fugue/docs/deploy.md).

## One-command install for 3 VPS

If you already have SSH aliases `gcp1`, `gcp2`, and `gcp3`, and each remote user is either `root` or has passwordless `sudo`, you can install the current all-in-one MVP with:

```bash
FUGUE_DOMAIN=<your-fugue-api-domain> ./scripts/install_fugue_ha.sh
```

This installer:

- builds `fugue-api` and `fugue-controller` images locally
- creates a 3-node k3s HA cluster on `gcp1/gcp2/gcp3`
- imports the images into each node's `containerd`
- installs the Helm chart on the cluster
- pins the single Fugue control-plane Pod to `gcp1`
- exposes the Fugue API through an internal `NodePort`
- optionally configures Caddy on `gcp1` so your HTTPS API domain proxies to that internal NodePort

The generated kubeconfig and bootstrap key are written into `.dist/fugue-install/`.
