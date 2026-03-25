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
- PostgreSQL-backed relational state store with automatic import from legacy `fugue_state` / `store.json`
- manifest rendering for Deployments and Services
- in-cluster apply for the managed runtime via Kubernetes API
- internal registry flow for imported app images
- Helm chart for installing the core control plane on k3s

## Current MVP constraints

- The core control plane now stores state in PostgreSQL tables and uses `LISTEN/NOTIFY` to wake the controller when new operations arrive.
- On the first relational PostgreSQL startup, Fugue automatically imports the legacy `fugue_state` row or `/var/lib/fugue/store.json` if either exists.
- The Helm chart now deploys `fugue-api` and `fugue-controller` as separate Deployments, defaults both to `replicaCount=2`, and enables controller leader election so API and controller can scale independently.
- The bundled install path still keeps PostgreSQL, the internal registry, and other stateful pieces inside the cluster with `hostPath` storage, so it is still an opinionated MVP deployment rather than a fully externalized production topology.

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
- separate runtime inventory and real cluster-node inventory, plus the deprecated compatibility nodes view
- one built-in managed shared runtime: `runtime_managed_shared`
- external node attachment through node bootstrap plus `fugue-agent`
- asynchronous app deploy, scale, migrate, disable, and delete operations
- `POST /v1/apps/import-github` for public GitHub repositories, with optional idempotency key support and `auto / static-site / dockerfile / buildpacks / nixpacks` build strategies
- `POST /v1/apps/{id}/rebuild` to rebuild an imported `github-public` app from the latest repo state, or an imported `upload` app from its saved archive, and redeploy it
- `GET/PATCH /v1/apps/{id}/env`, `GET/PUT/DELETE /v1/apps/{id}/files`, and `POST /v1/apps/{id}/restart` to inspect and change app config by queuing deploy operations
- `GET /v1/backing-services`, `GET /v1/backing-services/{id}`, and `GET /v1/apps/{id}/bindings` to inspect attached service inventory and binding env
- `DELETE /v1/tenants/{id}` for platform-admin tenant removal with best-effort namespace cleanup reporting
- `GET /install/join-cluster.sh`, `POST /v1/nodes/join-cluster`, and `POST /v1/nodes/join-cluster/env` for one-command cluster-join onboarding when cluster join is configured
- runtime-agent pull model: enroll, heartbeat, fetch tasks, mark task complete
- audit trail for control-plane actions

What is not implemented yet:

- general project/runtime/app metadata update APIs outside the deploy-oriented app env/files endpoints
- public APIs for creating/deleting backing services or binding/unbinding them manually
- kpack-style buildpacks operator integration
- autoscaling policies such as HPA/VPA
- scheduling policies, quotas, billing, or paywall logic

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
| `app.scale` | create scale or disable operations |
| `app.migrate` | create migrate operations |
| `app.delete` | delete apps without broad `app.write` scope |
| `platform.admin` | full platform admin behavior |

Notes:

- `GET` list and detail endpoints still require a valid bearer token, even when they do not check a dedicated write scope.
- Secrets returned by create endpoints are only shown once. Persist them on your side.

## API reference

### Public endpoints

| Method | Path | Auth | Notes |
| --- | --- | --- | --- |
| `GET` | `/healthz` | none | control-plane health check |
| `GET` | `/readyz` | none | returns `200` while the API is accepting work, `503` during shutdown drain |
| `GET` | `/install/join-cluster.sh` | none | serves the helper shell script for one-command cluster join when cluster join is configured |
| `POST` | `/v1/nodes/bootstrap` | none | exchanges a reusable node key for one machine-specific runtime key |
| `POST` | `/v1/nodes/join-cluster` | none | exchanges a reusable node key for a runtime record plus a k3s join plan |
| `POST` | `/v1/nodes/join-cluster/env` | none | form-encoded variant that returns shell-quoted `FUGUE_JOIN_*` values |
| `POST` | `/v1/agent/enroll` | none | exchanges an enroll token for a runtime record plus runtime key |

`POST /v1/nodes/bootstrap` request body:

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

`node_name` and `machine_name` are optional. If you use Fugue's one-line join script and do not pass `FUGUE_NODE_NAME`, the script defaults to the VPS hostname. `machine_fingerprint` is also optional, but in production it should stay stable per machine so repeated joins update the same runtime record instead of creating duplicates.

`POST /v1/nodes/join-cluster` accepts the same JSON body as `POST /v1/nodes/bootstrap`, but returns the created `node` plus a `join` plan containing the k3s server URL, token, labels, taints, runtime id, registry endpoint, and optional mesh settings needed by the install helper.

`POST /v1/nodes/join-cluster/env` accepts equivalent `application/x-www-form-urlencoded` fields (`node_key`, `node_name`, `runtime_name`, `machine_name`, `machine_fingerprint`, `endpoint`, `labels`) and returns shell-quoted `FUGUE_JOIN_*` lines for custom installers.

`GET /install/join-cluster.sh` serves the helper shell script used for one-command node onboarding. The script calls `/v1/nodes/join-cluster/env`, writes the k3s agent config, and applies optional registry / mesh settings when cluster join is enabled.

Legacy compatibility: `POST /v1/agent/enroll` still accepts one-time enroll tokens.

`POST /v1/agent/enroll` request body:

```json
{
  "enroll_token": "<fugue_enroll_...>",
  "machine_name": "tenant-vps-1",
  "machine_fingerprint": "6d6e7b1d9c...",
  "endpoint": "https://tenant-vps-1.example.com",
  "labels": {
    "region": "ap-east-1",
    "provider": "gcp"
  }
}
```

`runtime_name` and `machine_name` are optional. `machine_fingerprint` should stay stable per machine if you want repeated enroll/bootstrap flows to reuse the same runtime record.

### Platform and tenant endpoints

| Method | Path | Required scope | Notes |
| --- | --- | --- | --- |
| `GET` | `/v1/tenants` | any API credential | platform admin sees all; tenant key only sees its own tenant |
| `POST` | `/v1/tenants` | `platform.admin` | create tenant |
| `DELETE` | `/v1/tenants/{id}` | `platform.admin` | deletes the tenant and returns best-effort cleanup details for namespace / nodes |
| `GET` | `/v1/projects` | any API credential | platform admin should pass `tenant_id` query param |
| `POST` | `/v1/projects` | `project.write` | `tenant_id` optional for tenant key |
| `GET` | `/v1/api-keys` | any API credential | lists visible API keys, secrets are redacted |
| `POST` | `/v1/api-keys` | `apikey.write` | non-admin key cannot mint scopes it does not already hold |
| `GET` | `/v1/node-keys` | any API credential | lists visible node keys, secrets are redacted |
| `POST` | `/v1/node-keys` | `runtime.attach` | creates a reusable tenant node key |
| `GET` | `/v1/node-keys/{id}/usages` | any API credential | shows which runtimes have used a node key |
| `POST` | `/v1/node-keys/{id}/revoke` | `runtime.attach` | revokes a node key so it cannot register more machines |
| `GET` | `/v1/cluster/nodes` | any API credential | lists real Kubernetes cluster nodes; tenant keys only see their own attached cluster nodes |
| `GET` | `/v1/nodes` | any API credential | deprecated compatibility view that exposes runtime records, not physical machines |
| `GET` | `/v1/nodes/{id}` | any API credential | deprecated compatibility detail view for runtime records |
| `GET` | `/v1/runtimes` | any API credential | lists visible Fugue runtimes, with merged machine identity fields |
| `POST` | `/v1/runtimes` | `runtime.write` | manual runtime creation; `managed-shared` is platform-admin only |
| `GET` | `/v1/runtimes/{id}` | any API credential | tenant key can only see shared or same-tenant runtime |
| `GET` | `/v1/runtimes/enroll-tokens` | any API credential | platform admin should pass `tenant_id` |
| `POST` | `/v1/runtimes/enroll-tokens` | `runtime.attach` | creates one-time enroll token |
| `GET` | `/v1/backing-services` | any API credential | lists visible backing services |
| `GET` | `/v1/backing-services/{id}` | any API credential | fetch backing service detail if visible to the tenant |
| `GET` | `/v1/apps` | any API credential | lists visible apps |
| `POST` | `/v1/apps` | `app.write` | creates app metadata and desired spec |
| `POST` | `/v1/apps/import-github` | `app.write` + `app.deploy` | imports a public GitHub repository, allocates a default hostname, queues deployment, and honors `Idempotency-Key` |
| `GET` | `/v1/apps/{id}` | any API credential | fetch app detail |
| `GET` | `/v1/apps/{id}/bindings` | any API credential | returns app-to-service bindings plus the referenced backing services |
| `GET` | `/v1/apps/{id}/build-logs` | any API credential | returns latest import/build logs, or accepts `operation_id` |
| `GET` | `/v1/apps/{id}/runtime-logs` | any API credential | returns Kubernetes pod logs for `app` or `postgres` |
| `GET` | `/v1/apps/{id}/env` | any API credential | returns merged runtime env, including binding-provided variables |
| `PATCH` | `/v1/apps/{id}/env` | `app.write` or `app.deploy` | queues a deploy operation when env values change |
| `GET` | `/v1/apps/{id}/files` | any API credential | returns desired app files from `spec.files` |
| `PUT` | `/v1/apps/{id}/files` | `app.write` or `app.deploy` | upserts desired files and queues a deploy operation on change |
| `DELETE` | `/v1/apps/{id}/files` | `app.write` or `app.deploy` | deletes files named by repeated `path` query params and queues a deploy operation |
| `GET` | `/v1/apps/{id}/filesystem/tree` | any API credential | lists live entries from the app workspace volume, defaulting to `/workspace` |
| `GET` | `/v1/apps/{id}/filesystem/file` | any API credential | reads one live file from the app workspace volume |
| `PUT` | `/v1/apps/{id}/filesystem/file` | `app.write` or `app.deploy` | creates or overwrites one live file inside the app workspace volume |
| `POST` | `/v1/apps/{id}/filesystem/directory` | `app.write` or `app.deploy` | creates one live directory inside the app workspace volume |
| `DELETE` | `/v1/apps/{id}/filesystem` | `app.write` or `app.deploy` | deletes a live file or directory inside the app workspace volume |
| `POST` | `/v1/apps/{id}/rebuild` | `app.deploy` | rebuilds a `github-public` app from the latest GitHub code or an `upload` app from its saved archive, refreshes the workspace reset token when configured, then queues deployment |
| `POST` | `/v1/apps/{id}/deploy` | `app.deploy` | creates async deploy operation |
| `POST` | `/v1/apps/{id}/restart` | `app.deploy` | queues a deploy operation with a fresh restart token; disabled apps cannot be restarted, and persistent workspaces are preserved |
| `POST` | `/v1/apps/{id}/scale` | `app.scale` | creates async scale operation; `replicas` may be `0` |
| `POST` | `/v1/apps/{id}/disable` | `app.scale` | creates async disable operation and scales the app to `0` |
| `POST` | `/v1/apps/{id}/migrate` | `app.migrate` | creates async migrate operation |
| `DELETE` | `/v1/apps/{id}` | `app.write` or `app.delete` | creates async delete operation and removes the app route from the visible app list |
| `GET` | `/v1/operations` | any API credential | lists operations for visible tenant |
| `GET` | `/v1/operations/{id}` | any API credential | fetch operation detail |
| `GET` | `/v1/audit-events` | any API credential | newest first |

Important request payloads:

Inventory semantics:

- `/v1/runtimes`: the Fugue deploy-target inventory. Attached VPS metadata such as `machine_name`, `connection_mode`, `cluster_node_name`, and fingerprint fields now live here.
- `/v1/cluster/nodes`: the real Kubernetes node inventory from the cluster API.
- `/v1/node-keys/{id}/usages`: the mapping from one reusable node key to the runtimes that actually used it.
- `/v1/nodes`: deprecated compatibility runtime view kept for older clients.

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

`label` is optional and defaults to `default`.
For a tenant-scoped API key, the request body itself is optional; an empty `POST` creates a default reusable node key for the current tenant.

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
    "runtime_id": "runtime_managed_shared",
    "workspace": {
      "mount_path": "/workspace"
    }
  }
}
```

`spec.workspace` is optional. When present, Fugue mounts a persistent writable workspace volume (default mount path `/workspace`) and enables the live `/filesystem/*` endpoints for that app. This currently requires a `managed-owned` runtime because the workspace is backed by node-local `hostPath` storage.

`POST /v1/apps/import-github`

Request headers:

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

Import behavior in the current MVP:

- only public GitHub repositories are supported
- `project_id` is optional; if omitted, Fugue reuses the tenant's `default` project or creates it automatically
- `build_strategy` is optional; default is `auto`
- `auto` currently resolves in this order: `Dockerfile` -> ready static site -> `buildpacks` for supported apps -> `nixpacks`
- `static-site` expects `index.html` in the root, `dist/`, `build/`, `public/`, or `site/`
- `buildpacks` uses Paketo builders for common Node.js / Python / Go / Java / Ruby / PHP / .NET repositories
- `nixpacks` is the current zero-config app builder for common Node.js, Python, Go, and similar repositories
- `service_port` is optional; if omitted, Fugue uses the detected or strategy default port
- Git submodules are cloned recursively by default
- Fugue either packages static files into a Caddy image, builds from Dockerfile, runs Buildpacks/Paketo, or runs Nixpacks and then pushes the image into the internal registry
- the returned app includes a generated public hostname under your configured app base domain
- if the same `Idempotency-Key` is replayed with the same request body, Fugue returns the original app + operation instead of creating a duplicate app
- if the same `Idempotency-Key` is reused with a different request body, Fugue returns `409 Conflict`

`POST /v1/apps/{id}/rebuild`

```json
{}
```

Optional override:

```json
{
  "branch": "main",
  "source_dir": "apps/web",
  "dockerfile_path": "deploy/Dockerfile"
}
```

Rebuild behavior:

- works for apps originally created from `github-public` or `upload` source
- for `github-public`, pulls the latest code from the saved repository URL and branch; the optional `branch` override only applies to this source type
- for `upload`, reuses the saved `upload_id` archive and re-queues import with the saved build metadata
- clones Git submodules recursively for GitHub imports
- rebuilds with the saved build strategy (`static-site`, `dockerfile`, `buildpacks`, or `nixpacks`) and pushes a new image into the internal registry
- keeps the same app id, project, and public hostname, then queues a deploy operation with the new image

`GET /v1/apps/{id}/build-logs`

Query parameters:

- `operation_id` optional; defaults to the latest `import` operation of the app
- `tail_lines` optional; default `200`, max `5000`

Behavior:

- tries recent Kubernetes builder Job logs first
- falls back to stored operation error/result text if the Job is already gone

`GET /v1/apps/{id}/runtime-logs`

Query parameters:

- `component` optional; `app` by default, or `postgres`
- `pod` optional; restrict logs to one pod name
- `tail_lines` optional; default `200`, max `5000`
- `previous` optional; when `true`, returns previous container logs

Behavior:

- only works for managed runtimes
- reads logs directly from tenant namespace pods

`DELETE /v1/tenants/{id}`

No request body.

Behavior:

- requires platform-admin credentials
- deletes the tenant from Fugue state and returns a `cleanup` object with the namespace name, whether namespace deletion was requested, owned-node counts, and any warnings
- namespace removal is best-effort; managed-owned nodes may still need manual k3s agent uninstall on the tenant VPS

`GET /v1/backing-services` and `GET /v1/apps/{id}/bindings`

Behavior:

- backing services are currently exposed as read APIs; managed Postgres created from app specs is reflected here
- `GET /v1/apps/{id}/bindings` returns both the binding records and the referenced backing-service objects

`GET /v1/apps/{id}/env`

Behavior:

- returns the merged environment that the app sees after combining binding-provided env with `spec.env`

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

Behavior:

- requires `app.write` or `app.deploy` unless you are platform admin
- queues a `deploy` operation when the effective env changes
- returns `already_current: true` without creating an operation when the requested env already matches the stored desired spec

`GET /v1/apps/{id}/files`

Behavior:

- returns the desired file set currently stored in `spec.files`

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

Behavior:

- upserts files by `path`
- requires `app.write` or `app.deploy` unless you are platform admin
- queues a `deploy` operation when file contents change

`DELETE /v1/apps/{id}/files`

No request body. Use repeated `path` query parameters, for example `/v1/apps/<app-id>/files?path=/etc/caddy/Caddyfile&path=/app/.env`.

Behavior:

- requires `app.write` or `app.deploy` unless you are platform admin
- queues a `deploy` operation when at least one file is removed

`GET /v1/apps/{id}/filesystem/tree`

Query params:

- `path` defaults to the workspace root
- `component` currently only accepts `app`
- `depth` currently only accepts `1`

Behavior:

- lists the live contents of the persistent app workspace, not `spec.files`
- only paths inside the configured workspace root are allowed
- the workspace metadata directory used for rebuild resets is hidden from API responses

`GET /v1/apps/{id}/filesystem/file`

Query params:

- `path` is required and must stay inside the app workspace root
- `max_bytes` defaults to `262144`
- `component` currently only accepts `app`

Behavior:

- reads one live file from the persistent app workspace
- returns `encoding: "utf-8"` for valid UTF-8 content, otherwise `encoding: "base64"`
- returns `truncated: true` when the file is larger than `max_bytes`

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

Behavior:

- writes to the live persistent workspace without creating a deploy operation
- only paths inside the configured workspace root are allowed
- `encoding` accepts `utf-8` (default) or `base64`

`POST /v1/apps/{id}/filesystem/directory`

```json
{
  "path": "/workspace/assets",
  "mode": 493,
  "parents": true
}
```

Behavior:

- creates a live directory inside the persistent app workspace
- only paths inside the configured workspace root are allowed

`DELETE /v1/apps/{id}/filesystem`

No request body. Use `path=/workspace/...` and optional `recursive=true`.

Behavior:

- deletes one live file or directory from the persistent app workspace
- the workspace root itself cannot be deleted through this endpoint

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

`POST /v1/apps/{id}/restart`

No request body.

Behavior:

- requires `app.deploy`
- only works when the app currently has `replicas > 0`
- queues a `deploy` operation with a fresh `restart_token`
- preserves the persistent workspace volume when `spec.workspace` is configured

`POST /v1/apps/{id}/rebuild`

Behavior:

- requires `app.deploy`
- rebuilds from the app's saved source definition
- refreshes `spec.workspace.reset_token` when a persistent workspace is configured, so the next rollout recreates the workspace contents once

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

`POST /v1/apps/{id}/migrate`

```json
{
  "target_runtime_id": "runtime_xxx"
}
```

`DELETE /v1/apps/{id}`

No request body.

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
  -d '{}'
```

Use the returned `secret` as the bootstrap credential on any user VPS:

```bash
export FUGUE_NODE_KEY="<node-key-secret>"
```

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

Deploy the current directory with the CLI:

```bash
export FUGUE_BASE_URL="https://<your-fugue-api-domain>"
export FUGUE_TOKEN="<tenant-api-key-or-bootstrap-key>"
./bin/fugue deploy --name cerebr --project default
```

If you use the bootstrap admin key and multiple tenants are visible, also pass `--tenant` or `--tenant-id`.

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
