# Fugue deployment guide

This document describes how to deploy the current Fugue MVP onto your k3s environment and how to attach a user-owned VPS runtime.

For the production HA / DR path with external PostgreSQL, external registry, external secret materialization, and multi-edge ingress, see `docs/ha-dr.md`.

## Quick path for your current 3 VPS

If you want the current MVP on exactly three VPS and you already have SSH aliases `gcp1`, `gcp2`, and `gcp3`, use:

```bash
./scripts/install_fugue_ha.sh
```

If you also want the installer to configure the HTTPS API edge, set `FUGUE_DOMAIN=<your-fugue-api-domain>`. You can additionally set `FUGUE_APP_BASE_DOMAIN=<your-app-base-domain>` for generated app hostnames.

Assumptions:

- the local machine has `ssh`, `scp`, `openssl`, and `docker` or `podman`
- all three SSH aliases are reachable
- the remote SSH user is `root`, or can run `sudo -n`
- the three VPS use a `systemd`-based distro

This mode installs all three machines as `k3s server` nodes and deploys Fugue as separate `fugue-api` and `fugue-controller` Deployments.
The chart defaults both Deployments to `replicaCount=2`, keeps authoritative state in PostgreSQL, and enables controller leader election.
The same script also provisions an in-cluster internal registry and can optionally configure a wildcard HTTPS edge for app hostnames.

## 1. Topology

Quick 3-VPS topology used by `install_fugue_ha.sh`:

- `3 x k3s server` nodes: combined cluster-control-plane and Fugue system nodes
- `1 x fugue-api Deployment`: defaults to 2 replicas
- `1 x fugue-controller Deployment`: defaults to 2 replicas with leader election
- `1 x PostgreSQL Deployment`: relational state backend
- `1 x internal registry Deployment`
- `N x fugue-agent`: optional, for `external-owned` runtimes
- `N x managed-owned` tenant nodes: optional, joined through `/install/join-cluster.sh`

If you have dedicated worker nodes, taint the k3s server nodes and use labels, node selectors, and tolerations to keep Fugue system pods and tenant workloads on the pools you want. The bundled 3-VPS installer does not require separate worker nodes.

## 2. Build artifacts

Build binaries locally:

```bash
make test
make build
```

Build core images:

```bash
export VERSION=0.1.0

docker build -f Dockerfile.api -t fugue-api:${VERSION} .
docker build -f Dockerfile.controller -t fugue-controller:${VERSION} .
docker build -f Dockerfile.agent -t fugue-agent:${VERSION} .
```

For the current GitHub-import MVP, Fugue does not require an external registry. Imported apps are built into images and pushed to the internal registry exposed by the control plane.

## 3. Install k3s HA control plane

Pick a fixed registration address for the three server nodes. This can be:

- an external load balancer VIP
- a stable DNS record that points to the control-plane endpoint
- a temporary first server IP for bootstrap, if you do not have an LB yet

Example on `cp1`:

```bash
curl -sfL https://get.k3s.io | \
  INSTALL_K3S_EXEC="server \
    --cluster-init \
    --tls-san k3s-api.example.com \
    --disable traefik \
    --disable servicelb \
    --disable local-storage \
    --write-kubeconfig-mode 644" sh -
```

Get the cluster token on `cp1`:

```bash
sudo cat /var/lib/rancher/k3s/server/token
```

Join `cp2` and `cp3`:

```bash
export K3S_URL=https://k3s-api.example.com:6443
export K3S_TOKEN='<token-from-cp1>'

curl -sfL https://get.k3s.io | \
  INSTALL_K3S_EXEC="server \
    --tls-san k3s-api.example.com \
    --disable traefik \
    --disable servicelb \
    --disable local-storage \
    --write-kubeconfig-mode 644" sh -
```

Label and taint the control-plane nodes:

```bash
kubectl taint nodes cp1 node-role.kubernetes.io/control-plane=true:NoSchedule
kubectl taint nodes cp2 node-role.kubernetes.io/control-plane=true:NoSchedule
kubectl taint nodes cp3 node-role.kubernetes.io/control-plane=true:NoSchedule
```

Join workers:

```bash
export K3S_URL=https://k3s-api.example.com:6443
export K3S_TOKEN='<token-from-cp1>'

curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="agent --node-label nodepool=system" sh -
```

Label worker pools after join:

```bash
kubectl label node wk1 nodepool=system --overwrite
kubectl label node wk2 nodepool=apps --overwrite
```

## 4. Install Fugue on k3s

Create namespace:

```bash
kubectl create namespace fugue-system
```

Install the chart:

```bash
export VERSION=0.1.0
export FUGUE_BOOTSTRAP_KEY='replace-with-a-long-random-secret'

helm upgrade --install fugue ./deploy/helm/fugue \
  -n fugue-system \
  --set bootstrapAdminKey="${FUGUE_BOOTSTRAP_KEY}" \
  --set api.image.repository="fugue-api" \
  --set api.image.tag="${VERSION}" \
  --set controller.image.repository="fugue-controller" \
  --set controller.image.tag="${VERSION}" \
  --set api.apiPublicDomain="api.example.com" \
  --set api.appBaseDomain="apps.example.com" \
  --set api.registryPushBase="fugue-fugue-registry.fugue-system.svc.cluster.local:5000" \
  --set api.registryPullBase="<node-reachable-ip>:30500" \
  --set api.clusterJoinRegistryEndpoint="<node-reachable-ip>:30500" \
  --set registry.service.nodePort=30500 \
  --set nodeSelector.nodepool=system
```

The chart enables an internal PostgreSQL instance by default and also enables controller leader election by default.

Controller GitHub sync knobs:

- `FUGUE_CONTROLLER_GITHUB_SYNC_INTERVAL`: how often the controller checks imported GitHub apps for a newer branch commit. Set `0` to disable the automatic rebuild loop.
- `FUGUE_CONTROLLER_FOREGROUND_IMPORT_WORKERS`: controller-side concurrency cap for manual import/build operations. Set `0` to remove the cap so every ready import can start immediately; same-app ordering is still preserved.
- `FUGUE_CONTROLLER_GITHUB_SYNC_IMPORT_WORKERS`: controller-side concurrency cap for auto-triggered GitHub import/build operations. Set `0` to remove the cap so every ready import can start immediately; same-app ordering is still preserved, and compose dependency checks still gate activation/deploy.
- `FUGUE_CONTROLLER_GITHUB_SYNC_TIMEOUT`: timeout for each upstream GitHub check.
- `FUGUE_CONTROLLER_GITHUB_SYNC_RETRY_BASE_DELAY`: base retry delay after a tracked commit fails during auto sync. Repeated auto failures for the same commit back off from this value while automatic retries remain available.
- `FUGUE_CONTROLLER_GITHUB_SYNC_RETRY_MAX_DELAY`: maximum retry delay for repeated auto failures of the same tracked commit.
- GitHub sync stops auto retrying a tracked commit after `3` failed auto-triggered import or deploy attempts for the same app and commit. The controller only re-arms that commit after a non-`github-sync` GitHub rebuild or deploy is triggered manually, or after a release for that commit completes successfully.
- `FUGUE_CONTROLLER_MANAGED_APP_ROLLOUT_TIMEOUT`: maximum time a managed deploy operation waits for the Kubernetes rollout to finish before it is marked failed.

Builder scheduling notes:

- Shared nodes can build imported sources whenever their remaining CPU, memory, and ephemeral storage fit the requested build profile; `fugue.io/build=true` is treated as a preference signal instead of a hard gate.
- Fresh HA installs and control-plane upgrades label `fugue.install/profile=combined` nodes with `fugue.io/build=true`, so the internal control-plane nodes remain available as fallback builder capacity whenever their remaining resources can satisfy a build.

Registry and cluster-join notes:

- `api.registryPushBase` should be the in-cluster address builders use to push imported images.
- `api.registryPullBase` should be reachable from runtime nodes that need to pull those images.
- `api.clusterJoinRegistryEndpoint` should be reachable from VPS nodes joined through `/install/join-cluster.sh`.
- The bundled registry still defaults to a host-path convenience baseline. Do not keep it on the primary node root disk in production.
- If you must keep the bundled registry, move it to dedicated storage with either `registry.persistence.mode=pvc` or `registry.persistence.mode=existingClaim`.
- If you want Fugue to expose `/install/join-cluster.sh`, also set `api.clusterJoinServer="https://k3s-api.example.com:6443"` and `api.clusterJoinBootstrapTokenTTL="15m"`. Optional hardening is `api.clusterJoinCAHash="<sha256-of-server-ca>"`. Optional mesh settings are `api.clusterJoinMeshProvider`, `api.clusterJoinMeshLoginServer`, and `api.clusterJoinMeshAuthKey`.

Example bundled-registry PVC override:

```yaml
registry:
  persistence:
    mode: pvc
    size: 200Gi
    storageClassName: fast-rwo
```

Example bundled-registry existing-claim override:

```yaml
registry:
  persistence:
    mode: existingClaim
    existingClaim: fugue-registry-data
```

The bundled registry now enables Docker Distribution upload purging by default. For emergency host-level cleanup on a control-plane node, use:

```bash
sudo ./scripts/cleanup_fugue_registry_host.sh
```

Watch rollout:

```bash
kubectl -n fugue-system rollout status deploy/fugue-fugue-postgres
kubectl -n fugue-system rollout status deploy/fugue-fugue-api
kubectl -n fugue-system rollout status deploy/fugue-fugue-controller
kubectl -n fugue-system rollout status deploy/fugue-fugue-registry
kubectl -n fugue-system get pods -o wide
```

Port-forward for initial bootstrap:

```bash
kubectl -n fugue-system port-forward svc/fugue-fugue 8080:80
```

## 5. Bootstrap tenants and API keys

Create a tenant:

```bash
curl -sS http://127.0.0.1:8080/v1/tenants \
  -H "Authorization: Bearer ${FUGUE_BOOTSTRAP_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"name":"tenant-a"}'
```

Create a project:

```bash
curl -sS http://127.0.0.1:8080/v1/projects \
  -H "Authorization: Bearer ${FUGUE_BOOTSTRAP_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"<tenant-id>","name":"default","description":"default project"}'
```

Create a tenant API key:

```bash
curl -sS http://127.0.0.1:8080/v1/api-keys \
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

## 6. Deploy to the managed shared runtime

Create an app:

```bash
curl -sS http://127.0.0.1:8080/v1/apps \
  -H "Authorization: Bearer <tenant-api-key>" \
  -H 'Content-Type: application/json' \
  -d '{
    "project_id":"<project-id>",
    "name":"nginx-demo",
    "description":"demo app",
    "spec":{
      "image":"nginx:1.27",
      "ports":[80],
      "replicas":1,
      "runtime_id":"runtime_managed_shared"
    }
  }'
```

Create a deploy operation:

```bash
curl -sS http://127.0.0.1:8080/v1/apps/<app-id>/deploy \
  -H "Authorization: Bearer <tenant-api-key>" \
  -H 'Content-Type: application/json' \
  -d '{}'
```

Track operations:

```bash
curl -sS http://127.0.0.1:8080/v1/operations \
  -H "Authorization: Bearer <tenant-api-key>"
```

The current controller creates one namespace per tenant with the pattern `fg-<tenant-id-normalized>` and applies:

- one tenant `Namespace`
- one app `Deployment` and one app `Service` for each deployed app
- optional managed Postgres `Secret`, `Service`, and `Deployment` when the app spec or managed backing-service bindings require PostgreSQL

Imported GitHub static sites are exposed through the Fugue API edge proxy using the generated hostname under your configured app base domain.

## 7. Attach a user-owned VPS node

Fugue now supports two distinct attachment paths:

- `managed-owned`: the VPS joins the center k3s cluster as an agent node through `/install/join-cluster.sh`; Fugue schedules workloads onto it through the in-cluster controller
- `external-owned`: the VPS runs its own k3s and `fugue-agent`; Fugue hands operations to the agent over the runtime API

### Option A: join the center cluster (`managed-owned`)

Prerequisite: set `api.clusterJoinServer`, `api.clusterJoinBootstrapTokenTTL`, `api.registryPullBase`, and `api.clusterJoinRegistryEndpoint` in your Helm values so the join endpoints are enabled. Set `api.clusterJoinCAHash` as well if you want secure-format join tokens.

Create a reusable node key from Fugue:

```bash
curl -sS http://127.0.0.1:8080/v1/node-keys \
  -H "Authorization: Bearer <tenant-api-key>" \
  -H 'Content-Type: application/json' \
  -d '{"label":"cluster-node-key"}'
```

Join a VPS to the center cluster:

```bash
curl -fsSL https://fugue-api.example.com/install/join-cluster.sh | \
  sudo FUGUE_NODE_KEY='<secret-from-node-key-response>' \
  FUGUE_NODE_NAME='tenant-node-1' \
  FUGUE_RUNTIME_LABELS='region=ap-east-1,provider=gcp' \
  bash
```

Useful optional environment variables for the join script:

- `FUGUE_MACHINE_NAME`: override the stored machine name
- `FUGUE_MACHINE_FINGERPRINT`: override the stable machine identity used for deduplication
- `FUGUE_NODE_EXTERNAL_IP`: force the published node external IP
- `FUGUE_K3S_CHANNEL`: choose the k3s release channel
- `FUGUE_LIMIT_CPU`: cap Fugue allocatable CPU on the node, for example `2`, `1.5`, or `1500m`
- `FUGUE_LIMIT_MEMORY`: cap Fugue allocatable memory on the node, for example `4Gi`
- `FUGUE_LIMIT_DISK`: cap Fugue allocatable ephemeral storage on the node, for example `50Gi`
- `FUGUE_LIMIT_DISK_PATH`: optional filesystem path used to detect total disk size for `FUGUE_LIMIT_DISK`; defaults to `/`

Example with explicit resource caps:

```bash
curl -fsSL https://fugue-api.example.com/install/join-cluster.sh | \
  sudo FUGUE_NODE_KEY='<secret-from-node-key-response>' \
  FUGUE_NODE_NAME='tenant-node-1' \
  FUGUE_LIMIT_CPU='2' \
  FUGUE_LIMIT_MEMORY='4Gi' \
  FUGUE_LIMIT_DISK='50Gi' \
  bash
```

The script also accepts CLI flags if you prefer:

```bash
curl -fsSL https://fugue-api.example.com/install/join-cluster.sh | \
  sudo FUGUE_NODE_KEY='<secret-from-node-key-response>' \
  bash -s -- --cpu 2 --memory 4Gi --disk 50Gi
```

These caps are applied by translating the requested maximums into kubelet `system-reserved` values. In Kubernetes terms, the disk cap maps to allocatable `ephemeral-storage` on the detected filesystem, so the final allocatable value can be slightly lower after kubelet's own safety reservations.

This path creates a `managed-owned` runtime with `connection_mode=cluster`. Fugue labels the joined node with `fugue.io/runtime-id`, `fugue.io/tenant-id`, and `fugue.io/node-mode=managed-owned`, and taints it with `fugue.io/tenant=<tenant-id>:NoSchedule`.

### Option B: run `fugue-agent` against a tenant-owned k3s host (`external-owned`)

Install k3s:

```bash
curl -sfL https://get.k3s.io | \
  INSTALL_K3S_EXEC="server \
    --disable traefik \
    --disable servicelb \
    --disable local-storage \
    --write-kubeconfig-mode 644" sh -
```

Create a reusable node key from Fugue:

```bash
curl -sS http://127.0.0.1:8080/v1/node-keys \
  -H "Authorization: Bearer <tenant-api-key>" \
  -H 'Content-Type: application/json' \
  -d '{"label":"default-node-key"}'
```

Run the agent on the VPS host:

```bash
sudo env \
  KUBECONFIG=/etc/rancher/k3s/k3s.yaml \
  FUGUE_AGENT_SERVER=https://fugue-api.example.com \
  FUGUE_AGENT_NODE_KEY='<secret-from-node-key-response>' \
  FUGUE_AGENT_RUNTIME_NAME='tenant-vps-1' \
  FUGUE_AGENT_RUNTIME_ENDPOINT='https://tenant-vps-1.example.com' \
  FUGUE_AGENT_WORK_DIR=/var/lib/fugue-agent \
  FUGUE_AGENT_STATE_FILE=/var/lib/fugue-agent/state.json \
  FUGUE_AGENT_CELL_LISTEN_ADDR=':7831' \
  FUGUE_AGENT_APPLY_WITH_KUBECTL=true \
  ./bin/fugue-agent
```

This path creates an `external-owned` runtime with `connection_mode=agent`.
The agent also keeps a local cell store under `FUGUE_AGENT_WORK_DIR`, exposes a
mesh-restricted cell status API on `FUGUE_AGENT_CELL_LISTEN_ADDR`, and caches
control-plane completion events for replay after outages.

### Option C: run the agent in Docker on the VPS

```bash
docker run -d --name fugue-agent --restart unless-stopped \
  -e KUBECONFIG=/etc/rancher/k3s/k3s.yaml \
  -e FUGUE_AGENT_SERVER=https://fugue-api.example.com \
  -e FUGUE_AGENT_NODE_KEY='<secret-from-node-key-response>' \
  -e FUGUE_AGENT_RUNTIME_NAME='tenant-vps-1' \
  -e FUGUE_AGENT_RUNTIME_ENDPOINT='https://tenant-vps-1.example.com' \
  -e FUGUE_AGENT_WORK_DIR=/var/lib/fugue-agent \
  -e FUGUE_AGENT_STATE_FILE=/var/lib/fugue-agent/state.json \
  -e FUGUE_AGENT_CELL_LISTEN_ADDR=':7831' \
  -e FUGUE_AGENT_APPLY_WITH_KUBECTL=true \
  -p 7831:7831 \
  -v /etc/rancher/k3s/k3s.yaml:/etc/rancher/k3s/k3s.yaml:ro \
  -v /var/lib/fugue-agent:/var/lib/fugue-agent \
  <your-registry>/fugue-agent:${VERSION}
```

Replace `<your-registry>/fugue-agent:${VERSION}` with an image reference your VPS can pull.

Legacy compatibility: one-time enroll tokens are still available at `/v1/runtimes/enroll-tokens`, but the recommended path is the reusable `node-key` flow above.

## 8. Migrate an app from managed runtime to attached node

List runtimes and find the target runtime ID:

```bash
curl -sS http://127.0.0.1:8080/v1/runtimes \
  -H "Authorization: Bearer <tenant-api-key>"
```

Create the migration:

```bash
curl -sS http://127.0.0.1:8080/v1/apps/<app-id>/migrate \
  -H "Authorization: Bearer <tenant-api-key>" \
  -H 'Content-Type: application/json' \
  -d '{"target_runtime_id":"<attached-runtime-id>"}'
```

Audit failover readiness before doing that:

```bash
export FUGUE_BASE_URL=http://127.0.0.1:8080
export FUGUE_API_KEY=<tenant-api-key>
fugue app continuity audit
fugue app continuity audit <app-name>
```

Interpretation:

- `ready`: stateless failover is currently eligible
- `caution`: no hard blocker exists, but redundancy is incomplete or runtime posture is weak
- `blocked`: Fugue-managed state is still attached, so one-click failover remains intentionally disabled

Flow:

1. The API writes an async `operation`.
2. If the target runtime is `external-owned`, the controller dispatches the operation to `fugue-agent`.
3. If the target runtime is `managed-owned`, the controller renders and applies manifests through the center cluster Kubernetes API using the runtime's node labels and tenant taints.
4. Fugue updates the app status and audit log.

## 9. Operational notes

- `fugue-api` and `fugue-controller` now run as separate Deployments. The chart defaults both to 2 replicas and enables controller leader election.
- Authoritative control-plane state now lives in PostgreSQL. The API and controller still keep local scratch data under `/var/lib/fugue` for import / render work.
- The bundled chart keeps PostgreSQL, the internal registry, and optional `headscale` in-cluster with `hostPath` storage. For production, externalize or harden those stateful dependencies and their placement.
- The chart now supports `configSecret.existingSecretName` so production deployments can source Fugue credentials from an external secret manager instead of chart-generated literals.
- A production HA baseline is included in `deploy/helm/fugue/values-production-ha.yaml`.
- `fugue app continuity audit` uses the same migration blocker rules as the API, so you can audit app-level failover eligibility before an incident.
- `api.registryPushBase` must be reachable from builder jobs inside the cluster. `api.registryPullBase` and `api.clusterJoinRegistryEndpoint` must be reachable from the runtime nodes that pull images.
- If the controller cannot reach the in-cluster Kubernetes API, `managed-shared` and `managed-owned` deploys will stop at the render/apply stage.
