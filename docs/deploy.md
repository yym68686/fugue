# Fugue deployment guide

This document describes how to deploy the current Fugue MVP onto your k3s environment and how to attach a user-owned VPS runtime.

## Quick path for your current 3 VPS

If you want the current MVP on exactly three VPS and you already have SSH aliases `gcp1`, `gcp2`, and `gcp3`, use:

```bash
./scripts/install_fugue_ha.sh
```

Assumptions:

- the local machine has `ssh`, `scp`, `openssl`, and `docker` or `podman`
- all three SSH aliases are reachable
- the remote SSH user is `root`, or can run `sudo -n`
- the three VPS use a `systemd`-based distro

This mode installs all three machines as `k3s server` nodes and then runs the current single-replica Fugue Pod on `gcp1` with a `hostPath` data directory. That tradeoff is intentional for the current file-backed MVP.
The same script also provisions an internal registry on the primary node and can configure a wildcard HTTPS edge for app hostnames.

## 1. Topology

Recommended minimum topology for this repository:

- `3 x k3s server` nodes: dedicated control plane only
- `2 x worker` nodes: one can host `fugue-system`, one can host tenant workloads
- `1 x Fugue core Pod`: runs `fugue-api` + `fugue-controller`
- `N x fugue-agent`: optional, one per attached user-owned runtime

The three control-plane nodes should not host tenant applications. Taint them and keep Fugue workloads on worker nodes.

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

For the current GitHub-import MVP, Fugue does not require an external registry. Imported apps are built into images and pushed to the internal registry exposed by the control plane.
```

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
  --set api.appBaseDomain="app.example.com" \
  --set api.registryPushBase="<primary-private-ip>:30500" \
  --set registry.service.nodePort=30500 \
  --set nodeSelector.nodepool=system
```

Watch rollout:

```bash
kubectl -n fugue-system rollout status deploy/fugue-fugue
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

- one `Namespace`
- one `Deployment`
- one `Service`

Imported GitHub static sites are exposed through the Fugue API edge proxy using the generated hostname under your configured app base domain.

## 7. Attach a user-owned VPS node

### Option A: recommended

Install single-node k3s on the user VPS and run `fugue-agent` on that host.

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
  FUGUE_AGENT_APPLY_WITH_KUBECTL=true \
  ./bin/fugue-agent
```

### Option B: run the agent in Docker on the VPS

```bash
docker run -d --name fugue-agent --restart unless-stopped \
  -e KUBECONFIG=/etc/rancher/k3s/k3s.yaml \
  -e FUGUE_AGENT_SERVER=https://fugue-api.example.com \
  -e FUGUE_AGENT_NODE_KEY='<secret-from-node-key-response>' \
  -e FUGUE_AGENT_RUNTIME_NAME='tenant-vps-1' \
  -e FUGUE_AGENT_RUNTIME_ENDPOINT='https://tenant-vps-1.example.com' \
  -e FUGUE_AGENT_WORK_DIR=/var/lib/fugue-agent \
  -e FUGUE_AGENT_STATE_FILE=/var/lib/fugue-agent/state.json \
  -e FUGUE_AGENT_APPLY_WITH_KUBECTL=true \
  -v /etc/rancher/k3s/k3s.yaml:/etc/rancher/k3s/k3s.yaml:ro \
  -v /var/lib/fugue-agent:/var/lib/fugue-agent \
  ${REGISTRY}/fugue-agent:${VERSION}
```

Legacy compatibility: one-time enroll tokens are still available at `/v1/runtimes/enroll-tokens`, but the recommended path is the reusable `node-key` flow above.

## 8. Migrate an app from managed runtime to attached node

List nodes and find the attached node ID:

```bash
curl -sS http://127.0.0.1:8080/v1/nodes \
  -H "Authorization: Bearer <tenant-api-key>"
```

Create the migration:

```bash
curl -sS http://127.0.0.1:8080/v1/apps/<app-id>/migrate \
  -H "Authorization: Bearer <tenant-api-key>" \
  -H 'Content-Type: application/json' \
  -d '{"target_runtime_id":"<attached-runtime-id>"}'
```

Flow:

1. The API writes an async `operation`.
2. The controller dispatches the operation to the attached runtime.
3. The agent polls the operation, renders the manifest, optionally applies it with local `kubectl`, and reports completion.
4. Fugue updates the app status and audit log.

## 9. Operational notes

- `fugue-api` and `fugue-controller` currently share a local file store inside one Pod. Keep the Helm release at one replica.
- If the controller cannot reach the in-cluster Kubernetes API, managed-runtime deploys will stop at the manifest rendering stage.
- For production, replace the file store with PostgreSQL, split the API and controller into independent Deployments, and add a real queue or workflow engine.
