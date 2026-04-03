# Fugue HA / DR guide

This document describes the production-oriented high-availability and disaster-recovery path for Fugue.

The bundled `install_fugue_ha.sh` flow is still an MVP convenience installer. It is not the production HA baseline because it keeps PostgreSQL, the registry, and the Route A edge on bundled infrastructure inside the same control-plane footprint.

## 1. Control-plane topology

Recommended production topology:

- Run `fugue-api` and `fugue-controller` as multiple replicas.
- Externalize PostgreSQL to an HA service with automatic failover.
- Externalize the image registry and point Fugue at it through `api.registryPushBase`, `api.registryPullBase`, and `api.clusterJoinRegistryEndpoint`.
- Materialize Fugue runtime secrets from an external secret manager into a Kubernetes `Secret`.
- Run multiple synchronized Fugue edge nodes and place a health-checked load balancer or VIP in front of them.

The chart now includes a production baseline file at `deploy/helm/fugue/values-production-ha.yaml`.

## 2. External secrets

The chart now supports reusing a pre-created Secret through:

```yaml
configSecret:
  existingSecretName: fugue-production-config
```

When `existingSecretName` is set:

- the chart no longer generates the Fugue config Secret
- API and controller pods read credentials from the referenced Secret
- you can supply the Secret via External Secrets Operator, CSI Secret Store, or your own automation

An External Secrets Operator example is included at:

- `deploy/helm/fugue/examples/fugue-config-externalsecret.yaml`

The minimum keys expected in that Secret are:

- `FUGUE_BOOTSTRAP_ADMIN_KEY`
- `FUGUE_DATABASE_URL`
- `FUGUE_CLUSTER_JOIN_SERVER`
- `FUGUE_CLUSTER_JOIN_CA_HASH`
- `FUGUE_CLUSTER_JOIN_MESH_PROVIDER`
- `FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER`
- `FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY`
- `FUGUE_EDGE_TLS_ASK_TOKEN`
- `POSTGRES_PASSWORD`

If the bundled PostgreSQL Deployment is disabled, `POSTGRES_PASSWORD` can stay present but is no longer consumed.

## 3. PostgreSQL failover vs backup

Do not treat replication as backup.

- Replication or standby nodes reduce downtime.
- Backups give you historical recovery points.
- WAL archiving and PITR are for disaster recovery.
- Logical backups are still useful for operator mistakes and data extraction.

Recommended production pattern:

- HA PostgreSQL cluster with automatic primary failover
- WAL archiving / PITR on the database platform side
- Regular logical backups kept outside the database cluster

Repository scripts for logical backups:

```bash
FUGUE_DATABASE_URL='postgres://...' ./scripts/backup_fugue_postgres.sh
FUGUE_DATABASE_URL='postgres://...' ./scripts/restore_fugue_postgres.sh --input .dist/postgres-backups/fugue-postgres-20260402T000000Z.dump
```

These scripts are intentionally separate from replication. They provide a recovery artifact even if the standby replicated bad data.

## 4. Production Helm install

Render the chart with the production baseline:

```bash
helm upgrade --install fugue ./deploy/helm/fugue \
  -n fugue-system \
  -f ./deploy/helm/fugue/values-production-ha.yaml
```

Before doing that, replace the placeholder values in `values-production-ha.yaml`:

- `api.databaseURL`
- `api.appBaseDomain`
- `api.apiPublicDomain`
- `api.registryPushBase`
- `api.registryPullBase`
- `api.clusterJoinRegistryEndpoint`
- `configSecret.existingSecretName`

The baseline intentionally disables:

- bundled PostgreSQL
- bundled registry
- bundled headscale

It keeps the Fugue Service as `ClusterIP`, assuming an external ingress, cloud load balancer, or VIP layer will front the edge.

## 5. Edge high availability

Fugue's Route A edge currently depends on Caddy because Caddy owns custom-domain TLS automation and wildcard app ingress behavior.

Production guidance:

1. Run the Fugue edge Caddy config on multiple edge nodes.
2. Put a health-checked L4 load balancer or VIP in front of those edge nodes.
3. Keep TLS termination on the Fugue edge nodes so custom-domain automation continues to work.

You can still reuse the existing `scripts/sync_fugue_edge_proxy.sh` flow by targeting each edge node individually with the appropriate SSH alias.

To place a VIP in front of those nodes, render an HAProxy + Keepalived bundle:

```bash
export FUGUE_EDGE_VIP='10.0.0.50/24'
export FUGUE_EDGE_INTERFACE='eth0'
export FUGUE_EDGE_AUTH_PASS='replace-me'
export FUGUE_EDGE_BACKENDS='10.0.0.11,10.0.0.12,10.0.0.13'
export FUGUE_EDGE_PEER_IPS='10.0.0.21,10.0.0.22'
./scripts/render_fugue_edge_ha_bundle.sh
```

This generates:

- `haproxy.cfg`
- `keepalived-primary.conf`
- `keepalived-secondary.conf`

Use that bundle on dedicated VIP nodes or adapt the generated configs to your cloud load balancer.

## 6. Readiness probes

`/readyz` now checks:

- Fugue's authoritative store
- Kubernetes API reachability for the configured control-plane namespace

That means the probe now reflects actual dependency loss instead of only process liveness.

## 7. Stateful app failover boundary

Current Fugue support is intentionally split between `migrate` and `failover`:

- `migrate` remains the stateless path. It is still blocked for apps that keep Fugue-managed PostgreSQL or a persistent workspace attached.
- `failover` is now the controller-orchestrated managed path for apps that declare `app.spec.failover` and run on Fugue-managed runtimes.

The managed failover path now includes:

- CloudNativePG-backed managed PostgreSQL instead of the legacy single Deployment + PVC substrate
- PVC-backed workspaces with VolSync replication objects
- per-app Kubernetes `Lease` fencing
- controller orchestration that acquires the fence, waits for a final workspace sync, scales the source app to zero, switches the runtime, and restores replicas on the target runtime
- automatic failover queueing when `app.spec.failover.auto=true` and the current runtime is observed offline

That means Fugue now has a first-class stateful failover workflow inside the current managed-runtime/control-plane model.

Remaining boundary:

- Cross-cluster database replica bootstrap, replica promotion, and end-to-end data-plane validation are not fully modeled yet in Fugue's app spec.
- The current implementation is strongest when the control plane can coordinate both runtimes and their storage operators from the same Kubernetes control plane.
- For multi-cluster DR, you still need operator-specific replication topology, secret distribution, and recovery validation outside the current generic app model.

You can audit the current app portfolio directly from the CLI:

```bash
export FUGUE_BASE_URL=https://api.example.com
export FUGUE_API_KEY=<tenant-api-key>
fugue app continuity audit
fugue app continuity audit <app-name>
```

The CLI reports three classes:

- `ready`: no stateful blocker is attached and the app currently has a reasonable stateless failover posture
- `caution`: Fugue does not see a hard stateful blocker, but replicas/runtime posture still need work
- `blocked`: stateless migration is intentionally blocked because managed backing services or persistent workspace state are still attached

That audit is intentionally conservative: it reports stateless failover posture, not whether the new managed stateful failover workflow has been configured for a specific app. To execute a failover, use `fugue app failover run <app-name> --to <runtime>`.
