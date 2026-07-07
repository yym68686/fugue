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

If you temporarily keep the bundled registry, keep it PVC-backed or bind it to an existing claim. The chart defaults to `registry.persistence.mode=pvc` and blocks naked `hostPath` registry storage unless the operator explicitly opts into `registry.unsafeHostPath.enabled=true` with a reason. For an existing hostPath install, use `scripts/migrate_fugue_registry_hostpath_to_pvc.sh` during a maintenance window to copy the active store into a PVC; the script does not delete the old hostPath data.

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

The productized backup surface is documented in `docs/backup-system-plan.md`
and the CLI guide is in `docs/backup-cli-guide.md`. In the new baseline Fugue
keeps the control-plane database backup enabled by default, uses platform R2
when configured, retains three successful records, and keeps user app/database
backup disabled until the user explicitly creates an app backup policy.

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

The long-term data-plane split is documented in
`docs/regional-edge-data-plane.md`. That architecture keeps the control plane
responsible for route, certificate, state, and policy decisions while moving
public application traffic to regional edge nodes close to users or runtimes.

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

This is separate from the runtime cell substrate. Runtime cells keep Fugue's
system layer alive during control-plane outages: local state, mesh discovery,
route metadata, heartbeats, and deferred operation reports. They do not
automatically move apps, promote databases, create replicas, or shift traffic.
Those actions remain business-continuity policy and require explicit app or
database configuration.

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

## 8. Robustness and self-healing runbooks

Use the unified robustness surface before changing live DNS, route, edge, or
backup state:

```bash
fugue admin robustness status
fugue admin robustness incidents ls
fugue admin robustness check <hostname-or-node>
fugue admin robustness incidents show <incident-id>
fugue admin robustness repair-plan <incident-id>
fugue admin robustness repair <incident-id> --dry-run
```

The repair command is intentionally dry-run by default. Set
`FUGUE_ROBUSTNESS_REPAIR_DISABLED=true` on the control plane when every repair
path must be disabled during an incident. Non-dry-run automatic repair remains
blocked unless the incident class has an explicitly safe implementation and an
audit event.

Detailed incident runbooks:

- [Node DNS failure](runbooks/node-dns-failure.md)
- [Stale managed iptables rule](runbooks/stale-iptables-managed-rule.md)
- [Edge quarantine](runbooks/edge-quarantine.md)
- [Traffic safety zero eligible edge](runbooks/traffic-safety-zero-eligible-edge.md)
- [Release guard blocked](runbooks/release-guard-blocked.md)
- [Request attribution](runbooks/request-attribution.md)
- [Stateless runtime migration](runbooks/stateless-runtime-migration.md)
- [Stateful app preflight](runbooks/stateful-app-preflight.md)
- [Platform artifact release](runbooks/platform-artifact-release.md)
- [Platform artifact rollback](runbooks/platform-artifact-rollback.md)
- [Consumer generation drift](runbooks/consumer-generation-drift.md)
- [LKG expired](runbooks/lkg-expired.md)
- [Subsystem failure contract](runbooks/subsystem-failure-contract.md)
- [Automatic repair safety](runbooks/automatic-repair-safety.md)
- [Emergency disable switch](runbooks/emergency-disable-switch.md)

### Route, DNS, and TLS mismatch

1. Run `fugue admin robustness check <hostname>`.
2. Confirm `route_dns_invariant`, `edge_tls_ready`, route status, DNS answer
   set, and the selected edge group in the incident evidence.
3. If the incident is `block_publish`, do not push a new DNS or route bundle.
   Keep nodes serving their current LKG bundle.
4. If one edge is unsafe for the hostname, use the edge route policy exclusion
   path for that hostname/edge and rerun the robustness check before restoring
   traffic.
5. Only remove an exclusion after repeated successful TLS/API probes and a clean
   `fugue admin robustness check <hostname>`.

### LKG serving

1. Check `fugue_robustness_lkg_serving` and
   `fugue_robustness_node_generation_drift_seconds` in Prometheus.
2. Inspect the node with `fugue admin edge nodes` or `fugue admin dns status`.
3. Verify `serving_generation`, `lkg_generation`, `cache_status`, and any Caddy
   or DNS cache error.
4. Keep LKG active while the current generated bundle fails invariants.
5. Promote a new bundle only after robustness status has no `block_publish`
   incident for the affected artifact.

### DNS node resync

1. Identify the stale DNS node from the `dns_generation_drift` incident.
2. Confirm the node is still intended to serve DNS and is not excluded by node
   policy.
3. Restart or resync only the affected DNS node role; do not alter parent NS
   delegation during the first recovery attempt.
4. Watch for a fresh heartbeat with matching `dns_bundle_version`,
   `serving_generation`, and `lkg_generation`.
5. If the node cannot converge, keep it out of delegation candidates until it
   reports a healthy bundle and probe pass.

### Control-plane publish rejection

1. Treat `block_publish` as a real release blocker, not a transient API error.
2. Read the structured invariant fields: `name`, `subject`, `expected`,
   `observed`, `evidence`, and `repair_hint`.
3. Do not bypass the invariant by manually editing generated artifacts.
4. Fix the source object or policy, then regenerate and re-run the status check.
5. If a deployment already started, let the normal GitHub Actions rollback path
   handle the failed health gate.

### Synthetic probe failure

1. Compare the failed probe target with the generated route and DNS evidence.
2. Check TLS/SNI first for custom domains, then route readiness and upstream
   readiness.
3. If only one hostname-edge pair fails, quarantine that pair through route
   policy instead of removing the whole edge group.
4. If all probes fail for a bundle generation, keep the previous LKG bundle and
   inspect control-plane bundle generation before retrying.

### Staging failure drills

Run drills in staging or a disposable control-plane namespace:

- route/DNS mismatch after edge exclusion
- empty safe DNS answer set
- one stale DNS node generation
- Caddy reload failure with previous config retained
- missing backup backend and blocked backup run
- runtime-loss continuity check for a stateless app
- controller restart while an operation is pending/running

Each drill must record the generated incident, the chosen repair plan, the audit
event, and the metric/alert that would notify an operator in production.

### Manual override checklist

Before overriding a robustness gate:

- Confirm the incident is not `block_publish`, or document the reason a manual
  override is safer than rollback.
- Capture `fugue admin robustness status --json`.
- Capture the affected route/DNS/edge node status and current generation IDs.
- Verify backups are fresh enough for the change being made.
- Prefer a scoped exclusion or rollback over a broad node or edge-group change.
- Record the manual command, expected blast radius, rollback command, and the
  time when the override must be removed.
