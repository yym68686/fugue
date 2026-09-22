# etcd metrics and platform request ownership

## Confirmed causes

The existing `fugue-kubernetes-nodes` job uses the kubelet's default node proxy
`/metrics` endpoint. It does not contain the embedded etcd storage families.
The etcd member serves them on port 2381. The existing Prometheus service
account can read `/api/v1/nodes/<node>:2381/proxy/metrics`; no new listener,
credentials, RBAC grant, collector, or control-plane restart is necessary.

The production scrape policy adds `fugue-etcd`, discovers only nodes labelled
`node-role.kubernetes.io/etcd=true`, and uses that authenticated API proxy.
It verifies the Kubernetes API certificate, retains only `etcd_.*` metrics,
and uses a 30-second interval, 5-second timeout, 2 MB response limit and 2,500
sample limit. Node and instance labels come from discovery, not fixed IPs.

The original platform request rejection was a separate exporter bug. Platform
API/mesh routes legitimately have no tenant application. The old completeness
check required an app ID unconditionally and moved these requests into
`app_events` as `request_fact_incomplete`, hiding them from ordinary
`request_facts` queries. This did not itself generate the HTTP 503 responses.

Commits `0c73aaa2191db51393ca2749ad02f9c5785ad27a` and
`31cd09f78be9e55dd00b8363de41b737720d4f15` already fixed ownership validation:
explicit `platform-route`/`control-plane-*` routes may have empty app/tenant
IDs, provided tenant/project are absent and route, edge, host, request/trace,
path and status evidence is complete. Tenant requests still require an app.
Do not fabricate an app ID or relax tenant attribution to repair platform data.

On 2026-09-22 at approximately 04:02 UTC, a bounded read-only ClickHouse check
of the preceding 24 hours found 72,524 platform requests with empty app and
tenant IDs in `request_facts`, including 23 HTTP 503 and 4 HTTP 502 responses.
There were zero new `request_fact_incomplete` events. The deployed telemetry
configuration revision was `13ba31279722d558bc3c879a5b59f88df70f93c2`, which
contains both ownership fixes. The current ownership regression test and
replay of the previously captured original edge events both passed.
No further exporter change or historical backfill is needed to fix ingestion.

## Independent configuration deployment

Edit `deploy/environments/production/observability/scrape-policy.json` and push
to main. The `observability_configuration` Actions lane validates with the
running Prometheus `promtool`, patches only the ConfigMap using a resource
version precondition, waits for projection, and sends SIGHUP. It does not
replace the Prometheus Pod or TSDB and does not depend on a component build.
Superseded configuration runs do not overwrite newer intent.

`additionalScrapeConfigs` uses Prometheus's native job schema. The reconciler
owns a marked block and refuses name collisions with other jobs. Removing a
job from this list removes it from the block at the next configuration rollout.
For a separate Helm installation, use the existing
`observability.metrics.extraScrapeConfigs` value to supply the same job.

`verificationQueries` are PromQL filter predicates: each must return a nonempty
vector to pass (do not use a `bool` comparison). Rollout verification requires
a successful new reload, all etcd targets up, and fresh backend commit, WAL
fsync, database size and leader metrics. Missing metrics fail verification.

Useful queries:

```promql
up{job="fugue-etcd"}
time() - timestamp(etcd_disk_backend_commit_duration_seconds_count{job="fugue-etcd"})
histogram_quantile(0.99, sum by (node, le) (rate(etcd_disk_backend_commit_duration_seconds_bucket{job="fugue-etcd"}[5m])))
histogram_quantile(0.99, sum by (node, le) (rate(etcd_disk_wal_fsync_duration_seconds_bucket{job="fugue-etcd"}[5m])))
increase(etcd_server_slow_apply_total{job="fugue-etcd"}[5m])
```

To roll back only this observation job, remove it and its verification
predicates from the policy and push. Existing serving configuration, other
scrape jobs, and recorded historical time series remain intact.
