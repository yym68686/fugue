# Fugue Observability Platformization

This document describes the user-facing and operator-facing boundaries for
Fugue Observability after the Prometheus-compatible metrics, Loki-compatible
logs, and ClickHouse analytics planes are enabled.

Fugue Observability stores platform diagnostic copies. It is not the source of
truth for app-owned business logs, billing ledgers, audit trails, customer-facing
request history, or product analytics. App-owned product data must remain in the
app database, app OLAP store, or app archive path.

## Default Retention

The default retention for Fugue Observability data is 24 hours.

Longer retention must be explicitly configured with matching ingest quota, query
limits, and cost ownership. A shorter app-specific retention can be configured
with:

```text
FUGUE_OBSERVABILITY_APP_RETENTION_OVERRIDES=app_123=6h,app_456=30m
```

Queries cannot bypass retention. If a user asks for a window outside the app
retention policy, the app observability API returns a 400 response instead of
silently scanning old or missing data.

## Tenant Quota And Metering

Telemetry ingestion quotas are configured at the platform level:

```text
FUGUE_OBSERVABILITY_TENANT_EVENT_QUOTA_PER_MINUTE=10000
FUGUE_OBSERVABILITY_TENANT_EVENT_QUOTA_OVERRIDES=tenant_hot=50000
FUGUE_OBSERVABILITY_APP_EVENT_QUOTA_PER_MINUTE=5000
```

Quota is enforced inside the telemetry pipeline. Dropping diagnostic telemetry
must not block app requests.

The telemetry agent exposes tenant-level metering for observability cost and
billing pipelines:

```text
fugue_telemetry_tenant_events_total{tenant_id="tenant_123",outcome="received"}
fugue_telemetry_tenant_events_total{tenant_id="tenant_123",outcome="dropped"}
fugue_telemetry_tenant_events_total{tenant_id="tenant_123",outcome="quota_dropped"}
```

This metric is intentionally tenant-level. It must not include trace IDs,
request IDs, user IDs, credentials, raw IPs, emails, or request bodies.

## Zero-Code Baseline

For apps deployed on Fugue, zero-code observability provides:

- stdout/stderr log query.
- request summary query from platform request facts.
- basic app latency and status metrics.
- automatic diagnosis when enough platform evidence exists.

Useful CLI commands:

```text
fugue app metrics <app> --since 15m
fugue app logs query <app> --since 15m --grep timeout
fugue app requests <app> --since 15m
fugue app diagnose <app> --window 15m
```

Zero-code mode can identify platform and edge symptoms. It cannot identify
business-internal stages such as authorization checks, dependency selection,
DB pool wait, retry loops, or event loop lag unless the app emits those stages.

## Export

Use the export command to capture one app-scoped diagnostic bundle:

```text
fugue app observability export <app> --since 30m
fugue app observability export <app> --since 30m --trace trace_123
```

The bundle is assembled from existing app observability APIs. RBAC, retention,
query limits, and audit behavior remain enforced by the control plane.

This is an operator diagnostic export. It is not a business log export, billing
export, audit export, or customer-facing request-history export.

## Instrumented Mode

Instrumented mode is opt-in. Apps can send low-cardinality metrics, structured
diagnostic logs, and OTLP traces/logs/metrics to the Fugue telemetry agent.

Recommended OpenTelemetry environment shape:

```text
OTEL_EXPORTER_OTLP_ENDPOINT=http://<telemetry-agent-service>:7834
OTEL_EXPORTER_OTLP_PROTOCOL=http/json
OTEL_RESOURCE_ATTRIBUTES=fugue.tenant_id=<tenant>,fugue.project_id=<project>,fugue.app_id=<app>,service.name=<service>
```

Recommended stage names are generic and app-owned:

```text
request_received
body_parsed
auth_checked
dependency_selected
db_pool_wait
db_query
upstream_pool_wait
upstream_headers_received
first_payload
stream_end
usage_recorded
```

Prometheus labels must stay low-cardinality. Do not put trace IDs, request IDs,
user IDs, session IDs, credentials, emails, full IPs, or request bodies into
metric labels.

## Beta Rollout Checklist

Before opening observability to a beta tenant:

- Confirm default retention is acceptable, or configure an app retention override.
- Configure tenant and app ingest quotas.
- Confirm logs, metrics, and analytics sources report `available`.
- Confirm the tenant can query only its own apps.
- Confirm `fugue app observability export` works for a small recent window.
- Confirm the app does not depend on Fugue Observability for product-visible data.
- Confirm quota drops appear in telemetry agent metrics if the tenant exceeds quota.

## Initial Production SLOs

These SLOs apply to Fugue Observability as a diagnostic subsystem:

- Query API availability: 99.5% monthly for enabled observability backends.
- Freshness target: recent logs/request facts visible within 60 seconds under normal load.
- Default retention: 24 hours.
- Diagnostic export: best effort over the requested retained window.
- Data durability: diagnostic data may be lost if the observability backend is
  unavailable or retention expires; app business state must not depend on it.

The app control plane, app runtime, app billing, and app-owned databases must
continue operating when Fugue Observability is degraded or unavailable.

## Disaster Recovery

If a metrics/logs/analytics backend is lost:

- Keep app runtime traffic serving.
- Keep app deploy/control-plane operations serving.
- Mark observability source status as degraded or unavailable.
- Recreate the backend from Helm-managed templates.
- Resume ingest from current runtime logs and new telemetry.
- Do not claim unrecoverable expired diagnostic data as product data loss.

Cold archive and long-retention export can be added per plan, but they must be
explicitly priced and scoped. They do not replace app-owned business storage.
