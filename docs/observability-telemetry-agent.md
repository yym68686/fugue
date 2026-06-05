# Fugue Telemetry Agent

This document captures the first implementation slices for Fugue Observability.
The agent is intentionally safe-by-default until the Prometheus, Loki, and
ClickHouse backends are deployed behind explicit configuration.

## Boundaries

- Observability is disabled by default.
- Missing or unhealthy observability exporters must not block app requests,
  app lifecycle operations, or control-plane startup.
- Fugue Observability stores diagnostic copies only. App product data,
  user-visible request logs, billing state, and business audit data remain
  owned by the app's own data layer.
- The default diagnostic retention window is 24 hours.
- Backend DSNs, tokens, credentials, raw request bodies, cookies, and
  authorization headers must not be returned by status APIs or metrics.

## Configuration

The shared configuration is read from environment variables:

```text
FUGUE_OBSERVABILITY_ENABLED=false
FUGUE_OBSERVABILITY_RETENTION=24h
FUGUE_OBSERVABILITY_METRICS_REMOTE_WRITE_URL=
FUGUE_OBSERVABILITY_LOKI_URL=
FUGUE_OBSERVABILITY_CLICKHOUSE_DSN=
FUGUE_OBSERVABILITY_OTLP_ENDPOINT=
FUGUE_OBSERVABILITY_EXPORT_TIMEOUT=5s
FUGUE_OBSERVABILITY_QUEUE_SIZE=4096
FUGUE_OBSERVABILITY_SAMPLE_RATE=1
FUGUE_OBSERVABILITY_RUNTIME_LOG_PATHS=
FUGUE_OBSERVABILITY_PROMETHEUS_SCRAPE_URLS=
FUGUE_OBSERVABILITY_SCRAPE_INTERVAL=30s
FUGUE_OBSERVABILITY_BATCH_SIZE=128
FUGUE_OBSERVABILITY_MAX_PAYLOAD_BYTES=1048576
FUGUE_OBSERVABILITY_MEMORY_LIMIT_BYTES=67108864
FUGUE_OBSERVABILITY_RETRY_MAX_ATTEMPTS=3
FUGUE_OBSERVABILITY_TENANT_ID=
FUGUE_OBSERVABILITY_PROJECT_ID=
FUGUE_OBSERVABILITY_APP_ID=
FUGUE_OBSERVABILITY_RUNTIME_ID=
FUGUE_OBSERVABILITY_COMPONENT=telemetry-agent
```

The telemetry agent also reads:

```text
FUGUE_TELEMETRY_AGENT_BIND_ADDR=:7834
```

In Kubernetes, exporter endpoints and DSNs should be injected through an
existing Secret referenced by Helm `observability.exporterSecret`, not stored in
plain values files. The chart maps these optional secret keys into:

```text
FUGUE_OBSERVABILITY_METRICS_REMOTE_WRITE_URL
FUGUE_OBSERVABILITY_LOKI_URL
FUGUE_OBSERVABILITY_CLICKHOUSE_DSN
FUGUE_OBSERVABILITY_OTLP_ENDPOINT
```

The chart also includes a default-disabled Prometheus trial deployment under
`observability.metrics`. When explicitly enabled, it stores data in an
`emptyDir` and uses the same 24 hour diagnostic-retention default. It is meant
for platform-internal trials before a production multi-tenant metrics backend
is selected.

The chart includes a default-disabled Loki trial deployment under
`observability.logs` with filesystem storage and the same 24 hour diagnostic
retention default. It is also intended for internal trials before production
multi-tenant log storage is enabled.

The chart includes a default-disabled ClickHouse trial deployment under
`observability.analytics` with `emptyDir` storage. It is only a platform
internal analytics trial path; it is not a business database and does not
replace app-owned product logs or billing data.

## Current Runtime Contract

`fugue-telemetry-agent` currently exposes:

```text
GET /healthz
GET /readyz
GET /metrics
POST /v1/logs
POST /v1/metrics
POST /v1/traces
```

`/readyz` reports observability as:

- `skipped` when disabled.
- `degraded` when enabled but no exporters are configured.
- `degraded` when configured exporter endpoints are syntactically invalid.
- `ok` when at least one exporter is configured and configuration validates.

The control-plane API mirrors the same non-critical observability status in
its `/readyz` checks. A degraded observability status does not make the API
return HTTP 503; only critical store readiness failures do that.

## Pipeline Contract

The first pipeline implementation is a guarded local pipeline:

- Runtime log input tails configured file paths only. It starts at end of file
  so enabling it does not ingest old logs by surprise.
- Prometheus input scrapes configured HTTP(S) URLs and records sample counts.
- OTLP HTTP input accepts `/v1/logs`, `/v1/metrics`, and `/v1/traces`. It does
  not persist raw payload bodies in the skeleton path.
- The identity processor injects configured tenant/project/app/runtime/component
  attributes.
- The redaction processor drops secret fields and masks common secret
  assignments in log text.
- The memory limiter and bounded queue drop telemetry instead of blocking.
- The batch/retry exporter currently uses a no-op exporter until real backends
  are configured in a later stage.

When observability is disabled, ingestion endpoints return accepted responses
but do not export data. This keeps app-side telemetry exporters from turning
observability outages or disabled mode into request-path failures.

## Next Implementation Steps

- Add Secret/ExternalSecret-backed exporter configuration.
- Add Loki exporter for runtime logs.
- Add Prometheus remote-write exporter for scraped metrics.
- Add ClickHouse exporter for request facts and spans.
- Replace the no-op exporter with typed exporters selected by data kind.
