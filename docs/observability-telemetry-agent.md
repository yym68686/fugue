# Fugue Telemetry Agent

This document captures the first implementation slice for Fugue Observability.
The agent is intentionally a safe, no-op skeleton until the Prometheus, Loki,
and ClickHouse backends are deployed behind explicit configuration.

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
```

The telemetry agent also reads:

```text
FUGUE_TELEMETRY_AGENT_BIND_ADDR=:7834
```

## Current Runtime Contract

`fugue-telemetry-agent` currently exposes:

```text
GET /healthz
GET /readyz
GET /metrics
```

`/readyz` reports observability as:

- `skipped` when disabled.
- `degraded` when enabled but no exporters are configured.
- `degraded` when configured exporter endpoints are syntactically invalid.
- `ok` when at least one exporter is configured and configuration validates.

The control-plane API mirrors the same non-critical observability status in
its `/readyz` checks. A degraded observability status does not make the API
return HTTP 503; only critical store readiness failures do that.

## Next Implementation Steps

- Add optional Helm deployment for `fugue-telemetry-agent`.
- Add Secret/ExternalSecret-backed exporter configuration.
- Add runtime log collection pipeline.
- Add Prometheus scrape and remote-write pipeline.
- Add OTLP receiver.
- Add identity and secret-redaction processors.
- Add batch, retry, and memory limiter processors.
