# Observability Pilot

`fugue-observability-pilot` is an internal verification tool for the Fugue observability stack. It emits synthetic, redacted telemetry that covers the common app shapes used in the first platform pilot:

- HTTP request summaries.
- Streaming request summaries.
- Background worker checkpoints.
- Managed database wait/query spans.
- Low-cardinality runtime metrics.

The tool writes only observability data through the telemetry-agent HTTP receiver. It does not write app product data, billing data, user-visible request logs, credentials, request bodies, emails, cookies, authorization headers, or full client IPs.

## Build

```bash
make build-observability-pilot
```

## Dry Run

```bash
bin/fugue-observability-pilot --dry-run --count 2
```

Dry run prints the exact payloads without sending them.

## Internal Cluster Run

Run from a shell that can reach the internal telemetry-agent service, or from a temporary cluster exec session:

```bash
bin/fugue-observability-pilot \
  --endpoint http://fugue-fugue-telemetry-agent:7834 \
  --app-id app_observability_pilot \
  --scenario pool-wait \
  --count 24
```

The command prints a `trace_id` and verification commands:

```text
fugue app requests <app> --trace <trace_id> --since 15m
fugue app traces <app> <trace_id>
fugue app diagnose <app> --since 15m
```

## Scenarios

| Scenario | Purpose |
| --- | --- |
| `normal` | Confirms baseline request, log, metric, and span ingestion. |
| `pool-wait` | Confirms span-derived bottleneck reporting for pool/database waits. |
| `error-burst` | Confirms error burst and 5xx diagnosis paths. |

## Product Data Boundary

Pilot request summaries are diagnostic copies only. If an app exposes request logs, recent calls, billing explanations, or audit views to its own users, those remain the responsibility of the app's business data layer. Fugue Observability can store a short-retention diagnostic copy, but it must not be the only source of truth for app product features.
