# Request Attribution Runbook

## Trigger

Use this when a user reports a slow or failed request, or when alerts report
`edge.body_read_slow_spike`, `edge.origin_dns_error_spike`, or
`request.upstream_unavailable_spike`.

## Read-Only Diagnosis

```bash
fugue admin request explain <request-id> --json
fugue admin edge quality-rank <hostname> --json
fugue admin traffic-safety explain <hostname> --json
```

Check request id, edge id, edge group id, hostname, route generation, runtime
node, error class, body-read metrics, TCP retrans/RTO, and origin phase timings.

## Error Classes

- `auth`: authentication or authorization failure.
- `quota`: rate or quota failure.
- `business.4xx`: expected application 4xx.
- `origin.5xx`: upstream application failure.
- `edge.body_read_error` or `edge.body_incomplete`: client-to-edge upload path.
- `edge.upstream_unavailable.origin_dns`: origin DNS path.
- `edge.upstream_unavailable.origin_connect`: origin TCP connect path.
- `edge.upstream_unavailable.timeout`: upstream wait timeout.
- `edge.upstream_unavailable.origin_unavailable`: no narrower phase evidence.

## Recovery

1. For body-read issues, compare scoped edge ranking by request size class.
2. For DNS/connect issues, inspect edge-to-origin and node DNS health.
3. For origin 5xx or timeout, inspect app/runtime health before moving edge
   traffic.
4. Keep secrets out of incident notes; use the secret-safe explain output.

## Verification

- New samples show the error class has cleared or moved to the expected owner.
- Traffic safety and edge ranking agree on the selected edge.
- The incident records the explain command used for evidence.
