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

Use the actual `X-Fugue-Edge-Request-Id`, application request ID, or trace ID.
The endpoint reads individual `request_facts` and incomplete platform request
facts in `app_events`. Aggregate `edge_perf_*` sample IDs are not request IDs.
A trace or application request ID matching multiple records is ambiguous; use
the individual edge request ID to narrow it. No current route is substituted for
the recorded request's topology.

Read `evidence.lookup_status` before drawing conclusions: `found`, `not_found`,
`not_configured`, `backend_unavailable`, `ambiguous`, and `invalid_record` have
different meanings. An empty time-window lookup does not prove the request never
reached the platform, and does not distinguish expired evidence from collection
loss. The query is bounded and never changes serving state.

## Error Classes

- `edge.body_read_error` or `edge.body_incomplete`: recorded body-read error or
  explicit incomplete-body measurement. This does not by itself identify why
  the client/proxy/origin transfer stopped.
- `http.request_timeout`: recorded HTTP 408 without narrower body evidence.
- `http.error_response`: recorded HTTP error without an inferred owner.
- `incomplete_request_fact`: a record was found but it lacks an HTTP status.
- `evidence_unavailable`, `not_observed`, or `ambiguous_request`: inspect lookup
  status and coverage before attributing the failure.

Attribution lists observed stages, not proven root causes. A positive DNS or
connection duration alone is not evidence that DNS or connect failed. Omitted
byte counters are not zero measurements or proof of an incomplete upload.

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
