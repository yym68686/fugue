# Compare latency for the same model

A sticky canary can receive a reasoning model while the stable release receives
a fast model on the same HTTP operation. Comparing their latency as one cohort
can report a release regression caused by different request populations.

`gate.latency_cohort_model=true` opts an application into model-specific
comparisons. The default is false. The operator must trust the application's
model labels in the existing telemetry pipeline; this setting does not create
an authentication boundary or grant routing or billing authority.

The comparison uses application `request_facts` with `summary_json.model` (or
`model_id`) to partition the matching edge facts. Both facts must have the same
globally scoped app ID, nonempty request and trace IDs, HTTP method, path without
query parameters and streaming mode. Completion timestamps must be within five
seconds. The bounded correlation range must contain exactly one application
model receipt and exactly one edge request, including other release identities.
An explicit conflicting tenant, conflicting model aliases, duplicate identity,
invalid label, absent receipt or missing ID leaves that edge request unmatched.
Legacy application receipts with no tenant field are accepted only within the
same explicit app scope and these other checks. Labels are never inferred from
request contents. Generic unlabeled requests are not a fallback for this mode.

All latency values still come from the edge's HTTP observation. Application
TTFT, token usage, price or other billing fields never replace those values.
Each compared model cohort still requires the configured sample minimum on both
releases. Recent absolute latency limits, errors, active readiness and candidate
sample counts apply to every request. A real recent same-model regression cannot
be hidden by a longer window. Only a pure matched-sample deficit can use the
separately configured `latency_comparison_window_seconds` history.

SQL correlation is bounded by the observation window plus a five-second receipt
margin, ten seconds of execution time, 256 MiB of memory, one million scanned
rows and 100,000 joined identities. Overflow throws instead of supplying partial
evidence. The existing 512-cohort and response-byte completeness checks also
apply. Unsupported or unavailable correlation fails the comparison closed.

This capability does not change serving configuration. Enable it through the
normal continuity configuration API after both API and controller support it;
preserve all existing policy fields and verify the stored policy. Applications
without the explicit setting keep the previous comparison behavior.

Validation includes genuine ClickHouse execution over an inline VALUES fixture:
set `FUGUE_TEST_CLICKHOUSE_READONLY_DSN` and run
`go test ./internal/releaseflow -run '^TestModelLatencyClickHouseIntegration$'`.
That test only evaluates read-only expressions; it creates no tables, request
facts, traffic or release-gate samples.
