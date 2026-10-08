# Physical-edge network quality shadow

## Release boundary

This is the first executable part of the second phase, not completion of the
production routing migration. `GET /v1/edge/quality-shadow/{hostname}` and
`fugue admin edge quality-shadow capture` are platform-admin-only, read-only
observations. The evaluator is not called by DNS answer generation or the
automatic publication producer. It has no probe executor. `promotion_ready` and `probe_executed` are
always false; `dns_unchanged` is always true. A hypothetical switch is not a
serving authorization.

An explicit `physical_quality` query contract now supports a signed immutable
physical-edge order. Its compiler adapter requires replay-verified, fresh network
evidence and a matching actual DNS binding. DNS execution only filters this order
through independently proven readiness; group weights, country preferences,
application timings and random exploration cannot override it. This contract is
not automatically emitted or enabled by deploying the code. Existing published
policies remain readable until their replacement is accepted. The legacy producer
and selector must not be declared retired while those publications still depend
on them.

The existing serving score, group policy, country behavior, exploration and
cooldown remain unchanged. In particular, deploying this API does not fix an
existing detour. Do not report a candidate SERVFAIL, an API read, or a successful
shadow replay as public DNS or application acceptance.

## Origin socket evidence

Serving workers now sample kernel TCP RTT at the origin transport's `GotConn`,
before application response wait. Collection is passive: it opens no additional
connections and sends no business requests. It admits only direct, single-origin
Kubernetes Service routes, excludes peer fallback and loopback/link-local hops,
and binds each record to the physical edge, hostname, path, route-proof digest,
loaded bundle version and configured destination. Reused origin connections are
allowed; unavailable or zero kernel RTT remains unknown, never zero-cost proof.
The declared route determines the traffic class, not cache lookup activity.

The observer uses a nonblocking lock, a one-second process budget, a one-minute
route interval and a 32-record ring. It does not wait for telemetry persistence
on the proxy path. Heartbeat ingest checks the authenticated physical identity,
active worker declaration, loaded bundle and timestamp without rebinding foreign
records. Records are immutable on retry, retained for an hour, and stored apart
from legacy HTTP performance samples. Ingest errors do not reject an otherwise
accepted heartbeat or alter DNS. No serving artifacts, LKG or business policy
are modified by the observer.

During mixed-version rollout or API rollback, an exact old-API rejection of
the unknown `network_samples` field retries the same heartbeat once with only
that optional field removed. Identity, authentication, health and existing
performance data are preserved. Authentication failures, other validation
failures and server errors are never converted into a successful heartbeat.

Shadow receipts include these captured records as optional `network_samples`;
replay covers them in the receipt digest. They are deliberately not promoted to
scores merely because a worker reported them. Current public route proof still
has to bind the digest independently. Success-conditioned RTT observations do
not establish a failure rate, throughput, terminal-to-edge path or capacity
limit. None of those missing signals is fabricated. Old receipts without the
optional field retain their original digest and replay behavior.

## Verified measurement gaps

The existing `edgeDNSLatencyScore` includes HTTP TTFB, upstream duration, total
duration, origin response wait and origin total in its score. These are not
pure network measurements. The current request sample producer in
`internal/edge/service.go` does not populate `ClientTCP*` or
`OriginEndpointConnectMS`. The front has TCP_INFO observations, but the worker's
downstream connection can be a local proxy hop. A field name, a group rollup,
or a tiny loopback RTT does not prove the terminal-to-physical-edge path.

Likewise, nonzero route counts and a node TLS status do not prove that a
particular hostname and exact route generation are currently served there.
The legacy adapter therefore retains these measurements as diagnostics only.
It does not fabricate network samples, capacity limits, route proof, current
physical-edge selection, or last-switch time from current rankings. All these
missing inputs are visible and block promotion. This is an identified limitation
of the current evidence pipeline, not proof of any historical user's detour.

## Input and cost model

The pure evaluator in `internal/edgequality` consumes a captured snapshot:

- Explicit versioned policy, capture clock, hostname, traffic class, client scope.
- Physical `edge_id`, route generation, timestamped hostname route/TLS proof,
  and hard health/drain/exclusion gates. Group identity is informational; no
  sibling's observations are borrowed. Country is not a ranking input.
- Separate public-client RTT and service-endpoint TCP RTT/connect measurements,
  with explicit segment provenance. Local Caddy/tunnel listener measurements
  cannot become either public-client or final-service evidence.
- Network transfer throughput, separately attributed client and service network
  failure rates, and normalized capacity utilization against a known limit.
  HTTP 5xx, client cancellations, SSE lifetime, application queues and model
  inference time are not network failure or latency measurements.

Seven components contribute to the experimental score. RTT components use
milliseconds; the other components use explicit policy costs: bounded throughput
deficit, network failure probability and capacity utilization. Missing values
are `unknown`, distinct from observed zero, with a finite policy prior and
bounded uncertainty. They do not receive the legacy `999999` score.

The lower/upper bands are conservative **policy cost bands, not calibrated
statistical confidence intervals**. A record is one observation, not its
reported request count. Evidence must be fresh, match the exact context and
route generation, have sufficient independent records, and cover multiple
time buckets. Duplicate IDs do not increase confidence. Future, stale,
cross-class, cross-hostname, cross-scope and unmatched-edge records are rejected.

Switch hypotheses require the challenger's upper band to beat the incumbent's
lower band by both absolute and relative margins in consecutive completed,
non-overlapping buckets. The last switch must be known and cooldown expired.
An incumbent hard failure bypasses the comparative hold-down, but never the
replacement's health, fresh proof or evidence gates. Europe can win or lose
on the same evidence as any other physical edge.

Unknown eligible edges receive a deterministic round-robin **probe plan** with
an explicit interval and maximum budget. This code never converts it into live
user exploration or network traffic. A future executor must deduplicate by
interval, enforce the shared budget across replicas, and back off after failure.

## Capture and replay

```sh
fugue admin edge quality-shadow capture app.example.test \
  --traffic-class streaming --scope global --json > shadow.json
fugue admin edge quality-shadow replay shadow.json
```

To bind the capture to an actual public process answer, add
`--dns-node-id <node-id>`. The authenticated backend reader captures a recent
real receipt, replays its exact input, and retains it as `actual_dns_receipt`.
It uses the answer's actual publication, never the currently desired publication
or a newly computed ranking. Offline CLI replay verifies both receipts without
network access. Failed writes, expired route proofs, future observations,
ambiguous selection stages and mismatched hostname/scope remain blockers.

An origin socket record contributes only its service-side RTT when its physical
edge, group, hostname, path, declared traffic class, route digest and loaded bundle
version match that answer's still-fresh route proof. Multiple service-route proofs
are not silently collapsed into one route. This binding does not invent client
network cost, throughput, capacity, network failure denominators or switch time.
Unavailable audit backends affect this read only; they cannot block DNS serving.

## Physical selection artifact contract

An independent observer can capture a bounded client-side diagnostic without API
credentials or application requests:

```sh
fugue admin edge quality-probe app.example.test \
  --vantage affected-terminal --target edge-a=8.8.8.8 --rounds 3 --json
```

Use the actual configured edge address in place of the example. The command sends
only TLS nonce HEAD route proofs, verifies the returned physical identity and
records TCP connection timing separately from TLS/proof failure. Missing timings
remain null. The capture is explicitly `observer_local` and never routing
authorization. Transparent proxies can terminate the measured TCP connection;
neither a verified HTTPS proof nor a vantage label proves a direct terminal TCP
path. Do not import such measurements as population-wide DNS evidence.

`DNSPhysicalSelection` records the primary edge, ordered eligible fallback edges,
network evidence digest, actual answer ID, loaded digest, policy digest, exact
scope and capture time. The initial contract supports an explicit global query
view only; it rejects an unbound ASN/ECS scope instead of pretending it can infer
the requesting terminal's path. The order cannot include an unauthorized endpoint,
duplicate physical identity, legacy group override or live exploration budget.

Evidence age is checked when compiling a replacement. It is not a timer that
invalidates an already positive LKG. Fresh route/TLS readiness still independently
filters each answer. Failure immediately selects the next authorized ready
physical edge, including a sibling in the same group, without waiting for normal
quality-switch cooldown. Neither a failed compilation nor an audit failure writes
a new publication or clears an existing LKG.

This is a migration prerequisite, not production routing acceptance. The live
producer still needs verified two-segment collection, capacity/failure inputs,
an immutable evidence store and an explicitly scoped canary before it can emit
the new contract. Removing the legacy execution path before that cutover would
break the currently published artifact compatibility boundary.

The returned receipt includes the complete captured inputs, policy, result and
SHA-256 digest. Offline replay verifies the digest and exact result without an
API, credentials, current rankings, current clocks or external state. The digest
is an integrity check, not an authenticated telemetry signature or serving
artifact. This counterfactual must not be confused with Phase 1 actual-answer
receipts (`admin dns decisions explain/replay`). Export both when correlating
an incident.

New bound receipts declare `digest_format: embedded-json-sorted-v1`. Embedded actual
DNS JSON is normalized by object-key order without converting integer values to
floating point, so CLI redaction/formatting does not change evidence identity.
Receipts without the field retain their original digest algorithm; their exact
original encoding remains verifiable. Reordered exports of the original embedded
receipt format must be recaptured rather than silently accepting a bad digest.

The API uses an exact traffic class and explicit scope; it does not silently
fall back to platform or group averages. It bounds retained observations at
4096, scanned observations at 16384, and the data read at five seconds. Reaching
a bound is an explicit blocker, not a representative sample assertion. The
endpoint writes no configuration, artifacts, assignments or LKG state.

## Remaining production gates

1. Establish a tested no-interruption public DNS code release path and deploy
   actual-answer receipts to the real public processes. Bind each observation
   to the actual final RRset, loaded digest, physical edge and serving LKG.
2. Join front-side TCP observations to the physical edge, hostname and coarse
   client scope with trusted connection identity. Do not put raw client IPs into
   a durable routing receipt. A recursive resolver or ECS prefix is not a
   terminal path. No DNS-only design can promise per-request optimality.
3. Add bounded, authenticated endpoint probes from every eligible physical edge
   to the configured service endpoint, not a tunnel's local listener. Use TCP
   or a configured non-business probe; never replay user POSTs or inference
   requests. Record target, route/artifact generation, timeout, failures and
   actual measured time separately from application response wait.
4. Collect kernel/client-probe throughput with provenance, separately measured
   network failure denominators, and fresh capacity against configured limits.
   Do not treat zero legacy counters as observed zero failures.
5. Run the replayable model across peak and low-traffic periods. Exercise new
   edge sampling, capacity exhaustion, loss, service movement, proof expiry,
   probe failure, false HTTP failures, LKG recovery and process restart.
6. Only after sufficient evidence: compile an explicitly scoped signed canary
   policy through the existing intent/policy/artifact system. Compare actual
   canary RRsets and end-to-end client paths with shadow, then increase exposure.
   Static edge routes are outside this migration. Missing or conflicting facts
   prevent promotion; they never invalidate the positive serving LKG.

API deployment and code success alone satisfy none of gates 1–6. The experiment
policy is captured explicitly for replay, not owned by a serving binary. A live
rollout must reference independently recoverable signed configuration, preserve
previous positive artifacts on failure, and retain a configuration rollback path
even when a code release fails.
