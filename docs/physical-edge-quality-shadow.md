# Physical-edge network quality shadow

## Release boundary

This is the first executable part of the second phase, not completion of the
production routing migration. `GET /v1/edge/quality-shadow/{hostname}` and
`fugue admin edge quality-shadow capture` are platform-admin-only, read-only
observations. The evaluator is not called by DNS answer generation or the
automatic publication producer. It has no probe executor. `promotion_ready` and `probe_executed` are
always false; `dns_unchanged` is always true. A hypothetical switch is not a
serving authorization.

## Cohort network evaluator v2

New captures use `physical-network-cohort-v2`. Older receipts continue to replay
with their captured v1 evaluator; this is historical replay compatibility, not
the production legacy DNS ranking path. The v2 evaluator is still shadow-only.

V2 compares candidates within common observed TCP-peer /24 or /48 networks.
It never treats unrelated client populations, a DNS resolver address, country
labels or an observer-local probe as the same terminal path. Every shared
cohort used for a proposed switch must show sustained advantage in the last
three complete five-minute buckets. A pair with no common cohort cannot win a
normal switch; it is eligible for a bounded probe recommendation. This is
evidence about those observed networks, not a guarantee for every unobserved
terminal sharing the global DNS answer.

The core gates require current route/TLS proof, measured client and service
network RTT, and fresh authenticated physical-node CPU/memory headroom. The
score does not consume HTTP TTFB, inference wait, application queueing or
connection duration. Latency bands use observed P10/P90 variation plus an
explicit margin, without pretending repeated observations are independent
confidence samples. Optional throughput and failure rates remain unknown until
their denominators and provenance exist. Missing optional fields share one
finite uncertainty budget instead of multiplying an overwhelming penalty or
permanently excluding a node. That budget is configured risk tolerance, not a
physical bound on unknown behavior or a statistical confidence interval.

The experiment defaults use a total 30ms optional-uncertainty budget, a 5ms
margin per latency segment, at least 20ms and 15% band-separated improvement,
a 15-minute switch cooldown and a maximum 85% node resource utilization. These
are explicit captured experiment parameters, not serving policy owned by code.
Production publication must consume an independently signed policy and its
approval gates. Unknown switch history prevents a normal switch; actual hard
failure can bypass comparative cooldown. Unknown but freshly route-ready
physical edges remain last-resort fallback candidates after the quality-ready
set. They cannot displace a healthy primary solely because their measurements
are absent. No country is hardcoded or excluded by this evaluator.

Receipts include per-cohort comparisons, original capacity denominators and
bounded probe recommendations. No probe is executed by this evaluator. Node
capacity failure, missing shared cohorts and stale core samples remain visible
in candidate metrics and gates rather than being hidden behind unconditional
legacy-observation blockers.

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

## Public Front connection evidence

`public_front_tcp_info_v1` records are a separate optional network observation.
Their `client_network` payload contains kernel RTT, minimum RTT, variance and
raw retransmission counters from the public Front's downstream TCP socket, not
the worker's loopback connection to Caddy. A missing TCP_INFO snapshot remains
explicitly unavailable; successful connections alone do not establish a network
failure denominator, available throughput or a capacity ceiling.

Collection requires an explicitly configured `FUGUE_EDGE_FRONT_NETWORK_SOCKET`
on both Front and worker. The default is disabled. The node-local Unix socket is
0600, uses an exclusive ownership lock, and never exposes an HTTP network port.
An exact live remote address/port, physical edge, group and worker slot must
match uniquely. Only the trusted loopback Caddy hop with PROXY protocol enabled
can supply the overwritten connection identity. Nonces and bounded freshness
prevent response reuse. Raw peer endpoints are transient, not retained: stored
scope is IPv4 /24 or IPv6 /48, explicitly labelled `tcp_peer`, not an assertion
about the original terminal, recursive resolver or ECS prefix.

The lookup is asynchronous, allows only one in-flight request per worker, times
out after 300 ms, and observes process and per-route rate limits. Front refuses
over-budget scans or contended locks instead of delaying traffic. The bounded
32-record queue reserves capacity for both network segments under one-sided
traffic. Socket errors and missing connections affect observations only. An
older API's exact rejection of `client_network` also retries the heartbeat once
without the optional `network_samples`, preserving ordinary heartbeat fields.

Receipt binding still requires the exact hostname, route digest, path and
physical identity, with either the current bundle or an independently witnessed
historical bundle as described below. Global shadow aggregation is not proof
that different edges saw the same client population. A scoped observation can
only use the same recorded TCP-peer cohort; no ASN, terminal or ECS mapping is
invented. These observations do not authorize routing, remove missing-capacity
or missing-provenance gates, publish configuration, or modify LKG.

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

### Transport-selected Front sampling

The node-local observation socket is not proof that its process owns the public
listener. A separately published public Service can forward traffic to a
different retained Front while the old process remains Ready. When the local
socket cannot bind the request, the worker can asynchronously query
`POST /v1/edge/network-observation` using its exact node-scoped credential.
The API resolves the declared public Service, validates its digest, local
EndpointSlice and running Pod identity, reads that process's existing live TCP
inventory, then rechecks Service, Pod and EndpointSlice versions. Only a unique
established HTTPS connection with the same peer endpoint and worker slot can
produce a sample. The retained sample includes the transport identities but
never the raw client endpoint. An unavailable or changed transport yields no
sample and does not block the business request or DNS. This diagnostic fallback
does not restart, replace or reconfigure the serving Front.

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

This is a migration prerequisite, not production routing acceptance. Production
activation requires a separately published query policy and an explicitly scoped
canary. Removing the legacy execution path before that cutover would break the
currently published artifact compatibility boundary.

### Explicit physical publication adapter

The query policy may opt individual owned dynamic hostnames into `physical_routes`.
Each entry declares the traffic class and every v2 cost, window, capacity gate,
cooldown and probing budget; the serving binary does not fill missing configuration
with its shadow defaults. Country preferences are not a part of this contract.
No production hostname is opted in by a code release. The initial adapter supports
IPv4 global answers only; static records, pinned placement, shared aliases and
unmeasured IPv6 assignments are rejected rather than silently changed.

For each opted-in hostname and DNS consumer, the producer captures a real answer
from the selected public DNS backend. Before compiling an order it independently
replays that answer and reconstructs the network observations from captured raw
samples and exact route witnesses. A recalculated shadow digest cannot substitute
an invented measurement, omitted unfavorable record, desired publication or
caller-supplied successful-replay flag. The complete receipt travels in the
immutable runtime input's `physical_evidence`, separately from configuration intent
and the compact signed execution order. Missing evidence rejects the new
compilation; it neither changes the serving pointer nor clears the positive LKG.

The optional signed `primary_since` records a conservative assignment start.
Renewing evidence for the same primary preserves it, changing primary resets it,
and first enabling the physical policy starts it at capture time rather than guessing past
residence. Actual DNS evidence may expose it only when the answer is the signed
physical primary. Legacy scoring, readiness fallback and old artifacts lacking
this field do not manufacture a physical cooldown history. This timestamp is not
proof that an endpoint stayed healthy or every terminal used it continuously.

The adapter strips group ranking, geography, exploration and candidate weights
from opted-in queries. Other query rules and static DNS intent remain unchanged.
Code rollout, an adapter unit test or a signed order preserving the existing
primary is not evidence that a production detour has been eliminated.

The `physical_dns` producer reconfiguration operation is a separate configuration
transaction. It accepts only a continuously serving global predecessor, an exact
current full publication produced by that predecessor, and its unexpired verified
positive LKG. It may update the producer generation and pinned DNS policy only.
Both old and new input artifacts must have valid signatures; the static input,
topology, constraints, schedules and rollout settings stay unchanged. The DNS
policy may add exactly one `physical_routes` hostname without changing existing
entries or any other query strategy. A stale fence, changed source, frozen lane or
newer pending gray publication rejects the whole operation. File-store and isolated
PostgreSQL tests verify that success, rejection and idempotent retry do not rewrite
the serving full publication or its LKG. Candidate publication remains subject to
the independent evidence compiler and normal canary/readiness gates.

`scripts/reconfigure_physical_dns.py` is the bounded configuration executor for
this transaction. A single explicit declaration under
`deploy/environments/production/routing-physical-dns/` goes through the separate
`physical_dns_reconfiguration` lane in the normal CI entrypoint. That lane does
not depend on a component build or deploy, and does not run when only code
changes. Superseded declarations or executor changes stop an older run. The
executor preserves the exact predecessor's static source, controls and schedule;
creates and validates immutable DNS/producer inputs; and requires an actual bound
physical projection before attempting the transactional release. It rechecks the
full publication and positive LKG immediately before that attempt. A hold can
establish the physical primary's observation clock, but neither activation nor a
successful preview is no-detour acceptance. DNS receipts, offline replay and
production validation of the subsequently published order remain separate.

The declaration schema is `fugue.physical-dns-reconfiguration/v1`, with integer
`generation`, canonical HTTPS `origin`, one `hostname`, `producer_generation`,
the full successor `projection_policy`, and a `precondition` containing
`operation: physical_dns`, exact `previous_policy` and `serving_full` publication
references, and `verification_evidence_hash`. Each publication reference includes
artifact ID, content hash, release ID and fencing token. No recovery, override,
country exclusion, source invention or direct LKG write is available in this
executor. A rejected attempt may leave validated immutable draft inputs, but
cannot change the active producer or serving publication. No business hostname is
enabled merely by installing this executor.

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

### Historical network evidence across bundle renewal

An identity-accepted active worker heartbeat can trigger one independent TLS
nonce HEAD route observation per physical edge per minute per API replica, with
at most four concurrent observations and a four-second total deadline. The
target IP comes from registered node inventory, not the reported sample. The
existing route-proof endpoint answers without forwarding to the application.
The heartbeat never waits for this observation, and failure cannot affect DNS,
business requests, configuration or positive LKG.

The retained `route_tls_witness_v1` record is not a latency or availability
measurement. It binds an exact physical edge, group, hostname, path, class,
bundle version and route digest. A heartbeat caller cannot submit this source.
If an unchanged route is republished between the triggering measurement and
TLS lookup, the collector retains the actual new bundle version reported by
TLS. It does not relabel the triggering old measurement; that measurement
still needs its own exact-version witness. A later matching measurement may
use the independently recorded new-version witness.
Witnesses use a separate observation collection and database hostname-key
namespace; rolling-upgrade or rollback readers of the original network-sample
collection never receive an unsupported source. No database migration or
serving-artifact update is required.
It can link an immutable measurement only within two minutes of the TLS
observation and before its certificate/route validity deadline. A historical
bundle is accepted into shadow only if this original witness is captured and
a fresh actual DNS receipt independently proves the same current route content
on that same physical edge. Publication renewal alone is not a route change;
a changed digest, foreign edge/path/class, missing witness or missing current
proof still prevents binding. An expired historical witness never renews live
readiness. Original versions and sample times remain intact for offline replay.

Derived observations identify `route_witness_id`; replay validates the exact
raw measurement and witness rather than trusting the derived metric. This
removes the evidence-window reset at each bundle renewal without relaxing
promotion gates or making old samples current. It does not supply missing
failure denominators, capacity, throughput or terminal-path coverage.

The witness collector also attempts a bounded authenticated Kubernetes node
capacity read. It records Kubelet CPU nanocores and memory working-set bytes
against the same physical node's allocatable CPU and memory, with the original
metric timestamps, node UID and pressure conditions. Identity, readiness,
limits and pressure are reread before accepting; absent, stale, future or
foreign metrics remain unknown. Over-limit values and known pressure are
retained rather than concealed as absent or zero. A capacity-read failure
does not discard the independently valid route witness. This describes only
physical-node resource headroom, not a measured link bandwidth, a worker's
concurrency limit or the origin application's capacity. These raw observations
do not by themselves remove production-promotion gates.

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
