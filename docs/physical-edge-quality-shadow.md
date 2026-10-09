# Physical-edge network quality shadow

## Legacy production selector retirement

Production query selection accepts only signed `physical_quality` or
`physical_order` candidates. Geographic/group composite ranking and random
request exploration are no longer live execution paths. The old producer
scorer exists only in test reference files. The independent historical DNS
replayer can execute the captured old format only during offline receipt replay;
it cannot authorize a serving bundle or activate a policy.

The legacy `quality-rank` API returns 410 without computing a score. Request
rollups retain separate business and socket observations but do not calculate
the retired composite score. `admin dns answer-check --explain` reads retained
actual receipts; these are not guaranteed to be the receipts of that command's
probe. Match query and decision IDs for exact attribution and use
`admin dns decisions replay` to verify original evidence.

The migration configuration lane may explicitly resolve orders from the latest
verified full publication of the same producer. The declared record identities
and complete physical membership must remain unchanged. The transaction still
compares every resolved order against that exact signed baseline under locks.
Evidence retains the original declaration digest, resolved order digest, source
artifact and both identified business snapshots. MVCC snapshot IDs may differ
when the complete projected intent content is identical; equality of the IDs is
not a content-equivalence proof.

Configuration migration must reach verified full and positive LKG on every
serving and standby DNS instance before deploying retired execution code.
Previous code must already understand those signed physical orders. Rejecting
an incompatible new candidate never replaces the retained positive checkpoint.
Do not treat a successful migration transaction or offline artifact audit as
proof that the new code is already serving.

## Production migration evidence, 2026-10-09

The first opted-in production hostname now executes `physical_quality` from a
signed traffic publication. Both public DNS transports selected the compatible
A processes through independently declared transport generations 22 and 23.
The worker revision was `9bd81f51720446319ddd54660d963b94f00df678`; standby
DNS recovery then completed in CI run `37899891120` at revision
`66f1b335e46c40a3d7c1fa389728be5913cc7a7d`. Four DNS instances reported Ready
with zero restarts after that release. This observation does not retire the
legacy selector used by other published queries.

The immutable input for DNS artifact `artifact_1791530684_0048f1d1ccd6`
records a normal physical switch at 07:24:39 UTC: three complete comparison
buckets supported the US candidate, with challenger upper cost 138.10 and
incumbent lower cost 218.94. Both public authorities subsequently returned
`15.204.94.71`; the actual-answer bindings replayed successfully. Source
socket RTT samples were approximately 0.5 ms for that candidate, 23 ms for
the other US candidate and 156 ms for the European candidate. Client-path
measurements remained unknown, so this is evidence of the configured bounded
service-network comparison, not proof of every terminal's end-to-end route.

At 07:35:53 UTC, the immutable input for
`artifact_1791531355_aac959dbbe87` records a capacity-gated failover to the
other US edge. Its original evidence replays as `failover`, not a normal
quality switch. Subsequent public answers returned `95.169.10.156`; the
European endpoint remained eligible as fallback. No geography exclusion was
introduced. The full HTTP application behavior and every future route still
require their own validation; successful DNS answers alone do not establish
those properties.

Before removing execution code, audit every serving and recovery DNS artifact
with `python3 scripts/audit_dns_selector_retirement.py ARTIFACT_EXPORT...`.
The offline audit verifies exported content digests and exact query-rule
membership. Exit status 2 means nonphysical selector dependencies remain;
malformed or incomplete exports fail. The audit deliberately does not verify
signatures, live assignments or LKG, and never authorizes production retirement.
The artifact above still contained 251 distinct nonphysical dynamic hostnames,
so deleting its selector would break the serving artifact compatibility contract.

An earlier premature configuration activation exposed an executor-version
compatibility failure and caused DNS SERVFAIL. Recovery now restores the exact
verified positive LKG without requiring the failing candidate's live capability
facts. New physical publications require authenticated v3 executor capability.
Unselected incompatible standby DNS can be upgraded through the existing exact
predecessor recovery path; forward health checks and the public transport guard
remain mandatory. These safeguards do not turn an incompatible predecessor into
a newly verified LKG.

## Release boundary

An explicit `ordered_projection` can replace remaining unmeasured dynamic
queries with configured `physical_order` policies. The producer then bypasses
the legacy scoring catalog and country/group ranking. These configured orders
do not claim network optimality; measurement state stays unknown. Existing
`physical_routes` continue through the evidence compiler. Static DNS records,
route ownership and candidate constraints retain their separate authority.

The `retire_dns_selector` reconfiguration operation checks the exact signed
producer, source inputs and current full DNS member under the publication locks.
It requires no client mappings or scoped legacy profiles. Every existing dynamic
query needs a per-consumer override identical to the signed baseline's normal
global physical order, including all candidates. The explicit default covers
that candidate inventory for newly created records; route constraints still
apply. ECS and random exploration are disabled. Measured policies, TTL, static
intent, schedules and serving bounds cannot change in the same operation.
Changed order, membership, lineage, pending gray or expired LKG rejects the
transaction without changing serving publication or positive LKG.

Declarations in `deploy/environments/production/routing-dns-retirement` run via
the independent CI configuration lane and `scripts/retire_dns_selector.py`.
Code deployment alone cannot activate this projection. Every required executor
must support `physical_order_v1` and `physical_order_projection_v1` before
candidate admission. Legacy execution
cannot be removed until serving and recovery artifacts have actually migrated.
The frozen-order converter is for migration validation, never DNS request ranking.

This is the first executable part of the second phase, not completion of the
production routing migration. `GET /v1/edge/quality-shadow/{hostname}` and
`fugue admin edge quality-shadow capture` are platform-admin-only, read-only
observations. DNS answer generation never runs the shadow evaluator. An explicitly
opted-in publication producer can capture and independently validate its inputs
through the evidence compiler described below; installing the code alone does not
opt in a hostname. The evaluator has no probe executor. In shadow receipts,
`promotion_ready` and `probe_executed` are always false; `dns_unchanged` is always
true. A hypothetical switch is not itself a serving authorization.

## Versioned network evaluators

New captures use `physical-network-bounded-v3`. Older receipts continue to replay
with their captured v1/v2 evaluator; this is historical replay compatibility, not
the production legacy DNS ranking path. Its capture API remains shadow-only;
serving publication requires the separate explicit policy and evidence compiler.

V2 compares candidates within common observed TCP-peer /24 or /48 networks.
It never treats unrelated client populations, a DNS resolver address, country
labels or an observer-local probe as the same terminal path. Every shared
cohort used for a proposed switch must show sustained advantage in the last
three complete five-minute buckets. A pair with no common cohort cannot win a
normal switch; it is eligible for a bounded probe recommendation. This is
evidence about those observed networks, not a guarantee for every unobserved
terminal sharing the global DNS answer.

V3 keeps existing V2 receipts replayable and uses the same common-cohort
comparison when measured client populations overlap. If one side has no client
cohort, it compares service-network and node-capacity evidence only;
client latency remains explicitly unknown and receives the single bounded
unknown-cost budget. It never averages distinct measured client populations
into one global path, and distinct measured populations remain incomparable.
A switch still needs a sustained band-separated
advantage and a known cooldown. This fallback is a policy risk decision, not
a measurement of the missing user-to-edge path.

The core gates require current route/TLS proof, measured service network RTT,
and fresh authenticated physical-node CPU/memory headroom. Common-cohort
comparisons also require measured client network RTT. The
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

### Configured origin TCP-only probes

`FUGUE_EDGE_ORIGIN_NETWORK_PROBE_INTERVAL` (30 seconds through five minutes) and
`FUGUE_EDGE_ORIGIN_NETWORK_PROBE_HOSTNAMES` (at most eight explicit hostnames)
enable a separate background sampler. By default it is disabled. Each process
attempts at most one destination per interval, rotating hostname/path entries,
with a two-second deadline and no application bytes. The sampler reads only the
currently applied, healthy, unexpired serving index and rechecks active-slot
authority before retaining results. Candidates, exclusions,
peer/mesh fallback, weighted origins and external destinations are not probed.
An index change during the attempt discards its result rather than relabeling it.
An invalid observation configuration disables this sampler without disabling DNS
or the worker. Observational configuration never replaces signed serving intent.

`service_endpoint_tcp_probe_v1` preserves its distinct active provenance and the
raw `service_connect_failed` outcome. Failed connection/destination resolution
and unavailable TCP_INFO have null RTT. A successful connection reports only
kernel RTT, not resolution time, connect-wall-clock duration or HTTP wait. Probe
failures are retained for subsequent denominator-aware evaluation; v2 does not
invent a failure rate from them. Current public TLS proof and historical witnesses
must still bind the exact physical edge, route digest and bundle. Mixed-version
API rollback strips only the optional network extension on its exact unsupported
field rejection, preserving the ordinary heartbeat. The sampler changes no DNS
answers and does not provide terminal-to-edge evidence.

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

The separate `physical_order` contract is an explicit configuration order for
queries whose measured-quality migration is not yet complete. Its
`physical-order-v1` list is part of the signed answer rule and must exactly match
the compiled query policy. It does not carry or manufacture quality evidence.
DNS removes endpoints lacking current readiness and selects the first remaining
physical endpoint; candidate scores, weights, country hints and exploration do
not override the declared list. Deploying support does not rewrite any legacy
artifact. Publication requires `physical_order_v1` capability from the required
traffic executors. Migrating current configuration and recovery artifacts remains
a separate operation before retiring the old selector.

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

For opted-in producer captures, the collector issues one nonrecursive TCP A
query to each declared DNS consumer's fresh public endpoint. It binds only the
real retained answer with the same query ID, node, hostname, transport, observation
time and exact RRset, then verifies offline replay. Journal reads have a bounded
retry for asynchronous persistence. An unavailable or mismatching receipt blocks
the candidate without changing serving DNS. These control-plane queries prove
the authoritative execution path, not an end user's network latency. The admin
shadow capture endpoint remains passive and issues no DNS queries.

The route witness scheduler alternates controlled origin-probe records with
passive records when both are present. Each queue keeps its own hostname/path
cursor. This reserves half of the existing per-node witness budget for configured
network probes, so unrelated busy hostnames cannot consume their entire retention
window. The one-per-minute and four-in-flight bounds remain unchanged.

An explicit `baseline_mode: latest_verified_same_policy` allows a queued
configuration run to resolve a later full publication from the exact declared
producer policy. The fence must advance, the current full release must have its
own unexpired positive LKG, and the executor records both declared and resolved
preconditions. Compilation and the final atomic release use that one resolved
reference; subsequent changes still fail closed. The default `declared` mode
requires the original exact reference throughout.

The executor compares projection diagnostics with a fresh predecessor preview.
Existing migration equivalence advisories and an unchanged unrelated origin
observation may remain only when the complete configuration intent is identical.
New issues, target-host issues and validation failures still reject activation;
the evidence records the retained advisories. Normal compilation and gray/full
readiness independently validate the successor before it can serve.

Physical primary, ordered fallback identities and assignment start time are part
of the producer's change fingerprint. A changed physical decision triggers normal
publication before the periodic refresh; a new receipt ID or measurement timestamp
alone does not. Nonphysical source fingerprints retain their previous encoding.

Runtime input digests normalize object ordering inside embedded physical evidence
before hashing. JSONB storage may reorder keys; it must not change input identity.
The normalization preserves numeric tokens and all evidence values. Stored input
is still checked against its original digest, so changed measurements fail closed.

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

Eligible routes rotate by hostname, path and traffic class within that unchanged
per-edge budget. The latest sample is used within each route, not across all
routes; otherwise a busy hostname can indefinitely displace every less recent
eligible hostname. Rotation does not bypass freshness or identity checks, and
does not increase the four-probe in-flight bound. Per-edge cursors are capped at
256 and expire after ten idle minutes; a contended observer never blocks the
heartbeat. This improves proof scheduling, not terminal path coverage: absent
client measurements remain unknown.

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

A capture bound to a replay-verified real DNS answer also reads current node
capacity independently of route-witness scheduling and business request volume.
The read targets only registered physical nodes whose public address, group and
unique route proof match that answer. At most eight nodes, four concurrent
workers and a two-second total Kubernetes deadline bound the observation work.
The original raw facts are retained in `node_capacity_samples`; derived
observations reference `node_capacity_id` and are reconstructed during publication
and offline replay. Missing or expired capacity remains unknown. These current
facts cannot fill historical comparison buckets, create client/service RTT or
authorize a network switch on their own. Collection never runs on the DNS answer
path and cannot change a serving artifact or LKG.

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
