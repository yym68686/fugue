# Passive, cross-domain Edge quality

The API starts one bounded observer by default. It reads already authenticated
public Front TCP samples across domains; it sends no traffic and consumes
neither the GitHub recovery observer nor the client-probe report stream.
The selected public Front records coarse kernel-peer samples after forwarding
HTTPS bytes in both directions or while an open socket is making bidirectional
data progress. Each connection contributes at most one Front observation;
idle snapshots are excluded. Each bounded bucket favors diverse prefixes.
Only closed buckets enter durable storage. This works without Worker route matches or an
exact live-connection lookup. The control plane reads a bounded batch through
the Kubernetes pod proxy, verifies the actual public Service, ready Front
process and EndpointSlices before and after the read, and records it with
that provenance. Internal peers, handshake-only connections, claimed Worker
batches and nonmatching listeners do not become samples.

Samples must match the current inventory group and address family. Legacy
Worker samples without a public transport witness remain tied to the original
bundle. A verified witness can survive a routine bundle change while that
same selected Front process and Service are still proven. The observer can
replay the preceding retained 30-minute window separately, with the same
comparison requirements and original timestamps.
Hostname, application timing, origin and health-probe metrics are not inputs
to the line comparison.

The observer classifies a coarse Front TCP peer prefix when the active
producer's signed DNS client rules agree on one covering ASN. If all declared
DNS client rules are empty, it can classify using local copies of the public
IPv4 and IPv6 [RIPE RISwhois bulk dumps](https://ris.ripe.net/docs/ris-whois/).
It downloads the two public files on a bounded background schedule; no
client prefix, resolver address or hostname is sent to RIPE. It does not
trust request ASN headers, infer from a covering /0, classify partial or
ambiguous coverage, or turn a recursive resolver address into the user's ASN.
Failed refreshes preserve the previous index; after 48 hours the mapping
is no longer trusted. This freshness limit is for BGP attribution, not for
historical quality measurements.

Connections are deduplicated before comparing the same peer prefix, IP
family, and complete five-minute window. A result requires three shared
windows with at least three independent connections per edge in a common
prefix. Only qualifying public TCP ACK delivery rates and retransmissions
can contribute; low-volume or application-limited delivery is unknown. Tail
regressions or conflicts prevent a winner. These conservative
thresholds are not a guarantee of significance or a measurement of RTT from
every client behind the ASN.

The latest original comparison per ASN, IP family and edge pair is stored in
a separate observation checkpoint. Missing or delayed samples do not replace
it. No hourly raw sample cleanup or elapsed age deletes it. Incompatible ASN
mapping snapshots do not reuse prior comparisons. For bulk BGP data, the
exact coarse prefixes used in a comparison are rechecked against the latest
index. A refresh that preserves those mappings preserves the original
evidence timestamp; a changed or ambiguous origin does not transfer evidence
to the new ASN. The checkpoint is bounded:
additional pairs at the limit are reported rather than evicting retained
results. None of this state grants current route or TLS readiness.

API metrics `fugue_edge_passive_asn_observation` and the corresponding
log line show connection attribution, read caps, retained comparisons, and
coverage from up to sixteen recent receipts per selected DNS process. Only
replay-verified successful A/AAAA receipts enter the DNS denominator.
`dns_ecs_asn_hint_upper_bound` is deliberately not usable ECS coverage:
replay redacts the source prefix and cannot recompute its BGP classification.
`dns_passive_verified` counts selected processes that have a replay-verified
positive answer with a classifier result. `dns_wire_verified` separately
counts selected processes whose currently loaded full release produced a
replay-verified positive answer, with or without ECS. These bounded samples are not
request-, packet- or byte-level traffic shares.

The Front and API expose low-cardinality per-Edge counters for eligible
connections, accepted samples, live samples, delivery observations, and
rejection/cap reasons. Fleet tiers are exported separately per IP family,
with the evidence timestamp, number of matched peers and basis in API logs.
Before there is enough same-prefix evidence, an operator-only provisional
RTT tier may use contemporaneous observations matched by ASN and family.
This is lower-confidence evidence because populations can differ within an
ASN. A lone sparse window is only reported when its median RTT gap is at
least 25%. These estimates are retained separately, with their original timestamps
and mapping checks, and are never signed as DNS comparisons. When current
evidence cannot place every online endpoint, the diagnostic rank may reuse
the last complete snapshot for exactly that family and endpoint set, after
rechecking its original ASN attribution. It keeps the original evidence
timestamps. The current partial result and its reason remain visible in
`fugue_edge_passive_fleet_live_rank` and the rank logs. To start with no
compatible history, a provisional vote-based tier can use a complete set of
measured pairs. It gives opponents equal weight, one vote per ASN and pair,
and accounts for ties. With neither source, tier zero means unranked.
Offline nodes are excluded from quality tiers.

For the existing signed `all_dynamic` policy, each authoritative process
fetches a private HMAC-signed observation bound to its exact artifact, consumer
set, release and fence. Older Worker readers do not need new policy fields.
The observation starts in shadow mode. Active mode requires a replayable
positive DNS answer from each configured independent public authority's
selected process and current full release. This verifies the live wire path
without waiting for a particular client traffic mix. Each active query must
still carry a trusted, sufficiently specific ECS prefix for a per-ASN change
or exploration. A genuinely non-ECS query may only get a verified cross-ASN
preference (not an experiment or assumed client ASN). A restarted or
replaced authority has to reestablish its wire proof. Both Kubernetes-bound global DNS and Cell DNS
fetch the same private format, but a global process cannot activate using a
Cell process's proof or vice versa. Each consumer uses its own expected-set
cohort; an idle slot can verify shadow without using the on-duty slot's
active proof.

DNS accepts only a complete matching-family ECS prefix at least as specific as
`/24` or `/48` from a socket peer in the vendor-published
[Google Public DNS authoritative egress ranges](https://developers.google.com/speed/public-dns/faq).
No resolver address, ECS opt-in flag, or provider ASN is used as the client's
ASN. Invalid ECS, untrusted ECS sources, and unavailable mapping or trust
snapshots keep the baseline answer. Eligible ECS replies use the query prefix as their cache scope,
including during shadow and fallback, without using it to change the answer.

ASN ranking can select only a still-ready member of the signed serving order
of a single-address dynamic record. The default dynamic projection appends
placement-authorized members after the configured baseline, without touching
fixed overrides. Source policy can mark an exact order as `bootstrap_only`
under `all_dynamic`: it retains the declared order as a seed and uses the
same placement-filtered admission as a new domain. The compiler publishes
the effective order, not the migration marker; unmarked fixed overrides
remain in the signed runtime override list. The fenced dynamic-quality
configuration lane may mark existing rows in a metadata-only transition,
without changing the order, membership, or other source settings.
A single-answer availability policy on a dynamic record
does not itself fix the primary. When the compiler publishes an availability
baseline or bound quality order, the healthy candidates in that signed order
can compete by ASN, even if a bootstrap override supplied the earlier order.
A real readiness failover still uses the signed fallback without passive
reordering. Effective fixed per-name orders, explicit physical routes,
independent static edge routes, redundant responses and business allocation
keep their existing semantics. A recovered edge can compete with retained compatible
evidence once it has a current route/TLS proof and is back in that order.
When no local comparison exists, a new ASN or a trusted, complete prefix
without a learned mapping can use cross-ASN evidence derived from the same
signed pairs. Each ASN contributes at most one vote per edge pair, separately
by address family. To promote a candidate it needs at least three supporting
ASNs and two-thirds of the comparable ASNs; ties count in the denominator.
An existing local tie, loss, or conflict cannot be overridden by this fallback.
Without a clear advantage the signed baseline remains the default. About 5%
of eligible trusted client-prefix/minute cohorts can explore an unknown
pair. Assignment is deterministic across DNS processes and across domains
with the same candidate set. This is **not** a bound on the resulting HTTP
traffic share: resolver caching and client demand can skew it. An exploratory
answer has at most a 30-second TTL and never outlives readiness. New exploration
is held for nodes missing verified Front feedback or showing a burst of
Front-to-Worker setup errors; route and TLS readiness are still required
for every candidate. Already-compared pairs are not exploration targets; conflicting comparisons
retain the baseline. Historical comparisons have no age expiry; observation
leases, attribution, and route/TLS proofs still have freshness requirements.

The DNS decision receipt records the observation digest, mode, reason,
incumbent, considered edge, and the comparison timestamp and window counts,
including whether the evidence is historical. A cross-ASN result instead
records the decisive pair's comparable and supporting ASN counts and the
oldest supporting evidence timestamp. It replays the recorded wire
selection, not the private source-prefix classification. Neither the client
nor the resolver address is recorded in the receipt.

Before a production upgrade, identify the serving workloads from each
authority's live decision endpoint (`backend.pod_name` and owner), not
from component names. A ready parallel or validation workload can load a
signed observation without owning the public listener. After rollout,
query both public addresses and confirm that the resulting receipts came
from the upgraded serving workloads. A successful shadow load establishes
observation, not active steering. Inspect replayed live decisions for mode
and comparison evidence; ordinary DNS transport probes can confirm active mode
and its baseline fallback, but cannot establish a real ASN-ranked answer.
