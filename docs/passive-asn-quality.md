# Passive, cross-domain Edge quality

The API starts one bounded observer by default. It reads already authenticated
public Front TCP samples across domains; it sends no traffic and consumes
neither the GitHub recovery observer nor the client-probe report stream.
Samples must match the current inventory group, address family and bundle.
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
positive answer with a classifier result. These bounded samples are not
request-, packet- or byte-level traffic shares.

For the existing signed `all_dynamic` policy, each authoritative process
fetches a private HMAC-signed observation bound to its exact artifact, consumer
set, release and fence. Older Worker readers do not need new policy fields.
The observation starts in shadow mode. It may change answers only after all
configured independent authorities have verified classified ECS on their
selected process and current full release. A restarted/replaced authority
has to reestablish that proof.

DNS accepts only a complete matching-family ECS prefix within a known
`/24` or `/48` from a socket peer in the vendor-published
[Google Public DNS authoritative egress ranges](https://developers.google.com/speed/public-dns/faq).
No resolver address, ECS opt-in flag, or provider ASN is used as the client's
ASN. Non-ECS queries and unknown or unavailable mappings keep the baseline
answer. Eligible ECS replies use the query prefix as their cache scope,
including during shadow and fallback, without using it to change the answer.

ASN ranking can select only a still-ready member of the signed serving order
of a single-address dynamic record. Fixed per-name orders, explicit physical
routes, redundant responses and business allocation keep their existing
semantics. A recovered edge can compete immediately with retained compatible
evidence once it has a current route/TLS proof and is back in that order.
Missing comparisons keep the existing answer, except for at most one in 200
eligible classified DNS responses per executor offered to exploration. An
exploratory reply has at most a 30-second TTL (and never outlives readiness).
DNS caches mean this is **not** a bound on the resulting HTTP traffic share.
Already-compared pairs are not exploration targets; conflicting comparisons
retain the baseline. Historical comparisons have no age expiry; observation
leases, attribution, and route/TLS proofs still have freshness requirements.

The DNS decision receipt records the observation digest, mode, reason,
incumbent, considered edge, and the comparison timestamp and window counts,
including whether the evidence is historical. It replays the recorded wire
selection, not the private source-prefix classification. Neither the client
nor the resolver address is recorded in the receipt.
