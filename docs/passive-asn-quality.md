# Passive, cross-domain Edge quality: observation phase

The API starts one bounded observer by default. It reads already authenticated
public Front TCP samples across domains; it sends no traffic and consumes
neither the GitHub recovery observer nor the client-probe report stream.
Samples must match the current inventory group, address family and bundle.
Hostname, application timing, origin and health-probe metrics are not inputs
to the line comparison.

The observer classifies a coarse Front TCP peer prefix only when the active
producer's signed DNS client rules agree on one covering ASN. It does not
trust request ASN headers, infer from a covering /0, classify a partial
overlap, or turn the IP of a recursive resolver into the user's ASN. Empty,
unmapped, ambiguous and inconsistent mappings remain unknown. There is no
additional external data feed or cross-service disclosure of client prefixes.

Connections are deduplicated before comparing the same peer prefix, IP
family, and complete five-minute window. A result requires three shared
windows with at least three independent connections per edge in a common
prefix. Only qualifying public TCP ACK delivery rates and retransmissions
can contribute; low-volume or application-limited delivery is unknown. Tail
regressions or conflicts prevent a winner. These conservative shadow
thresholds are not a guarantee of significance or a measurement of RTT from
every client behind the ASN.

The latest original comparison per ASN, IP family and edge pair is stored in
a separate observation checkpoint. Missing or delayed samples do not replace
it. No hourly raw sample cleanup or elapsed age deletes it. Incompatible ASN
mapping snapshots do not reuse prior comparisons. The checkpoint is bounded:
additional pairs at the limit are reported rather than evicting retained
results. None of this state grants current route or TLS readiness.

API metrics `fugue_edge_passive_asn_observation` and the corresponding shadow
log line show connection attribution, read caps, retained comparisons, and
coverage from up to sixteen recent receipts per selected DNS process. Only
replay-verified successful A/AAAA receipts enter the DNS denominator.
`dns_ecs_asn_hint_upper_bound` is deliberately not usable ECS coverage:
replay redacts the source prefix and cannot validate that it lies wholly
inside a signed ASN mapping. These bounded diagnostic samples are not
request-, packet- or byte-level traffic shares.

This is the first deployment gate, not an ASN DNS policy. No public DNS
answer, candidate set, route pin, signed publication, or exploration budget is
changed by this binary. Before promoting traffic, supply a vetted
prefix-to-ASN source and establish usable coverage at the actual DNS
executor. A subsequent signed serving policy must preserve fixed routes,
intersect ASN rankings with the current per-hostname eligible candidates,
fall back without trustworthy ECS, enforce a capped default exploration
budget and stop on regressions. Do not use shadow comparisons as an ambient
DNS override or repurpose the recovery capacity proof as an ASN result.
