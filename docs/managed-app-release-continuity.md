# Managed application release continuity

For ordinary in-place public application rollouts, deployment completion now
requires the same serving proof used by the traffic compiler, not only aggregate
Ready replica counts. The controller observes the selected Service,
EndpointSlices, Pods, ReplicaSets and Deployment through the shared
`internal/releaseproof` verifier. Ownership, image, current cohort, readiness and
service port checks remain mandatory. Deployment and Service identities and
resource versions must remain stable across the bounded observation. Missing or
temporarily unreadable evidence stays pending within the existing rollout timeout.

The API also guards the independent observation window after a system-authored
stable release switch. If the new stable is not yet independently ready, and the
only serving-authority change is that automatic stable identity, capture waits
while the existing signed positive LKG remains valid and consumer-verified. It
does not publish an unavailable replacement, copy an old route into new authority,
renew an expiry, or declare the new release healthy. Normal producer retries resume
publication once complete serving evidence is available.

This protection is not unconditional zero downtime. Without a valid positive LKG
the existing strict fail-closed compiler remains authoritative. Explicit app or
scope configuration changes, revocation, DNS recovery and policy changes are not
held behind a code rollout. Independent revision/candidate rollouts retain their
existing bound-workload readiness path. No API contract or configuration intent
is changed by the new observers.

Regression tests cover incomplete ownership and cohort evidence, failed reads,
concurrent resource replacement, delayed successful observation, timeout,
independent readiness recovery, LKG expiry and explicit configuration changes.
