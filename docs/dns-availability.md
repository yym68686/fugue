# Independent DNS availability

An explicit `dns-availability-v1` policy separates a host's availability pool
from its performance experiment. A historical quality stop or a narrow quality
candidate set can otherwise exclude every endpoint even when another signed
route has a fresh TLS/readiness proof. Existing policies keep their exact
behavior; installing code does not enable this policy.

The source is `dns_query_policy.availability_routes`, an array of exact owned
hostnames and complete policies:

```json
{
  "hostname": "api.example.test",
  "policy": {
    "version": "dns-availability-v1",
    "ordered_edge_ids": ["edge-a", "edge-b", "edge-c"],
    "recovery_successes": 3,
    "recovery_min_seconds": 60
  }
}
```

The declared pool intersects the existing signed endpoint topology, tenant,
region, route state, all-path quorum and exact current route/TLS proofs. An
entry cannot add an endpoint, revive an administrative hold, bypass a pinned
placement or extend a proof/checkpoint deadline. Static records are unchanged.

Quality selection first operates on recovered candidates. If it produces no
endpoint, the explicit availability order supplies the first eligible backup,
including an authorized endpoint outside the experiment or with a historical
quality stop. The stop and allocation are not deleted or reset. The answer is
recorded as `availability_readiness_failover`, distinct from quality allocation.
With no eligible endpoint, the declared record still returns SERVFAIL.

Recovery requires two to five distinct successful observations spanning the
configured ten to three hundred seconds. The span cannot exceed fifteen probe
intervals, since the observer retains at most sixteen timestamps per immutable
probe identity. Repeated or cached proof reads never increase the count. Failed
probes reset continuity; scheduler omissions do not fabricate failures. An old
still-valid proof may retain its original historical semantics, but a failed
refresh removes its availability recovery history. Recovery cannot outlive the
latest exact proof or the original signed serving checkpoint.

Candidate, serving and retained-LKG probe schedulers share bounded observation
continuity for identical immutable probe identities. Their authority and probe
budgets remain independent. Switching to retained-LKG observation during a
control-plane outage does not by itself reset healthy nodes. Restarting the
process loses volatile history and requires fresh probes; persisted artifacts
or counters cannot manufacture recovery. History stores have a hard bound and
are never serving configuration. No DNS request performs network probes or
history mutation.

If a quality capture cannot obtain a successful actual DNS answer, an opted-in
host may instead compile its independently declared availability order. It is
labeled `availability_only_quality_unavailable`, with no forged quality receipt
or new business allocation. Old allocations remain immutable historical
artifacts. Resuming a business experiment must still satisfy its normal
predecessor/intent rules; availability cannot silently reset sticky experiment
state. Normal generic quality refresh can continue as evidence becomes available.

The `dns_availability` producer operation changes one host declaration while
preserving all unrelated source and producer fields. It uses the established
configuration lane and exact producer/full/LKG fences; it does not itself mark
a route ready, hand off DNS transport or replace LKG. Configuration uses the
existing `scripts/configure_dynamic_quality.py` executor with operation
`dns_availability`; its preview separately reports `runtime_verified: false`.
An explicit `dns_availability_recovery` transaction additionally accepts a current
rollback of the **same** unexpired, signed positive LKG. It binds the exact
producer/full fences, original verified producer release, immutable source
digests, and real rollback ledger message. It cannot accept an ordinary
unverified release or a newer pending gray publication. The configuration delta
may add at most one automatically recoverable quota/network hold inside the
changed host's declared pool; existing holds and all other constraints remain
immutable. This operation requires `baseline_mode: declared` and refuses silent
fence renewal. Configuration previews compare frozen intent without requiring a
successful failed DNS answer; the successor still needs live preview, normal
consumer capability checks and gray/full evidence. It changes neither serving
publication nor positive LKG and never fabricates an ACK.

In a complete traffic publication, consumers of the shared route, DNS and TLS
policy must advertise authenticated `dns_availability_v1` capability. This legacy
composition is not a suitable recovery path when a Worker cannot be upgraded.
An unsupported reader cannot silently ignore the policy.

An independent `cell-dns` publication can explicitly use
`route_authority_transition.reference_mode: retained-global-v1`. It retains the
exact current verified global parent, route, TLS and DNS artifacts. Replacement
routing Cell publications are forbidden in this mode. Explicit topology aliases
change no physical membership. Every compiled target must reproduce all original
hostname/path dependencies, endpoint ownership, route constraints and quorum;
proof and checkpoint lifetimes cannot increase. Only DNS processes belong to the
new publication's immutable expected set. `dns_retained_routes_v1` gates those
readers; no Worker capability or ACK is invented or skipped in its own authority.

Approved global producer references use the existing independent routing-source
observer. DNS can follow that producer's actual signed publications without a
DNS code release, while preserving current exclusions, fresh exact route proofs,
monotonic fences and positive LKG recovery. The DNS publication cannot change the
referenced route/TLS artifacts or their LKG. Installing reader code, staging a
shadow, promoting configuration, and handing off public transport remain separate
operations. In particular, shadow success alone never authorizes public traffic.
A complete migration must also preserve the original routing producer's ability
to publish; moving listeners alone is not evidence of release-plane independence.

`stage_dns_authority_transition.py` v4 stages this mode with an explicit empty
`route_sources` list and the original global producer approval. It accepts an old
but currently selected, verified and unexpired source, rather than waiting for a
new Worker release. It never refreshes captured observation timestamps. Its
`dns_configuration` compiler-input view is a projection of a fully verified
stored input, with a separate projection digest; bulky replay evidence remains
available from the unchanged default full view.

Decision receipts include the constrained authorized/recovered candidate counts,
quality primary, actual serving primary and whether availability fallback ran.
Rejections distinguish an endpoint outside the availability pool, pending
recovery, missing/expired readiness and a quality stop. Replay captures the
original policy, clock and probe history; historical receipts without the new
policy or fields are unchanged.

Validate in isolated slots before a separate configuration activation. Cover
an unavailable primary, a quality-stopped backup, an authorized backup outside
the experiment, all endpoints failing, proof expiry or identity mismatch,
recovery flapping, repeated observations, control-plane loss and process restart.
Also verify capability rejection and exact transaction rollback/idempotency.
The HTTPS route-proof path retains its existing protocol scope; do not claim
unverified HTTP/3 failover. DNS caching and broken existing connections remain
outside an authoritative answer's ability to repair.
