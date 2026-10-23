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
may add at most one explicit quota/network hold inside the
changed host's declared pool; existing holds and all other constraints remain
immutable. Recovery policy is optional. Without it, the hold remains until an
authorized configuration change; a short successful request, Ready, a billing
date or a code upgrade cannot release it. This permits safe recovery while a
bandwidth-limited endpoint cannot yet demonstrate capacity. This operation
requires `baseline_mode: declared` by default and refuses silent fence renewal.
An explicitly selected `latest_positive_same_artifact` mode may follow only the
same artifact ID and complete content digest under the exact same producer
policy. Release fences cannot regress or alias another release; the current
unexpired positive LKG and, when needed, exact rollback ledger are revalidated.
Resolution runs before preview and immediately before the transaction, whose
normal atomic exact-fence checks remain unchanged. Different content, producer,
ordinary unverified publication or missing historical evidence is rejected.
Configuration previews compare frozen intent without requiring a
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

The v5 staging declaration adds one explicit `availability_route` to that
retained DNS policy. It cannot replace an existing host policy and still only
publishes shadow. An availability handoff verifier authenticates the old and
new artifacts, requires this exact declared addition, and compares every other
per-node DNS behavior, including unrelated records, TTLs and query policy.
It separately replays the current signed checkpoint and fresh runtime facts;
the immutable comparison alone never authorizes a public selector change.

Runtime facts include at most sixteen original successful `probe_history`
timestamps. External observation verification checks strict ordering, association
with the current proof, and the signed recovery count, span and continuity.
Missing history cannot establish availability recovery; duplicated timestamps,
future observations and observations ending at another proof are rejected.
This is relevant to rate-limited nodes that remain connected: a Kubernetes
Ready condition or one successful request cannot establish service recovery.
The protocol readiness probe is not a bandwidth-capacity measurement; a
provider quota notice must not be relabeled as a measured throughput value.

When one physical probe is evaluated against multiple authorized routing
publications, recovery history belongs to each exact immutable requirement.
The source observer records a success only after validating the complete
publication binding. It records failures before retaining any still-valid old
proof, so retained proof leases cannot retain pre-failure recovery progress.
Repeated evaluation of the same observation does not increase the count.

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

Private promotion verifies recent shadow evaluation separately from serving
leases. The signed DNS-only membership, authenticated executor capabilities and
three advancing observations precede gray publication. Gray and full each need
fresh signed-checkpoint verification and positive per-record readiness before
LKG is retained. A proof that expires in transit is removed from the current
quorum; it cannot invalidate another endpoint's complete fresh proof set or
renew its own deadline. A claim already invalid at capture is rejected.

A provider quota restriction must keep the endpoint outside an explicitly
configured availability pool until recovery is demonstrated. Small route-proof
responses and Kubernetes Ready do not measure sustainable transfer capacity.
The current availability history is not a provider quota-reset detector.

The runtime-facts HTTP endpoint reports a bounded
`X-Fugue-DNS-Observation-Rejection` classification on failure. It distinguishes
missing state, failed checkpoint validation, identity/plan/policy mismatches,
encoding/size limits and concurrent state replacement. These diagnostics expose
no raw error or credential and never change serving or proof eligibility.


### Resuming private promotion after executor replacement

A code rollout can replace Pod UIDs while the exact signed DNS configuration
remains active in gray. A v2 private promotion declaration may embed the original
v1 declaration and pin the existing gray release ID and fence. Its generation
must advance. Only Pod name, UID, immutable image and verifier revision can change;
logical reader membership, service accounts, container/port, artifacts, scope,
shadow predecessor and observation policy must remain identical.

The original declaration digest must match the active gray operation's stored
idempotency key. A missing gray or another release/fence/operation is rejected;
resume never recreates gray or adopts an unrelated publication. New executors
must pass the full current identity, signed checkpoint, fresh answer and advancing
probe watch before a positive LKG is retained. Full publication belongs to the
new declaration. This configuration lane does not change workloads or public DNS
transport and remains independent of component build/deploy success.


### Identical routing publication renewal

An explicitly authorized dynamic routing source can republish the same immutable
parent as a newer verified full release while a DNS gray watch runs. DNS runtime
proofs already bind that newer release, but an exact historical baseline fence
previously blocked the LKG transaction. Static DNS references remain exact pins.

For dynamic sources only, configuration reference validation accepts this narrow
renewal when the original superseded full is verified for the exact same parent and
generation, the current full is active and not failed, the fence increases, and
the release time advances. The new full may still be awaiting its own verification:
that status cannot erase verification of the unchanged historical configuration. The original publication row is loaded explicitly in the
PostgreSQL transaction. Different parent content, gray candidates, missing or
rolled-back history and failed renewals remain rejected. Producer policy
bindings, artifact signatures, current lane locks and fresh DNS convergence still
run. No runtime proof is relabeled or assigned a newer fence by this rule.


Private promotion may declare an explicit public `api_connect_address` for
recovery when authoritative DNS still selects an impaired Edge. The HTTPS origin,
Host header, TLS SNI, certificate verification and API credential remain unchanged;
only the TCP destination changes. Redirects and ambient proxies remain disabled.
The address is configuration data and must be canonical and globally routable.
No node address is embedded in the client. This recovery path does not change
public DNS answers or grant serving authority and still runs all promotion gates.
Transport errors identify whether connection/headers or the response body failed,
without exposing credentials or raw exception details.

The dynamic availability configuration lane accepts the same optional
`api_connect_address`, preserving HTTPS origin, SNI, certificate and Host checks.
Its configuration executor runs its own validation tests and is independent of
component compilation and deployment. A broken code build cannot prevent a
compatible deployed API from accepting a valid recovery declaration.


When a routing source changes while some physical proofs remain valid, the DNS
checkpoint must bind the newly replayed source context. Previously retaining any
old proof copied the entire old checkpoint over that current context, yielding
`plan_mismatch` from runtime observation and rejection of refreshed proofs. The
consumer now retains only the old application time when sources differ. The new
context still passes signature, plan replay, current assignment and ledger checks;
old physical proof deadlines and the checkpoint lease are never extended. A
synthetic mixed-source renewal regression verifies exact replay and lease bounds.

The current DNS publication still requires complete positive consumer convergence
and fresh exact-release physical proofs. An unverified source renewal is never
marked verified by DNS, and its fencing token is never copied onto an old proof.
This prevents a redundant old DNS consumer from blocking recovery of an
independent DNS authority that already proves the same unchanged route behavior.
Both memory-store and PostgreSQL regressions explicitly reject negative current
DNS observations even when the historical source was verified.
