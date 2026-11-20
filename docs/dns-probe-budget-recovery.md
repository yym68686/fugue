# Preserve serving evidence when a probe sweep reaches its budget

A bounded candidate sweep reports `not_observed` for probes it did not start.
The retained-release refresh previously treated those entries as completed
negative observations and skipped its own probe. An existing primary proof
could then expire while the scheduler had not attempted its replacement.

The retained scheduler now treats `not_observed` as missing work. It makes a
bounded attempt with the original target, concurrency and timeout policy.
Actual observed failures remain negative and are not automatically retried by
this reuse path. Fresh results still need exact release, route and identity
validation; an unsuccessful attempt cannot extend the old proof.

New assignments also order equally unattempted probes by the primary endpoints
already selected in the signed record policy for that physical DNS node. Every
declared dependency of such a target gets the same preference. This ordering
does not add targets or change the signed plan. It preserves progress for other
unobserved or aged work, expiry ordering on subsequent rounds and connection
backoff. Route-source variants share only their physical scheduling preference,
never proof authority or readiness.

The bounded priority queue treats the signed `physical-edge-network-v1`
candidate order like the fixed physical order. When an explicit availability
pool exists, it keeps precedence. This does not change business-allocation
policy, probe concurrency, deadlines, or which proofs can authorize an answer.

The two changes address missed work without increasing the declared probe
budget, relaxing quorum or changing endpoint eligibility. If the target really
fails, records still fail closed when their original valid proofs expire.
