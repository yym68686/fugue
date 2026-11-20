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

For investigation, the existing private `.probe-observations.json` contains a
`probe_dispatch` sample of at most 24 physical requests per sweep. Join on the
same process and sweep `observation_id`, then on either the first 12 bytes of
SHA-256 of the requirement ID or of the physical `ProbeKey` (the latter also
matches authorized variants sharing that request). The sample includes actual
queue position, candidate rank, previous scheduler expiry, dispatch, worker
start and completion, the scheduler's result and expiry, and target retry
state when dispatch stopped. It prefers earliest previous expiries and, when
no expiry is known, unstarted or rejected work. Absent entries are unknown.

Physical scheduler success does not prove a particular signed variant matched
or that any query saw the result. Check the query's own facts and clock before
attributing adoption or denial. Stop state and retry time describe the point
dispatch ended; they do not reconstruct earlier target backoff. The file
remains private (`0600`), retains at most eight sweeps and 128 KiB, and evicts
the oldest rows first when needed. No per-probe fields enter the journal, DNS
answer, proof, or recovery history.
