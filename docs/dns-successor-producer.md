# Independent DNS successor production

The transitional `verified-global-dns` producer runs in the existing independent
authority worker. Its signed policy names one exact global producer publication,
the complete physical topology and DNS member set. It does not infer authority
from healthy nodes, dates, service names or the latest available policy.

Capture requires that producer to remain continuously serving and its current
global full publication to have a matching positive, authenticated, unexpired
LKG. The original signed intent, policy, route/TLS/DNS children and stored compiler
input are read without probing an Edge or taking the global producer election.
A coherent read-only selection check binds both active lanes, fences, signatures
and LKG before capture returns. No source timestamp is renewed.

The mechanical projection emits a `cell-dns` release set containing only a DNS
child. It preserves every member's complete DNS behavior, including unrelated
records, candidate order, TTLs, nameservers, client policy and availability rules.
Only explicit authority aliases and the DNS process identity change. Every
candidate is independently replayed and compared against its retained signed DNS
predecessor. This comparison also runs inside publication admission; an otherwise
valid artifact with an added record or changed TTL cannot use the producer's
authority. New records are accepted only when present in a new verified source.

The existing producer ledger provides shadow, gray, full and recovery transitions.
Serving starts only with an existing verified full DNS LKG. Actual consumer
capability and fresh observations still gate serving, and timeout recovery retains
the separately verified baseline. A revoked or changing source prevents a new
candidate without rewriting that baseline. PostgreSQL takes nonblocking shared
source locks under the existing DNS authority transaction; contention rejects the
attempt instead of waiting while blocking other authority publication.

This is a migration prerequisite, not public DNS delegation. It still obtains
business DNS changes from a verified global publication. It does not remove the
global producer's own DNS convergence requirement, admit an offline Edge, change
public Services, or enable itself by installing code. Public handoff must wait
for explicit delegation and demonstrated continuing source publication. Separate
configuration activation and private acceptance precede that handoff.

Validation covers deterministic projection and new source records, preservation
of source inputs and timestamps, complete per-node behavior, wrong/frozen source
selection, forged or expired LKG, cancellation, repeated shadow reconciliation,
and publication guard parity in an isolated PostgreSQL schema. No production
configuration is part of the capability release.
