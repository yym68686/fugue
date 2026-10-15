# Recovering DNS across equivalent physical-order representations

The compiler can preserve an existing physical edge order while quality evidence
is learning, then represent that same order as a physical network selection when
fresh evidence arrives. In that case route digests, authorized addresses, ordered
edge IDs and the primary's original assignment time can all remain unchanged.
Previously the different `physical_order`/`physical_quality` representation alone
blocked proof bridging and per-record recovery. When edges adopted the successor
publication first, DNS rejected their otherwise matching proofs and returned
SERVFAIL until whole-artifact convergence.

Comparison copies now recognize the two representations as equivalent only when
the ordered edge IDs and primary timestamp are identical, the quality selection
uses the known network-selection version and global scope, and no business
allocation is present. All tenant, application, endpoint, candidate, quorum,
ECS, TTL and other policy checks remain unchanged. For proof bridging, changes to
the ordered edge IDs, scope, business allocation, version or primary epoch remain
incompatible.

Forward per-record recovery also permits removing fallback edges while keeping
the same primary, primary epoch and relative order. For example, a retained
fixed order of [a, b, c] may recover through a signed quality selection of [a, b]
when that successor has its own complete, fresh proof quorum. This does not
permit proof bridging into the broader old selector. The successor answers
with its original policy, and an expired or incomplete successor still fails
closed. Reordering, widening the selection or changing the primary remains
incompatible. The original quorum is not reduced to the number of selected
endpoints.

No signed payload is mutated and the retained checkpoint continues answering
with its original selection. Fresh successor proofs still require the exact
verified release and route digest, their original expiry and complete quorum.
The existing bounded LKG and record-transition rules remain in force.

Regression tests reproduce the previous rejection in both directions, verify
equivalent recovery, reject the relevant authorization changes, and check signed
payload immutability. Incident configuration comparisons also verify that the
captured unchanged route requirements become compatible; historical receipts are
replayed unchanged rather than rewritten to report hypothetical success.
Additional regression coverage exercises narrowed recovery in both directions,
exact answer replay, expired proofs, insufficient quorum, rule disagreement and
duplicate rules. Rule lookups for this comparison are indexed per hostname so
the added checks remain linear in the query and rule inventory.
