# Isolating record intent during DNS recovery

Adding an unrelated DNS record, such as a short-lived TXT challenge, previously
disabled proof bridging for every hostname. The compatibility check compared the
entire artifact's `records` list before its existing per-host query, policy and
readiness checks. An unchanged record could therefore lose its old-publication
quorum after edges advanced, even though its exact route, ownership and DNS
answer policy were unchanged.

Record intent is now compared per hostname alongside that hostname's query
views, traffic constraints and DNS rules. Every intent field and every record
type for that hostname remains included, including tenant/application ownership,
values, value expiry, status and TTL. Adding, removing or editing another
hostname cannot veto this record's recovery; changing this hostname still does.
Shared hard policy, exact route proofs, quorum and publication authority retain
their existing checks. No artifact, selection, expiry or positive LKG is changed.

Regression tests reproduce the former global veto for unrelated additions,
removals and edits, then verify that target-host authority changes remain
incompatible. Captured incident configurations independently verify recovery of
unchanged route requirements when a separate TXT challenge is added.
