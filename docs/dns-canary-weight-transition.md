# DNS recovery during an application canary weight step

An edge can apply the next signed traffic release before every DNS record is
ready for whole-artifact activation. A canary step from 1% to 5% changes the
route digest. Previously a record with unchanged tenant, application, DNS
addresses and release identities could lose its old route quorum and also be
excluded from the existing per-record successor path because its traffic
weights differed. The record then returned SERVFAIL until the entire DNS
candidate became ready.

The transition comparison now permits a weight-only change between the same
two already-active releases in canary mode. Both weights must remain positive
and total 100. The constraint identity, tenant, application, release identities,
fallback and stickiness remain exact comparisons, as do DNS policy, endpoints,
ownership, readiness quorum and other policy fields. Enabling or disabling a
release is not a weight-only transition.

This affects only admission to the existing volatile per-record successor path.
It does not rebind new proofs to old routes. The successor answers using its own
unchanged signed policy and exact fresh proofs, after the existing signature,
assignment and route checks. A failed or expired successor still returns
SERVFAIL. Its state never becomes a positive checkpoint until full activation
passes. The previous positive LKG, its original deadline, and the stricter old-
proof compatibility path remain unchanged.

Regression coverage reproduces weight-only rejection before the fix, verifies
recovery with the successor's complete quorum, rejects identity/policy/endpoint
changes and expired evidence, and checks that neither signed policy nor the
positive checkpoint is mutated. A separate DNS selection-policy change remains
incompatible; failure receipts must not be used to authorize such a change.
