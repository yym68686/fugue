# Business exploration publication validation, 2026-10-13

The initial observe declaration at `6baf81b7` did not acquire serving authority.
CI `38371200506`, job `115208864940`, stopped at typed input validation at
08:49:26 UTC. Draft `artifact_1791881366_228d3a7e8fbb` reports the sole failing
check as `secret_safe.content`, at the declared business policy's
`initial_dns_publications` anchor `fencing_token`. This is a positive numerical
publication sequence, not a credential. The generic substring scanner had
classified every key containing `token` as secret material.

A regression test reproduces the rejection before the correction. Scanning now
recognizes only a complete typed anchor inside a business policy's
`initial_dns_publications`. The canonical field must be a positive, exactly
representable integer with a valid digest and no unknown fields. Credentials,
numeric strings, fractions, overflow, nulls, misplaced fields and unrelated
secret-like keys remain rejected. Published allocations retain the same scan.
This does not relax publication signatures, fencing, baseline proofs or LKG.

The API-only release intent is generation 748. Its exact predecessor was read
from the live two-ready-replica deployment: source `016ced49`, image
`sha256:2b080ba5d93c0b6425d6b9a7136938c967c2f5d9f6fd9a0ba9271585e298f458`.
Activation and production configuration acceptance still require normal CI.

Separately, bounded observation at 08:43–08:47 UTC recorded five new German DNS
SERVFAILs, from 1 to 6. The subsequent bounded journal read contained no matching
failure receipts; five increments remain unattributed. At 09:22 UTC its count
was still 6, the current publication matched loaded positive LKG, and journal
drops were zero. Eviction was nonzero. Neither the validation rejection nor the
DNS count establishes a cause for those failures. No speculative DNS behavior
change is authorized by these observations.

## Publication renewal while configuration is queued

The read-only business capture using the 08:43 declaration later rejected
initialization from an unfenced DNS predecessor. The producer had legitimately
renewed its full publication. The existing configuration lane refreshed the
full-publication precondition but left the business intent's initial DNS
anchors unchanged. These two references then described different publications.

`business_anchor_mode: latest_verified_same_baseline` explicitly permits the
configuration lane to bind its new intent to a later verified full publication
under the exact same predecessor producer. The default remains `declared`.
Each declared DNS node must provide a successful actual receipt for that full
parent, with the same declared baseline edge, service context and positively
admitted candidates. A changed edge, negative evidence or regressed fence is
rejected. Candidate sets, budgets, expiry, stop rules and intent identity stay
exactly as declared. A binding digest makes resolved artifact generations
immutable and distinct; the CI receipt records both the declaration digest and
the resolved policy digest and anchors.

After preview, the verified full predecessor must still be identical. A change
causes at most three complete bounded attempts; no release is sent for a stale
preview. The existing server transaction still compares both producer and full
publication authorities under its original locks. This feature does not change
serving configuration until the normal CI configuration job succeeds.

Focused tests exercise separate DNS baselines, immutable budgets and leases,
default opt-out, changed context/edge/proof rejection, bounded retry, successful
ledger publication, and a full-publication race with zero release writes.

API generation 748 completed normal CI `38379154113`, deployment job
`115237214624`, and the 10:05 UTC read found two Ready/available replicas at
source `02e9461c`, image
`sha256:55d094005c316f20eee1df7a1da29b5aecaebfd91dac895bcffcd1bd86dba5b4`.
The complete backend suite passed. Frontend snapshot/type generation checks
passed; its existing 147 TypeScript errors remain unchanged.

At 10:11:43–10:11:47 UTC, the new read-only resolver matched both actual DNS
receipts to verified full `artifact_1791886026_78d42905a601`, fence 3465, retaining
each declared baseline. This read grants no serving authority. The renewed
observe declaration must still pass normal configuration CI and gray/full ACKs.
