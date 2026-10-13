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
