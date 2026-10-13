# Edge authority storage pressure during unified routing release

At 18:21:54 and 18:21:56 UTC on 2026-10-13, the US authority rejected
Worker staging with `candidate_unavailable`. The exact control log identifies
`edge-control group state exceeds durable size limit after compaction`.
CI 38443849658 left the US Worker transition `recovery_required`; the new business
policy and public DNS E handoff had not been activated.

A read-only in-node JSON analysis at 18:34 UTC found a 100,144,586-byte state:
57,299 shadow entries used 55,808,909 bytes and 55,029 authority entries used
44,086,905 bytes. Only the newest 240,218-byte shadow bundle remained. Existing
compaction removes old bundle payloads but cannot reduce these append-only audit
metadata arrays. The 100,663,296-byte durable limit consequently rejects a
candidate write even after bundle compaction. No audit history or key is deleted.

## Compatible recovery sequence

First release a reader that accepts both existing checksummed JSON and a bounded
single gzip member containing exactly the same JSON. Compression writes default
off. Only after this reader is the verified code LKG, enable
`FUGUE_EDGE_CONTROL_COMPRESS_STATE_WRITES=true` through an independent declared
configuration release. The previous reader then understands any newly compressed
state and continues its encoding after a rollback. The original checksum,
sequence validation, signed publication, candidate binding, atomic replace and
fsync boundaries are unchanged. Readers reject corruption, extra members,
trailing input and expansion beyond 256 MiB; the encoded limit stays 96 MiB.

The storage report contains only group ID, byte sizes, limits and ledger counts,
at most once every five minutes or at an encoding transition. It exposes the
remaining expanded-data ceiling; compression is not an assertion of unlimited
retention. Segmented durable audit storage is a separate longer-term change,
not a reason to discard existing evidence or relax bounds in this recovery.

Tests reproduce metadata-only pressure beyond the original bound and verify
lossless continuation, code rollback compatibility and corruption rejection.
`make test` and the focused race test pass. A private read-only production copy
also passes checksum/authority validation and exact deep-equality round trip:
100,150,205 expanded bytes become 18,772,186 encoded bytes, preserving all 57,302
shadow entries, 55,032 authority entries and revision 408845. The backup remains
private outside the repository and is not a serving input.
The compatible reader was released by CI 38450034542 (`f2ca3f17`), then
the independent compression declaration by CI 38451410183 (`6b105f40`).
The latter component deployment succeeded; its separate public DNS observation
failed and is tracked in `dns-business-proof-renewal-20261014.md`.
At 19:05:49 UTC the live control reported 18,775,914 encoded bytes and
100,161,024 expanded bytes with 57,308 shadow and 55,038 authority entries.
The new Pod was Ready with zero restarts. CI 38456758394 subsequently completed
the US Worker deployment (`892b7176`); at 19:59:23 UTC Guardian was stable with
local, dependency and route health verified. This confirms candidate staging
can proceed without deleting historical state.
