# Unified producer input persistence

Producer policy fence 41 was activated by CI 38461304882 at
2026-10-13 20:21:58 UTC. The producer retained verified full fence 3552
while subsequent attempts exceeded their 90-second compilation deadline.
At 20:26:55 UTC, phase timing recorded 53,782 ms capturing inputs, 7,638 ms
compiling, and 30,034 ms materializing, including 27,954 ms in compiler-input
persistence. The next attempt reached 99,599 ms overall and failed in the
same storage phase. These observations establish the deadline failure; they
do not attribute it to a slow SQL statement.

A bounded read-only projection returned 133,000,208 bytes. DNS selections
accounted for 132,426,541 bytes, including 6,706,188 bytes for the two unified
business envelopes. The remaining dynamic selections belong to the existing
global publication. No source evidence or historical records are discarded.

The chunk writer already canonicalizes the typed snapshot, checks its exact
lineage digest, and re-reads every stored chunk before committing. The reader
checks ordinals, sizes, per-chunk hashes, total length/count and the complete
content hash. The implementation then redundantly decoded and canonicalized
the same verified bytes a second time inside the 30-second storage transaction.
The new readback drains this verifying reader to EOF without reconstructing
another snapshot. Existing-content and public reader behavior remain unchanged.

On one isolated loopback PostgreSQL database per implementation, the exact
private snapshot retained digest
`sha256:5bd2866032e13a60ac35b70ac759e00796541c3515abcd728bf46931dea9aa32`.
Persistence took 6.582 seconds before and 3.473 seconds after. This local
measurement is not a production latency claim. CPU profiling attributes the
majority of sampled work to JSON snapshot encoding and canonicalization.

The producer's total compilation deadline increases from 90 to 180 seconds,
covering capture plus materialization rather than consuming almost its entire
budget before storage. Database transactions retain their existing 30-second
bound. Evidence ages, signatures, policy leases, CAS, required consumers, gray
and full verification, and independent positive LKG are unchanged. Production
phase timings must confirm the improvement after normal API release generation
755; activation acceptance is still pending.

API generation 755 deployed through CI 38465718827 successfully. Both replicas
are Ready with zero restarts. At 21:04:10 UTC the producer completed in 65,797 ms;
compiler-input persistence was 9,459 ms and materialization was 10,893 ms.
The producer published successfully. This is one observed production cycle,
not a claim that every future cycle has the same latency.

Disposable PostgreSQL integration tests inject changed stored bytes, changed
bytes with a matching chunk hash, and a missing chunk before commit. Every
case fails and rolls back both chunks and manifest. Existing corruption,
legacy-reader compatibility, wrong-digest, retry and concurrent-writer tests
also pass. Private snapshots, local profiles and raw diagnostics stay outside
the repository.
