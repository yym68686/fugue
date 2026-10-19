# Compiler input write batching

Large compiler snapshots used one database round trip per256KiB chunk. A current
production observation retained511INSERTs for one input:133.88MB of known
parameters,5.71seconds summed statement time and17.97seconds between the first
and last observed statement. Recorder gaps limit completeness; this does not
attribute the entire compile or discovery latency to those writes.

The writer now sends up to16chunks per statement. One4MiB binary parameter is
split into the original chunk rows by PostgreSQL. Small ordinal, hash, offset and
length parameters precede that payload so the driver's encoder does not repeatedly
grow buffers for each large parameter. One reusable4MiB buffer bounds additional
writer memory. A final partial batch is flushed before the manifest is inserted.

Chunk format,256KiB row size, whole-input digest and DNS range offsets are
unchanged. The existing cluster-wide transaction lock and30-second bound still
apply. Every actual stored chunk is hashed by PostgreSQL and checked against the
encoder's independent hashes before commit. A changed byte, even with a matching
forged stored hash, missing chunk or failed later statement rolls back all batches
and the manifest. Existing inputs are verified through the previous read path.

The change reduces N insert round trips toceil(N/16); serialization, data transfer
and integrity work remain linear in input size. It adds no schema, API, signing,
authorization, serving configuration or LKG changes.

Validation uses a disposable loopback PostgreSQL through
`FUGUE_MEMORY_TEST_DATABASE_URL`. Statement triggers verify that25chunks require
2INSERTs; row triggers raise an error or corrupt bytes and their declared hash
in the second batch to verify whole-transaction rollback. Unit coverage checks
exact and partial batch boundaries, caller/encoder buffer reuse and failure
propagation. Existing chunk, selective DNS-input, corruption and compatibility
tests remain required. `BenchmarkCompilerInputNewWrite` measures a new32MiB input
with129chunks; local results do not replace production validation.
