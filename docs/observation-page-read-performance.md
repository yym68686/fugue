# Observation page read performance

`fugue_observation_checkpoint_entries` stores bounded rebuildable runtime pages.
The writer still reads the head and all page hashes inside one transaction, but
unchanged page bodies are now reused from a bounded process-local cache. The
database returns `BYTEA` only for a new page or a page whose database-side SHA-256
changed. Cache entries are keyed by checkpoint and page name, validated on each
read, capped at 16 MiB of conservatively accounted storage and 1,024 entries.
Only rebuildable page bytes are cached, never serving or authorization decisions.

The cache is an optimization only. A cache miss, eviction, process restart,
replica change or hash mismatch reads the page body again, in batches of at most
128 pages. Readers use a repeatable-read transaction; writers retain their
transaction-scoped advisory lock. Fetched bodies must match the observed size
and actual SHA-256 before entering the cache. Existing size, key, page
hash and manifest checks remain enforced. The file backend and legacy checkpoint
fallback are unchanged.

The regression test proves that the first read fetches page bodies and the next
read fetches only the head and hashes while returning identical pages. The
PostgreSQL atomic cursor, cancellation, lock and inventory page tests continue
to pass. A production sample before this change read about 2.45 MB of page
bodies per checkpoint read; 607 of 608 entries were unchanged across 20 seconds.
That observation is a sizing signal, not an end-to-end benchmark.

A disposable PostgreSQL benchmark with 64 synthetic 56 KiB pages compared full
body reads with verified warm reuse. Two local arm64 runs allocated about
8.45 MB/read versus 4.24 MB/read; times overlapped at 3.86–4.56 ms and
3.64–4.13 ms respectively. The cache still copies returned page buffers and the
database still hashes every value. These measurements do not promise a matching
production latency improvement or remove downstream page decoding.

The update callback receives mutable maps and byte slices. Before invoking it,
the writer records independent page hashes and membership, so in-place edits
and deletions cannot alter the baseline used to decide which rows to write.
A real PostgreSQL regression previously returned the old body after a successful
callback update; it now verifies replacement, byte mutation, deletion and empty
pages together with the existing atomic cursor/cancellation checks.
