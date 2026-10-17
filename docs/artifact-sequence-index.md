# Artifact sequence allocation

The allocator compares its durable counter with the highest retained artifact
sequence before assigning the next value. This preserves monotonic allocation
when restored artifacts are newer than the counter or retention removes older
artifacts.

The history lookup now explicitly selects positive sequences, matching the
existing partial index on artifact kind, scope and sequence. Legacy unallocated
zero values cannot advance the counter, which starts at zero. PostgreSQL can
read the last index entry instead of scanning every artifact in the scope.
The counter update, transaction boundaries and kind/scope isolation are unchanged.

A bounded production read comparison examined 5,458 history rows and 5,568
buffer blocks with the old lookup, versus one row and six blocks with the
indexed lookup. These are observations from one pair of reads, not a controlled
latency benchmark. A generation-counter UPDATE was also canceled during a
compiler timeout; the available logs do not establish how much of that timeout
was scanning, lock waiting or infrastructure delay.

Real PostgreSQL tests cover legacy values, restored high-water marks, counters
ahead of retained history, rollback, cancellation, kind/scope isolation, and
eight simultaneous transactions on distinct database connections. A fixture
with 100,000 historical artifacts checks the execution plan of the actual
allocator UPDATE. The old implementation fails that bounded-read check.

Run these tests against a disposable loopback database whose name contains
`fugue_test`, using `FUGUE_TEST_DATABASE_URL` and
`go test -race ./internal/store -run TestPlatformSequencePostgres -count=3`.
The public API and database schema do not change.
