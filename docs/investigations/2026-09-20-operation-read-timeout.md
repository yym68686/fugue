# Operation inventory read timeout

## Confirmed mechanism

Production API and controller still used `ListOperations("", true)` for lifecycle-only checks. That reader selects every historical row, including full `desired_spec_json` and `desired_source_json`, and consumes the result under a client-side 10-second context deadline.

A passive observation on 2026-09-20 correlated one existing controller query without replaying the expensive read:

- Query started at 10:59:21.069730 UTC.
- Samples at +0.53, +2.44, +4.44, +6.62 and +8.70 seconds showed the same backend waiting in `ClientWrite`; parallel workers waited in `MessageQueueSend`.
- The controller reported `iterate operations: context deadline exceeded` at 10:59:31.033771 UTC.
- The same PostgreSQL backend logged SQLSTATE 57014, `canceling statement due to user request`, at 10:59:31.125 UTC, with the identical unfiltered SQL.
- The database-level `statement_timeout` and `lock_timeout` were both zero. The cancellation came from the Go reader deadline, not a configured PostgreSQL query timeout.

At one nearby snapshot there were no active operations, yet the reader hydrated roughly 26,500 terminal operations. The two desired-state columns occupied about 107 MB of stored values, excluding result/error text and possible JSON decompression expansion. A read-only lifecycle scalar-size estimate for the same inventory was about 2.42 MB. This establishes an unnecessary data read/transfer/decoding dependency in recovery and diagnostics. `ClientWrite` does not by itself distinguish network throughput from client decode/scheduling delays; this change makes no claim about that lower-level split and does not change transport settings or timeouts.

## Change and invariants

Recovery and robustness now read only `pending`, `running`, and `waiting-agent` lifecycles, using the existing `(status, created_at)` index. The distributed image retention sweep reads every operation identity, app, type, status, creation/start/completion timestamp, without deployment payloads or large diagnostic strings.

The distributed retention scan has no time cutoff, pagination truncation or status exclusion: old operations can still protect images or determine retention ordering. Any incomplete database result returns an error and no inventory. Execution readers and stored configuration are unchanged. No schema migration, live configuration mutation, timeout increase or weakened rollout/retention gate is involved.

The registry-backed legacy retention branch still requires source/image payloads and is deliberately not switched to lifecycle summaries. The production distributed branch only uses operation IDs, statuses and completion timestamps in the pure retention planner.

Two bounded-count logs expose successful recovery/retention inventory reads and their elapsed milliseconds, without SQL parameters, source documents or secrets.

## Verification

- Disposable loopback PostgreSQL test: 30,000 terminal operations containing synthetic large configuration payloads plus one old waiting-agent operation. Active inventory approximately 1 ms, complete lifecycle inventory approximately 54 ms in the first passing run. These local timings are not production latency claims.
- Column-level database privileges deny deployment payload reads: the old full reader fails while both lifecycle readers succeed.
- Retention plan equivalence verifies keep/drop decisions and ordering from complete versus projected operation records, including old active operations, missing source operations and null completion times.
- Interrupted row streams return no partial inventory.
- Recovery tests preserve duplicate-failover suppression for all three active statuses.
- Full `make test` passes with `GOFLAGS=-p=2`. The initial unrestricted package-parallel run hit unrelated existing wall-clock fixture bounds; assertions were not relaxed.
- Production read-only retention preview before release contained 206 app plans; none selected a current-workload, active-operation or user-pinned image for dropping.

Release verification must record the exact deployed API/controller revision, consecutive successful recovery inventory observations, successful distributed retention inventory collection, robustness response, and public/edge health after the normal declarative GitHub Actions release.
