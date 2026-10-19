# Observation history retention

The API runs an independent, bounded history sweep. Its configuration lives in
`observation-retention.json` in the existing storage-policy ConfigMap projection.
The storage-policy CI lane publishes it without rebuilding or restarting code.
Missing configuration starts disabled. Invalid updates retain the last accepted
policy and expose a read failure. An explicit `enabled: false` stops new sweeps.

The deletion allowlist is closed in code:

- `fugue_audit_events` rows whose action is exactly
  `platform_consumer.heartbeat_accepted`, in the corresponding heartbeat chain.
- `fugue_edge_network_samples`, `fugue_edge_performance_samples`, and
  `fugue_resource_usage_samples`.

Configuration cannot add tables or predicates. Account and business records,
non-heartbeat audit events, current consumer state, configuration intent, signed
artifacts, releases, LKGs, compiler inputs, source uploads, image cleanup plans,
and checkpoints are outside the deletion scope. Existing independent retention
mechanisms are not broadened by this sweep.

Time and row budgets select the oldest eligible observations. A heartbeat chain
always keeps its latest event and at least 24 hours of history. Probe history
keeps at least seven days, including the existing aggregation and sizing window.
Safety floors take precedence over row budgets. Network ingestion already has a
shorter independent one-hour retention policy; the new loop does not extend it.
Each table or chain transaction deletes no more than `batch_rows` (maximum 1000),
with a 100 ms lock timeout, two-second statement timeout, and twelve-second total
sweep budget. At most eight heartbeat chains are considered per sweep. Concurrent
API replicas share one advisory lock. Failure rolls back the individual batch;
earlier committed batches are reported separately.

Heartbeat history is a signed chain. The sweep verifies a contiguous expired
prefix against the current audit trust keys, then atomically records the original
signed last-deleted event under the `fugue_meta` key
`observation-retention/heartbeat-boundary/<chain_id>` and deletes only that prefix.
The first retained event must link to this boundary's hash and next sequence.
The current chain head and replay sequence remain unchanged. A missing key,
corrupted signature, gap, or changed link prevents deletion of that chain.
The boundary allows verification of the retained suffix; it does not recreate
deleted history or claim that a truncated history begins at sequence one.

Live diagnostic Jobs already use Kubernetes `ttlSecondsAfterFinished: 3600`,
bounded execution deadlines and scratch volumes. Reports belong to those Jobs,
not an unbounded PostgreSQL snapshot table. Active diagnostics are never removed
by this database sweep.

`runtime-storage-budget` exposes accepted retention policy, last run, failure
stage, last-batch and process cumulative deleted rows, and minimum history floors.
Row budgets describe retained logical records. Ordinary deletion makes space
available for PostgreSQL reuse after vacuum; it does not promise an immediate
filesystem size reduction. No `VACUUM FULL`, table rewrite or data-file deletion
is performed. Capacity planning must include protected tables: this mechanism
cannot bound a database whose retained configuration or source data grows forever.

Production enabled the following policy on 2026-10-20 (Asia/Shanghai):

| Setting | Value |
| --- | ---: |
| Sweep interval | 30 seconds |
| Maximum rows per table/chain batch | 500 |
| Heartbeat age budget | 7 days |
| Heartbeat row budget | 10,000 per chain |
| Probe age budget | 7 days |
| Probe row budget | 1,000,000 per table |
| Completed diagnostic Job TTL | 1 hour |

The row budgets are soft where the minimum-history floor prevents deletion:
heartbeats younger than 24 hours and probe samples younger than seven days stay
protected even when the row budget is exceeded. Heartbeat chain heads are always
retained. The production declaration is in
`deploy/environments/production/storage-policy/policy.json`; the published
retention document remains separate from code and other storage-policy data.

Verification uses an isolated loopback PostgreSQL database named `fugue_test_*`
via `FUGUE_RETENTION_TEST_DATABASE_URL`. Tests cover batch limits, protected data,
signed boundaries, corruption rollback and replay-head preservation. The online
performance timestamp index uses the existing concurrent-index migration lane.
