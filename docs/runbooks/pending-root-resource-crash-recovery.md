# Pending resource ledger crash recovery test

Run the opt-in test with PostgreSQL 17+ server tools in an absolute directory:

```sh
FUGUE_RESOURCE_CRASH_POSTGRES_BIN=/absolute/postgresql/bin go test -race ./internal/store -run '^TestPendingRootResourcePrivatePostgresCrashRecovery$' -count=1 -v -timeout=180s
```

The test initializes a new disposable cluster, listening only on a newly chosen
loopback port. It never connects to `FUGUE_TEST_DATABASE_URL` or reuses a data
directory. Without the tools environment variable it skips. Each database is
synthetic and the resource limits exercise accounting, not production budgets.

It exercises immediate-stop WAL recovery after a durably acknowledged ticket
whose journal rolls back, an acknowledged journal without an outcome, and an
acknowledged terminal outcome. A reopened native provider must retain the full
charge and reject replay without another ticket. PostgreSQL can recover a
sequence ahead of its last returned value even with `CACHE 1`; every skipped
ticket remains charged. The test intentionally permits such a forward jump.

Only the newly initialized cluster is stopped immediately. Cleanup stops it
before removing the test's temporary directory. Existing databases, source
bodies, original recovery copies and production clusters are never selected.

This tests database crash recovery, not power loss, filesystem errors or a lost
network acknowledgement. It establishes neither an allocation/WAL maximum nor
an aggregate wait/exit-time bound. Bootstrap, pool installation and all mandatory
failure/outcome writes still need independently reserved capacity before use.
