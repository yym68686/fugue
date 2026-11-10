# Compiler input writer diagnostics

The private Unix storage provider includes
`compiler_input_writer_diagnostic` with schema
`fugue.compiler-input-writer-diagnostic.observation.v1`. The existing
`compiler_input_writer_activity` v1 counter field set is unchanged.

The diagnostic retains one most recently recorded failed invocation per process.
Success does not erase it. It contains no input content, SQL, identity, error
message or operation history. The eight phase keys are fixed: `validate`,
`encode_snapshot`, `begin_tx`, `append_queue`, `writer_barrier`, `persist_verify`, `commit` and
`after_commit_return`. The persistence phase includes representation checks,
encoding, database calls and full readback verification. The existing separated
encoder profile describes its own nested work independently.

`append_queue` measures serialization with other append writers; `writer_barrier`
measures shared lifecycle acquisition, which still conflicts with exclusive
maintenance and older writers. `phase_total_ms` accumulates completed attempts'
phase time (including failures); it is not an in-flight timer. The signed
`runtime-storage-budget` Live Diagnostics recipe selects both the unchanged
writer counters and this diagnostic from the private storage provider.

Phase durations use the monotonic clock. Context state and active-phase time
are frozen before deferred rollback and the writer's own cancellation. Total
duration includes cleanup; `cleanup_ms` separately reports that interval.
Parent and effective writer deadline budgets are sampled at entry and after
successful BeginTx and barrier acquisition. A budget of -1 means no deadline or
an unreached checkpoint; context state and the writer checkpoint distinguish
these cases. Zero means less than one millisecond remains or the deadline passed.

Deadline labels ending in `_observed` describe sampled context state and the
owner of the effective deadline. They do not identify a SQL wait cause. SQLSTATE
57014 is `database_query_canceled`, since it does not by itself prove statement
timeout. An error wrapping DeadlineExceeded or Canceled while both contexts are
active is explicitly unattributed. Other errors use a fixed non-deadline label.

Counter and diagnostic reads, separate replicas and the nested encoder profile
are not an atomic per-operation trace. Do not add their failure counts, pair
them as individual operations, or interpret a nil return/commit-driver return
as an independent durable-commit receipt. A last failure does not describe every
failed invocation or current in-flight work. Process restart resets all fields.
The observer must bind them to the exact runtime identity.

Instrumentation preserves caller cancellation, the existing writer deadlines,
the single append writer and maintenance exclusion, transaction boundaries, verification, fallback
and panic propagation. It performs no additional database operation or input
read. New evidence informs a separate recovery decision; it grants no archive,
fence, retirement, retention, resource or availability-policy permission.
