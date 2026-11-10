# Pending-root resource observation

The API's private runtime Unix socket has a demand-driven `pending-resources`
snapshot. It reads the actual Store connection pool and exact Kubernetes objects;
it does not install a ledger, grant database privileges, issue a reservation, or
enable input capture. This is a source for resource investigation, not an
implementation of `PendingRootResourceSource` or proof of its six operation bounds.

`pending-resource-status` reads the last completed attempt in the same API process
without another database or Kubernetes request. It records only fixed phase,
stage, reason and SQL category values with start/completion timestamps. It never
returns raw transport/driver errors, SQL statements, paths, object bodies or
credentials. A concurrent attempt may replace that last result; this is not a
durable operation history or per-operation correlation. A refused resource
snapshot still returns HTTP 503, and neither status success nor a fixed refusal
category grants admission or changes any observation guard.

The observer currently supports one healthy CNPG primary backed by a bound
Longhorn filesystem PVC. It verifies the primary owner, SQL endpoint address and
postmaster identity, PVC/PV claim UID, CSI handle, Longhorn attachment and node UID.
It rereads those objects and the database identity, rejecting drift, resize,
replacement, restart, duplicate volume samples and ambiguous mounts. A hidden
`data_directory` setting is handled with the postgres container's literal PGDATA;
the response explicitly identifies this configuration provenance. It does not
claim an independently attested physical path or WAL/tablespace placement.

Kubelet volume statistics must match the exact Pod UID and PVC and be at most
30 seconds old, with at most 5 seconds of forward clock skew. They are cached
kubelet statistics, not a synchronous `statvfs` measurement or a reservation.
The before/after reads bracket sequential sources; no atomic cross-source
snapshot is claimed. Changes in resource versions cause a refusal even when
identities happen to remain the same. Refusal does not stop ordinary workloads.

Each observation permits one in-flight request per API process, a 2.5 second
enclosing context and at most 2 MiB for each exact Kubernetes response. SQL uses
the existing 5 second statement/250 ms lock/1 MiB work-memory/zero-temp guard
inside read-only repeatable-read transactions, additionally constrained by the
enclosing context. Catalog point lookups cover only the four known ledger
relations; absent relations remain absent. Input bodies, environment values,
credentials, full node inventory and raw Pod specifications are not returned.

The result always has `observation_only=true`, `admission_allowed=false`,
`whole_operation_budget_proven=false` and null whole-operation bounds. Database
size and existing ledger allocations are samples. WAL statistics are cluster-wide,
temp statistics are database-wide and both have independent reset identities;
neither gives attributable per-operation or peak usage. The result does not
establish Longhorn backing-pool reserves, worker capacity, wait growth, bootstrap
cost, failure/uncertain-ACK exit cost or operation completion time.

Production admission remains unavailable until an independent resource source
proves all six full-operation bounds against the exact storage/pool/operation,
including pool installation and its own ticket/reservation/outcome allocation.
Keep the existing PG/Longhorn/worker reserves, no-refund accounting, retention,
reader/writer barriers and physical-retirement gates. Observation success is not
copy acceptance, a root-closure proof, or evidence of freed old relation bytes.

Native pending transactions also install a transaction-local server timer
(PostgreSQL 17 or later is required) before ledger/business SQL. It is capped at
the remaining enclosing context, or one minute for a caller without a deadline;
a stricter existing session timer is preserved. The owned connection and context
are released on commit, rollback and initialization failure. Pending-root
preparation and application use this path too. Ordinary activation transactions
retain their existing behavior. This server timer complements client cancellation
when a connection is lost; it does not prove I/O cancellation, rollback completion,
the sum of all transactions, or a six-bound whole-operation budget.

Every native pending write transaction, including reservation journals and
outcomes, uses transaction-local synchronous commit. A stronger `remote_apply`
session policy is preserved; `fsync` and `full_page_writes` must already be enabled.
No server/role setting, replica count or membership is changed. A failed or lost
commit acknowledgement remains uncertain, with the existing full charge retained
and no retry/refund. Installation/bootstrap costs, all ledger allocation/WAL,
and failure/uncertain-ACK exit escrow still require independent bounds.
