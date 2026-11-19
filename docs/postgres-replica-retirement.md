# Retire the only managed PostgreSQL standby

Use the dedicated retirement command when the primary should stay where it is.
`service localize` is a placement and storage workflow, not a standby-removal
command for a database already on its target.

```sh
fugue service postgres retire-replica <service> --node <standby-node> \
  --acknowledge-replica-removal
```

Use `--expected-pod-uid` to require a specific standby Pod incarnation.
`--wait=false` returns the operation ID without waiting.

Retirement requires exactly two healthy instances on distinct nodes, each with
one verified, bound node-local PGDATA volume. The operation records identities
before writing anything. If built-in synchronous replication is enabled, it
first disables and verifies it on the original SQL primary with the standby
still present. It preserves the primary Cluster affinity and does not change
the application rollout, database endpoint or primary storage.

The standby loses its PVC and is no longer available for failover. Kubernetes
applies the existing PV reclaim policy: `Delete` removes the standby volume;
`Retain` leaves it released. Fugue reports completion only when this is
observed and the original primary Pod, data volume and PostgreSQL process are
verified through the same writable service endpoint. A change in primary
identity during execution leaves the operation unsettled for investigation;
Fugue does not automatically restart, relocate, or recreate the database.
