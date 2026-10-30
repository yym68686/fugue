# Project migration

```sh
fugue project move <project> --to <node-or-runtime>
```

The command preflights the project's applications and managed databases. When
the destination names a physical node, it localizes independent and app-owned
databases to one instance on that node, waits for database operations, and then
migrates applications. It preserves each application's replica count, including
applications intentionally stopped before the move.

Movable RWO volumes receive a fresh destination claim before placement is
evaluated. When the source class cannot provision at the destination, the
controller selects an expandable, platform-approved class using current CSI
capacity, topology, driver registration, and Longhorn node readiness. An explicit
replacement class remains authoritative. Unknown capacity does not by itself
justify replacing a storage class. Source claims and data are retained.

After a database storage-class switch, an obsolete local claim can prevent
CloudNativePG scale-down because it cannot attach under the destination node
selector. Fugue retains that claim outside CloudNativePG discovery and removes
only an unassigned, never-started replica Pod, after verifying the destination
primary and resource identities. It never deletes the source PVC or PV.
Legacy clusters that omit a storage class use the ready primary's actual claim
as the destination-class evidence.

With the default `--wait`, physical-node moves verify application placement,
replica counts, and database convergence before returning success. Rerunning the
same command also verifies resources whose desired runtime already matches the
destination. A queued operation or extra database replica is insufficient.

`--dry-run` does not mutate resources. `--skip-blocked` explicitly allows a partial
move. Offline database recovery and stopped dedicated PVC conversion retain
their separate `--recover-offline --storage-class <class>` workflow; they are not
inferred from an unhealthy source.
