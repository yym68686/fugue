# Retained orphan PostgreSQL lifecycle

When a ManagedApp no longer has an app record in the Fugue store, the controller
stops its application Deployment and verifies that matching application Pods
have exited. It retains the ManagedApp and database storage for recovery.

After five minutes of a current, verified zero-workload observation, it rechecks
the missing app, missing backing-service records, and absence of other bound
apps, then defaults retained managed PostgreSQL services to `suspended=true`.
Store errors are not evidence of absence. The intent update tests the exact
ManagedApp UID and resource version. A subsequent reconciliation observes that
intent and requests CNPG hibernation with a cluster UID/version fence. Neither
operation deletes Cluster resources, PVCs, credentials, or backups.

If a retained desired cluster is missing, the controller can repair its name
only when exactly one live cluster matches its ManagedApp owner reference UID
and the Fugue tenant, project, owner app, service ID, service type, and managed-by
labels. Ambiguous identities stay in conflict. Foreign-owned clusters are never
modified, even when their names match an old orphan snapshot.

The internal `fugue.pro/orphan-postgres-lifecycle` annotation records whether
the current orphan episode has initialized automatic intent or received an
explicit administrator request. `fugue service postgres orphan resume APP_ID`
persists explicit intent even if the orphan is already resumed, so automatic
initialization cannot undo the request. Suspend works the same way. Adoption
clears this marker after the app record returns; a subsequent orphan episode
must pass the default policy again.

Validate a release using fresh orphan inventory, CNPG hibernation conditions,
absence of database Pods, and unchanged bound PVC identities. A hibernated
Cluster can still declare `spec.instances: 1`: CNPG hibernation stops its Pods
without changing the topology or removing its disks. `ready_instances: 0` alone
does not prove hibernation. Physical storage remains allocated.
