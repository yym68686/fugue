# Migrate an existing PostgreSQL primary to a baked-manager image

`fugue service postgres migrate-image <service>` returns a configuration plan
without creating an operation or changing the database. Live preflight is explicitly
reported as pending; a configuration plan is not a health or compatibility certificate.

After reviewing the plan, execute:

```sh
fugue service postgres migrate-image SERVICE_ID --apply \
  --acknowledge-downtime --expected-config-hash CONFIG_HASH
```

This is a dedicated single-instance database lifecycle operation, not an app
deployment or a zero-downtime rollout. A brief database interruption is expected.
The consumer application's zero-downtime policy is neither changed nor bypassed.

The first implementation accepts app-owned, exclusively bound, active single-instance
PostgreSQL services only. The source digest and target must match the platform's
same-version upgrade rehearsal receipt. Arbitrary image upgrades, major-version
changes, replica changes, storage copying and placement migration are out of scope.
The existing CNPG primary update policy must be `unsupervised` / `restart`;
the operation never changes a supervised or switchover policy on the user's behalf.

Before mutation, Fugue checks the rehearsed operator, current Cluster/primary
identity, healthy node, unambiguous existing runtime affinity, matching live/template
resource envelope, original bound PVC/PVs, physical database system ID, and writable
SQL. It persists a credential-free witness and intent before compare-and-swapping
only `Cluster.spec.imageName`. CNPG performs the primary replacement using its
existing lifecycle and storage. Fugue does not delete the Pod or PVC itself.

Completion requires a new healthy primary Pod on the original node and PVC/PVs,
the unchanged Cluster UID, preserved specification/resources and physical system ID,
the approved baked-manager command with no init containers, and a successful SQL
temporary-table write/read/rollback probe. Only then is the backing service image
persisted. Consumer AppSpec, routes, bindings, backup policy and resource intent are
not changed.

If a PATCH response is lost, a post-change check fails, or the controller is
interrupted, the operation stays Running and retains its exclusive mutation lock.
The controller's recovery observer resumes the durable witness; it never turns an
unknown outcome into Failed, makes it cancellable, or automatically rolls back.
Inspect `fugue operation show OPERATION_ID` and its evidence for the waiting reason.
An invariant violation needs investigation; do not clear the lock by deleting data.
