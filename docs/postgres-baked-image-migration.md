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

Accepts active single-instance PostgreSQL services that are app-owned or independent,
with exactly one consumer binding in the same tenant and project. Independent
services keep an empty `owner_app_id`: the real consumer anchors the lifecycle
operation and exclusive lock, without assigning database ownership or deploying the app.
The plan exposes `ownership` (`app_owned` or `independent`) and the anchor `app_id`.
Unbound and shared independent databases are explicitly rejected. Their safe
coordination requires a service-only or multi-consumer operation model and is not
implemented by assigning a synthetic owner. Binding changes invalidate an independent
service's configuration plan; applying an operation locks binding, app and service
mutations until verification completes.

The source digest and target must match the platform's
same-version upgrade rehearsal receipt. Arbitrary image upgrades, major-version
changes, replica changes, storage copying and placement migration are out of scope.
The existing CNPG primary update policy must be `unsupervised` / `restart`;
the operation never changes a supervised or switchover policy on the user's behalf.

A Cluster marked by a cold recovery is eligible only as the exact completed
destination. Fugue checks the retained recovery state, Cluster UID and PostgreSQL
system ID, and verifies that the stable Service and its ready backends point
exclusively to the current primary. A retained source must still be fenced.
The marker and recovery record are preserved; an incomplete or unverifiable
recovery and an actively fenced, hibernating, or paused Cluster still block migration.

Before mutation, Fugue checks the rehearsed operator, current Cluster/primary
identity, healthy node, unambiguous existing runtime affinity, the fully applied
persisted resource envelope, original bound PVC/PVs, physical database system ID, and writable
SQL. It persists a credential-free witness and intent before compare-and-swapping
`Cluster.spec.imageName`. When a prior in-place resize left a stale restart template,
the same atomic PATCH also aligns `Cluster.spec.resources` with the verified running
requests and limits. This requires a matching persisted `runtime_resources` target;
unknown drift and changes to other resource dimensions are rejected. The source and
target specification hashes are retained for recovery. There is no separate template
update or extra resource-change rollout. CNPG performs the primary replacement using its
existing lifecycle and storage. Fugue does not delete the Pod or PVC itself.

Completion requires a new healthy primary Pod on the original node and PVC/PVs,
the unchanged Cluster UID, preserved specification/resources and physical system ID,
the approved baked-manager command with no init containers, and a successful SQL
temporary-table write/read/rollback probe. Only then are the backing service image
and, when aligned, its restart resource template persisted. The authorized runtime
requests/limits, consumer AppSpec, routes, bindings and backup policy are not changed.

If a PATCH response is lost, a post-change check fails, or the controller is
interrupted, the operation stays Running and retains its exclusive mutation lock.
The controller's recovery observer resumes the durable witness; it never turns an
unknown outcome into Failed, makes it cancellable, or automatically rolls back.
Inspect `fugue operation show OPERATION_ID` and its evidence for the waiting reason.
An invariant violation needs investigation; do not clear the lock by deleting data.
