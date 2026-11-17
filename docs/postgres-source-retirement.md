# Retire a completed PostgreSQL migration source

`fugue service postgres retire-source <service>` returns a read-only plan for a
completed cold migration whose destination is the current database. This is not
`service delete` or `service suspend`: those commands affect the active service.

Only platform administrators can apply retirement. Initially, the service must
be active, single-instance, and have exactly one bound consumer in the same
tenant/project. App-owned and independent services are supported without changing
ownership. Unbound/shared databases and unknown migrations fail closed.

Inspect the source/destination Cluster UIDs, exact Pods, and retained PVC/PV pairs
in `--json` output. Confirm the identity/configuration hash explicitly:

```sh
fugue service postgres retire-source <service> --json
fugue service postgres retire-source <service> --apply \
  --expected-source-uid <source-uid> --expected-plan-hash <plan-hash>
```

The exclusive, durable operation checks Kubernetes identities, destination
readiness and writable-primary SQL connectivity, stable endpoint backends, source
node reachability, source fencing and stopped PostgreSQL processes. It pauses the
old controller, sets source PV reclaim policies to `Retain`, detaches source PVC
ownership and CNPG discovery labels, then deletes only the exact source Cluster
with orphan propagation and gracefully deletes its recorded Pods. UID and
resource-version preconditions fence every mutation. Unexpected storage consumers,
Jobs, replacement identities, configuration changes or missing evidence block it.
Source Pod adoption, extra containers, ephemeral containers and native sidecars
also block retirement because they fall outside the stopped-PostgreSQL proof.

A PVC with a matching discovery label but no owner is not assumed to belong to
the source. Unless it is the recorded primary claim, it may appear separately in
`protected_volumes`: exact Bound PVC/PV identities, complete PV policy and metadata
configuration are pinned and rechecked. These objects are never patched or deleted,
including their existing reclaim policy. Adoption, configuration changes, missing
objects, or any Pod/Job using them block retirement. The recorded primary claim
must still be owned by the source. Plans with this inventory require a CLI that
understands the field (v0.8.69 or later); older clients fail plan-hash validation.

It never deletes PVCs, PVs, Services, secrets, Jobs or destination resources; never
rebuilds/redeploys the consumer or changes traffic, resources or placement. Old
storage remains Bound and marked
with its retired source UID and plan hash. Storage reclamation is a separate,
explicitly authorized action, not part of this command. Existing billing policy
is unchanged; retirement does not reclaim storage capacity.

An interrupted accepted operation remains running with its exclusive interlock.
Controller recovery resumes the same durable intent; ambiguous mutation responses
are not interpreted as failures or permission to retry with different identities.
Completion requires source Cluster/Pod absence, retained storage verification and
fresh destination checks. Use `fugue operation show <operation-id>` and its timeline
to inspect a waiting operation; never bypass its interlock by deleting storage.
