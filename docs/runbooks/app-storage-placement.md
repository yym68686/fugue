# Managed application storage placement

Fugue filters compute candidates using the volumes actually mounted by the rendered workload. A Longhorn CSI registration alone is insufficient: the Kubernetes node must also have a Ready Longhorn Node. Replica/disk scheduling flags do not exclude a diskless attachment node.

Before selecting a node and again before applying workload objects, the controller checks:

- The PVC/PV claim identity, deletion state, PV node affinity, pending claim's selected node, and StorageClass allowed topologies.
- CSI registration on the target, plus Longhorn node readiness and volume health where applicable.
- Existing RWO/RWOP attachment requests and live volume users. A pending or detaching VolumeAttachment still prevents selecting another writer node.
- Runtime selectors, taints, node readiness and available compute capacity.

Unavailable observations block a new placement before workload writes. An unchanged, verified serving Deployment remains in place; a candidate revision always receives its own preflight. Existing non-CSI volumes use their native topology rather than requiring CSI registration.

An incompatible placement may be repaired automatically only when the workload has no serving history, all observed Pods are unstarted and match the Deployment template and owner UID chain, no ready endpoint exists, and no attachment remains. Running or previously serving RWO workloads must use the fenced migration workflow. The controller does not expand the Longhorn manager's installation scope or change disk policy.

Existing PVC allocation is monotonic during reconciliation: preserve the larger of its requested size and allocated capacity when recorded intent asks for less. This also applies to Pending claims, and does not prevent a requested expansion. Shrinking requires an explicit data migration to a new claim.

Replica-only intent is compared after removing Fugue-generated environment fields. User environment changes and restart tokens remain significant. A requested replica count of zero is an intentional stop, including for a Pending workload; it does not need a healthy storage target and preserves the PVC.

## Durable scheduling decisions

Every managed operation that evaluates application placement, surge capacity,
or the workload-write storage boundary records `scheduling_decision` evidence
before returning. Read it with:

```sh
fugue operation show op_example --json
fugue api request GET '/v1/operations/op_example/evidence?type=scheduling_decision&include_payload=true'
```

The snapshot records the actual evaluation time, stage and outcome, each observed
candidate's rejection codes, allocatable resources, total requests, existing-app
requests subtracted for replacement, effective requests, memory-policy ratio and
budget, CPU ranking ratio, Pod IP availability, and volume-affinity matches.
Application placement uses the existing workload-class budget and app request;
surge admission uses the rendered Pod request (including sidecars), a 100% memory
budget and no subtraction of the serving Pod. CPU remains a ranking input, not a
new Fugue admission gate. `resource_inventory_observed=false` distinguishes a
storage-only recheck or an unrelated runtime from an observed zero request.

Tenant views use observation-local `candidate-N` and `volume-N` aliases, expose
capacity only for runtime-matching candidates, and never include physical node
names, selector values, PV identities, host paths or foreign workload identities.
Platform administrators can read the separate `scheduling_diagnostics` payload
through the operation evidence endpoint with `include_payload=true`. It contains
the alias mapping, required selectors, taints and PVC/PV affinity expressions,
but not complete Node/Pod/PV specifications or application environment. Timelines
and debug bundles omit that administrator-only payload.

Inventories are split into correlated parts under the existing evidence row-size
and retention bounds. Readers require a complete latest observation and a failure
message matching that observation before reporting a confirmed scheduling cause.
Missing, truncated or superseded evidence is explicitly insufficient; current
node status must never be substituted for historical evidence. Earlier operations
without snapshots remain `insufficient_evidence` with
`missing_evidence=[scheduling_decision]`, not a fabricated memory/storage diagnosis
or a recommendation to redeploy merely to obtain logs.

Evidence writes are best-effort and do not alter the scheduling outcome. Healthy
serving-node preservation, unknown-observation fallback, deliberate stops,
replica counts, capacity budgets and volume single-writer protections are unchanged.

## Release and verification

Publish controller code through `main` and the declarative CI release. The Helm chart grants storage observation permissions for new installations. Existing production permissions are maintained by the independent storage observation configuration step in the same CI workflow.

After release, verify the controller's observed image identity and leader lease, then compare durable app replicas, ManagedApp replicas and Deployment replicas. For a stopped app, verify that no app Pods remain and that its PVC remains Bound. Inspect only events whose latest occurrence is after deployment when checking for recurring attachment failures; cumulative historical counts are not proof of a new failure.
