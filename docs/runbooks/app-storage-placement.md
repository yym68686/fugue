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

## Release and verification

Publish controller code through `main` and the declarative CI release. The Helm chart grants storage observation permissions for new installations. Existing production permissions are maintained by the independent storage observation configuration step in the same CI workflow.

After release, verify the controller's observed image identity and leader lease, then compare durable app replicas, ManagedApp replicas and Deployment replicas. For a stopped app, verify that no app Pods remain and that its PVC remains Bound. Inspect only events whose latest occurrence is after deployment when checking for recurring attachment failures; cumulative historical counts are not proof of a new failure.
