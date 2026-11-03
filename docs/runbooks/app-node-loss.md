# Application node loss

Fugue maintains correctness while a user workload is unavailable. Availability,
replication and standby resource spending remain explicit user choices.

`AppSpec.node_loss` controls only application Deployment Pods:

| Mode | Node not-ready/unreachable lifecycle |
| --- | --- |
| `wait` | Infinite `NoExecute` tolerations retain the original Pod until recovery or explicit user action. Default for newly created apps. |
| `replace` | Native Kubernetes eviction after `toleration_seconds` (default 300, explicit 0 allowed). Placement and replicas remain unchanged. |
| `legacy` / absent on an existing app | Keep existing Kubernetes tolerations and behavior. No automatic migration or workload rollout during a controller update. |

This is not a high-availability guarantee. Replacement requires eligible capacity
within existing constraints, and Kubernetes eviction is not proof that an old
process stopped. Apps requiring exclusive external writers must retain `wait`
until real fencing is available. Database instances have their own independently
configured lifecycle and failover; this field does not modify them or node taints.

Inspect or explicitly deploy a policy:

```sh
fugue app node-loss show <app>
fugue app node-loss set <app> --mode wait
fugue app node-loss set <app> --mode replace --toleration-seconds 300
```

The API equivalent is `PATCH /v1/apps/{app_id}` with
`{"node_loss":{"mode":"wait"}}`. Creation accepts this field in AppSpec. The
console exposes it in app settings. Changing it is a normal explicit deployment
that may replace Pods and consume normally billed rollout resources. It does not
add replicas, standby capacity, regions, failover intent or billing rules, and
cannot undo a Pod deletion already in progress. Omitting it on an update or
recovering an older observation preserves the durable configured policy.

Compare the completed deployment operation with actual Pod tolerations before
considering a policy applied. Existing Pods may keep their previous tolerations
until the explicit rollout replaces them. `CPURequestRestoreWaiting` and
`NodeExecutionUnknown` conditions report waiting/unknown observations without
creating replacement capacity. No successful control-plane delete, replica
counter or Lease is treated as physical node fencing.

Fugue no longer force-removes Pods from unavailable nodes as rollout cleanup.
Cross-runtime failover records source node UIDs before shutdown and refuses to
start the target if the source is unavailable, its identity changed, or any
nonterminal source Pod remains (including terminating Pods). There is currently
no physical power/storage fencing provider in this path: a partitioned source
must recover sufficiently to confirm shutdown before the cutover can proceed.
