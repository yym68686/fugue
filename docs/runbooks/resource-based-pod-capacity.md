# Resource-based Pod admission

The default kubelet ceiling of 110 Pods can reject a deployment while its node
still has memory. Fugue supports an explicit `refresh-join-config` task with
`pod_capacity_mode=resources`: kubelet `max-pods` is derived from its available
UID/GID namespace range (normally 65,535) and `pods-per-core=0`.
The int32 maximum is invalid when kubelet user namespace support is enabled.
This removes the operational count quota; it does
not create unlimited physical memory, Pod addresses, PIDs or storage.

The node configuration is durable and is not rewritten just because API or
updater code changes. An existing node needs updater v41 and a fresh platform
administrator task. Run a dry-run first, then apply with `allow_restart=true` on
one healthy node at a time. The task restores the previous k3s configuration if
its guarded service reload or kubelet health check fails. The reload is bounded
to 90 seconds; systemd start failure returns immediately rather than entering
an unbounded restart wait. `pod_capacity_mode=default` restores 110.
Do not renumber an existing PodCIDR as part of a capacity change. The controller
checks IPv4 Pod address headroom for placement and online surge admission.

New joins use the same resource-based default. Operators can set
`FUGUE_KUBELET_MAX_PODS` to an explicit positive value within that host UID/GID
capacity before joining a node. Custom kubelet user namespace mappings must
pass the same alignment and capacity checks as kubelet itself.

Memory admission is separate configuration, in
`deploy/environments/production/workload-memory/policy.json`. The independent
`workload_memory_policy` CI job applies namespace LimitRanges and publishes the
policy ConfigMap. New tenant namespaces receive that policy through the
controller. It defaults only missing memory requests, preserves explicit
requests and existing namespace defaults, and never creates a new memory limit.
The configuration lane does not depend on a successful component build.

The reconciler uses the Kubernetes resize subresource to add missing requests
on existing Burstable Pods, fenced by resourceVersion. BestEffort Pods cannot
change QoS class in place: they are reported as `requires_rollout` and must be
recreated through the application's normal guarded rollout. A failed rollout
must keep its positive serving LKG. Do not mutate a Deployment template just to
retrofit a renderer default: that changes historical release identities.

Release validation:

1. Confirm memory policy CI completed and inspect any `requires_rollout` entries.
2. Upgrade one node updater; confirm its fresh heartbeat reports v41 or newer.
3. Plan and apply the capacity task, then verify Node Ready, `allocatable.pods`,
   kubelet loaded configuration and application readiness.
4. Observe at least the configured node canary soak before proceeding to other
   failure domains. Exclude intentionally offline nodes.
5. Verify real deployment success, memory requests, Pod address availability,
   API/edge health and absence of new scheduler/CNI errors.

This release also preserves request facts with explicit platform route
ownership, separates serving endpoint availability from completed rollouts,
marks guardian probes as health traffic, and increases declared telemetry,
canary and edge-control resources. Recovery/lifecycle scans use the existing
bounded lifecycle queries. Legacy image maintenance now reads only necessary
image inputs, keeping all historical references and failing closed on read
errors; it does not truncate history to make retention appear faster.
