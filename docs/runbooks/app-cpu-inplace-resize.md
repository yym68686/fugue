# Application CPU request in-place resize

Application CPU resize discovers Pods by app identity and proves their serving
Deployment through the Pod controller ReplicaSet UID and the ReplicaSet controller
Deployment UID. Older Pods may lack `fugue.pro/app-workload`; they remain eligible
only with the same app, tenant and Fugue manager labels and that verified owner
chain. Explicit labels naming another workload are excluded. Candidate and orphan
Pods cannot join the serving cohort merely by sharing app labels.

Compatibility discovery does not add labels, rewrite Deployment templates or
restart Pods. The normal complete-replica, readiness, CPU-only resource envelope,
QoS, effective-request, UID, startup-time and restart-count checks still apply.
Unavailable or changed ownership evidence blocks the resize.

After a controller release, check `app-cpu-resize` operation evidence and compare
the preflight Pod identities with current Pod spec, allocated and reported CPU
resources. A container downscale blocked by a higher effective init-container
request remains expected. Automatic right-sizing retains its failure backoff;
deploying a compatibility fix does not erase failed operation history.

Restore reconciliation and execution share the same preflight. A terminating old
Pod remains part of the cohort: a 2/1 cohort is a waiting condition, and creates
no operation. Pending replacements can still restore a previously verified CPU
request. The wait reason is exposed as `CPURequestRestoreWaiting` on ManagedApp.
Unchanged waits are logged only on transition.

An observation checkpoint reserves each automatic attempt before enqueue, across
controller replicas/restarts. Fast failures retry after 5s, 10s, 20s, 40s, 80s,
160s, then at most once every 5 minutes for unchanged intent and runtime facts.
Resource versions and heartbeat timestamps do not reset this budget; a changed
Pod identity, resource envelope, deployment generation or target can. Automatic
restores use one dedicated worker rather than consuming user activation workers.
Accepted PATCH journals still hold the app mutation interlock until verified
convergence or compensation; retry state never grants mutation authority.
