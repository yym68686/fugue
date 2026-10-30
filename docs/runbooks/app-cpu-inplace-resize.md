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
