# Continuing a safe rollout during canonical drain

A promoted independent revision can receive new connections through the canonical Service while the original canonical Deployment retains long-running connections. The original cohort must not be terminated to make a later release possible.

The handoff annotation retains the original Deployment UID, release key, image and source release ID. Its traffic target may advance only to the current 100% stable release whose durable rollback predecessor is the recorded target. Both revisions must have immutable workload bindings. The controller verifies the destination's health, exact Service identity and fresh edge publication before changing the canonical Service selector with UID, resource-version and selector tests.

`source_release_id` remains unchanged across promotions. `previous_target_release_id` permits recovery if a process stops after persisting the new target but before replacing the Service selector. A previous revision cannot retire or have resources removed while a managed canonical Service still selects it. It must also pass the existing positive connection-drain and current traffic-proof checks.

If the preceding pass already replaced the canonical Deployment but did not restore its Service, only the exact preceding promoted executable snapshot can establish the next drain cohort. Rendering that snapshot with observed scheduling must reproduce the live release key and image; the complete Pod cohort still undergoes ordinary drain validation.

An incomplete canonical drain does not prevent independent cleanup of other withdrawn revisions. Active operations, grace and rollback windows, immutable identities, traffic references, endpoint evidence and positive drain observations continue to govern that cleanup. Failures preserve serving workloads and retry; they never authorize deletion by timeout.

A deployment operation that changes only an already-enabled safe-rollout policy may commit that policy when canonical convergence is pending. Its full remaining desired spec must match the current stored spec, and it must not change deployment source. Mixed image, environment, storage, replica or other runtime changes cannot use this path. The managed desired-state projection is written before the pending handoff is returned; background reconciliation continues to enforce the same drain and readiness guards.
