# Recovering interrupted canonical handoffs

A promoted revision can serve requests while the original canonical Deployment still drains long-lived connections. The logical rollback target can therefore differ from the executable snapshot of those original Pods.

Recovery verifies the original Deployment UID, matching Deployment/template release keys, and observed image. It follows a bounded same-tenant, same-app chain of bound previous/draining revisions to the recorded handoff destination. If an older marker names a different executable as its source, recovery first loads release metadata and considers at most 32 previous/draining snapshots with the observed image. Only an exact rendered runtime key and image match can replace the source reference. Missing history, changed identity, mismatched configuration, unrelated chains and retired intermediates remain errors.

Before updating canonical Service selectors, the corrected original source identity is checkpointed with the existing UID/resource-version fences. Existing positive drain probes and destination readiness checks remain mandatory; recovery does not delete the original workload or infer drain from its age.

Manual promotion to 100% supersedes the controller's pending canary observation. The controller continues through normal final readiness and drain gates instead of failing the now-obsolete weighted route observation. Repeated promotion preserves the original predecessor and promotion timestamp; stale automatic canary steps cannot make the stable release its own candidate or rollback target.

For an affected deployment, inspect its stored intent, bound revision, handoff marker and live runtime evidence. After publishing the controller repair through the standard CI entrypoint, reconcile the committed spec. Do not erase the handoff marker or manually delete old Pods to bypass missing proof.
