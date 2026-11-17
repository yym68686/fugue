# Build results and image-cache retirement

An import and its deployment are separate outcomes. GitHub imports persist the
verified final image, source and configuration on the current operation before
requesting deployment. A failed billing admission leaves that result available
without changing the serving application's source or configuration.

Automatic GitHub releases preview deployment billing before building. Acceptance
still rechecks billing after the build. A blocked preview creates no operation.
When funding recovers, the controller can deploy the saved result instead of
building again, even if the original billing failures consumed the retry budget.
Other deployment failures retain the existing bounded retry policy.

Reuse requires matching app/tenant identity and build-input fingerprint, the
original configuration baseline, an immutable digest, and a fresh complete graph
verification at the original destination cache. Source/configuration changes do
not reuse old outputs. Artifact verification has an eight second deadline;
missing or unverifiable content blocks automatic reuse. Explicit manual intent
still provides the normal rebuild path. No Git SHA alone proves that two images
have identical content. Credentials are excluded from fingerprints; environment
and file values are hashed rather than emitted.

The automatic retry history tracks GitHub origin for both source builds and
image-based deployments. Controller/system operations cannot masquerade as a
manual reset. Successful deployment and explicit manual intent preserve their
existing reset behavior. An automatic source checkout that differs from the queued commit
is rejected before building; the next discovery must make a fresh decision.

`source_release_decision` evidence records changed decisions, the full commit,
input fingerprint, operation inventory count, failure count, and reset/failure
operation IDs. Controller logs also identify memory recovery and input-policy
changes that permit another attempt. Repeated identical blocked polls do not
append duplicate evidence. `build_storage_footprint` records the verified digest,
blob/manifest counts, referenced bytes and blob overlap with the previous build.
These are physical observations, not estimates of exclusive reclaimable bytes;
shared blobs can remain referenced by other repositories or generations.

Image-cache retirement policy `image-cache-retirement/v3` keeps live workloads,
active pins/tasks, incomplete migrations, local pins and minimum replicas
protected. Protection summaries distinguish `current_workload`, `active_pin`,
`migration_cutover_pending` and `active_app_image`, and retain exact reference IDs.
Available/importing images belonging to active apps remain protected until their
normal lifecycle makes a positive retirement decision.

An explicitly deleted/deleting generation with an authenticated control-plane
image identity and complete graph may use a 15 minute age floor (or an explicitly
configured shorter floor). Other content retains the configured default age.
The node's targeted execution age matches the admitted plan; current authority
and node-local graph/pin checks remain mandatory. Shared aliases and graphs
retain their existing protections and deletion byte budget.

Unknown provenance never becomes deletion authority merely through elapsed time.
Existing receipt recovery requires exact app/tenant/operation ownership and a
complete verified graph; ambiguous claims remain quarantined. Reconciliation
logs physical unattributed graph, verified claim, ambiguous claim, recovered and
unresolved counts. Existing independent orphan quarantine/observation policy is
unchanged. This release does not manually clean node storage or alter tenant
placement, resource, spending, replica or retention intent.
