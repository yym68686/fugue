# Managed SSH configuration convergence

A Kubernetes Secret write acknowledges publication, not loading by the SSH runtime.
The controller persists these facts independently for each active, candidate and
retained draining slot:

- `artifact` is the desired signed authorization.
- `loaded_artifact` is the last known signed artifact confirmed by bound runtime evidence.
- `evidence` is the latest accepted or rejected observation. Failed runtime probes
  invalidate its binding proof, or clear it if no digest could be read.
- `last_valid_evidence` retains the last positive receipt for diagnosis; it never
  substitutes for fresh evidence after an error or expiry.
- `pending_apply` persists the start and deadline of an outstanding publication.

A publication has a two-minute propagation window. Reconciliation and controller
restarts preserve that deadline. A pending authorization is not periodically
reissued, avoiding a moving digest target. Permission changes still publish
immediately, without waiting for target discovery or old publication completion.
An expired desired authorization is renewed to permit recovery after a long outage;
this does not extend the original deadline or authorize an old runtime snapshot.

`verified` continues to mean fresh, exact confirmation of the **desired** digest.
Activation, rollback, route publication and retirement require this exact gate.
`serving` can additionally describe a known previous artifact during propagation,
provided its host identity, target, permissions, key grants, authorization and
bound receipt remain valid. Removed keys, changed permissions, expired grants,
unknown digests and elapsed deadlines fail closed. A new exact confirmation can
recover an overdue publication. No last-positive receipt can prove a new artifact.

The status API adds `convergence_state` (`applied`, `pending_apply`, `rejected`),
`loaded_artifact_digest`, `loaded_generation`, `pending_apply`, `serving`,
`last_valid_evidence`, and `verification_failure`. Existing `artifact_digest`,
`generation`, `valid_until` and `verified` continue to refer to desired state.
These are additive persisted JSON and API fields; existing control records remain
readable. Published artifact history and pending deadlines are checked on writes.

During `pending_apply`, compare desired and loaded digests and the persisted
deadline. Investigate rejected observations; do not relax digest, identity or
revocation checks to silence a controller error. The underlying SSH server retains
its signed authorization checks and enforces authorization expiry independently.

The runtime rejects a readiness snapshot if configuration changes during target
validation. The controller recaptures HTTP 503 or 429 once after 100 milliseconds,
within the original observation deadline, then repeats all receipt binding checks.
Persistent failure, cancellation, unknown digests and revoked permissions still
fail closed. Failed observations without a timestamp do not emit a synthetic
negative observation age.

## Production Consumer Maintenance

Runtime intent generation 13 removes the quota-exhausted `bwg` front from the
required serving consumers, matching the existing edge-offline declaration.
The US primary, US secondary and German fronts still require exact Pod, Service,
host-key and route acknowledgements. This is a configuration-only change with a
fenced generation-10 predecessor; it does not replace the SSH runtime image or
change any application authorization. Before returning the offline front to the
serving set, verify its transport and current route acknowledgement and explicitly
reenroll it through a new runtime intent. An offline machine cannot supply fresh
drain evidence and must not remain a mandatory online consumer.

The SSH runtime lane selects changed configuration on a hosted runner before
acquiring its production concurrency fence. Unrelated pushes therefore cannot
cancel a pending configuration publication. Generation 11 was cancelled in the
old queue. Generation 12 was skipped when the workflow advanced before execution;
generation 13 retains the actual generation-10 predecessor.
