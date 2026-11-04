# Managed application serving handoffs

A managed in-place code rollout can replace the canonical Deployment before the
completed-deploy transaction changes its stable release. Independent release
readiness must reject the old stable image in that new Deployment. That rejection
is a runtime fact, not an instruction to withdraw an unchanged serving path.

The producer may retain one application's exact signed positive route during
this transition. This does not defer the scope's unrelated DNS or application
configuration. It also does not declare either workload or release healthy.

Admission requires all of the following:

- The application's exact current deploy operation on a managed runtime must be
  pending, running or waiting-agent and nonfailed, with an enabled single
  stable100 policy naming the same old stable release. Its lifecycle identity is
  captured with the application in the same business snapshot. The application's
  presentation phase may already be `deployed` before the operation commits;
  that phase is not evidence of completion.
- The exact independent old-release/current-image readiness transition. Only
  the corresponding `current_image_mismatch` origin status may use this path;
  other negative origins and explicit route maintenance remain closed.
- Unchanged per-application HTTP authority, ownership, runtime, endpoints, TLS,
  cache policy and constraints. Only the current operation's generic deployment
  generation and derived cache namespace can differ from the signed old input.
- A currently verified full source, signed positive LKG, fresh actual consumer
  convergence, exact source fence and exact old compiled route content.
- The original evidence expiry covers the remaining rollout budget. Captures,
  signing, publication and repeated retention never extend that expiry.

`runtime_snapshot.facts.managed_serving_handoffs` records the operation and
release provenance. API materialization rechecks the durable operation and
compares both immutable input authorization and exact source routes. Store
publication continues to enforce source baseline, signature, fence and expiry.
The existing retained-partition envelope is unchanged for Edge consumers.
Independent origin invariant failures take precedence over a controller's Ready
message; preserving their reason does not declare the origin ready.

Explicit route/policy edits, tenant/runtime changes, failed/finished/replaced
operations, unavailable source evidence and expiry use normal strict admission.
Once a fresh successor passes independent readiness, normal compilation selects
its desired epoch and release; no old route or new readiness assertion is carried
forward by this continuity mechanism.
