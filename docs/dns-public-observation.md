# Public DNS observation expectations

Public DNS observation is read-only evidence, not a traffic authorization or a
complete correctness proof. Static v1 declarations keep their explicit answer
sets. A v2 address query may instead declare
`answer_source: verified_serving_policy`, without a second static allowlist.

The normal CI entrypoint first captures expectations on the production runner:

1. Resolve each declared public port-53 listener to its current managed Service,
   node-local backend, Ready endpoint, immutable image and typed consumer scope.
2. Read that exact backend's checkpoint and runtime assignment. Verify the
   checkpoint MAC, current signing-key revocations, artifact signatures, parent
   composition, scope, generation, fence, plan and checkpoint freshness using the
   DNS consumer's existing verifier. Recheck Service and Pod UID/resourceVersion.
   A normal same-assignment refresh may publish signed in-memory state before
   its post-activation checks finish and its checkpoint is persisted. If, and
   only if, the verifier reaches `checkpoint_deadline`, discard the mixed pair
   and recollect the entire request, including current trust material. Every new
   pair must pass the complete unchanged verifier. Retry at most six total
   observations per backend, with two-second spacing and no new retry starting
   after a sixty-second retry-initiation budget. Individual Kubernetes reads and
   verification retain their existing bounded timeouts. A changed Pod or Service
   identity/resourceVersion aborts capture, including between retries. All other
   verifier rejections fail immediately; a persistent deadline mismatch still
   fails with `checkpoint_deadline` rather than exporting an old receipt.
3. Export only the requested address records and reduced provenance. Membership
   comes from signed candidates intersected with physical order/selection and
   explicit availability pools and allowed groups. Temporary proof failure does
   not erase an already authorized recovery target. Neither observed DNS answers
   nor runtime health add a new member. Unsupported scoped policies fail closed.
4. Transfer this reduced receipt to the independent external runner as an
   exact-run/attempt artifact. Never transfer keys, checkpoints or private proof
   payloads. Bind it to the byte digest of the Git observation declaration.
5. Independently query UDP/TCP authoritative and recursive servers. Each
   authoritative server uses only its own captured set; recursive samples may
   use the explicit union of these sets. Check response identity, RCODE, minimum
   count, address membership and the captured signed TTL ceiling.

Expectation lifetime is at most ten minutes and never exceeds the checkpoint
lease or an included static value's original expiry. Missing, expired, foreign,
duplicate or mismatched expectations do not fall back to an old allowlist.
Policy export never changes serving state, performs a handoff, renews a proof,
waives a recovery window, or claims that a candidate is currently healthy.

The result retains expected sets and source assignments alongside actual answers
and TTLs, with `authorizes_traffic: false`. It explicitly does not establish exact
selection/ranking correctness or client traffic share. A failure to capture
authenticated expectations is a distinct failed CI job, not evidence of healthy
DNS. Live readiness and actual SERVFAIL still require investigation separately.

Capture failures retain a fixed verifier category (checkpoint authentication,
authority/freshness, policy shape, timeout, or bounded output validation).
Unknown verifier stderr is reduced to `verifier_failed`; raw stderr, checkpoints,
keys and private proof payloads are never surfaced. These categories diagnose
the failed gate and do not make it pass or renew evidence.

Policy authority/freshness rejection identifies the first failing gate in its
original order: query bounds, checkpoint time, snapshot schema/node/group/parent/
plan, assignment (fixed field name), checkpoint deadline, or evaluation time.
The gate category never includes arbitrary verifier stderr or private input.
The collector also records bounded `dns_observation_policy_capture` events with
attempt/outcome, opaque Pod UID, a canonical checkpoint digest and strictly
formatted applied/evaluation/valid-until timestamps. It never records signing
material, checkpoint contents, assignments or proof payloads. These events
distinguish a recovered cross-read race from a persistent mismatch without
changing freshness, readiness or serving decisions. A gate by itself remains
evidence of the failed comparison, not proof of the upstream cause.

An intervening policy/authority cutover or a recursive cache holding an older
generation can cause a conservative mismatch. Capture a fresh receipt and
inspect version/TTL evidence; do not learn an allowlist from the observed answer,
union arbitrary historical policies, or weaken a release/transport gate to pass.

## Agent grant failure diagnostics

The API's existing local operations snapshot also records one fixed
`agent-response-*` outcome per successful or unavailable Edge-candidate response:
capture, keyring, signing-key validity, remaining lease, authority recheck,
signature generation, or success. Capture cancellation/deadline adds a separate
fixed context classification when the error preserves that cause. Compare
successive snapshots from the same process with its existing probe/capacity
stages; these are cumulative counts, not request-correlated root causes. No
tenant identity, key, address, raw error, or request value becomes a stage label.
The public 503 body and all authorization/capacity gates remain unchanged.
