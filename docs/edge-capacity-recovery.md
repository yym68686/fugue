# Edge capacity recovery

An offline member can answer a small HEAD request while its provider limits
bandwidth enough to make application traffic unusable. A heartbeat, Ready pod,
billing date, or successful HEAD response does not establish recovered capacity.

The signed DNS projection policy can opt an individual offline declaration into
`recovery.capacity` using `edge-capacity-recovery-v1`. Existing declarations without
this field retain their previous semantics; a declaration without `recovery`
remains held indefinitely. Code deployment never edits these declarations.

## Scheduling and failure isolation

Recovery runs in a separate background worker and a separate PostgreSQL advisory
lock from configuration publication. Claiming a checkpoint before network I/O
prevents concurrent API replicas or restarts from multiplying probes. Each cycle
has a 45-second budget, at most two attempts, and stops after one capacity attempt.
A capacity attempt has a 30-second overall budget; its paired body transfers have
the policy's shorter deadline (at most 15 seconds). Code observations have separate
three-second deadlines. Completion writes have a two-second budget even when a
probe times out. Shutdown cancels network work.

The configuration producer only reads recovery checkpoints. Unavailable or corrupt
recovery evidence retains the affected hold; it cannot release the member or block
publication to other authorized members. Preview and inventory calls do not probe.

## Evidence needed for capacity recovery

- The exact hostname/path must be authorized by the signed static platform intent.
  These platform routes use the `streaming` traffic class.
- Read the selected public Service, its ready Front and EndpointSlices, and the
  exact Worker that the Front forwards HTTPS to, either by Pod IP or by a
  declared hostPort on an address of the exact Kubernetes node. Check node/pod/service UIDs,
  running Edge and Caddy image digests, source commit, active slot and current group authority.
  Any unfinished group transition, ambiguity, missing observation or old code
  prevents admission. Legacy Front names and arbitrary same-node Workers are not
  used as public-serving identities.
- Obtain fresh route/TLS proofs from the held member and one available reference
  named in policy. Another held member cannot act as the reference. The reference
  must have a different public address.
- Issue existing signed measurement permits for the same random, uncompressed
  1 MiB payload. Both public HTTPS transfers must pass signatures, exact length,
  content digest, route/bundle binding and public-socket attestation checks.
  Ordinary route authorization and applied traffic proof checks remain enforced.
- Apply both the maximum duration and minimum transfer rate to **the complete
  request**, including connection, TLS and headers. Fast body delivery after slow
  headers cannot pass. An unavailable or slow reference also prevents admission;
  the system does not infer provider quota from a single failed path.
- Observe the held member's physical code again and require it to be unchanged.
  Bind its socket attestation to the observed public Front, Service and slot.
- Persist the signed round before recording success. Require distinct scheduled
  successes spanning the configured stability period, on the same physical code.
  A failure, code replacement or stale sequence resets progress. A failed capacity
  round immediately retracts capacity admission, irrespective of legacy HEAD
  failure hysteresis.

These observations grant only candidate eligibility. Configuration compilation,
consumer expectations, rollout proof verification and positive LKG still apply.
They never remove a code-release hold or repair an obsolete Worker image.

## Observability and bounds

Node admission observations expose the last result, completed-transfer rate,
complete elapsed time and report digest. Results distinguish route, reference,
capacity, physical code and evidence persistence failures. They do not claim to
know whether the provider has reset a quota.

Each declaration retains its latest eight full rounds separately from the compact
recovery checkpoint, bounded to 1 MiB. Full reports are not copied into DNS bundles.
The signed parent pins compact successful evidence for historical membership;
later observation failures cannot rewrite already accepted historical evidence.
Kubernetes metadata reads have an 8 MiB response limit. No new public probe endpoint
or authentication bypass is introduced.

## Validation and remaining rollout work

Regression tests cover slow headers, incomplete responses, forged and revoked
signatures, missing references, replaced code, ambiguous physical targets,
replica replay, cancellation, corrupt observations and publication while a recovery
probe is hung. Disposable PostgreSQL tests verify independent elections and atomic
checkpoint persistence across separate store instances.

Enabling capacity policy is a separate configuration rollout. Automatic detection
of a newly degraded member, isolated code catch-up and automatic removal of a code
release hold require their own authorized recovery workflow; the capacity gate
alone does not implement those transitions.

## Bounded configuration previews

The offline configuration publisher explicitly requests `view=runtime_summary`.
This performs the same strict live capture as `view=runtime`, then removes only
`runtime_snapshot.dns_selections[].physical_evidence` from the HTTP response.
Intent, policy, candidate identities, physical selections, recovery observations,
readiness facts and their original timestamps remain present. The response reports
the canonical digest of the full capture, omitted record count and raw JSON byte
count, and explicitly lists the omitted field. The publisher rejects unexpected
omissions or a response from a server that does not support this contract. It
retains the summaries with configuration preview evidence before validation.

The full runtime preview and automatic compiler still use complete replay evidence.
The summary neither renews observations nor authorizes a release. This keeps large
quality histories out of configuration comparison payloads without raising the
publisher's 128 MiB response limit. Full capture memory is unchanged; this is an
export optimization and diagnostic improvement, not a claim of lower DNS RSS.

## Code recovery observations

`code_not_current` requires a successfully observed public Front and its exact
running Worker/Caddy process to prove an older code generation. Transport failures
and missing metadata report `code_observation_unavailable` with the failed stage;
identity disagreement reports `code_identity_conflict`. An unfinished group journal
reports `code_transition_in_progress`. Unknown observations cannot generate a code
catch-up plan. A fresh failure clears any old plan from the inventory response.

For proven old code, `recovery_code_plan` derives at most two stages from the
committed current/previous authority. If a node and the current group use opposite
slots, it can prepare the current slot. If both use the same slot, the retained
previous slot must have a fence strictly between the node and current fences:
prepare/prove/switch to that previous slot, then prepare/prove/switch to the current
slot. Replanning after the first stage yields just the second stage. Missing
history, conflicting same fences and ahead-of-authority states cannot fabricate an
intermediate image or fence. The plan is advisory (`execution_authorized=false`);
it never restarts a Pod, activates a Front or changes membership by itself.

The eventual executor must confirm capacity before downloading code, keep the
current serving process until a prepared inactive Worker proves current signed
configuration, revalidate group/physical identities under bounded leases, drain
old connections before replacing their Worker, and retain DNS/code isolation until
all final capacity and publication gates pass. These execution steps are not
implemented by the advisory planner.
