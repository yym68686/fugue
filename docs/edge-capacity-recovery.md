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
