# Native production release runtime: single Deployment

This is an implementation boundary, not proof of a production cutover. The
legacy workflow and runtime are still needed for resources that have not passed
the independent cutover checks.

Before cutover, `guardian-native-shadow KUBECONFIG CONTEXT PLAN_DIR CLUSTER_ID REQUEST_ID`
compares legacy and native publication planning against the same candidate.
It reads Kubernetes without publishing, checks the live Deployment UID and
resourceVersion, and requires matching predecessor, candidate, health, and
rollback bindings. The report is not an approval or proof of a live rollout,
rollback, or old writer revocation.

- On an approved, marked DesiredRelease, Guardian compares the live pointer,
  candidate, and rollback monitor with the signed journal payload. It reserves
  an append-only launch entry under the pending resource fence and passes the
  exact pointer resourceVersion, candidate, LKG monitor, intent, and fence to
  its child. Publication and child launch consume different permissions. A
  recorded launch is never renewed or replayed, even if Guardian crashes before
  starting the child. Unknown outcomes require separate observation and recovery.
  A dedicated native Guardian must be configured with
  `FUGUE_RELEASE_NATIVE_REGISTRY` (explicit HTTPS registry host, optionally with
  port), and optionally `FUGUE_RELEASE_NATIVE_REGISTRY_AUTH_FILE` (mounted
  read-only JSON with `username` and `password`). Neither the mounted credential
  nor authorization to promote is provided by GitHub.
- `FUGUE_RELEASE_NATIVE_JOURNAL_FILE` names a read-only mounted JSON file
  containing `cluster`, `database` (one PostgreSQL endpoint with hostname-verified
  TLS), and `ownerPublicKeys` (key ID to base64-encoded Ed25519 public key).
  Provision the principal outside GitHub: `USAGE` on `fugue_control`, `SELECT`
  on intents, approvals, resource baselines, claims, results and launches, and
  `INSERT` only on `execution_launches`. It must not own the schema. It cannot
  add approval or terminal records or update and delete historical entries.
  Claim, launch and completion entries are written to the execution audit
  in the same SQL transaction. If the audit fails, no corresponding entry
  or authorization commits.
- The dedicated writer handles exactly one enrolled target. It does not start
  legacy candidate imports, authority activators, or the legacy artifact
  pruner, and it will not publish status or reconcile an unmarked pointer.
- Before leasing and writing, the child reads the immutable approval-bound
  execution snapshot and exact monitor record, verifies the full forward and
  rollback OCI images directly from the registry, and rechecks the pointers
  after acquiring the component Lease. Current-only repair does not walk or
  require the historical predecessor. A marked target cannot fall back to the
  Python verifier or the historical artifact shortcut. This adapter supports
  one existing Deployment and one approved image per manifest, including every
  declared sidecar and init container. It does not authorize deletion,
  additional image volumes, or delegated writes.
- Each native workload write is one UID/resourceVersion-bound server-side
  apply with no forced ownership transfer or client replay. A rejected CAS
  or unknown response cannot authorize compensation against a drifted
  Deployment, or be treated as a completed execution.
- A registry readback from Guardian does not prove that nodes can pull with
  GitHub disconnected, that registry retention pins both versions, or that old
  writers have been revoked and drained. Before switching a resource, prove all
  three, along with exact rollout/rollback and admission at the serving
  endpoints. An existing LKG pointing at GHCR cannot be adopted by merely
  copying its bytes to another registry: its sealed manifest uses the original
  immutable reference. The registry transition needs its own verified baseline
  and cutover procedure. Do not rewrite its historical monitor record to
  synthesize that evidence.
- `ObserveServingRelease` is read-only. It separately verifies the exact
  marked pointer, immutable monitor record and live Deployment, matches the
  ReplicaSet/Pod ownership chain and actual image IDs, and checks recent
  local, dependency, and route health. It never advances the PostgreSQL
  last-verified version: that additionally needs proof this claim cannot
  issue a delayed write. Unknown state is not proof of no write.
- Guardian status reports the serving record separately from the durable
  last-verified record. A healthy provider pointer with an unresolved journal
  claim remains `verifying`; a missing journal does not advance status.
