# Verified LKG Promotion Runbook

## Trigger

Use this after a Platform Artifact release is serving as
`serving_unverified`, or when explicitly bootstrapping the first LKG from a
shadow release for non-traffic artifacts. A TrafficReleaseSet requires real
gray serving convergence; shadow evidence cannot establish its LKG.

## Required Evidence

- Required consumers converged to the candidate generation.
- Local apply and serving probes passed.
- Public synthetic probes passed.
- The subsystem-specific watch window completed.
- Baseline comparison found no new or worsened blocker.
- Database state remains rollback compatible.
- The release still owns the current fencing token.

Record durable evidence references. Do not mark a boolean true only because the
signal is unavailable.

## Command

```bash
fugue admin artifact verify-lkg <release-id> \
  --fencing-token <token> \
  --consumer-convergence \
  --local-probe \
  --platform-evidence \
  --watch-window \
  --baseline-monotonic \
  --database-rollback-compatible \
  --expected-consumer-set <set-id> \
  --evidence-ref <ref> \
  --reason "<why this generation is verified>"
```

Add `--allow-initial-lkg` only for the first verified generation in a scope.
For TrafficReleaseSet, use an explicit signed `cohort=<id>` gray publication
with current authenticated applied/passed route, DNS and TLS evidence. The
store rechecks the exact release, fence, latest immutable expectations and
fresh facts in the same transaction. Caller assertions cannot bypass them.
The signed typed policy must match the policy embedded in all three children.
Verification commits five recovery references atomically: parent, route, DNS,
TLS and policy. All share the verification release and evidence hash. Failure
preserves every previous reference. Later replacement requires full serving
and fresh full-publication evidence; prior gray facts are insufficient.

Member recovery references do not authorize traffic outside a gray cohort.
Legacy route/DNS readers retain their latest independently verified release
until a parent traffic publication selects them. These are filtered views of
the existing ledger, not another release state machine.

Other artifact kinds retain their explicit initial shadow-seed workflow.

## Verification

```bash
fugue admin artifact lkg <artifact-id>
fugue admin artifact show <artifact-id>
```

The release must show `verification_state=verified`; LKG must reference the same
release and contain a SHA-256 evidence hash. Its schema, generation sequence,
content hash, artifact provenance, and snapshot provenance must match the
referenced artifact, and both signatures must verify against a trusted,
non-revoked key. Repeating the same request is idempotent and must return the
same signed LKG snapshot.

`previous_verified_lkg_generations` is a derived, newest-verification-first list
of distinct generations excluding the current verified LKG. A rollback followed
by re-verification creates a new immutable verification event, but does not
duplicate that generation in the derived list. An expired historical event may
remain available for audit and must not be selected as a healthy rollback
target.

## Failure Handling

- Stale fencing token: stop and inspect the current active release.
- Missing evidence: keep the candidate serving-unverified or abort it.
- Public or local probe failure: rollback using the pinned generation.
- Missing consumer: do not promote. Treat missing evidence as unknown, not pass.
- Expired or signature-invalid current LKG: stop full promotion and follow
  `pinned-rollback-recovery.md`; do not treat the invalid snapshot as an
  existing healthy LKG.

## Group code recovery and traffic rollback

When a Group bundle carries a TrafficReleaseSet binding, code recovery can
renew or restore only the identical traffic binding. A previously published
signed bundle does not authorize a different parent, child, release, channel,
fence or cohort. Rejected historical recovery returns `409
traffic_release_conflict` and preserves the durable bundle and ledger; retrying
with a different code recovery sequence cannot grant traffic authority.

Use the parent ReleaseSet rollback API to authorize an older traffic artifact
under a new fence. It reaches Group Authority through the ordinary route
publication path. Current-bundle lease renewal remains available without
changing the traffic binding. Recovery of existing unbound legacy bundles
retains its previous behavior during migration.

## Consumer evidence after an older-artifact rollback

Use the explicit parent rollback operation, then prepare immutable consumer
expectations for that new publication. A lower child generation is accepted
only for its exact signed parent/child and the current rollback ledger message,
lane, fence, signed cohort and latest expected set. A newer applicable full or
gray release supersedes that permission. The `rollback_target_generation`
field on a normal release is a pinned recovery target, not rollback authority.

The transition keeps the authenticated heartbeat sequence, timestamp and nonce
checks. Consumers must advance their durable heartbeat cursor and generate
fresh evidence even when restoring an older artifact. A replay, frozen lane,
changed topology, bad signature or revoked publication preserves the previous
trusted fact. PostgreSQL serializes backwards transitions with publication and
verification; it never upgrades a shared advisory lock while holding row locks.

### Failed Edge serving observations

Active Workers read the current applicable serving assignment, including gray/full
precedence. After signed parent, route, TLS and policy validation, an apply, cache
or readiness failure promptly submits failed route and TLS heartbeats. These
facts carry no unverified actual/LKG generation. The observer rechecks assignment,
activation and loaded binding before reporting; an inactive or superseded Worker
cannot revoke another publication's facts.

Success and failure share a durable monotonic cursor. Failures retain the last
positive receipt and existing serving bundle, discard volatile positive evidence,
and force fresh probing on recovery. If credentials, immutable inputs or cursor
persistence are unavailable, no success is claimed; central freshness expiry
remains the fallback when a negative fact cannot be delivered.

### Executor compatibility evidence

The versioned `traffic_release_v1` capability is declared by the executor in its
trusted heartbeat, including shadow observations. It is scoped to the reported
artifact kind: route/TLS declares signed parent authority, durable Caddy apply,
actual probes, negative facts and recovery; DNS declares atomic zone snapshots,
value expiration, answer-time proof expiry and recovery. Capability data is part
of the heartbeat evidence hash. It never establishes applied/passed or LKG and
must not be copied from a desired assignment. Admission combines fresh, authenticated capability evidence with prepared
required topology and the selected signed cohort. The file and PostgreSQL stores
check every route, DNS and TLS executor in the publication transaction, rechecking
freshness before commit. Full convergence and verified LKG remain separate gates.
Standalone leased DNS stays blocked, and soft overrides cannot bypass capability
admission. A fresh failed serving fact can prove binary support for recovery;
it cannot prove that the candidate is applied, healthy or an LKG.
