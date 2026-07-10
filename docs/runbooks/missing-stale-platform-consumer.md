# Missing Or Stale Platform Consumer Runbook

## Trigger

Use this runbook when an ExpectedConsumerSet has a required consumer with no
heartbeat, a heartbeat beyond its freshness window, or fewer observed required
consumers than the fixed expected cardinality.

## Evidence Semantics

- No heartbeat for a required consumer is `unknown`, never `pass`.
- A previously valid heartbeat beyond freshness is `stale`, never `pass`.
- A fresh heartbeat with a definite invariant violation is `fail`.
- Only fresh, authenticated, compatible, converged evidence is `pass`.

Missing and stale evidence must hold release expansion when consumer
convergence enforcement is enabled. It must not automatically quarantine every
node, remove the last healthy edge group, or replace the current verified LKG.

## Read-Only Triage

```bash
fugue admin consumer expected --release-set <release-set-id> --json
fugue admin artifact consumers <artifact-id-or-generation> --json
fugue admin artifact show <artifact-id-or-generation> --json
fugue admin artifact lkg <artifact-id-or-generation> --json
fugue admin release guard status --json
fugue admin robustness status --json
```

Compare by exact `consumer_id`, not only component type. Record:

- ExpectedConsumerSet id and revision;
- required and optional cardinality;
- component, node, scope, failure domain, and cohort;
- heartbeat deadline and freshness duration;
- last accepted heartbeat time;
- desired/actual/LKG generation;
- apply/probe state;
- protocol/schema version;
- release-set id and fencing token.

## Classification

### Consumer Never Appeared

Classify as `unknown`. Confirm whether the component was present in trusted
topology at release prepare time. Do not silently shrink the expected set to
make convergence pass.

### Consumer Heartbeat Became Stale

Classify as `stale`. Determine whether the component process stopped, its
credential expired, the control-plane path is unreachable, or heartbeat
validation rejected newer reports. A still-serving local LKG can preserve data
plane service, but it does not make stale control-loop evidence fresh.

### Topology Changed During Release

Keep the prepared ExpectedConsumerSet revision fixed. Record the topology
change separately. Regenerate a set only through an explicit release prepare
revision; never mutate the existing snapshot in place.

### Optional Consumer Missing

Record the gap, but do not block solely on an optional consumer unless the
release policy requires its cohort or failure domain for this change.

## Safe Response

1. Hold promotion or further canary expansion for the affected release set.
2. Keep serving the current verified LKG when it is valid and unexpired.
3. Preserve existing eligible routes while minimum healthy edge-group
   invariants hold.
4. Restart or re-enroll only the identified component through its normal
   rollout path; do not perform cluster-wide restarts.
5. If evidence is missing because observability is unavailable, keep the state
   `unknown`; do not infer success from lack of errors.
6. If the expected topology itself is wrong, abort the release prepare revision
   and create a new immutable ExpectedConsumerSet from trusted topology.

## Recovery Conditions

The hold can clear only when:

- observed required cardinality equals expected required cardinality;
- each required heartbeat is fresh and identity-bound;
- desired/actual/LKG generations are present;
- desired and actual equal the expected generation;
- apply and probe states pass;
- protocol/schema compatibility passes;
- no generation, sequence, nonce, or fencing rollback is present;
- the release's local/public probes and watch window pass.

If the deadline expires without trustworthy recovery, abort or roll back to the
pinned verified generation. Do not promote on a manually edited heartbeat row.
