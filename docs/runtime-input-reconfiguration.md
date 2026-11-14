# Selecting configuration-only runtime inputs

`producer_reconfiguration.operation=runtime_input` selects a new projection
generation for an existing continuously serving producer. It changes only the
producer generation and its projection-policy artifact ID and digest. The signed
projection policy changes only its generation and `runtime_input_mode`, from
omitted to `configuration-v1`. Static intent, target scope, authority, membership,
placement, DNS and routing policy, schedules and serving controls remain exact.
Generated execution intent is not a substitute for supported static input.

Use the normal artifact validation and shadow producer-release API. Supply the
exact previous producer publication, current full publication, positive LKG
verification evidence and canonical `producer-reconfiguration/<digest>`
idempotency key defined by the OpenAPI contract. Both immutable source signatures
and their original full-publication metadata must match. The current full must
belong to the predecessor producer and be the authenticated unexpired positive
LKG. This operation accepts no unverified rollback-fence exception, soft override,
forced publication or combined canary change. A newer pending gray publication,
frozen lane, changed baseline or competing successor conflicts without writes.

The existing producer configuration CI lane accepts one
`fugue.runtime-input-reconfiguration/v1` declaration under
`deploy/environments/production/runtime-input-reconfiguration/` per push.
It pins the predecessor, derives the successor from its exact signed projection
(only generation and runtime input mode change), and checks current
and automatic rollback readers. `baseline_mode=latest_verified_same_policy`
may resolve a newer verified full/LKG from only that exact predecessor to
survive queue delay; declared and resolved pins are retained. It cannot accept
a regressed fence or waive positive LKG. Only a configuration preview is
fetched, not physical transcripts. A configuration receipt is not serving
acceptance: observe subsequent runtime digests and verified LKGs separately.

The direct database-reader gate does not establish executable DNS acceptance.
Dependent DNS consumers, including isolated/private deployments, must decode the
v37 signed candidate and provide fresh authenticated shadow capability facts.
Check the actual source revision and immutable release record, not only Pod
readiness or `/livez`. Update existing consumers through their component intents
before expecting a dependent DNS producer to progress. Check promised automatic
rollback readers too; a retained v36 checkpoint is not proof that an old binary
can restart from a newly written v37 checkpoint. Preserve resource counts,
public transport, signatures, source authority and positive-LKG gates.

Every member of the shared producer concurrency group uses `queue: max` and
`cancel-in-progress: false`. A newer unrelated configuration must not replace
a pending input cutover. Queueing does not waive the executor's current-source,
positive-LKG, declaration-freshness or transactional CAS checks; stale queued
declarations still stop without changing serving authority.

The queue uses a new `-queued-v2` concurrency namespace so requests from older
workflow revisions with single-pending semantics cannot cancel its waiting
declarations during migration. This is queue isolation, not a serving lock:
old and new jobs must still pass the same backend scope locks and exact CAS.

The store locks the producer scope before its target scope, then rechecks the
signed sources, publication and LKG in the same transaction. An identical retry
succeeds only while that exact successor and baseline remain current. The
operation preserves the selected full publication and LKG; it does not publish
new traffic, rewrite historical artifacts, change original observation expiry,
or release a retention root.

Before selecting this mode, verify the actual readers and promised rollback
versions for the exact scope. Subsequent candidate compilation, fresh consumer
convergence and positive verification use their normal admission checks. An old
generation still requires its original signed inputs and restore, full SHA,
current-trust verification and durable pin before activation. A newly working
current generation does not prove that an unavailable historical generation can
be restored or lawfully discarded. The public history-retirement safety refusal
is unaffected by this configuration transaction.
