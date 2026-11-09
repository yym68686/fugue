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
