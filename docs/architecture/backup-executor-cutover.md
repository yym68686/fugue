# Backup executor cutover

The durable backup run, not a goroutine created by the API request, is the
work item. The API continues authorizing the target, inserting the run and
returning it under the existing HTTP contract. In this compatibility stage,
the API is still the execution owner: its scheduler rediscovers accepted work
after an API restart. `wait=true` keeps the existing `200` response shape and
reflects the stored run; the request does not own the accepted operation's
context. A disconnected request must not cancel an accepted run. Restore
replacement continues to reference its persisted protective backup run rather
than creating another backup on each attempt to observe the restore.

The bounded ordinary-work scanner is read-only and discovers unclaimed
initial runs in stable order; the API scheduler dispatches those IDs to the
existing executor. The coordinated lane handles control-plane and Longhorn
work, and the retry lane handles due retries. Keeping these scans separate
prevents an unavailable coordination lease or future retry from hiding fresh
ordinary work. Multiple API replicas may discover the same ID, but the atomic
database claim fences execution so only one may start its effects. Where
required, the executor must also hold and renew the Kubernetes coordination
lease before starting those effects. This is durable rediscovery, not yet a
transfer to an independent backup worker.

Restarting the API does not mean replaying a side effect. An unstarted pending
run is rediscovered as that same run; stale recovery only selects previously
claimed `running` runs, so pending requests cannot be expired or consume the
bounded recovery page while the API is offline. A previously claimed run must
go through the stale-lease compare-and-swap and effect-specific recovery. A
timeout during an object upload can mean that the object exists; a Longhorn
request can outlive its caller. Verify the original run's object or operation
identity and its recorded artifact before scheduling another effect. Keep
orphan cleanup and inventory checkpoints distinct from backup claims. In
particular, do not use a successful inventory scan as permission to replay a
backup.

Before activation, verify the actual forward and rollback API builds do not
execute worker-owned backup runs. Exercise enqueue followed immediately by API
termination, loss of the worker after claim, and a lost response from the
external backend. Assert exactly one owner, an unchanged HTTP contract, and
either one finalized artifact or an explicit unresolved state with no blind
replay. No execution policy, Kubernetes permissions, or credentials change
in this rediscovery compatibility stage.
