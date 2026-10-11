# Recovering managed deployment completion after a control-store outage

A transient control PostgreSQL failure could end rollout observation and then also
prevent the failure transaction from committing. The worker returned, leaving a
`running` operation with no executor. Pending workers, source sync and normal app
reconciliation correctly respected that operation's interlock, but no periodic
recovery existed. A healthy deployed revision could therefore remain blocked
until controller restart.

The controller now retains ownership and retries transient active-check, ready
status and terminal transaction errors with exponential backoff capped at 15
seconds. Each Store call retains its existing timeout. Leadership loss cancels
retry. A database outage that exhausts the rollout observation budget resumes
observation; it is not evidence of a failed workload and does not replay apply or
rollback. Logs include the operation ID, persistence stage and retry count.

Final writes and interruption requeues are fenced transactionally by the running
attempt's start timestamp (at PostgreSQL microsecond precision). Completion also
checks operation intent. Cancellation, a newer attempt and intervening config
changes cannot be overwritten. A lost response to a committed completion is
recognized from its same-attempt terminal receipt.

At leader startup, before existing in-flight requeue, and once per minute while
leader, an unowned ordinary deploy can be completed from observed runtime facts.
A claim mutex and attempt registrations exclude live workers. Recovery checks the
rebased desired configuration, complete ManagedApp spec hash, actual scheduling,
release key, image, rollout policy, observed generation, desired ready replicas,
matching pod templates, backing-service readiness and unchanged ManagedApp
identity. The guarded transaction rechecks app configuration under the app row
lock. It records completion without applying Kubernetes resources, restarting
pods, rebuilding an image or changing traffic. Runtime-injected environment is
not persisted as user configuration.

Recovery fails closed when this proof is absent. Canary/revision promotion,
stateful operations and legacy deploys without a configuration baseline retain
their existing recovery paths. There is no age-only force-failure or lock bypass.
The controller uses its existing exclusive leader lease; no additional schema,
public API or deployment-specific exception is introduced.

Regression coverage exercises transient failures, lost commit responses,
cancellation and new-attempt fences, concurrent config changes, database timestamp
precision, normal workload failures, and read-only recovery with wrong images,
unready pods, changed configuration or replacement objects. Guarded completion is
also exercised against PostgreSQL when `FUGUE_TEST_DATABASE_URL` is supplied.
