# Candidate startup retry budget

An independent candidate gets a budget of three abnormal main-process
restarts while it has not recovered readiness. The budget is shared by the
candidate's replicas and survives Pod replacement and controller takeover.
Repeated observations of the same restart count do not consume another retry.
A container that has already recovered Ready is not failed because of its
lifetime restart count. Ten minutes of continuously observed readiness resets
the consecutive startup-failure count.

This policy is scoped to the candidate rollout wait. Stable workloads and
CloudNativePG recovery retain their existing policies. Startup probes still
control each start's grace period; the existing rollout deadline also bounds
processes that stay alive without becoming Ready. Image/configuration errors,
init-container failures, sidecar failures and terminal Pod failures retain
their immediate failure handling.

At three abnormal restarts while the process is still failing, the controller records the existing rollout
failure evidence (including logs and container termination facts) and aborts
the candidate through the normal safe-rollout path. Withdrawal restores stable
traffic. The existing retirement worker owns all connection draining, scaling
and deletion; this policy does not bypass its UID, traffic or drain checks.
Candidate admission remains a separate guard against overlapping revisions.

After three releases exhaust this startup budget for the same immutable image
digest and executable configuration, image tracking pauses automatic retries.
Its decision remains `retry_suppressed`, with a startup-budget reason. Time
alone does not reset the budget. A new digest or changed executable input has
its own budget. An explicit API-key/bootstrap deployment resets that input's
budget once per operation; successful deployment completion clears consecutive
failed-release observations. Non-startup failures retain the existing bounded
backoff. The same automatic candidate creation gate checks the budget so a
queued image-tracking, GitHub-sync or image-rebuild operation cannot bypass a
later failure observation. Other system-initiated candidates use this gate too.

Runtime counters and failed-operation receipts are persisted in observation
checkpoints under `candidate-startup-observation/v1/` and
`candidate-startup-failures/v1/`. Only hashes and runtime identity/count facts
are stored, never environment values or file contents. Observation persistence
failure retains the operation interlock and retries observation, without
reapplying the workload or resetting its budget. Invalid observation versions
or changed Deployment identity fail closed. A ledger at its bounded input
capacity requires archival; existing exhausted budgets are preserved.

For verification, check `candidate startup observation` controller logs,
operation rollout evidence, and the existing image-tracking decision history.
Confirm a failed candidate loses traffic and is eventually retired through the
normal worker; deleting a Pod alone is not a retry limit.
