# Deployment results and durable build evidence

The CLI adds a `result` object with schema version `fugue.deploy-result.v1` to
source deploy JSON and to app deploy/build results. A submitted deployment that
fails or becomes unobservable emits a complete result on stdout before exiting
nonzero. Existing successful response fields remain for compatibility.

- `succeeded`: the tracked operations completed, including the explicitly linked
  deployment. Current serving observation is reported separately.
- `accepted`: work was submitted without waiting; it is not a successful rollout.
- `failed`: a terminal operation failure, cancellation, supersession, or an
  explicit structured API rejection of the submission was observed.
- `unknown`: the final outcome could not be verified. An interrupted observation
  is not proof of a failed operation and must not trigger a duplicate deployment.

`operations` contains only the tracked operation IDs and statuses. `attempts`
contains the persisted build attempts, ordered by attempt number, and their
resource requests/limits in bytes. `causes` contains stable codes, public messages,
confidence, and evidence IDs. `missing_evidence` distinguishes absent history and
failed evidence retrieval. Resource limits are not usage measurements. A storage
limit eviction does not establish which files consumed the space, and a scheduler
memory rejection does not establish actual node memory utilization.

`serving_state` is an independently observed current state, never evidence that a
failed build deployed. Missing, stale or incomplete runtime observations remain
unknown. The CLI does not retry a deployment, restart an app, or change routing to
obtain evidence. Read-only retrieval succeeds even when the underlying operation
failed:

```sh
fugue operation result op_example --json
```

Failed deployments return exit code 5. Unknown results return 6. Exit code 0 with
`--wait=false` means acceptance only. Existing preflight validation errors retain
their exit behavior. Progress is separate from JSON, and `--output-file` mirrors
stdout exactly, including failures. Human-facing deploy wait output deliberately
avoids the legacy cluster-status projection.

Request failures also include an optional `request` object with `stage`
(`upload`, `submission`, or `observation`), the received `http_status`, and
validated edge `request_id` and `trace_id` when available. Causes use fixed public
messages; raw server error bodies, URLs, headers and network exception strings
are never forwarded. An upload rejected with HTTP 408, 413 or 429 reports the
rejection without asking for a nonexistent operation result. This does not change
the upload deadline, admission policy or retry behavior.

A transport timeout, truncated/invalid response, proxy error without a structured
API rejection, or HTTP 5xx leaves submission acceptance unknown. The result asks
the operator to preserve the execution time, CLI version and correlation IDs.
When an operation ID was already obtained, a failed observation cannot change
that operation into a reported terminal failure. It remains queryable using the
tracked ID. No deployment submission is automatically retried by this diagnostic.

## Evidence collection

The import controller records a `build_attempt` evidence item for every completed
builder attempt before the next attempt deletes its predecessor. Capture uses a
separate eight-second read context, including after the operation deadline. It
reads at most four current-attempt pod snapshots, at most 32 UID-scoped events per
pod, and at most 4 KiB of log tail per failing pod. Collection errors do not change
the build result. Storage uses the existing operation evidence ledger and retention;
no database migration or serving-configuration change is required.

The public `payload.build_attempt` is an explicit field allowlist. Diagnostic
snapshots in `payload.builder_diagnostics` contain the correlation details needed
by operators. They are omitted from tenant evidence and from timelines/debug
bundles. Platform-admin requests to the evidence endpoint can retrieve them with
`include_payload=true`; existing API credential redaction still applies.

The controller also persists a `deploy_queued` evidence item. The operation API
projects its validated, same-app child ID as `queued_deploy_operation_id`. The CLI
prefers that field; older servers are supported via their explicit queued-deploy
message. It never infers deployment ownership from creation time, a nearby
operation, a currently serving image, or a runtime ReplicaSet. Missing builder
Job names remain empty.

## User-output security

Deployment failure results never forward raw cluster exception strings, logs,
payload maps, private snapshots, node/Pod/Job names, internal URLs, IP addresses,
registry paths, or credentials. Public explanations are selected from a fixed
cause-code allowlist. This boundary also applies when using a bootstrap key and
cannot be disabled with `--redact=false`. Unrecognized reasons remain unknown.
Operator diagnostics and user deployment results have different disclosure scopes.

Older operations are not backfilled. Against a server without the new collector,
the CLI reports the recorded terminal symptom and explicitly identifies missing
attempt history. It does not invent the earlier failure chain from the final timeout.
