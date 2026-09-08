# CLI v0.2.0 workflows and contracts

The migration release is v0.1.124. Version v0.2.0 removes the command paths listed in [the migration map](cli-migration-v0.2.0.md). The replacements keep their existing success JSON by default. App image, release version and release attempt commands also accept `--json --output-version v1` for an explicit typed `schema_version` / `data` envelope.

## Choose the intended effect

| Command | Effect and completion condition |
| --- | --- |
| `deploy .` | Import local source, build and deploy. Existing multipart behavior remains the default; `--request-id` opts into the resumable protocol. |
| `app build` | Rebuild from saved source; the existing API may also deploy. This is not a build-only artifact command. |
| `app deploy` | Apply current desired configuration through the existing deploy API. It does not adopt a different durable source. |
| `app reconcile --plan` | Read committed spec and runtime evidence, with no mutation. This is the default. |
| `app reconcile --apply` | Reapply the committed spec, guarded by an atomic spec hash and active-operation check. Wait for the exact accepted spec to match runtime observations. It neither builds nor selects another image. |
| `app rollback` | Redeploy a historical image. It does not roll back environment, database, file contents or a complete release. |
| `app traffic show` / `set` | Read / modify independent traffic intent. `show --observed` adds bounded route decision samples, which do not establish a global traffic percentage. |
| `app rollout policy` | Configure zero-downtime rollout. Cross-runtime app/database targets belong to `app failover policy`. Combined historical edits must be split explicitly. |
| `operation show` / `watch` / `wait` | Snapshot / observation / terminal assertion. A local timeout never cancels server work. |

### Drift and configuration recovery

`app drift show/check --scope runtime` inspects process environment and command, file hashes, mounts, image digest, ready Pods and endpoint membership. It reads every inspected Pod, up to 16 Pods and 32 files per Pod, with bounded file sizes and a server deadline. Unsupported or missing facts are unknown. Multiple release Pods are compared to their own immutable spec snapshots.

The default `--scope all` also includes recent primary-route decision evidence. Samples are identified as samples: a recent explicit failure can demonstrate route drift, but absence of a failure cannot prove that all edges loaded the current artifact. `check` returns 6 when global routing equivalence is unproven. `app reconcile` applies only committed app configuration; signed route/DNS artifact recovery remains independent through `admin state` / `admin artifact`.

Inspect a plan and optionally save its `expected_spec_hash`. Apply it with `app reconcile <app> --apply --expected-spec-hash <hash>`. Concurrent intent changes or active operations produce a conflict before another deployment is enqueued. After submission, a changed spec hash cannot count as successful verification of the original request.

## Resumable local uploads

```sh
fugue deploy . --request-id source-change-001
fugue operation recover --request-id source-change-001
fugue source-upload status <session-id>
fugue source-upload resume <session-id> <original-archive.tgz>
```

The CLI saves the exact archive and a receipt under its private config directory before sending the first request. The receipt contains API/tenant identity, request/session IDs and archive SHA256; it does not copy environment values, credentials or the import intent. The archive itself may contain project source and is saved with mode 0600. The printed receipt path identifies the exact archive for recovery.

The protocol uses 4 MiB digest-checked chunks, at most 128 MiB per archive, four active sessions per tenant, and a 24-hour upload lifetime. Missing chunks can be resumed; mismatched content or a changed import intent is rejected. Expired incomplete chunks and successfully assembled chunks are reclaimed without deleting durable receipts or referenced source archives.

A session freezes one canonical import request. Operations and their request association are committed in the same transaction, including multi-app topology imports. Repeated submission reads the receipt and cannot run the import twice. If the API dies after reserving a request, the result stays `submitting` / unknown; recovery displays confirmed effects and does not automatically resubmit. This conservative behavior avoids duplicate apps when the client cannot prove whether submission completed. A fresh request ID is an explicit new action, not a safe retry of an unknown one.

Recovery by request ID here refers specifically to the durable source-upload protocol; transport request IDs for other APIs retain their existing request diagnostics.

## Images, data and runtime caches

`app image` manages an app's image inventory, retention and external tracking. `app release versions/version` reads actual serving deployment versions; `app release attempt` inspects execution history. Global `image` commands use exact names/digests/IDs and tenant/project filtering.

`image verify` is a freshness check of reported inventory. `image verify <image> --probe --node <node>` queues the existing authenticated node-updater image-graph verification. Success requires a completed task and a fresh matching digest report from that exact node; neither inventory counts nor a pending job are reported as an active verification. Pins and replication requests do not fabricate replica facts. Follow copies with `image transfer watch`.

`data workspace delete` and `data snapshot delete --workspace <workspace>` show an advisory deletion plan by default. `--confirm` requests deletion after checking the plan. The backend repeats the reference checks under locking. Apps, queued operations, active transfers, unexpired grants, retained snapshots and caches awaiting cleanup can block deletion. Workspace deletion removes metadata; snapshot deletion is logical. Neither command claims that object storage was reclaimed. Use `data gc` with its own retention policy before deleting the workspace metadata. `data grant ls` never exports a token hash.

```sh
fugue data prewarm <workspace> --version <snapshot> --runtime <owned-runtime>
fugue data transfer show <transfer>
fugue data transfer wait <transfer> --timeout 35m
fugue data transfer cancel <transfer>
fugue data prewarm evict <transfer> --confirm
```

Runtime prewarm supports an S3-compatible backend and a concrete managed runtime owned by the workspace tenant. The controller schedules a bounded Job on that runtime, using its own deployed digest-pinned worker image and short-lived object URLs. It passes no control-plane database credentials or Kubernetes service-account token to the worker. A dedicated PVC contains content-addressed verified blobs and the snapshot manifest; manifest filenames are never interpreted as host paths. Completion requires matching digest/byte/file receipts from a successful Pod owned by the exact Job on the intended node.

The 24-hour cache is separate from an app mount; prewarm does not attach or remount app data. A single transfer is limited to 10 GiB and 10,000 manifest entries. The worker has a 30-minute execution deadline. Job/credential cleanup follows completion; failed/canceled work and explicit eviction also remove the PVC. `cache.state=cleanup_pending` remains until Kubernetes reports the objects absent. PVC removal does not override a storage class's PV reclaim policy. Operators can choose the normal default storage class or set `FUGUE_DATA_PREWARM_STORAGE_CLASS` in declarative controller configuration.

Old prewarm records were never authorized for runtime execution. The new controller fails these inert plans with a resubmission hint instead of suddenly executing historical requests. User transfer-complete/checkpoint APIs cannot manufacture prewarm progress or readiness.

## Evidence, contexts and discovery

`diagnose request`, `diagnose operation`, `diagnose trace <app> <trace-id>` and `diagnose incident` share `fugue.evidence.v1`, source states, partial completeness, exact structured relations and diagnostic redaction. They reuse existing server diagnosis rather than inventing a second cause classifier. Missing permissions remain visible. `--require-complete` makes missing evidence an assertion failure; explicit app scope is required for traces.

`context create/use/show/ls/clear/delete` stores only named API/web targets and tenant/project selections. Credentials retain the existing per-API auth storage. Explicit flags and environment override a context; switching the API target does not silently retain a different context's project. `FUGUE_CONTEXT=none` bypasses a corrupt active context for recovery. `capabilities --local` reads the CLI catalog offline; `capabilities` reports advertised server operations and current principal scopes separately. An OpenAPI operation does not prove a feature is enabled or a principal is authorized to call it.

`help --json`, `help search`, `help --format markdown` and shell completion derive from the Cobra command definitions. Documented shell examples are parsed in tests without executing business requests. Export the current full command reference with:

```sh
fugue help --format markdown > cli-command-reference.md
fugue migrate scan ./scripts ./docs --json
```

The local migration scanner reports fixed command prefixes and locations, never argument values or full source lines. It marks dynamic shell construction for review, skips credential/dependency/binary files, and never rewrites files or uploads shell history. Historical audit and migration documents intentionally retain removed names.

## Platform artifacts and cancellation

`admin state show/explain --kind <kind> --scope <scope> --channel <channel>` separates artifacts, current references, verified LKG and consumer convergence. `admin artifact plan` validates without publishing. `admin artifact wait --for verified|consumers` reads trusted server records and expected consumer sets; a published artifact or missing consumer inventory cannot count as a verified LKG. The CLI never turns operator flags into authenticated ACKs.

The current backend already implements `operation cancel --confirm` for pending operations only. Running work is rejected; this refactor does not claim generic compensation or cancellation of in-flight deployment/database effects. Use the state machine appropriate to the object: operation cancel, release abort or data transfer cancel. Timeout only ends the local wait.
