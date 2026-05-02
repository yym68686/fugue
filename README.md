# Fugue

[中文 README](README.zh-CN.md)

Fugue is a multi-tenant application control plane for k3s. It combines an OpenAPI-first API, an async controller, and a semantic CLI for deploying and operating apps across shared managed runtimes and attached user-owned runtimes.

## Current status

- `fugue-api` and `fugue-controller` now run as separate control-plane components and scale independently. The normal control-plane deployment path uses PostgreSQL as the authoritative store, and the controller is woken by `LISTEN/NOTIFY` when new operations arrive.
- The HTTP surface is OpenAPI-first. `openapi/openapi.yaml` is the source of truth, generated routes are derived from it, and the server publishes `/openapi.yaml`, `/openapi.json`, and `/docs`.
- The CLI is the main operator interface. It supports deploys from local source, GitHub repositories, and container images, plus day-to-day app, runtime, service, and operation workflows.
- GitHub imports now support public and private repos, automatic build detection (`static-site`, `dockerfile`, `buildpacks`, `nixpacks`), stack-aware imports from `fugue.yaml` or Compose, and background sync for tracked repositories.
- Failover is now a first-class workflow: inspect current posture, set app/database failover targets, and execute controller-driven failover for managed runtimes.
- The bundled Helm chart is still an opinionated self-hosted baseline. The production HA path externalizes PostgreSQL, the registry, secrets, and the edge.

## What Fugue can do today

- Multi-tenant tenants, projects, API keys, audit events, and platform-admin views.
- Runtime inventory for `managed-shared`, `managed-owned`, and `external-owned` runtimes, including attached nodes through reusable node keys and `fugue-agent`.
- App deployment from local uploads, GitHub repositories, or container images.
- Async deploy, rebuild, scale, restart, migrate, failover, and delete operations.
- App domains/routes, env/config/files/workspace management, generated secret env from `fugue.yaml`, runtime/build logs, and operation history.
- Backing services and service bindings, including managed PostgreSQL flows.
- Cluster inventory, current/resource-request capacity overlays, resource right-sizing, runtime sharing, and control-plane status inspection.

## CLI quick start

Install a released CLI:

macOS / Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.sh | sh
```

Windows PowerShell:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "irm https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.ps1 | iex"
```

Create or copy an access key from the Access keys page:

- Fugue Cloud: `https://fugue.pro/app/api-keys`
- Self-hosted: your Fugue web URL plus `/app/api-keys`, for example `https://app.example.com/app/api-keys`

Use a tenant-scoped access key for normal deploys. Reserve platform-admin keys or bootstrap keys for `fugue admin ...`, cross-tenant investigation, or product-layer admin diagnostics.

Use it with one copied access key:

```bash
export FUGUE_API_KEY=<copied-access-key>
fugue deploy .
fugue app ls
```

To check the current CLI version or upgrade in place later:

```bash
fugue version --check-latest
fugue upgrade
```

For self-hosted control planes, set the base URL once:

```bash
export FUGUE_BASE_URL=https://api.example.com
export FUGUE_WEB_BASE_URL=https://app.example.com
export FUGUE_API_KEY=<copied-access-key>
fugue app ls
```

If you want Codex to take over the deploy, export the key in the shell Codex will use and give it a direct prompt such as:

```text
Use fugue CLI and the current FUGUE_API_KEY to deploy this project.
```

Common workflows:

- `fugue deploy github owner/repo --branch main`
- `fugue deploy github owner/repo --service-env-file gateway=.env.gateway --service-env-file runtime=.env.runtime`
- `fugue deploy github owner/repo --project argus --dry-run`
- `fugue deploy github owner/repo --project argus --replace --wait`
- `fugue deploy github https://github.com/example/app --private --repo-token $GITHUB_TOKEN`
- `fugue deploy image nginx:1.27`
- `fugue tenant ls`
- `fugue app create my-app --github owner/repo --branch main`
- `fugue app status my-app`
- `fugue app overview my-app`
- `fugue app logs build my-app --operation op_import_123`
- `fugue app env ls my-app`
- `fugue app fs ls my-app / --source live`
- `fugue app db query my-app --sql "select * from gateway_request_logs order by created_at desc limit 50"`
- `fugue app logs query my-app --table gateway_request_logs --since 1h --match status=500`
- `fugue app logs pods my-app`
- `fugue app request my-app GET /admin/requests --query page=2 --query status=500 --header-from-env X-Service-Key=SERVICE_KEY`
- `fugue app diagnose my-app`
- `fugue app logs runtime my-app --follow`
- `fugue app service attach my-app postgres`
- `fugue app resources recommend my-app`
- `fugue app resources auto my-app --mode auto`
- `fugue app failover status my-app`
- `fugue app failover run my-app --to runtime-b`
- `fugue runtime enroll create edge-a`
- `fugue runtime doctor shared`
- `fugue project overview marketing`
- `fugue project watch marketing`
- `fugue project verify marketing --path /healthz`
- `fugue project delete marketing --wait`
- `fugue project images usage marketing`
- `fugue operation ls --app my-app`
- `fugue operation ls --project marketing --type deploy --status pending`
- `fugue operation show op_123 --show-secrets`
- `fugue api request GET /v1/apps`
- `fugue diagnose timing -- app overview my-app`
- `fugue admin cluster status`
- `fugue admin cluster pods --namespace kube-system`
- `fugue admin cluster events --namespace kube-system --limit 20`
- `fugue admin cluster logs --namespace kube-system --pod coredns-abc --container coredns --tail 200`
- `fugue admin cluster exec --namespace kube-system --pod coredns-abc -- cat /etc/resolv.conf`
- `fugue admin cluster exec --namespace app-demo --pod postgres-0 --retries 4 --timeout 2m -- sh -lc "psql -c 'select now()'"`
- `fugue admin cluster workload show kube-system deployment coredns`
- `fugue admin cluster rollout status kube-system deployment coredns`
- `fugue admin cluster node inspect gcp1`
- `fugue admin cluster node disk gcp1`
- `fugue admin cluster node journal gcp1`
- `fugue admin cluster node metrics gcp1`
- `fugue admin cluster dns resolve api.github.com --server 10.43.0.10`
- `fugue admin cluster net connect api.github.com:443`
- `fugue admin cluster net websocket my-app --path "/socket.io/?EIO=4&transport=websocket"`
- `fugue admin cluster tls probe 104.18.32.47:443 --server-name api.github.com`
- `fugue admin users ls`
- `fugue admin users show user@example.com`
- `fugue admin users resolve user@example.com`
- `fugue web diagnose admin-users`
- `fugue web diagnose /api/fugue/console/pages/api-keys --cookie 'fugue_session=...'`

`fugue app overview` and `fugue operation ls/show/watch` now redact env values, passwords, repo tokens, and secret-backed file content by default in JSON output. Pass `--show-secrets` only when you explicitly need the raw values during a debugging session.

`fugue app fs` now supports both persisted storage roots and the live runtime filesystem. Use `--source persistent` to stay inside workspace/persistent storage mounts, or `--source live` to inspect the running container filesystem such as `/`, `/app`, `/tmp`, or `/etc`.

`fugue app db query` lets you run read-only SQL against an app's effective PostgreSQL connection without first dropping into `cluster exec`. It is intended for direct business-table inspection such as `users`, `gateway_request_logs`, or request audit tables, and caps rows by default so routine diagnostics stay safe.

`fugue app logs query` is the semantic wrapper for log-style tables stored in the app database. Instead of writing raw SQL for every investigation, you can point it at a table, apply `--since` / `--until`, add exact or substring filters, and let the CLI build the read-only query.

`fugue app logs build` now renders the artifact chain as part of the text output: build, push, publish, deploy, and runtime. It also tells you whether a builder job was actually observed, whether registry logs showed a manifest `PUT`, and whether the current root cause is "published earlier and later deleted" versus "no publish was observed for this import". Use it when `"import build completed"` alone is too weak and you need to verify whether the image was recorded, published to the registry, linked to deploy, and then observed in runtime pods.

`fugue app logs pods` shows the current pod group plus recent ReplicaSet rollout context, including the revision that replaced an older pod set. This is the CLI path for seeing old rollout context even after `app overview` has moved on to the new revision.

`fugue app resources recommend/apply/auto` uses the control plane's 7-day resource samples to right-size app and managed PostgreSQL requests. The policy keeps memory conservative, leaves CPU limits unset for normal services, and keeps critical workloads such as PostgreSQL pinned to tighter request/limit envelopes.

`fugue app request` lets you call an app's own internal HTTP routes from the control plane side, including admin endpoints that require service keys already present in the app env. Pass `--header-from-env Header=ENV_KEY` to fill auth headers from the effective app env instead of copying secrets into your shell.

When `fugue app request` fails with a low-level error such as `connection refused`, the CLI now also asks the control plane for runtime diagnosis and appends the likely scheduling or storage root cause. This turns a plain transport failure into evidence like "PVC node affinity conflict" or "pod was evicted after disk pressure".

`fugue app overview` now includes a diagnosis section that stitches together the latest import, linked deploy, image inventory, and current runtime pod state. This is the single-command path for cases like "import succeeded, deploy ran, but the runtime image never became available".

`fugue tenant ls` is the direct answer to "which workspace can this key see?". It removes the need to fall back to `fugue api request GET /v1/tenants` before choosing `--tenant`.

`fugue source-upload show <upload-id>` is the read-only inspection path for uploaded source archives. It exposes archive metadata plus the import operations and apps that currently reference that upload, so you no longer need to guess from raw `upload_id` values or hit a missing metadata endpoint.

`fugue deploy` now reuses the same app artifact diagnosis path after a waited import finishes. When the current release image is missing from registry inventory, the command prints that root cause directly instead of making you manually chain `app logs build`, `app release ls`, `app overview`, and `operation explain`.

`fugue operation explain` now inspects the deploy image reference itself. For pending or running deploys that already point at a missing managed image, the diagnosis says so explicitly instead of falling back to "no blocker detected" or a generic queue summary.

`fugue app diagnose` is the direct root-cause command for managed runtimes. Use it when you need the CLI to say "pod was evicted, node had disk pressure, replacement pod is blocked by volume node affinity" instead of making you reconstruct that chain from logs and events by hand.

`fugue app env ls` text output now renders a table with separate source and reference columns plus override information, so normal terminal output is usable without falling back to `--json`.

`fugue operation ls` defaults to a smaller text-mode window and supports `--project`, `--type`, and `--status`, so single-app or single-project investigations no longer require manual eyeballing of a full operation dump.

`fugue api request` shows raw status, headers, server-timing, body, and transport timings for any control-plane endpoint. `fugue diagnose timing -- <command...>` wraps any Fugue CLI command and reports DNS/connect/TLS/TTFB/total timing for each HTTP request it makes.

`fugue deploy github ... --service-env-file service=.env.file` lets topology imports inject different env overrides into different compose or fugue-manifest services. Use this when `gateway`, `runtime`, `worker`, or similar services need different credentials or feature flags without flattening everything into one shared env file.

`fugue admin cluster net websocket` runs the same websocket handshake twice: directly against the app cluster service and again through the app public route. It returns both statuses plus an automatic conclusion, so a `service=101 / public_route=502` incident can be diagnosed from the CLI without dropping to SSH and `kubectl`.

`fugue admin cluster exec` now retries transient EOF and stream-reset failures by default, and exposes `--retries`, `--retry-delay`, and `--timeout` for longer diagnostic commands.

`fugue admin cluster node inspect` uses the existing `node-janitor` DaemonSet to gather host-side `df` / `du` snapshots, kubelet eviction journal lines, related events, and fresh `stats/summary` evidence without SSH. The narrower `node disk`, `node journal`, and `node metrics` subcommands expose the same data in focused views.

Released CLI builds can upgrade themselves with `fugue upgrade`. When the current binary is behind the latest GitHub Release, normal text-mode commands also print a reminder telling you which version is available. Set `FUGUE_SKIP_UPDATE_CHECK=1` if you need to suppress that reminder in a shell session.

`fugue admin users` and the admin aliases under `fugue web diagnose` read the same `fugue-web` page snapshot routes that power the admin product UI. Set `FUGUE_WEB_BASE_URL` (or pass `--web-base-url`) for those commands. Admin page snapshots accept bootstrap bearer auth; workspace-scoped console page routes can also be diagnosed by passing a session cookie with `--cookie`.

`fugue admin users resolve <email>` is the direct answer to "which tenant/workspace does this user actually land in?". It resolves one email to the enriched workspace snapshot, including tenant id/name, default project, first app, and whether a workspace admin key is available.

When `FUGUE_CONTROL_PLANE_GITHUB_REPOSITORY` is configured on the API, `fugue admin cluster status` also shows the latest `deploy-control-plane` workflow run so you can correlate control-plane image rollouts with cluster state. The same command now includes both the desired deployment image and the live observed control-plane pod tags, so you can tell which `fugue-api` / `fugue-controller` images are actually running without dropping to `kubectl`.

`build-cli` packages CLI archives on relevant pushes to `main`, and `release-cli` publishes them as GitHub Release assets when a `v*` tag is pushed.

## Deploying the control plane

Normal remote control-plane releases go through [`.github/workflows/deploy-control-plane.yml`](.github/workflows/deploy-control-plane.yml). Push to `main` or run that workflow manually; it builds and pushes the `fugue-api` and `fugue-controller` images, then upgrades the control plane on the self-hosted runner.

`scripts/install_fugue_ha.sh` is only for initial bootstrap of the bundled three-VPS topology. Do not use it for routine control-plane updates.

Further deployment docs:

- [Bundled/self-hosted deploy guide](docs/deploy.md)
- [Production HA / DR guide](docs/ha-dr.md)
- [Default Helm values](deploy/helm/fugue/values.yaml)
- [Production HA values](deploy/helm/fugue/values-production-ha.yaml)

## Local development

```bash
make test
make build
```

To build only the CLI:

```bash
make build-cli
./bin/fugue --help
```

If you change the HTTP API contract, start in `openapi/openapi.yaml` and regenerate artifacts:

```bash
make generate-openapi
```

`make test` already checks OpenAPI generated-artifact drift.

For quick local runs, the binaries fall back to `./data/store.json` when `FUGUE_DATABASE_URL` is unset.

Run the API and controller in separate terminals:

```bash
export FUGUE_BOOTSTRAP_ADMIN_KEY='fugue_bootstrap_admin_local'
make run-api
```

```bash
make run-controller
```

With the API running locally, the contract is served at `http://127.0.0.1:8080/openapi.yaml`, `http://127.0.0.1:8080/openapi.json`, and `http://127.0.0.1:8080/docs`.

## Repository layout

```text
cmd/fugue                  CLI
cmd/fugue-api              API server
cmd/fugue-controller       Async controller
cmd/fugue-agent            Attached runtime agent
openapi/                   Authoritative API contract
internal/api               HTTP handlers and contract serving
internal/cli               CLI commands and UX
internal/controller        Operation workers and reconciliation
internal/runtime           Managed-runtime rendering/apply logic
internal/sourceimport      Source import and build detection
internal/store             PostgreSQL-backed state store
deploy/helm/fugue          Control-plane Helm chart
docs/                      Deployment and HA/DR guides
```
