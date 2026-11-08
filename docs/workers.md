# Workers-compatible applications

Fugue can deploy HTTP/WebSocket Workers directly from GitHub or uploaded source.
The `auto` build strategy detects `wrangler.jsonc`, `wrangler.json`, or
`wrangler.toml`, or a repository containing one unambiguous standalone module
`export default { fetch(...) }` / `addEventListener('fetch', ...)` handler.
An explicit Dockerfile takes precedence. A declared Node start script prevents
configuration-free Worker detection. Multiple candidate handlers require a
Wrangler `main` entrypoint. `--build workers` explicitly selects this strategy.

```sh
fugue deploy github example/worker --env-file ./worker.env
```

The source repository needs no Dockerfile, package.json, or Fugue-specific
wrapper. The build service adds a versioned overlay to its private workspace,
bundles JS/TS and npm dependencies with esbuild, and runs the bundle on workerd.
Upstream source files remain unchanged. GitHub and upload imports use the same
adapter, resource admission, builder evidence, and normal rollout workflow.

## Supported configuration

- `main`, `compatibility_date`, `compatibility_flags`, and `vars`.
- Module Worker objects and legacy Service Worker fetch listeners.
- HTTP, streaming responses, WebSocket upgrade and outbound WebSockets.
- Runtime environment variables/secrets supplied by normal Fugue app configuration.
- JSON Wrangler vars preserve their types. Explicit app environment overrides
  take precedence over defaults from `vars`.

`name`, `account_id`, `routes`, `route`, `workers_dev`, `preview_urls`,
`send_metrics`, and `observability` are accepted Cloudflare deployment metadata;
Fugue's own identity, routes, logging and telemetry remain authoritative.
Workers are not uploaded to the user's Cloudflare account.

D1, KV, R2, Durable Objects, queues, cron triggers, service bindings, assets,
custom builds, environment sections and other unimplemented configuration fail
with their configuration keys before a builder job starts. The adapter does not
substitute development emulators for production data services.

The adapter pins workerd 1.20261108.1 and esbuild 0.28.2. Explicit compatibility
dates/flags are retained; the default date is 2026-11-08. Newer dates fail with an
actionable error. The Linux base image is digest-pinned and includes CA roots.

Secrets are never passed to the builder, embedded in the image, or regenerated
on redeploy. Supply required values with `--env-file` / normal app configuration.
The generic importer does not infer whether an unknown variable is a random
secret, an external API credential, or optional configuration.

## Public requests and state

The edge overwrites forwarded scheme/authority from the public request. The
adapter restores that public URL only when it matches the selected edge route;
it preserves paths, queries, request bodies, Origin and authentication. The
workerd socket stays inside the workload's normal tenant network boundary.
workerd isolates do not replace Fugue's workload isolation and resource controls.

Fugue's configured replica count and placement remain authoritative. An app that
stores sessions in process memory must use a single replica or supply an
explicit shared-state/session-routing design. Runtime compatibility does not
provide Cloudflare's global storage, routing, capacity, pricing, or availability.

Code/runtime images and runtime secrets/route intent remain separate. All
configuration publication uses the normal artifact pipeline; build or rollout
failure must retain the previous serving configuration and image. CLI waiting
tracks the linked deployment and public-route observation. Application-specific
health and protocol checks are still required to verify application behavior.

## Verification

`TestWorkersDetection` checks explicit configuration, standalone detection,
precedence, ambiguity and unsupported bindings. `TestWorkersRuntime` bundles
synthetic module and service workers and executes the pinned real runtime,
checking env/JSON bindings, public URLs, paths, explicit ports and binary
WebSocket round trips. CI runs that integration check before component release.

```sh
npm install --prefix /tmp/worker-tools --save-exact workerd@1.20261108.1 esbuild@0.28.2
FUGUE_TEST_WORKERS_NODE_MODULES=/tmp/worker-tools/node_modules go test ./internal/sourceimport -run TestWorkersRuntime -count=1 -v
```
