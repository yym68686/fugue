# Image-cache platform-plan shadow consumer

## Scope

This runbook covers the default-disabled, observation-only
`image_replication_plan` consumer inside `fugue-image-cache`. It does not
authorize image replication, chart installation, release promotion, or a
production rollout. Existing registry reads, writes, hydration, and pruning
remain owned by the legacy image-cache path.

## Safety contract

- The consumer accepts only `image-plane.fugue.dev/v1` state for its exact
  lower-case node and `node:<node>` scope.
- The only release channel is `shadow`.
- The active artifact, release, expected consumer set, release set, and fencing
  token must agree before anything is persisted or reported.
- The rotated credential is read from a bounded regular file on every cycle.
  Symlinks, broad write permissions, cross-node identity, unexpected
  capabilities, expiry, and malformed TTLs fail closed.
- HTTP redirects are forbidden so a bearer credential cannot cross origins.
- The persisted observation never contains a bearer token. It is atomically
  replaced with mode 0600, while one previous generation and a bounded archive
  remain available for diagnosis and later lane-local recovery.
- Successful shadow parsing reports `apply_status=observed`, never `applied`.
  It therefore cannot satisfy production consumer convergence.
- Failure of this loop does not fail the registry liveness endpoint and does
  not delete cached images. It is visible as a degraded nested status on
  `GET /fugue/cache/v1/health`.
- `GET /fugue/cache/v1/platform-plan/readyz` is an independent readiness gate.
  It returns success only after a fresh observation within two minutes; it does
  not change `/healthz` or the legacy registry readiness contract.

## Configuration

The loop is disabled unless all configuration is deliberately provided:

```text
FUGUE_IMAGE_CACHE_PLATFORM_PLAN_SHADOW_ENABLED=true
FUGUE_API_BASE=https://<control-plane-root>
FUGUE_IMAGE_CACHE_CLUSTER_NODE_NAME=<lower-case-node>
FUGUE_IMAGE_CACHE_PLATFORM_CREDENTIAL_FILE=/run/fugue/image-cache/platform-component-credential.json
FUGUE_IMAGE_CACHE_REPLICATION_PLAN_PATH=/var/lib/fugue/image-cache/replication-plan.json
```

Optional bounded knobs are:

```text
FUGUE_IMAGE_CACHE_PLATFORM_PLAN_LONG_POLL=30s
FUGUE_IMAGE_CACHE_PLATFORM_PLAN_REQUEST_TIMEOUT=40s
FUGUE_IMAGE_CACHE_PLATFORM_PLAN_RETRY_MIN=2s
FUGUE_IMAGE_CACHE_PLATFORM_PLAN_RETRY_MAX=1m
FUGUE_IMAGE_CACHE_PLATFORM_PLAN_NO_PLAN_RETRY=15s
FUGUE_IMAGE_CACHE_PLATFORM_CREDENTIAL_MIN_VALIDITY=30s
FUGUE_IMAGE_CACHE_PLATFORM_PLAN_ARCHIVE_LIMIT=5
```

HTTPS is required by default. A controlled test or explicitly isolated
test harness may set
`FUGUE_IMAGE_CACHE_PLATFORM_PLAN_ALLOW_INSECURE_HTTP=true`; this exception must
not be carried into any Kubernetes chart or production ownership cutover. The
independent chart has no value or environment mapping for this exception and
requires an HTTPS API URL. Redirects remain forbidden in either mode.

Invalid explicit configuration disables the optional loop and is logged; it
does not take down the serving registry. Management health reports
`configuration_error`; the independent chart must surface this as a rollout
failure before any later ownership cutover.

## Independent chart boundary

`deploy/helm/fugue-image-plane` belongs only to the image-plane lane. It is
inert by default: `helm template` with default values must return no objects.
An explicitly enabled render still creates only one shadow DaemonSet and cannot
publish, replicate, prune, or serve registry traffic:

- the image is required as a fully qualified repository plus exact lowercase
  `sha256` digest; tags are rejected, and the repository must end in
  `/fugue-image-plane-agent` so the legacy cache image cannot be selected;
- replacement is `OnDelete`, so a chart update cannot automatically fan out;
- scheduling is restricted to the exact opt-in label
  `fugue.io/image-plane-shadow=true`;
- the health listener is Pod-loopback-only, with no declared container port,
  Service, host port, or host network;
- the dedicated image entrypoint runs only the platform-plan agent. Its build
  tag and exact Docker source closure omit the legacy registry, disk store,
  image-location reporter, management mutation endpoints, registry background
  workers, OCI registry library, and all Fugue internal packages;
- the Pod has no Kubernetes API token, ServiceAccount, RBAC, init container, or
  privileged capability;
- startup/liveness check agent health, while readiness checks only the fresh
  platform-plan observation endpoint;
- the dedicated host state directory is mounted read/write, and the component
  credential directory is mounted read-only.

Before any release coordinator later authorizes a bounded shadow install, the
node-platform lane must atomically pre-create:

```text
/var/lib/fugue/image-plane-shadow  uid=65532 gid=65532
/run/fugue/image-cache             contains platform-component-credential.json mode 0640
```

The chart deliberately requires `hostPath.type=Directory`; it must not create
or repair host directories. The shadow state path must not equal, contain, or
be contained by the legacy `/var/lib/fugue/image-cache` host path. This keeps a
bad shadow observation, restart loop, or rollback local to the new lane.

Node-updater v37 owns the prerequisite transition. Once a release coordinator
later authorizes `FUGUE_IMAGE_CACHE_PLATFORM_IDENTITY_ENABLED=true` on one
bounded node, every identity refresh first prepares the fixed shadow directory
with uid/gid 65532 and mode 0750. The updater opens every path component without
following symlinks, uses directory-relative create/open operations, rejects
legacy or credential overlap, preserves existing files, and refuses to request
a credential if preparation fails. There is no configurable production state
path and no recursive chown. While the freeze remains active, do not roll out
node-updater v37 or set this flag; the checked-in implementation and tests are
not live readiness evidence.

Safe repository-only validation while production release is frozen is:

```console
helm lint deploy/helm/fugue-image-plane
go test ./deploy/helm/fugue-image-plane
test -z "$(helm template image-plane-shadow deploy/helm/fugue-image-plane)"
```

Do not run `helm install`, `helm upgrade`, label a production node, publish an
image, or dispatch a release until the unique release coordinator explicitly
unfreezes and authorizes that exact digest and node. A later authorized rollback
removes only the independent shadow DaemonSet (or returns the chart to disabled)
and leaves both the legacy DaemonSet and its host state untouched; retain the
shadow observation directory as diagnostic LKG unless incident policy requires
its separately approved removal.

## Read-only diagnosis

1. Read `/fugue/cache/v1/health` and record only the nested state, generation,
   LKG generation, timestamps, and bounded error. Do not copy the credential.
2. Confirm the credential file is a non-symlink regular file, mode 0640 or
   stricter, and still valid for the expected node.
3. Confirm the observation file is mode 0600 and that its outer generation,
   content hash, and LKG generation agree with the embedded plan.
4. Inspect the control-plane expected set and active shadow release. The
   release-set id, expected-set id, release id, fence, sequence floor, and
   issued-at floor must match the component heartbeat.
5. Treat HTTP 409 as a cursor/fence refresh event. The client performs one
   immediate refetch and bounded retry, then backs off without changing the
   registry.

## Recovery

- Credential unavailable or expired: repair only node-local identity rotation;
  do not substitute the bootstrap admin key.
- Control plane unavailable: preserve the current observation and cached image
  data; the loop retries with bounded exponential backoff.
- Invalid or cross-node plan: hold the image-plane lane and investigate the
  server-side artifact/release binding. Do not edit the local JSON to force it.
- Persistence failure: repair the dedicated state path and permissions. The
  component reports a failed observation when it has a trustworthy heartbeat
  contract, but continues serving the existing registry.
- Repeated 409: verify that only one active shadow release and one expected set
  exist for the node, then compare the server cursor with the current release
  fence. Never reset the server cursor manually to make a stale client pass.

Enablement, shadow installation, fault injection, and live rollback evidence
remain prohibited while the production release freeze is active. The chart and
repository checks above are design/build evidence only, not production health
evidence.
