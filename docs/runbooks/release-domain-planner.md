# Release domain planner

The release-domain planner is a side-effect-free gate for the shared Fugue Helm
release. Boundary A classifies evidence and writes an immutable
`release-domain-plan.json`; it does not invoke Helm, Kubernetes, host mutation,
GitHub Actions, or a domain transaction adapter.

The planner consumes SSoT values only as opaque base, target, and live digest
strings. It does not define an SSoT document, migration, or write algorithm.

## Fixed release domains

`deploy/release-domains/ownership-v1.yaml` is the release-time mutation
allowlist. It is not workload RBAC. A rendered object or field not positively
matched by this file is unknown.

| Domain | Rendered ownership | Read-only dependencies and exclusions |
| --- | --- | --- |
| `node-local` | NodeLocal PriorityClass, ServiceAccount, ConfigMap, upstream/cache/active Services, and preserved/active DaemonSets | May inspect CoreDNS, Nodes, authoritative DNS, and release identity. It never owns the backup Lease, API/controller, authoritative DNS, image-cache, CRDs, or host repair. |
| `authoritative-dns` | Main and normalized group DNS DaemonSets | May inspect API health, Nodes, NodeLocal coexistence, and transport probes. The shared edge image does not grant edge proxy, Caddy, or Ingress ownership. |
| `control-plane` | API/controller Deployments and PDBs, API Service, optional Ingress, generated CNPG Secret, and CNPG Cluster fields outside `/spec/backup` | May inspect CNPG, release state, and all other domains for invariance. Only a later control-plane adapter may share the explicit backup coordination fence. |
| `image-cache` | Image-cache DaemonSet | May inspect registry/API endpoints, Nodes, cache state, and preserved workloads. It does not own builder labels, registry, image-store defaults, or node maintenance. |
| `backup` | ScheduledBackup, restore-drill CronJob, and CNPG Cluster `/spec/backup` | May inspect CNPG and object-store state. It does not roll API/controller and does not delete historical Backup objects without a separate bounded contract. |

The ownership matcher requires group, version, kind, scope, namespace, resolved
name, and the labels already emitted by the chart. DNS group names are the only
prefix rule; their suffix must exactly equal the suffix of the
`app.kubernetes.io/component=dns-…` label. A missing binding, label mismatch,
overlap, duplicate identity, `generateName`, malformed YAML, or changed CRD is
unknown.

CNPG Cluster ownership uses a single object matcher with a longest-JSON-Pointer
override. `/spec/backup` and all descendants are `backup`; every other desired
Cluster field is `control-plane`. A diff touching both is multiple.

## Dual evidence

The planner accepts two independent classifications:

1. Changed-file evidence identifies the runtime consumers of each changed
   path. Documentation and proven test-only paths may be non-runtime. Shared
   Helm helpers, chart metadata/locks, CRDs, workflows, release entrypoints,
   this planner, and its ownership file are unknown. Runtime Go files require
   package-to-binary consumer evidence; semantic backup ownership is added to,
   not substituted for, the actual image consumers. Any out-of-domain consumer
   is unknown. Versioned values files require changed leaf JSON Pointers.
2. Rendered-object evidence structurally compares base and target Kubernetes
   manifests. It expands `List`, rejects duplicate identities, removes only
   `status` and an empty renderer-created `metadata.creationTimestamp`, and
   preserves labels, annotations, checksums, and array ordering. Every changed
   JSON Pointer must have one owner. Helm test hooks are ignored only when the
   caller explicitly confirms the real upgrade does not execute them; the plan
   records that ignored evidence.

The two known-domain sets must be identical. The planner never takes their
union to guess an executable result.

## Planner results

| Result | Meaning before integration |
| --- | --- |
| `zero` | Both classifiers are empty and every changed file is proven non-runtime. A future dispatcher must perform no Lease, Helm, Kubernetes, host, or transaction write. |
| `single` | Both classifiers identify the same one domain. A future dispatcher may select only that adapter under the existing global production mutex. |
| `multiple` | Both classifiers identify the same set of two or more domains. The release is blocked before the first write and is not split automatically. |
| `unknown` | Parsing, matching, values/dependency evidence, digest stability, or the file/render conjunction failed. No generic release-risk approval may bypass this result. |

The plan binds opaque base/target/live identities, base/target/repeated-target
manifest digests, the ownership digest, changed-file evidence digest, and one
canonical classification context. That context records the default release
namespace, ownership bindings sorted by name, the Helm-test-hook ignore policy,
and its own digest. The same immutable context is used for both base and target;
the CLI has no separate base/target namespace or binding flags. Base must equal
live, and two independently produced target renders must have the same digest.
`planDigest` covers the full canonical JSON payload; transaction phases must
verify it again when Boundary C is eventually authorized.

## CLI

The CLI accepts already rendered manifests and enriched changed-file evidence.
It deliberately does not run Git, Helm, `kubectl`, or a renderer.

```sh
go build -o /tmp/fugue-release-domain-plan ./cmd/fugue-release-domain-plan

/tmp/fugue-release-domain-plan \
  --ownership deploy/release-domains/ownership-v1.yaml \
  --changed-files /path/to/changed-files.json \
  --base-manifest /path/to/base.yaml \
  --target-manifest /path/to/target.yaml \
  --repeated-target-manifest /path/to/target-repeat.yaml \
  --base-digest OPAQUE_BASE \
  --target-digest OPAQUE_TARGET \
  --live-digest OPAQUE_BASE \
  --namespace fugue-system \
  --binding nodeLocalNamespace=kube-system \
  --binding nodeLocalName=fugue-node-local-dns \
  --binding nodeLocalUpstreamServiceName=fugue-dns-upstream \
  --binding nodeLocalActiveName=fugue-node-local-dns-active \
  --binding dnsName=fugue-dns \
  --binding apiName=fugue-api \
  --binding controllerName=fugue-controller \
  --binding serviceName=fugue \
  --binding ingressName=fugue \
  --binding imageCacheName=fugue-image-cache \
  --binding controlPlanePostgresName=fugue-control-plane-postgres \
  --binding controlPlanePostgresSecretName=fugue-control-plane-postgres-app \
  --binding controlPlaneRestoreDrillName=fugue-control-plane-restore-drill \
  --output release-domain-plan.json
```

All bindings and `--namespace` must come from the one exact render context used
for both base and target; do not re-derive them from path prefixes or invoke the
planner with reconstructed per-side contexts. `--ignore-helm-test-hooks` is
part of that same persisted context. `--changed-files-z` also accepts `git diff
--no-renames --name-status -z`, but runtime Go and versioned values files remain
unknown until enriched consumer or leaf-pointer evidence is used.

Changed-file JSON is an array:

```json
[
  {
    "status": "M",
    "path": "deploy/helm/fugue/values.yaml",
    "valuePointers": ["/controlPlanePostgres/backup/destinationPath"]
  },
  {
    "status": "M",
    "path": "internal/api/backup.go",
    "consumerDomains": ["control-plane"],
    "semanticDomains": ["backup"]
  }
]
```

Boundary B provides a dormant, refs-only producer for the enriched array:

```sh
go run ./cmd/fugue-release-domain-evidence \
  --repo . \
  --base BASE_COMMIT \
  --target TARGET_COMMIT \
  --output /private/path/changed-file-evidence.json
```

The producer resolves both revisions to commit OIDs and wraps `changes` in a
revision-bound `ChangedFileEvidence` document. Its `digest` is SHA-256 over the
compact JSON payload containing `apiVersion`, `kind`, `policy`, `baseCommit`,
`targetCommit`, and `changes`; the digest field itself is excluded. Go consumer
graphs are built from exact Git tree blobs for Linux amd64 and arm64 with
network access disabled, private temporary Go caches, and no local `go.mod`
replacement. Incomplete enrichment writes no output. File output is an atomic
0600 replacement and never follows a destination symlink.

The Boundary A planner still consumes the embedded `changes` array. A future
Boundary C activation must validate the envelope digest and exact base/target
OIDs in the same immutable context as the rendered manifests before extracting
that array. The default release path does not invoke this producer or the
planner yet.

Boundary B also provides a private canonical-manifest subcommand for the
dormant render seam:

```sh
fugue-release-domain-evidence canonicalize-manifest \
  --ownership deploy/release-domains/ownership-v1.yaml \
  --input /private/path/helm-release.json \
  --input-format helm-release-json \
  --namespace fugue-system \
  --release-name fugue \
  --release-version 42 \
  --output /private/path/target.manifest
```

Raw render input must be a regular, non-symlink file with no group or other
permissions. Helm JSON is parsed strictly, bound to the expected release name,
namespace, and version, and reduced to the optional main manifest plus every
hook manifest. Canonicalization then uses the same strict YAML/object semantics as
the planner: it expands `List`, injects the effective namespace, sorts objects
and map keys, preserves array order, hooks, Secrets, and exact numeric values,
and removes only the two normalized renderer fields described above. Output is
an atomic `0600` file and is never written to stdout.

`scripts/lib/control_plane_release_render.sh` contains a still-dormant executor
that derives target, pinned-base, and repeated-target commands from the one
frozen Helm upgrade argv. It captures every raw stream in an isolated `0700`
temporary tree under a hard 16 MiB callback file limit; canonical inputs are
then bounded to 8 MiB. It canonicalizes all three, requires the two target
renders to be byte-identical after canonicalization, lends the files only to a
synchronous consumer, and removes the tree and any callback process group on
every return or signal. No default release entrypoint calls this executor
before the Boundary C atomic activation gate.

Runner, canonicalizer, and consumer callbacks are trusted synchronous release
functions. Their contract forbids daemonizing, double-forking, reparenting, or
creating a new session. The executor keeps their process-group leader anchored,
rejects a successful callback that leaves a same-group descendant, and kills
the anchored group before it can be reaped or its PGID reused.

The built binary exits `0` for `zero` or `single`, `2` for the expected blocked
results `multiple` or `unknown`, and `1` for invalid CLI/input-file framing. A
blocked plan is written to the requested output (or stdout) before the binary
exits `2`; do not infer this contract from `go run`, which reports child exit
statuses through the Go tool itself.

## NodeLocal-only acceptance invariants

Boundary C must not be activated until a NodeLocal-only functional harness
proves all of the following by command trace and before/after object identity:

- backup coordination Lease create/get-for-update/patch/delete/replace count is
  zero, and backup acquire/drain/release helpers are never called;
- API/controller canary, rollout, restart, and patch helpers are never called;
- authoritative-DNS and image-cache run/restore/finalize helpers are never
  called;
- CRD apply, primary recovery, CoreDNS repair, builder labels, route sync,
  node-janitor, and generic host mutation are never called;
- only allowlisted NodeLocal objects change, while API/controller, DNS, and
  image-cache generation/template/controller-revision/Pod identities remain
  unchanged; and
- although the shared Helm revision may advance once, the canonical digest of
  every non-NodeLocal rendered object remains equal to base.

## Current integration boundary

Boundary B now defines the unique real Helm argument builder, the private
three-render executor, the canonicalizer, and revision-bound changed-file
evidence. These seams remain unreachable from the default upgrade `main`; they
acquire neither the existing global mutex nor the control-plane backup Lease and
perform no production write.

There is still no production domain dispatcher, real transaction adapter,
rollback ownership proof, workflow evidence upload, bootstrap authorization,
or no-op release integration. Those pieces must enter together behind the
Boundary C pre-write atomic activation gate and its functional safety harness.
