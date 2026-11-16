# Preinstalled PostgreSQL instance manager

New managed PostgreSQL databases use an immutable operand image containing the
original CNPG 1.29 manager for each architecture. The patched operator trusts
only the verified digest catalog embedded into its controller executable;
tenant images, mutable tags and self-asserted labels never enable this mode.
Existing clusters retain their already-defaulted `spec.imageName`. Omitting an
image during an unrelated Fugue update no longer reselects today's default.
An explicit different image remains an image-upgrade request; clearing the
optional image is not an implicit database migration.

## Startup and upgrades

The image contains `/usr/local/libexec/cnpg-manager`. The generated instance Pod
and initialization Jobs omit only `bootstrap-controller` and execute
`/usr/local/bin/fugue-cnpg-manager`. This tiny entrypoint creates
`/controller/manager` as a symlink to the immutable binary and then uses `exec`.
It does not copy a binary, run another persistent process, or reserve a separate
init-container request. Manager memory is still real consumption within the
database container's unchanged resource budget; this is not a promise that
PostgreSQL can actually run within a 10 MiB memory limit.

Do not remove the `/controller` scratch mount: CNPG also uses it for certificates,
WAL archive/restore helpers, sockets, logs and manager upgrades. Keeping the
standard manager path as a symlink preserves those subprocess invocations.
CNPG's existing hash-verified download and atomic rename can replace the symlink
with an upgraded executable. The entrypoint preserves that file on a container
restart. A fresh Pod gets a fresh scratch directory and starts the image's
manager. The root filesystem remains read-only and the upstream PostgreSQL user,
entrypoint, storage, permissions and resource limits remain unchanged.

The controller artifact continues to retain both original `/operator/manager_*`
binaries byte-for-byte. The catalog and production release guard must match
that exact inventory. Changing the underlying CNPG manager version requires an
explicit upgrade design and validated operand-image transitions; do not discard
historical approved images, weaken the binary guard, or trigger a rolling loop.
Controller-only fixes can preserve the manager inventory and all existing Pods.

## Release and verification

Use only `git push` to `main` and `.github/workflows/ci.yml`.
Changes to Fugue's own controller also require the component's same-commit
`deploy/releases/controller/intent.json`, with its verified live predecessor.
The candidate lane compares source paths against the declared operator's source
revision, so correcting a failed intent cannot silently skip pending builds.

The `cnpg_candidate_artifact` lane first publishes amd64/arm64 PostgreSQL variants
in the existing public controller artifact package (versioned tags start with
`18.3-baked-`). It verifies both managers, original PostgreSQL configuration,
read-only startup, symlink behavior and preservation of atomic upgrades. It also
checks anonymous registry/layer access, so enabling the image never silently
requires new tenant registry credentials. It merges the already-released
catalog with the new digest and embeds that exact catalog in the controller.
The source-tree catalog is intentionally empty; build-derived approvals are
retained with the artifact receipt, not inferred from mutable tags.

Before promotion, a revision-scoped disposable kind cluster on the hosted runner
must pass initialization without bootstrap, SQL write/read, container restart,
Pod recreation with data preserved, and an unchanged legacy-image database.
All kubectl calls in the rehearsal have an explicit temporary kubeconfig,
revision-specific context, loopback endpoint and exact kind-node identity.
The rehearsal never uses the production cluster. Both architecture binaries
are verified; the full Kubernetes rehearsal runs on amd64.

Publishing is not promotion. Update
`deploy/environments/production/cnpg-controller/release.json` in a second commit
with the successful artifact receipt and exact current predecessor. The existing
`external_controller_release` lane requires the unchanged Deployment/ConfigMap
witness, original manager hashes and a full database/Pod identity and health
snapshot before and after the operator rollout. It refuses incomplete rehearsal
receipts and removal of a previously approved image. No existing tenant database
is migrated, restarted, resized, rescheduled, rebuilt or stripped of storage by
this feature rollout. Old databases need a separately authorized maintenance
migration to eliminate their existing bootstrap request floor.

Observe controller readiness, webhook health, logs, database Pod UIDs, restart
counts, primary identities and readiness after promotion. If anything changes
unexpectedly, stop the next promotion and diagnose the stored witnesses rather
than deleting a database Pod or changing its storage. Rollback of an already-used
baked catalog must retain support for those images; an old operator without
baked-mode support is not a safe rollback after new databases have used it.
