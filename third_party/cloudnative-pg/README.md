# CloudNativePG status write regression

This patch applies only to upstream commit
`23eae00cd7aad82978397798dd27b600eb25ae3d` (CloudNativePG 1.29.0).
It is a prepared candidate, not an instruction to update the live operator.
`Dockerfile.cnpg-candidate` and the isolated `cnpg_candidate_artifact` job build
and verify that candidate. The job has no production environment or Kubernetes
access. Its receipt records the immutable image and unchanged instance-manager
hashes; no live workload references the candidate until a separate release
intent is enrolled. `deploy/environments/production/cnpg-controller/release.json`
authorizes the fenced release after successful production preflight. Its `external_controller_release` job in
the same `ci.yml` verifies both OCI digests, the original Deployment UID and
configuration digest, operator ConfigMap data, and database instance-manager
hashes. It uploads a fresh recovery witness before any mutation can run.

The external controller writer only replaces the selected container image and
command using UID/resourceVersion tests. The operand image environment, volumes,
replica policy, RBAC and database specifications are not write targets. Every
observed database Pod identity and restart count is compared, while primary,
database configuration and manager hashes must remain stable and readiness
must not decrease. An activation failure restores the exact original immutable
image and command only if the current controller still matches the declared
candidate and preserved configuration. Drift blocks that rollback to avoid
overwriting another writer. Existing unhealthy databases remain visible and
must not regress during the operator rollout.

The candidate also passed the upstream `Pod upgrade` suite: 21 selected specs,
zero failures, with race detection. These tests complement the immutable image
receipt; they do not establish that the production deployment has succeeded.

Three controller paths compare Go collections with `reflect.DeepEqual` even
though their JSON fields use `omitempty`. After an API round trip, empty maps
and slices become nil. An unchanged reconciliation therefore sends two
wire-identical status UPDATEs and, with no plugins, a PATCH whose body is `{}`.
The patch uses Kubernetes semantic equality in those three comparisons.
Tests call the upstream reconciler methods with synthetic objects, check zero
requests for unchanged wire state, and require one request for real resource,
instance and plugin changes. The other status comparisons remain unchanged.

Reproduce in a separate upstream checkout:

```sh
git checkout 23eae00cd7aad82978397798dd27b600eb25ae3d
git apply --check /path/to/fugue/third_party/cloudnative-pg/semantic-status.patch
git apply /path/to/fugue/third_party/cloudnative-pg/semantic-status.patch
go test -race ./internal/controller -run '^TestNoopStatusRegression' -v
```

Live Diagnostics can establish the caller/object write counts, unchanged
resource version and status, configured plugins, and the blocked scheduling
condition. The code regression is additionally reproduced against the exact
upstream source revision reported by the live cluster. Empty writes do not
explain the scheduler's incompatible Pod and PV placement constraints or grant
permission to alter the database's storage/placement intent.

Production release prerequisites:

- Build a controller-only candidate from the pinned upstream commit and patch.
- Preserve the original `/operator/manager_*` binaries and `OPERATOR_IMAGE_NAME`
  so the operator fix does not request an instance-manager or database rollout.
- Verify every retained manager binary digest against the original image,
  and exercise the upgrade-decision tests against the candidate.
- Enroll the existing operator in the normal declarative CI release path with
  an exact predecessor and recoverable previous manifest. Do not bypass that
  path with a live Deployment patch.
- Verify all database Pod identities, readiness and manager hashes before and
  after, then repeat the object-level audit and status watch probes.

The patch is derived from CloudNativePG, licensed under Apache-2.0. Its source
files retain the upstream copyright and license headers.
