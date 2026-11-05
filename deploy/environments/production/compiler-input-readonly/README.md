# Isolated compiler-input diagnostic

`operation.json` authorizes exactly one metadata graph attempt and at most three
complete native reads of one already verified archive. The executable is an
immutable, actually serving API image; no component rollout is requested.

The main-only `compiler_input_storage` lane in the sole `ci.yml` previews and
executes this declaration. Install `scripts/requirements-input-readonly.txt`
before running its tests. No local or SSH apply path is supported.

The trusted runner decrypts only the managed archive reader in memory. It
creates a suspended Job, an owner-bound reader session Secret, rechecks health,
then releases the Job. Platform encryption/signing and writer keys are never
forwarded. The ordinary application database role is unchanged; the exact
program uses read-only transactions and a read-only connection default. This
does not create a database role with reduced SQL privileges.

The Job has one CPU, 512MiB memory, 256MiB ephemeral storage, a ten-minute
deadline, no retries, no service-account token, no mounts and no preemption.
Node identity, usage and reservations, serving Pod imageIDs/endpoints/restarts,
primary identity/restarts, free space and public readiness are checked. Health
failure cancels only this exact Job; foreground UID-fenced cleanup confirms its
Pod is gone before removing the exact session Secret. Cleanup failure remains
visible and must be inspected; never force-delete a production Pod.

Version 2 creates an exact-owned temporary PriorityClass at -1,000,000 with
`preemptionPolicy: Never`, because Kubernetes priority admission rejects a Pod
that sets this policy without a corresponding class. The class is not a global
default and is removed by exact UID after the Job is gone. Admission failures
cancel the attempt immediately instead of waiting or changing business policy.

An immutable ConfigMap receipt survives resource cleanup and refuses replay of
the same declaration. A CI rerun is not permission to repeat reads. A failure
before unsuspension leaves no running inspector. An ambiguous create/start
outcome is not automatically retried. Inspect the saved result and exact
objects before any new declaration.

Structured graph refusal remains a diagnostic result, never a complete graph
or retirement authorization. Full graph success still does not prove local
body verification, complete external ledgers, manual pins or reactivation
admission. Readback failure or incomplete cleanup fails the lane. There are no
copy, route, hot-copy, fence, DELETE or TRUNCATE operations against PostgreSQL
or the archive store.
