# Managed PostgreSQL credential isolation

Managed database credentials are identified by backing-service identity, not
application display names. Separate projects may use the same application name
inside one tenant namespace. Their credential Secrets must remain independent.

`postgres.credential_secret_name` is a server-owned, persisted configuration
reference. Ordinary spec updates preserve it; clients cannot replace it. The
initial allocation uses a stable digest of the backing-service ID. Renaming an
application or publishing an image does not rotate database credentials.

On legacy adoption the controller checks the exact database identity, Secret
ownership and all Cluster references. It retains an unambiguous legacy Secret.
A shared or foreign legacy Secret gets a separate identity, using the password
already stored in that backing service. It does not generate a new password.
The assignment uses a narrow store transaction and rejects stale service
configuration. Credential recovery runs before executable/image rollout gates.

The application owner is reconciled by CNPG through both its bootstrap Secret
and its managed role. Migrations update these two references together with a
Cluster UID/resourceVersion precondition. They preserve the bootstrap method,
database name, owner, recovery settings, storage, placement and database Pods.
This does not re-run initdb. Updating only `managed.roles` is insufficient:
CNPG can restore the password from the old application Secret after a later
Secret change or instance-manager restart.

Secret writes use Create/Update with optimistic concurrency and reject another
backing-service identity. No-op reconciles issue no Secret write. A consumer
bound to an app-owned database does not render or acquire its provider's
database resources. Independently managed services do not acquire a consumer's
ManagedApp ownerReference.

Credential Secrets have deletion protection. Explicit cleanup and garbage
collection retain them while any CNPG bootstrap or managed role references
them. Once all references disappear, the finalizer is removed with concurrency
preconditions. The background loop can finish already-requested deletion after
the original ManagedApp has disappeared. Read failures retain the Secret.

For services with persisted identities, rollout readiness requires the role to
reference that identity and CNPG's password ACK to match its observed managed
Secret version. This is an operator acknowledgement, not a substitute for
bounded verification of a new database connection.

## Verification

1. Compare the persisted backing-service credential reference, live Cluster
   bootstrap/role references and Secret owner identity. Do not print passwords.
2. Verify CNPG acknowledgement and a new read-only database connection using
   the application's current credentials.
3. Compare original database Pod UID/restart count, primary and data sentinel.
4. Observe multiple natural reconciliation cycles. References and password
   values must remain stable; unchanged Secrets must not change version.
5. Keep unrelated applications, database storage and workload images unchanged.

Focused regression coverage includes same-name project rendering, owner
conflicts, optimistic update retries, persisted-reference protection, shared
Secret migration, idempotence and reference-aware deletion.

An opt-in real CNPG rehearsal runs only against a disposable loopback kind
context named `kind-fugue-credential-test`:

```sh
FUGUE_CREDENTIAL_TEST_KUBECONFIG=/path/to/disposable-kubeconfig \
  go test ./internal/controller -run '^TestCredentialMigrationLiveCNPG$' -count=1 -v
```

Install CNPG 1.29 in that isolated cluster first. The rehearsal creates two
databases with a shared legacy reference, migrates them through the actual
controller helpers and verifies fresh password authentication, retained rows,
unchanged Pod identities and stable Secret versions. It never loads the
default kubeconfig. The test cluster is disposable and should be deleted after
inspection.

The SQL transaction rehearsal requires a dedicated local database named
`fugue_credential_test` and `FUGUE_CREDENTIAL_TEST_DATABASE_URL`. It rejects
non-loopback hosts and exercises the production PostgreSQL store path.

## Recovery

Treat the persisted per-service references and passwords as configuration
intent. Never repair one project by copying another project's database password
or by overwriting a shared Secret. Failed steps are retried with the same
identity; they do not recreate databases or delete the previous credential.

After migration, a controller rollback must retain credential-reference support.
Older renderers derive credentials from display names and can reintroduce the
collision. Use a compatible repair release, preserving current per-service
intent, rather than restoring an old conflicting reference. The original
database passwords and volumes remain the recovery baseline.
