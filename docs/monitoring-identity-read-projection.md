# Monitoring identity read projection

Background resource sampling and the managed-DNS pending metric need only the
application identity used to associate Kubernetes Pods or domains with an app:
ID, tenant/project, name, runtime ID, desired replicas and current status. They
do not inspect source, routes, executable configuration, files, environment,
backing-service bindings or response payloads.

The monitoring paths now use `ListAppWorkloadIdentitiesWithTiming`. PostgreSQL
projects only those identity and status fields in its `SELECT`; the file-backed
store retains its existing metadata-read fallback. Existing attribution, tombstone
filtering and tenant boundaries remain unchanged. The complete app read remains
available to user-facing and configuration paths that need it.

The disposable PostgreSQL regression test creates apps with large private env
and file payloads, then verifies that the monitoring calls emit exactly one
identity query each, preserve resource attribution and managed-DNS counts, and
leave the full stored configuration unchanged. A baseline overlay of the prior
implementation fails because it emits the full app-row query. Current code passes
under the race detector when the test database is configured.
