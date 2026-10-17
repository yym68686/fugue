# Route compiler application read projection

`CaptureRouteBusinessSnapshot` previously transferred every visible application's
mounted file contents, generated-environment instructions, command and arguments.
These execution fields are not inputs to route intent, runtime identity or release
readiness. A production size-only read found 1,750,904 bytes of application specs,
including 1,382,387 bytes of mounted files; excluding these four fields leaves
344,047 bytes. This measures one read family, not total database traffic.

PostgreSQL now excludes those fields in the SELECT projection, before network
transfer and JSON decoding. The file store excludes them before detaching its
compiler view. Both retain all other spec fields, especially `Env`, which holds
app-scoped request-body constraints. No stored application or release config is
updated. This is a reduction in transferred and decoded bytes, with the same
linear scan of visible application rows.

The repeatable-read transaction, visible-row predicate, ownership fields, routes,
runtime observations and readiness checks are unchanged. A snapshot is a partial
read model and must not be persisted as application configuration. Non-object
JSON and invalid retained fields still reach the normal decoder and fail as
before; failed captures return no partial snapshot.

Regression tests cover SQL-level payload exclusion, retained fields and route
policies, unchanged persisted bytes, detached caller mutations, null/malformed
specs, repeatable-read consistency, and full versus projected route compilation.
Valid body policies remain in the route; invalid policies still fail closed.

Validation: `go test ./internal/store ./internal/api -run
'Test(RouteBusinessSnapshot|RouteBusinessAppProjection)' -count=1 -v`, with
`FUGUE_TEST_DATABASE_URL` pointing at a disposable loopback PostgreSQL database
to exercise the real SQL tests. The 1.20 MB synthetic execution-payload case
returns a 187-byte spec directly from the SQL projection.
