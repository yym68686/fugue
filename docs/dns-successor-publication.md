# Updating an established DNS authority

Initial staging refuses an authority that already has gray or full publication.
That restriction must not be worked around by deleting its serving lanes or LKG.
An established authority instead uses an explicit successor declaration.

`PlatformArtifactReleaseRequest.dns_publication_precondition` binds all three DNS
lanes and the exact current signed positive LKG. The optional guard applies only
to a `cell-dns` release set. It cannot be combined with overrides or producer
reconfiguration. The transaction checks the immutable artifact identities,
digests, selected release IDs, fences, frozen state and LKG evidence/expiry under
the DNS scope lock. A deterministic idempotency key covers the entire request.
An active retry must be the exact successor at the next fence; an old retry cannot
return a superseded publication as success. Memory and PostgreSQL stores perform
the same checks before mutation and before commit.

These preconditions do not replace source authorization, capability admission,
fresh DNS proofs or convergence. Publishing a candidate does not write an LKG.
If observation fails, the old positive LKG remains available with its original
identity and expiry. Only the separate verification operation may replace it.

The `dns-authority-transition-stage/v6` declaration uses the existing retained
global source mode and an explicit `publication_precondition`. It copies the
verified global DNS intent and policy, retaining existing availability policies;
it cannot inject an additional availability override. It requires the exact
approved current global producer and unchanged physical topology. Compilation
still produces only a DNS child, and staging only changes shadow. The optional
`api_connect_address` changes TCP routing while retaining HTTPS/SNI/Host checks.

The `private-dns-promotion/v3` declaration promotes one explicit channel at a
time. Gray needs advancing shadow observations. Full needs fresh advancing
observations of that successor's exact gray publication. Each operation has its own exact lane/LKG preconditions and
physical reader identities. Three to six fresh advancing observations and the
normal checkpoint verifier are required before verifying a new full positive LKG;
`allow_initial_lkg` stays false. Gray and full therefore remain separate,
reviewable configuration changes. Gray never replaces an existing positive LKG;
that old recovery target remains until full verification succeeds. Public
transport handoff is another operation.

The executors remain in the existing configuration CI lanes, independent of image
builds. A failed code release leaves the prior artifact usable and does not require
changing configuration to match a failed binary. The API must already support the
explicit guard before using the new declarations; older readers reject the new
field instead of silently omitting its preconditions.

Tests cover complete signed DNS publication, memory and PostgreSQL transactions,
concurrent successors, stale or absent lanes, frozen lanes, wrong or expired LKG,
foreign publication roles, idempotency, unchanged full/LKG, and refusal to promote
without capability or convergence. This facility does not yet constitute a
periodic independent DNS producer; activating a successor and verifying public
migration remain separate rollout steps.

Established readers also need independent observation state. The
`/v1/platform-state/consumers/shadow-heartbeat` operation accepts only an exact
current, signed `cell-dns` shadow with `staged/shadow_validated` status. It uses
an additive `fugue_platform_shadow_consumers` table (one bounded latest JSON
record per consumer/kind/scope), its own monotonic cursor and its own signed
audit chain. It validates the signed parent/child lineage, selected shadow lane,
expected set and generation in the transaction. Kubernetes backend identity
checks are shared with the original receipt endpoint.

Only an explicit shadow convergence read can select these records. Exact current
shadow capability can be projected into publication admission, but that projection
never modifies serving facts. Full convergence and LKG verification continue to
read only the original serving table. Shadow audit events have a separate action
and target type and cannot appear as accepted serving heartbeats. Legacy initial
enrollment remains compatible.

Deploy the additive schema before enabling the new API. A failure in either
release leaves current serving observations and artifacts intact. Readers must
subsequently use separate local proof state and the isolated endpoint before an
established authority can validate a successor; the storage/API release alone does
not activate that reader behavior or promote gray/full.

Scoped DNS readers run the isolated validator beside serving refresh. It has no
listeners or serving sync, and owns a separate probe scheduler/history, route
facts and source/receipt cache namespace. Candidate validation may observe the
current serving generation for reporting only. It writes the new shadow endpoint
without falling back to the serving endpoint on error. Only its candidate status
is copied into `/healthz`; serving errors, checkpoints, readiness and LKG remain
owned by serving refresh. Cancellation stops both loops. Regression tests hold
the serving lock while the production loop validates a real signed successor,
then reject a receipt, restart validation and check unchanged retained files.
