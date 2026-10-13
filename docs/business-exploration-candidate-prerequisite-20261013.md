# Candidate canary placement prerequisite

## Exact failed run

US worker release `5df1df2a`, CI `38302769930`, staged two Ready candidate
workers but did not commit their authority. At 20:29:57 UTC on 2026-10-12 the
execution timed out waiting for CurrentAuthority. The active slot remained `a`
with source `4c107203` and image `sha256:e0176ddcc130d1e6e688613e2be9198f79fb8a80531b5b97071b85406ea74bcf`.

The independent canary-prober repeatedly recorded `candidate worker cohort is
not ready`. Its exact group/slot selector returned the two Ready new workers
plus one BWG worker marked for deletion on 2026-10-11 at 22:12:04 UTC. That
retained object's controller UID matches the current DaemonSet. The current
DaemonSet explicitly requires `fugue.io/schedulable=true`; BWG does not satisfy
that selector. The prober's unconditional deletion-timestamp rejection prevents
creation of a signed candidate canary. A neutral regression fixture reproduces
that rejection. No absent route samples are interpreted as successful probes.

The failed execution also reports unproven rollback health for the staged
standby's image provenance. Subsequent monitoring reports an exact predecessor
record mismatch. Those are separately retained recovery failures; they are not
resolved by claiming the active route is healthy. Do not clear status or weaken
immutable LKG matching to hide them.

## Narrow repair

Reuse the strict read-only retired-placement predicate already used for workload
health. A retained terminating Pod may be excluded only with exact current
DaemonSet ownership, identified current Node, and a demonstrably nonmatching
explicit node selector. Additionally require a current controller observation,
matching group/slot and Pod selector, and the complete desired count of retained
members. Missing nodes, changed owners, required terminating Pods, missing
members, or stale workload facts still reject the candidate. Every retained
member still passes the original immutable-image, readiness, restart and signed
route-canary checks. Reads are bounded, and exact excluded Pod/Node/workload
identities are logged. Nothing is deleted or restarted to clear this condition.

The prober receives only additional `get` access to DaemonSets and Nodes, not
mutation access. The existing Guardian code-release lane deploys this repair.
This is not an exploration intent, a member-removal request, or permission to
restore quota-exhausted nodes. Production acceptance and the separate recovery
record condition must be checked after release.

The Guardian activator has a second, independently verified placement mismatch:
its explicit baseline remains two and `FromNodes` adds the admitted managed node,
requiring three members. The current worker DaemonSet and the already-reviewed
worker release declaration require one baseline plus one admitted member. Its
group Pod reads also include the explicitly suspended BWG objects. The same
frozen cohort now records only an identified Node's explicit
`fugue.io/schedulable=false` declaration for filtering; it never derives suspension
from readiness or connectivity and never reduces the declared member count.
An adversarial test proves that filtering a suspended Pod still leaves a missing
declared member as a failure. The Guardian's separate configuration baseline is
explicitly corrected to one, matching the worker declaration, while the admitted
member remains required. No Node labels or independent Cell topology are changed.

## Exact Node name validation prerequisite

Guardian release `f0ab7694`, CI `38308672354`, completed on 2026-10-12 at
21:13 UTC. Its two controller Pods run image
`sha256:166d814a9ec4cf523693b93d569d589e0a7c09a0e9e19663a073762cc0f15432`,
Ready with zero restarts when read through the cluster API on 2026-10-13.
At 03:43:07–03:43:37 UTC the prober repeatedly logged the exact retired BWG
Pod UID and current DaemonSet/Node identities, proving that placement filtering
now completed. The next rejection was `candidate worker cohort member is invalid`.

The two remaining candidate Pods have distinct Node names and Pod UIDs, exact
matching declared/runtime image digests, Ready containers and zero restarts.
Their Node names are `vps-591f4447` and
`86eabf26-42a9-4f9d-998a-754115f8b829`. The latter fails the component identifier
regex because it starts with a digit. `CandidateWorkerCohort.validateUnsigned`
incorrectly uses that regex for a Kubernetes Node name. The same incorrect
validation appears in baseline, normalization and transition-journal witnesses.
This explains the observed cohort rejection; it does not establish the cause of
any earlier transport or recovery-ledger failure.

Use the existing Kubernetes DNS-subdomain validator specifically for these four
Node-name fields. Neutral tests cover numeric-leading names, UUID-shaped names,
subdomains, repeated internal hyphens, and the length boundary; malformed names,
duplicate identities and invalid Pod identities remain rejected. Component and
group validation, source/image binding, signatures, readiness, quorum, and LKG
predecessor validation are unchanged. Publish through the Guardian lane with
the exact verified predecessor. Exploration remains unactivated until every
required runtime advertises the feature and the separate publication gates pass.

Release `62088513`, CI `38345961113`, completed successfully. Both actual
Guardian Pods run image
`sha256:48e1c56cedfe4b666a72ae34a305a5dcbd16166d81b3e68765c6e540613d5bce`,
Ready with zero restarts. The retained US candidate subsequently advanced from
`loaded` generation 232 to `verified` generation 233 with signed canary result
`sha256:c836bcf5c19261ee8767fd3f5b631870bb19bb43287a0edaf9802fd6f044b583`.
This establishes that the Node-name rejection is resolved in production.

The 04:31 UTC read still binds CurrentAuthority to slot `a`, source `4c107203`,
and its previous image. The distinct worker recovery status still reports the
standby/stable image mismatch and exact monitor-predecessor error; the candidate
has not acquired ordinary traffic authority. Worker intent 209 is therefore a
new normal CI attempt against that unchanged exact serving predecessor. It must
re-run prewrite, retained-connection, candidate, authority and full release checks;
it does not clear recovery state or manually promote the older candidate.

## Bounded HTTP failure evidence

Worker intent 209 (`c99ab300`, CI `38347656328`) failed at 04:51:57 UTC with
`roll inactive edge slot b: pod HTTP response is invalid`. The current authority
remains slot `a`. The managed node's standby Pod now runs the new immutable
target, while the other selected standby still runs `5df1df2a`; the signed canary
correctly refuses that mixed cohort. The DaemonSet is deliberately `OnDelete`,
with two desired/Ready members and one updated member. No manual deletion or
restart is authorized to bypass the release executor.

The recorded HTTP error contains neither endpoint identity, status, response
size nor digest. Both exact standby health endpoints return HTTP 200 and a
group-bound candidate when read at 05:10:39 UTC, which does not explain the earlier
failure. Its specific cause remains unproven; no health rule is relaxed or retry
policy changed. Instead, HTTP failures now retain bounded structured evidence:
UTC time, Pod name/UID, address, path, phase, HTTP status, captured size and body
digest, completeness, and a coarse transport error class. Response bodies and raw
transport errors are not logged. The existing one-MiB limit, status predicate,
request timeout and failure behavior remain unchanged. Target waits retain their
last exact observation when they expire, rather than discarding it.

Tests reproduce the prior information loss for non-200, oversized and canceled
requests and verify identity binding, bounded output and absence of raw response
content. Publish this observation improvement through the Guardian lane before
another bounded worker attempt. This change supplies evidence and is not a claim
to have fixed the unobserved HTTP failure.
