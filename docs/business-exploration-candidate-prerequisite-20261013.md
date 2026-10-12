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
