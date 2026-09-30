# DNS routing authority transition

A `cell-dns` intent can declare `route_authority_transition` while public Edge
endpoints move from a complete global publication to independent routing Cells.
The declaration pins the previous full parent and all three route, TLS and DNS
children, plus the complete previous topology. Removing explicit authority
aliases must produce the new topology exactly. No machine, pool, capability,
label or failure domain can be changed by this operation.

The compiler compares complete compiled routes, cache policies, TLS authorization
and hard route policy with every referenced Cell. Only explicitly declared DNS
placement and excluded-group aliases can differ. Deployment generations, cache
namespaces, ownership, paths, upstream behavior, state and minima remain bound.
TLS diagnostic event timestamps stay in each immutable source; comparing behavior
does not renew them. HTTPS probes still require fresh certificate and route
proofs with their own deadlines.

The previous signed DNS child supplies the exact physical endpoint and whole
record dependency set. A probe can accept its ordinary neutral binding or one
explicit previous binding. A target still represents one physical Edge. All of
its dependencies must come from one actual publication and authority; partial
old and new proof sets cannot be combined. Existing record minima and evidence
lifetime bounds are preserved. Both local answers and remote runtime-fact
validation enforce the same target rule. Ordinary DNS release refresh behavior
is unchanged.

Compilation verifies signatures and selected references in one store snapshot.
Publication and LKG transactions recheck exact parent/child content, selected
release IDs, fences and frozen lanes. The previous global source must be a
verified full publication. PostgreSQL uses nonblocking shared authority locks,
so a concurrent source mutation rejects the attempt instead of deadlocking or
replacing a positive LKG. Every required DNS process must report fresh
`dns_authority_transition_v1` support before gray or full publication.

Deploy API and private DNS readers through the ordinary component CI path before
publishing a transition artifact. The signed DNS artifact retains every source
for independent offline verification and positive checkpoint recovery. This
protocol does not switch public Services or Fronts. Public handoff additionally
requires fresh transport observations, complete DNS behavior validation and the
existing UID/resource-version selector CAS. A continuously changing source needs
new exact references; a previous proof never silently inherits a newer fence.
