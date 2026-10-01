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

The immutable-input handoff gate also authenticates both DNS artifacts and
compares each physical DNS node's complete consumer/query views, zone policy,
client matching and answer rules. Only the declared routing aliases and the
DNS process's own probe-record authority are translated. Probe addresses and
all wire values remain exact. Record and zone enumeration order is canonicalized;
candidate/preference order stays significant. TTL, nameserver, ECS, record-set,
client-selection, ownership or candidate changes reject this gate. It does not
replace the subsequent fresh-runtime and Service identity checks.

Before using a new transition checkpoint, record a compatible API/DNS
predecessor in the code release chain. The first reader release retains the
old predecessor only while no transition artifact exists. A separately accepted
reader release then pins that compatible reader as its rollback target, so a
later code rollback can still validate the currently serving signed artifact.

The `dns-authority-stage` configuration lane accepts one explicit declaration
at a time. It pins the neutral Cell full publications, previous topology and
physical DNS membership, then waits for a fresh verified global full source.
It retains the compiler request's exact immutable source references. The lane
can only compile, publish shadow and prepare expected consumers. It refuses
existing gray/full DNS authority and unrelated shadow predecessors. A retry of
its own identical staging only re-prepares that exact shadow. A separate
observed serving promotion and public transport handoff are still required.

Version 2 staging declarations bind each routing Cell's exact continuous
producer policy artifact, digest, release and fence. The configuration runner
resolves current verified full/LKG publications from only those authorities on
each bounded capture attempt. It retains all concrete parent/child references
before compiling; those exact references, rather than the selectors, enter the
signed artifact and transactional checks. A changed producer, foreign lineage
or incomplete full publication cannot authorize staging. Independent Cell
refreshes can retry a compiler behavior mismatch within the capture deadline;
other validation failures remain fatal. This keeps queued configuration intent
separate from the immutable execution snapshot without inheriting newer fences.

DNS route probes need public HTTPS connectivity from their actual Pod network.
After Kubernetes translates a public Front Service address to its Pod backend,
an IP-block rule excluding private ranges is insufficient. The independent
`dns-probe-egress` configuration lane pins each public Service UID, declared
generation and logical spec digest. It adds only TCP 443 permission from the
declared DNS authority to those exact backend selectors in the same namespace.
Private/staged Services, management-port translations, foreign ownership and
concurrent Service changes reject the update. NetworkPolicy updates use their
own generation and UID/resource-version CAS; neither Pods, public Services,
traffic artifacts nor code images are changed.
