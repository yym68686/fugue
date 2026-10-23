# Explicit Edge release holds

A bandwidth-limited Edge can remain Kubernetes Ready while its Worker health
interface times out or its code cannot be downloaded. Scheduling readiness is
therefore insufficient to decide code-release membership. A release hold is
separate administrator configuration, bound to the current Node UID and Edge
group. Its reason is `quota_exhausted`, `network_unavailable`, or `maintenance`.

The declaration adds `fugue.io/edge-release-hold` to Node annotations. It changes
neither placement labels nor workloads, routes, DNS, traffic authority, or LKG.
The configuration writer requires OnDelete Front and Worker DaemonSets, so later
candidate templates cannot automatically replace a held member's process. Code
execution and Front activation freeze the declared membership for an operation.
The canary observer and continuous Guardian health use the same explicit hold.
Continuous health checks every remaining Pod's owner, node, revision, declared
image, running image digest and readiness; whole-DaemonSet counters still include
held nodes and cannot represent this smaller release cohort.

An admitted extension contributes no required slot while held. A fixed baseline
member can be held only with complete, unambiguous baseline inventory. Missing
members, health failures or labels alone never lower that baseline. At least one
required member remains. Changed Node UIDs, malformed holds and mixed or missing
remaining processes fail closed. Health evidence identifies the required and
held Node UIDs, without labeling the held node healthy.

`scripts/configure_edge_release_hold.py` is a configuration-only executor in the
main CI workflow. It requires exact deployed Guardian and canary-reader revisions,
images, Deployment UIDs, their advertised hold capability and healthy process
readback. It binds the immutable component release plan to the authority group,
baseline and concurrency key; takes the normal component and authority leases;
rejects unfinished authority transitions; and applies a Node UID/resourceVersion
CAS. Its JSON evidence records the operation and actual reader identities. A code
build failure does not prevent an already compatible writer from applying a new
declaration. Repeated identical declarations perform no further Node write.

The first version only installs a hold; it never removes one from Ready, HEAD,
an expected billing date, a successful code install or an expired timer. Safe
readmission additionally requires a separately verified capacity observation,
current code/configuration catch-up and Front recovery. Those automatic capacity
and readmission mechanisms are not implemented by this change. A code-release
hold also does not exclude the endpoint from DNS; that remains separate signed
serving intent and fresh runtime evidence. No production hold is activated by
installing this executor alone.
