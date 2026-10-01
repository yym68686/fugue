# Independent DNS and routing publication

Implementation work for the remaining migration must account for routing Cells
advancing before the next DNS publication. A periodic DNS producer alone does
not close that interval: application deployment generations change route proof
digests, and a failed routing candidate must leave the previous positive LKG
usable. Preserving an original unexpired proof is a bounded bridge, not a
continuous source publication mechanism.

The intended contract is an explicit source authorization in the signed DNS
artifact. DNS configuration continues to own its record set, hostname/tenant/app
ownership, physical candidate addresses, pools, failure-domain constraints,
client policy and TTL limits. Each authorization names a routing scope and exact
approved immutable producer policy artifact by ID and digest; it is not a public
discovery mechanism. The actual policy activation and traffic release IDs and
fences are separate observed facts. Approval does not infer a publication or
advance any fence. This permits approving a new policy before activating it.
The baseline embedded route publications remain sufficient for offline
verification of the original compiled artifact. Existing artifacts without this
authorization retain their exact-reference semantics.

A DNS consumer can read current route/TLS publications through a private,
assignment-bound API only for sources authorized by its selected signed DNS
artifact. The response must bind the DNS assignment, source scope, producer
policy, signed parent/children, exact release ID/channel/fence and selection
revision. It may include the current gray candidate, current full publication
and current unexpired verified full LKG. Failed or superseded candidates are not
an open historical allowlist. Retaining the current LKG is necessary while a
routing candidate fails or while physical consumers apply at different times.

The DNS reader verifies signatures, source ownership and source selection
before using a fresh HTTPS route proof. All dependencies for one physical
target must come from the same exact publication. The current signed routing
policy can further restrict DNS eligibility and raise quorum requirements.
DNS must continue intersecting these facts with its own signed candidates and
ownership constraints: a source update cannot add an address, change a DNS
record's owner, move a physical Edge between Cells or widen the configured
serving pools. Newly configured hostnames, candidates, source policies and
membership still require a new signed DNS artifact.

The source selection is checked again after observation. A changed fence or
assignment cannot install observations from an unrelated source. Persisted
source context carries verifiable immutable artifacts and monotonic selection
cursors; it is not a persisted positive readiness observation. On restart all
readiness must be probed again. Control-plane loss preserves the previously
validated source context and original checkpoint deadline, never an unobserved
publication. Unrecognized sources do not acquire authority through timestamps,
successful HTTP responses or a matching hostname alone.

During the public transition the signed DNS configuration can explicitly
authorize the retained complete global producer as a previous source in
addition to the neutral Cells. Its physical topology aliases remain exact.
Once all public Fronts use neutral Cells, remove that previous authorization,
then retire the old global publication/inventory/Lease/LKG through observed
configuration steps. This avoids depending on continued legacy DNS convergence
after public Fronts have stopped returning legacy authority proofs.

The implementation is incomplete until the following are wired and tested:

- OpenAPI source-authorization and private source-observation contracts, with
  generated backend and frontend consumers.
- Compiler and transaction validation of baseline source ownership and signed
  authorization, plus consumer capability gating.
- A private consumer API that reads only the selected DNS artifact's authorized
  scopes and rechecks exact source selection after observation.
- DNS readiness derived from verified source publications without widening the
  signed DNS record/candidate policy, with bounded offline recovery.
- An independent DNS producer for business DNS configuration changes, and
  observed private gray/full/LKG promotion.
- Regression scenarios covering route renewal, application deployment changes,
  gray/full/LKG overlap, failed source rollout, changed tenant/hostname ownership,
  source policy changes, source fencing races, restart and control-plane loss.
- Public handoff using the existing whole-node DNS behavior and live transport
  checks, followed by source authorization and legacy identity retirement.

This document describes pending work. No source delegation or public handoff is
enabled merely by the existing shadow artifact or the proof-lease bridge.
