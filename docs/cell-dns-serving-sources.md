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

Publication admission separates historical provenance from live selection. The
embedded baseline must match its original release identity, channel, canary and
fence; active or superseded history is accepted, while failed/rolled-back
history is rejected. A previous-authority baseline must retain its verified
status. Its artifacts remain validated and signed. The current selected routing
artifacts may differ when they belong to an explicitly approved producer.

Under the same authority locks, admission reads and authenticates the bounded
current gray/full/LKG source snapshot using the runtime's source verifier. A
frozen lane, missing approved source, invalid signature or expired-only LKG
rejects admission. A failed current candidate may retain an independently valid
positive LKG. PostgreSQL takes nonblocking shared source locks and captures the
same bounded read set for the shared validator; it never scans source history.
Fresh DNS proofs, consumer capability/convergence gates and separate full LKG
verification remain mandatory. This avoids coupling every DNS publication to
the routing artifact that happened to be selected at compile time.

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

Implementation status: the DNS runtime now verifies and follows explicitly
approved sources through the private assignment-bound API. It derives a bounded
intersection plan, probes each physical endpoint once, selects a complete
publication per target, rechecks source selection after probing and applying,
and authenticates both checkpoint context and monotonic source cursors. Restart
requires fresh observations. The runtime-facts reader independently replays the
same requirements and verifies current selection. Artifacts without explicit
approvals retain their original exact-reference behavior.

The v3 staging declaration explicitly pins both neutral producer policies and
the transitional global producer. Enabling it is a configuration release; the
runtime deployment is separate. DNS record ownership, new hostnames, candidates
and source-policy changes continue to require signed DNS configuration.

The broader migration also includes:

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

The runtime and source-verification portions above are implemented. The remaining
independent DNS producer and public migration still require explicit configuration
and observed promotion; a shadow artifact alone does not enable public handoff.

The configuration approval and private source observation interfaces are now
implemented. `GET /v1/platform-state/consumers/artifacts/{artifact_id}/route-sources`
requires a scoped DNS component identity and its current expected consumer set.
It reads only approved scopes in one read-only repeatable-read transaction,
returns current gray/full lane cursors and the current verified full LKG, and
rechecks the DNS assignment before disclosure. A failed candidate is excluded
without removing an independently valid LKG. Responses are private, no-store and
bounded to 16 MiB. Their selection digest excludes observation time; a reader
must compare a second observation after collecting route proofs.

The shared verifier authenticates source policy, parent, route/TLS children and
the optional signed LKG, then checks consistency of the activation, publication
and lane bindings returned by the private API. The selection digest is a change
detector, not a signature of the mutable release ledger. A response must come
from the authenticated API; saved source context must additionally be protected
by the DNS checkpoint signature. Foreign scopes, mismatched fences, unselected
releases, invalid artifact signatures and mismatched DNS assignments are rejected
even if their selection digest is recomputed. Verification of a saved snapshot
alone does not prove current selection or create readiness.

The runtime DNS consumer advertises `dns_route_sources_v1` and implements dynamic
readiness intersection and bounded checkpoint recovery. Existing artifacts without
source approvals retain exact-reference semantics. The independent DNS
configuration producer, observed public handoff to neutral Cells and retirement
of legacy sources remain separate migration steps.

Recovery continuity for approved dynamic sources is keyed independently of the
routing publication ID. Its identity includes the selected DNS artifact ID and
digest, producer policy ID and digest, source scope and signed source policy,
and the complete physical probe requirement with only publication identities
removed. Endpoint, route behavior, ownership, state, freshness constraints and
previous-authority group/digest remain bound. Only a fresh exact-publication
match can append a success; a release advance cannot transfer authorization.

One physical observation may match one of several authorized release variants.
A mismatch against another variant of the same behavior does not erase the
matched success. With no matching valid observation, the failure barrier resets
that recovery history. Unobserved work and repeated cached proofs add no
successes. Histories remain bounded and volatile; restart begins recovery again.
The original proof validity deadlines, source selection recheck and DNS-owned
membership, quorum and recovery thresholds remain enforced independently.
