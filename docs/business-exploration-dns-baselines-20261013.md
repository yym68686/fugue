# Node-local safe baselines for business exploration

## Exact serving evidence

US worker intent 210, commit `980535fa4986d87691fea485abe530ef1b9549d4`,
completed through CI `38353149603`. At 06:07 UTC the US CurrentAuthority
selected slot B and image
`sha256:613fb924ddfe77b20721798cc1507fb4714ddbdd193accb5a221efcb41d8e2aa`.
Both exact worker Pods were Ready with zero restarts; the previous slot A
authority remained retained. The earlier invalid HTTP response has not been
attributed: later success does not prove its transport cause.

The read-only preparation at 06:07:50-51 UTC did not activate exploration.
Both DNS receipts referred to the same signed DNS child
`sha256:da19d991fdbbcd49867e56f982ffda81a77f3f169f4bf13ccffb2e26b296f7fa`,
parent `artifact_1791871268_ceec55f0f6e0`, fence 3426. However:

- US decision `ed1139b354eb4b4597e681d8be493856-52150` retained vmarck as
  its declared physical primary.
- Germany decision `38d738d6f8026616b837913bd94f980e-46858` declared US OVH
  as its physical primary. This was not merely a random fallback answer.

At 06:26:07 UTC both choices remained in verified parent
`artifact_1791872401_ef34c033ce55`. The passive quality comparisons held both
existing choices without claiming sustained advantage. This proves the
single global baseline cannot preserve these two serving assignments; it does
not establish that either earlier dynamic choice was a quality regression.

## Bounded correction

An initial DNS publication anchor may explicitly declare its own baseline edge.
The API and read-only CLI resolve that node's baseline, require exact replayed
serving equality, and carry the resolved policy into the signed allocation.
Compiler and artifact validation bind it back to the exact DNS node. Missing
anchors, undeclared candidates and cross-node substitution are rejected.
Same-intent renewal retains the actual node's stopped state and cooldown;
neither a new receipt nor another node can silently reset them.

All required route, TLS and DNS consumers must advertise the additional
`business_dns_baseline_v1` capability before such configuration is admitted.
Old configurations without the optional field retain their old behavior.
This code release activates no exploration. DNS D candidates are isolated;
public C Services remain unchanged until a separately verified handoff.

The prospective observe declaration uses 1% total non-baseline DNS allocation,
three complete five-minute windows and at least three independent qualified
connections per cohort per window. Ten connections per five-minute cohort
would be incompatible with the existing one-minute sampler suppression;
follow-up snapshots must not inflate connection counts. Missing qualifying
evidence means hold, not a synthetic comparison or a lowered quality claim.

## Validation

Full `make test` passed after correcting the new test's expected success case.
The focused tests cover different baselines, replay, sticky stops, undeclared
baselines and missing capability for each of route/TLS/DNS consumers.
Frontend snapshot and generated types are synchronized. Typecheck still reports
147 pre-existing errors; this change does not claim the frontend build passes.
Neither controlled exploration nor adaptive mode has been activated yet.

## Isolated verification prerequisite

CI `38360741561`, job `115175417078`, failed at 07:13:26 UTC while creating
the isolated SSH transport fixture. The API Server explicitly rejected both
Pods because `transport-fixture/default` did not exist. The script submitted
a new Namespace and its Pods together and relied on the namespace controller
to create the default ServiceAccount asynchronously. No production SSH
deployment was selected by this failure.

The fixture now creates its own token-free ServiceAccount before its two
Pods and explicitly binds both Pods to it. The regression check verifies
creation order and account binding without depending on controller timing.
Transport preservation and compensation assertions remain unchanged. The real
isolated test still has to pass through normal CI; a unit test alone does not
prove its network behavior.

API 747 and both workers (US 211, DE 385) completed their deployment jobs in
CI `38360741561`; its overall failure is the isolated fixture issue above.
The candidate DNS D Pods are Ready at zero restarts. The US D UID is
`9a00624a-c1dc-48d7-8505-e5266956ab7c`, image
`sha256:6e1a40288c1806c7ffa81e42a90685206fcaec49ffce29a4e9b449b8bcf6ffa8`.
At 07:42:55 UTC the US old/candidate runtime snapshots passed existing strict
handoff validation on `artifact_1791876939_cee1d7771ff0`. Germany's snapshot
was not admissible in that capture and is not authorized by this observation.
Transport generation 37 selects only US D; all original CI preflight, CAS,
signed proof and public query checks remain required. The German public
selector remains C until an independent successful verification.

07:13-07:18 UTC: the bounded DNS collector retained 11 samples, no collection
errors and no SERVFAIL increase. Historical cumulative totals were US 16 and
DE 9, unchanged from 05:44-05:45; these totals have not been attributed here.
Native SSH from this workstation at 07:22 UTC timed out during banner exchange
for both aliases; the trust file hash was unchanged. This is an unsuccessful
client acceptance attempt, not evidence of a DNS or SSH regression cause.
