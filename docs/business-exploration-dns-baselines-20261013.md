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
