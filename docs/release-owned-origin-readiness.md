# Release-owned origin readiness during canonical drain

A promoted independent revision can keep serving while an earlier canonical
Deployment drains a connection. During the next canary, an app-level observation
has no single serving release. Comparing the canonical Deployment to current
code can then produce `current_image_mismatch`, despite both traffic targets
being independently ready. That stale origin result used to suppress dynamic
DNS queries and stop the complete traffic producer from publishing the canary.

The compiler input capture now derives origin readiness for a release-owned route
from the effective targets of its frozen traffic policy. It first captures the
existing exact Deployment, image, Service, EndpointSlice, Pod and ReplicaSet
ownership evidence. The normal policy resolver determines effective weights and
any explicitly authorized stable fallback. An origin becomes ready only if every
effective target has fresh positive managed-workload proof in the same runtime.

Only runtime facts change. Disabled route intent and traffic-denying route policy
remain disabled; unrelated and unverified origins retain their original facts.
Missing, stale, unavailable or foreign runtime evidence cannot authorize traffic.
DNS quality evidence, signed artifact rollout and fresh edge proofs still run.
A failed capture preserves the currently serving artifact.

Regression coverage reproduces a draining canonical Deployment on older code,
independent stable and candidate revisions, and desired code advancing ahead of
stable. It checks that dynamic DNS queries and exact canary weights survive the
full business capture and compiler pipeline, while the app observer still avoids
claiming that a multi-release canary has one serving release.
