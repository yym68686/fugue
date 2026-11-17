# Reclaim a serving release's original revision

An immutable `revision_workload` records the candidate that originally served a
release. Canonical alignment changes its current serving target, not that
provenance. The serving release must not be retired merely to reclaim redundant
candidate replicas.

The bounded background retirement worker also discovers aligned serving
releases. After the rollback/retention window it scales only the original
candidate Deployment to zero, provided all of these facts are freshly verified:

- Exclusive single-release traffic policy and unchanged application intent;
  no active application operation.
- The expected canonical Deployment, healthy complete Pod ownership chain,
  application image, canonical Service selector and exact ready endpoint set.
- Canonical publication acknowledged by the existing traffic-proof mechanism.
- Every original Pod's drain agent positively observes zero connections for
  its configured quiet period, with unchanged workload/container identities.
- Revalidation immediately before a UID/resourceVersion-conditional patch.

Unknown evidence, ongoing connections, retention, concurrent changes and patch
conflicts retain the candidate for a later bounded retry. The worker never
changes the canonical replicas, traffic policy, image, storage or release
provenance. Audit action `app.release.aligned_revision.quiesced` records success;
controller logs identify the condition preventing quiescence.

The zero-replica Deployment and Service remain as provenance/rollback metadata.
Controller restart is idempotent. If the release is later superseded, ordinary
retirement can validate the quiescence marker, converged zero replicas and no
owned Pods before its existing transactional retirement and object cleanup.
