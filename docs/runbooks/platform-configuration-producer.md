# Platform configuration shadow producer

The API replicas elect one producer with the existing store advisory lock. It
uses the shared business capture and deterministic compiler, then publishes the
global shadow TrafficReleaseSet and prepares its immutable consumer topology.
It never publishes gray/full traffic, verifies LKG, or changes serving pointers.

Create a `policy_snapshot` artifact with scope
`{"scope_type":"global","key":"platform-config-producer"}` and this content:

```json
{
  "schema_version": "fugue.platform.producer/v1",
  "generation": "producer-policy-1",
  "mode": "shadow",
  "input_source": "business-migration",
  "target_scope": "global",
  "interval_seconds": 60,
  "refresh_seconds": 600
}
```

Use the normal artifact create, validate (`dry_run=false`) and shadow release
APIs. The envelope generation must equal the content generation. A draft has no
effect. Create and release a new generation with `mode=paused` to stop automatic
publication. Policy revisions are polled every 30 seconds, independently of the
previous compilation interval. Rollback uses the existing artifact rollback API
and the shadow channel. Full/gray publication of this operational policy is
rejected, including through soft override.

`interval_seconds` is the minimum delay between compilation attempts, subject to
the policy poll interval. An unchanged desired intent and policy reuse the
existing producer release until `refresh_seconds` requires a new runtime capture.
Runtime observations keep their original timestamps. Producer metadata is only
provenance: the release must also have been published by the producer identity
before it is eligible for reuse. A new owner or policy generation cannot be
silently treated as a successful prior run.

The publication transaction verifies the current signed policy, policy lane,
target predecessor, signed parent and three children, and their lineage. A
concurrent pause/freeze or manual target change rejects the queued publication.
The next cycle reads current state again. Pause the producer before making a
manual shadow publication that should remain selected; an enabled producer
continues reconciling its declared target. Partial artifact writes have no serving
authority. A retry reuses immutable identities, and an interrupted consumer
preparation is completed for the already published release.

The exact normalized runtime snapshot is saved in the existing content store.
Read it through `GET /v1/admin/artifacts/{artifact_id}/compiler-input`, which checks
the signed artifact, digest and generation binding. Combine it with the stored
intent/policy artifacts to call `compile-from-artifacts`. Replaying a producer
artifact preserves its source binding and original creator. It does not publish
another release. Missing or corrupt input returns an error and never substitutes
current business state. Older artifacts may lack a retained snapshot until their
original fixed input has been replayed through the compiler.

Watch the `platform configuration producer published`, `unchanged`, and failure
logs, plus the normal release/expected-consumer/convergence APIs. Shadow consumer
reports remain non-serving; observed consumers alone do not authorize promotion.

`business-migration` deliberately uses the current migration capture, including
legacy serving inputs. This stage maintains the business-to-artifact path while
serving migration is unfinished. Durable intent/policy ownership, output
equivalence, real gray/full/rollback and removal of legacy sources must still be
completed before declaring the architecture migration finished.
