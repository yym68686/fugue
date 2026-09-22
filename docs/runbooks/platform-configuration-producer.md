# Platform configuration producer

The API replicas elect one producer with the existing store advisory lock. It
uses the shared business capture and deterministic compiler, then publishes the
global shadow TrafficReleaseSet and prepares its immutable consumer topology.
Shadow mode never publishes gray/full traffic or verifies LKG. Serving mode uses
the same producer with complete pinned configuration and bounded promotion gates.
The retired business-migration source is rejected in every mode.

Create a `policy_snapshot` artifact with scope
`{"scope_type":"global","key":"platform-config-producer"}` and this content:

```json
{
  "schema_version": "fugue.platform.producer/v1",
  "generation": "producer-policy-1",
  "mode": "shadow",
  "input_source": "business-static-intent",
  "static_intent_artifact_id": "<exact-validated-intent-id>",
  "static_intent_digest": "sha256:<content-hash>",
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

Every producer starts from an exact validated signed PlatformIntent. Missing,
invalid or revoked references reject capture without falling back to the process
environment. Historical policies and compiler inputs remain readable; retired
policy sources cannot be validated or reactivated.

## Pin static platform input

Import the two static environment inputs once with the existing
`POST /v1/admin/platform-config/import-env` API. Keep its returned exact
`platform_intent` ID and `content_hash`, migration actor and source digest.
Alternatively create and validate an equivalent static PlatformIntent through
the artifact APIs. The static subset supports plain platform routes and DNS
records; dynamic app bindings, per-path routing, cache/TLS/consumer declarations
and other unrepresentable fields are rejected rather than silently dropped.
Disabled routes and per-value DNS expiration are preserved.

Create a new producer policy generation using:

```json
{
  "input_source": "business-static-intent",
  "static_intent_artifact_id": "<exact-artifact-id>",
  "static_intent_digest": "sha256:<content-hash>"
}
```

Retain the other required policy fields from the preceding example. Preview
the explicit source with `GET /v1/admin/platform-config/routes/project?producer_policy_artifact_id=<policy-id>`
and compare desired output before activating its shadow policy release. A missing
policy reference returns 400; the retired static-only selector returns 410. This
mode never reads the API's ambient platform-route or static-DNS arrays. It
continues projecting App/Domain changes from one consistent business snapshot.
An invalid, missing, wrong-scope, altered or revoked explicit source rejects the
run without falling back to environment. The store revalidates the source at
publication while holding the normal transaction locks.

The retained compiler input records the source ID/digest, and the signed parent
artifact exposes `producer_static_intent_id` and `producer_static_intent_digest`.
An operator replay retains both references. Editing static configuration means
creating another PlatformIntent and publishing another producer policy that
pins it; changing the environment or restarting code cannot replace that input.

This stage removes only the two static inputs from the producer. Authority,
base-domain/default policy and selection capture still follow their existing
migration paths. Legacy serving paths remain until their traffic cutover and
recovery checks are complete.

## Pin DNS declarations and policy

Create a new version of the static PlatformIntent that also declares
`dns_consumers` (physical node ID, group, base zones, probe label and TTL).
Create a separate validated global `policy_snapshot` containing only
`schema_version`, `generation`, `scope`, `dns_authorities`,
`dns_client_policies`, `dns_readiness`, `tls_readiness`, and
`traffic_rollout_cohorts`. Authority and client policy must completely cover the
declared consumers and zones. Extra fields are rejected by this input adapter.

Add `dns_policy_artifact_id` and `dns_policy_digest` to the producer policy,
alongside the new static intent reference. The DNS reference is optional only
for the older static-only migration mode; declarations with no pinned DNS policy
fail closed. Each reader and the publication transaction verify both input
signatures, exact identities, digests, validation status and ownership.

To preserve automatic hosted-zone onboarding, set `hosted_zone_templates` to
one `{ "node_id": "<consumer>", "template_zone": "<base-zone>" }` per consumer.
The named base-zone authority supplies NS/SOA parameters for active business
hosted zones. Empty templates means only explicitly declared base zones. Deleted
or suspended business zones disappear unless they are independently declared
base zones. Old heartbeat aliases cannot recreate them. Runtime inventory supplies
endpoint addresses; it cannot add desired consumers, change ownership, or invent
authority configuration. Missing or conflicting endpoint facts reject capture.

Preview the complete configuration through
`routes/project?producer_policy_artifact_id=<exact-validated-policy-id>` before
publishing the producer policy. The policy reference is required; static-only previews are retired. Pinned DNS capture never queries DNS DaemonSet environment. The signed
parent and retained runtime input also record the DNS policy reference so an
operator replay preserves it. Address collection does not overwrite the pinned
readiness intervals, freshness, concurrency or rollout cohorts.

Base-domain/default application policy and query selection capture still need
their remaining migration. This step does not activate serving or grant LKG.

## Retired migration comparisons

`GET /v1/admin/platform-config/routes/compare` and
`GET /v1/admin/platform-config/dns/compare` return 410 to authorized platform
administrators. Their legacy business/standalone-bundle comparators and the old
DNS compiler are retained only as test references. They are no longer production
configuration or verification paths.

Use `/v1/dns/delegation/preflight` and `/v1/admin/robustness/status` for current
signed TrafficReleaseSet diagnostics. Their evidence binds the selected parent,
all three member digests and the actual publication's authenticated consumers.
Use artifact/hostname lineage and Runtime Facts for provenance and history.
An old migration-equivalence result cannot prove current serving health.
