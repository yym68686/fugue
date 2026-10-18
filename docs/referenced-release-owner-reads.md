# Read endpoint owners without historical ReplicaSet inventories

Release readiness captures EndpointSlices and Pods, then validates each endpoint
through its Pod, ReplicaSet and Deployment UID chain. Previously, every capture
also downloaded all ReplicaSets, including unrelated rollout history. A live
read found 1,522 ReplicaSets and 20.7 MB of decoded JSON; only 167 were referenced
by current endpoint Pods.

The ownership capture now derives unique namespace/name references from the
captured endpoint and Pod identities and reads those ReplicaSets with at most
four concurrent requests. It includes negative endpoints: their invalid owner
chain must still make readiness fail. It does not filter out an unready endpoint
or infer authority from labels. Historical ReplicaSets are not deleted.

The existing evaluator continues to verify Pod and owner UIDs, controller kind,
namespace, labels, release key, executable image and readiness. An owner replaced
under the same name is rejected by its UID. A missing object remains absent,
matching a list that no longer contains it. Authorization, transport, decoding
and other server failures abort the capture without publishing partial maps.
The cluster identity is still checked before and after capture. No cross-call
cache, expiry renewal, serving configuration or LKG change is introduced.

The read cost now depends on endpoint owner count rather than historical
ReplicaSet count. More small requests replace one large list. In a bounded
read-only measurement, 167 GETs with four workers took 0.16 seconds versus
0.96 seconds for the full list, and decoded 2.46 MB versus 20.7 MB. Both transferred
about 2.4 MB because the list was compressed and the small GETs were not. These
are different requests under natural load, not a production latency guarantee.

Regression tests compare exact readiness output, including missing/replaced
owners, invalid Pod UIDs, conflicting ownership, negative endpoints, pagination,
deduplication, cancellation and failed reads. A concurrent test enforces the
four-request limit. The synthetic benchmark measures decoded bytes and request
counts separately from allocation and elapsed time.
