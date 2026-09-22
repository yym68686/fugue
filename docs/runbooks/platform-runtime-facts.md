# Release and consumer facts

Use `GET /v1/admin/platform-state/runtime-facts` with a platform administrator
credential that has `artifact.read`. The response contains original audit events
and the query time. It does not grant promotion or recovery authority.

Filter with `release_set_id`, `artifact_kind`, and/or `consumer_id`. Filters run
before `limit` (default 200, range 1–1000), so unrelated recent events cannot hide
older matching facts. Results use descending creation time, then descending ID.
An empty result means no retained matching events; it does not prove convergence.

Producer shadow, gray, full, verified-LKG and rollback events are included with
existing artifact and accepted heartbeat events. A ReleaseSet filter recognizes
its typed audit target or explicit artifact reference. Rollback events can be
found using either the rejected parent or their explicit recovery LKG reference.

Consumer queries accept canonical identities such as `edge-worker:<node>` or the
historical stored instance ID. New signed heartbeat events carry `consumer_id`;
older events use the retained consumer instance mapping. Queries never add fields
to, re-sign, or rewrite the historical event. The response retains chain sequence,
hashes and provenance. Ordinary unsigned producer audit events remain unsigned;
they cannot substitute for authenticated consumer evidence.

For current readiness, also inspect the exact publication's expected consumer
sets and `/v1/admin/platform-state/convergence`. Historical success does not
replace fresh evidence for the current release and fence.
