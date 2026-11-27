# API and background execution handoff

The API remains the HTTP authentication, validation, intent submission and
read boundary. An operation accepted through its existing route remains
queryable through that route independently of which process advances it.
Immediate, read-only cluster queries and API-local metrics, caches and
connection preparation are not durable background operations.

## Execution boundaries

| Work | Target executor | Durable state or retry invariant |
| --- | --- | --- |
| Billing query preparation | API database pool | PostgreSQL prepared statements belong to a connection. Preparing the same query in another process does not warm the API. Do not remove the current bounded warmer without measuring a replacement on the API pool. |
| Edge quality rollups | Observation worker | Commit rollups and completed window watermarks together. Retry uncommitted windows on the next pass. |
| Passive ASN, DNS policy verification, observation and quality-evidence retention | Observation worker | Reuse observation cursors or bounded retention passes. The current DNS loop also reconciles serving hosted-DNS flattened records; separate those writes from observation before moving it. Preserve the quality-evidence capacity guard on insert. |
| Storage policy and lifecycle | API-local pool and codec configuration; storage worker for audit and maintenance | Reload the local policy in each consumer; separate the artifact deduplication writes currently bundled in its reload loop. Give maintenance its own bounded work progress. |
| Backup execution, retirement, inventory | Backup execution and inventory workers | Preserve atomic backup claims, coordination leases and artifact identities. Inventory remains separate from execution and does not enqueue replacement backup effects. |
| Database imports and TLS issuance | Import and certificate workers | Preserve the durable jobs and replay rules. Require explicit treatment of ambiguous SQL or CA effects before any retry. |
| App mail and other application-resource reconciliation | Controller | Advance the existing app desired state and report status on persistent app resources. |
| Platform compile/publish | Guardian | Do not bypass existing publication fences, signing or consumer selection. Move read-only capture separately where necessary. |
| Automation shadow | Observation/automation worker | Reports are evidence only; do not inherit actuator credentials. |
| Node participation | Dedicated executor after read-only comparison | Preserve target identity, lease fencing and actual consumer evidence. Both the API and scheduled Actions currently invoke the legacy apply path; migrate their authority together. |

This is the target allocation, not a claim that these tasks have moved.

## First compatibility release: Edge quality rollups

The extraction leaves the deployed API as the writer and keeps its metrics
and persisted window format. It recognizes a persisted dedicated-worker owner
under the same named advisory lock used by the current deployment. No
component in this release changes that owner.

Before setting the owner, verify that every serving API revision and its
permitted rollback target honor the gate, including pods still terminating
after a rollout. The dedicated process must prove its readiness and use the
same fence before acquiring ownership. Do not reset the persisted owner on
API rollback. For future transfers, the owner check and the batch commit
must be fenced in the same PostgreSQL write transaction; the compatibility
read under a pinned session lock alone is not proof of that invariant
across loss of the lock connection. Releasing API compatibility is not
evidence that cross-process handoff or rollback has been exercised.

After every batch, verify the exact rollout and its consumer separately:
carry an in-flight operation across API rollout, restart the selected worker
from persisted state, observe one committed outcome with an old instance
present, and roll back a compatible binary without reopening authority.
Only then delete the retired startup path and unused grants, secrets and
image tools.
