# Business evidence renewal and retained DNS proofs

The Germany DNS D receipt `d77252342aa20b135e5a9a7cac516d6e-51936`,
query 63712 at 2026-10-13 19:18:37.076602730 UTC, returned SERVFAIL.
Offline replay matches its original answer and readiness rejection. Both
declared edges returned fresh route proofs for the new gray publication while
DNS retained the previous full publication. Their route digest was unchanged.
Both facts were rejected as `traffic_release_mismatch`.

The two exact DNS artifacts were retained privately. For this hostname, the
query record differences were only the comparison receipt digest, evaluation
time, and physical selection's capture time, DNS receipt ID, evidence digest,
loaded digest and parent policy digest. The service policy, candidate set,
weights, route plan and per-record constraints were unchanged. The bridge
compared these observation references as authority and rejected the record.
The private artifact regression confirms compatibility after normalizing only
the observation fields. The unchanged historical failure still replays as a
failure; its receipt is not rewritten.

The bridge now compares a cloned selection with observation references and
allocation evidence renewal metadata removed. It retains exact policy intent,
scope, candidate order, primary, allocation weights, stopped candidates,
executor binding, migration, exploration and quality budgets. Original serving
objects and their evidence deadlines are never modified: a renewed successor
does not renew the old allocation. Signed successor and exact route-proof
checks, endpoint identity, tenant, quorum and hard policy checks still apply.

Synthetic tests reject changed weights, intent, lease, candidate set, stopped
state, scope, primary, ordering and executor. They verify that expired original
allocations still fall back at their original deadline and that comparison does
not mutate either retained authorization or evidence. Both isolated DNS E
instances receive this fix through the normal release lane before public
transport handoff.

CI 38451410183 also recorded Germany SERVFAIL for queries 21901 and 35916
at 19:04:56 and 19:04:57 UTC. Their detailed receipts were already evicted when
inspected, so the proven cause above is not assigned to those earlier queries.

CI 38453583498 stopped before API mutation because the declared predecessor
image digest did not match Guardian. The successful prior CI artifact receipt
and live Deployment agree on `sha256:2fde89a385a7c1fea94bd0a59decfe4ca6563848823a27cd4655ffade4c9df18`.
The retry corrects that declaration and increases its generation. It preserves
the failed attempt as audit history and does not bypass predecessor checking.
