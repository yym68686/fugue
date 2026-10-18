# DNS continuity during a stable application replacement

A DNS consumer can observe two successive single-release application policies
without observing an intermediate canary. Once Edge applies the new route,
the retained DNS plan rejects its changed route digest. If another hostname
still needs time to converge, the entire successor DNS bundle cannot yet be
accepted. Previously the stable-ID comparison also rejected per-record forward
recovery, leaving the ready application with SERVFAIL.

Forward recovery now recognizes a sole stable target replaced by another when
tenant, application, policy identity and every other traffic-policy field stay
identical. Both policies must explicitly assign 100 percent to one nonempty
stable release, with no candidate configured. This does not change canary
admission, permit reviving a zero-weight candidate, or authorize a changed owner,
fallback, stickiness, endpoint, DNS selector, quorum or evidence lifetime.

Only comparison copies are normalized. The successor remains a separately
signed current assignment, and every answer requires a complete fresh quorum
bound to its original route digest, release, parent and fence. New proofs are
never relabeled as old proofs. The current assignment is rechecked after probes;
partial recovery does not write a positive checkpoint, acknowledge whole-release
success, or replace the previous LKG. Missing or expired proof still fails closed.

The full sync regression reproduces the old SERVFAIL, including a worker changing
between the candidate and retained-plan probes. It verifies live UDP/TCP answers,
decision replay, wrong proofs, changed assignments, immutable checkpoints,
control-plane loss, restart and eventual ordinary whole-release acceptance.
Independent policy-boundary tests also check both versions remain unmodified.
