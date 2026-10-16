# DNS continuity after a candidate wire check fails

A complete route quorum does not necessarily produce an authorized DNS answer.
The sole ready endpoint can be excluded by a record's signed selection policy.
Previously this case failed the whole snapshot wire check, then refreshed only
the retained publication. If another hostname's Edge had already switched to
the successor route digest, its usable successor proofs were discarded and that
healthy hostname temporarily returned SERVFAIL.

Snapshot wire failures now enter the existing incomplete-candidate path. It
rechecks the serving assignment, preserves the positive checkpoint and creates
only the already supported volatile per-record transition. Each answer still
requires compatible signed authority and a complete, fresh quorum belonging to
one exact successor publication. No selection rule, proof lifetime, or quorum
threshold changes. A wire-incomplete candidate cannot emit a successful apply
ACK or overwrite LKG. Public errors continue to omit record identities.

The regression uses two signed hostnames and two endpoints. The candidate has
quorum for both, but one hostname's permitted endpoint still serves the old
publication. UDP/TCP answers for the independently healthy hostname must remain
available, the other hostname retains its old authorized answer, and receipts
must replay exactly. The test also covers assignment changes, proof expiration,
unchanged persisted LKG, and normal activation once all endpoints converge.
