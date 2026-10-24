# Runner-generated checkout metadata

Privileged imports can leave root-owned Python bytecode; privileged Git worktree
operations can similarly own the checkout's Git administrative worktree entries.
The runner cannot clean these directories during checkout.

Before checkout, each self-hosted job inspects only scripts/__pycache__ and
.git/worktrees. If either contains root-owned entries, the runner uses its own
parent-directory permissions to move the entire generated directory into an
owner-only quarantine next to the checkout. Contents remain intact, and normal
checkout rebuilds its own generated metadata. No sudo or permission expansion
is used. Quarantined metadata is retained for inspection, outside git clean.

Validation requires a canonical runner-owned workspace under RUNNER_TEMP's
parent, a matching device, ordinary file types and unique file links. The entire
scan completes before mutation and is capped at 100,000 entries. Symbolic links
and unexpected ownership fail closed. No business files, Pods or cluster
configuration are read or changed. Existing job dependencies remain unchanged.

The workflow disables Python bytecode generation. Remote diagnostics importing
repository modules must also use Python -B to remain filesystem read-only.
