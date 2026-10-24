# Runner-generated checkout metadata

Privileged imports can leave root-owned Python bytecode; privileged Git worktree
operations can similarly own the checkout's Git administrative worktree entries.
The runner cannot clean these directories during checkout.

Before checkout, each self-hosted job inspects only scripts/__pycache__ and
.git/worktrees. The audit is observational because the runner must not attempt
permission expansion or mutate a parent it does not own. Self-hosted checkouts
set `clean: false` so checkout never invokes `git clean` against privileged
generated metadata; tracked files are still reset and the requested ref is
checked out.

No sudo or permission expansion is used. The runner owner can later remove
these generated directories during normal host maintenance.

Validation requires a canonical runner-owned workspace under RUNNER_TEMP's
parent, a matching device, ordinary file types and unique file links. The entire
scan completes before checkout and is capped at 100,000 entries. Symbolic links
and unexpected ownership fail closed. No business files, Pods or cluster
configuration are read or changed. Existing job dependencies remain unchanged.

The workflow disables Python bytecode generation. Remote diagnostics importing
repository modules must also use Python -B to remain filesystem read-only.
