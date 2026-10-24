# Runner-generated checkout metadata

Privileged imports can leave root-owned Python bytecode; privileged Git worktree
operations can similarly own the checkout's Git administrative worktree entries.
The runner cannot clean these directories during checkout.

Before checkout, each self-hosted job audits only scripts/__pycache__ and
.git/worktrees. When root-owned
entries are present, the runner renames the entire stale checkout into a unique
quarantine sibling under its runner-owned parent. It does not mutate or traverse
outside those bounded metadata trees, and never follows links. Checkout then
creates a fresh workspace. Self-hosted checkouts also set `clean: false` so a
normal reuse cannot invoke `git clean` against privileged generated metadata.

No sudo or permission expansion is used. Quarantined workspaces remain available
for host maintenance and are outside the active checkout path.

Validation requires a canonical runner-owned workspace under RUNNER_TEMP's
parent, a matching device, ordinary file types and unique file links. The entire
scan completes before any rename and is capped at 100,000 entries. Symbolic links
and unexpected ownership fail closed. No business files, Pods or cluster
configuration are read or changed. Existing job dependencies remain unchanged.

The workflow disables Python bytecode generation. Remote diagnostics importing
repository modules must also use Python -B to remain filesystem read-only.
