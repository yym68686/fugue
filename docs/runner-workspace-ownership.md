# Runner-generated checkout metadata

Privileged imports can leave root-owned Python bytecode; privileged Git worktree
operations can similarly own the checkout's Git administrative worktree entries.
The runner cannot clean these directories during checkout, so a successful
configuration job can be followed by checkout failures in unrelated jobs.

Each self-hosted job independently restores ownership of these two generated
metadata trees before checkout. It validates the workspace against RUNNER_TEMP,
the runner UID, canonical paths, mount device, ordinary file types and link
counts, then changes only root-owned entries. It never follows links, deletes
files, traverses business configuration or accesses cluster resources. The scan
is capped at 100,000 entries and validates the entire scan before any changes.
Existing configuration/build dependencies and production release fences stay
unchanged; recovery does not require a checkout, API or code build.

The workflow disables Python bytecode generation. Remote read-only diagnostics
that import repository modules must also use Python -B; reading a module without
it is not necessarily filesystem read-only.
