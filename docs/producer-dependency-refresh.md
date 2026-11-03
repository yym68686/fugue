# Repairing signed producer dependencies

Business route writes and deletes check active placement migrations in the store
transaction. A pinned source cannot change silently; a new legacy alias needs an
explicit signed producer constraint refresh. Unrelated neutral constraints remain
writable even when another source is broken. PostgreSQL uses bounded current
policy discovery and nonblocking shared authority locks, never history scans.

Two explicit `producer_reconfiguration.operation` values repair existing
continuous producers without resetting the serving checkpoint:

* `placement_constraints`: change generation and the signed constraint list only.
  Both topologies, input pins, membership, schedules and serving gates stay exact.
* `dns_source`: change generation and the exact global producer publication pin
  only. The new global producer must be selected, signed and continuously serving.
  DNS member and topology changes require their separate authority workflows.

Both use the exact previous policy, selected full publication and authenticated
unexpired positive LKG as transaction preconditions. A recovery full fence may
reference its original verified LKG only when the server authenticates the exact
rollback and original positive evidence. This does not verify that new fence.
Newer in-flight gray traffic and competing successors conflict without writes.

Use schema `fugue.producer-dependency-refresh/v1` in one declaration under
`deploy/environments/production/cell-producer-reconfiguration/`. It has the same
top-level fields as the cell reconfiguration declaration: schema, generation,
origin, authority_cell_id, precondition and the complete successor policy. The
existing CI lane runs `scripts.reconfigure_cell_producer`, which dispatches this
schema to `scripts.refresh_producer_dependencies`. Preview the exact successor;
never infer an approved source from health or silently follow a latest policy.

After a global producer replacement, explicitly refresh dependent DNS producer
pins and its generated source authorizations. The compiler projects the new
verified source; normal shadow/gray/full progression and fresh exact HTTPS proofs
still apply. Check source reads and consumer convergence before considering the
handoff complete. Installing this capability alone does not update configuration.
