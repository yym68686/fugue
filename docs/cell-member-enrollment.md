# Established Cell member enrollment

An established Cell adds a physical member through signed execution membership,
private execution, and finally inventory activation. Authorization alone never
counts a member as healthy. The existing first-Cell bootstrap remains limited to
Cells without a previous full publication.

1. Expand the producer's signed membership in shadow, preserving the previous
   verified full and positive LKG with the transactional expansion guard.
2. Deploy an isolated Worker with a dedicated PVC, Pod identity and reader token.
   Its activation file is absent. Its only Service exposes private health. It has
   no host ports, host network or bootstrap permission ConfigMap. Check actual
   schedulable capacity before its first-install component intent is published.
3. Observe the new signed candidate, then explicitly activate the producer's
   bounded serving rollout. Existing members can compile the new group bundle;
   the private Worker can load it and provide real route/TLS proofs without
   reporting inventory. All declared members must converge before full/LKG.
4. Commit one `fugue.cell-member-enrollment/v1` declaration under
   `deploy/environments/production/cell-member-enrollment/`. The CI lane reads the
   exact verified full, positive LKG, complete member set, current control epoch,
   and the new Pod's fresh proofs. It initializes only that Worker's absent
   activation file, then verifies subsequent inventory heartbeats. It does not
   publish artifacts or alter public transport.
5. Evaluate DNS readiness and public handoff separately against their signed
   publications and actual runtime proofs. Enrollment is not a traffic cutover.

The declaration extends the exact private Worker pins used by
`scripts/bootstrap_cell_inventory.py` with:

- `full_publication`: artifact ID, content hash, release ID and numeric fencing
  token; `verification_evidence_hash` pins its positive LKG.
- `member_node_ids`: the complete sorted physical membership, including at least
  one established peer.
- `serving_epoch`: the current slot, fence sequence and minimum healthy count.
- `control`: the serving controller's Deployment, Pod UID, source/image and
  absolute group-state path. The script checks owner, process identity and fresh
  inventory before using this epoch.
- `admission_window`: explicit `not_before` and `expires_at` timestamps, at most
  fifteen minutes apart. Retries do not renew this window.

The `bootstrap_config_map` name still identifies the Worker's optional projection,
but the object must be absent throughout established member admission. The
`release_set` fields must match `full_publication`. `worker`, `observation`,
`cohort`, `origin`, `namespace`, `authority_cell_id`, and declaration `generation`
retain their strict initial-enrollment validation.

The CAS helper's optional `--initial-generation` supplies the verified current
Cell fence when initializing a new node. Omission retains generation one.
Existing files cannot be rebased or overwritten with this option. Subsequent
promotion and rollback continue monotonically from the initialized fence.

If a check fails, CI retains observation evidence and stops. A retry reconciles
an already completed initialization only when every declaration-bound identity
matches and the recorded initialization occurred inside its original window.
That completed operation can be verified read-only after its window closes;
it cannot create another activation. Each run also observes its declared
`observation.timeout_seconds` deadline. Missing proof, capacity or an expired window is never a reason to
weaken quorum, synthesize health, or modify the retained positive artifact.
