# Action Safety Evaluator Runbook

## Purpose

The Action Safety Evaluator is the common precondition checker for bounded
automatic platform actions. It evaluates a registered action contract and
returns a decision; the evaluation endpoint never performs the requested
action.

Production action sites must be migrated individually. Until migration and an
explicit promotion complete, their gates remain `shadow` or `disabled`.

## Inspect

```bash
fugue admin action-contract ls
fugue admin action-contract show <contract-id>
fugue admin gate show <gate-id>
fugue admin action-safety evaluate --file request.json
```

The request must identify the action, contract, triggering invariant, scope,
subject, evidence, sample and failure-domain counts, candidate blast radius,
TTL, rollback target when required, audit/WAL readiness, idempotency key, and
fencing token when required.

## Decision Semantics

- `shadow`: a fully safe request returns `would_action=true` and
  `production_mutation_allowed=false`.
- `canary`: production mutation is allowed only inside the configured canary
  scope and only after every contract check passes.
- `enforced`: production mutation is allowed only after every contract check
  passes.
- `disabled`, missing or false action enable switch, active action kill switch,
  unknown contract, missing/stale
  evidence, or any violated boundary: fail closed.

The global autonomy kill switch takes precedence over every gate and contract.
An action-specific enable switch must also be explicitly enabled. Enable
switches use positive `*_ENABLED` semantics; kill switches use positive
`*_KILL_SWITCH` semantics. They are separate fields and are never interpreted
in reverse.

## Mandatory Checks

- action, trigger invariant, and scope match the registered contract
- effective gate mode and canary scope are valid
- global and action kill switches permit evaluation
- every required evidence item is fresh, trusted, and passing
- minimum samples, independent failure domains, and soak window are satisfied
- TTL does not exceed the contract maximum
- rollback/compensation and recovery conditions exist
- required rollback target and human approval are present
- audit, WAL, idempotency, and fencing requirements are satisfied
- candidate state stays inside the compiled blast-radius cap

## Recovery

1. Do not bypass a failed decision by calling the underlying mutation directly.
2. Inspect each violation code and the bound invariant/gate/contract.
3. Refresh stale evidence or collect the missing independent source.
4. Reduce the candidate scope when the blast-radius cap fails.
5. Restore audit/WAL/fencing dependencies before retrying.
6. Use the action kill switch to stop the whole action class during an incident.
7. Keep the gate in shadow until a full soak window and canary verification
   complete.

## Invariants

- Evaluation is side-effect free except for an audit record of the decision.
- Shadow mode never authorizes production mutation.
- Configuration can tighten but cannot enlarge compiled blast-radius limits.
- Unknown, stale, missing, replayed, or untrusted evidence cannot authorize an
  action.
- No production action is considered migrated until its actual mutation site
  consumes the evaluator decision.
