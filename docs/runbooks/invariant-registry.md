# Invariant Registry Runbook

## Purpose

The invariant registry is the single code-level inventory for Fugue platform
safety and resilience invariants. Each definition records its category, scope,
owner, evidence sources, freshness policy, unknown/stale behavior, gate policy,
automatic action contract, rollback signal, and runbook.

The registry is descriptive and binding metadata. It does not independently
execute an automatic action. Enforcement remains in the registered gate,
release guard, safety kernel, or migrated action site.

## Inspect

```bash
fugue admin invariant ls
fugue admin invariant show <invariant-id>
fugue admin invariant inventory
fugue admin gate show <gate-id>
fugue admin action-contract show <contract-id>
```

Use `--json` when comparing generated output between releases.

The inventory reports:

- platform artifact kinds and expected consumers
- gate policies and their effective modes
- registered automatic action contracts
- autonomy enable switches and kill switches
- release guard signals
- public and direct-edge synthetic probes
- per-artifact LKG freshness policy
- whether each control-loop mechanism is designed, shadow, canary, or enforced

## Failure Conditions

Treat the registry as invalid when:

- an invariant has no stable id, owner, scope, evidence source, or runbook
- a referenced gate policy or automatic action contract is absent
- a safety-kernel invariant is missing or no longer enforced
- unknown or stale evidence has no explicit behavior
- a production mechanism is reported as enforced while its action site is not
  actually migrated

Registry invalidity must block promotion of the affected control mechanism. It
must not silently enable an automatic action.

## Recovery

1. Keep the affected gate in `shadow` or `disabled`.
2. Compare `fugue admin invariant inventory --json` with the code release.
3. Restore the missing definition or binding in `internal/platformcontrol`.
4. Run registry, release-guard, OpenAPI, and CLI tests.
5. Publish through the normal control-plane release path.
6. Recheck the inventory and public synthetic probes before any gate promotion.

## Invariants

- Registry identifiers are stable and unique.
- Safety-kernel definitions cannot be weakened by configuration.
- Unknown and stale evidence never become implicit pass.
- Registry metadata does not itself mutate production state.
- Mechanism status must distinguish design, shadow, canary, and enforcement.
