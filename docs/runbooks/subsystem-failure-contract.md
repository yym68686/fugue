# Subsystem Failure Contract Runbook

## Trigger

Use this when an incident references a subsystem failure contract or when a new
subsystem is added to the control plane, data plane, or runtime.

## Diagnosis

```bash
fugue admin failure-contract ls --json
fugue admin failure-contract show <subsystem> --json
```

Confirm owner, failure modes, detection signals, quarantine action, repair or
rollback action, fallback behavior, attribution classes, metrics, and runbook.

## Safety Rules

- Every guardian, traffic safety check, request attribution class, alert, and
  operator runbook must map to a subsystem contract.
- Missing metrics are degraded signals; they must not be interpreted as healthy.
- Contract changes must be released as platform state artifacts when they affect
  traffic or repair behavior.

## Verification

- `robustness status` lists the contract source.
- Alerts and request explain include the subsystem name.
- Runbook links resolve.
