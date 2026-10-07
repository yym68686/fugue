# Actual DNS decision receipts

Phase 1 is observation only. It does not change candidate eligibility, scores,
selection, exploration buckets, cooldown policy, DNS TTLs, releases or positive
checkpoints. A receipt is captured from the authoritative platform-serving
answer path, not by issuing a second DNS query or rerunning today's ranking.

## Read and replay

```sh
fugue admin dns decisions explain <dns-node-id> --hostname app.example.test --limit 5 > decisions.json
fugue admin dns decisions explain <dns-node-id> --decision-id <decision-id> --limit 1
fugue admin dns decisions replay decisions.json
```

`explain` requires platform-admin `artifact.read` access. The API resolves a
fresh, authenticated DNS consumer and verifies its Kubernetes Pod UID and the
Service selector before proxying the selected process. It never queries a
random Pod or falls back to recomputing rankings. Backend unavailability is a
management-query failure, not a DNS serving failure.

`replay` accepts one receipt or the exported explain response. It uses only the
recorded query, client scope classification, immutable record inputs, readiness
facts, clocks, and exploration bucket values. It needs no API connection or
credentials, and rejects mismatched evidence or outputs. Use a CLI built from
the same release when investigating historical algorithm behavior. A schema or
behavior incompatibility must fail replay rather than silently substitute
current facts. The evidence hash detects corruption; it is not a publisher
signature or proof against an administrator who can rewrite the entire receipt.

## Interpret the evidence

- `rrset`, `authority`, `rcode` are the response handed to the DNS writer.
  `write_succeeded` means writer acceptance, not confirmed delivery to a client.
- `publication.desired` is the last publication observed by the sync loop;
  `observed_at` and `desired_known` expose freshness and missing knowledge.
- `publication.loaded` identifies the captured serving checkpoint, while `lkg`
  retains the last known positive checkpoint even if live readiness fails.
  `serving_lkg` does not assert that every query will return a positive answer.
- `rejected` / `outcome` describe candidate adoption independently from loaded
  state. A serving-observation reporting failure is not candidate rejection.
- `answer_publication` identifies the artifact whose response actually won.
  This can differ from `loaded` during an already-supported per-record transition.
- Each record includes the input/materialized candidates and score breakdowns,
  scope source and resolution, selected group, exact answer candidates, materialization
  filtering, policy rejection, answer-limit omissions and exploration outcome.
- `cooldown_until` / `cooldown_result` describe the published scoped cooldown
  window at the captured clock, not a new cooldown decision. Global profiles
  carry no deadline and report `not_recorded`; absence is never called expiry.

Raw resolver IPs and ECS addresses are not retained. ECS metadata in the
additional section is redacted, and replay uses recorded classification and
entropy instead of deriving scope from an address. Public candidate addresses,
record contents, and route proof metadata remain administrator-only evidence.

## Failure isolation and bounds

The DNS handler never performs journal disk I/O, waits for a queue consumer,
calls the control plane, or waits for audit recovery. A 32-entry queue feeds a
separate worker. The worker retains at most 64 receipts, at most 256 KiB each,
in memory and in a separate `<cache-path>.decisions` directory (0600 files).
This journal is never used as serving configuration or as a positive checkpoint.
Disk errors preserve the in-memory receipt. Overflow, oversize evidence and
recovery failures are counted and cannot invalidate serving state.

Retention is intentionally bounded; an absent receipt does not prove that a
query was not answered. Export promptly. Explain returns the retained count,
retention and byte limits, dropped/evicted counts, persistence/recovery errors,
and process identity. Corresponding `fugue_dns_decision_audit_*` metrics expose
audit loss separately from DNS health. A process restart recovers valid retained
files without blocking DNS and keeps their original process/decision identities.

Before considering a routing-policy change, correlate a specific receipt with
its loaded and answer artifacts, scope source, original scores, readiness facts,
cooldown window and exploration result. Publication, current rankings, a present
DNS lookup, or a screenshot alone cannot establish why an earlier answer occurred.
