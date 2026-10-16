# Release Guard Blocked Runbook

## Trigger

Use this when `fugue admin release guard status` reports
`block_rollout=true` or the deploy workflow logs a release guard warning or
failure.

## Read-Only Diagnosis

```bash
fugue admin release guard status --json
fugue admin robustness status --json
fugue admin artifact ls --json
```

Classify the blocker as robustness baseline, platform artifact validation,
consumer generation drift, LKG expiry, request/traffic safety regression, or
gate policy violation.

Check `blocked_reasons`, `gate_policy_violations`, artifact validation failures,
and platform consumer drift. If the block comes from a tenant workload, confirm
whether that workload is an explicit platform release signal.

## Safety Rules

- Do not continue a normal rollout while a `block_publish` incident is active.
- Force publish requires an explicit reason and audit trail.
- Prefer rollback, gate demotion, or scoped exclusion over manual live edits.
- Unsafe new gates must be demoted to `shadow` or `disabled` before release
  expansion continues.

## Recovery

1. Fix invalid artifact source or rollback to the last validated generation.
2. If consumer drift blocks rollout, follow
   `docs/runbooks/consumer-generation-drift.md`.
3. If LKG expired blocks rollout, follow `docs/runbooks/lkg-expired.md`.
4. Demote unsafe new gates to `shadow`:
   `fugue admin gate promote <gate-id> --mode shadow --reason <reason>`.
5. Use the declarative GitHub Actions and Guardian recovery path. Bind any
   recovery intent to the exact failed target and verified successful LKG.

## Configuration changes during an Edge code rollout

A new configuration generation may retire an inactive Worker candidate while
the code rollout is still waiting for it. The executor can restage at most four
times when Edge Control reports a newer healthy publication, an empty candidate
slot, and the same recovery epoch. The Front activation and active Worker code
identity must remain unchanged, with fresh inventory evidence. Each new stage
gets a new signed candidate and must pass the normal independent canary.

The executor must first observe that Guardian has neither a prepared nor an
activated transition journal. An existing or unreadable journal means the code
transaction may still be committing, so observation continues. After journal
absence, it re-reads CurrentAuthority before declaring the candidate retired.
This prevents configuration progress from starting workload compensation while
Guardian is still committing the exact candidate.

A staging CAS conflict against a healthy current configuration refreshes the
Worker/Front snapshot before retrying. The new snapshot must preserve the
active code grant and node cohort, contain fresh healthy facts, and not regress
the loaded publication sequence. The signed staging request then binds the
observed current Worker configuration. This path does not renew the LKG or
increment its recovery epoch; the existing four-attempt bound still applies.

An already committed CurrentAuthority is a separate outcome. The executor
requires the exact staged record, Worker source/image, slot and original bundle
generation, plus a complete ready Worker cohort with fresh active inventory.
A later verified configuration publication is allowed without requiring the
old candidate headers to remain present. Front and public-route verification
still run before the release is complete.

After Front CAS, a missing candidate header requires a fresh, independent
code/image/Front-generation witness for each successful public-route sample.
The expected response body must still match. Contradictory or partial headers
cannot use this fallback, and the observed headers are never synthesized.

An absent candidate, failed status request, changed recovery epoch, changed
traffic activation or unknown error alone does not authorize restaging. These
conditions remain blocked; preserve their failed record for explicit recovery.

## Verification

- `release guard status` reports `pass=true`.
- `blocked_reasons` is empty.
- `gate_policy_violations` is empty.
- Robustness status has no platform `block_publish` incident.
- Public synthetic probes do not return Fugue routing 503 classes.
- Deploy logs include the pre-deploy and post-deploy release guard summary.

## Preparing an ordinary Worker publication before Front activation

Worker code promotion prepares the signed current publication before changing
Front's active slot. Guardian first persists a separate, 90-second preparation
record using the same local activation lock. The record binds the complete
predecessor activation, target slot, source revision, image digest and promoted
publication version. It never changes Front activation or creates configuration.
Only the named inactive Worker may use it to fetch the ordinary signed stream.
Signature, group, publication sequence, recovery epoch and expiry checks remain
mandatory; the record is rechecked before accepting the fetched bundle.

Guardian requires every exact target Pod to load an ordinary publication and
apply the same version to Caddy before it attempts Front CAS. Missing capability,
partial cohort, invalid signatures, timeout or expired preparation preserve
the old traffic pointer. CAS rechecks the same unexpired preparation under its
file lock. Replays do not extend its deadline, and any predecessor change fences
the record automatically. Rollback remains independent of preparation.

Release Guardian before the supporting Worker code. An older Worker CAS binary
rejects the preparation flag without activating traffic; do not bypass that
capability failure. This ordering avoids the prior interval in which HTTP traffic
reached a candidate bundle that was correctly unable to issue ordinary DNS route
proofs. Candidate proof rejection and DNS eligibility policy remain unchanged.

## Retained DNS observations during assignment churn

A changed assignment rejects the candidate publication and its transition
bridge. It does not invalidate a newly collected physical proof that exactly
matches the retained signed release and the identical probe requirement. DNS
keeps those fresh observations in memory while retrying authority at most three
times. Foreign-release, changed-endpoint, negative, duplicate, expired, future
and older observations cannot replace the retained proof.

This refresh does not persist a checkpoint, acknowledge the rejected assignment,
advance the completed-scan clock, renew a pending transition, or extend the
retained publication/source deadline. Original probe timestamps remain intact
and their validity is capped by the retained policy. Offline proofs still expire
normally. Roll out DNS code to an unselected standby, verify all readiness probes
and UDP/TCP answers, then use the separate signed transport intent for handoff.

## Comparing application latency with different request mixes

Comparative canary latency uses the same hostname, redacted path without query
string, HTTP method and streaming mode on both releases in one query window.
Every compared cohort must meet the existing minimum request count on both
sides. The P95 TTFB and non-streaming P99 thresholds are unchanged. A streaming
response remains subject to TTFB but is not treated as an ordinary duration.

Global absolute latency, error and status-class checks still include all
requests. Unmatched and undersampled requests are counted in gate warnings;
they must not be compared to a different operation. No sufficiently sampled
matching cohort fails the gate. A deficit in total or matched samples is
inconclusive when another bounded canary step remains. For example, 25% may
advance to 50% to collect samples; 50% cannot advance to 100% without sufficient
matched evidence. Errors, latency regressions, missing stable metrics and query
failures are never classified as a sample deficit.

The query is bounded to 512 cohorts, and its full row count detects partial
backend results. Missing identities, truncation or backend errors fail closed.
Use the gate's `latency_comparison` evidence to distinguish a same-operation
regression from a shifted mix of health checks, ordinary calls and long streams.
