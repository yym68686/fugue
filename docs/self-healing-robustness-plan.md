# Fugue Self-Healing Robustness Plan

This document describes a systematic plan for improving Fugue's self-healing
ability. The goal is not to assume bugs can be eliminated. The goal is to make
incorrect state hard to publish, easy to detect, and automatically recoverable
when the recovery action is safe and reversible.

All implementation that affects the control plane, edge proxy, DNS, Caddy,
Ingress, cluster bootstrap, runtime routing, or platform-level traffic rules
must go through the normal repository release path: commit to `main`, then let
`.github/workflows/deploy-control-plane.yml` update the remote control plane.
Manual SSH changes are reserved for explicit emergency recovery and must be
backported into this repository before the incident is considered closed.

## 1. Problem Statement

Fugue currently has several subsystems that can compute and publish desired
state, but the system is still too dependent on humans noticing when published
state drifts from policy intent.

The recent shared custom-domain target DNS incident showed the failure mode:

- route policy was correct
- the generated DNS answer set violated that route policy
- the invariant suite did not cover the exact DNS record kind involved
- public traffic could therefore be sent to an edge that was intentionally
  excluded for that hostname

The important lesson is not tied to one hostname. The generic lesson is:

```text
When route policy, DNS answers, edge route bundles, and TLS readiness describe
the same public hostname, they must be validated as one publishable unit.
```

If any generated artifact violates that unit-level invariant, Fugue should not
publish the artifact. It should keep serving the last known good state, explain
why the new state was rejected, and retry reconciliation after the underlying
input changes.

## 2. Desired Operating Model

Fugue should move from command execution to continuous reconciliation.

The target model is:

```text
observe current state
  -> compute desired state
  -> validate hard invariants
  -> publish through staged rollout
  -> probe the published state
  -> auto-repair reversible drift
  -> fall back to last known good when repair is unsafe
  -> expose diagnosis and operator action
```

Important properties:

- Bugs and partial outages are expected.
- Broken generated state must fail closed before publication.
- Broken live state must trigger automatic quarantine, rollback, or LKG
  serving when those actions are safe.
- Diagnostics must state the exact mismatch, not just "unhealthy".
- Operators must be able to see what the system tried, what it refused to do,
  and what manual action remains.

## 3. Core Principles

### 3.1 Invariants Before Automation

Automatic recovery without strong invariants can make outages worse. Every
self-healing action must be guarded by an invariant that defines a safe target
state.

Examples:

- DNS may only answer edge IPs that are route-ready for the queried hostname.
- A route bundle may only publish a route if its upstream can be resolved to a
  valid runtime service, mesh endpoint, or explicit fallback.
- Caddy may only serve a hostname when the route metadata and TLS policy agree.
- A backup run may only proceed when its backend is reachable and credentials
  can be loaded.
- A failover may only proceed after the current lease/fence state is known.

### 3.2 Fail Closed, Serve LKG

When a new artifact fails validation, the safe default is:

- do not publish the new artifact
- keep serving the last known good artifact
- mark the subsystem degraded
- emit a diagnosis event with the rejected diff

This applies to DNS bundles, route bundles, Caddy configs, edge discovery
bundles, mesh recovery manifests, and generated install/update manifests.

### 3.3 Reversible Automation First

The first class of self-healing actions should be reversible:

- quarantine one edge for one hostname
- roll back one bundle generation
- reissue a DNS bundle from existing control-plane state
- ask one node to resync
- restart an unhealthy worker when no state mutation is in progress
- switch traffic weight back to the last stable release

Risky actions require stronger guards or manual approval:

- deleting user data
- promoting a database primary
- changing persistent storage topology
- rotating production credentials
- performing cross-region stateful failover

### 3.4 Diagnosis Is a Product Surface

If a subsystem has an invariant, it needs a diagnostic view that uses the same
logic. Operators should not have to mentally join route policy, edge inventory,
DNS answers, Caddy TLS state, and public probe output.

For every check, expose:

- expected state
- observed state
- invariant result
- evidence
- automatic action taken or refused
- next manual action when automation is unsafe

## 4. Target Self-Healing Architecture

### 4.1 Guardian Layer

Add a control-plane guardian layer that runs continuously and also gates
publication. It should not replace existing controllers. It should coordinate
cross-subsystem correctness.

Initial guardians:

- `route-dns-guardian`
- `edge-tls-guardian`
- `bundle-rollout-guardian`
- `node-health-guardian`
- `backup-restore-guardian`
- `runtime-continuity-guardian`

Each guardian owns:

- input inventory
- expected state derivation
- invariant validation
- safe repair actions
- LKG fallback behavior
- diagnosis events

### 4.2 Artifact Lifecycle

Every generated artifact should follow this lifecycle:

```text
draft
  -> validated
  -> staged
  -> probed
  -> active
  -> superseded
```

Rejected artifacts should be persisted with:

- artifact kind
- generation
- validation failure
- input fingerprints
- diff against active generation
- whether LKG remained active

This makes "why did Fugue not change state?" answerable from the CLI and API.

### 4.3 Last Known Good Registry

Create a generic LKG registry for generated artifacts.

Artifact kinds:

- edge route bundle
- DNS bundle
- Caddy route config
- edge discovery bundle
- node updater desired state
- mesh recovery directory
- backup schedule manifest
- runtime failover plan

LKG rules:

- only validated and probed artifacts can become LKG
- LKG must include enough provenance to be audited
- nodes can continue serving LKG when control plane is down
- LKG use must be visible in status and metrics

## 5. Control Plane Robustness

### 5.1 Publish Gates

Control-plane publication should have hard gates:

- schema validation
- invariant validation
- compatibility validation against current node versions
- shadow generation
- single-node staging where possible
- post-publish probe

If any gate fails, the publish should stop before global rollout.

### 5.2 Post-Deploy Health Gates

The GitHub Actions deployment path should not end at "Kubernetes rollout
completed". It should also verify:

- `/readyz`
- control-plane store readiness
- edge route bundle generation
- DNS bundle generation
- DNS invariant pass
- node inventory freshness
- synthetic probes for key platform routes

The workflow should fail if the new control plane cannot generate valid state.
It should not require manual browsing to discover that a bad release is active.

### 5.3 Control-Plane Reconciliation

Add a periodic reconciliation loop for platform state:

- derive expected routes, DNS records, edge policies, and TLS readiness
- compare against node-reported active state
- write drift records
- trigger safe repair where possible
- keep retrying until desired and observed state converge

The reconciler must be idempotent and must avoid project-specific heuristics.

## 6. Route / DNS / Edge Self-Healing

### 6.1 Required Invariants

For every public hostname `H`:

```text
dns_answers(H) ⊆ route_ready_edges(H)
route_ready_edges(H) ⊆ tls_ready_or_tls_preparable_edges(H)
published_edges(H) ⊆ policy_allowed_edges(H)
```

For every shared DNS target `T`:

```text
dns_answers(T) ⊆ intersection(route_ready_edges(H) for all H sharing T)
dns_answers(T) ⊆ intersection(policy_allowed_edges(H) for all H sharing T)
```

For every edge node `E` and hostname `H`:

```text
if DNS can send H to E:
  E must have route metadata for H
  E must be able to complete SNI/TLS for H
  E must have a valid upstream or safe fallback
```

### 6.2 Route-DNS Guardian

Responsibilities:

- continuously compute route-ready edge sets per hostname
- compute DNS-publishable edge sets per hostname and target
- reject DNS bundles that point at non-route-ready edges
- detect stale DNS node bundle versions
- detect public recursive answers that violate control-plane intent
- quarantine unsafe edge answers for affected hostnames

Safe repairs:

- republish DNS bundle without unsafe answers
- ask stale DNS node to resync
- keep LKG answer set when new answer set is empty or unsafe
- mark hostname degraded when no safe answer exists

### 6.3 Edge-TLS Guardian

Responsibilities:

- validate SNI readiness for route-publishable hostnames
- detect edge nodes that fail TLS for hostnames they may receive
- prewarm active certificates on candidate edges
- separate TLS failure from upstream failure in diagnosis

Safe repairs:

- remove one hostname from one edge's DNS answer set until TLS is ready
- request Caddy reload/resync when route config is stale
- keep serving other hostnames on the same edge

### 6.4 Shared Target Handling

When multiple hostnames share one DNS target, the target cannot have different
answers per original hostname. The system must either:

- publish only the edge set allowed by every hostname sharing the target, or
- split the target so hostnames with different policies receive different DNS
  names

Preferred long-term behavior:

- automatically detect policy divergence across shared target hostnames
- create a new target for the divergent hostname group
- update the app domain record
- expose a migration event
- keep the old target serving LKG until recursive DNS TTLs expire

### 6.5 Synthetic Probes

Add probes after every route or DNS change:

- authoritative DNS query for the hostname or target
- public recursive DNS query
- SNI/TLS handshake to every published edge
- HTTP GET or HEAD for `/`
- platform API synthetic request for known platform paths
- streaming probe for streaming-enabled routes when practical

The probe should record:

- edge IP
- edge group
- route generation
- DNS generation
- TLS certificate subject and expiry
- HTTP status
- response headers
- upstream latency

## 7. Node Health Self-Healing

### 7.1 Node State Model

Every infrastructure node should report:

- role set
- software version
- desired state generation
- active state generation
- LKG generation
- health summary
- last successful reconciliation time
- last error
- disk pressure
- memory pressure
- network reachability

### 7.2 Node Quarantine

Nodes should be quarantined by role and scope, not globally when avoidable.

Examples:

- DNS role unhealthy: remove from delegation candidates, keep edge role if
  healthy.
- Edge TLS unhealthy for one hostname: remove only that hostname answer.
- Edge upstream unhealthy for one runtime: avoid route answers for affected
  routes.
- Node disk pressure: stop assigning new cache-heavy workloads.

### 7.3 Resync Protocol

Add an explicit resync protocol:

```text
control plane marks node generation stale
  -> node receives resync request
  -> node fetches desired artifact
  -> node validates signature/checksum
  -> node stages artifact
  -> node activates artifact
  -> node reports observed generation
```

If resync fails, the node continues LKG and reports a diagnosis event.

## 8. Runtime and App Continuity

### 8.1 Stateless App Recovery

For stateless apps:

- detect runtime node loss
- keep desired replicas available on healthy runtime cells
- shift route/DNS answers only after replacement pods are ready
- preserve old route LKG until new route passes synthetic probes

### 8.2 Stateful App Recovery

For stateful apps, recovery must be explicit and fenced.

Guardrails:

- never perform destructive failover without lease/fence evidence
- require data-plane replication status
- require target runtime readiness
- require restore or replica promotion plan
- validate app-level health after failover

Self-healing can prepare the failover plan automatically, but execution should
respect the app's configured continuity policy.

### 8.3 Operation Recovery

Long-running operations should be resumable:

- every operation has a durable phase log
- every phase is idempotent or has a compensation action
- controller restart resumes from last safe phase
- stuck operation diagnosis explains blocking dependency

## 9. Backup / Restore Robustness

Backups are self-healing only if they are continuously validated.

Requirements:

- backup backend readiness probe
- scheduled control-plane backup health
- backup artifact integrity check
- restore-plan dry run
- periodic restore drill into isolated target
- alert when last successful backup is older than policy

Safe repairs:

- retry transient backend errors with bounded backoff
- block runs when backend is missing or credentials are unavailable
- do not silently mark a backup successful when artifact count or bytes are
  inconsistent with target type

## 10. Observability and Alerting

### 10.1 Metrics

Add or standardize metrics:

- `fugue_guardian_invariant_pass`
- `fugue_guardian_repair_attempt_total`
- `fugue_guardian_repair_success_total`
- `fugue_bundle_publish_rejected_total`
- `fugue_bundle_lkg_serving`
- `fugue_dns_answer_policy_violation_total`
- `fugue_edge_tls_probe_failure_total`
- `fugue_node_generation_drift_seconds`
- `fugue_operation_stuck_seconds`

### 10.2 Events

Every automatic repair should emit an event:

- `detected`
- `repair_started`
- `repair_succeeded`
- `repair_failed`
- `rollback_started`
- `rollback_succeeded`
- `manual_action_required`

Events should include affected hostname/app/node, invariant name, generation,
and diff summary.

### 10.3 CLI/API Diagnosis

The CLI should expose server-authoritative checks where possible:

- `fugue admin robustness status`
- `fugue admin robustness incidents`
- `fugue admin robustness check <hostname|app|node>`
- `fugue admin robustness repair <incident-id> --dry-run`
- `fugue admin robustness explain <incident-id>`

Existing commands should gradually converge on shared check logic rather than
duplicating partial local diagnosis.

## 11. Failure Drills

Create failure drills that run in CI or a controlled staging environment.

Required drills:

- DNS answer contains an edge excluded by route policy.
- Shared DNS target is used by hostnames with conflicting edge policies.
- Edge route bundle lacks a hostname that DNS wants to answer.
- Edge node can answer DNS but fails SNI/TLS for that hostname.
- DNS node serves stale bundle generation.
- Control plane generates invalid DNS bundle and must keep LKG.
- Caddy config reload fails and edge must continue LKG.
- Runtime disappears while stateless app has another healthy runtime target.
- Long-running operation is interrupted and resumes from durable phase.
- Backup backend disappears and backup run blocks instead of starting worker.
- Public recursive DNS still serves old answer after authority is fixed.

Each drill must assert:

- detection happens
- invariant result is explicit
- unsafe artifact is not published
- safe repair or LKG fallback happens
- diagnosis output names the exact reason

## 12. Phased Roadmap

### Phase 0: Baseline Documentation and Incident Capture

Goal: make current boundaries explicit.

Deliverables:

- this plan
- incident class documentation for route/DNS/TLS mismatch
- checklist for future robustness changes

### Phase 1: Publish-Time Guardrails

Goal: stop new bad state before it is published.

Deliverables:

- route/DNS invariant coverage for all route-publishable DNS record kinds
- shared target policy intersection validation
- rejected artifact persistence
- LKG publish fallback
- `make test` failure drills for route/DNS mismatch

### Phase 2: Continuous Detection

Goal: detect live drift without waiting for user reports.

Deliverables:

- route-dns guardian loop
- node generation drift detection
- public recursive DNS sampling
- SNI/TLS synthetic probes
- guardian metrics and events

### Phase 3: Safe Auto-Repair

Goal: repair reversible drift automatically.

Deliverables:

- per-hostname edge quarantine
- DNS node resync request path
- bundle rollback to LKG
- route/Caddy resync
- operator-visible repair event log

### Phase 4: Broader Continuity

Goal: extend the same model to apps, runtimes, backups, and operations.

Deliverables:

- stateless app recovery reconciler
- stateful failover preflight and guarded execution
- backup restore drill automation
- resumable operation controller framework
- unified robustness CLI/API surface

## 13. Detailed TODO List

### A. Invariant Framework

- [ ] Inventory every generated artifact kind and its current validation path.
- [x] Define a shared invariant result schema with `name`, `pass`, `severity`,
      `subject`, `expected`, `observed`, `evidence`, and `repair_hint`.
- [ ] Persist invariant results for rejected artifacts.
- [ ] Make bundle publish code return structured invariant failures instead of
      only log strings.
- [x] Add severity levels: `block_publish`, `degraded`, `warning`, `info`.
- [ ] Add tests proving `block_publish` prevents activation.
- [x] Add CLI rendering for structured invariant failures.

### B. Route / DNS Correctness

- [ ] Build a route-ready edge index keyed by hostname and path prefix.
- [ ] Build a DNS-publishable edge index keyed by DNS record name.
- [x] Validate `dns_answers(hostname) ⊆ route_ready_edges(hostname)`.
- [x] Validate `dns_answers(target) ⊆ intersection(route_ready_edges(shared_hosts))`.
- [x] Validate route policy exclusions for platform routes and custom domains.
- [ ] Validate DNS scoped candidates and answer policy fields, not just static
      A/AAAA values.
- [x] Add failure drill for shared target policy conflict.
- [x] Add failure drill for route-ready mismatch after edge exclusion.
- [ ] Add failure drill for empty safe answer set.
- [ ] Decide fail-closed behavior for empty answer sets: LKG, SERVFAIL, or
      explicit degraded answer.
- [ ] Add per-hostname diagnosis that shows route-ready set, policy-allowed
      set, DNS answer set, and diff.

### C. DNS Node Reconciliation

- [ ] Track desired DNS bundle generation per DNS node.
- [ ] Track observed active/LKG DNS generation per DNS node.
- [ ] Detect stale DNS node generation.
- [ ] Add control-plane initiated DNS node resync request.
- [ ] Add DNS node acknowledgement with staged/active generation.
- [ ] Add timeout and retry policy for DNS resync.
- [ ] Add event when DNS node serves LKG.
- [ ] Add metric for DNS generation drift seconds.
- [ ] Add drill where one DNS node misses an update and self-recovers.

### D. Edge / Caddy / TLS Recovery

- [ ] Define per-hostname edge TLS readiness.
- [ ] Probe SNI/TLS for every DNS-publishable hostname-edge pair.
- [ ] Record certificate subject, SAN match, issuer, expiry, and handshake
      failure reason.
- [ ] Add edge route generation drift detection.
- [ ] Add Caddy reload failure detection as a structured state.
- [ ] Keep previous Caddy config active when new config fails validation.
- [ ] Add per-hostname edge quarantine for TLS failure.
- [ ] Add automatic unquarantine after N consecutive successful probes.
- [ ] Add drill for edge TLS failure on one hostname.
- [ ] Add drill for Caddy config reload failure.

### E. Synthetic Probes

- [ ] Add a probe scheduler in the control plane.
- [ ] Add probe targets for DNS, TLS, HTTP, platform API, and streaming routes.
- [ ] Store probe results with generation IDs.
- [ ] Use probes as post-publish health gates.
- [ ] Use probes as continuous drift detection.
- [ ] Add probe budget and rate limits.
- [ ] Add probe result summaries to CLI diagnosis.
- [ ] Add public recursive DNS sampling with resolver labels.
- [ ] Add EDNS client subnet sampling for DNS steering validation.

### F. LKG Registry

- [ ] Define generic LKG metadata schema.
- [ ] Store LKG for DNS bundle.
- [ ] Store LKG for edge route bundle.
- [ ] Store LKG for Caddy route config or rendered config material.
- [ ] Require validation and probe pass before promoting to LKG.
- [ ] Add node-side LKG reporting.
- [ ] Add rollback API for artifact kind and generation.
- [ ] Add drill for bad new bundle with LKG serving.
- [ ] Add operator documentation for LKG states.

### G. Guardian Controllers

- [ ] Create guardian controller interface.
- [ ] Implement route-dns guardian as first guardian.
- [ ] Implement edge-tls guardian.
- [ ] Implement node-health guardian.
- [ ] Implement bundle-rollout guardian.
- [ ] Add leader election or singleton scheduling for guardians.
- [ ] Add bounded concurrency and rate limits.
- [ ] Add repair dry-run mode.
- [ ] Add repair audit events.
- [ ] Add repair disable switch for emergency operator control.

### H. Release Pipeline Hardening

- [x] Add post-deploy DNS invariant check to GitHub Actions.
- [x] Add post-deploy route-check for platform hostnames.
- [ ] Add post-deploy synthetic TLS/API probe.
- [x] Fail deployment workflow when new control plane cannot generate valid
      route/DNS bundles.
- [ ] Add staged rollout for control-plane bundle-affecting changes.
- [ ] Record deployed commit SHA in control-plane status.
- [ ] Add rollback instructions for failed deployment health gates.

### I. Runtime and App Continuity

- [ ] Define app continuity invariant schema.
- [ ] Separate stateless and stateful recovery policies.
- [ ] Detect app replicas below desired count because runtime is offline.
- [ ] Add stateless runtime replacement preflight.
- [ ] Add route shift only after replacement readiness.
- [ ] Add stateful failover preflight result object.
- [ ] Require lease/fence evidence before stateful execution.
- [ ] Add operation phase resume tests for failover operations.
- [ ] Add drill for runtime loss with stateless app recovery.

### J. Backup / Restore

- [ ] Add backup backend readiness invariant.
- [ ] Add scheduled backup freshness invariant.
- [ ] Add artifact integrity invariant.
- [ ] Add isolated restore dry-run plan.
- [ ] Add periodic restore drill job.
- [ ] Add blocked-run event when backend is missing.
- [ ] Add metric for age of last successful backup per policy.
- [ ] Add drill for transient backend failure and retry.
- [ ] Add drill for missing backend and blocked run.

### K. Operations and Jobs

- [ ] Inventory long-running operations and phase boundaries.
- [ ] Mark each phase idempotent or compensatable.
- [ ] Persist phase input/output.
- [ ] Resume operations after controller restart.
- [ ] Detect stuck operations.
- [ ] Add operation diagnosis with blocking dependency.
- [ ] Add operation cancellation semantics.
- [ ] Add drill for controller restart during operation.

### L. Observability

- [ ] Add guardian metrics.
- [ ] Add bundle publish rejection metrics.
- [ ] Add LKG serving metrics.
- [ ] Add node generation drift metrics.
- [ ] Add synthetic probe metrics.
- [ ] Add repair attempt/success/failure counters.
- [ ] Add event stream for automatic repairs.
- [ ] Add dashboard for robustness status.
- [ ] Add alert rules for block-publish and prolonged degraded states.

### M. CLI and API

- [x] Add OpenAPI schemas for robustness checks.
- [x] Add `GET /v1/admin/robustness/status`.
- [x] Add `GET /v1/admin/robustness/incidents`.
- [x] Add `GET /v1/admin/robustness/incidents/{id}`.
- [x] Add `POST /v1/admin/robustness/incidents/{id}/repair-plan`.
- [x] Add `POST /v1/admin/robustness/incidents/{id}/repair`.
- [x] Add CLI command `fugue admin robustness status`.
- [x] Add CLI command `fugue admin robustness check`.
- [x] Add CLI command `fugue admin robustness incidents`.
- [ ] Make existing DNS/edge diagnosis commands use shared server-side check
      logic where possible.

### N. Documentation and Runbooks

- [ ] Write route/DNS/TLS mismatch runbook.
- [ ] Write LKG serving runbook.
- [ ] Write edge quarantine runbook.
- [ ] Write DNS node resync runbook.
- [ ] Write control-plane publish rejection runbook.
- [ ] Write synthetic probe failure runbook.
- [ ] Add a robustness section to `docs/ha-dr.md`.
- [ ] Add failure drill instructions for staging.
- [ ] Add operator checklist for safe manual override.

## 14. Definition of Done

The robustness program is not done when checks exist. It is done when Fugue can
demonstrate these behaviors in tests and staging drills:

- invalid route/DNS state is rejected before publish
- live DNS drift is detected without user reports
- unsafe edge answers are automatically removed or rolled back
- nodes can keep serving LKG during control-plane failure
- diagnostics show exact expected vs observed state
- automatic repair actions are visible and auditable
- dangerous recovery actions stop at an explicit manual approval boundary

The practical target is:

```text
No single generated-state bug should silently send production traffic to an
edge, route, DNS answer, TLS path, runtime, or backup action that violates the
published policy for that resource.
```
