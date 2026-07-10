# Platform Consumer Identity Runbook

## Purpose

Use this runbook when a platform consumer heartbeat reports an identity,
protocol, replay, fencing, or generation violation. Platform consumers include
edge workers, DNS servers, Caddy edge fronts, node updaters/guardians, and
runtime agents.

The response priority is:

1. Prevent untrusted evidence from promoting a platform release.
2. Preserve the current verified LKG and serving data plane.
3. Revoke or rotate only the affected credential scope.
4. Restore trusted heartbeat evidence before resuming promotion.

An identity failure is not, by itself, permission to quarantine the node or
remove all edge groups.

## Identity Contract

A trusted platform component identity must bind all of these values on the
server side:

- credential id and short-lived token id;
- component;
- node id;
- scope key;
- allowed artifact kinds;
- signing key id;
- issued-at and expiry.

The heartbeat must additionally carry a release-set id, fencing token,
protocol/schema version, monotonic sequence, issued-at, nonce, generation
sequence, desired/actual/LKG generations, apply/probe state, and canonical
SHA-256 evidence hash.

Do not accept a tenant API key or a client-declared component/node/scope as a
substitute for this binding.

## Current Rollout State

The short-lived identity token and heartbeat validation primitives exist, but
production credential issuance and heartbeat handler enforcement are not yet
wired. Until that integration is complete:

- legacy heartbeat rows are not trusted promotion evidence;
- do not enable consumer convergence enforcement merely because rows exist;
- continue to require independent local probes, public synthetics, watch
  window evidence, and the pinned rollback target.

## Detection

Treat any of these as a security or release-integrity signal:

- component, node, scope, or artifact kind differs from credential claims;
- signature key id is unknown or the signature is invalid;
- token is expired or issued too far in the future;
- sequence does not increase;
- nonce is already in the retained replay window;
- heartbeat is older than its freshness policy;
- generation sequence or fencing token decreases;
- canonical evidence hash does not match the body;
- protocol/schema version is missing or incompatible.

## Read-Only Triage

```bash
fugue admin release guard status --json
fugue admin robustness status --json
fugue admin consumer expected --release-set <release-set-id> --json
fugue admin artifact consumers <artifact-id-or-generation> --json
fugue admin artifact lkg <artifact-id-or-generation> --json
```

Record the exact release set, artifact kind, scope, expected consumer id,
observed consumer id, credential id, token id, signing key id, sequence,
issued-at, generation sequence, fencing token, and evidence hash. Do not record
the bearer token or signing secret.

## Containment

1. Hold promotion for the affected release lane. Keep the current verified LKG
   available.
2. Exclude the rejected heartbeat from convergence evidence.
3. If one component credential is compromised, revoke that credential/token
   id. Do not rotate unrelated node or tenant credentials.
4. For normal signing-key rotation, issue with the new active key and retain
   the previous verification key for at least the maximum 15-minute token TTL
   plus clock skew.
5. For confirmed signing-key compromise, remove the compromised verification
   key immediately. This intentionally invalidates every in-flight token
   signed by that key; keep the release lane held until affected components
   obtain new identities.
6. Do not use kernel break-glass to accept an identity mismatch.

## Recovery Conditions

Resume release verification only after all are true:

- the expected consumer set revision is unchanged or deliberately regenerated;
- every required consumer has a newly authenticated heartbeat;
- sequence, nonce, time window, generation sequence, and fencing token validate;
- desired/actual generation and apply/probe evidence converge;
- no revoked credential or signing key appears in accepted evidence;
- local probes, public synthetics, and the changed-subsystem watch window pass;
- release guard reports no new blocker.

Retain the rejected evidence hash, validation reason, credential metadata, and
rotation/revocation action in the tamper-evident audit chain once production
heartbeat auditing is enabled.
