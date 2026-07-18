# Release-domain gate

The release-domain gate is the production mutation boundary for the shared
Fugue Helm release. `scripts/upgrade_fugue_control_plane.sh` enables the gate
before it installs release cleanup and signal handling, then delegates the
release to the fixed production dispatcher. The former cross-domain upgrade
tail is not an alternate production path.

The planner and evidence producer introduced in Boundaries A and B remain
side-effect-free building blocks. Boundary B3 activates them as one atomic
production path with canonical rendering, reverse ownership proof, sealed
authorization, fixed adapters, durable trace, rollback, and public evidence.

## Fixed release domains

`deploy/release-domains/ownership-v1.yaml` is the release-time mutation
allowlist. It is not workload RBAC. A rendered object or field not positively
matched by this file is unknown.

| Domain | Rendered ownership | Production boundary |
| --- | --- | --- |
| `node-local` | NodeLocal PriorityClass, ServiceAccount, ConfigMap, upstream/cache/active Services, and preserved/active DaemonSets | Does not use the backup Lease and does not invoke API/controller rollout, authoritative-DNS transaction, image-cache mutation, CoreDNS repair, or generic node maintenance. |
| `authoritative-dns` | Main and normalized group DNS DaemonSets | Owns only the bounded authoritative-DNS transaction; the shared edge image does not grant edge proxy, Caddy, or Ingress ownership. |
| `control-plane` | API/controller Deployments and PDBs, API Service, optional Ingress, generated CNPG Secret, and CNPG Cluster fields outside `/spec/backup` | May acquire the existing backup coordination Lease before its adapter writes. |
| `image-cache` | Image-cache DaemonSet | Does not own builder labels, registry, image-store defaults, NodeLocal, or generic node maintenance. |
| `backup` | ScheduledBackup, restore-drill CronJob, and CNPG Cluster `/spec/backup` | May acquire the existing backup coordination Lease; it does not roll API/controller or delete historical Backup objects. |

The matcher requires exact group, version, kind, scope, namespace, resolved
name, and chart labels. DNS group names are the only prefix rule, and their
suffix must equal the suffix of the
`app.kubernetes.io/component=dns-...` label. Missing bindings, label mismatch,
ownership overlap, duplicate identity, `generateName`, malformed YAML, or a
changed CRD produce `unknown`.

CNPG Cluster ownership uses longest-JSON-Pointer matching. `/spec/backup` and
its descendants belong to `backup`; every other desired Cluster field belongs
to `control-plane`. A diff that touches both is `multiple`.

## Evidence and canonical render set

Every ordinary dispatch regenerates revision-bound changed-file evidence from
the dedicated release baseline commit to the exact target commit. The producer
resolves both revisions to commit OIDs, enriches Go consumers and versioned
values leaves, and writes a digest-bound private artifact. Shared or
insufficiently attributable runtime input is unknown; the gate never guesses a
domain by taking the union of inconsistent evidence.

The production path freezes one Helm upgrade argv snapshot containing exactly
one bare `--no-hooks` argument. From that snapshot it produces three private
canonical manifests in one attempt:

1. the live base manifest at the current Helm revision;
2. the target manifest at the immediately adjacent revision; and
3. an independent repeated target render at that same adjacent revision.

The target renders explicitly exclude hooks, must be byte-identical after
canonicalization, and must agree with the changed-file classification. The
base identity must equal the live release identity. Canonicalization expands
`List`, injects the effective namespace, sorts objects and map keys, preserves
array order, labels, annotations, Secrets, and numeric values, and removes only
`status` plus an empty renderer-created `metadata.creationTimestamp`.

The real upgrade also runs with `--no-hooks`; hooks are therefore neither
retained in the canonical evidence nor executed as an unowned side channel.

## Planner outcomes

| Result | Production behavior |
| --- | --- |
| `zero` | Both classifiers are empty and all changed files are proven non-runtime. The gate performs no Lease, Helm, Kubernetes, operational host, or transaction write. It records public no-write evidence and returns without selecting an adapter. |
| `single` | Both classifiers identify the same one domain. The gate seals the exact authorization and invokes only that domain's fixed adapter. |
| `multiple` | Two or more domains are identified. The gate blocks before every operational write, records blocked public evidence, and does not split the release. |
| `unknown` | Parsing, ownership, changed-file enrichment, render equality, digest, or context validation failed. The gate blocks before every operational write; no generic risk approval can bypass it. |

`multiple` and `unknown` are safe blocked results, not adapter choices. A
blocked or zero result never enters the transaction dispatcher.

The production file-only precheck deliberately stops before rendering when it
already sees multiple domains. Its public no-write artifact is therefore
projected conservatively as `unknown`, rather than claiming that an unexecuted
render independently proved `multiple`. Both outcomes have the same zero-write
and lane-freeze behavior.

## Reverse ownership proof and sealed authorization

Before a single-domain write is possible, authorization proves ownership in
both directions:

- base to target permits changes only in the selected domain; and
- target to base proves that the rendered rollback also restores only that
  same domain.

The proof binds the exact release name and namespace, adjacent Helm revisions,
ownership and changed-evidence digests, base/target/repeated-target manifests,
the frozen NUL-delimited Helm argv, the immutable plan, and its execution
binding. The private authorization bundle is created in a `0700` directory
from `0600` regular files, rejects symlinks and replacement races, and writes
its decision last. The dispatcher verifies the complete bundle again directly
before Apply; Apply reads the sealed argv snapshot rather than reconstructing
the command from mutable shell state.

Only a strict, fully verified bundle can authorize execution. Public evidence
is a separate secret-free artifact. It records the outcome, selected domain,
plan digest, run identity, write-boundary state, and rollback state without
publishing argv, environment, manifests, private paths, or secrets.

## Literal dispatcher and transaction

`scripts/lib/control_plane_release_domains.sh` contains one literal `case` for
the five allowed domains:

- `node-local`
- `authoritative-dns`
- `control-plane`
- `image-cache`
- `backup`

Each branch names its `prepare`, `apply`, `verify`, and `rollback` callbacks
literally. Domain or phase text is never normalized, concatenated, evaluated,
or used for dynamic callback lookup. `prepare` is read-only. A durable
`apply/started` trace is the conservative write boundary; the final sealed
authorization verification is immediately adjacent to the selected Apply
callback. After that boundary, every forward failure takes the selected
adapter's exactly-once rollback path. Rollback failure is recorded and is not
retried or replaced by a different domain's recovery helper.

The fsynced `transaction/succeeded` trace record is the commit linearization
point. No fallible rollback-eligible domain mutation or verification runs after
it. Post-commit Lease owner-CAS release, DNS bookkeeping, private cleanup, or
public-evidence publication may still fail; such a failure freezes the lane and
must never trigger rollback of the committed business state. The workflow
retains the existing shared Helm release/global mutation mutex. The backup
coordination Lease is additionally available only to the literal
`control-plane` and `backup` branches; `node-local`, `authoritative-dns`, and
`image-cache` cannot acquire, drain, release, or restore it.

## Production entrypoint

The ordinary workflow supplies these exact production inputs to
`scripts/upgrade_fugue_control_plane.sh`:

- `FUGUE_RELEASE_DOMAIN_BASE_SHA`
- `FUGUE_RELEASE_DOMAIN_TARGET_SHA`
- `FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL`
- `FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL`
- `FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE`

After configuration and read-only Helm/Kubernetes discovery, `main` reads the
current Helm revision and calls the atomic release-domain gate. It does not run
the legacy monolithic mutation sequence afterward. Signal and EXIT handling
remain bound to the selected transaction: an interrupted write rolls back only
the selected domain, and a committed transaction is not contradicted by a
later rollback.

## Dispatch-only workflow and baseline

`.github/workflows/deploy-control-plane.yml` is dispatch-only. A caller must
provide `expected_sha`, and the input guard requires an exact lowercase
40-character SHA equal to `github.sha` on `refs/heads/main`. A push alone does
not start a production release.

Domain comparison uses the dedicated forward-only branch
`refs/heads/fugue-control-plane-release-baseline`. A normal code baseline
resolves directly to one exact ancestor commit. The one-time RP0 bridge may
instead resolve to a parentless, one-file metadata commit; the dormant
resolver accepts only the canonical schema-1 payload and uses its runtime SHA
as the domain ancestor while retaining the metadata commit as the observed ref
object. After the independent recorder-compatibility checkpoint and a complete
runtime release, the hosted recorder advances the observed object with one
GraphQL
`updateRefs` mutation whose `beforeOid` is the exact object observed by the
resolver, whose `afterOid` is the dispatched SHA, and whose `force` value is
false. After that single mutation attempt, bounded exact readback is the sole
settlement authority: an exact target object succeeds even when the mutation
transport fails or returns an unexpected echo. An absent, unreadable,
divergent, or concurrently changed branch that never settles at the exact
target fails closed. The live-image baseline used by existing image and
release safety checks remains independent from this domain-planner baseline.

There is no runtime genesis fallback in the forward transport. The one-time
RP0 migration is split into independent prerequisites. The first hosted
checkpoint binds the successful historical runtime run, immutable artifact
digest, Helm revision transition, complete observation window, and exact
policy SHA, then materializes a canonical one-file orphan Git commit. The
commit contains `fugue-runtime-baseline.json` with schema version 1, the last
verified runtime SHA, and a null previous-object field. It creates no Git ref,
performs no cluster write, and proves the baseline branch remains absent after
the immutable blob, tree, and commit objects are read back. Reader and
ref-creation checkpoints follow separately. The RP0 policy SHA is not written
as a runtime baseline, and no external PAT or local OAuth credential is used.

The second checkpoint is a separate GitHub-hosted read-only consumer. It binds
the exact successful materialization run and immutable result artifact, reads
the root commit, one-file tree, and canonical blob through the Git database
API, rechecks the historical runtime run and attribution artifact named by that
result, proves runtime ancestry, observes unchanged production health, and
requires the baseline ref to remain absent throughout. Its output evidence
retains both provenance layers. It cannot create Git objects or refs and has no
self-hosted or cluster path.

The following checkpoint creates the still-absent baseline ref exactly once at
the validated orphan metadata commit. It consumes the successful reader run
and immutable artifact, revalidates the root object chain, uploads intent
evidence, completes the unchanged-health window, then performs one REST
create-reference attempt as its final write. The call has expected-absence
semantics, accepts no force option, and targets the metadata-only orphan commit
rather than a code commit containing workflow files. Immediately before that
attempt, the reader, materializer, and legacy deploy lanes are rechecked as
disabled. The writer then performs only bounded readback settlement: an exact
metadata-root ref settles a lost or failed transport response, while an absent,
unreadable, malformed, or different ref fails closed. No cluster path is
available.

The following dormant compatibility checkpoint changes only the deploy
resolver's read path. A parentless ref object's non-recursive root tree must
contain exactly one regular blob named `fugue-runtime-baseline.json`; its bytes
must be the compact, sorted schema-1/null-previous-object JSON followed by
exactly one newline. The embedded runtime SHA must exist locally and be an
ancestor of the dispatched target. A ref object with parents remains an
ordinary direct code baseline. The recorder is intentionally unchanged in
this checkpoint, and the deploy workflow remains disabled, so no self-hosted
or cluster path is exercised.

The self-hosted deploy job preloads and verifies the exact Linux AMD64 and
ARM64 command dependency graphs before building the private evidence tools.
Evidence generation then uses that checksum-verified download cache as an
offline file proxy; an absent or incomplete cache remains a fail-closed
evidence error.

Every successful ordinary deploy must upload exactly one
secret-free release-domain evidence artifact with a 90-day retention policy.
A missing or invalid artifact is itself a deploy failure. A failure in the
input guard, prerequisites, deploy, evidence upload, or baseline advancement
enters the freeze job: it uploads lane-freeze evidence, disables the workflow,
and cancels other non-terminal runs. The lane is not silently re-enabled or
advanced after a failed release.

## Operator interpretation

- `zero`: verify the public artifact says the write boundary was not crossed;
  no adapter or rollback should appear in the trace.
- `single`: verify the selected literal domain, plan digest, successful durable
  transaction, and unchanged non-owned canonical objects.
- `multiple` or `unknown`: treat the run as a safe block and frozen release
  lane, not as permission to split, SSH-patch, or bypass the gate.
- rollback succeeded: preserve the evidence and keep the lane frozen for
  investigation.
- rollback failed: preserve the evidence and keep the lane frozen; do not run a
  different adapter or a legacy cross-domain recovery path.

The Boundary A planner and Boundary B evidence/render/envelope work are
historical implementation milestones. Boundary B3 is their sole formal
production activation path.
