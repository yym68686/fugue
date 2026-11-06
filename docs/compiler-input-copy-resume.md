# Bounded compiler-input copy resume

Copy preserves the source rows and the existing read route. It does not
reclaim database space, install a hot copy, pin data, fence writers, or retire
a relation. Copying an input does not require the complete hot dependency
graph; routing or retirement still requires its separate safety proof.

The original outer and native v1 declarations remain single-attempt and
require a previously unattempted input. They cannot contain resume fields.
A resume is a new, explicitly authorized outer v2 declaration, with a new
immutable reservation. The original reservation and any partial immutable
objects are retained. No operation retries automatically, including an
ambiguous claim commit or release of a suspended Job.

## Contract and proof

The outer v2 declaration adds `resumeProof` and
`terminalReceiptRequired: true` to the original bounded declaration.
Its native `copy` contract is v2 and includes `resume`:

- Complete source-manifest hash, including creation time and legacy location.
- Archive backend, object prefix, epoch and previous fencing token.
- Ordered contiguous verified segment-receipt hash, count and stored bytes.
- Original declaration hash, immutable reservation UID and complete data hash.

`resumeProof` contains the complete original v1 declaration, an exact GitHub
artifact contract, and a cumulative payload/request/source-byte envelope.
The artifact contract specifies repository, original declaration path, main
commit, workflow run and attempt, artifact ID and SHA, and result member and
SHA. A trusted runner verifies the original declaration against that Git
commit and fetches the exact terminal main-push run of the sole `ci.yml`
workflow. ZIP and JSON parsing are bounded and reject ambiguous fields.
The original immutable reservation contains intent, not actual usage; it
cannot replace this terminal result proof.

The result must prove exact worker identity, no restart, confirmed resource
cleanup and a fully checkpointed partial prefix. Unconfirmed extra PUTs are
not inferred away. This version accepts a resume from the original v1
attempt only, rather than recursively trusting an arbitrary chain of resumes.

Within the existing bounded Repeatable Read READ ONLY query, the runner
fetches all source identity fields, archive identity, ordered receipts and
maintenance state. No source body or R2 object is fetched by preflight.
Immediately before unsuspending the worker, it rechecks fresh metadata.
The native transaction independently checks the same source, token, epoch,
prefix, receipts and absence of an active owner before a token-increment CAS.
Proof verification alone returns `execution_authorized: false` and never
grants permission to execute.

## Budgets and completion

The worker uses the original immutable prefix. It re-reads the complete
source and verifies prior receipts by GET; it PUTs only remaining segments.
A second independent complete remote reconstruction verifies the original
SHA. Future GET requests therefore remain twice the complete segment count.

The new declaration counts the prior actual usage plus the new maximum
allowance against its explicit cumulative envelope. Cumulative PUT and GET
byte ceilings cannot exceed the original declaration's byte ceilings. The
request and complete-source reread allowances are explicit as well.
Independent PUT/GET ceilings are stop limits; exhausting both need not leave
enough budget to complete. No bounds are silently increased to obtain success.
The original database reserve, WAL/drop/error bounds, rate and time limit
cannot be weakened. Namespace, database and cluster identity remain bound.

New gzip objects also have an enforced encoding bound of
`raw + 6 * ceil(raw/65535) + 32` bytes per segment. An expanding BestSpeed
member falls back to stored gzip, and the result is checked before PUT.
Existing receipts are read with their original hashes and representation;
they are neither recompressed nor subject to the new-generation bound.
This permits a complete payload envelope without assuming a compression
ratio. Payload pacing time leaves a separate reserve for database work,
compression, locks and request latency; that reserve is not a guarantee
against the unchanged SQL/barrier deadlines or external failures.

The worker still has one connection, one CPU, bounded memory/ephemeral space,
no Kubernetes token, no platform signing key, no object-delete capability,
and no routing opt-in.

Native success must report all original bytes verified and both full GET
passes, with only remaining PUTs. The durable archive's stored bytes equal
the old verified prefix plus this attempt's PUT bytes. After exact-UID
Job/session/PriorityClass cleanup, the trusted runner creates and reads back
an additional immutable terminal ConfigMap containing bounded result and
cumulative usage. Missing or ambiguous terminal confirmation is a failure;
the reservation remains and the operation must be inspected before any new
authorization. Neither receipt is overwritten or deleted for a retry.

Deploying this capability does not authorize a production resume. The
production operation must stay unchanged until a fresh complete declaration
and its concrete resource/IO envelope are explicitly approved. The normal
main-push CI path remains the only production release/execution entrypoint.

## Writer impact observations

The private runtime storage snapshot exposes a separate
`compiler_input_writer_activity` observation with schema
`fugue.compiler-input-writer.observation.v1`. Its process-local counters cover
the whole `pgEnsureChunkedInput` call, including transaction start, the writer
barrier queue, existing-input verification, encoding and commit. They retain
no raw errors, input digests, SQL or source content. A coherent snapshot obeys
`attempts = in_flight + successful_returns + deadline_failures + canceled_failures + other_failures`.
`successful_returns` counts nil function returns; it is not an independent
durable-commit receipt. In-flight calls have no completed outcome yet. A panic
still propagates and is counted as another failure rather than success.

The copy runner requires this complete observation from every exact serving
API owner before creating an execution reservation and on every impact check.
Missing, malformed, inconsistent or reset counters refuse execution. New
cancellations or other failures stop the attempt; new outer deadline failures
use the existing per-API deadline allowance. The separated writer's existing
failure/fallback checks and counter semantics stay intact. The two deadline
counters can describe the same call and are checked independently against the
same limit, rather than added together. This adds observation coverage without
changing writer deadlines, cancellation propagation, validation or copy limits.
Process replacement and restarts remain independent identity-gate failures;
a counter snapshot or a quiet log tail cannot establish their absence.
