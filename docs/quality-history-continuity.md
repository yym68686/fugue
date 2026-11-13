# Quality history continuity

V7 separates ranking age from permission to serve. A new capture may carry one
original quality receipt recovered from the independently verified full LKG's
compiler input. The original selection is reconstructed at its original time,
under its original policy. V2–V6 receipts are not rewritten into authenticated
V7 probe rounds. Unknown metrics stay unknown.

When fresh comparable V7 evidence exists, it takes precedence. Otherwise a
compatible historical order can be retained or restored without requiring a
new measurement campaign. Only the same DNS name, service, path, traffic class,
network scope, physical edge/group and route content are comparable. Current
health, recovery admission, route/TLS proof and negative capacity gates still
apply. Unmeasured recovered candidates rejoin as deterministic fallbacks, not
as invented quality winners. Shared aliases require every owner to agree.

The response and published physical selection expose `history`: original
artifact/input/receipt digests, policy version, observation time, age, original
order, reused order and retention/restoration reason. `retained` never means
newly measured. Merely refreshing a receipt cannot renew the original clock.
The original receipt is base64-encoded JSON so legacy byte-order-sensitive
embedded DNS evidence survives exports. History remains single-level across
restarts; fresh measured captures discard unused history.

No new observer, browser measurement service, Actions measurement schedule or
capacity-recovery threshold is introduced. Actions remains the release path.
All route, TLS and DNS executors must advertise V7 capability before a V7 signed
parent can enter gray/full publication. This is executable compatibility, not a
claim that the candidate was applied or that DNS switched. Final verification
requires fresh consumer convergence and actual authoritative DNS answers.

DNS code releases must stage an unselected private slot first. After its exact
reader identity, current assignment, positive checkpoint and direct UDP/TCP
answers pass the existing transport verifier, publish a separate declarative
transport handoff. Only then replace the former public slot. A code intent must
not restart the selected public backend or bypass this handoff protection.

A cold historical compiler input needs a complete verified read before its
selective DNS index is available. Historical recovery explicitly allows up to
90 seconds for that first read, before starting the separate live observation
deadline and capture clock, still bounded by the caller's cancellation. This
prepares only byte positions and hashes: binding rereads evidence and rechecks
the original positive authority. Producer captures prepare once before their
bounded job batch, not once per candidate or DNS consumer.
Ordinary selective reads retain their 30-second default. A timeout is logged as
unavailable history; it never deletes live observations or renews old evidence.
The same preparation phase completes the runtime and original-input read
admissions. Its immutable source binding is shared only in that request/batch
for at most 30 seconds after preparation; it is never stored on the server.
Live binding rereads the signed source and positive LKG and rejects any change.
It does not rerun a potentially minute-long closure check inside a five-second
fresh-probe window. Missing original admission can still retain an independently
admitted published order. Publication keeps its separate complete admission.
On restart or delayed reader ACKs, the producer retries its exact owned shadow
before capturing a replacement, within the configured rollout timeout. Current
publication gates remain mandatory, and a failed-source marker still prevents
retrying a rejected source. Stale process observations cannot veto new reader
compatibility; missing fresh support still blocks publication.
CLI capture with an explicit DNS node allows two minutes for this cold-history
request; ordinary commands retain their timeout and caller cancellation wins.

When a positive fallback publication contains only an order, capture verifies
its signed DNS member and exact actual-DNS predecessor separately. The response
labels this `history.evidence_kind=published_order`: it preserves the original
quality timestamp and incumbent, but makes no claim of replaying missing raw
measurements. Current eligible recovered nodes rejoin its fallback order.
This path does not require new client probes and does not expire an otherwise
valid order merely because no replacement measurement exists. It cannot restore
a different historical winner. Full receipt replay still has priority, and
fresh comparable measurements supersede both forms of history.
Opted-in configuration inputs intentionally omit probe transcripts; receipt
replay reads the separately admitted original input, not that projection.
