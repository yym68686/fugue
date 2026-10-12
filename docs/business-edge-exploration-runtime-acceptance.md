# Business exploration runtime acceptance

## First compatible runtime release

Revision `afaf749e61e948d6439878b8cc644eb213900e76`, CI `38285438374`,
selected API, the two guarded worker groups and non-public DNS C instances.
Full `make test`, affected tests/vet, compile, declarative release tests, schema
drift and race tests passed locally. No business exploration intent was enabled.

API, Germany workers and both DNS candidates deployed. The US worker stopped
before its prewrite plan and before Guardian publication. At 17:58:13 UTC its
exact error was `edge group has 2 healthy nodes, want 3`.

The declared US baseline count was two. `edgecohort.FromNodes` adds one explicitly
admitted managed member. The frozen inventory contains one admitted member, but
all three DaemonSet selectors require `fugue.io/schedulable=true`: the established
US node and admitted member match; the quota-exhausted baseline node does not.
All three desired/ready counts are two. The selected signed global policy
`edge-offline-20261012-v1` explicitly excludes the quota-exhausted node. This is a
stale release-cohort declaration, not a transport-score failure.

The repair is an explicit US release declaration with baseline count one; the
existing managed-enrollment rule still adds the admitted member, so exactly two
nodes must pass the complete frozen Front/worker/authority/route checks. This is
not dynamic removal based on failed health. It changes no node labels, Pod,
runtime role, routing intent, independent Cell membership, or quorum rule.
Restoring the suspended baseline requires a new explicit declaration and normal
admission; the quota recovery date does not authorize restoration.

## Passive-evidence association regression

At 17:57 UTC the exact `/`, `streaming` service for the observed production
hostname has two signed DNS candidates and the currently serving first choice
is the managed member. Do not replace that current baseline with an older
declaration's order. The US captured source contains 115 passive records for the
managed member and five for the other candidate. Other sources are excluded.

The initial business converter looked up each raw sample ID in a map keyed by
derived observation ID. Actual observation IDs are
`network:<edge_id>:<sample_id>`, not `<sample_id>`, so all 120 bound passive records
were silently omitted. This was established from the retained actual source and
an offline conversion, not inferred from empty coverage. Exploration remained
disabled, so this did not authorize a business allocation.

The repair shares the exact composite identity mapping between CLI and producer;
the producer still independently reconstructs route witnesses before conversion.
Tests reject raw-ID confusion, cross-edge identity, different sources, missing
observations and undeclared candidates. A producer integration test uses the
actual observation builder and requires the original TCP sample in its receipt.

## Read-only acceptance evidence

- Both DNS C instances run `afaf749e`, are Ready with zero restarts, and passed
  checkpoint/artifact signature, current verified full publication, every positive
  probe and complete per-record quorum verification. Each had 503 positive probes.
  An initial US read failed the authority/freshness predicate; a fresh complete
  read passed. The original raw predicate operands were not retained, so its
  specific cause is not attributed to a race or another mechanism.
- Five-minute public DNS counter sampling completed with 30 samples, no new
  SERVFAIL and no observation errors; historical totals are not attributed.
- Native strict-host-key `ssh poly` and `ssh argus`, without remote commands,
  returned `-bash`, `SHELL=/bin/bash`, `HOME=/workspace` and exit zero. The trust file
  hash was unchanged. Existing business Pod/container identities and zero restart
  counts remain the recorded baseline; Warp UI was not tested.

Local bounded evidence is retained in `/tmp/fugue-business-*`. No credentials or
raw business responses are included in this document. Public DNS handoff,
phase-one activation, real comparison acceptance and phase-two activation are
still pending; runtime deployment alone is not completion.

At 18:26 UTC both logical DNS-node captures using the repaired CLI retained 118
actual passive records and replayed successfully. They reported incomplete common
cohort evidence, not a winner; no serving authority or allocation was published.
Both isolated candidates also passed all 24 actual UDP/TCP A/AAAA/NS/SOA queries.
Transport generation 35 selects only the US public listener's C backend; Germany
remains D until independent handoff acceptance. The existing CI selector-CAS and
fresh signed observation checks remain mandatory.

CI `38291664514` successfully applied the US handoff. At 18:46 UTC the public US
Service selected the exact C Pod UID and a Ready EndpointSlice, while Germany
still selected D. All 24 public UDP/TCP checks passed. At 18:47 UTC fresh complete
reads again verified both C signatures, current full publications and all 503
positive probes per instance. Transport generation 36 selects Germany C only.

The US worker retry passed the corrected member-count declaration but was stopped
before writes at 18:32:48 UTC: the managed node's legacy Front retained eight
active connections. Its Pod UID and host ports 80/443 are confirmed; no public
Front Service selects an independent executor on that node. Guardian subsequently
returned to stable with the previous worker authority. This is an intentional
connection-preservation gate, not permission to delete the Pod or force drainage.
An independently staged Front and the existing fenced transport handoff are
required before retrying the worker update. Fixed Cell membership is unchanged.

The post-US-handoff monitor observed five new SERVFAIL counters on the unchanged
Germany D instance at 18:49 UTC; US C remained at zero. The retained latest-20
receipt query did not contain those failures (journal totals: 17 dropped, 44,258
evicted). The increment is therefore unattributed, not evidence for a particular
route/probe cause or an exploration regression. The raw bounded timeline is
retained; neither historical success nor current absence of failures closes that
evidence gap. Germany C independently passed the signed complete verification.

## Independent Front prerequisite

The new `edge-client-front-us-managed` declaration stages a separate Pod-network
executor on the explicitly identified admitted US node. It uses the existing
read-only activation directory, no host ports, no API credential and no Cell
membership label. It neither changes the public listener nor writes activation.
The existing isolated probe, internal stage, fenced public handoff and retained
socket proof must pass before a worker retry. Its network evidence uses the
existing authenticated API fallback, which binds the selected public Service,
exact Ready Pod, EndpointSlice and connection tuple; it does not take over the
legacy Front's Unix socket. Old connections and old executors remain intact.

The read-only observation lane now selects only added or modified explicit
profiles, with the same current-main check and a 32-profile bound. Previously a
single new profile ran every historical profile, including intentionally offline
nodes. No mutation lane or health/quorum check is relaxed by this selection.

The existing transport selector exposed another deterministic pipeline gap:
`handoff` selects no `probe` mutation, but the same-run external witness job
required `probe.selected=true`. Consequently a distinct handoff intent could
never reach its required external evidence. A regression test reproduced the
missing witness selection. The repair resolves a separate read-only witness
target for `probe` or `handoff`, preserving exclusive mutation phases. Both
external and restricted-network checks use only that explicitly bound listener;
all current route, TLS, exact tuple, held socket and CAS checks remain mandatory.

Germany handoff CI `38293798130` succeeded; both public Services now select their
exact C Pod UIDs and all 24 public queries passed again. A five-minute recheck
retained 58 samples with zero SERVFAIL increments and zero observation errors.
An earlier local monitor failed before SSH authentication because its temporary
ControlPath plus OpenSSH suffix exceeded the Unix socket bound; that failed
collection is retained and is not counted as a healthy observation window.

The independent Front bootstrap is `f96dc5e9`, CI `38294675745` attempt two. The
first attempt stopped before writes on an API Server readiness observation;
the same immutable job passed all original checks on retry. The actual Pod UID
`4567c919-16a4-4040-be5d-b5f3578b68da` is Ready with zero restarts and image
`sha256:7508005ba14e61cc90124b17634b77723b8e9dea380d3f271e95db2a22a5bc9d`.
The next explicit transition only creates the isolated 15443 listener. It grants
no public 80/443 handoff or business exploration authority.

The isolated-probe run `38296485245` passed exact Front equivalence, held-socket
attribution, five hosted-runner observations and 42 restricted-network checks.
All evidence explicitly has `authorizes_traffic=false`. A subsequent read-only
drain job failed at 19:24:44 UTC because it enumerated every historical handoff,
although this run had performed no handoff. Its first error was an exact internal
stage mismatch. The repair schedules drain observation only after this run's
selected handoff and successful public verification, and reads only that target.
The next transition stages the internal-only Service; no public address is added.

The stage release `8f3dece0`, CI `38297915746`, passed. Service UID
`33a28c0e-3dd2-4d49-84d9-c22b5531bfd0` is at transport generation two, phase
`staged`, with no external IP and one Ready EndpointSlice referencing the exact
candidate Pod UID. The next separate handoff intent requires fresh same-run
external and restricted-network proofs, a held original socket, exact new-flow
attribution, Service CAS and compensation. It does not authorize deleting the
retained Front. Native strict-key poly/argus logins at 19:34 UTC still returned
`-bash`, `/bin/bash`, `/workspace`, and the trust file hash was unchanged.
