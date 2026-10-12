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
  An initial US checkpoint/read race was rejected; a fresh complete read passed.
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
