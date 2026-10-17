# DNS plan validation decoding

DNS runtime observations revalidate the current artifact before and after their
network read. Each ValidateDNSCellPlan invocation previously serialized the
same artifact twice for an ordinary publication and three times for a Cell
publication, including an authority-transition predecessor. It also decoded the
same policy twice inside one validation.

The validator now serializes once and passes those exact local bytes to its
existing decoding stages. It reuses the first decoded policy only inside that
invocation. Independently callable public decoders still serialize their own
input; there is no cross-request or accepted-authority cache. Embedded Cell
references, predecessor intent, policy decoders, lineage, quorum, deterministic
plan replay, and rejection ordering remain checked. Store signatures, mutable
release/consumer reads, network rechecks and LKG behavior are unchanged.

Noncanonical case-insensitive policy aliases retain the prior typed merge and
error ordering. That uncommon path decodes policy again from the same serialized
bytes; ordinary compiler output reuses the initial decoded policy.

The ordinary path is still O(N) in serialized artifact size. Removing repeated
serialization reduces its constant factor and temporary allocations. Synthetic
local benchmarks compare the same valid artifact with 32 and 1,024 static DNS
records. In the 1,024-record fixture, medians were 2.433 ms and 790,546 B/op
before, versus 1.645 ms and 403,078 B/op after; allocation count fell from
18,712 to 9,366. These measurements are not a production latency guarantee.
Actual DNS fact gathering also includes database reads and remote observations.

Tests count content serialization for ordinary, Cell and transition artifacts,
compare malformed envelopes against the prior validation order, and mutate an
accepted artifact to ensure validation is not retained across calls. Existing
runtime tests cover authority changes during network observations, invalid
evidence, and unsafe redirects.

Deploy this compatible library optimization to the API through its explicit
production intent and normal CI. Other components can retain their previous
code and serving artifacts; no configuration or wire-schema migration is needed.
