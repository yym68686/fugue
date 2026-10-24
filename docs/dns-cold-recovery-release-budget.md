# DNS cold recovery and component health budgets

A restarted DNS reader must build new observation history. Its persisted positive
checkpoint does not authorize reusing old recovery successes. A policy requiring
three distinct successes spanning 60 seconds can need four 30-second sweeps:
the first three probe timestamps may span only 59.5 seconds because scheduling,
network latency and scan ordering vary. Artifact fetch, a complete bounded scan,
assignment revalidation and checkpoint persistence add time after those probes.

An observed standby release recorded two replayable snapshot failures. The later
failure contained three successful, fresh route proofs per eligible endpoint but
each history spanned less than 60 seconds. The unchanged selector correctly
returned `availability_recovering`. The release executor's fixed 120-second
health wait then rolled back the candidate about 117 seconds after process start,
before a complete qualifying publication was observed. This is not OOM evidence
and is not a reason to weaken the signed recovery gate.

The component registry now accepts optional `workload.healthTimeoutSeconds`,
bounded to 120–300 seconds. Omission preserves the previous wait. The value is
included in the immutable release plan digest; it is independent of serving
configuration and does not change DNS policy. The affected non-public standby
uses 240 seconds, allowing additional complete sweeps and publication checks.
That direct CI lane explicitly tracks the declarative executor and contract in
its source roots because interpreting this budget is part of its release
execution. Changing those sources still requires a same-commit component intent;
the release planner's dependency checks remain enforced.

This changes only the bounded `WaitHealthy` window. Exact workload/image identity,
complete health probes, any continuous soak, predecessor checks, the outer
execution deadline and rollback remain mandatory. Expiration still fails the
release. No elapsed-time condition manufactures readiness or selects a backend.
Public UDP/TCP handoff is a separate declaration requiring fresh verified facts.
