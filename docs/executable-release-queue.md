# Preserve queued API and DNS predecessors

Successive component intents bind an exact predecessor image and configuration.
With GitHub Actions' default single pending slot, a third release can cancel the
second while the first is still waiting for a runner, even when
`cancel-in-progress` is false. The third then cannot satisfy its predecessor
check, and a successfully built fix never reaches production.

The API and edge-client execution lanes use `queue: max` and keep their existing
component-scoped concurrency groups. This retains up to 100 pending jobs without
allowing simultaneous execution. Configuration recovery remains independent of
code builds. The existing controller lanes already use this setting.

Queue admission follows job arrival, not Git commit order; different build times
can still reorder arrivals, and a full queue can still reject a job. The exact
predecessor, manifest and image checks therefore remain mandatory. Missing or
canceled predecessors require a reviewed declarative recovery intent against
the verified live state; queuing does not authorize skipping those checks.

See [GitHub's concurrency documentation](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency).
