# Release drain observation failures

A failed observation is unknown execution state, never proof that a workload is
idle or stopped. Do not bypass traffic withdrawal, complete ownership, workload
identity, quiet-period, runtime or compare-and-swap retirement guards to make a
release converge.

The controller exposes read-only observations on its existing administrator-local
Unix socket. With already-authorized Kubernetes administrator access, inspect
each controller replica (leadership may have changed):

```sh
kubectl -n fugue-system exec CONTROLLER_POD -- curl --fail --silent --show-error \
  --max-time 5 --unix-socket /tmp/fugue-observation.sock \
  http://localhost/v1/snapshots/release-drain
```

This does not call the tenant application, start draining, or change routing.
The socket remains local, mode 0600; no listener or public API is added.

The snapshot retains the latest validated result/failure for at most 256 Pods
and, independently, the last 64 failures. Successful polling cannot evict rare
failures. Process restart clears this in-memory evidence; ordinary operation
steps and controller logs retain the corresponding safe error summary.

Inspect `stage`, `cause`, HTTP status, `context`, elapsed time, `has_deadline`,
initial request budget and remaining budget. An expired context differs from a
connection refusal, reset, DNS failure, transport timeout or invalid receipt.
`active_connections=-1` means no validated observation, not zero connections.
When `has_deadline=false`, budget and remaining time are -1. A deadline that was
already expired can legitimately have a negative initial budget.

Canonical handoff wait messages also distinguish endpoint convergence from the
last observed connection count. That count is historical, not a fresh proof.
Keep the existing independent Edge traffic proof and per-Pod identity/quiet
checks as the authority for any retirement.

Diagnostics deliberately exclude bearer tokens, URLs, queries, nonces, response
bodies and raw transport error strings. Error wrapping retains `errors.Is` /
`errors.As` causes for in-process callers without interpolating those strings.
If the historical failure predates these observations, do not infer its exact
cause from a later successful probe; collect a new failure first.

## Superseded release with a canceled deployment

An operation can be canceled after its candidate was already promoted. Its
cancellation history must remain intact, but cancellation alone must not retain
that immutable revision forever after a newer stable release supersedes it.
Only a bound previous/draining revision with a valid promotion timestamp before
the terminal operation's completion can enter the ordinary retirement gates.
Pending/running operations and unpromoted cancellations cannot use this path.
The same retention, grace, fresh traffic withdrawal, canonical Service reference,
complete Pod identity, positive drain and final compare-and-swap guards apply;
this is not permission to stop a busy process or retire an active release.
