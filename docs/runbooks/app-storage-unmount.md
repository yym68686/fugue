# Remove one persistent mount online

Use `fugue app storage unmount` when the application no longer needs a
particular file or directory mount. This changes the mount declaration only;
the PVC, PV, and old contents are retained. At least one mount must remain.

```sh
fugue app storage unmount APP /etc/example.conf --plan
fugue app storage unmount APP /etc/example.conf \
  --expect SPEC_SHA256_FROM_PLAN --authorize-overlap --probe-file probe.json
```

The preview is read-only. `allowed` is specification eligibility, not a
live readiness or capacity guarantee. The second command explicitly authorizes
one temporary Pod and overlapping use of the retained mounts. Do not authorize
it for a single-writer application, even when the PVC permits multiple Pods on
one node. It currently supports one replica on an eligible dedicated filesystem
`movable_rwo` claim, canonical cluster-service traffic, and an existing drain
hook. It does not fall back to a stop/recreate rollout.

Example `probe.json`:

```json
{
  "path": "/v1/catalog",
  "method": "GET",
  "bearer_env": "CATALOG_KEY",
  "expected_body_contains": "catalog_version"
}
```

Use an idempotent, finite HTTP request that proves the replacement can serve
business traffic without the removed mount. The controller sends it directly
to the candidate's first declared port, with a 10-second timeout, no redirects,
and an assertion within the first 64 KiB of a 2xx response. A JSON `body` string
is supported for `POST`; it must not contain credentials. `bearer_env` is
optional and names an existing app environment variable, not a token value.
The check can retry, and is repeated for recreated Pods of the committed
revision until the next ordinary rollout changes the template.

The old Pod remains serving while the same-node candidate starts and passes
its probe. The Deployment remains paused until the canonical EndpointSlice
publishes both verified Pods; only then may the old Pod drain. A timeout before
candidate admission restores the prior template only if the old serving Pod
is still proven. If ownership, placement, or traffic cannot be established,
the operation retains its fence and requires observation rather than forcing
a restart. A lost node or PVC is outside this operation's availability guarantee.
