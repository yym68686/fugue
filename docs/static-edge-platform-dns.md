# Platform app DNS through an independent static edge

`GET /v1/apps/{id}/route/static-edge` returns the app's desired DNS routing and
an opaque route revision. `PUT` requires platform admin and `expected_revision`;
omitting `routing` restores the ordinary platform policy. The CLI provides
`fugue static-edge app dns get|set|reset <app>` (`set --file routing.json`). A
successful write stores desired configuration; it does not attest to DNS convergence.

This capability supports sole-owner primary app root routes in the platform
application namespace and global consumer-readiness publications. Shared-host
path routing and Cell DNS publications are rejected. Each static registration
must be ready and belong to the app's tenant; platform admin can explicitly
share an independent edge across separate projects in that tenant. Projects,
app state, origins and releases remain distinct. Revoked registrations are
removed at the next normal signed publication.

Example routing file (identities and addresses must be replaced with the
operator's registered endpoints):

```json
{
  "version": "static-edge-dns-v1",
  "candidates": [{
    "registration_id": "static_edge_registration",
    "edge_id": "independent-entry",
    "addresses": ["1.1.1.1"],
    "probe_path": "/health",
    "route_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "status_code": 200
  }],
  "availability": {
    "version": "dns-availability-v1",
    "ordered_edge_ids": ["independent-entry", "managed-entry"],
    "recovery_successes": 3,
    "recovery_min_seconds": 30
  }
}
```

The full mixed order is explicit and may put either system first. At least one
managed fallback is required. Existing readiness, route exclusions and recovery
hysteresis apply; no healthy candidate means fail closed. DNS TTLs and recursive
caches still bound how quickly clients observe a change.

Before enabling the configuration, independently provision the app's actual
edge-to-origin route and a valid public certificate for the exact hostname.
The configured HEAD probe must return the exact expected status with:

* `X-Fugue-Static-Edge`: configured edge ID
* `X-Fugue-Static-Route`: configured app-specific route digest
* `X-Fugue-Static-Nonce`: echo of the request's unpredictable header
* `Cache-Control: no-store`

Attach these response headers only to the target app route and only for probe
requests; let the request reach the real app. Keep ordinary application caching
unchanged. The route digest is an operator assertion of that explicit app route,
not a platform deployment digest. Redirects are never followed. TLS uses public
trust with SNI and is pinned to the configured IP, with proxies disabled. A
static proof is separately typed and cannot impersonate a Fugue release proof.

Fugue signs the authorized endpoints and checks fresh probes in the authoritative
DNS consumers. Static processes are not enrolled as Fugue edges or deployment
consumers; their managers, binaries, certificates and configurations remain under
the independent operator's lifecycle. This feature never calls a static manager
or SSH to update them. Old DNS consumers must advertise `dns_static_edge_v1`
before the normal serving admission can accept such a publication.

After the producer publishes, use `fugue admin dns answer-check <hostname>` and
query every authoritative node over UDP and TCP. Verify TLS and application
responses directly at the static IP, then verify ordinary resolved access. For
rollback, use `dns reset` with a current route revision and wait for a new
verified platform publication; changing desired configuration alone is not a
completed rollback.
