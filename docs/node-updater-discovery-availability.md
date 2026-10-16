# Node updater discovery availability

The desired-state endpoint depends on the same verified discovery configuration
as the public discovery bundle. When that verified source is unavailable, both
endpoints return HTTP 503 with Cache-Control: no-store and no replacement
configuration. The updater retains its previous verified state and follows its
existing retry behavior. Other errors retain their existing status mapping.

The missing- or changed-authority checks, artifact signatures, current LKG
recheck and configuration recovery rules remain unchanged. An unavailable
response must never be converted into a fresh signed bundle or an empty policy.
The API logs the fixed discovery-unavailable classification without credentials
or configuration payloads, so later incidents can be distinguished from other
store failures.

A disposable-store regression first obtains a valid desired state, corrupts the
LKG signature, and verifies that public discovery and node desired state both
return 503 without changing stored configuration. Restoring the verified source
returns the endpoint to 200. Before the fix, the node endpoint returned 500 for
the same error that public discovery mapped to 503.

Production investigation observed one node-updater 500 spanning an LKG update
on October 16, 2026. The caller retried and refreshed its desired-state cache.
Its old response body was not retained, so the precise cause of that historical
request remains unproven; the regression proves the independent status-mapping
bug, not the attribution of every 500 to that branch.
