# OOM scan Kubernetes connection lifecycle

The periodic OOM observer previously created a separate Kubernetes HTTP client for
every eligible app. Its completed requests left idle connections and their
goroutines alive until the transport timeout. A scan also continued constructing
clients after its context was canceled.

Each scan now owns at most one client per namespace, reuses it for that namespace's
app and PostgreSQL pod queries, and closes its idle connections on every exit.
The next scan constructs fresh clients. Credentials and observation results are
not cached across scans. Client and TLS setup grows with the number of eligible
namespaces instead of the number of apps; pod queries still visit every eligible
app. Cancellation stops further app inspection before new
right-sizing work is queued.

The app selectors, OOM lookback, event deduplication, billing checks, and desired
resource calculation retain their existing behavior. This change does not
change serving artifacts, configuration authority, or the public HTTP contract.

`TestOOMScanReusesNamespaceConnectionsAndClosesEachPass` exercises real TLS
connections for 20 synthetic apps in two namespaces. It checks every selector,
namespace and authorization, one connection per namespace per pass, fresh
clients in the next pass, and connection cleanup after successful scans,
Kubernetes read errors, and cancellation. Existing OOM tests retain coverage of
target selection, resource growth, billing caps, and event deduplication.
