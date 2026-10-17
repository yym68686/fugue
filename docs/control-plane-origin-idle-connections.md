# Control-plane origin idle connections

The API listener allows ten seconds to receive the first HTTP request header on
an accepted connection. The Edge HTTP/1 transport can finish a speculative dial
after another connection has already satisfied the request. Go puts that unused
connection into its idle pool and reports it as reused on its first checkout.
The normal 90-second transport idle timeout therefore exceeds the API's initial
header deadline. A POST can be written while the API's close notification is
still in transit, resulting in EOF without a response.

For signed routes of kind `control-plane-api`, the Edge caps the origin pool's
idle timeout at five seconds, preserving any stricter injected transport limit.
This retires the observed unused connection before the initial header deadline.
Idle expiration also has a checkout-time age check in Go's transport, so delayed
execution of the idle timer does not make an expired pooled connection usable.
Active requests, streams and uploads are not subject to this idle timeout.
Subsequent requests within the idle window still reuse connections.

The API idle policy is included in the existing artifact/generation-scoped pool
key so application routes cannot accidentally share the API's connection policy.
Application domain kinds still share their existing deployment-scoped pool. The
prototype and application-origin settings are unchanged. Artifact replacement
still retires obsolete pools; runtime connection policy does not change serving
intent, signed bundles, ACK validation or the positive LKG.

No extra POST retry is added. A write followed by EOF does not in general prove
that the origin did not execute the request. This change addresses the observed
unused-connection deadline race; other transport failures must retain their
original failure semantics. The margin is not a guarantee against arbitrary
network stalls or a different origin timeout configuration.

The integration regression uses real TCP connections and the API's ten-second
header timeout, forces a speculative dial to enter the unused pool, and delays
its EOF delivery. It fails with EOF before the fix and verifies exactly one POST
after the fix. Companion tests cover requests active beyond five seconds,
connection reuse, route-kind separation, artifact pool retirement, and the
existing consumed-POST EOF test.

## Separating initial idle waiting from header reads

The five-second pool limit increases connection churn for low-frequency API
requests, which is especially visible on intercontinental origins. The API now
allows an unused plain HTTP/1 TCP connection to wait up to two minutes for its
first byte. Receiving that byte starts the existing ten-second header budget
exactly once: sending more partial header bytes does not extend it. Established
keep-alive connections also have a two-minute idle limit. Previously their idle
wait was unbounded. Bodies, active handlers, streams and explicit deadlines
remain under net/http's normal lifecycle and request-specific controls.

The listener adapter uses socket deadlines and no additional timer goroutine.
It preserves TCP half-close and response-copy behavior, honors newer explicit
read/whole-connection deadlines, and fails closed if installing the header
deadline fails. It accepts only plain TCP and is used only on the public API's
HTTP/1 listener. The separate metrics and mTLS RouteIntent listeners are
unchanged. This is a connection lifecycle change, not a new route or API schema;
no artifact, configuration intent, admission rule or positive LKG is rewritten.

Roll this server change through the API lane first, while Edge retains its
five-second compatibility limit. Verify both API replicas and a direct health
request on a connection left unused beyond ten seconds before changing the
Edge pool policy in a separate release. A later Edge release may restore its
normal ninety-second idle reuse once all origins support the initial idle
window. An explicit rollback to an older API implementation must restore the
five-second Edge policy first; never rely on POST replay to bridge incompatible
timeout policies. A failed server release leaves the existing Edge mitigation
in place.

Regression coverage includes unused and previously used connections, a genuine
speculative transport dial, exactly-once POST handling without GetBody,
dripped headers on first and subsequent requests, a body arriving after the
header budget, an active handler exceeding the idle budget, explicit deadline
replacement, empty-connection expiry, bind failure, and graceful shutdown.
