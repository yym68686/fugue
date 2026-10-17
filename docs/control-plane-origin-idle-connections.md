# Control-plane origin idle connections

The Edge HTTP/1 transport can finish a speculative dial after another
connection has already satisfied the request. Go puts that unused connection
into its idle pool and reports it as reused on its first checkout. Previously,
the API started its ten-second header deadline as soon as it accepted that
connection, while the Edge pool retained it for ninety seconds. A POST could
be written while the API close notification was still in transit, returning
EOF without a response.

The initial mitigation capped the API origin pool at five seconds. It prevented
the observed race but added intercontinental connection setup latency for
low-frequency API requests. The final policy separates idle waiting from header
reading on the server and restores the normal ninety-second Edge idle window.

## Server lifecycle

The public API plain HTTP/1 listener allows two minutes for the first byte on a
new TCP connection. Receiving that byte starts the existing ten-second header
budget exactly once; sending more partial bytes does not extend it. Established
keep-alive connections also have a two-minute idle limit. Previously their idle
wait was unbounded. Bodies, active handlers, streams and explicit deadlines
remain under net/http's normal lifecycle and request-specific controls.

The adapter uses socket deadlines without a timer goroutine per connection.
It preserves TCP half-close and response-copy behavior, honors newer explicit
read or whole-connection deadlines, and closes on failure to install the header
deadline. It accepts only plain TCP and is used only on the public API HTTP/1
listener. Separate metrics and mTLS RouteIntent listeners are unchanged.

## Client policy and rollout

Signed routes of kind `control-plane-api` retain connections for at most ninety
seconds, with a thirty-second margin before the API's initial idle deadline.
Stricter injected transport limits remain effective. Go's transport checks idle
age under its pool lock at checkout, so a late idle timer cannot make an expired
connection usable. Active requests are not subject to the pool idle timeout.
Application routes retain their existing policy and cross-domain pool sharing.
The API policy stays in the artifact/generation-scoped pool key; obsolete pools
still retire when signed serving artifacts change.

Deploy the server change through the API lane first with the five-second Edge
mitigation still active. Verify both replicas and direct health requests on
connections left unused beyond ten seconds, then deploy the ninety-second Edge
policy through the normal separate worker release. A failed API release leaves
the mitigation active. An explicit rollback to an API version without initial
idle support must first restore the five-second Edge policy. A later failed
release that rolls back to the initial-idle-capable API retains this behavior.

No POST retry is added. A write followed by EOF does not establish that the
origin did not execute it. The timeout margin is not a guarantee against
arbitrary network stalls or a differently configured origin. Neither runtime
policy rewrites serving intent, signed bundles, authorization, ACK checks or
the positive LKG.

## Verification

Real TCP regressions cover first and subsequent idle periods, a speculative
dial reused after eleven seconds, exactly-once POST handling without GetBody,
dripped headers on first and subsequent requests, slow bodies, handlers active
beyond the idle budget, explicit deadline replacement, empty-connection expiry,
bind failure and graceful shutdown. The Edge's consumed-POST EOF regression
continues to require one backend invocation with no replay. The original
five-second policy fails the new reuse test; the unadapted API listener fails
the server idle regression.
